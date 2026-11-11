"""Pydantic validation of the wire schema (see ``docs/event_schema.md``).

Validation is strict about what the collector relies on (ids, timezone-aware
timestamps, event types, size limits) and permissive about everything else:
unknown fields are allowed, so an SDK newer than the collector still delivers.
The *original* JSON is what gets stored, so unknown fields are preserved.
"""

from __future__ import annotations

import uuid
from typing import Any, Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

HexTraceId = Field(pattern=r"^[0-9a-f]{32}$")
HexSpanId = Field(pattern=r"^[0-9a-f]{16}$")


class _Open(BaseModel):
    model_config = ConfigDict(extra="allow")


class FrameIn(_Open):
    filename: str = ""
    abs_path: str | None = None
    module: str | None = None
    function: str = "?"
    lineno: int | None = None
    in_app: bool = False
    context_line: str | None = None


class StacktraceIn(_Open):
    frames: list[FrameIn] = Field(default_factory=list, max_length=1000)
    frames_omitted: int = 0


class ExceptionValueIn(_Open):
    id: int = 0
    parent_id: int | None = None
    relation: Literal["cause", "context", "group"] | None = None
    type: str = Field(min_length=1, max_length=512)
    module: str | None = None
    message: str = Field(default="", max_length=65_536)
    stacktrace: StacktraceIn = Field(default_factory=StacktraceIn)


class ExceptionIn(_Open):
    values: list[ExceptionValueIn] = Field(min_length=1, max_length=64)


class TraceIn(_Open):
    trace_id: str = HexTraceId
    span_id: str = HexSpanId
    parent_span_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{16}$")
    sampled: bool = False


class SpanIn(_Open):
    span_id: str = HexSpanId
    parent_span_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{16}$")
    op: str = Field(max_length=200)
    name: str = Field(max_length=2048)
    start_timestamp: AwareDatetime
    duration_ms: float = Field(ge=0)
    status: str = Field(default="ok", max_length=64)


class TransactionIn(_Open):
    name: str = Field(max_length=2048)
    op: str = Field(max_length=200)
    start_timestamp: AwareDatetime
    duration_ms: float = Field(ge=0)
    status: str = Field(default="ok", max_length=64)
    spans: list[SpanIn] = Field(default_factory=list, max_length=5000)


class EventIn(_Open):
    schema_version: int = Field(default=1, ge=1, le=1)
    event_id: uuid.UUID
    timestamp: AwareDatetime
    type: Literal["error", "message", "transaction"]
    level: Literal["debug", "info", "warning", "error", "fatal"] = "error"
    service: str = Field(min_length=1, max_length=200)
    environment: str = Field(default="production", min_length=1, max_length=200)
    release: str | None = Field(default=None, max_length=200)
    message: str | None = Field(default=None, max_length=65_536)
    exception: ExceptionIn | None = None
    request: dict[str, Any] | None = None
    trace: TraceIn | None = None
    transaction: TransactionIn | None = None
    breadcrumbs: list[dict[str, Any]] = Field(default_factory=list, max_length=500)
    tags: dict[str, str] = Field(default_factory=dict)
    fingerprint: list[str] | None = Field(default=None, max_length=32)

    @model_validator(mode="after")
    def _payload_matches_type(self) -> Self:
        if self.type == "error" and self.exception is None:
            raise ValueError("error events require 'exception'")
        if self.type == "message" and not self.message:
            raise ValueError("message events require 'message'")
        if self.type == "transaction" and (self.transaction is None or self.trace is None):
            raise ValueError("transaction events require 'transaction' and 'trace'")
        return self


class BatchIn(_Open):
    schema_version: int = Field(default=1, ge=1, le=1)
    sent_at: AwareDatetime | None = None
    sdk: dict[str, Any] | None = None
    events: list[Any]  # validated one by one so one bad event cannot sink a batch


class RejectedEvent(BaseModel):
    index: int
    event_id: str | None = None
    error: str


class IngestResponse(BaseModel):
    accepted: int
    duplicates: int
    rejected: int
    errors: list[RejectedEvent] = Field(default_factory=list)
