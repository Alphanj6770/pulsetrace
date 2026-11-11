"""The PulseTrace wire schema (version 1).

Events are plain JSON-compatible dictionaries described by ``TypedDict``s:

* they are what ``before_send`` hooks receive, so users edit ordinary dicts;
* they serialise with ``json.dumps`` without a conversion layer;
* mypy still checks every place the SDK constructs one.

The collector validates the same shape with Pydantic (see ``collector/schemas.py``);
the human-readable description lives in ``docs/event_schema.md``.
"""

from __future__ import annotations

from typing import Any, Literal, NotRequired, TypedDict

SCHEMA_VERSION = 1

EventType = Literal["error", "message", "transaction"]
Level = Literal["debug", "info", "warning", "error", "fatal"]
LEVELS: tuple[Level, ...] = ("debug", "info", "warning", "error", "fatal")

_LEVEL_ALIASES: dict[str, Level] = {
    "debug": "debug",
    "info": "info",
    "warning": "warning",
    "warn": "warning",
    "error": "error",
    "fatal": "fatal",
    "critical": "fatal",
}


def normalize_level(level: str | None, default: Level = "info") -> Level:
    if level is None:
        return default
    return _LEVEL_ALIASES.get(str(level).lower(), default)


class FramePayload(TypedDict):
    filename: str
    abs_path: str
    module: str | None
    function: str
    lineno: int | None
    in_app: bool
    context_line: NotRequired[str]
    pre_context: NotRequired[list[str]]
    post_context: NotRequired[list[str]]


class StacktracePayload(TypedDict):
    frames: list[FramePayload]  # oldest call first, crash site last
    frames_omitted: NotRequired[int]


ExceptionRelation = Literal["cause", "context", "group"]


class ExceptionValue(TypedDict):
    id: int
    parent_id: int | None
    relation: ExceptionRelation | None
    type: str
    module: str
    message: str
    stacktrace: StacktracePayload
    notes: NotRequired[list[str]]
    is_group: NotRequired[bool]
    group_index: NotRequired[int]


class MechanismPayload(TypedDict):
    type: str
    handled: bool


class ExceptionPayload(TypedDict):
    values: list[ExceptionValue]  # values[0] is the exception that was captured
    mechanism: MechanismPayload
    truncated: NotRequired[bool]


class BreadcrumbPayload(TypedDict):
    timestamp: str
    type: str
    category: str
    level: Level
    message: NotRequired[str]
    data: NotRequired[dict[str, Any]]


class RequestPayload(TypedDict, total=False):
    method: str
    route: str | None
    path: str
    query: dict[str, Any]
    headers: dict[str, str]
    status_code: int
    duration_ms: float
    client_ip: str


class TracePayload(TypedDict):
    trace_id: str
    span_id: str
    parent_span_id: str | None
    sampled: bool


class SpanPayload(TypedDict):
    span_id: str
    parent_span_id: str | None
    op: str
    name: str
    start_timestamp: str
    duration_ms: float
    status: str
    data: NotRequired[dict[str, Any]]


class TransactionPayload(TypedDict):
    name: str
    op: str
    start_timestamp: str
    duration_ms: float
    status: str
    spans: list[SpanPayload]
    dropped_spans: NotRequired[int]
    data: NotRequired[dict[str, Any]]


class SdkPayload(TypedDict):
    name: str
    version: str


class Event(TypedDict):
    schema_version: int
    event_id: str
    timestamp: str
    type: EventType
    level: Level
    platform: str
    service: str
    environment: str
    release: str | None
    sdk: SdkPayload
    server_name: NotRequired[str]
    runtime: NotRequired[dict[str, str]]
    message: NotRequired[str]
    exception: NotRequired[ExceptionPayload]
    request: NotRequired[RequestPayload]
    trace: NotRequired[TracePayload]
    transaction: NotRequired[TransactionPayload]
    breadcrumbs: NotRequired[list[BreadcrumbPayload]]
    tags: NotRequired[dict[str, str]]
    user: NotRequired[dict[str, Any]]
    extra: NotRequired[dict[str, Any]]
    fingerprint: NotRequired[list[str]]
