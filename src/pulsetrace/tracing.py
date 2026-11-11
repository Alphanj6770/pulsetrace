"""Spans, transactions and W3C Trace Context propagation.

A *span* is one timed operation. The first span of a trace inside a service is
its *transaction* (the local root, e.g. one HTTP request); every span started
while it is active becomes a descendant and is reported inside the
transaction's event when the transaction finishes.

Spans propagate across services with the W3C ``traceparent`` header::

    traceparent: 00-<32 hex trace-id>-<16 hex parent span-id>-<2 hex flags>

so a request that travels payment-service -> fraud-service produces one trace:
fraud-service's transaction records payment-service's HTTP client span as its
parent.

Durations come from ``time.perf_counter_ns()`` (monotonic, immune to NTP/clock
adjustments). Only the transaction's start reads the wall clock; child span
start times are derived from it plus a monotonic offset, so a waterfall can
never show a child starting before its parent because the wall clock moved.
"""

from __future__ import annotations

import asyncio
import re
import threading
import time
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

from pulsetrace._log import logger
from pulsetrace.context import _SPAN
from pulsetrace.events import SpanPayload, TracePayload
from pulsetrace.serialize import normalize
from pulsetrace.utils import format_timestamp, new_span_id, truncate

if TYPE_CHECKING:
    import contextvars

    from pulsetrace.client import PulseTrace
    from pulsetrace.scope import Scope

_TRACEPARENT_RE = re.compile(r"([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})(-.*)?")
_INVALID_TRACE_ID = "0" * 32
_INVALID_SPAN_ID = "0" * 16
_MAX_TRACESTATE = 512
_MAX_NAME = 512


@dataclass(frozen=True, slots=True)
class TraceParent:
    trace_id: str
    parent_span_id: str
    sampled: bool

    @classmethod
    def parse(cls, header: str | None) -> TraceParent | None:
        """Parse a ``traceparent`` header; ``None`` for anything invalid.

        Follows the W3C rules: lowercase hex only, version ``ff`` is invalid,
        all-zero ids are invalid, version ``00`` has exactly four fields, and
        higher versions may append fields we must ignore (forward compatibility).
        """
        if not header or len(header) > 512:
            return None
        match = _TRACEPARENT_RE.fullmatch(header.strip())
        if match is None:
            return None
        version, trace_id, span_id, flags, rest = match.groups()
        if version == "ff" or (version == "00" and rest):
            return None
        if trace_id == _INVALID_TRACE_ID or span_id == _INVALID_SPAN_ID:
            return None
        return cls(trace_id=trace_id, parent_span_id=span_id, sampled=bool(int(flags, 16) & 0x01))

    def to_header(self) -> str:
        return f"00-{self.trace_id}-{self.parent_span_id}-{'01' if self.sampled else '00'}"


def sanitize_tracestate(value: str | None) -> str | None:
    """``tracestate`` is opaque vendor data: forward it unchanged if it is sane."""
    if not value or len(value) > _MAX_TRACESTATE:
        return None
    if any(ord(char) < 0x20 or ord(char) > 0x7E for char in value):
        return None
    return value


def status_from_http(code: int) -> str:
    if code < 400:
        return "ok"
    return {
        400: "invalid_argument",
        401: "unauthenticated",
        403: "permission_denied",
        404: "not_found",
        408: "deadline_exceeded",
        409: "already_exists",
        429: "resource_exhausted",
        499: "cancelled",
        501: "unimplemented",
        503: "unavailable",
        504: "deadline_exceeded",
    }.get(code, "invalid_argument" if code < 500 else "internal_error")


class Span:
    """A timed operation. Use as ``with``/``async with``, or call :meth:`finish`.

    Spans never swallow exceptions: leaving the block with an exception marks the
    span as failed and lets the exception continue unchanged.
    """

    __slots__ = (
        "_children",
        "_client",
        "_data",
        "_dropped_children",
        "_end_ns",
        "_lock",
        "_max_children",
        "_scope",
        "_segment",
        "_start_ns",
        "_start_wall",
        "_token",
        "name",
        "op",
        "parent_span_id",
        "sampled",
        "span_id",
        "status",
        "trace_id",
        "tracestate",
    )

    def __init__(
        self,
        *,
        client: PulseTrace | None,
        trace_id: str,
        span_id: str,
        parent_span_id: str | None,
        op: str,
        name: str,
        sampled: bool,
        segment: Span | None = None,
        scope: Scope | None = None,
        tracestate: str | None = None,
        max_children: int = 0,
    ) -> None:
        self._client = client
        self.trace_id = trace_id
        self.span_id = span_id
        self.parent_span_id = parent_span_id
        self.op = op
        self.name = truncate(name, _MAX_NAME)
        self.sampled = sampled
        self.status = "ok"
        self.tracestate = tracestate
        self._data: dict[str, Any] = {}
        self._start_ns = time.perf_counter_ns()
        self._start_wall = time.time()
        self._end_ns: int | None = None
        self._token: contextvars.Token[Span | None] | None = None
        self._scope = scope
        # None on the transaction itself. Pointing it at `self` would make every
        # transaction a reference cycle that only the cyclic GC can free; under
        # load those GC passes showed up as a 3x p99 latency tail.
        self._segment: Span | None = segment
        # Only meaningful on a transaction (segment root).
        self._children: list[SpanPayload] = []
        self._dropped_children = 0
        self._max_children = max_children
        self._lock: threading.Lock | None = threading.Lock() if segment is None else None

    # -- public API --------------------------------------------------------------

    @property
    def is_transaction(self) -> bool:
        return self._segment is None

    @property
    def finished(self) -> bool:
        return self._end_ns is not None

    @property
    def duration_ms(self) -> float | None:
        if self._end_ns is None:
            return None
        return (self._end_ns - self._start_ns) / 1_000_000

    def start_child(self, name: str, *, op: str = "function") -> Span:
        return Span(
            client=self._client,
            trace_id=self.trace_id,
            span_id=new_span_id(),
            parent_span_id=self.span_id,
            op=op,
            name=name,
            sampled=self.sampled,
            segment=self._segment or self,
            tracestate=(self._segment or self).tracestate,
        )

    def set_data(self, key: str, value: object) -> None:
        if self.sampled:
            self._data[truncate(str(key), 128)] = normalize(value, max_depth=3, max_items=50)

    def set_status(self, status: str) -> None:
        self.status = status

    def set_http_status(self, code: int) -> None:
        self.set_data("http.status_code", code)
        self.status = status_from_http(code)

    def traceparent(self) -> str:
        """The header value that makes a downstream service a child of this span."""
        return TraceParent(self.trace_id, self.span_id, self.sampled).to_header()

    def trace_context(self) -> TracePayload:
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "sampled": self.sampled,
        }

    def finish(self) -> None:
        """End the span. Idempotent."""
        end_ns = time.perf_counter_ns()
        lock = self._lock
        if lock is not None:
            # Set under the lock so a child finishing concurrently on another
            # thread is either included or counted as dropped, never lost.
            with lock:
                already_finished = self._end_ns is not None
                if not already_finished:
                    self._end_ns = end_ns
            if already_finished:
                return
        else:
            if self._end_ns is not None:
                return
            self._end_ns = end_ns
        if not self.sampled or self._client is None:
            return
        if self._segment is None:
            self._client._capture_transaction(self)
        else:
            self._segment._record_child(self)

    # -- context management -------------------------------------------------------

    def activate(self) -> None:
        """Make this the current span (what ``with span:`` does on entry)."""
        self._token = _SPAN.set(self)

    def deactivate(self) -> None:
        token, self._token = self._token, None
        if token is None:
            return
        try:
            _SPAN.reset(token)
        except ValueError:
            # Entered in one context, exited in another (e.g. across tasks).
            # Resetting is impossible there; leaving the variable alone is safe.
            logger.debug("span %s exited in a different context than it was entered", self.name)

    def _exit(self, exc: BaseException | None) -> None:
        if exc is not None and self.status == "ok":
            self.status = (
                "cancelled" if isinstance(exc, asyncio.CancelledError) else "internal_error"
            )
        try:
            self.finish()
        finally:
            self.deactivate()

    def __enter__(self) -> Self:
        self.activate()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._exit(exc)

    async def __aenter__(self) -> Self:
        self.activate()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._exit(exc)

    def __repr__(self) -> str:
        return (
            f"<Span op={self.op!r} name={self.name!r} trace_id={self.trace_id} "
            f"span_id={self.span_id} sampled={self.sampled}>"
        )

    # -- transaction internals ------------------------------------------------------

    def _record_child(self, child: Span) -> None:
        """Store a finished child as plain data.

        Keeping the child ``Span`` itself would create a cycle (the child points
        at its transaction, the transaction's list points back), so an abandoned
        transaction would become garbage only the cyclic GC can reclaim.
        """
        lock = self._lock
        if lock is None:
            return
        end_ns = child._end_ns if child._end_ns is not None else child._start_ns
        payload: SpanPayload = {
            "span_id": child.span_id,
            "parent_span_id": child.parent_span_id,
            "op": child.op,
            "name": child.name,
            "start_timestamp": format_timestamp(self._wall_time(child._start_ns)),
            "duration_ms": round((end_ns - child._start_ns) / 1_000_000, 3),
            "status": child.status,
        }
        if child._data:
            payload["data"] = child._data
        with lock:
            if self._end_ns is not None or len(self._children) >= self._max_children:
                # Late (transaction already reported) or over the per-transaction cap.
                self._dropped_children += 1
                return
            self._children.append(payload)

    def _wall_time(self, perf_ns: int) -> float:
        return self._start_wall + (perf_ns - self._start_ns) / 1_000_000_000

    def _take_child_payloads(self) -> tuple[list[SpanPayload], int]:
        lock = self._lock
        if lock is None:
            return [], 0
        with lock:
            children, self._children = self._children, []
            return children, self._dropped_children
