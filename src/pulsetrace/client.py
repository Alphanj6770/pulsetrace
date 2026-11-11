"""The PulseTrace client: the public entry point of the SDK.

Every public method obeys the same contract:

* it never raises into the application (configuration errors at construction
  time are the only exception);
* it never blocks on I/O - capturing an event serialises it and hands it to a
  bounded buffer; a background worker does the network I/O;
* it never changes the behaviour of the code it observes - exceptions are
  reported, not swallowed or replaced, and user objects are copied, not kept.
"""

from __future__ import annotations

import asyncio
import atexit
import json
import os
import platform
import socket
import sys
import threading
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

from pulsetrace._background import BackgroundLoop
from pulsetrace._log import RateLimitedLog, enable_debug_logging, logger
from pulsetrace._version import __version__
from pulsetrace.config import BeforeSend, Options, OverflowPolicy, TraceSampling
from pulsetrace.context import _CAPTURING, _SCOPE, _SPAN
from pulsetrace.events import SCHEMA_VERSION, Event, EventType, Level, normalize_level
from pulsetrace.exceptions import serialize_exception
from pulsetrace.processors import Scrubber
from pulsetrace.sampling import Sampler
from pulsetrace.scope import Scope
from pulsetrace.serialize import normalize
from pulsetrace.tracing import Span, TraceParent, sanitize_tracestate
from pulsetrace.transport.base import NullTransport, ShutdownReport, Transport
from pulsetrace.utils import (
    format_timestamp,
    new_event_id,
    new_span_id,
    new_trace_id,
    now_timestamp,
    safe_str,
    truncate,
)

if TYPE_CHECKING:
    from pulsetrace.integrations import Integration

SDK_NAME = "pulsetrace.python"
_MIB = 1024 * 1024
_RUNTIME = {"name": platform.python_implementation(), "version": platform.python_version()}
_COUNTERS = (
    "captured",
    "sampled_out",
    "duplicates_suppressed",
    "dropped_before_send",
    "before_send_errors",
    "dropped_oversized",
    "dropped_queue",
    "internal_errors",
)


def _env(name: str) -> str | None:
    value = os.environ.get(name, "").strip()
    return value or None


def _hostname() -> str | None:
    try:
        return socket.gethostname() or None
    except OSError:
        return None


class PulseTrace:
    """An observability client for one service.

    >>> telemetry = PulseTrace(endpoint="http://localhost:9000/v1/events",
    ...                        service_name="payment-service")  # doctest: +SKIP
    >>> await telemetry.start()                                   # doctest: +SKIP

    ``endpoint``, ``api_key``, ``service_name``, ``environment`` and ``release``
    fall back to the ``PULSETRACE_*`` environment variables of the same name.
    Without an endpoint (and without a custom ``transport``) the client is a
    cheap no-op, so instrumented code needs no ``if telemetry:`` guards.
    """

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        api_key: str | None = None,
        service_name: str | None = None,
        environment: str | None = None,
        release: str | None = None,
        server_name: str | None = None,
        enabled: bool = True,
        traces_sample_rate: float = 0.0,
        error_sample_rate: float = 1.0,
        trace_sampling: TraceSampling = "consistent",
        queue_size: int = 2048,
        max_queue_bytes: int = 16 * _MIB,
        overflow_policy: OverflowPolicy = "drop_newest",
        max_batch_size: int = 100,
        max_batch_bytes: int = _MIB,
        flush_interval: float = 1.0,
        max_retries: int = 5,
        initial_backoff: float = 0.5,
        max_backoff: float = 30.0,
        max_retry_after: float = 300.0,
        request_timeout: float = 5.0,
        compress: bool = True,
        max_breadcrumbs: int = 100,
        max_spans_per_transaction: int = 1000,
        max_frames: int = 100,
        max_exceptions: int = 16,
        max_message_length: int = 8192,
        max_event_bytes: int = _MIB,
        send_default_pii: bool = False,
        redact_keys: Sequence[str] = (),
        scrub_card_numbers: bool = True,
        in_app_include: Sequence[str] = (),
        in_app_exclude: Sequence[str] = (),
        before_send: BeforeSend | None = None,
        debug: bool = False,
        tags: Mapping[str, object] | None = None,
        integrations: Sequence[Integration] = (),
        transport: Transport | None = None,
    ) -> None:
        self.options = Options(
            endpoint=endpoint if endpoint is not None else _env("PULSETRACE_ENDPOINT"),
            api_key=api_key if api_key is not None else _env("PULSETRACE_API_KEY"),
            service_name=service_name or _env("PULSETRACE_SERVICE_NAME") or "unknown-service",
            environment=environment or _env("PULSETRACE_ENVIRONMENT") or "production",
            release=release if release is not None else _env("PULSETRACE_RELEASE"),
            server_name=server_name,
            enabled=enabled,
            traces_sample_rate=traces_sample_rate,
            error_sample_rate=error_sample_rate,
            trace_sampling=trace_sampling,
            queue_size=queue_size,
            max_queue_bytes=max_queue_bytes,
            overflow_policy=overflow_policy,
            max_batch_size=max_batch_size,
            max_batch_bytes=max_batch_bytes,
            flush_interval=flush_interval,
            max_retries=max_retries,
            initial_backoff=initial_backoff,
            max_backoff=max_backoff,
            max_retry_after=max_retry_after,
            request_timeout=request_timeout,
            compress=compress,
            max_breadcrumbs=max_breadcrumbs,
            max_spans_per_transaction=max_spans_per_transaction,
            max_frames=max_frames,
            max_exceptions=max_exceptions,
            max_message_length=max_message_length,
            max_event_bytes=max_event_bytes,
            send_default_pii=send_default_pii,
            redact_keys=frozenset(redact_keys),
            scrub_card_numbers=scrub_card_numbers,
            in_app_include=tuple(in_app_include),
            in_app_exclude=tuple(in_app_exclude),
            before_send=before_send,
            debug=debug,
        )
        if debug:
            enable_debug_logging()

        # An explicitly supplied transport always enables the client (even a
        # NullTransport: "instrument everything, transmit nothing").
        has_destination = transport is not None or self.options.endpoint is not None
        if transport is None:
            if self.options.endpoint is not None:
                from pulsetrace.transport.http import AsyncHttpTransport

                transport = AsyncHttpTransport(self.options)
            else:
                transport = NullTransport()
                if self.options.enabled:
                    logger.debug("no endpoint configured; PulseTrace is disabled")
        self._transport: Transport = transport
        self._enabled = self.options.enabled and has_destination
        self._closed = False
        self._scrubber = Scrubber(
            extra_keys=self.options.redact_keys,
            scrub_card_numbers=self.options.scrub_card_numbers,
        )
        self._trace_sampler = Sampler(self.options.traces_sample_rate)
        self._error_sampler = Sampler(self.options.error_sample_rate)
        self._root_scope = Scope(max_breadcrumbs=self.options.max_breadcrumbs, root=True)
        if tags:
            self._root_scope.set_tags(tags)
        self._server_name = self.options.server_name or _hostname()
        self._counter_lock = threading.Lock()
        self._counters = dict.fromkeys(_COUNTERS, 0)
        self._warn = RateLimitedLog()
        self._background: BackgroundLoop | None = None
        self._report: ShutdownReport | None = None
        self._integrations: list[Integration] = []
        for integration in integrations:
            self._install(integration)

    # ------------------------------------------------------------------ state

    @property
    def enabled(self) -> bool:
        return self._enabled and not self._closed

    @property
    def transport(self) -> Transport:
        return self._transport

    def stats(self) -> dict[str, Any]:
        """A snapshot of what the SDK did: captured, sampled, dropped, delivered."""
        with self._counter_lock:
            counters = dict(self._counters)
        try:
            transport = self._transport.stats()
        except Exception:
            transport = {}
        return {"enabled": self.enabled, "events": counters, "transport": transport}

    def _count(self, name: str, amount: int = 1) -> None:
        with self._counter_lock:
            self._counters[name] += amount

    def _install(self, integration: Integration) -> None:
        try:
            integration.setup(self)
        except Exception:
            logger.warning("failed to set up integration %r", integration, exc_info=True)
            return
        self._integrations.append(integration)

    # -------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        """Start the background delivery worker on the running event loop."""
        if self._closed:
            return
        try:
            await self._transport.start()
        except Exception:
            logger.warning("failed to start the PulseTrace transport", exc_info=True)

    async def flush(self, timeout: float = 5.0) -> bool:
        """Wait (up to ``timeout`` seconds) until queued events are delivered or dropped."""
        try:
            return await self._transport.flush(timeout)
        except Exception:
            self._count("internal_errors")
            return False

    async def shutdown(self, timeout: float = 5.0) -> ShutdownReport:
        """Stop accepting events, deliver what is queued within ``timeout``, release resources.

        Returns a report including how many events were still unsent at the
        deadline. Safe to call more than once.
        """
        if self._report is not None:
            return self._report
        self._closed = True
        try:
            report = await self._transport.shutdown(timeout)
        except Exception:
            logger.warning("PulseTrace transport failed to shut down cleanly", exc_info=True)
            report = ShutdownReport(delivered=0, dropped=0, unsent=0, elapsed=0.0)
        self._teardown_integrations()
        if self._background is not None:
            self._background.stop()
        self._report = report
        if report.unsent:
            logger.warning("PulseTrace shut down with %d unsent events", report.unsent)
        return report

    def _teardown_integrations(self) -> None:
        for integration in reversed(self._integrations):
            try:
                integration.teardown()
            except Exception:
                logger.debug("failed to tear down %r", integration, exc_info=True)
        self._integrations.clear()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.shutdown()

    # -- synchronous applications ------------------------------------------------

    def start_in_background(self) -> None:
        """Run delivery on a private loop in a daemon thread (no asyncio needed).

        Registers an ``atexit`` hook that flushes queued events for up to two
        seconds when the interpreter exits.
        """
        if self._background is not None or self._closed:
            return
        background = BackgroundLoop()
        background.start()
        background.run(self._transport.start(), timeout=5.0)
        self._background = background
        atexit.register(self._atexit)

    def flush_sync(self, timeout: float = 5.0) -> bool:
        if self._background is None or not self._background.alive:
            return False
        try:
            return self._background.run(self._transport.flush(timeout), timeout + 1.0)
        except Exception:
            return False

    def shutdown_sync(self, timeout: float = 5.0) -> ShutdownReport:
        if self._report is not None:
            return self._report
        background = self._background
        if background is not None and background.alive:
            try:
                report = background.run(self.shutdown(timeout), timeout + 2.0)
            except Exception:
                report = ShutdownReport(delivered=0, dropped=0, unsent=0, elapsed=timeout)
            background.stop()
            return report
        return asyncio.run(self.shutdown(timeout))

    def _atexit(self) -> None:
        if not self._closed:
            self.shutdown_sync(timeout=2.0)

    # ------------------------------------------------------------------ scopes

    def current_scope(self) -> Scope:
        """The isolation scope of the current request, or the root scope."""
        return _SCOPE.get() or self._root_scope

    @contextmanager
    def isolation_scope(self) -> Iterator[Scope]:
        """Run a block with its own scope (usable in sync and async code).

        Integrations wrap every request in one of these; use it yourself for
        background jobs: ``with telemetry.isolation_scope(): process(job)``.
        """
        scope = self.current_scope().fork()
        token = _SCOPE.set(scope)
        try:
            yield scope
        finally:
            _SCOPE.reset(token)

    def set_tag(self, key: str, value: object) -> None:
        self._safely(self.current_scope().set_tag, key, value)

    def set_tags(self, tags: Mapping[str, object]) -> None:
        self._safely(self.current_scope().set_tags, tags)

    def set_extra(self, key: str, value: object) -> None:
        self._safely(self.current_scope().set_extra, key, value)

    def set_user(self, user: Mapping[str, object] | None) -> None:
        self._safely(self.current_scope().set_user, user)

    def last_event_id(self) -> str | None:
        """Id of the last event captured in this scope (e.g. for a support reference)."""
        return self.current_scope().last_event_id

    def add_breadcrumb(
        self,
        message: str | None = None,
        *,
        category: str = "default",
        level: str = "info",
        type: str = "default",
        data: Mapping[str, object] | None = None,
    ) -> None:
        if not self.enabled:
            return
        try:
            self.current_scope().add_breadcrumb(
                message=safe_str(message, 1024) if message is not None else None,
                category=truncate(safe_str(category), 128),
                level=normalize_level(level),
                type=truncate(safe_str(type), 64),
                data=normalize(dict(data), max_depth=3, max_items=20, max_string=512)
                if data
                else None,
            )
        except Exception:
            self._count("internal_errors")

    def _safely(self, fn: Any, *args: Any) -> None:
        try:
            fn(*args)
        except Exception:
            self._count("internal_errors")

    # ----------------------------------------------------------------- capture

    def capture_exception(
        self,
        error: BaseException | None = None,
        *,
        level: str = "error",
        tags: Mapping[str, object] | None = None,
        extra: Mapping[str, object] | None = None,
        fingerprint: Sequence[str] | None = None,
        mechanism: str = "manual",
        handled: bool = True,
    ) -> str | None:
        """Report an exception; returns the event id, or ``None`` if not sent.

        With no argument, reports the exception currently being handled. The
        exception itself is never modified, wrapped or suppressed.
        """
        if not self.enabled or _CAPTURING.get():
            return None
        try:
            # `object`: untyped callers can pass anything; never trust the annotation.
            candidate: object = error if error is not None else sys.exception()
            if not isinstance(candidate, BaseException):
                return None
            error = candidate
            scope = self.current_scope()
            if not scope.mark_captured(error):
                self._count("duplicates_suppressed")
                return None
            event_id = new_event_id()
            if not self._error_sampler(event_id):
                self._count("sampled_out")
                return None
            values, truncated = serialize_exception(
                error,
                max_exceptions=self.options.max_exceptions,
                max_frames=self.options.max_frames,
                max_message_length=self.options.max_message_length,
                in_app_include=self.options.in_app_include,
                in_app_exclude=self.options.in_app_exclude,
            )
            event = self._new_event("error", normalize_level(level, "error"), event_id)
            event["exception"] = {
                "values": values,
                "mechanism": {"type": mechanism, "handled": handled},
            }
            if truncated:
                event["exception"]["truncated"] = True
            return self._dispatch(event, scope, tags=tags, extra=extra, fingerprint=fingerprint)
        except Exception:
            self._internal_error("capture_exception")
            return None

    def capture_message(
        self,
        message: str,
        level: str = "info",
        *,
        tags: Mapping[str, object] | None = None,
        extra: Mapping[str, object] | None = None,
        fingerprint: Sequence[str] | None = None,
    ) -> str | None:
        """Report a message (e.g. ``capture_message("Payment failed", "error")``)."""
        if not self.enabled or _CAPTURING.get():
            return None
        try:
            event_id = new_event_id()
            if not self._error_sampler(event_id):
                self._count("sampled_out")
                return None
            event = self._new_event("message", normalize_level(level), event_id)
            event["message"] = safe_str(message, self.options.max_message_length)
            return self._dispatch(
                event, self.current_scope(), tags=tags, extra=extra, fingerprint=fingerprint
            )
        except Exception:
            self._internal_error("capture_message")
            return None

    # ----------------------------------------------------------------- tracing

    def start_transaction(
        self,
        name: str,
        *,
        op: str = "function",
        traceparent: str | None = None,
        tracestate: str | None = None,
    ) -> Span:
        """Start the local root span of a trace, optionally continuing an upstream one.

        The sampling decision is made here, once per trace: deterministically
        from the trace id (``trace_sampling="consistent"``, the default) or by
        inheriting the upstream ``sampled`` flag (``"parent_based"``).
        """
        parent = TraceParent.parse(traceparent) if traceparent else None
        if parent is not None:
            trace_id = parent.trace_id
            parent_span_id: str | None = parent.parent_span_id
            if self.options.trace_sampling == "parent_based":
                sampled = parent.sampled
            else:
                sampled = self._trace_sampler(trace_id)
        else:
            trace_id = new_trace_id()
            parent_span_id = None
            sampled = self._trace_sampler(trace_id)
        enabled = self.enabled
        return Span(
            client=self if enabled else None,
            trace_id=trace_id,
            span_id=new_span_id(),
            parent_span_id=parent_span_id,
            op=op,
            name=name,
            sampled=sampled and enabled,
            scope=self.current_scope(),
            tracestate=sanitize_tracestate(tracestate) if parent is not None else None,
            max_children=self.options.max_spans_per_transaction,
        )

    def start_span(self, name: str, *, op: str = "function") -> Span:
        """A child of the current span, or a new transaction if there is none.

        >>> async with telemetry.start_span("payment.authorize"):  # doctest: +SKIP
        ...     await authorize_payment()
        """
        parent = _SPAN.get()
        if parent is not None:
            return parent.start_child(name, op=op)
        return self.start_transaction(name, op=op)

    def current_span(self) -> Span | None:
        return _SPAN.get()

    def traceparent(self) -> str | None:
        """``traceparent`` header value for manual propagation (non-HTTPX clients)."""
        span = _SPAN.get()
        return span.traceparent() if span is not None else None

    def _capture_transaction(self, segment: Span) -> None:
        """Called by a transaction span when it finishes (only if sampled)."""
        if not self.enabled:
            return
        try:
            end_ns = segment._end_ns if segment._end_ns is not None else segment._start_ns
            spans, dropped = segment._take_child_payloads()
            event = self._new_event("transaction", "info", new_event_id())
            event["timestamp"] = format_timestamp(segment._wall_time(end_ns))
            transaction: Any = {
                "name": segment.name,
                "op": segment.op,
                "start_timestamp": format_timestamp(segment._start_wall),
                "duration_ms": round((end_ns - segment._start_ns) / 1_000_000, 3),
                "status": segment.status,
                "spans": spans,
            }
            if dropped:
                transaction["dropped_spans"] = dropped
            if segment._data:
                transaction["data"] = segment._data
            event["transaction"] = transaction
            event["trace"] = segment.trace_context()
            scope = segment._scope if segment._scope is not None else self.current_scope()
            self._dispatch(event, scope, include_breadcrumbs=False, low_priority=True)
        except Exception:
            self._internal_error("transaction")

    # ---------------------------------------------------------------- pipeline

    def _new_event(self, event_type: EventType, level: Level, event_id: str) -> Event:
        event: Event = {
            "schema_version": SCHEMA_VERSION,
            "event_id": event_id,
            "timestamp": now_timestamp(),
            "type": event_type,
            "level": level,
            "platform": "python",
            "service": self.options.service_name,
            "environment": self.options.environment,
            "release": self.options.release,
            "sdk": {"name": SDK_NAME, "version": __version__},
            "runtime": dict(_RUNTIME),
        }
        if self._server_name:
            event["server_name"] = self._server_name
        return event

    def _dispatch(
        self,
        event: Event,
        scope: Scope,
        *,
        tags: Mapping[str, object] | None = None,
        extra: Mapping[str, object] | None = None,
        fingerprint: Sequence[str] | None = None,
        include_breadcrumbs: bool = True,
        low_priority: bool = False,
    ) -> str | None:
        """Scope -> context -> scrub -> before_send -> serialise -> queue."""
        token = _CAPTURING.set(True)
        try:
            scope.apply(event, include_breadcrumbs=include_breadcrumbs)
            if tags:
                event["tags"] = {
                    **event.get("tags", {}),
                    **{
                        truncate(safe_str(key), 128): truncate(safe_str(value), 256)
                        for key, value in tags.items()
                    },
                }
            if extra:
                event["extra"] = {**event.get("extra", {}), **normalize(dict(extra))}
            if fingerprint:
                event["fingerprint"] = [truncate(safe_str(part), 256) for part in fingerprint[:32]]
            if "trace" not in event:
                span = _SPAN.get()
                if span is not None:
                    event["trace"] = span.trace_context()

            event = self._scrubber.scrub(event)

            hook = self.options.before_send
            if hook is not None:
                try:
                    result = hook(event)
                except Exception:
                    # Dropping is the privacy-safe choice: the hook may exist
                    # precisely to remove data, so sending the unmodified event
                    # could leak what it was meant to strip.
                    self._count("before_send_errors")
                    self._warn(
                        30, "before_send", "before_send raised; event dropped", exc_info=True
                    )
                    return None
                if result is None:
                    self._count("dropped_before_send")
                    return None
                event = result

            payload = self._encode(event)
            if payload is None:
                return None
            if not self._transport.submit(payload, low_priority=low_priority):
                self._count("dropped_queue")
                return None
            self._count("captured")
            event_id = event["event_id"]
            scope.last_event_id = event_id
            return event_id
        finally:
            _CAPTURING.reset(token)

    def _encode(self, event: Event) -> bytes | None:
        limit = self.options.max_event_bytes
        payload = _dumps(event)
        if len(payload) > limit:
            _shrink(event)
            payload = _dumps(event)
        if len(payload) > limit:
            self._count("dropped_oversized")
            self._warn(30, "oversized", "dropping an event larger than %d bytes", limit)
            return None
        return payload

    def _internal_error(self, where: str) -> None:
        self._count("internal_errors")
        self._warn(30, f"internal:{where}", "PulseTrace internal error in %s", where, exc_info=True)


def _json_default(value: object) -> str:
    return safe_str(value, 1024)


def _dumps(event: Event) -> bytes:
    try:
        text = json.dumps(
            event, ensure_ascii=False, separators=(",", ":"), allow_nan=False, default=_json_default
        )
    except ValueError:
        # A before_send hook may have put NaN or other non-JSON values back in.
        text = json.dumps(
            normalize(event, max_depth=16, max_items=10_000, max_string=65_536),
            ensure_ascii=False,
            separators=(",", ":"),
            default=_json_default,
        )
    # Lone surrogates (e.g. undecodable file names) cannot be encoded as UTF-8.
    return text.encode("utf-8", "replace")


def _shrink(event: Event) -> None:
    """Remove the least valuable data from an oversized event."""
    event.pop("breadcrumbs", None)
    transaction = event.get("transaction")
    if transaction is not None and len(transaction["spans"]) > 100:
        dropped = len(transaction["spans"]) - 100
        transaction["spans"] = transaction["spans"][:100]
        transaction["dropped_spans"] = transaction.get("dropped_spans", 0) + dropped
    exception = event.get("exception")
    if exception is not None:
        for value in exception["values"]:
            for frame in value["stacktrace"]["frames"]:
                frame.pop("pre_context", None)
                frame.pop("post_context", None)
    event.pop("extra", None)
