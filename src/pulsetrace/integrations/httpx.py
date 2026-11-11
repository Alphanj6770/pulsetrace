"""Outbound HTTP instrumentation for HTTPX (``AsyncClient`` and ``Client``).

For each request made while a span is active it creates an ``http.client``
child span, injects the W3C ``traceparent`` (and forwards ``tracestate``) so
the downstream service continues the same trace, and records an ``http``
breadcrumb with method, URL, status and latency.

URLs are recorded as ``scheme://host[:port]/path``: no credentials, query string
or fragment, since those are where tokens tend to live.

The SDK's own transport also uses HTTPX. Its worker task runs with
instrumentation suppressed (see ``pulsetrace.context``), so delivering telemetry
never produces more telemetry.
"""

from __future__ import annotations

import asyncio
import functools
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import httpx

from pulsetrace.context import get_current_span, is_instrumentation_suppressed
from pulsetrace.integrations import Integration

if TYPE_CHECKING:
    from pulsetrace.client import PulseTrace
    from pulsetrace.tracing import Span

_ORIGINAL = "__pulsetrace_original__"
_active: HttpxIntegration | None = None


class HttpxIntegration(Integration):
    identifier = "httpx"

    def __init__(
        self,
        *,
        propagate_to: Callable[[httpx.URL], bool] | None = None,
        record_breadcrumbs: bool = True,
    ) -> None:
        """``propagate_to`` limits which hosts receive ``traceparent`` (default: all)."""
        self._propagate_to = propagate_to
        self._record_breadcrumbs = record_breadcrumbs
        self.client: PulseTrace | None = None

    def setup(self, client: PulseTrace) -> None:
        global _active
        self.client = client
        _active = self
        _patch()

    def teardown(self) -> None:
        global _active
        if _active is self:
            _active = None
            _unpatch()

    def _should_propagate(self, url: httpx.URL) -> bool:
        if self._propagate_to is None:
            return True
        try:
            return bool(self._propagate_to(url))
        except Exception:
            return False


def _patch() -> None:
    async_send = httpx.AsyncClient.send
    if not hasattr(async_send, _ORIGINAL):

        @functools.wraps(async_send)
        async def send_async(
            self: httpx.AsyncClient, request: httpx.Request, *args: Any, **kwargs: Any
        ) -> httpx.Response:
            call = _before(request)
            if call is None:
                return await async_send(self, request, *args, **kwargs)
            try:
                response = await async_send(self, request, *args, **kwargs)
            except BaseException as exc:
                call.done(error=exc)
                raise
            call.done(response=response)
            return response

        setattr(send_async, _ORIGINAL, async_send)
        setattr(httpx.AsyncClient, "send", send_async)  # noqa: B010

    sync_send = httpx.Client.send
    if not hasattr(sync_send, _ORIGINAL):

        @functools.wraps(sync_send)
        def send_sync(
            self: httpx.Client, request: httpx.Request, *args: Any, **kwargs: Any
        ) -> httpx.Response:
            call = _before(request)
            if call is None:
                return sync_send(self, request, *args, **kwargs)
            try:
                response = sync_send(self, request, *args, **kwargs)
            except BaseException as exc:
                call.done(error=exc)
                raise
            call.done(response=response)
            return response

        setattr(send_sync, _ORIGINAL, sync_send)
        setattr(httpx.Client, "send", send_sync)  # noqa: B010


def _unpatch() -> None:
    for cls in (httpx.AsyncClient, httpx.Client):
        original = getattr(cls.send, _ORIGINAL, None)
        if original is not None:
            setattr(cls, "send", original)  # noqa: B010


def sanitize_url(url: httpx.URL) -> str:
    host = url.host
    if ":" in host:  # IPv6 literal
        host = f"[{host}]"
    port = f":{url.port}" if url.port is not None else ""
    return f"{url.scheme}://{host}{port}{url.path}"


class _Call:
    __slots__ = ("integration", "method", "span", "start_ns", "url")

    def __init__(
        self, integration: HttpxIntegration, span: Span | None, method: str, url: str
    ) -> None:
        self.integration = integration
        self.span = span
        self.method = method
        self.url = url
        self.start_ns = time.perf_counter_ns()

    def done(
        self, *, response: httpx.Response | None = None, error: BaseException | None = None
    ) -> None:
        try:
            duration_ms = round((time.perf_counter_ns() - self.start_ns) / 1e6, 3)
            data: dict[str, Any] = {
                "method": self.method,
                "url": self.url,
                "duration_ms": duration_ms,
            }
            level = "info"
            span = self.span
            if response is not None:
                data["status_code"] = response.status_code
                if response.status_code >= 500:
                    level = "error"
                elif response.status_code >= 400:
                    level = "warning"
                if span is not None:
                    span.set_http_status(response.status_code)
            elif error is not None:
                data["error"] = type(error).__name__
                level = "error"
                if span is not None:
                    cancelled = isinstance(error, asyncio.CancelledError)
                    span.set_status("cancelled" if cancelled else "internal_error")
                    span.set_data("error.type", type(error).__name__)
            if span is not None:
                span.finish()
            client = self.integration.client
            if client is not None and self.integration._record_breadcrumbs:
                client.add_breadcrumb(category="http", type="http", level=level, data=data)
        except Exception:
            pass  # instrumentation must never break the user's HTTP call


def _before(request: httpx.Request) -> _Call | None:
    integration = _active
    if integration is None or is_instrumentation_suppressed():
        return None
    client = integration.client
    if client is None or not client.enabled:
        return None
    try:
        url = sanitize_url(request.url)
        parent = get_current_span()
        span = None
        if parent is not None:
            span = parent.start_child(f"{request.method} {url}", op="http.client")
            span.set_data("http.request.method", request.method)
            span.set_data("url.full", url)
            if integration._should_propagate(request.url) and "traceparent" not in request.headers:
                request.headers["traceparent"] = span.traceparent()
                if span.tracestate and "tracestate" not in request.headers:
                    request.headers["tracestate"] = span.tracestate
        return _Call(integration, span, request.method, url)
    except Exception:
        return None
