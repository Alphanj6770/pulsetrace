"""Pure ASGI middleware for FastAPI, Starlette and any ASGI 3 application.

For every HTTP request it:

1. forks an isolation scope (so concurrent requests never share context);
2. starts a transaction, continuing an incoming W3C ``traceparent``;
3. runs the application;
4. reports an unhandled exception (``mechanism="asgi"``, ``handled=False``) and
   **re-raises the original exception object** - the framework's own error
   handling sees exactly what it would have seen without PulseTrace;
5. records route template, status and duration, finishes the transaction and
   resets every ``ContextVar`` in ``finally`` - including on cancellation.

It is written as raw ASGI rather than Starlette's ``BaseHTTPMiddleware``, which
runs the downstream app in a separate task and has a history of breaking
streaming responses and context propagation.

Privacy defaults: the *route template* (``/payments/{payment_id}``) is always
recorded, never the raw path; only an allowlist of harmless headers is kept; the
query string, raw path, client IP and all headers are recorded only with
``send_default_pii=True`` (and are still scrubbed). Request bodies are never read.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable, MutableMapping
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs

from pulsetrace.events import RequestPayload

if TYPE_CHECKING:
    from pulsetrace.client import PulseTrace
    from pulsetrace.scope import Scope as PulseScope
    from pulsetrace.tracing import Span

ASGIScope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[ASGIScope, Receive, Send], Awaitable[None]]

UNMATCHED_ROUTE = "<unmatched route>"
SAFE_HEADERS = frozenset(
    {
        "accept",
        "accept-encoding",
        "accept-language",
        "content-length",
        "content-type",
        "host",
        "user-agent",
        "x-request-id",
    }
)


class PulseTraceMiddleware:
    def __init__(self, app: ASGIApp, client: PulseTrace) -> None:
        self.app = app
        self.client = client

    async def __call__(self, scope: ASGIScope, receive: Receive, send: Send) -> None:
        scope_type = scope["type"]
        if scope_type == "http" and self.client.enabled:
            await self._http(scope, receive, send)
        elif scope_type == "websocket" and self.client.enabled:
            await self._websocket(scope, receive, send)
        else:  # lifespan, or the SDK is disabled
            await self.app(scope, receive, send)

    async def _http(self, scope: ASGIScope, receive: Receive, send: Send) -> None:
        client = self.client
        start_ns = time.perf_counter_ns()
        method = str(scope.get("method", "GET")).upper()
        send_pii = client.options.send_default_pii
        traceparent, tracestate = _trace_headers(scope)
        status_code: int | None = None

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = int(message["status"])
            await send(message)

        with client.isolation_scope() as request_scope:
            request_scope.set_request_factory(lambda: request_info(scope, method, send_pii))
            transaction = client.start_transaction(
                f"{method} {UNMATCHED_ROUTE}",
                op="http.server",
                traceparent=traceparent,
                tracestate=tracestate,
            )
            transaction.activate()
            error: BaseException | None = None
            try:
                await self.app(scope, receive, send_wrapper)
            except BaseException as exc:
                error = exc
                raise
            finally:
                self._finish(
                    request_scope, transaction, scope, method, status_code, start_ns, error
                )

    def _finish(
        self,
        request_scope: PulseScope,
        transaction: Span,
        scope: ASGIScope,
        method: str,
        status_code: int | None,
        start_ns: int,
        error: BaseException | None,
    ) -> None:
        try:
            if isinstance(error, Exception) and status_code is None:
                status_code = 500  # nothing was sent; the server will answer 500
            route = route_template(scope)
            transaction.name = f"{method} {route or UNMATCHED_ROUTE}"
            transaction.set_data("http.request.method", method)
            fields: dict[str, Any] = {
                "duration_ms": round((time.perf_counter_ns() - start_ns) / 1e6, 3)
            }
            if status_code is not None:
                fields["status_code"] = status_code
                transaction.set_http_status(status_code)
            request_scope.update_request(**fields)
            if isinstance(error, Exception):
                # Not BaseException: CancelledError/KeyboardInterrupt are not bugs.
                self.client.capture_exception(error, mechanism="asgi", handled=False)
        except Exception:
            pass  # never mask the application's own exception
        finally:
            transaction._exit(error)

    async def _websocket(self, scope: ASGIScope, receive: Receive, send: Send) -> None:
        client = self.client
        send_pii = client.options.send_default_pii
        with client.isolation_scope() as request_scope:
            request_scope.set_request_factory(lambda: request_info(scope, "WEBSOCKET", send_pii))
            try:
                await self.app(scope, receive, send)
            except Exception as exc:
                client.capture_exception(exc, mechanism="asgi.websocket", handled=False)
                raise


def _trace_headers(scope: ASGIScope) -> tuple[str | None, str | None]:
    traceparent = tracestate = None
    for name, value in scope.get("headers") or ():
        if name == b"traceparent":
            traceparent = value.decode("latin-1")
        elif name == b"tracestate":
            tracestate = value.decode("latin-1")
    return traceparent, tracestate


def route_template(scope: ASGIScope) -> str | None:
    """The matched route pattern, e.g. ``/payments/{payment_id}``.

    Starlette's router and FastAPI's ``APIRoute`` both store the matched route
    in ``scope["route"]`` while routing. ``path_format`` strips converters
    (``{id:int}`` -> ``{id}``).
    """
    route = scope.get("route")
    if route is None:
        return None
    for attribute in ("path_format", "path"):
        value = getattr(route, attribute, None)
        if isinstance(value, str) and value:
            return value
    return None


def request_info(scope: ASGIScope, method: str, send_pii: bool) -> RequestPayload:
    """Built lazily, only when an event is actually captured."""
    headers: dict[str, str] = {}
    for raw_name, raw_value in scope.get("headers") or ():
        name = raw_name.decode("latin-1").lower()
        if not send_pii and name not in SAFE_HEADERS:
            continue
        value = raw_value.decode("latin-1")
        headers[name] = f"{headers[name]}, {value}" if name in headers else value
    info: RequestPayload = {"method": method, "route": route_template(scope)}
    if headers:
        info["headers"] = headers
    if send_pii:
        info["path"] = str(scope.get("path", ""))
        raw_query = scope.get("query_string") or b""
        if raw_query:
            parsed = parse_qs(raw_query.decode("latin-1"), keep_blank_values=True)
            info["query"] = {
                key: value[0] if len(value) == 1 else value for key, value in parsed.items()
            }
        peer = scope.get("client")
        if peer:
            info["client_ip"] = str(peer[0])
    return info
