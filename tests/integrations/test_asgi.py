"""FastAPI through the real ASGI stack (httpx.ASGITransport), no mocks of the framework."""

from __future__ import annotations

import asyncio
import contextvars
from typing import Any

import httpx
import pytest
from fastapi import FastAPI, HTTPException

from pulsetrace import PulseTrace, PulseTraceMiddleware
from pulsetrace.context import get_current_span, get_isolation_scope
from pulsetrace.testing import CapturingTransport
from tests.conftest import ClientFactory

TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


class PaymentError(Exception):
    pass


def build_app(client: PulseTrace) -> FastAPI:
    app = FastAPI()
    app.add_middleware(PulseTraceMiddleware, client=client)

    @app.get("/payments/{payment_id}")
    async def get_payment(payment_id: int) -> dict[str, Any]:
        if payment_id == 404:
            raise HTTPException(status_code=404, detail="no such payment")
        if payment_id == 500:
            raise PaymentError(f"ledger unavailable for {payment_id}")
        return {"payment_id": payment_id}

    @app.get("/sync")
    def sync_endpoint() -> dict[str, Any]:
        client.add_breadcrumb("inside the threadpool", category="sync")
        raise RuntimeError("sync failure")

    @app.get("/manual")
    async def manual() -> dict[str, Any]:
        try:
            raise PaymentError("captured then re-raised")
        except PaymentError as exc:
            client.capture_exception(exc)
            raise

    @app.get("/work")
    async def work() -> dict[str, Any]:
        async with client.start_span("payment.authorize"):
            await asyncio.sleep(0)
        return {"ok": True}

    return app


def http(app: FastAPI, *, raise_app_exceptions: bool = False) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=raise_app_exceptions)
    return httpx.AsyncClient(transport=transport, base_url="http://testserver")


async def test_unhandled_exception_is_captured_and_the_original_propagates(
    client: PulseTrace, transport: CapturingTransport
) -> None:
    app = build_app(client)
    # The framework must see the *same* exception type it would see without PulseTrace.
    async with http(app, raise_app_exceptions=True) as api:
        with pytest.raises(PaymentError, match="ledger unavailable"):
            await api.get("/payments/500")

    (event,) = transport.errors()
    assert event["exception"]["values"][0]["type"] == "PaymentError"
    assert event["exception"]["mechanism"] == {"type": "asgi", "handled": False}
    request = event["request"]
    assert request["method"] == "GET"
    assert request["route"] == "/payments/{payment_id}"  # the template, not /payments/500
    assert request["status_code"] == 500
    assert request["duration_ms"] > 0
    assert "path" not in request  # raw paths only with send_default_pii


async def test_client_receives_a_normal_500(client: PulseTrace) -> None:
    async with http(build_app(client)) as api:
        response = await api.get("/payments/500")
    assert response.status_code == 500


async def test_handled_http_errors_are_not_reported_as_errors(
    client: PulseTrace, transport: CapturingTransport
) -> None:
    async with http(build_app(client)) as api:
        response = await api.get("/payments/404")
    assert response.status_code == 404
    assert transport.errors() == []
    (transaction,) = transport.transactions()
    assert transaction["transaction"]["status"] == "not_found"


async def test_transactions_record_route_spans_and_status(
    client: PulseTrace, transport: CapturingTransport
) -> None:
    async with http(build_app(client)) as api:
        await api.get("/work")
    (event,) = transport.transactions()
    assert event["transaction"]["name"] == "GET /work"
    assert event["transaction"]["op"] == "http.server"
    assert event["transaction"]["status"] == "ok"
    assert [span["name"] for span in event["transaction"]["spans"]] == ["payment.authorize"]
    assert event["request"]["status_code"] == 200


async def test_incoming_traceparent_is_continued(
    client: PulseTrace, transport: CapturingTransport
) -> None:
    async with http(build_app(client)) as api:
        await api.get("/work", headers={"traceparent": TRACEPARENT})
    trace = transport.transactions()[0]["trace"]
    assert trace["trace_id"] == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert trace["parent_span_id"] == "00f067aa0ba902b7"


async def test_unmatched_routes_have_bounded_names(
    client: PulseTrace, transport: CapturingTransport
) -> None:
    async with http(build_app(client)) as api:
        await api.get("/wp-admin/../../etc/passwd")
    assert transport.transactions()[0]["transaction"]["name"] == "GET <unmatched route>"


async def test_secrets_in_headers_are_not_captured_by_default(
    client: PulseTrace, transport: CapturingTransport
) -> None:
    async with http(build_app(client)) as api:
        await api.get(
            "/payments/500", headers={"Authorization": "Bearer s3cr3t-token", "Cookie": "sid=1"}
        )
    headers = transport.errors()[0]["request"]["headers"]
    assert "authorization" not in headers
    assert "cookie" not in headers
    assert headers["user-agent"].startswith("python-httpx")


async def test_with_pii_enabled_headers_are_captured_but_redacted(
    make_client: ClientFactory, transport: CapturingTransport
) -> None:
    client = make_client(send_default_pii=True)
    async with http(build_app(client)) as api:
        await api.get(
            "/payments/500?token=abc&page=2",
            headers={"Authorization": "Bearer s3cr3t-token", "X-API-Key": "k"},
        )
    request = transport.errors()[0]["request"]
    assert request["headers"]["authorization"] == "[REDACTED]"
    assert request["headers"]["x-api-key"] == "[REDACTED]"
    assert request["query"] == {"token": "[REDACTED]", "page": "2"}
    assert request["path"] == "/payments/500"
    assert request["client_ip"]


async def test_sync_endpoints_run_in_the_request_scope(
    client: PulseTrace, transport: CapturingTransport
) -> None:
    async with http(build_app(client)) as api:
        await api.get("/sync")
    (event,) = transport.errors()
    assert event["breadcrumbs"][0]["message"] == "inside the threadpool"
    assert event["request"]["route"] == "/sync"


async def test_manual_capture_plus_middleware_reports_once(
    client: PulseTrace, transport: CapturingTransport
) -> None:
    async with http(build_app(client)) as api:
        await api.get("/manual")
    assert len(transport.errors()) == 1
    assert transport.errors()[0]["exception"]["mechanism"]["type"] == "manual"


async def test_cancelled_request_resets_context_and_is_not_an_error(
    client: PulseTrace, transport: CapturingTransport
) -> None:
    started = asyncio.Event()

    async def slow_app(scope: Any, receive: Any, send: Any) -> None:
        started.set()
        await asyncio.sleep(10)

    middleware = PulseTraceMiddleware(slow_app, client=client)
    scope = {"type": "http", "method": "GET", "path": "/slow", "headers": []}
    context = contextvars.copy_context()

    async def no_receive() -> Any:
        return {}

    async def no_send(message: Any) -> None:
        return None

    task = asyncio.create_task(middleware(scope, no_receive, no_send), context=context)
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert context.run(get_isolation_scope) is None
    assert context.run(get_current_span) is None
    assert transport.errors() == []
    assert transport.transactions()[0]["transaction"]["status"] == "cancelled"


async def test_lifespan_and_disabled_client_pass_through(transport: CapturingTransport) -> None:
    disabled = PulseTrace(service_name="svc", transport=transport, enabled=False)
    async with http(build_app(disabled)) as api:
        response = await api.get("/payments/500")
    assert response.status_code == 500
    assert transport.events == []


async def test_unsampled_requests_still_report_errors_with_trace_ids(
    make_client: ClientFactory, transport: CapturingTransport
) -> None:
    client = make_client(traces_sample_rate=0.0)
    async with http(build_app(client)) as api:
        await api.get("/payments/500")
        await api.get("/work")
    assert transport.transactions() == []
    (event,) = transport.errors()
    assert len(event["trace"]["trace_id"]) == 32
