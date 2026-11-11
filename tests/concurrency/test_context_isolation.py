"""Hundreds of interleaved requests must never see each other's telemetry context."""

from __future__ import annotations

import asyncio
import random
from typing import Any

import httpx
from fastapi import FastAPI, Request

from pulsetrace import PulseTrace, PulseTraceMiddleware
from pulsetrace.testing import CapturingTransport

CONCURRENT_REQUESTS = 200


def build_app(client: PulseTrace) -> FastAPI:
    app = FastAPI()
    app.add_middleware(PulseTraceMiddleware, client=client)

    @app.get("/checkout")
    async def checkout(request: Request) -> dict[str, Any]:
        request_id = request.headers["x-request-id"]
        client.set_user({"id": request_id})
        client.set_tag("request_id", request_id)
        client.add_breadcrumb(f"login {request_id}", category="auth")
        await asyncio.sleep(random.uniform(0, 0.01))  # force interleaving with other requests
        async with client.start_span(f"authorize {request_id}"):
            await asyncio.sleep(random.uniform(0, 0.01))
        client.add_breadcrumb(f"checkout {request_id}", category="checkout")
        await asyncio.sleep(random.uniform(0, 0.01))
        raise ValueError(f"failed {request_id}")

    return app


async def test_concurrent_requests_keep_their_own_context() -> None:
    transport = CapturingTransport()
    client = PulseTrace(service_name="svc", transport=transport, traces_sample_rate=1.0)
    app = build_app(client)
    asgi = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=asgi, base_url="http://test") as api:
        responses = await asyncio.gather(
            *(
                api.get("/checkout", headers={"x-request-id": f"req-{n}"})
                for n in range(CONCURRENT_REQUESTS)
            )
        )
    assert all(response.status_code == 500 for response in responses)

    errors = transport.errors()
    assert len(errors) == CONCURRENT_REQUESTS
    for event in errors:
        request_id = event["exception"]["values"][0]["message"].removeprefix("failed ")
        assert event["user"] == {"id": request_id}
        assert event["tags"]["request_id"] == request_id
        assert [crumb["message"] for crumb in event["breadcrumbs"]] == [
            f"login {request_id}",
            f"checkout {request_id}",
        ]

    transactions = transport.transactions()
    assert len(transactions) == CONCURRENT_REQUESTS
    trace_ids = {event["trace"]["trace_id"] for event in transactions}
    assert len(trace_ids) == CONCURRENT_REQUESTS  # one trace per request
    for event in transactions:
        (span,) = event["transaction"]["spans"]
        assert span["name"] == f"authorize {event['user']['id']}"

    errors_by_trace = {event["trace"]["trace_id"]: event for event in errors}
    for event in transactions:
        assert errors_by_trace[event["trace"]["trace_id"]]["user"] == event["user"]
