from __future__ import annotations

import httpx
import pytest

from pulsetrace import PulseTrace, suppress_instrumentation
from pulsetrace.integrations.httpx import HttpxIntegration
from pulsetrace.testing import CapturingTransport
from tests.conftest import ClientFactory

ORIGINAL_ASYNC_SEND = httpx.AsyncClient.send
ORIGINAL_SYNC_SEND = httpx.Client.send


@pytest.fixture
def traced(make_client: ClientFactory) -> PulseTrace:
    return make_client(traces_sample_rate=1.0, integrations=[HttpxIntegration()])


def recording_transport(status: int = 200) -> tuple[httpx.MockTransport, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status, json={})

    return httpx.MockTransport(handler), seen


async def test_outbound_request_gets_a_child_span_and_traceparent(
    traced: PulseTrace, transport: CapturingTransport
) -> None:
    mock, seen = recording_transport()
    async with (
        traced.start_span("POST /checkout", op="http.server") as transaction,
        httpx.AsyncClient(transport=mock) as http,
    ):
        await http.post("https://fraud.internal:8443/score?token=abc", json={"amount": 10})

    (event,) = transport.transactions()
    (span,) = event["transaction"]["spans"]
    assert span["op"] == "http.client"
    assert span["name"] == "POST https://fraud.internal:8443/score"  # no query string
    assert span["parent_span_id"] == transaction.span_id
    assert span["data"]["http.status_code"] == 200
    header = seen[0].headers["traceparent"]
    assert header == f"00-{transaction.trace_id}-{span['span_id']}-01"


async def test_failures_are_recorded_and_re_raised(
    traced: PulseTrace, transport: CapturingTransport
) -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    with traced.start_span("txn"):
        async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
            with pytest.raises(httpx.ConnectError):
                await http.get("http://bank.internal/charge")
        traced.capture_message("after the failure")

    (span,) = transport.transactions()[0]["transaction"]["spans"]
    assert span["status"] == "internal_error"
    crumb = transport.messages()[0]["breadcrumbs"][-1]
    assert crumb["category"] == "http"
    assert crumb["level"] == "error"
    assert crumb["data"]["error"] == "ConnectError"


async def test_5xx_responses_are_spans_with_error_status(
    traced: PulseTrace, transport: CapturingTransport
) -> None:
    mock, _ = recording_transport(503)
    with traced.start_span("txn"):
        async with httpx.AsyncClient(transport=mock) as http:
            await http.get("http://fraud/score")
    assert transport.transactions()[0]["transaction"]["spans"][0]["status"] == "unavailable"


async def test_no_active_span_means_no_header(traced: PulseTrace) -> None:
    mock, seen = recording_transport()
    async with httpx.AsyncClient(transport=mock) as http:
        await http.get("http://fraud/score")
    assert "traceparent" not in seen[0].headers


async def test_existing_traceparent_is_respected(traced: PulseTrace) -> None:
    mock, seen = recording_transport()
    custom = "00-11111111111111111111111111111111-2222222222222222-01"
    with traced.start_span("txn"):
        async with httpx.AsyncClient(transport=mock) as http:
            await http.get("http://fraud/score", headers={"traceparent": custom})
    assert seen[0].headers["traceparent"] == custom


async def test_propagation_can_be_limited_to_internal_hosts(make_client: ClientFactory) -> None:
    client = make_client(
        traces_sample_rate=1.0,
        integrations=[HttpxIntegration(propagate_to=lambda url: url.host.endswith(".internal"))],
    )
    mock, seen = recording_transport()
    with client.start_span("txn"):
        async with httpx.AsyncClient(transport=mock) as http:
            await http.get("https://api.stripe.example/v1/charges")
            await http.get("http://fraud.internal/score")
    assert "traceparent" not in seen[0].headers
    assert "traceparent" in seen[1].headers


async def test_suppressed_requests_are_not_instrumented(
    traced: PulseTrace, transport: CapturingTransport
) -> None:
    mock, seen = recording_transport()
    with traced.start_span("txn"), suppress_instrumentation():
        async with httpx.AsyncClient(transport=mock) as http:
            await http.get("http://collector/v1/events")
    assert "traceparent" not in seen[0].headers
    assert transport.transactions()[0]["transaction"]["spans"] == []


def test_sync_client_is_instrumented(traced: PulseTrace, transport: CapturingTransport) -> None:
    mock, seen = recording_transport()
    with traced.start_span("job"), httpx.Client(transport=mock) as http:
        http.get("http://fraud/score")
    assert "traceparent" in seen[0].headers
    assert transport.transactions()[0]["transaction"]["spans"][0]["op"] == "http.client"


async def test_shutdown_restores_the_original_methods(traced: PulseTrace) -> None:
    assert httpx.AsyncClient.send is not ORIGINAL_ASYNC_SEND
    await traced.shutdown(0.1)
    assert httpx.AsyncClient.send is ORIGINAL_ASYNC_SEND
    assert httpx.Client.send is ORIGINAL_SYNC_SEND
