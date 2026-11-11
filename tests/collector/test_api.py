from __future__ import annotations

import uuid

import httpx

from tests.helpers import error_event, transaction_event

PAYMENT_ROOT = "1" * 16
PAYMENT_HTTP_CLIENT = "2" * 16
FRAUD_ROOT = "3" * 16
FRAUD_DB = "4" * 16


async def post(collector: httpx.AsyncClient, *events: dict[str, object]) -> None:
    response = await collector.post("/v1/events", json={"events": list(events)})
    assert response.status_code == 200, response.text


async def test_health(collector: httpx.AsyncClient) -> None:
    assert (await collector.get("/health")).json() == {"status": "ok", "database": True}


async def test_issue_detail_includes_latest_event_and_history(collector: httpx.AsyncClient) -> None:
    await post(collector, *(error_event(message=f"Invalid payment amount: {n}") for n in range(3)))
    (issue,) = (await collector.get("/issues")).json()
    detail = (await collector.get(f"/issues/{issue['fingerprint']}")).json()
    assert detail["occurrence_count"] == 3
    assert len(detail["recent_events"]) == 3
    assert detail["latest_event"]["payload"]["type"] == "error"
    assert (await collector.get("/issues/does-not-exist")).status_code == 404


async def test_event_filters(collector: httpx.AsyncClient) -> None:
    trace_id = uuid.uuid4().hex
    await post(
        collector,
        error_event(service="payment-service"),
        error_event(service="fraud-service", trace_id=trace_id),
        transaction_event(trace_id=trace_id, span_id=FRAUD_ROOT, service="fraud-service"),
    )
    assert len((await collector.get("/events", params={"service": "fraud-service"})).json()) == 2
    assert len((await collector.get("/events", params={"type": "error"})).json()) == 2
    assert len((await collector.get("/events", params={"trace_id": trace_id})).json()) == 2
    assert (await collector.get(f"/events/{uuid.uuid4()}")).status_code == 404


async def test_trace_is_stitched_across_services(collector: httpx.AsyncClient) -> None:
    trace_id = uuid.uuid4().hex
    payment = transaction_event(
        trace_id=trace_id,
        span_id=PAYMENT_ROOT,
        name="POST /checkout",
        service="payment-service",
        start="2026-11-11T10:30:00.000000Z",
        duration_ms=120,
        spans=[
            {
                "span_id": PAYMENT_HTTP_CLIENT,
                "parent_span_id": PAYMENT_ROOT,
                "op": "http.client",
                "name": "POST http://fraud/score",
                "start_timestamp": "2026-11-11T10:30:00.010000Z",
                "duration_ms": 80,
                "status": "ok",
            },
        ],
    )
    fraud = transaction_event(
        trace_id=trace_id,
        span_id=FRAUD_ROOT,
        parent_span_id=PAYMENT_HTTP_CLIENT,  # fraud's root is a child of payment's HTTP call
        name="POST /score",
        service="fraud-service",
        start="2026-11-11T10:30:00.015000Z",
        duration_ms=70,
        spans=[
            {
                "span_id": FRAUD_DB,
                "parent_span_id": FRAUD_ROOT,
                "op": "db.query",
                "name": "SELECT score",
                "start_timestamp": "2026-11-11T10:30:00.020000Z",
                "duration_ms": 30,
                "status": "ok",
            },
        ],
    )
    failure = error_event(service="fraud-service", trace_id=trace_id, span_id=FRAUD_ROOT)
    await post(collector, fraud, payment, failure)  # arrival order must not matter

    trace = (await collector.get(f"/traces/{trace_id}")).json()
    assert trace["services"] == ["payment-service", "fraud-service"]  # call order
    assert [(span["depth"], span["name"]) for span in trace["spans"]] == [
        (0, "POST /checkout"),
        (1, "POST http://fraud/score"),
        (2, "POST /score"),
        (3, "SELECT score"),
    ]
    assert [span["offset_ms"] for span in trace["spans"]] == [0.0, 10.0, 15.0, 20.0]
    assert trace["duration_ms"] == 120.0
    assert trace["spans"][2]["has_error"] is True
    assert trace["errors"][0]["service"] == "fraud-service"
    assert (await collector.get(f"/traces/{uuid.uuid4().hex}")).status_code == 404


async def test_stats_report_ingestion_outcomes(collector: httpx.AsyncClient) -> None:
    event = error_event()
    await post(collector, event)
    await post(collector, event)
    stats = (await collector.get("/stats")).json()
    assert stats["ingestion"]["accepted"] == 1
    assert stats["ingestion"]["duplicates"] == 1
    assert stats["stored"] == {"events": 1, "issues": 1}


async def test_viewer_pages_render(collector: httpx.AsyncClient) -> None:
    trace_id = uuid.uuid4().hex
    event = error_event(trace_id=trace_id, span_id=PAYMENT_ROOT)
    event["breadcrumbs"] = [
        {
            "timestamp": "2026-11-11T10:29:59.000000Z",
            "type": "log",
            "category": "payment",
            "level": "info",
            "message": "Checkout initiated",
        }
    ]
    await post(collector, event, transaction_event(trace_id=trace_id, span_id=PAYMENT_ROOT))
    (issue,) = (await collector.get("/issues")).json()
    for path in (
        "/ui/issues",
        f"/ui/issues/{issue['fingerprint']}",
        "/ui/events",
        f"/ui/events/{uuid.UUID(event['event_id'])}",
        f"/ui/traces/{trace_id}",
    ):
        response = await collector.get(path)
        assert response.status_code == 200, path
    assert "Checkout initiated" in (await collector.get(f"/ui/issues/{issue['fingerprint']}")).text
    assert (await collector.get("/")).status_code in (302, 307)


async def test_viewer_escapes_attacker_controlled_text(collector: httpx.AsyncClient) -> None:
    payload = "<script>alert('xss')</script>"
    await post(collector, error_event(message=payload))
    (issue,) = (await collector.get("/issues")).json()
    for path in ("/ui/issues", f"/ui/issues/{issue['fingerprint']}"):
        html = (await collector.get(path)).text
        assert payload not in html
        assert "&lt;script&gt;" in html
