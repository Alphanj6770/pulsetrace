"""The whole pipeline, exercised the way an operator would.

Tests in this module share one running stack and run in file order; the outage
test stops and restarts the collector, so it is last.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

import httpx
import pytest

from tests.e2e.conftest import Stack, wait_for

pytestmark = pytest.mark.e2e


def traceparent(trace_id: str) -> str:
    return f"00-{trace_id}-{uuid.uuid4().hex[:16]}-01"


def issue_titled(stack: Stack, prefix: str) -> dict[str, Any] | None:
    for issue in stack.collector_get("/issues"):
        if issue["title"].startswith(prefix):
            return issue  # type: ignore[no-any-return]
    return None


def test_unhandled_errors_are_stored_and_grouped(stack: Stack) -> None:
    for amount in (-5, -7, -9):
        response = httpx.post(
            f"{stack.payment.url}/checkout",
            json={"customer_id": "c-1", "amount": amount},
            timeout=5,
        )
        assert response.status_code == 500

    issue = wait_for(
        lambda: (
            (found := issue_titled(stack, "ValueError: Invalid payment amount"))
            and found["occurrence_count"] == 3
            and found
        )
    )
    assert issue["service"] == "payment-service"
    assert issue["culprit"] == "examples.payment_service.main in checkout"

    detail = stack.collector_get(f"/issues/{issue['fingerprint']}")
    payload = detail["latest_event"]["payload"]
    assert payload["request"]["route"] == "/checkout"
    assert payload["request"]["status_code"] == 500
    assert payload["user"] == {"id": "c-1"}
    assert payload["tags"]["currency"] == "INR"
    assert any(c.get("message") == "Checkout initiated for c-1" for c in payload["breadcrumbs"])


def test_a_request_through_two_services_is_one_trace(stack: Stack) -> None:
    trace_id = uuid.uuid4().hex
    header = traceparent(trace_id)
    response = httpx.post(
        f"{stack.payment.url}/checkout",
        json={"customer_id": "c-2", "amount": 250},
        headers={"traceparent": header},
        timeout=5,
    )
    assert response.status_code == 200

    trace = wait_for(
        lambda: (
            (t := stack.collector_get(f"/traces/{trace_id}"))
            and len(t.get("services", [])) == 2
            and t
        )
    )
    spans = {(span["service"], span["op"], span["name"]): span for span in trace["spans"]}
    payment_root = spans[("payment-service", "http.server", "POST /checkout")]
    http_client = next(s for s in trace["spans"] if s["op"] == "http.client")
    fraud_root = spans[("fraud-service", "http.server", "POST /score")]

    assert payment_root["depth"] == 0
    assert payment_root["parent_span_id"] == header.split("-")[2]  # continued our traceparent
    assert http_client["parent_span_id"] != payment_root["span_id"]  # nested under fraud_check
    assert fraud_root["parent_span_id"] == http_client["span_id"]  # stitched across processes
    assert fraud_root["depth"] == http_client["depth"] + 1
    assert any(
        s["op"] == "db.query" and s["name"].startswith("INSERT INTO payments")
        for s in trace["spans"]
    )
    assert ("fraud-service", "ml.inference", "fraud.model.predict") in spans


def test_downstream_failure_shows_in_both_services(stack: Stack) -> None:
    trace_id = uuid.uuid4().hex
    response = httpx.post(
        f"{stack.payment.url}/checkout",
        json={"customer_id": "c-3", "amount": 60_000},
        headers={"traceparent": traceparent(trace_id)},
        timeout=5,
    )
    assert response.status_code == 500

    trace = wait_for(
        lambda: (
            (t := stack.collector_get(f"/traces/{trace_id}"))
            and len(t.get("errors", [])) == 2
            and t
        )
    )
    titles = sorted(error["title"] for error in trace["errors"])
    assert titles[0].startswith("FraudModelError")
    assert titles[1].startswith("PaymentProcessingError")

    payment_error = next(e for e in trace["errors"] if e["service"] == "payment-service")
    event = stack.collector_get(f"/events/{payment_error['event_id']}")
    chain = [(v["type"], v["relation"]) for v in event["payload"]["exception"]["values"]]
    assert chain == [("PaymentProcessingError", None), ("HTTPStatusError", "cause")]


def test_collector_outage_does_not_affect_the_app_and_events_arrive_later(stack: Stack) -> None:
    issue = (
        wait_for(lambda: issue_titled(stack, "RefundError"), timeout=1)
        if issue_titled(stack, "RefundError")
        else None
    )
    before = issue["occurrence_count"] if issue else 0

    stack.collector.stop()
    try:
        latencies = []
        for index in range(5):
            started = time.monotonic()
            response = httpx.post(f"{stack.payment.url}/refunds/pay-{index}", timeout=5)
            latencies.append(time.monotonic() - started)
            assert response.status_code == 500  # the app's own behaviour, unchanged
        assert httpx.get(f"{stack.payment.url}/health", timeout=5).status_code == 200
        assert max(latencies) < 1.0  # telemetry delivery never blocks requests

        stats = wait_for(
            lambda: (
                (s := httpx.get(f"{stack.payment.url}/debug/telemetry", timeout=5).json())
                and s["transport"]["retries"] > 0
                and s
            )
        )
        assert "ConnectError" in (stats["transport"]["last_error"] or "")
    finally:
        stack.collector.start()

    recovered = wait_for(
        lambda: (
            (found := issue_titled(stack, "RefundError"))
            and found["occurrence_count"] >= before + 5
            and found
        ),
        timeout=30,
    )
    assert recovered["occurrence_count"] == before + 5  # retried, delivered once each
    stats = httpx.get(f"{stack.payment.url}/debug/telemetry", timeout=5).json()["transport"]
    assert stats["dropped_retries_exhausted"] == 0
