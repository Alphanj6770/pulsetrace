from __future__ import annotations

import asyncio
import gzip
import json
import uuid
from typing import Any

import httpx

from tests.collector.conftest import CollectorFactory
from tests.helpers import error_event, transaction_event


def batch(*events: dict[str, Any]) -> dict[str, Any]:
    return {"schema_version": 1, "sent_at": "2026-11-11T10:30:01Z", "events": list(events)}


async def test_valid_batch_is_stored_and_retrievable(collector: httpx.AsyncClient) -> None:
    event = error_event()
    response = await collector.post("/v1/events", json=batch(event))
    assert response.status_code == 200
    assert response.json() == {"accepted": 1, "duplicates": 0, "rejected": 0, "errors": []}

    stored = (await collector.get(f"/events/{event['event_id']}")).json()
    assert stored["type"] == "error"
    assert stored["title"] == "ValueError: Invalid payment amount: -5"
    assert stored["status_code"] == 500
    assert stored["payload"]["exception"]["values"][0]["message"] == "Invalid payment amount: -5"


async def test_redelivery_is_idempotent(collector: httpx.AsyncClient) -> None:
    events = [error_event(message=f"Invalid payment amount: {n}") for n in range(3)]
    first = (await collector.post("/v1/events", json=batch(*events))).json()
    retry = (await collector.post("/v1/events", json=batch(*events))).json()
    assert first["accepted"] == 3
    assert retry == {"accepted": 0, "duplicates": 3, "rejected": 0, "errors": []}
    (issue,) = (await collector.get("/issues")).json()
    assert issue["occurrence_count"] == 3  # retries never inflate counts


async def test_duplicates_inside_one_batch_are_ignored(collector: httpx.AsyncClient) -> None:
    event = error_event()
    result = (await collector.post("/v1/events", json=batch(event, event))).json()
    assert result["accepted"] == 1
    assert result["duplicates"] == 1


async def test_concurrent_redelivery_counts_each_event_once(collector: httpx.AsyncClient) -> None:
    events = [error_event(message=f"Invalid payment amount: {n}") for n in range(20)]
    results = await asyncio.gather(
        *(collector.post("/v1/events", json=batch(*events)) for _ in range(5))
    )
    assert sum(r.json()["accepted"] for r in results) == 20
    (issue,) = (await collector.get("/issues")).json()
    assert issue["occurrence_count"] == 20


async def test_one_bad_event_does_not_sink_the_batch(collector: httpx.AsyncClient) -> None:
    good = error_event()
    naive_timestamp = error_event(timestamp="2026-11-11T10:30:00")
    no_exception = {**error_event(), "exception": None}
    result = (
        await collector.post("/v1/events", json=batch(good, naive_timestamp, no_exception, "junk"))
    ).json()
    assert result["accepted"] == 1
    assert result["rejected"] == 3
    assert [error["index"] for error in result["errors"]] == [1, 2, 3]
    assert "timestamp" in result["errors"][0]["error"]


async def test_unknown_fields_are_preserved(collector: httpx.AsyncClient) -> None:
    event = {**error_event(), "future_field": {"added_in": "sdk 9.0"}}
    await collector.post("/v1/events", json=batch(event))
    stored = (await collector.get(f"/events/{event['event_id']}")).json()
    assert stored["payload"]["future_field"] == {"added_in": "sdk 9.0"}


async def test_invalid_envelope_is_rejected(collector: httpx.AsyncClient) -> None:
    assert (await collector.post("/v1/events", content=b"not json")).status_code == 400
    assert (
        await collector.post("/v1/events", json={"schema_version": 99, "events": []})
    ).status_code == 400


async def test_gzip_bodies(collector: httpx.AsyncClient) -> None:
    body = gzip.compress(json.dumps(batch(error_event())).encode())
    ok = await collector.post("/v1/events", content=body, headers={"Content-Encoding": "gzip"})
    assert ok.json()["accepted"] == 1
    corrupt = await collector.post(
        "/v1/events", content=b"\x1f\x8b garbage", headers={"Content-Encoding": "gzip"}
    )
    assert corrupt.status_code == 400
    brotli = await collector.post("/v1/events", content=b"x", headers={"Content-Encoding": "br"})
    assert brotli.status_code == 415


async def test_gzip_bombs_are_refused(make_collector: CollectorFactory) -> None:
    _, collector = await make_collector(max_decompressed_bytes=1_000_000)
    bomb = gzip.compress(b"\x00" * 50_000_000)  # ~50 KB on the wire, 50 MB inflated
    assert len(bomb) < 100_000
    response = await collector.post(
        "/v1/events", content=bomb, headers={"Content-Encoding": "gzip"}
    )
    assert response.status_code == 413


async def test_large_bodies_and_batches_are_refused(make_collector: CollectorFactory) -> None:
    _, collector = await make_collector(max_body_bytes=10_000, max_events_per_batch=5)
    big = await collector.post("/v1/events", content=b"x" * 20_000)
    assert big.status_code == 413
    many = await collector.post("/v1/events", json=batch(*[error_event() for _ in range(6)]))
    assert many.status_code == 413


async def test_api_keys_are_enforced(make_collector: CollectorFactory) -> None:
    _, collector = await make_collector(api_keys=frozenset({"key-1"}))
    body = batch(error_event())
    assert (await collector.post("/v1/events", json=body)).status_code == 401
    wrong = await collector.post("/v1/events", json=body, headers={"Authorization": "Bearer nope"})
    assert wrong.status_code == 401
    right = await collector.post("/v1/events", json=body, headers={"Authorization": "Bearer key-1"})
    assert right.status_code == 200


async def test_rate_limit_answers_429_with_retry_after(make_collector: CollectorFactory) -> None:
    _, collector = await make_collector(rate_limit_per_second=1, rate_limit_burst=2)
    first = await collector.post("/v1/events", json=batch(error_event(), error_event()))
    second = await collector.post("/v1/events", json=batch(error_event()))
    assert first.status_code == 200
    assert second.status_code == 429
    assert int(second.headers["Retry-After"]) >= 1


async def test_transactions_are_stored_but_not_grouped(collector: httpx.AsyncClient) -> None:
    trace_id = uuid.uuid4().hex
    event = transaction_event(trace_id=trace_id, span_id="b" * 16)
    assert (await collector.post("/v1/events", json=batch(event))).json()["accepted"] == 1
    assert (await collector.get("/issues")).json() == []
    stored = (await collector.get("/events", params={"type": "transaction"})).json()
    assert stored[0]["title"] == "POST /checkout"
    assert stored[0]["duration_ms"] == 50.0


async def test_issue_tracks_first_and_last_seen(collector: httpx.AsyncClient) -> None:
    late = error_event(timestamp="2026-11-11T12:00:00Z")
    early = error_event(timestamp="2026-11-11T09:00:00Z")
    await collector.post("/v1/events", json=batch(late))
    await collector.post("/v1/events", json=batch(early))  # arrives out of order
    (issue,) = (await collector.get("/issues")).json()
    assert issue["first_seen"].startswith("2026-11-11T09:00:00")
    assert issue["last_seen"].startswith("2026-11-11T12:00:00")
    assert issue["last_event_id"] == str(uuid.UUID(late["event_id"]))
