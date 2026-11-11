"""The HTTP transport against scripted collector behaviour (httpx.MockTransport).

Time is faked: backoff and Retry-After waits are recorded instead of slept, so
retry schedules can be asserted exactly and the suite stays fast.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import random
import threading
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
import pytest

from pulsetrace.config import Options
from pulsetrace.transport.http import AsyncHttpTransport

Handler = Callable[[httpx.Request], Awaitable[httpx.Response]]


class FakeTime:
    """Records requested waits. ``real_delay`` makes each wait also take a little real time."""

    def __init__(self, real_delay: float = 0.0) -> None:
        self.now = 1_000.0
        self.sleeps: list[float] = []
        self.real_delay = real_delay

    def clock(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(round(seconds, 6))
        self.now += seconds
        await asyncio.sleep(self.real_delay)


class Collector:
    """Scripted collector: records every batch it receives."""

    def __init__(self, responder: Callable[[int, list[dict[str, Any]]], Any] | None = None) -> None:
        self.batches: list[list[dict[str, Any]]] = []
        self.requests: list[httpx.Request] = []
        self.calls = 0
        self._responder = responder or (lambda call, events: 200)

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        self.requests.append(request)
        envelope = json.loads(gzip.decompress(request.content))
        events = envelope["events"]
        result = self._responder(self.calls, events)
        if isinstance(result, BaseException):
            raise result
        if asyncio.iscoroutine(result):
            result = await result
        response = result if isinstance(result, httpx.Response) else httpx.Response(result)
        if 200 <= response.status_code < 300:
            self.batches.append(events)
        return response

    @property
    def delivered(self) -> list[dict[str, Any]]:
        return [event for batch in self.batches for event in batch]


def make_transport(
    collector: Collector, *, real_delay: float = 0.0, **overrides: Any
) -> tuple[AsyncHttpTransport, FakeTime]:
    settings: dict[str, Any] = {
        "endpoint": "http://collector.test/v1/events",
        "service_name": "svc",
        "flush_interval": 0.01,
    }
    settings.update(overrides)
    fake = FakeTime(real_delay)
    transport = AsyncHttpTransport(
        Options(**settings),
        client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(collector)),
        sleep=fake.sleep,
        clock=fake.clock,
        rng=random.Random(7),
    )
    return transport, fake


def event(index: int = 0) -> bytes:
    return json.dumps({"event_id": uuid.uuid4().hex, "n": index}).encode()


async def test_events_are_delivered_in_bounded_batches() -> None:
    collector = Collector()
    transport, _ = make_transport(collector, max_batch_size=100)
    for index in range(250):
        transport.submit(event(index))
    await transport.start()
    assert await transport.flush(5)
    assert [len(batch) for batch in collector.batches] == [100, 100, 50]
    assert sorted(item["n"] for item in collector.delivered) == list(range(250))
    report = await transport.shutdown(1)
    assert report.delivered == 250
    assert report.unsent == 0


async def test_wire_format_is_a_gzipped_versioned_envelope() -> None:
    collector = Collector()
    transport, _ = make_transport(collector, api_key="secret-key")
    await transport.start()
    transport.submit(event())
    await transport.flush(5)
    request = collector.requests[0]
    assert request.headers["content-encoding"] == "gzip"
    assert request.headers["authorization"] == "Bearer secret-key"
    assert request.headers["user-agent"].startswith("pulsetrace-python/")
    envelope = json.loads(gzip.decompress(request.content))
    assert envelope["schema_version"] == 1
    assert envelope["sdk"]["name"] == "pulsetrace.python"
    await transport.shutdown(1)


@pytest.mark.parametrize(
    ("status", "counter"), [(400, "dropped_rejected"), (401, "dropped_config_error")]
)
async def test_permanent_errors_are_not_retried(status: int, counter: str) -> None:
    collector = Collector(lambda call, events: status)
    transport, fake = make_transport(collector)
    await transport.start()
    for index in range(3):
        transport.submit(event(index))
    assert await transport.flush(5)
    assert collector.calls == 1
    assert transport.stats()[counter] == 3
    assert fake.sleeps == []
    await transport.shutdown(1)


async def test_server_errors_are_retried_a_bounded_number_of_times() -> None:
    collector = Collector(lambda call, events: 500)
    transport, fake = make_transport(collector, max_retries=3, initial_backoff=1, max_backoff=100)
    await transport.start()
    transport.submit(event())
    assert await transport.flush(5)
    assert collector.calls == 4  # one attempt + three retries
    stats = transport.stats()
    assert stats["retries"] == 3
    assert stats["dropped_retries_exhausted"] == 1
    # Exponential backoff with jitter: delay i lies in [0.5, 1.5] * 2**i.
    assert len(fake.sleeps) == 3
    for attempt, delay in enumerate(fake.sleeps):
        assert 0.5 * 2**attempt <= delay <= 1.5 * 2**attempt
    await transport.shutdown(1)


async def test_429_waits_exactly_retry_after_then_delivers() -> None:
    def responder(call: int, events: list[dict[str, Any]]) -> Any:
        if call == 1:
            return httpx.Response(429, headers={"Retry-After": "7"})
        return 200

    collector = Collector(responder)
    transport, fake = make_transport(collector)
    await transport.start()
    transport.submit(event())
    assert await transport.flush(5)
    assert fake.sleeps == [7.0]
    assert transport.stats()["rate_limited"] == 1
    assert len(collector.delivered) == 1
    await transport.shutdown(1)


async def test_rate_limit_pause_applies_to_the_next_batch_too() -> None:
    def responder(call: int, events: list[dict[str, Any]]) -> Any:
        return httpx.Response(429, headers={"Retry-After": "30"}) if call == 1 else 200

    collector = Collector(responder)
    transport, fake = make_transport(collector, max_retries=0)
    await transport.start()
    transport.submit(event(1))
    await transport.flush(5)  # first batch dropped (no retries allowed), pause recorded
    transport.submit(event(2))
    await transport.flush(5)
    assert fake.sleeps == [30.0]  # the second batch waited for the window to end
    await transport.shutdown(1)


async def test_absurd_retry_after_is_capped() -> None:
    def responder(call: int, events: list[dict[str, Any]]) -> Any:
        return httpx.Response(429, headers={"Retry-After": "86400"}) if call == 1 else 200

    collector = Collector(responder)
    transport, fake = make_transport(collector, max_retry_after=60)
    await transport.start()
    transport.submit(event())
    await transport.flush(5)
    assert fake.sleeps == [60.0]
    await transport.shutdown(1)


async def test_timeouts_are_retried() -> None:
    def responder(call: int, events: list[dict[str, Any]]) -> Any:
        return httpx.ReadTimeout("slow") if call <= 2 else 200

    collector = Collector(responder)
    transport, _ = make_transport(collector)
    await transport.start()
    transport.submit(event())
    assert await transport.flush(5)
    assert collector.calls == 3
    assert len(collector.delivered) == 1
    assert "timeout" in transport.stats()["last_error"]
    await transport.shutdown(1)


async def test_413_splits_the_batch() -> None:
    collector = Collector(lambda call, events: 413 if len(events) > 2 else 200)
    transport, _ = make_transport(collector)
    for index in range(10):
        transport.submit(event(index))
    await transport.start()
    assert await transport.flush(5)
    assert sorted(item["n"] for item in collector.delivered) == list(range(10))
    assert all(len(batch) <= 2 for batch in collector.batches)
    await transport.shutdown(1)


async def test_collector_outage_keeps_submit_fast_and_memory_bounded() -> None:
    collector = Collector(lambda call, events: httpx.ConnectError("connection refused"))
    transport, _ = make_transport(collector, real_delay=0.01, queue_size=50, max_retries=1000)
    await transport.start()

    started = time.perf_counter()
    accepted = sum(transport.submit(event(index)) for index in range(10_000))
    elapsed = time.perf_counter() - started
    await asyncio.sleep(0.05)

    stats = transport.stats()
    assert accepted <= 50 + 100  # queue capacity + at most one batch taken by the worker
    assert stats["queue_depth"] <= 50
    assert stats["dropped_queue_full"] >= 10_000 - 150
    assert elapsed < 1.0  # ~10k non-blocking submits; generous bound for slow CI machines
    report = await transport.shutdown(0.2)
    assert report.unsent > 0


async def test_recovers_when_the_collector_comes_back() -> None:
    def responder(call: int, events: list[dict[str, Any]]) -> Any:
        return 200 if call > 5 else httpx.ConnectError("down")  # five failed attempts, then up

    collector = Collector(responder)
    transport, fake = make_transport(collector, max_retries=10)
    await transport.start()
    for index in range(20):
        transport.submit(event(index))
    assert await transport.flush(5)
    assert len(fake.sleeps) == 5  # backed off before each retry
    assert sorted(item["n"] for item in collector.delivered) == list(range(20))
    await transport.shutdown(1)


async def test_flush_times_out_and_shutdown_cancels_a_hung_request() -> None:
    never = asyncio.Event()

    async def hang() -> int:
        await never.wait()
        return 200

    collector = Collector(lambda call, events: hang())
    transport, _ = make_transport(collector)
    await transport.start()
    for index in range(5):
        transport.submit(event(index))
    assert await transport.flush(0.1) is False

    started = time.monotonic()
    report = await transport.shutdown(0.2)
    assert time.monotonic() - started < 2
    assert report.unsent == 5
    assert not report.clean
    remaining = [t for t in asyncio.all_tasks() if t.get_name() == "pulsetrace-transport-worker"]
    assert all(t.done() for t in remaining)


async def test_shutdown_sends_the_final_partial_batch_without_lingering() -> None:
    collector = Collector()
    transport, _ = make_transport(collector, flush_interval=60)
    await transport.start()
    for index in range(3):
        transport.submit(event(index))
    started = time.monotonic()
    report = await transport.shutdown(5)
    assert time.monotonic() - started < 1
    assert report.delivered == 3
    assert report.clean


async def test_submit_after_shutdown_is_rejected() -> None:
    transport, _ = make_transport(Collector())
    await transport.start()
    await transport.shutdown(1)
    assert transport.submit(event()) is False
    assert transport.stats()["dropped_after_shutdown"] == 1


async def test_shutdown_without_start_reports_unsent() -> None:
    transport, _ = make_transport(Collector())
    transport.submit(event())
    report = await transport.shutdown(1)
    assert report.unsent == 1


async def test_submissions_from_many_threads_are_all_delivered() -> None:
    collector = Collector()
    transport, _ = make_transport(collector, queue_size=10_000)
    await transport.start()

    def produce(offset: int) -> None:
        for index in range(250):
            transport.submit(event(offset + index))

    threads = [threading.Thread(target=produce, args=(n * 1000,)) for n in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert await transport.flush(10)
    assert len(collector.delivered) == 2000
    await transport.shutdown(1)


async def test_flush_does_not_starve_the_event_loop() -> None:
    """Regression: an urgent-but-empty worker once spun without awaiting."""
    collector = Collector()
    transport, _ = make_transport(collector, flush_interval=30)
    await transport.start()
    transport.submit(event())
    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0.001)

    ticking = asyncio.create_task(ticker())
    assert await asyncio.wait_for(transport.flush(2), 3)
    await asyncio.sleep(0.05)
    ticking.cancel()
    assert ticks > 5
    await transport.shutdown(1)


async def test_unexpected_errors_drop_the_batch_but_keep_the_worker_alive() -> None:
    def responder(call: int, events: list[dict[str, Any]]) -> Any:
        return RuntimeError("bug in a proxy") if call == 1 else 200

    collector = Collector(responder)
    transport, _ = make_transport(collector)
    await transport.start()
    transport.submit(event(1))
    await transport.flush(5)
    transport.submit(event(2))
    await transport.flush(5)
    assert transport.stats()["dropped_rejected"] == 1
    assert [item["n"] for item in collector.delivered] == [2]
    await transport.shutdown(1)
