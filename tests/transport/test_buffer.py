from __future__ import annotations

import asyncio
import threading
import time

import pytest

from pulsetrace.transport.buffer import EventBuffer


def test_capacity_is_bounded_and_overflow_is_counted() -> None:
    buffer = EventBuffer(max_items=10, max_bytes=10_000)
    results = [buffer.put(b"x") for _ in range(25)]
    assert results.count(True) == 10
    assert len(buffer) == 10
    assert buffer.dropped_full == 15


def test_drop_oldest_evicts_from_the_head() -> None:
    buffer = EventBuffer(max_items=3, max_bytes=10_000, overflow="drop_oldest")
    for index in range(5):
        assert buffer.put(str(index).encode())
    assert buffer.drain() == [b"2", b"3", b"4"]
    assert buffer.evicted == 2


def test_byte_budget_is_enforced() -> None:
    buffer = EventBuffer(max_items=1000, max_bytes=100)
    assert buffer.put(b"a" * 60)
    assert not buffer.put(b"b" * 60)
    assert not buffer.put(b"c" * 101)
    assert buffer.size_bytes == 60


def test_low_priority_events_cannot_fill_the_buffer() -> None:
    buffer = EventBuffer(max_items=100, max_bytes=100_000)
    accepted = sum(buffer.put(b"txn", low_priority=True) for _ in range(100))
    assert accepted == 75
    assert sum(buffer.put(b"error") for _ in range(100)) == 25  # errors still have room


def test_closed_buffer_rejects() -> None:
    buffer = EventBuffer(max_items=10, max_bytes=1000)
    buffer.close()
    assert not buffer.put(b"x")
    assert buffer.dropped_closed == 1


def test_concurrent_producers_never_exceed_capacity() -> None:
    buffer = EventBuffer(max_items=500, max_bytes=10**9)
    per_thread, threads = 5_000, 8

    def produce() -> None:
        for _ in range(per_thread):
            buffer.put(b"event")

    workers = [threading.Thread(target=produce) for _ in range(threads)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    assert len(buffer) == 500
    assert buffer.accepted + buffer.dropped_full == per_thread * threads


async def test_next_batch_respects_size_and_lingers_for_more() -> None:
    buffer = EventBuffer(max_items=100, max_bytes=10**6)
    buffer.bind(asyncio.get_running_loop())
    for index in range(5):
        buffer.put(str(index).encode())

    async def late_producer() -> None:
        await asyncio.sleep(0.02)
        buffer.put(b"late")

    producer = asyncio.create_task(late_producer())
    batch = await buffer.next_batch(max_items=10, max_bytes=10**6, linger=0.2, urgent=lambda: False)
    await producer
    assert batch == [b"0", b"1", b"2", b"3", b"4", b"late"]

    for _ in range(25):
        buffer.put(b"x")
    batch = await buffer.next_batch(max_items=10, max_bytes=10**6, linger=5, urgent=lambda: False)
    assert len(batch) == 10  # full batch returns without lingering


async def test_urgent_cuts_the_linger_short() -> None:
    buffer = EventBuffer(max_items=100, max_bytes=10**6)
    buffer.bind(asyncio.get_running_loop())
    buffer.put(b"only")
    started = time.monotonic()
    batch = await buffer.next_batch(max_items=10, max_bytes=10**6, linger=5, urgent=lambda: True)
    assert batch == [b"only"]
    assert time.monotonic() - started < 0.5


async def test_producers_on_other_threads_wake_the_consumer() -> None:
    buffer = EventBuffer(max_items=100, max_bytes=10**6)
    buffer.bind(asyncio.get_running_loop())
    thread = threading.Timer(0.05, lambda: buffer.put(b"from-thread"))
    thread.start()
    batch = await asyncio.wait_for(
        buffer.next_batch(max_items=10, max_bytes=10**6, linger=0.0001, urgent=lambda: False), 2
    )
    thread.join()
    assert batch == [b"from-thread"]


async def test_closed_and_empty_returns_empty_batch() -> None:
    buffer = EventBuffer(max_items=10, max_bytes=1000)
    buffer.bind(asyncio.get_running_loop())
    buffer.close()
    assert (
        await buffer.next_batch(max_items=10, max_bytes=1000, linger=1, urgent=lambda: True) == []
    )


@pytest.mark.parametrize("policy", ["drop_newest", "drop_oldest"])
def test_memory_stays_bounded_under_flood(policy: str) -> None:
    buffer = EventBuffer(max_items=2048, max_bytes=1_000_000, overflow=policy)  # type: ignore[arg-type]
    for _ in range(100_000):
        buffer.put(b"e" * 300)
    assert len(buffer) <= 2048
    assert buffer.size_bytes <= 1_000_000
