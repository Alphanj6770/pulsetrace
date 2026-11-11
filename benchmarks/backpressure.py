"""Backpressure: memory and producer latency when the collector cannot keep up.

Scenarios
---------
1. **Collector down** - every delivery attempt is refused. Capture far more
   errors than the queue holds; memory must plateau and capture must stay fast.
2. **Slow collector** - each batch takes 50 ms to acknowledge while the app
   offers a steady load above what the collector can absorb.

    uv run python benchmarks/backpressure.py
"""

from __future__ import annotations

import array
import asyncio
import json
import statistics
import sys
import time
import tracemalloc
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmarks.overhead import environment
from pulsetrace import PulseTrace
from pulsetrace.transport.http import AsyncHttpTransport


def payment_failure(order: int) -> None:
    amount = -order
    raise ValueError(f"Invalid payment amount: {amount}")


def make_client(handler: Any, *, queue_size: int = 2048) -> PulseTrace:
    client = PulseTrace(
        service_name="bench",
        endpoint="http://collector.bench/v1/events",
        queue_size=queue_size,
        flush_interval=0.05,
        max_retries=1_000_000,
        initial_backoff=0.05,
        max_backoff=0.2,
    )
    client._transport = AsyncHttpTransport(
        client.options,
        client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    return client


def message(client: PulseTrace, order: int) -> None:
    client.capture_message(f"Payment {order} failed", "error")


def capture(client: PulseTrace, order: int) -> int:
    started = time.perf_counter_ns()
    try:
        payment_failure(order)
    except ValueError as exc:
        client.capture_exception(exc)
    return time.perf_counter_ns() - started


async def collector_down(events: int) -> dict[str, Any]:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    client = make_client(refuse)
    await client.start()
    # Preallocated so the benchmark's own bookkeeping is not counted as SDK memory.
    samples = array.array("q", bytes(8 * events))
    checkpoints: dict[int, float] = {}
    tracemalloc.start()
    baseline = tracemalloc.get_traced_memory()[0]
    for order in range(events):
        samples[order] = capture(client, order)
        if order % 500 == 0:
            await asyncio.sleep(0)  # let the worker run, as a real server would
        if order + 1 in (2_000, 10_000, events):
            checkpoints[order + 1] = (tracemalloc.get_traced_memory()[0] - baseline) / 1e6
    peak = (tracemalloc.get_traced_memory()[1] - baseline) / 1e6
    tracemalloc.stop()
    stats = client.stats()["transport"]
    event_bytes = stats["queue_bytes"] / max(stats["queue_depth"], 1)
    report = await client.shutdown(0.5)
    ordered = sorted(samples)
    return {
        "events_captured": events,
        "capture_p50_us": ordered[len(ordered) // 2] / 1000,
        "capture_p99_us": ordered[int(len(ordered) * 0.99)] / 1000,
        "memory_mb_after": checkpoints,
        "peak_memory_mb": round(peak, 2),
        "queue_capacity": stats["queue_capacity"],
        "queue_high_watermark": stats["queue_high_watermark"],
        "dropped_queue_full": stats["dropped_queue_full"],
        "avg_event_bytes": round(event_bytes),
        "unbounded_queue_estimate_mb": round(events * event_bytes / 1e6, 1),
        "unsent_at_shutdown": report.unsent,
    }


async def slow_collector(seconds: float, offered_per_second: int) -> dict[str, Any]:
    async def slow(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.05)
        return httpx.Response(200)

    client = make_client(slow)
    await client.start()
    tick = 0.01
    per_tick = int(offered_per_second * tick)
    order = 0
    lags: list[float] = []
    deadline = time.perf_counter() + seconds
    while time.perf_counter() < deadline:
        tick_start = time.perf_counter()
        for _ in range(per_tick):
            message(client, order)
            order += 1
        await asyncio.sleep(max(0.0, tick - (time.perf_counter() - tick_start)))
        lags.append(time.perf_counter() - tick_start - tick)
    stats = client.stats()["transport"]  # snapshot at the end of the load window
    await client.flush(10)
    await client.shutdown(5)
    return {
        "offered_events": order,
        "offered_per_second": offered_per_second,
        "delivered": stats["delivered"],
        "dropped_queue_full": stats["dropped_queue_full"],
        "delivered_per_second": round(stats["delivered"] / seconds),
        "queue_high_watermark": stats["queue_high_watermark"],
        "loop_lag_ms_p50": round(statistics.median(lags) * 1000, 2),
    }


async def main() -> None:
    env = environment()
    down = await collector_down(30_000)
    print(json.dumps({"collector_down": down}, indent=2))
    slow = await slow_collector(seconds=3.0, offered_per_second=10_000)
    print(json.dumps({"slow_collector": slow}, indent=2))

    out = Path(__file__).parent / "results"
    out.mkdir(exist_ok=True)
    stamp = env["date"][:10]
    (out / f"backpressure-{stamp}.json").write_text(
        json.dumps({"environment": env, "collector_down": down, "slow_collector": slow}, indent=2)
    )
    memory = ", ".join(f"{k:,} events: {v:.1f} MB" for k, v in down["memory_mb_after"].items())
    lines = [
        f"# Backpressure benchmark ({stamp})",
        "",
        f"- Python {env['python']} on {env['platform']}, CPU: {env['cpu']}",
        "",
        "## Collector down (every delivery refused)",
        "",
        f"- Captured {down['events_captured']:,} real exceptions (avg serialised size {down['avg_event_bytes']:,} B).",
        f"- Queue capacity {down['queue_capacity']:,}; high-water mark {down['queue_high_watermark']:,}; "
        f"dropped when full: {down['dropped_queue_full']:,}.",
        f"- Traced memory growth: {memory}; peak {down['peak_memory_mb']} MB.",
        f"- An unbounded queue would have held ~{down['unbounded_queue_estimate_mb']} MB of events.",
        f"- `capture_exception` latency while the collector was down: p50 {down['capture_p50_us']:.1f} µs, "
        f"p99 {down['capture_p99_us']:.1f} µs.",
        "",
        "## Slow collector (50 ms per batch)",
        "",
        f"- Offered {slow['offered_per_second']:,} events/s for 3 s ({slow['offered_events']:,} events).",
        f"- Delivered during the load window: {slow['delivered']:,} ({slow['delivered_per_second']:,}/s); "
        f"dropped {slow['dropped_queue_full']:,} at the bounded queue.",
        f"- Queue high-water mark {slow['queue_high_watermark']:,}; median event-loop lag "
        f"{slow['loop_lag_ms_p50']} ms per 10 ms tick.",
        "- Theoretical collector capacity: 100 events per 50 ms batch = 2,000 events/s.",
    ]
    (out / f"backpressure-{stamp}.md").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    asyncio.run(main())
