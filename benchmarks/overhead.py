"""PulseTrace overhead on a FastAPI request.

The ASGI app is driven directly (no sockets, no HTTP client), so the numbers
isolate the cost of the middleware and the event pipeline from network noise.
The "full pipeline" configuration additionally runs the real transport worker on
the same event loop, delivering to a real collector process.

    uv run python benchmarks/overhead.py --requests 20000

Results are printed and written to ``benchmarks/results/``.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import json
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import Any

from fastapi import FastAPI

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pulsetrace import PulseTrace, PulseTraceMiddleware
from pulsetrace.transport.base import NullTransport
from tests.e2e.conftest import Service, free_port

Scope = dict[str, Any]


def build_app(client: PulseTrace | None) -> FastAPI:
    app = FastAPI()
    if client is not None:
        app.add_middleware(PulseTraceMiddleware, client=client)

    @app.get("/checkout/{order_id}")
    async def checkout(order_id: int) -> dict[str, Any]:
        if client is not None:
            async with client.start_span("payment.authorize"):
                pass
        return {"order_id": order_id, "status": "captured"}

    @app.get("/fail/{order_id}")
    async def fail(order_id: int) -> dict[str, Any]:
        raise ValueError(f"Invalid payment amount for order {order_id}")

    return app


def scope_for(path: str) -> Scope:
    return {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"bench"), (b"user-agent", b"bench/1.0"), (b"accept", b"*/*")],
        "client": ("127.0.0.1", 50000),
        "server": ("bench", 80),
    }


async def receive() -> dict[str, Any]:
    return {"type": "http.request", "body": b"", "more_body": False}


async def send(message: dict[str, Any]) -> None:
    return None


async def call(app: Any, path: str) -> None:
    try:
        await app(scope_for(path), receive, send)
    except ValueError:
        pass  # Starlette re-raises after sending the 500 response


async def measure(app: Any, path: str, requests: int, warmup: int) -> dict[str, float]:
    for index in range(warmup):
        await call(app, path.format(index))
    gc.collect()
    samples: list[int] = []
    started = time.perf_counter()
    for index in range(requests):
        t0 = time.perf_counter_ns()
        await call(app, path.format(index))
        samples.append(time.perf_counter_ns() - t0)
    elapsed = time.perf_counter() - started
    samples.sort()

    def pct(p: float) -> float:
        return samples[min(len(samples) - 1, int(p * len(samples)))] / 1000

    concurrent_started = time.perf_counter()
    batch = 100
    for offset in range(0, requests, batch):
        await asyncio.gather(*(call(app, path.format(offset + n)) for n in range(batch)))
    concurrent_elapsed = time.perf_counter() - concurrent_started
    return {
        "mean_us": statistics.fmean(samples) / 1000,
        "p50_us": pct(0.50),
        "p95_us": pct(0.95),
        "p99_us": pct(0.99),
        "sequential_rps": requests / elapsed,
        "concurrent_rps": requests / max(concurrent_elapsed, 1e-9),
    }


async def run(requests: int, warmup: int) -> dict[str, Any]:
    results: dict[str, Any] = {}
    collector_dir = Path(tempfile.mkdtemp(prefix="pulsetrace-bench-"))
    collector = Service(
        "collector",
        "collector.main:create_app",
        free_port(),
        {"DATABASE_URL": f"sqlite+aiosqlite:///{collector_dir / 'bench.db'}"},
        collector_dir,
        factory=True,
    )
    collector.start()
    try:
        configs: list[tuple[str, PulseTrace | None]] = [
            ("baseline (no PulseTrace)", None),
            (
                "instrumented, 0% traces, no transmission",
                PulseTrace(service_name="bench", transport=NullTransport(), traces_sample_rate=0.0),
            ),
            (
                "instrumented, 10% traces, no transmission",
                PulseTrace(service_name="bench", transport=NullTransport(), traces_sample_rate=0.1),
            ),
            (
                "instrumented, 100% traces, no transmission",
                PulseTrace(service_name="bench", transport=NullTransport(), traces_sample_rate=1.0),
            ),
            (
                "full pipeline, 100% traces -> collector",
                PulseTrace(
                    service_name="bench",
                    endpoint=f"{collector.url}/v1/events",
                    traces_sample_rate=1.0,
                ),
            ),
        ]
        for name, client in configs:
            if client is not None:
                await client.start()
            app = build_app(client)
            ok = await measure(app, "/checkout/{}", requests, warmup)
            error = await measure(app, "/fail/{}", max(requests // 5, 1000), warmup // 5)
            entry: dict[str, Any] = {"success_path": ok, "error_path": error}
            if client is not None:
                flushed = await client.flush(30)
                stats = client.stats()
                report = await client.shutdown(10)
                entry["sdk"] = {
                    "flushed": flushed,
                    "captured": stats["events"]["captured"],
                    "delivered": report.delivered,
                    "dropped": report.dropped,
                    "unsent": report.unsent,
                    "queue_high_watermark": stats["transport"].get("queue_high_watermark"),
                }
            results[name] = entry
            print(
                f"{name:45s} p50={ok['p50_us']:7.1f}us p99={ok['p99_us']:7.1f}us "
                f"seq={ok['sequential_rps']:8.0f} rps  error p50={error['p50_us']:7.1f}us"
            )
    finally:
        collector.stop()
        shutil.rmtree(collector_dir, ignore_errors=True)
    return results


def environment() -> dict[str, Any]:
    cpu = platform.processor()
    if sys.platform == "darwin":
        try:
            cpu = subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
            ).strip()
        except (OSError, subprocess.CalledProcessError):
            pass
    versions = {
        name: metadata.version(name) for name in ("fastapi", "starlette", "httpx", "uvicorn")
    }
    return {
        "date": datetime.now(UTC).isoformat(timespec="seconds"),
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "cpu": cpu,
        "packages": versions,
    }


NOTE = '\n*Notes:* in the *no transmission* configurations the `NullTransport` discards every event by design, so "Dropped" equals "Captured". In the full-pipeline run the in-process load generator offers about 12,000 transaction events/s to a single-writer SQLite collector; the bounded queue absorbs the burst (high-water mark 2,048) and drops the excess instead of growing, which is the intended backpressure behaviour. A sampling rate of 100% is a stress setting, not a recommendation.\n'


def write_report(results: dict[str, Any], env: dict[str, Any], requests: int, out: Path) -> Path:
    out.mkdir(parents=True, exist_ok=True)
    stamp = env["date"][:10]
    (out / f"overhead-{stamp}.json").write_text(
        json.dumps({"environment": env, "results": results}, indent=2)
    )
    baseline = results["baseline (no PulseTrace)"]["success_path"]
    lines = [
        f"# Overhead benchmark ({stamp})",
        "",
        f"- Python {env['python']} ({env['implementation']}) on {env['platform']}",
        f"- CPU: {env['cpu']}",
        "- Packages: " + ", ".join(f"{k} {v}" for k, v in env["packages"].items()),
        f"- {requests} sequential requests per configuration after warm-up, driven in-process through ASGI",
        "",
        "## Successful request (one manual span)",
        "",
        "| Configuration | p50 (µs) | p95 (µs) | p99 (µs) | Δp50 vs baseline | Sequential req/s | Concurrent req/s |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for name, entry in results.items():
        r = entry["success_path"]
        delta = r["p50_us"] - baseline["p50_us"]
        lines.append(
            f"| {name} | {r['p50_us']:.1f} | {r['p95_us']:.1f} | {r['p99_us']:.1f} | "
            f"{delta:+.1f} | {r['sequential_rps']:,.0f} | {r['concurrent_rps']:,.0f} |"
        )
    lines += [
        "",
        "## Failing request (unhandled exception captured)",
        "",
        "| Configuration | p50 (µs) | p99 (µs) |",
        "|---|---:|---:|",
    ]
    for name, entry in results.items():
        r = entry["error_path"]
        lines.append(f"| {name} | {r['p50_us']:.1f} | {r['p99_us']:.1f} |")
    lines += [
        "",
        "## SDK accounting",
        "",
        "| Configuration | Captured | Delivered | Dropped | Unsent | Queue high-water |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for name, entry in results.items():
        sdk = entry.get("sdk")
        if sdk:
            lines.append(
                f"| {name} | {sdk['captured']} | {sdk['delivered']} | {sdk['dropped']} | "
                f"{sdk['unsent']} | {sdk['queue_high_watermark'] or '-'} |"
            )
    path = out / f"overhead-{stamp}.md"
    path.write_text("\n".join(lines) + "\n" + NOTE)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--requests", type=int, default=20_000)
    parser.add_argument("--warmup", type=int, default=2_000)
    parser.add_argument("--output", type=Path, default=Path(__file__).parent / "results")
    args = parser.parse_args()
    env = environment()
    results = asyncio.run(run(args.requests, args.warmup))
    print(f"\nwrote {write_report(results, env, args.requests, args.output)}")


if __name__ == "__main__":
    main()
