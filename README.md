# PulseTrace: an async-first Python observability SDK

PulseTrace is a lightweight, async-first observability SDK for Python services.
It captures exceptions, instruments HTTP requests, propagates distributed trace
context and delivers telemetry through a bounded asynchronous transport. It is
designed around one principle:

> **Monitoring must never compromise the application being monitored.**

If the collector goes down, the payment service keeps taking payments. If the
queue fills, events are dropped and counted, and memory stays flat. If a
thousand requests run concurrently, each event carries only its own request's
context. If the SDK has a bug, it is caught and counted, never raised into your
code.

```text
src/pulsetrace/   the SDK (one runtime dependency: httpx; strict mypy)
collector/        FastAPI + PostgreSQL/SQLite ingestion, dedup, issue grouping, viewer
examples/         payment-service -> fraud-service, fully instrumented
tests/            282 tests: unit, transport, integrations, concurrency, collector, multi-process E2E
benchmarks/       overhead and backpressure benchmarks with recorded results
docs/             architecture, design decisions, event schema, reliability
```

## What it does

| Area | What you get |
|---|---|
| Exceptions | Chained causes, implicit context, `ExceptionGroup` members, notes, source context, in-app detection; cycle-safe and bounded (a 1000-frame `RecursionError` keeps the first and last 50 frames) |
| Requests | Pure ASGI middleware for FastAPI/Starlette: route templates (`/payments/{payment_id}`), status, duration, allowlisted headers, re-raises the original exception |
| Tracing | Spans and transactions, W3C `traceparent`/`tracestate` in and out, deterministic BLAKE2b sampling per trace |
| Integrations | HTTPX (child spans and propagation), `logging` (breadcrumbs and events), SQLAlchemy sync and async (query spans without parameter values) |
| Context | `ContextVar` isolation scopes; propagation into tasks, `to_thread`, FastAPI's thread pool and SQLAlchemy greenlets is tested; `bind_context` covers executors and threads |
| Privacy | Key-based redaction, Luhn-validated card-number scrubbing, bearer tokens, URL passwords; no raw paths, query strings, IPs or bodies by default; `before_send` |
| Transport | Thread-safe buffer bounded by count and bytes, gzip batches, jittered exponential backoff, `Retry-After`, 413 batch splitting, flush/shutdown deadlines, exact accounting |
| Collector | Per-event validation, idempotent ingestion, race-free issue upserts, zip-bomb limits, API keys, rate limiting, trace stitching across services |

## Quick start

```bash
pip install .             # from a clone of this repository (not yet published to PyPI)
```

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pulsetrace import PulseTrace, PulseTraceMiddleware
from pulsetrace.integrations.httpx import HttpxIntegration
from pulsetrace.integrations.logging import LoggingIntegration

telemetry = PulseTrace(
    endpoint="http://localhost:9000/v1/events",
    service_name="payment-service",
    environment="development",
    release="1.2.0",
    traces_sample_rate=0.1,
    queue_size=2048,
    integrations=[HttpxIntegration(), LoggingIntegration()],
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await telemetry.start()
    yield
    await telemetry.shutdown(timeout=3.0)


app = FastAPI(lifespan=lifespan)
app.add_middleware(PulseTraceMiddleware, client=telemetry)


@app.get("/checkout")
async def checkout():
    raise ValueError("Payment processing failed")  # reported, then re-raised unchanged
```

The manual API works anywhere, with or without a web framework:

```python
telemetry.capture_exception(exc)  # or with no argument inside `except`
telemetry.capture_message("Payment failed", "error")
telemetry.set_user({"id": customer_id})
telemetry.set_tag("currency", "INR")

async with telemetry.start_span("payment.authorize"):
    await authorize_payment()

with telemetry.isolation_scope():  # background jobs get their own context
    process(job)

await telemetry.flush(timeout=5.0)
# ShutdownReport(delivered, dropped, unsent, elapsed)
report = await telemetry.shutdown(timeout=5.0)
telemetry.stats()  # captured / sampled / dropped / retried / queue depth
```

Synchronous programs (scripts, workers) call `telemetry.start_in_background()`
instead of `await telemetry.start()`; delivery then runs on a private event loop
thread and is flushed at interpreter exit.

## Run the demo

**Without Docker** (SQLite storage):

```bash
uv sync
scripts/run_local.sh                               # collector :9000, fraud :8001, payment :8000
uv run python scripts/traffic.py --requests 60     # in another terminal
open http://127.0.0.1:9000/ui/issues
```

**With Docker Compose** (PostgreSQL storage):

```bash
docker compose up --build
scripts/demo.sh            # traffic -> stop the collector -> app keeps serving -> restore -> events arrive
```

The demo shows a checkout travelling payment-service → fraud-service → database
as one trace, errors grouped into issues, a downstream failure reported by both
services with its exception chain, and the collector being stopped and restored
while `/debug/telemetry` shows the SDK queueing, retrying and then delivering.

## Delivery guarantees

Bounded, best-effort, **at-least-once while the process lives**, with
**server-side deduplication**. Each event has a UUID; the collector inserts with
`ON CONFLICT (event_id) DO NOTHING` and only counts newly inserted events toward
issues, so a batch redelivered after a lost acknowledgement changes nothing.
Events still in memory when the process dies are lost (there is no disk spool),
and events beyond the queue bound are dropped and counted, never buffered
without limit. See [docs/reliability.md](docs/reliability.md) for every failure
scenario and the test that proves it.

## Measured overhead

Apple M4, CPython 3.14.7, FastAPI 0.162, 20,000 requests per configuration,
driven in-process through ASGI so network noise does not hide the SDK's cost.
Full report: [benchmarks/results/overhead-2026-11-11.md](benchmarks/results/overhead-2026-11-11.md).

| Configuration | p50 | p99 | Added p50 |
|---|---:|---:|---:|
| FastAPI, no PulseTrace | 22.5 µs | 28.6 µs | |
| Instrumented, 0% traces | 39.3 µs | 79.4 µs | +16.9 µs |
| Instrumented, 10% traces | 42.8 µs | 84.4 µs | +20.3 µs |
| Instrumented, 100% traces | 79.6 µs | 90.3 µs | +57.1 µs |
| Full pipeline to a real collector, 100% traces | 79.4 µs | 90.8 µs | +57.0 µs |
| Request raising an exception (captured) | 229 µs | 423 µs | +206 µs |

Backpressure ([report](benchmarks/results/backpressure-2026-11-11.md)): with the
collector refusing every connection, capturing 30,000 real exceptions kept
traced memory at about 3.2 MB (an unbounded queue would have held about 41 MB)
and `capture_exception` stayed at about 141 µs p50. Against a collector that
takes 50 ms per batch (2,000 events/s capacity), it delivered 1,833 events/s
and dropped the excess at the queue.

Profiling these numbers found two real problems, both fixed and described in
[docs/reliability.md](docs/reliability.md#bugs-the-suite-and-benchmarks-found):
the redaction step dominated CPU, and a per-request reference cycle tripled the
p99 tail. Reproduce with `make bench`. For HTTP-level load testing,
[benchmarks/locustfile.py](benchmarks/locustfile.py) drives the example service.

## Design highlights

* **Deterministic sampling.** A trace is kept iff
  `BLAKE2b64(trace_id) < floor(p · 2^64)`. Every span of a trace, in every
  service, reaches the same decision without coordination, and decisions are
  nested across rates (traces kept at 5% ⊂ traces kept at 20%). Error sampling
  is independent, so a 1% trace rate never hides exceptions.
* **Grouping that survives real code.** Fingerprints hash the exception type,
  cause types, in-app `module:function` frames and the crash line. They ignore
  line numbers (edits elsewhere), framework frames (upgrades) and dynamic
  messages (`amount: -5` vs `-7`).
* **A thread-safe queue, not `asyncio.Queue`.** Events come from FastAPI's
  thread pool and logging calls on arbitrary threads; `asyncio.Queue` is not
  thread-safe.

The reasoning behind these and nine other decisions is in
[docs/design_decisions.md](docs/design_decisions.md);
[docs/architecture.md](docs/architecture.md) covers the data flow, concurrency
model and lifecycle, and [docs/event_schema.md](docs/event_schema.md) the wire
format.

## Development

```bash
uv sync                    # Python 3.11+
make check                 # ruff, ruff format --check, mypy --strict, pytest
make test-fast             # everything except the multi-process E2E tests
PULSETRACE_TEST_DATABASE_URL=postgresql+asyncpg://user:pass@localhost/pulsetrace_test uv run pytest
```

CI (GitHub Actions) runs lint and strict type checking, the full suite on
Python 3.11 to 3.14 against a PostgreSQL service, and installs the built wheel
into a clean environment.

## Prior art

PulseTrace is an independent implementation, informed by studying how
[sentry-python](https://github.com/getsentry/sentry-python) structures its
client, scopes, transport and integrations, and by the
[W3C Trace Context](https://www.w3.org/TR/trace-context/) specification.

## License

MIT
