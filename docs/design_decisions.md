# Design decisions

Each entry records the context, the decision and what it costs. The guiding
constraint for all of them: **monitoring must never become a source of failure
for the software it monitors.**

## 1. One runtime dependency

**Decision.** The SDK depends only on `httpx`. Framework integrations import
their framework lazily, inside their own module.

**Why.** An SDK is installed into other people's dependency trees. Requiring
Pydantic, for example, would conflict with applications pinned to an older
major version. The event schema is therefore `TypedDict`s (checked by mypy,
zero runtime cost) rather than Pydantic models; the collector, which is ours,
uses Pydantic for validation.

## 2. Serialise at capture time, on the caller's thread

**Decision.** Exceptions are walked and events are encoded to JSON bytes inside
`capture_*`, before they enter the queue.

**Why.** Frames only exist while the traceback does; keeping the exception in
the queue would keep every frame (and every local variable in it) alive until
delivery. Pre-serialised bytes also make the memory bound exact.

**Cost.** Latency on the capturing request: about 210 µs for a FastAPI error,
about 57 µs for a sampled transaction on an Apple M4 (`benchmarks/results/`).
In asyncio, doing it "later in the worker" would not be cheaper anyway: the
worker runs on the same event loop thread.

## 3. A thread-safe buffer instead of `asyncio.Queue`

**Decision.** A `deque` behind a `threading.Lock`, plus an `asyncio.Event` that
producers on other threads set through `loop.call_soon_threadsafe`.

**Why.** Events are produced from FastAPI's thread pool (`def` endpoints),
from logging calls on arbitrary threads and from background threads.
`asyncio.Queue` is not thread-safe: `put_nowait` from another thread can corrupt
its waiter bookkeeping and lose wake-ups. The lock is held for a few hundred
nanoseconds and never across I/O, and a producer only pays for a cross-thread
wake-up when the worker is actually waiting.

## 4. Bounded twice, never blocking, `drop_newest` by default

**Decision.** The buffer is bounded by count (`queue_size=2048`) *and* bytes
(`max_queue_bytes=16 MiB`). When full, `submit` drops instead of waiting.
Transactions may only use 75% of the capacity.

**Why.** If the collector accepts 2,000 events/s and the app produces 10,000/s,
an unbounded queue grows until the SDK takes down the application it monitors
(the backpressure benchmark estimates ~41 MB for 30,000 queued errors versus a
~3 MB plateau with the bound). Blocking the request path would turn a monitoring
outage into an application outage. Keeping the *oldest* events by default
preserves the first errors of an incident, usually the cause rather than the
cascade; `overflow_policy="drop_oldest"` is available. Reserving capacity for
errors means a burst of performance data cannot crowd out exceptions.

## 5. Delivery semantics: bounded, best-effort, at-least-once while alive

**Decision.** Events are retried until delivered, rejected, or `max_retries`
is exhausted; the collector deduplicates on `event_id`.

**Why, precisely.** Exactly-once delivery is not achievable here: if the
collector commits a batch and the acknowledgement is lost, the SDK must retry
and the collector receives the batch twice. A UUID per event plus
`ON CONFLICT DO NOTHING` makes the *effect* idempotent, and issue counters are
only incremented for rows the insert reports as new, so counts stay exact.
Events still in memory when the process crashes are lost; a SQLite-backed disk
spool would be the next step if that mattered.

## 6. Retry classification and a transport-wide pause

**Decision.** 400/422 are never retried; 401/403/404/3xx are configuration
errors (never retried, logged at most once a minute); 413 splits the batch;
408/5xx/network errors use `min(d_max, d_0·2^i)·U(0.5, 1.5)`; 429/503 honour
`Retry-After` (both delta-seconds and HTTP-date), capped at `max_retry_after`.
After any failure the pause applies to the *next* batch too.

**Why.** Retrying invalid data only produces more load. Jitter prevents
thousands of SDK instances that saw the same outage from retrying in lock-step.
A transport-wide pause stops a stream of fresh batches each hammering a collector
that just said "slow down". The `Retry-After` cap protects against a
misconfigured server stalling delivery for hours.

## 7. `before_send` failures drop the event

**Decision.** If the user's hook raises, the event is dropped and counted.

**Why.** The hook frequently exists to remove sensitive data. Sending the
unmodified event when it crashes would leak exactly what it was written to
strip. The hook also receives data that has *already* been scrubbed.

## 8. Deterministic, hash-based trace sampling

**Decision.** Keep a trace iff `H(trace_id) < floor(p·2^64)`, where `H` is the
first 64 bits of BLAKE2b. The default (`trace_sampling="consistent"`) ignores
the upstream `sampled` flag; `"parent_based"` follows it.

**Why.** Sampling per span would keep a parent and drop its children. A
function of the trace id gives every span of a trace the same decision, in every
service, with no coordination. Comparing hashes against thresholds is *monotone
in p*: traces kept at 5% are a subset of those kept at 20% (tested), so a
low-rate service's traces are complete across higher-rate services. BLAKE2b is
used instead of `hash()` (salted per process) and instead of reading the id's
low bits (not uniform for ids with timestamp prefixes). Error sampling is
independent of trace sampling: a 1% trace rate never suppresses exceptions.

## 9. Issue fingerprints

**Decision.** `SHA-256(service, exception type, cause types, in-app frames as
module:function, crash-line source)`, truncated to 128 bits. Messages are used
only when no stack exists, after normalising numbers, UUIDs, hex ids, emails
and timestamps.

**Why.** No line numbers: an edit above a function must not split its issue.
In-app frames only: a framework upgrade must not split every issue. No raw
messages: `Invalid amount: -5` and `Invalid amount: -7` are one bug. The crash
line keeps two different `raise` statements in one function apart. 128 bits
make collisions irrelevant (about `n²/2¹²⁹`, so ~10⁻²⁷ for a million issues). The
hashed components are stored with the issue, so grouping is explainable.

## 10. Privacy defaults

Route templates (`/payments/{payment_id}`), never raw paths; an allowlist of
harmless headers; no query strings, client IPs or bodies unless
`send_default_pii=True` (and then still scrubbed); SQL statements without bound
parameters; URLs in spans without query strings or credentials. Card numbers are
recognised by the Luhn checksum, which over-redacts some 13 to 19 digit ids. For
a payments SDK that is the right side to err on, and it can be switched off.

## 11. No implicit patching

Integrations are active only when passed to `PulseTrace(integrations=[...])`
(or, for the middleware, added to the app), and `shutdown()` removes every
patch and listener. The SDK's own HTTP requests run in a context with
instrumentation suppressed, and its own log records are ignored by the logging
integration, so telemetry never generates telemetry about itself.

## 12. Pure ASGI middleware, re-raising the original exception

`BaseHTTPMiddleware` runs the downstream app in a separate task and has a
history of breaking streaming responses and context propagation, so the
middleware is raw ASGI. It catches `Exception` (not `BaseException`:
cancellations and `KeyboardInterrupt` are not bugs), reports it with
`handled=False`, and re-raises the *same object*. A test asserts the framework
still sees a `PaymentError`, not a wrapper. An exception reported manually and
then re-raised through the middleware is reported once: isolation scopes keep
strong references to the exceptions they reported (built-in exceptions cannot
be weakly referenced), which is safe because the scope dies with the request.
