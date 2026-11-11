# Reliability

## Failure scenarios and the tests that prove them

| Scenario | Expected result | Test |
|---|---|---|
| Application raises `ValueError` | captured **and** the same exception re-raised to the framework | `tests/integrations/test_asgi.py::test_unhandled_exception_is_captured_and_the_original_propagates` |
| Collector unavailable | app keeps serving (< 1 s per request), events delivered after recovery, counted once | `tests/e2e/test_pipeline.py::test_collector_outage_does_not_affect_the_app_and_events_arrive_later` |
| 10,000 events with the collector refusing connections | `submit` stays non-blocking, queue ≤ capacity, excess counted | `tests/transport/test_http_transport.py::test_collector_outage_keeps_submit_fast_and_memory_bounded` |
| Queue at capacity | memory bounded by count and bytes, both overflow policies | `tests/transport/test_buffer.py::test_memory_stays_bounded_under_flood` |
| HTTP 429 with `Retry-After: 7` | waits exactly 7 s, then delivers; next batch also waits | `test_http_transport.py::test_429_waits_exactly_retry_after_then_delivers`, `::test_rate_limit_pause_applies_to_the_next_batch_too` |
| HTTP 500 | `max_retries` retries with jittered exponential backoff, then dropped and counted | `test_http_transport.py::test_server_errors_are_retried_a_bounded_number_of_times` |
| HTTP 400 / 401 | no retry; rejected or configuration error | `test_http_transport.py::test_permanent_errors_are_not_retried` |
| HTTP 413 | batch split until accepted | `test_http_transport.py::test_413_splits_the_batch` |
| Network timeout | retried, then delivered | `test_http_transport.py::test_timeouts_are_retried` |
| 200 concurrent interleaved requests | every event carries only its own user, tags, breadcrumbs and trace | `tests/concurrency/test_context_isolation.py` |
| Context across tasks, threads, executors | measured, not assumed (table in [architecture](architecture.md#3-context-propagation)) | `tests/concurrency/test_context_propagation.py` |
| Request cancelled mid-flight | `ContextVar`s restored, transaction `cancelled`, no error event | `test_asgi.py::test_cancelled_request_resets_context_and_is_not_an_error` |
| Shutdown with pending events | final partial batch sent immediately (no linger) | `test_http_transport.py::test_shutdown_sends_the_final_partial_batch_without_lingering` |
| Shutdown while the collector hangs | worker cancelled at the deadline, unsent count reported, no task left behind | `test_http_transport.py::test_flush_times_out_and_shutdown_cancels_a_hung_request` |
| Same batch delivered twice / 5 times concurrently | stored once, issue counts exact (SQLite and PostgreSQL) | `tests/collector/test_ingestion.py::test_redelivery_is_idempotent`, `::test_concurrent_redelivery_counts_each_event_once` |
| `Authorization` header present | not captured by default; `[REDACTED]` with `send_default_pii` | `test_asgi.py::test_secrets_in_headers_are_not_captured_by_default`, `::test_with_pii_enabled_headers_are_captured_but_redacted` |
| Trace sample rate 0 | no transaction events, errors still carry trace ids | `tests/unit/test_tracing.py::test_unsampled_transactions_send_nothing_but_still_propagate`, `test_asgi.py::test_unsampled_requests_still_report_errors_with_trace_ids` |
| Buggy `before_send`, broken `__str__`, broken transport | event dropped or placeholder used; never raises into the app | `tests/unit/test_client.py`, `tests/unit/test_exceptions.py` |
| Telemetry about telemetry | SDK logs, SDK HTTP calls and the worker's httpx logs never become events | `tests/integrations/test_logging.py`, `tests/integrations/test_httpx.py::test_suppressed_requests_are_not_instrumented` |
| Gzip bomb, oversized body, XSS in an exception message | 413 / 413 / escaped in the viewer | `tests/collector/test_ingestion.py`, `tests/collector/test_api.py::test_viewer_escapes_attacker_controlled_text` |

`pytest` runs with `filterwarnings = error`, so an unclosed file or socket, an
un-awaited coroutine or an exception swallowed during garbage collection fails
the suite rather than printing a warning.

The end-to-end tests start the collector and both example services as **separate
processes on real ports**; the outage test kills and restarts the collector
process. The collector tests (and, when `PULSETRACE_TEST_DATABASE_URL` is set,
the end-to-end tests) run against PostgreSQL as well as SQLite.

## Bugs the suite and benchmarks found

These were found while building PulseTrace. Each has a regression test.

1. **The worker could starve the event loop.** After the last batch of a flush,
   the waiting `flush()` had not yet resumed, so the worker still saw "urgent"
   work, received an empty batch and looped again *without awaiting*. The event
   loop never ran, so even `flush()`'s timeout could not fire. Fix: an empty
   batch now only means "closed and drained"; urgency only shortens the linger.
   Test: `test_flush_does_not_starve_the_event_loop`.
2. **Authentication headers were lost with an injected HTTP client.**
   `Authorization` and `User-Agent` were configured on the default client only.
   They are now set per request. Test: `test_wire_format_is_a_gzipped_versioned_envelope`.
3. **Key redaction dominated CPU.** Profiling showed `is_sensitive_key` called
   45 times per transaction and 183 times per error event, each lowercasing and
   scanning 11 substrings. A bounded decision cache and one compiled regex cut
   the 100%-sampled request from 99.6 µs to 79.5 µs (p50) and error capture from
   349 µs to 232 µs.
4. **A reference cycle per request caused a 3x latency tail.** A transaction
   span pointed at itself (`_segment = self`), so every request left garbage that
   only the cyclic collector could free. With instrumentation on and tracing
   off, p99 was 226 µs against 71 µs with GC disabled. Removing the cycle
   brought it to 47 µs. Finished child spans are now stored as plain data for
   the same reason (an abandoned transaction would otherwise be cyclic).
   Test: `test_finished_transactions_need_no_cycle_collector` asserts that
   `gc.collect()` finds nothing to free after 200 requests' worth of spans.
5. **Delivery logs could become telemetry.** httpx logs every request,
   including the SDK's own deliveries, and the logging integration recorded
   them. It now ignores records emitted where instrumentation is suppressed.
6. **The test harness leaked file descriptors** (service log files), caught by
   `filterwarnings = error`.

### Investigated and ruled out

While generating demo traffic, the request *after* an unhandled-exception 500
on a reused keep-alive connection was reset. With the same app and the same
uvicorn, 20 of 20 follow-up requests reset **without** PulseTrace too: uvicorn
closes the connection after an exception escapes the app, without sending
`Connection: close`, and the client reuses the connection before the close
lands. The SDK does not change this behaviour; the traffic script now opens a
fresh connection per request.

## Limits, stated plainly

* Delivery is at-least-once while the process lives. Events in memory are lost
  if the process is killed; there is no disk spool.
* Drop counts are visible locally (`telemetry.stats()`), but are not reported to
  the collector ("client reports").
* The collector writes synchronously in the request path. It is a test bed for
  the SDK, not an ingestion tier.
* Grouping is a deterministic heuristic; there is no issue merging, regression
  detection or resolution workflow.
