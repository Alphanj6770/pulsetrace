"""Asynchronous HTTP transport: bounded buffer -> batches -> retries -> collector.

Delivery semantics are *bounded, best-effort, at-least-once while the process
lives*: a batch may be delivered twice (the collector committed it but the
acknowledgement was lost), which the collector absorbs by deduplicating on
``event_id``. Events still in memory when the process crashes are lost.
"""

from __future__ import annotations

import asyncio
import gzip
import logging
import random
import time
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

import httpx

from pulsetrace._log import RateLimitedLog
from pulsetrace._version import __version__
from pulsetrace.config import Options
from pulsetrace.transport.base import ShutdownReport
from pulsetrace.transport.buffer import EventBuffer
from pulsetrace.transport.retry import (
    Backoff,
    Decision,
    Outcome,
    classify_exception,
    classify_response,
)
from pulsetrace.transport.worker import BatchWorker
from pulsetrace.utils import now_timestamp, truncate

_THREAD_COMPRESS_THRESHOLD = 64 * 1024
_USER_AGENT = f"pulsetrace-python/{__version__}"


class AsyncHttpTransport:
    def __init__(
        self,
        options: Options,
        *,
        client_factory: Callable[[], httpx.AsyncClient] | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
        rng: random.Random | None = None,
    ) -> None:
        if options.endpoint is None:
            raise ValueError("AsyncHttpTransport requires an endpoint")
        self._options = options
        self._endpoint = options.endpoint
        self._client_factory = client_factory
        self._sleep = sleep
        self._clock = clock
        self._buffer = EventBuffer(
            max_items=options.queue_size,
            max_bytes=options.max_queue_bytes,
            overflow=options.overflow_policy,
        )
        self._worker = BatchWorker(
            self._buffer,
            self._deliver,
            max_batch_size=options.max_batch_size,
            max_batch_bytes=options.max_batch_bytes,
            linger=options.flush_interval,
        )
        self._backoff = Backoff(options.initial_backoff, options.max_backoff, rng=rng)
        self._log = RateLimitedLog()
        self._client: httpx.AsyncClient | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._state = "new"
        self._blocked_until = 0.0
        self._failure_streak = 0
        self._report: ShutdownReport | None = None
        self._counts: dict[str, int] = dict.fromkeys(
            (
                "delivered",
                "batches_delivered",
                "retries",
                "rate_limited",
                "dropped_rejected",
                "dropped_config_error",
                "dropped_retries_exhausted",
                "unsent_at_shutdown",
            ),
            0,
        )
        self._last_status: int | None = None
        self._last_error: str | None = None

    # -- producer API ---------------------------------------------------------------

    def submit(self, payload: bytes, *, low_priority: bool = False) -> bool:
        return self._buffer.put(payload, low_priority=low_priority)

    # -- lifecycle -------------------------------------------------------------------

    async def start(self) -> None:
        if self._state != "new":
            return
        loop = asyncio.get_running_loop()
        self._loop = loop
        self._buffer.bind(loop)
        if self._client_factory is not None:
            self._client = self._client_factory()
        else:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self._options.request_timeout),
                limits=httpx.Limits(max_connections=2, max_keepalive_connections=1),
            )
        self._worker.start()
        self._state = "running"

    async def flush(self, timeout: float) -> bool:
        if self._state != "running":
            return len(self._buffer) == 0
        if not self._on_own_loop():
            return bool(await self._run_on_own_loop(self._worker.flush(timeout)))
        return await self._worker.flush(timeout)

    async def shutdown(self, timeout: float) -> ShutdownReport:
        if self._report is not None:
            return self._report
        started = time.monotonic()
        if self._state == "new":
            self._buffer.close()
            unsent = len(self._buffer.drain())
        elif not self._on_own_loop():
            return await self._run_on_own_loop(self.shutdown(timeout))  # type: ignore[no-any-return]
        else:
            unsent = await self._worker.stop(timeout)
            if self._client is not None:  # created in start(), so owned here
                try:
                    await self._client.aclose()
                except Exception:
                    pass
        self._state = "closed"
        self._counts["unsent_at_shutdown"] = unsent
        self._report = ShutdownReport(
            delivered=self._counts["delivered"],
            dropped=self._dropped_total(),
            unsent=unsent,
            elapsed=time.monotonic() - started,
        )
        return self._report

    def _on_own_loop(self) -> bool:
        try:
            return asyncio.get_running_loop() is self._loop
        except RuntimeError:
            return False

    async def _run_on_own_loop(self, coro: Coroutine[Any, Any, Any]) -> Any:
        """Support calling flush/shutdown from a different loop (e.g. sync mode)."""
        assert self._loop is not None
        try:
            future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        except RuntimeError:
            return False
        return await asyncio.wrap_future(future)

    # -- delivery ----------------------------------------------------------------

    async def _deliver(self, batch: list[bytes]) -> None:
        body = await self._encode(batch)
        # Set per request (not on the client) so they apply to any injected client too.
        headers = {"User-Agent": _USER_AGENT, "Content-Type": "application/json"}
        if self._options.compress:
            headers["Content-Encoding"] = "gzip"
        if self._options.api_key:
            headers["Authorization"] = f"Bearer {self._options.api_key}"
        attempt = 0
        while True:
            wait = self._blocked_until - self._clock()
            if wait > 0:
                await self._sleep(wait)
            decision = await self._post(body, headers)
            outcome = decision.outcome
            if outcome is Outcome.DELIVERED:
                self._failure_streak = 0
                self._counts["delivered"] += len(batch)
                self._counts["batches_delivered"] += 1
                return
            if outcome is Outcome.TOO_LARGE and len(batch) > 1:
                middle = len(batch) // 2
                await self._deliver(batch[:middle])
                await self._deliver(batch[middle:])
                return
            if outcome in (Outcome.REJECTED, Outcome.TOO_LARGE):
                self._counts["dropped_rejected"] += len(batch)
                self._log(
                    logging.WARNING,
                    "rejected",
                    "PulseTrace collector rejected a batch: %s",
                    decision.reason,
                )
                return
            if outcome is Outcome.CONFIG_ERROR:
                self._counts["dropped_config_error"] += len(batch)
                self._log(
                    logging.ERROR, "config", "PulseTrace cannot deliver events: %s", decision.reason
                )
                return
            # Retryable. The pause applies transport-wide, so the next batch
            # also waits instead of hammering a collector that is down.
            self._failure_streak += 1
            if decision.retry_after is not None:
                delay = min(decision.retry_after, self._options.max_retry_after)
            else:
                delay = self._backoff.delay(self._failure_streak - 1)
            self._blocked_until = self._clock() + delay
            if decision.rate_limited:
                self._counts["rate_limited"] += 1
            if attempt >= self._options.max_retries:
                self._counts["dropped_retries_exhausted"] += len(batch)
                self._log(
                    logging.WARNING,
                    "retries",
                    "PulseTrace dropped a batch after retries: %s",
                    decision.reason,
                )
                return
            attempt += 1
            self._counts["retries"] += 1

    async def _post(self, body: bytes, headers: dict[str, str]) -> Decision:
        assert self._client is not None
        try:
            response = await self._client.post(self._endpoint, content=body, headers=headers)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            decision = classify_exception(exc)
            self._last_error = decision.reason
            return decision
        self._last_status = response.status_code
        decision = classify_response(response.status_code, response.headers)
        if decision.outcome is not Outcome.DELIVERED:
            self._last_error = f"{decision.reason}: {truncate(response.text, 200)}"
        return decision

    async def _encode(self, batch: list[bytes]) -> bytes:
        header = (
            f'{{"schema_version":1,"sent_at":"{now_timestamp()}","sdk":{{"name":"pulsetrace.python","version":"{__version__}"}},"events":['
        ).encode()
        raw = header + b",".join(batch) + b"]}"
        if not self._options.compress:
            return raw
        if len(raw) >= _THREAD_COMPRESS_THRESHOLD:
            # zlib releases the GIL, so large batches compress off the event loop.
            return await asyncio.to_thread(gzip.compress, raw, 6)
        return gzip.compress(raw, 6)

    # -- introspection ----------------------------------------------------------

    def _dropped_total(self) -> int:
        return (
            self._buffer.dropped_full
            + self._buffer.dropped_closed
            + self._buffer.evicted
            + self._counts["dropped_rejected"]
            + self._counts["dropped_config_error"]
            + self._counts["dropped_retries_exhausted"]
        )

    def stats(self) -> dict[str, Any]:
        buffer = self._buffer
        return {
            "transport": "http",
            "state": self._state,
            "queue_depth": len(buffer),
            "queue_capacity": buffer.capacity,
            "queue_bytes": buffer.size_bytes,
            "queue_high_watermark": buffer.high_watermark,
            "accepted": buffer.accepted,
            "dropped_queue_full": buffer.dropped_full,
            "dropped_after_shutdown": buffer.dropped_closed,
            "evicted": buffer.evicted,
            "in_flight": self._worker.in_flight,
            **self._counts,
            "last_status": self._last_status,
            "last_error": self._last_error,
        }
