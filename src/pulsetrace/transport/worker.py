"""The background batching worker and its lifecycle.

One asyncio task drains the :class:`~pulsetrace.transport.buffer.EventBuffer`
in batches and hands each batch to a ``deliver`` coroutine (which owns HTTP and
retries). The worker owns three things:

* **batch formation** - send when ``max_batch_size`` is reached or
  ``flush_interval`` has passed since the first event of the batch;
* **accounting** - every accepted event is eventually *settled* (delivered or
  permanently dropped) or reported as *unsent* at shutdown, so
  ``accepted == settled + evicted + buffered + in_flight`` always holds;
* **lifecycle** - flush (wait for what is already queued), and stop (drain
  within a deadline, then cancel and report what is left).

The task is created with an empty ``contextvars.Context``. Otherwise it would
inherit whatever context ``start()`` was called from (possibly a request's
scope), and with instrumentation suppressed so the SDK's own HTTP calls are
never instrumented.
"""

from __future__ import annotations

import asyncio
import contextvars
from collections.abc import Awaitable, Callable

from pulsetrace._log import logger
from pulsetrace.context import _SUPPRESS
from pulsetrace.transport.buffer import EventBuffer

Deliver = Callable[[list[bytes]], Awaitable[None]]


class BatchWorker:
    def __init__(
        self,
        buffer: EventBuffer,
        deliver: Deliver,
        *,
        max_batch_size: int,
        max_batch_bytes: int,
        linger: float,
    ) -> None:
        self._buffer = buffer
        self._deliver = deliver
        self._max_batch_size = max_batch_size
        self._max_batch_bytes = max_batch_bytes
        self._linger = linger
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self._flush_waiters = 0
        self._settled = 0
        self._in_flight = 0
        self._progress: asyncio.Event | None = None
        self.internal_errors = 0

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def in_flight(self) -> int:
        return self._in_flight

    @property
    def settled(self) -> int:
        return self._settled

    def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._progress = asyncio.Event()
        self._task = loop.create_task(
            self._run(), name="pulsetrace-transport-worker", context=contextvars.Context()
        )
        self._task.add_done_callback(self._on_done)

    def _urgent(self) -> bool:
        return self._stopping or self._flush_waiters > 0

    async def _run(self) -> None:
        _SUPPRESS.set(True)  # this task's private context only
        while True:
            batch = await self._buffer.next_batch(
                max_items=self._max_batch_size,
                max_bytes=self._max_batch_bytes,
                linger=self._linger,
                urgent=self._urgent,
            )
            if not batch:
                return  # closed and drained
            self._in_flight = len(batch)
            try:
                await self._deliver(batch)
            except asyncio.CancelledError:
                raise  # the batch stays counted as in flight -> reported as unsent
            except Exception:
                # deliver() accounts for every outcome itself; reaching this is
                # a bug. Count it and keep the worker alive.
                self.internal_errors += 1
                logger.exception("PulseTrace transport failed to deliver a batch")
            self._in_flight = 0
            self._settle(len(batch))

    def _settle(self, count: int) -> None:
        self._settled += count
        progress = self._progress
        if progress is not None:
            progress.set()
            self._progress = asyncio.Event()

    def _on_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error("PulseTrace transport worker crashed", exc_info=exc)

    async def flush(self, timeout: float) -> bool:
        """Wait until every event accepted before this call has settled."""
        target = self._buffer.accepted
        if self._settled_total() >= target:
            return True
        if not self.running:
            return False
        self._flush_waiters += 1
        self._buffer.wake()
        try:
            async with asyncio.timeout(timeout):
                while self._settled_total() < target:
                    progress = self._progress
                    assert progress is not None
                    await progress.wait()
            return True
        except TimeoutError:
            return False
        finally:
            self._flush_waiters -= 1

    def _settled_total(self) -> int:
        return self._settled + self._buffer.evicted

    async def stop(self, timeout: float) -> int:
        """Drain what is buffered within ``timeout``; return how many events are left unsent."""
        self._stopping = True
        self._buffer.close()
        self._buffer.wake()
        task = self._task
        if task is not None and not task.done():
            try:
                done, _ = await asyncio.wait({task}, timeout=max(0.0, timeout))
            except asyncio.CancelledError:
                task.cancel()  # never leak the worker if shutdown itself is cancelled
                raise
            if not done:
                task.cancel()
                await asyncio.wait({task}, timeout=1.0)
        unsent = len(self._buffer.drain()) + self._in_flight
        self._in_flight = 0
        return unsent
