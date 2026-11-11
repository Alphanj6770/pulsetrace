"""A bounded, thread-safe event buffer with an asyncio wake-up for one consumer.

Why not ``asyncio.Queue(maxsize=...)``? Because producers are not only
coroutines on the event loop. FastAPI runs ``def`` endpoints in a thread pool,
logging handlers run on whichever thread logged, and background threads capture
exceptions too. ``asyncio.Queue`` is not thread-safe: calling ``put_nowait``
from another thread can corrupt its internal waiter bookkeeping and lose
wake-ups. This buffer is a ``deque`` guarded by a ``threading.Lock`` (held for a
few hundred nanoseconds, never across I/O), and wakes the consumer with
``loop.call_soon_threadsafe`` when the producer is not on the loop's thread.

Memory is bounded twice: by event count and by bytes. Events are stored
pre-serialised, so the byte bound is exact rather than an estimate.

Overflow never blocks the caller. ``drop_newest`` (the default) rejects the
incoming event and keeps the oldest ones, which during an incident are usually
the first errors, i.e. the cause rather than the cascade. ``drop_oldest`` keeps
the most recent state instead. Low-priority events (transactions) may only use
part of the capacity, so a burst of performance data cannot crowd out errors.
"""

from __future__ import annotations

import asyncio
import threading
from collections import deque
from collections.abc import Callable
from typing import Literal

OverflowPolicy = Literal["drop_newest", "drop_oldest"]
LOW_PRIORITY_SHARE = 0.75


class EventBuffer:
    def __init__(
        self,
        *,
        max_items: int,
        max_bytes: int,
        overflow: OverflowPolicy = "drop_newest",
        low_priority_share: float = LOW_PRIORITY_SHARE,
    ) -> None:
        self._items: deque[bytes] = deque()
        self._bytes = 0
        self._lock = threading.Lock()
        self._max_items = max_items
        self._max_bytes = max_bytes
        self._low_priority_limit = max(1, int(max_items * low_priority_share))
        self._overflow = overflow
        self._closed = False
        # Consumer side; only touched on the loop's thread.
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: int | None = None
        self._wakeup: asyncio.Event | None = None
        self._consumer_waiting = False
        # Counters (guarded by _lock).
        self.accepted = 0
        self.dropped_full = 0
        self.dropped_closed = 0
        self.evicted = 0
        self.high_watermark = 0

    # -- producer side (any thread) ----------------------------------------------

    def put(self, item: bytes, *, low_priority: bool = False) -> bool:
        size = len(item)
        with self._lock:
            if self._closed:
                self.dropped_closed += 1
                return False
            if size > self._max_bytes:
                self.dropped_full += 1
                return False
            limit = self._low_priority_limit if low_priority else self._max_items
            if len(self._items) >= limit or self._bytes + size > self._max_bytes:
                if self._overflow == "drop_newest" or low_priority:
                    self.dropped_full += 1
                    return False
                while self._items and (
                    len(self._items) >= self._max_items or self._bytes + size > self._max_bytes
                ):
                    self._bytes -= len(self._items.popleft())
                    self.evicted += 1
            self._items.append(item)
            self._bytes += size
            self.accepted += 1
            if len(self._items) > self.high_watermark:
                self.high_watermark = len(self._items)
            notify = self._consumer_waiting
            if notify:
                self._consumer_waiting = False
            loop = self._loop
        if notify and loop is not None:
            self._notify(loop)
        return True

    def _notify(self, loop: asyncio.AbstractEventLoop) -> None:
        wakeup = self._wakeup
        if wakeup is None:
            return
        if threading.get_ident() == self._loop_thread:
            wakeup.set()
            return
        try:
            loop.call_soon_threadsafe(wakeup.set)
        except RuntimeError:
            pass  # the loop is closed; the events stay in memory and are reported as unsent

    # -- consumer side (event loop thread) -------------------------------------------

    def bind(self, loop: asyncio.AbstractEventLoop) -> None:
        """Attach the consumer. Must be called from ``loop``'s thread."""
        with self._lock:
            self._loop = loop
            self._loop_thread = threading.get_ident()
            self._wakeup = asyncio.Event()

    def wake(self) -> None:
        """Interrupt a waiting consumer (used for flush and shutdown)."""
        if self._wakeup is not None:
            self._wakeup.set()

    def _take(self, max_items: int, max_bytes: int) -> list[bytes]:
        with self._lock:
            batch: list[bytes] = []
            total = 0
            while self._items and len(batch) < max_items:
                size = len(self._items[0])
                if batch and total + size > max_bytes:
                    break
                batch.append(self._items.popleft())
                total += size
                self._bytes -= size
            return batch

    def _arm(self) -> bool:
        """Prepare to wait. ``False`` if items arrived meanwhile (do not wait)."""
        assert self._wakeup is not None
        self._wakeup.clear()
        with self._lock:
            if self._items:
                return False
            self._consumer_waiting = True
            return True

    async def next_batch(
        self,
        *,
        max_items: int,
        max_bytes: int,
        linger: float,
        urgent: Callable[[], bool],
    ) -> list[bytes]:
        """Wait for events, then gather up to ``max_items`` of them.

        After the first event arrives, keep collecting for at most ``linger``
        seconds, so one HTTP request carries many events. ``urgent()`` (a flush
        or shutdown is waiting) cuts the linger short. Returns an empty list
        only once the buffer is closed and fully drained.

        Every path that does not return a batch awaits: a consumer that loops
        without yielding would starve the event loop - including the very
        flush() it is trying to serve.
        """
        assert self._wakeup is not None, "bind() must be called first"
        wakeup = self._wakeup
        while True:
            batch = self._take(max_items, max_bytes)
            if batch:
                break
            if self._closed:
                return []
            if self._arm():
                await wakeup.wait()

        loop = asyncio.get_running_loop()
        deadline = loop.time() + linger
        batch_bytes = sum(len(item) for item in batch)
        while len(batch) < max_items and not urgent():
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            if self._arm():
                try:
                    async with asyncio.timeout(remaining):
                        await wakeup.wait()
                except TimeoutError:
                    pass
            more = self._take(max_items - len(batch), max_bytes - batch_bytes)
            if more:
                batch.extend(more)
                batch_bytes += sum(len(item) for item in more)
            elif len(self) > 0:
                break  # the next event does not fit in this batch's byte budget
        return batch

    # -- lifecycle / introspection --------------------------------------------------

    def close(self) -> None:
        """Reject all further ``put`` calls."""
        with self._lock:
            self._closed = True

    def drain(self) -> list[bytes]:
        with self._lock:
            items = list(self._items)
            self._items.clear()
            self._bytes = 0
            return items

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def capacity(self) -> int:
        return self._max_items

    @property
    def size_bytes(self) -> int:
        with self._lock:
            return self._bytes

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)
