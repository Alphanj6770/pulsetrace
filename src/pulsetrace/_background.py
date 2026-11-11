"""A private event loop on a daemon thread, for synchronous applications.

Scripts, Celery workers or Django apps have no running event loop to host the
transport worker. ``PulseTrace.start_in_background()`` runs the very same
async transport on this loop instead; producers on any thread keep using the
thread-safe buffer.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Coroutine
from typing import Any, TypeVar

T = TypeVar("T")


class BackgroundLoop:
    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run, name="pulsetrace-loop", daemon=True)

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def alive(self) -> bool:
        return self._thread.is_alive()

    def start(self) -> None:
        self._thread.start()
        self._ready.wait(timeout=5.0)

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.call_soon(self._ready.set)
        try:
            self._loop.run_forever()
        finally:
            try:
                pending = [task for task in asyncio.all_tasks(self._loop) if not task.done()]
                for task in pending:
                    task.cancel()
                if pending:
                    self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            finally:
                self._loop.close()

    def run(self, coro: Coroutine[Any, Any, T], timeout: float) -> T:
        """Run ``coro`` on the background loop and wait for its result."""
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout)

    def stop(self, timeout: float = 2.0) -> None:
        if self._thread.is_alive():
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout)
