"""The transport interface and its trivial implementation."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class ShutdownReport:
    """What happened to the events a transport accepted over its lifetime."""

    delivered: int
    dropped: int
    unsent: int
    elapsed: float

    @property
    def clean(self) -> bool:
        """``True`` if nothing was left in memory when the transport closed."""
        return self.unsent == 0


@runtime_checkable
class Transport(Protocol):
    """Moves serialised events out of the process.

    ``submit`` is called on the application's request path, from any thread,
    so it must be non-blocking, thread-safe and must never raise.
    """

    def submit(self, payload: bytes, *, low_priority: bool = False) -> bool:
        """Accept one serialised event. Returns ``False`` if it was dropped."""
        ...

    async def start(self) -> None: ...

    async def flush(self, timeout: float) -> bool:
        """Wait until everything submitted so far is delivered or dropped."""
        ...

    async def shutdown(self, timeout: float) -> ShutdownReport: ...

    def stats(self) -> dict[str, Any]: ...


class NullTransport:
    """Accepts events and discards them.

    Used when no endpoint is configured, and in benchmarks to measure the cost
    of instrumentation without the cost of transmission.
    """

    def __init__(self) -> None:
        self._accepted = 0
        self._lock = threading.Lock()

    def submit(self, payload: bytes, *, low_priority: bool = False) -> bool:
        with self._lock:
            self._accepted += 1
        return True

    async def start(self) -> None:
        return None

    async def flush(self, timeout: float) -> bool:
        return True

    async def shutdown(self, timeout: float) -> ShutdownReport:
        return ShutdownReport(delivered=0, dropped=self._accepted, unsent=0, elapsed=0.0)

    def stats(self) -> dict[str, Any]:
        return {"transport": "null", "accepted": self._accepted}
