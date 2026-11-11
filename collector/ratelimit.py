"""A token bucket used to answer 429 + Retry-After (and to exercise the SDK's handling of it)."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable


class TokenBucket:
    def __init__(
        self, rate: float, burst: float, clock: Callable[[], float] = time.monotonic
    ) -> None:
        if rate <= 0 or burst <= 0:
            raise ValueError("rate and burst must be positive")
        self._rate = rate
        self._burst = burst
        self._tokens = burst
        self._clock = clock
        self._updated = clock()
        self._lock = threading.Lock()

    def try_acquire(self, amount: float) -> float | None:
        """Take ``amount`` tokens; return ``None`` on success or the seconds to wait."""
        amount = min(amount, self._burst)  # a batch larger than the burst is admitted when full
        with self._lock:
            now = self._clock()
            self._tokens = min(self._burst, self._tokens + (now - self._updated) * self._rate)
            self._updated = now
            if self._tokens >= amount:
                self._tokens -= amount
                return None
            return (amount - self._tokens) / self._rate
