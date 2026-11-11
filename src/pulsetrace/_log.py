"""The SDK's own diagnostics.

PulseTrace logs to the ``pulsetrace`` logger, which has a ``NullHandler`` like
any well-behaved library. The logging integration ignores this logger, so SDK
diagnostics can never be turned into telemetry events (no feedback loop).
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

logger = logging.getLogger("pulsetrace")
logger.addHandler(logging.NullHandler())


class _DebugHandler(logging.StreamHandler):  # type: ignore[type-arg]
    pass


def enable_debug_logging() -> None:
    """Print SDK diagnostics to stderr (``PulseTrace(debug=True)``)."""
    if any(isinstance(handler, _DebugHandler) for handler in logger.handlers):
        return
    handler = _DebugHandler()
    handler.setFormatter(logging.Formatter("[pulsetrace] %(levelname)s: %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)


class RateLimitedLog:
    """Log a given key at most once per ``interval`` seconds.

    During a collector outage every batch fails; without this, the SDK would
    flood the host application's logs with identical warnings.
    """

    def __init__(self, interval: float = 60.0, clock: Callable[[], float] = time.monotonic) -> None:
        self._interval = interval
        self._clock = clock
        self._last: dict[str, float] = {}
        self._lock = threading.Lock()

    def __call__(
        self, level: int, key: str, message: str, *args: object, exc_info: bool = False
    ) -> None:
        now = self._clock()
        with self._lock:
            last = self._last.get(key)
            if last is not None and now - last < self._interval:
                return
            self._last[key] = now
        logger.log(level, message, *args, exc_info=exc_info)
