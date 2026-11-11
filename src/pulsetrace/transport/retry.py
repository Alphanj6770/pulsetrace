r"""Delivery outcome classification and retry timing.

==========================  =============================================
Response                    Behaviour
==========================  =============================================
2xx                         delivered
400, 422 and other 4xx      rejected: the data is invalid, retrying is futile
401, 403                    configuration error: wrong API key, never retried
404, 3xx                    configuration error: wrong endpoint URL
408                         retried with backoff
413                         batch too large: split in half and resend
429                         retried after ``Retry-After`` (transport-wide pause)
5xx                         retried with backoff (503 honours ``Retry-After``)
timeouts, network errors    retried with backoff
==========================  =============================================

Backoff uses exponential growth with multiplicative jitter:

.. math:: d_i = \min(d_{max},\ d_0 \cdot 2^i) \cdot U(0.5, 1.5)

Jitter de-synchronises thousands of SDK instances that all saw the collector
fail at the same moment; without it they would all retry in lock-step and hit
the recovering collector with synchronised waves of traffic.
"""

from __future__ import annotations

import email.utils
import enum
import math
import random
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC

import httpx


class Outcome(enum.Enum):
    DELIVERED = "delivered"
    RETRY = "retry"
    REJECTED = "rejected"
    TOO_LARGE = "too_large"
    CONFIG_ERROR = "config_error"


@dataclass(frozen=True, slots=True)
class Decision:
    outcome: Outcome
    retry_after: float | None = None
    rate_limited: bool = False
    reason: str = ""


def parse_retry_after(value: str | None, *, now: float | None = None) -> float | None:
    """Seconds to wait according to a ``Retry-After`` header.

    Accepts both forms from RFC 9110: delay-seconds (``"120"``) and an
    HTTP-date (``"Wed, 11 Nov 2026 10:30:00 GMT"``). Dates in the past mean
    "now". Garbage yields ``None`` so the caller falls back to backoff.
    """
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            when = email.utils.parsedate_to_datetime(value)
        except (TypeError, ValueError, IndexError):
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        seconds = when.timestamp() - (time.time() if now is None else now)
    if not math.isfinite(seconds):
        return None
    return max(0.0, seconds)


def classify_response(
    status: int, headers: Mapping[str, str], *, now: float | None = None
) -> Decision:
    if 200 <= status < 300:
        return Decision(Outcome.DELIVERED)
    if status in (401, 403):
        return Decision(Outcome.CONFIG_ERROR, reason=f"HTTP {status}: check the API key")
    if status == 404 or 300 <= status < 400:
        return Decision(Outcome.CONFIG_ERROR, reason=f"HTTP {status}: check the endpoint URL")
    if status == 413:
        return Decision(Outcome.TOO_LARGE, reason="HTTP 413")
    if status == 429:
        return Decision(
            Outcome.RETRY,
            retry_after=parse_retry_after(headers.get("retry-after"), now=now),
            rate_limited=True,
            reason="HTTP 429",
        )
    if status == 408:
        return Decision(Outcome.RETRY, reason="HTTP 408")
    if 500 <= status < 600:
        retry_after = (
            parse_retry_after(headers.get("retry-after"), now=now) if status == 503 else None
        )
        return Decision(Outcome.RETRY, retry_after=retry_after, reason=f"HTTP {status}")
    return Decision(Outcome.REJECTED, reason=f"HTTP {status}")


def classify_exception(exc: BaseException) -> Decision:
    reason = type(exc).__name__
    if isinstance(exc, httpx.TimeoutException):
        return Decision(Outcome.RETRY, reason=f"timeout ({reason})")
    if isinstance(exc, httpx.UnsupportedProtocol | httpx.LocalProtocolError | httpx.InvalidURL):
        return Decision(Outcome.CONFIG_ERROR, reason=reason)
    if isinstance(exc, httpx.HTTPError | OSError):
        return Decision(Outcome.RETRY, reason=reason)
    # An unexpected exception is a bug, not a network condition: retrying it
    # would just loop. Drop the batch and let the caller log it.
    return Decision(Outcome.REJECTED, reason=f"unexpected {reason}")


class Backoff:
    def __init__(
        self,
        initial: float,
        maximum: float,
        *,
        rng: random.Random | None = None,
        jitter: tuple[float, float] = (0.5, 1.5),
    ) -> None:
        self._initial = initial
        self._maximum = maximum
        self._rng = rng if rng is not None else random.Random()
        self._jitter = jitter

    def base(self, attempt: int) -> float:
        """``min(d_max, d_0 * 2**attempt)`` without overflowing for large attempts."""
        if attempt >= 62:
            return self._maximum
        return min(self._maximum, self._initial * (1 << max(0, attempt)))

    def delay(self, attempt: int) -> float:
        low, high = self._jitter
        return self.base(attempt) * self._rng.uniform(low, high)
