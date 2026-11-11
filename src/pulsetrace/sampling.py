r"""Deterministic, coordination-free probabilistic sampling.

A trace is kept when

.. math::

    S(T) = \frac{H(T)}{2^{64}} < p

where ``H`` is the first 64 bits of BLAKE2b over the (lower-cased) trace id and
``p`` is the configured sample rate. Instead of comparing floats we compare the
integer ``H(T)`` against the integer threshold ``floor(p * 2**64)``.

Properties this buys us (each one is tested in ``tests/unit/test_sampling.py``):

* **Whole traces, never half traces.** Every span of a trace shares the trace
  id, so every span gets the same decision.
* **No coordination.** Two services configured with the same rate reach the
  same decision independently, without propagating a random number.
* **Nesting.** If ``p1 <= p2`` then every trace kept at ``p1`` is also kept at
  ``p2``. A service sampling at 10% keeps a subset of the traces a service
  sampling at 50% keeps, so the traces that *are* kept by the low-rate service
  are complete across the high-rate ones.
* **Reproducibility.** ``hash()`` is salted per process (PYTHONHASHSEED); BLAKE2b
  is stable across processes, machines and Python versions.
* **Robust to non-random ids.** Hashing the id (rather than reading its low bits
  directly) keeps decisions uniform even for id schemes with timestamp prefixes.
"""

from __future__ import annotations

import hashlib
import math

_SPACE = 1 << 64


def sampling_value(key: str) -> int:
    """Map ``key`` to a stable, approximately uniform integer in ``[0, 2**64)``."""
    digest = hashlib.blake2b(key.lower().encode("utf-8", "surrogatepass"), digest_size=8).digest()
    return int.from_bytes(digest, "big")


def rate_to_threshold(rate: float) -> int:
    """Integer threshold ``t`` with ``P(sampling_value(key) < t) == rate``."""
    if isinstance(rate, bool) or not isinstance(rate, int | float):
        raise ValueError("sample rate must be a number")
    if math.isnan(rate) or not 0.0 <= rate <= 1.0:
        raise ValueError(f"sample rate must be within [0, 1], got {rate!r}")
    if rate >= 1.0:
        return _SPACE
    return int(rate * _SPACE)


def should_sample(key: str, rate: float) -> bool:
    return sampling_value(key) < rate_to_threshold(rate)


class Sampler:
    """A precomputed sampling decision for a fixed rate."""

    __slots__ = ("_threshold", "rate")

    def __init__(self, rate: float) -> None:
        self._threshold = rate_to_threshold(rate)
        self.rate = float(rate)

    def __call__(self, key: str) -> bool:
        if self._threshold == 0:
            return False
        if self._threshold == _SPACE:
            return True
        return sampling_value(key) < self._threshold

    def __repr__(self) -> str:
        return f"Sampler(rate={self.rate!r})"
