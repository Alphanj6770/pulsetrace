"""Statistical and structural properties of deterministic trace sampling."""

from __future__ import annotations

import math
import random

import pytest

from pulsetrace.sampling import Sampler, rate_to_threshold, sampling_value, should_sample

N = 200_000


def _trace_ids(count: int, seed: int = 1234) -> list[str]:
    rng = random.Random(seed)
    return [f"{rng.getrandbits(128):032x}" for _ in range(count)]


IDS = _trace_ids(N)


def test_same_trace_id_always_gets_the_same_decision() -> None:
    sampler = Sampler(0.3)
    for trace_id in IDS[:1000]:
        assert sampler(trace_id) == sampler(trace_id) == should_sample(trace_id, 0.3)


def test_hash_is_stable_across_processes() -> None:
    # Pinned value: if this changes, services on different SDK versions would
    # disagree about which traces to keep.
    assert sampling_value("4bf92f3577b34da6a3ce929d0e0e4736") == 0x2680765D0110C054


def test_decision_ignores_hex_case() -> None:
    trace_id = IDS[0]
    assert sampling_value(trace_id) == sampling_value(trace_id.upper())


@pytest.mark.parametrize("rate", [0.01, 0.1, 0.5, 0.9])
def test_kept_fraction_matches_rate_within_five_sigma(rate: float) -> None:
    sampler = Sampler(rate)
    kept = sum(1 for trace_id in IDS if sampler(trace_id))
    sigma = math.sqrt(N * rate * (1 - rate))
    assert abs(kept - N * rate) < 5 * sigma


def test_rate_zero_keeps_nothing_and_rate_one_keeps_everything() -> None:
    assert not any(Sampler(0.0)(trace_id) for trace_id in IDS[:5000])
    assert all(Sampler(1.0)(trace_id) for trace_id in IDS[:5000])


def test_decisions_are_nested_across_rates() -> None:
    """Every trace kept at a lower rate is kept at every higher rate."""
    low, high = Sampler(0.05), Sampler(0.2)
    kept_low = {trace_id for trace_id in IDS if low(trace_id)}
    kept_high = {trace_id for trace_id in IDS if high(trace_id)}
    assert kept_low <= kept_high
    assert len(kept_high) > len(kept_low)


def test_hash_values_are_uniform_chi_square() -> None:
    buckets = 64
    counts = [0] * buckets
    for trace_id in IDS:
        counts[sampling_value(trace_id) * buckets >> 64] += 1
    expected = N / buckets
    chi_square = sum((count - expected) ** 2 / expected for count in counts)
    # 63 degrees of freedom; the 99.9th percentile of chi^2(63) is ~103.4.
    assert chi_square < 103.4


def test_threshold_is_exact_at_the_boundaries() -> None:
    assert rate_to_threshold(0.0) == 0
    assert rate_to_threshold(1.0) == 1 << 64
    assert rate_to_threshold(0.5) == 1 << 63


@pytest.mark.parametrize("rate", [-0.1, 1.5, float("nan"), True, "0.5"])
def test_invalid_rates_are_rejected(rate: object) -> None:
    with pytest.raises(ValueError):
        rate_to_threshold(rate)  # type: ignore[arg-type]
