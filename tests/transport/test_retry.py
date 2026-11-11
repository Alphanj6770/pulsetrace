from __future__ import annotations

import random

import httpx
import pytest

from pulsetrace.transport.retry import (
    Backoff,
    Outcome,
    classify_exception,
    classify_response,
    parse_retry_after,
)


@pytest.mark.parametrize(
    ("status", "outcome"),
    [
        (200, Outcome.DELIVERED),
        (202, Outcome.DELIVERED),
        (400, Outcome.REJECTED),
        (422, Outcome.REJECTED),
        (401, Outcome.CONFIG_ERROR),
        (403, Outcome.CONFIG_ERROR),
        (404, Outcome.CONFIG_ERROR),
        (301, Outcome.CONFIG_ERROR),
        (408, Outcome.RETRY),
        (413, Outcome.TOO_LARGE),
        (429, Outcome.RETRY),
        (500, Outcome.RETRY),
        (502, Outcome.RETRY),
        (503, Outcome.RETRY),
    ],
)
def test_status_classification(status: int, outcome: Outcome) -> None:
    assert classify_response(status, {}).outcome is outcome


def test_429_carries_retry_after() -> None:
    decision = classify_response(429, {"retry-after": "7"})
    assert decision.rate_limited
    assert decision.retry_after == 7.0


def test_503_honours_retry_after_but_500_does_not() -> None:
    assert classify_response(503, {"retry-after": "3"}).retry_after == 3.0
    assert classify_response(500, {"retry-after": "3"}).retry_after is None


def test_retry_after_http_date() -> None:
    now = 1_794_393_000.0  # 2026-11-11T10:30:00Z
    assert parse_retry_after("Wed, 11 Nov 2026 10:30:30 GMT", now=now) == pytest.approx(30.0)
    assert parse_retry_after("Wed, 11 Nov 2026 10:29:00 GMT", now=now) == 0.0


@pytest.mark.parametrize("value", [None, "", "soon", "nan", "inf", "Wed, 99 Foo"])
def test_invalid_retry_after_is_ignored(value: str | None) -> None:
    assert parse_retry_after(value) is None


def test_negative_retry_after_means_now() -> None:
    assert parse_retry_after("-5") == 0.0


@pytest.mark.parametrize(
    ("exc", "outcome"),
    [
        (httpx.ReadTimeout("t"), Outcome.RETRY),
        (httpx.ConnectError("refused"), Outcome.RETRY),
        (httpx.RemoteProtocolError("eof"), Outcome.RETRY),
        (httpx.UnsupportedProtocol("ftp"), Outcome.CONFIG_ERROR),
        (ConnectionResetError(), Outcome.RETRY),
        (RuntimeError("bug"), Outcome.REJECTED),
    ],
)
def test_exception_classification(exc: BaseException, outcome: Outcome) -> None:
    assert classify_exception(exc).outcome is outcome


def test_backoff_grows_exponentially_within_jitter_bounds() -> None:
    backoff = Backoff(0.5, 30.0, rng=random.Random(42))
    for attempt in range(12):
        base = min(30.0, 0.5 * 2**attempt)
        for _ in range(50):
            assert 0.5 * base <= backoff.delay(attempt) <= 1.5 * base


def test_backoff_is_jittered_and_never_overflows() -> None:
    backoff = Backoff(0.5, 30.0, rng=random.Random(1))
    assert len({round(backoff.delay(3), 6) for _ in range(20)}) > 10
    assert backoff.base(10_000) == 30.0
