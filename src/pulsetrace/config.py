"""Validated, immutable SDK configuration.

Configuration errors are the one place where PulseTrace raises: an invalid
sample rate or queue size is a programming mistake that should fail loudly at
start-up, not silently degrade telemetry in production. Everything that happens
*after* construction is guarded and never raises into the host application.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from pulsetrace.events import Event

OverflowPolicy = Literal["drop_newest", "drop_oldest"]
TraceSampling = Literal["consistent", "parent_based"]
BeforeSend = Callable[["Event"], "Event | None"]


class ConfigurationError(ValueError):
    """Raised when the SDK is constructed with invalid options."""


@dataclass(frozen=True, slots=True, kw_only=True)
class Options:
    # Identity -----------------------------------------------------------------
    endpoint: str | None = None
    api_key: str | None = None
    service_name: str = "unknown-service"
    environment: str = "production"
    release: str | None = None
    server_name: str | None = None
    enabled: bool = True

    # Sampling -----------------------------------------------------------------
    traces_sample_rate: float = 0.0
    error_sample_rate: float = 1.0
    trace_sampling: TraceSampling = "consistent"

    # Transport ----------------------------------------------------------------
    queue_size: int = 2048
    max_queue_bytes: int = 16 * 1024 * 1024
    overflow_policy: OverflowPolicy = "drop_newest"
    max_batch_size: int = 100
    max_batch_bytes: int = 1024 * 1024
    flush_interval: float = 1.0
    max_retries: int = 5
    initial_backoff: float = 0.5
    max_backoff: float = 30.0
    max_retry_after: float = 300.0
    request_timeout: float = 5.0
    compress: bool = True

    # Event size limits ----------------------------------------------------------
    max_breadcrumbs: int = 100
    max_spans_per_transaction: int = 1000
    max_frames: int = 100
    max_exceptions: int = 16
    max_message_length: int = 8192
    max_event_bytes: int = 1024 * 1024

    # Privacy ------------------------------------------------------------------
    send_default_pii: bool = False
    redact_keys: frozenset[str] = frozenset()
    scrub_card_numbers: bool = True

    # Stack traces -------------------------------------------------------------
    in_app_include: tuple[str, ...] = ()
    in_app_exclude: tuple[str, ...] = ()

    # Hooks / diagnostics --------------------------------------------------------
    before_send: BeforeSend | None = None
    debug: bool = False

    def __post_init__(self) -> None:
        _require_rate("traces_sample_rate", self.traces_sample_rate)
        _require_rate("error_sample_rate", self.error_sample_rate)
        for name in (
            "queue_size",
            "max_queue_bytes",
            "max_batch_size",
            "max_batch_bytes",
            "max_breadcrumbs",
            "max_spans_per_transaction",
            "max_frames",
            "max_exceptions",
            "max_message_length",
            "max_event_bytes",
        ):
            _require_positive_int(name, getattr(self, name))
        if not isinstance(self.max_retries, int) or self.max_retries < 0:
            raise ConfigurationError("max_retries must be a non-negative integer")
        for name in ("flush_interval", "initial_backoff", "request_timeout", "max_retry_after"):
            _require_positive_float(name, getattr(self, name))
        _require_positive_float("max_backoff", self.max_backoff)
        if self.max_backoff < self.initial_backoff:
            raise ConfigurationError("max_backoff must be >= initial_backoff")
        if self.overflow_policy not in ("drop_newest", "drop_oldest"):
            raise ConfigurationError("overflow_policy must be 'drop_newest' or 'drop_oldest'")
        if self.trace_sampling not in ("consistent", "parent_based"):
            raise ConfigurationError("trace_sampling must be 'consistent' or 'parent_based'")
        if not self.service_name or not self.service_name.strip():
            raise ConfigurationError("service_name must be a non-empty string")
        if self.endpoint is not None:
            parts = urlsplit(self.endpoint)
            if parts.scheme not in ("http", "https") or not parts.netloc:
                raise ConfigurationError(
                    f"endpoint must be an absolute http(s) URL, got {self.endpoint!r}"
                )
        if self.before_send is not None and not callable(self.before_send):
            raise ConfigurationError("before_send must be callable")


def _require_rate(name: str, value: float) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ConfigurationError(f"{name} must be a number between 0 and 1")
    if not math.isfinite(value) or not 0.0 <= value <= 1.0:
        raise ConfigurationError(f"{name} must be between 0 and 1, got {value!r}")


def _require_positive_int(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigurationError(f"{name} must be a positive integer, got {value!r}")


def _require_positive_float(name: str, value: float) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ConfigurationError(f"{name} must be a positive number")
    if not math.isfinite(value) or value <= 0:
        raise ConfigurationError(f"{name} must be a positive number, got {value!r}")
