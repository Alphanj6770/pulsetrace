"""Collector configuration, read from the environment."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

DEFAULT_DATABASE_URL = "sqlite+aiosqlite:///./pulsetrace.db"
MIB = 1024 * 1024


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str = DEFAULT_DATABASE_URL
    # Empty means "no authentication" (local development).
    api_keys: frozenset[str] = frozenset()
    max_body_bytes: int = 5 * MIB
    max_decompressed_bytes: int = 20 * MIB
    max_events_per_batch: int = 1000
    # Token bucket over events/second; 0 disables rate limiting.
    rate_limit_per_second: float = 0.0
    rate_limit_burst: float = 0.0
    # Fraction of ingestion requests answered with 503 (failure injection for demos).
    failure_rate: float = 0.0

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if environ is None else environ
        keys = frozenset(
            key.strip() for key in env.get("PULSETRACE_API_KEYS", "").split(",") if key.strip()
        )
        rate = float(env.get("COLLECTOR_RATE_LIMIT", "0") or 0)
        return cls(
            database_url=env.get("DATABASE_URL", DEFAULT_DATABASE_URL),
            api_keys=keys,
            max_body_bytes=int(env.get("COLLECTOR_MAX_BODY_BYTES", 5 * MIB)),
            max_decompressed_bytes=int(env.get("COLLECTOR_MAX_DECOMPRESSED_BYTES", 20 * MIB)),
            max_events_per_batch=int(env.get("COLLECTOR_MAX_EVENTS_PER_BATCH", 1000)),
            rate_limit_per_second=rate,
            rate_limit_burst=float(env.get("COLLECTOR_RATE_LIMIT_BURST", rate * 2) or 0),
            failure_rate=float(env.get("COLLECTOR_FAILURE_RATE", "0") or 0),
        )
