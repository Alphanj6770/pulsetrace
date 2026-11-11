"""Small, dependency-free helpers shared across the SDK.

Every helper in this module is written for the *unhappy path*: they accept
arbitrary user objects and must never raise, never block and never keep a
reference to the object they were given.
"""

from __future__ import annotations

import os
import reprlib
import time
import uuid
from datetime import UTC, datetime

_ELLIPSIS = "…"

_repr = reprlib.Repr()
_repr.maxstring = 256
_repr.maxother = 256
_repr.maxlong = 64
_repr.maxlist = 20
_repr.maxtuple = 20
_repr.maxdict = 20
_repr.maxset = 20
_repr.maxfrozenset = 20
_repr.maxdeque = 20
_repr.maxarray = 20
_repr.maxlevel = 3


def truncate(text: str, max_length: int) -> str:
    """Shorten ``text`` to at most ``max_length`` characters, marking the cut."""
    if len(text) <= max_length:
        return text
    if max_length <= 1:
        return text[:max_length]
    return text[: max_length - 1] + _ELLIPSIS


def type_name(value: object) -> str:
    try:
        return type(value).__name__
    except Exception:
        return "object"


def safe_str(value: object, max_length: int = 8192) -> str:
    """``str(value)`` that survives a broken ``__str__`` and bounds the result."""
    if isinstance(value, str):
        return truncate(value, max_length)
    try:
        text = str(value)
    except Exception:
        return f"<unprintable {type_name(value)} object>"
    return truncate(text, max_length)


def safe_repr(value: object, max_length: int = 1024) -> str:
    """Bounded ``repr`` that survives a broken ``__repr__``."""
    try:
        text = _repr.repr(value)
    except Exception:
        return f"<unrepresentable {type_name(value)} object>"
    return truncate(text, max_length)


def new_event_id() -> str:
    """32 lowercase hex characters (a random UUID4 without dashes)."""
    return uuid.uuid4().hex


def _random_hex(num_bytes: int) -> str:
    # os.urandom rather than the `random` module: a forked worker process
    # (gunicorn, multiprocessing) must never repeat its parent's identifiers.
    while True:
        value = os.urandom(num_bytes)
        if any(value):  # all-zero ids are invalid in W3C Trace Context
            return value.hex()


def new_trace_id() -> str:
    """128-bit W3C trace id as 32 lowercase hex characters."""
    return _random_hex(16)


def new_span_id() -> str:
    """64-bit W3C span id as 16 lowercase hex characters."""
    return _random_hex(8)


def format_timestamp(epoch_seconds: float) -> str:
    """RFC 3339 UTC timestamp with microseconds, e.g. ``2026-11-11T10:30:00.123456Z``."""
    return datetime.fromtimestamp(epoch_seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def now_timestamp() -> str:
    return format_timestamp(time.time())
