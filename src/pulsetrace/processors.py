"""Event processors: privacy scrubbing applied to every event before it is queued.

Two complementary strategies:

1. **Key-based redaction.** Any mapping entry whose key looks sensitive
   (``Authorization``, ``X-API-Key``, ``password``, ``session_id``, ``cvv``...)
   has its value replaced with ``[REDACTED]``, wherever it appears in the event
   (request headers, ``extra``, breadcrumb data, span data, tags).
2. **Pattern-based scrubbing of free text.** Strings are searched for
   payment card numbers (13-19 digits that pass the Luhn checksum), bearer/basic
   credentials and passwords embedded in URLs.

Identifier fields (event/trace/span ids, timestamps) are exempt from pattern
scrubbing: a random hex trace id contains a Luhn-valid 16-digit run often enough
that scrubbing it would corrupt traces.

The scrubber never mutates its input. It builds a new structure, so dictionaries
shared with a scope (breadcrumbs, tags) are left untouched.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

REDACTED = "[REDACTED]"

_SENSITIVE_SUBSTRINGS = (
    "password",
    "passwd",
    "secret",
    "token",
    "authorization",
    "cookie",
    "api_key",
    "apikey",
    "private_key",
    "credential",
    "session",
)
_SENSITIVE_EXACT = frozenset(
    {
        "auth",
        "pwd",
        "pin",
        "otp",
        "pan",
        "cvv",
        "cvv2",
        "cvc",
        "ssn",
        "card",
        "card_number",
        "cardnumber",
        "x_api_key",
        "csrf",
        "xsrf",
    }
)
# Schema fields whose values are identifiers or code names, never free text.
# Pattern scrubbing would corrupt identifiers (a hex trace id can contain a
# Luhn-valid digit run) and is wasted work on code names.
_NO_PATTERN_KEYS = frozenset(
    {
        "event_id",
        "trace_id",
        "span_id",
        "parent_span_id",
        "timestamp",
        "start_timestamp",
        "fingerprint",
        "release",
        "type",
        "level",
        "platform",
        "service",
        "environment",
        "op",
        "status",
        "relation",
        "module",
        "function",
        "filename",
        "abs_path",
        "method",
        "route",
        "version",
    }
)
_SENSITIVE_SUBSTRING_RE = re.compile("|".join(re.escape(part) for part in _SENSITIVE_SUBSTRINGS))
_KEY_CACHE_LIMIT = 4096

_CARD_RE = re.compile(r"(?<![\w-])(?:\d[ -]?){12,18}\d(?![\w-])")
_AUTH_RE = re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/-]{6,}=*")
_URL_CREDENTIALS_RE = re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://[^/\s:@]+):([^/\s@]+)@")


def normalize_key(key: str) -> str:
    return key.strip().lower().replace("-", "_")


def luhn_valid(digits: str) -> bool:
    """Luhn (mod 10) checksum used by every major card network."""
    total = 0
    for index, char in enumerate(reversed(digits)):
        digit = ord(char) - 48
        if index % 2 == 1:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return total % 10 == 0


def _redact_card(match: re.Match[str]) -> str:
    candidate = match.group(0)
    digits = "".join(char for char in candidate if char.isdigit())
    if 13 <= len(digits) <= 19 and luhn_valid(digits):
        return REDACTED
    return candidate


class Scrubber:
    def __init__(self, *, extra_keys: Iterable[str] = (), scrub_card_numbers: bool = True) -> None:
        self._exact = _SENSITIVE_EXACT | {normalize_key(key) for key in extra_keys}
        self._cards = scrub_card_numbers
        # Event keys repeat constantly ("filename", "lineno", ...): remember
        # decisions. Bounded, because user-supplied keys are unbounded.
        self._key_cache: dict[str, bool] = {}

    def is_sensitive_key(self, key: str) -> bool:
        cached = self._key_cache.get(key)
        if cached is not None:
            return cached
        normalized = normalize_key(key)
        result = normalized in self._exact or _SENSITIVE_SUBSTRING_RE.search(normalized) is not None
        if len(self._key_cache) < _KEY_CACHE_LIMIT:
            self._key_cache[key] = result
        return result

    def scrub_text(self, text: str) -> str:
        if self._cards and len(text) >= 13:
            text = _CARD_RE.sub(_redact_card, text)
        if "earer" in text or "asic" in text or "EARER" in text or "ASIC" in text:
            text = _AUTH_RE.sub(lambda m: f"{m.group(1)} {REDACTED}", text)
        if "://" in text and "@" in text:
            text = _URL_CREDENTIALS_RE.sub(lambda m: f"{m.group(1)}:{REDACTED}@", text)
        return text

    def scrub(self, value: Any, key: str | None = None) -> Any:
        if isinstance(value, dict):
            return {
                item_key: (
                    REDACTED
                    if isinstance(item_key, str)
                    and item_value is not None
                    and self.is_sensitive_key(item_key)
                    else self.scrub(item_value, item_key)
                )
                for item_key, item_value in value.items()
            }
        if isinstance(value, list):
            return [self.scrub(item, key) for item in value]
        if isinstance(value, str):
            if key in _NO_PATTERN_KEYS:
                return value
            return self.scrub_text(value)
        return value
