"""Turn arbitrary user values into bounded, JSON-safe copies.

``normalize`` is applied to everything a user hands the SDK (``extra``, tags,
breadcrumb data, span data, user info). It returns a *copy*, so:

* later mutation of the user's object never changes an already-captured event;
* the SDK never keeps a reference to user objects inside its queue;
* the result always serialises with ``json.dumps(..., allow_nan=False)``.

Only built-in containers (and their subclasses) are traversed. Other objects,
including generators and custom ``Mapping`` implementations, are summarised
with a bounded ``repr`` instead of being iterated: iterating them could consume
the user's iterator or trigger lazy loading, which would change program
behaviour.
"""

from __future__ import annotations

import datetime as _dt
import math
import uuid
from collections import deque
from decimal import Decimal
from enum import Enum
from typing import Any

from pulsetrace.utils import safe_repr, safe_str, truncate, type_name

JSONValue = Any  # recursive JSON types are not expressible precisely without a lot of noise

_MAX_INT_BITS = 64


def normalize(
    value: object,
    *,
    max_depth: int = 8,
    max_items: int = 100,
    max_string: int = 1024,
) -> JSONValue:
    try:
        return _normalize(value, 0, max_depth, max_items, max_string, set())
    except Exception:
        return f"<unserializable {type_name(value)}>"


def _normalize(
    value: object,
    depth: int,
    max_depth: int,
    max_items: int,
    max_string: int,
    active: set[int],
) -> JSONValue:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        return truncate(value, max_string)
    if isinstance(value, int):
        # int -> str conversion of enormous ints is slow and can raise
        # (sys.set_int_max_str_digits), so summarise them instead.
        if value.bit_length() > _MAX_INT_BITS:
            return f"<int with {value.bit_length()} bits>"
        return int(value)
    if isinstance(value, float):
        return float(value) if math.isfinite(value) else repr(float(value))
    if isinstance(value, bytes | bytearray | memoryview):
        return f"<{type(value).__name__} len={len(value)}>"
    if isinstance(value, _dt.datetime | _dt.date | _dt.time):
        return value.isoformat()
    if isinstance(value, _dt.timedelta):
        return value.total_seconds()
    if isinstance(value, uuid.UUID | Decimal):
        return str(value)
    if isinstance(value, Enum):
        return safe_str(value, max_string)

    if depth >= max_depth:
        return "[MaxDepth]"

    if isinstance(value, dict):
        oid = id(value)
        if oid in active:
            return "[Circular]"
        active.add(oid)
        try:
            out: dict[str, JSONValue] = {}
            for index, (key, item) in enumerate(value.items()):
                if index >= max_items:
                    out["[truncated]"] = f"{len(value) - max_items} more items"
                    break
                out[_key(key)] = _normalize(
                    item, depth + 1, max_depth, max_items, max_string, active
                )
            return out
        finally:
            active.discard(oid)

    if isinstance(value, list | tuple | set | frozenset | deque):
        oid = id(value)
        if oid in active:
            return "[Circular]"
        active.add(oid)
        try:
            items: list[JSONValue] = []
            for index, item in enumerate(value):
                if index >= max_items:
                    items.append(f"[{len(value) - max_items} more items]")
                    break
                items.append(_normalize(item, depth + 1, max_depth, max_items, max_string, active))
            return items
        finally:
            active.discard(oid)

    return safe_repr(value, max_string)


def _key(key: object) -> str:
    if isinstance(key, str):
        return truncate(key, 256)
    return safe_str(key, 256)
