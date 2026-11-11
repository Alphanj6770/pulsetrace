r"""Issue grouping: many events, one issue.

.. math:: F = H(\text{service}, \text{exception type}, \text{cause types},
                 \text{normalised in-app frames}, \text{crash line})

Design choices:

* **No line numbers.** Editing code above a function shifts its line numbers;
  that must not split an issue. Frames are identified by ``module:function``,
  plus the source text of the crashing line (which distinguishes two different
  ``raise`` statements in the same function).
* **In-app frames only** (when there are any). Upgrading FastAPI changes the
  framework frames in every traceback; that must not split every issue.
* **No exception message** when a stack trace exists. Messages carry dynamic
  values (``Invalid amount: -5`` vs ``-7``) that would explode one bug into
  thousands of issues. Messages are only used when there is nothing better
  (message events, or exceptions that were never raised), and then only after
  normalising numbers, UUIDs, hex ids, emails and timestamps.
* **SHA-256 truncated to 128 bits.** With *n* issues the collision probability
  is about :math:`n^2 / 2^{129}`, roughly :math:`10^{-27}` for a million issues.
* The components are stored with the issue, so grouping is explainable.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any

MAX_GROUPING_FRAMES = 12

_NORMALIZERS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(
            r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.IGNORECASE
        ),
        "<uuid>",
    ),
    (
        re.compile(
            r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?"
        ),
        "<timestamp>",
    ),
    (re.compile(r"\b[\w.+-]+@[\w-]+(?:\.[\w-]+)+\b"), "<email>"),
    (re.compile(r"\b(?=[0-9a-f]*\d)[0-9a-f]{12,}\b", re.IGNORECASE), "<hex>"),
    (re.compile(r"(?<![\w.])[-+]?\d+(?:\.\d+)?(?![\w.])"), "<num>"),
)
_WHITESPACE = re.compile(r"\s+")


def normalize_message(message: str) -> str:
    text = message.strip()
    for pattern, replacement in _NORMALIZERS:
        text = pattern.sub(replacement, text)
    return _WHITESPACE.sub(" ", text)[:512]


@dataclass(frozen=True, slots=True)
class Grouping:
    fingerprint: str
    components: list[str]
    title: str
    exception_type: str | None
    culprit: str | None
    message: str


def _qualified_type(value: dict[str, Any]) -> str:
    module = value.get("module")
    name = str(value.get("type") or "Exception")
    return name if module in (None, "", "builtins", "__main__") else f"{module}.{name}"


def _frame_key(frame: dict[str, Any]) -> str:
    location = frame.get("module") or frame.get("filename") or "?"
    return f"{location}:{frame.get('function') or '?'}"


def _chain_types(values: list[dict[str, Any]]) -> list[str]:
    """Types along the cause/context chain of the captured exception."""
    by_parent = {
        value.get("parent_id"): value
        for value in values
        if value.get("relation") in ("cause", "context")
    }
    types: list[str] = []
    current_id = values[0].get("id", 0)
    while current_id in by_parent and len(types) < 8:
        child = by_parent[current_id]
        types.append(_qualified_type(child))
        current_id = child.get("id")
    return types


def group_event(event: dict[str, Any]) -> Grouping | None:
    """Fingerprint an error or message event; ``None`` for transactions."""
    event_type = event.get("type")
    if event_type == "transaction":
        return None
    service = str(event.get("service", ""))
    exception = event.get("exception") or {}
    values: list[dict[str, Any]] = exception.get("values") or []
    custom = event.get("fingerprint")

    exception_type: str | None = None
    culprit: str | None = None
    if values:
        primary = values[0]
        exception_type = str(primary.get("type") or "Exception")
        message = str(primary.get("message") or "")
        title = f"{exception_type}: {message.splitlines()[0][:200]}" if message else exception_type
        frames: list[dict[str, Any]] = (primary.get("stacktrace") or {}).get("frames") or []
        selected = [frame for frame in frames if frame.get("in_app")] or frames
        selected = selected[-MAX_GROUPING_FRAMES:]
        if selected:
            crash = selected[-1]
            culprit = f"{crash.get('module') or crash.get('filename')} in {crash.get('function')}"
        components = [f"type:{_qualified_type(primary)}"]
        components += [f"caused-by:{name}" for name in _chain_types(values)]
        components += [f"frame:{_frame_key(frame)}" for frame in selected]
        if selected and selected[-1].get("context_line"):
            components.append(f"line:{str(selected[-1]['context_line']).strip()}")
        if not frames:
            components.append(f"message:{normalize_message(message)}")
    else:
        message = str(event.get("message") or "")
        title = message.splitlines()[0][:200] if message else "<empty message>"
        components = [f"message:{normalize_message(message)}"]

    if custom:
        components = ["custom", *(str(part) for part in custom)]

    digest = hashlib.sha256("\x1f".join([service, *components]).encode("utf-8")).hexdigest()[:32]
    return Grouping(
        fingerprint=digest,
        components=components,
        title=title[:512],
        exception_type=exception_type,
        culprit=culprit[:512] if culprit else None,
        message=message[:2048],
    )
