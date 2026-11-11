"""Scopes: the contextual data attached to every event captured while active.

The client owns one *root* scope for process-wide data (tags such as the
region). Each request runs in an *isolation scope* forked from it and stored in
a ``ContextVar`` (see :mod:`pulsetrace.context`), so concurrent requests can
never observe each other's user, tags or breadcrumbs.

A scope can be touched from more than one thread (FastAPI runs ``def``
endpoints in a thread pool with a copy of the request context, so the loop and
a worker thread may share a scope). All mutation therefore happens under a small
lock, which also makes snapshots consistent.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable, Mapping
from typing import Any

from pulsetrace.events import BreadcrumbPayload, Event, Level, RequestPayload
from pulsetrace.serialize import normalize
from pulsetrace.utils import format_timestamp, safe_str, truncate

# (epoch seconds, type, category, level, message, data). Timestamps are kept as
# floats and only formatted when an event is built: breadcrumbs are recorded far
# more often than they are sent.
_Breadcrumb = tuple[float, str, str, Level, str | None, dict[str, Any] | None]

_MAX_TAG_KEY = 128
_MAX_TAG_VALUE = 256
_MAX_TRACKED_EXCEPTIONS = 16


class Scope:
    __slots__ = (
        "_breadcrumbs",
        "_captured",
        "_extra",
        "_fingerprint",
        "_is_root",
        "_lock",
        "_request_factory",
        "_request_overrides",
        "_tags",
        "_user",
        "last_event_id",
        "max_breadcrumbs",
    )

    def __init__(self, *, max_breadcrumbs: int = 100, root: bool = False) -> None:
        self._lock = threading.Lock()
        self._is_root = root
        self.max_breadcrumbs = max_breadcrumbs
        self._breadcrumbs: deque[_Breadcrumb] = deque(maxlen=max_breadcrumbs)
        self._tags: dict[str, str] = {}
        self._extra: dict[str, Any] = {}
        self._user: dict[str, Any] | None = None
        self._fingerprint: list[str] | None = None
        self._request_factory: Callable[[], RequestPayload] | None = None
        self._request_overrides: dict[str, Any] = {}
        # Exceptions already reported in this unit of work (strong references:
        # built-in exceptions do not support weakrefs). Only isolation scopes
        # track them, so nothing outlives the request that raised it.
        self._captured: deque[BaseException] | None = None
        self.last_event_id: str | None = None

    # -- forking ---------------------------------------------------------------

    def fork(self) -> Scope:
        """A copy whose later changes do not affect this scope (and vice versa).

        Tags, extra and user are inherited. Breadcrumbs are inherited from a
        request scope (a nested scope is part of the same timeline) but not from
        the root scope: root breadcrumbs come from unrelated concurrent work and
        would pollute every request's timeline.
        """
        child = Scope.__new__(Scope)
        child._lock = threading.Lock()
        child._is_root = False
        child.max_breadcrumbs = self.max_breadcrumbs
        child.last_event_id = None
        with self._lock:
            child._breadcrumbs = (
                deque(maxlen=self.max_breadcrumbs)
                if self._is_root
                else deque(self._breadcrumbs, maxlen=self.max_breadcrumbs)
            )
            child._tags = dict(self._tags)
            child._extra = dict(self._extra)
            child._user = dict(self._user) if self._user is not None else None
            child._fingerprint = list(self._fingerprint) if self._fingerprint is not None else None
            child._request_factory = self._request_factory
            child._request_overrides = dict(self._request_overrides)
            # Nested scopes in the same request share one dedup list, so an
            # exception captured deep inside is not reported again by the
            # middleware when it propagates out.
            child._captured = (
                self._captured
                if self._captured is not None
                else deque(maxlen=_MAX_TRACKED_EXCEPTIONS)
            )
        return child

    # -- data ------------------------------------------------------------------

    def set_tag(self, key: str, value: object) -> None:
        tag_key = truncate(safe_str(key), _MAX_TAG_KEY)
        tag_value = truncate(safe_str(value), _MAX_TAG_VALUE)
        with self._lock:
            self._tags[tag_key] = tag_value

    def set_tags(self, tags: Mapping[str, object]) -> None:
        for key, value in tags.items():
            self.set_tag(key, value)

    def remove_tag(self, key: str) -> None:
        with self._lock:
            self._tags.pop(key, None)

    def set_extra(self, key: str, value: object) -> None:
        normalized = normalize(value)
        with self._lock:
            self._extra[truncate(safe_str(key), _MAX_TAG_KEY)] = normalized

    def set_user(self, user: Mapping[str, object] | None) -> None:
        normalized = normalize(dict(user), max_depth=3) if user is not None else None
        with self._lock:
            self._user = normalized

    def set_fingerprint(self, fingerprint: list[str] | None) -> None:
        value = (
            [truncate(safe_str(part), 256) for part in fingerprint[:32]] if fingerprint else None
        )
        with self._lock:
            self._fingerprint = value

    def add_breadcrumb(
        self,
        *,
        message: str | None,
        category: str,
        level: Level,
        type: str = "default",
        data: dict[str, Any] | None = None,
        timestamp: float | None = None,
    ) -> None:
        crumb: _Breadcrumb = (
            time.time() if timestamp is None else timestamp,
            type,
            category,
            level,
            message,
            data,
        )
        with self._lock:
            self._breadcrumbs.append(crumb)

    def clear_breadcrumbs(self) -> None:
        with self._lock:
            self._breadcrumbs.clear()

    def set_request_factory(self, factory: Callable[[], RequestPayload] | None) -> None:
        """Request info is built lazily: most requests never produce an event."""
        with self._lock:
            self._request_factory = factory

    def update_request(self, **fields: Any) -> None:
        with self._lock:
            self._request_overrides.update(fields)

    # -- dedup -----------------------------------------------------------------

    def mark_captured(self, exc: BaseException) -> bool:
        """Record ``exc`` as reported; ``False`` if it already was."""
        with self._lock:
            captured = self._captured
            if captured is None:
                return True
            if any(seen is exc for seen in captured):
                return False
            captured.append(exc)
            return True

    # -- applying --------------------------------------------------------------

    def apply(self, event: Event, *, include_breadcrumbs: bool = True) -> None:
        """Merge this scope into ``event``; values already on the event win."""
        with self._lock:
            tags = dict(self._tags)
            extra = dict(self._extra)
            user = dict(self._user) if self._user is not None else None
            crumbs = list(self._breadcrumbs) if include_breadcrumbs else []
            fingerprint = list(self._fingerprint) if self._fingerprint is not None else None
            factory = self._request_factory
            overrides = dict(self._request_overrides)

        if tags:
            event["tags"] = {**tags, **event.get("tags", {})}
        if extra:
            event["extra"] = {**extra, **event.get("extra", {})}
        if user is not None and "user" not in event:
            event["user"] = user
        if fingerprint is not None and "fingerprint" not in event:
            event["fingerprint"] = fingerprint
        if crumbs:
            event["breadcrumbs"] = [_breadcrumb_payload(crumb) for crumb in crumbs]
        if factory is not None or overrides:
            request: dict[str, Any] = {}
            if factory is not None:
                try:
                    request.update(factory())
                except Exception:
                    pass  # request details are best-effort; never lose the event over them
            request.update(overrides)
            request.update(event.get("request", {}))
            event["request"] = request  # type: ignore[typeddict-item]


def _breadcrumb_payload(crumb: _Breadcrumb) -> BreadcrumbPayload:
    timestamp, kind, category, level, message, data = crumb
    payload: BreadcrumbPayload = {
        "timestamp": format_timestamp(timestamp),
        "type": kind,
        "category": category,
        "level": level,
    }
    if message is not None:
        payload["message"] = message
    if data:
        payload["data"] = data
    return payload
