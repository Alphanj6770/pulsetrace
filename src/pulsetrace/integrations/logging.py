"""Standard-library ``logging`` integration.

* Records at or above ``breadcrumb_level`` (INFO) become breadcrumbs on the
  current scope, so an error event carries the log lines that led up to it.
* Records at or above ``event_level`` (ERROR) become events: an exception event
  when the record has ``exc_info`` (``logger.exception(...)``), otherwise a
  message event grouped by the *unformatted* template (``"charge %s failed"``),
  so dynamic arguments do not split one problem into many issues.

Recursion guards: records from the ``pulsetrace`` logger are ignored, records
emitted where instrumentation is suppressed (the transport worker, whose httpx
requests are logged by httpx itself) are ignored, and a ``ContextVar``
re-entrancy flag drops any record emitted while this handler is already running.

Note: the handler only sees records that pass logger levels. Python's root
logger defaults to WARNING; configure INFO (``logging.basicConfig(level=INFO)``)
to collect INFO breadcrumbs. PulseTrace never changes logger levels itself.
"""

from __future__ import annotations

import contextvars
import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING

from pulsetrace.context import is_instrumentation_suppressed
from pulsetrace.integrations import Integration

if TYPE_CHECKING:
    from pulsetrace.client import PulseTrace

_IN_HANDLER: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "pulsetrace.logging.reentry", default=False
)
_ALWAYS_IGNORED = ("pulsetrace",)


def _level_name(levelno: int) -> str:
    if levelno >= logging.CRITICAL:
        return "fatal"
    if levelno >= logging.ERROR:
        return "error"
    if levelno >= logging.WARNING:
        return "warning"
    if levelno >= logging.INFO:
        return "info"
    return "debug"


class LoggingIntegration(Integration):
    identifier = "logging"

    def __init__(
        self,
        *,
        breadcrumb_level: int = logging.INFO,
        event_level: int | None = logging.ERROR,
        logger: logging.Logger | None = None,
        ignore_loggers: Iterable[str] = (),
    ) -> None:
        self._breadcrumb_level = breadcrumb_level
        self._event_level = event_level
        self._logger = logger
        self._ignore = (*_ALWAYS_IGNORED, *ignore_loggers)
        self._handler: PulseTraceLogHandler | None = None

    def setup(self, client: PulseTrace) -> None:
        self._handler = PulseTraceLogHandler(
            client,
            breadcrumb_level=self._breadcrumb_level,
            event_level=self._event_level,
            ignore=self._ignore,
        )
        (self._logger or logging.getLogger()).addHandler(self._handler)

    def teardown(self) -> None:
        if self._handler is not None:
            (self._logger or logging.getLogger()).removeHandler(self._handler)
            self._handler = None


class PulseTraceLogHandler(logging.Handler):
    def __init__(
        self,
        client: PulseTrace,
        *,
        breadcrumb_level: int,
        event_level: int | None,
        ignore: tuple[str, ...],
    ) -> None:
        threshold = breadcrumb_level if event_level is None else min(breadcrumb_level, event_level)
        super().__init__(level=threshold)
        self._client = client
        self._breadcrumb_level = breadcrumb_level
        self._event_level = event_level
        self._ignore = ignore

    def handle(self, record: logging.LogRecord) -> bool:
        # logging.Handler.handle() serialises every thread on one handler lock.
        # emit() below is thread-safe on its own, so skip that contention.
        result: object = self.filter(record)  # a LogRecord on Python 3.12+
        if isinstance(result, logging.LogRecord):
            record = result
        if result:
            self.emit(record)
        return bool(result)

    def _ignored(self, name: str) -> bool:
        return any(name == prefix or name.startswith(prefix + ".") for prefix in self._ignore)

    def emit(self, record: logging.LogRecord) -> None:
        # Suppressed contexts include the transport worker: httpx logs every
        # delivery request, and those records must never become telemetry.
        if _IN_HANDLER.get() or is_instrumentation_suppressed() or self._ignored(record.name):
            return
        token = _IN_HANDLER.set(True)
        try:
            try:
                message = record.getMessage()
            except Exception:
                message = str(record.msg)
            level = _level_name(record.levelno)
            if self._event_level is not None and record.levelno >= self._event_level:
                self._capture(record, message, level)
            if record.levelno >= self._breadcrumb_level:
                self._client.add_breadcrumb(
                    message,
                    category=record.name or "root",
                    level=level,
                    type="log",
                )
        except Exception:
            pass  # a logging handler must never raise into the caller
        finally:
            _IN_HANDLER.reset(token)

    def _capture(self, record: logging.LogRecord, message: str, level: str) -> None:
        extra = {"logger": record.name, "log_message": message}
        exc_info = record.exc_info
        if exc_info and exc_info[1] is not None:
            self._client.capture_exception(
                exc_info[1], level=level, extra=extra, mechanism="logging"
            )
            return
        template = record.msg if isinstance(record.msg, str) else message
        self._client.capture_message(
            message,
            level,
            extra=extra,
            fingerprint=["log", record.name, template] if record.args else None,
        )
