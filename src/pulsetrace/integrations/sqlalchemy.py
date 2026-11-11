"""SQLAlchemy query instrumentation (sync and async engines).

Each statement executed while a span is active becomes a ``db.query`` child
span, and every statement adds a ``query`` breadcrumb, so an error event shows
which database operation preceded it. This answers "which query was slow/failed?".

**Bound parameter values are never recorded** - they are where card numbers,
emails and passwords live. Only the statement text (whitespace-collapsed and
truncated) is kept, and it still passes through the scrubber like any other
string, in case an application inlines literals.

Async engines run their DBAPI calls inside greenlets; SQLAlchemy propagates the
current ``contextvars`` context into them, so spans attach to the right request
(verified in ``tests/integrations/test_sqlalchemy.py``).
"""

from __future__ import annotations

import re
import time
from typing import TYPE_CHECKING, Any

from sqlalchemy import event
from sqlalchemy.engine import Engine

from pulsetrace.context import get_current_span, is_instrumentation_suppressed
from pulsetrace.integrations import Integration
from pulsetrace.utils import truncate

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection
    from sqlalchemy.engine.interfaces import ExceptionContext
    from sqlalchemy.ext.asyncio import AsyncEngine

    from pulsetrace.client import PulseTrace
    from pulsetrace.tracing import Span

_WHITESPACE = re.compile(r"\s+")
_STACK_KEY = "pulsetrace.query_stack"


class SqlAlchemyIntegration(Integration):
    identifier = "sqlalchemy"

    def __init__(
        self,
        engine: Engine | AsyncEngine | None = None,
        *,
        max_statement_length: int = 1000,
        record_breadcrumbs: bool = True,
    ) -> None:
        """Instrument ``engine``, or every engine in the process if omitted."""
        self._engine = engine
        self._max_statement_length = max_statement_length
        self._record_breadcrumbs = record_breadcrumbs
        self._client: PulseTrace | None = None
        self._target: Any = None

    def setup(self, client: PulseTrace) -> None:
        self._client = client
        engine = self._engine
        target: Any = Engine if engine is None else getattr(engine, "sync_engine", engine)
        event.listen(target, "before_cursor_execute", self._before)
        event.listen(target, "after_cursor_execute", self._after)
        event.listen(target, "handle_error", self._on_error)
        self._target = target

    def teardown(self) -> None:
        target = self._target
        if target is None:
            return
        for name, fn in (
            ("before_cursor_execute", self._before),
            ("after_cursor_execute", self._after),
            ("handle_error", self._on_error),
        ):
            if event.contains(target, name, fn):
                event.remove(target, name, fn)
        self._target = None

    def _statement(self, statement: str) -> str:
        return truncate(_WHITESPACE.sub(" ", statement).strip(), self._max_statement_length)

    def _before(
        self,
        conn: Connection,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        client = self._client
        if client is None or not client.enabled or is_instrumentation_suppressed():
            return
        try:
            text = self._statement(statement)
            parent = get_current_span()
            span = None
            if parent is not None:
                span = parent.start_child(text, op="db.query")
                span.set_data("db.system", conn.dialect.name)
                if executemany:
                    span.set_data("db.executemany", True)
            conn.info.setdefault(_STACK_KEY, []).append((span, time.perf_counter_ns(), text))
        except Exception:
            pass

    def _pop(self, conn: Connection) -> tuple[Span | None, int, str] | None:
        stack = conn.info.get(_STACK_KEY)
        if not stack:
            return None
        result: tuple[Span | None, int, str] = stack.pop()
        return result

    def _after(
        self,
        conn: Connection,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        try:
            entry = self._pop(conn)
            if entry is None:
                return
            span, start_ns, text = entry
            rowcount = getattr(cursor, "rowcount", -1)
            if span is not None:
                if isinstance(rowcount, int) and rowcount >= 0:
                    span.set_data("db.rowcount", rowcount)
                span.finish()
            self._breadcrumb(text, start_ns, "info")
        except Exception:
            pass

    def _on_error(self, exception_context: ExceptionContext) -> None:
        try:
            conn = exception_context.connection
            if conn is None:
                return
            entry = self._pop(conn)
            if entry is None:
                return
            span, start_ns, text = entry
            error = exception_context.original_exception
            if span is not None:
                span.set_status("internal_error")
                span.set_data("error.type", type(error).__name__)
                span.finish()
            self._breadcrumb(text, start_ns, "error", error=type(error).__name__)
        except Exception:
            pass

    def _breadcrumb(self, text: str, start_ns: int, level: str, error: str | None = None) -> None:
        client = self._client
        if client is None or not self._record_breadcrumbs:
            return
        data: dict[str, Any] = {"duration_ms": round((time.perf_counter_ns() - start_ns) / 1e6, 3)}
        if error is not None:
            data["error"] = error
        client.add_breadcrumb(text, category="query", type="query", level=level, data=data)
