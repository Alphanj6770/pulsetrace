"""Per-request telemetry context built on :mod:`contextvars`.

Why ``ContextVar`` and not a global or a thread-local: an asyncio server runs
thousands of requests on *one* thread, interleaved at every ``await``. A global
or ``threading.local`` would be shared by all of them; a ``ContextVar`` is
per-task, because each ``asyncio.Task`` runs in its own copy of the context.

Propagation across execution boundaries (verified in
``tests/concurrency/test_context_propagation.py``):

=================================  ===============================
Boundary                           Sees the request's scope?
=================================  ===============================
``await`` inside the same task     yes
``asyncio.create_task``            yes (context copied at creation)
``asyncio.to_thread``              yes
``anyio.to_thread.run_sync``       yes (FastAPI ``def`` endpoints)
SQLAlchemy async greenlets         yes
``loop.run_in_executor``           **no** - use :func:`bind_context`
``threading.Thread``               **no** - use :func:`bind_context`
=================================  ===============================

A copied context shares the *objects* its variables point to. That is why every
request must ``set`` a new, forked :class:`~pulsetrace.scope.Scope` rather than
mutate the one it inherited, and why the variable is always reset with the
token returned by ``set`` (in a ``finally``), so a cancelled request cannot
leave its scope behind.
"""

from __future__ import annotations

import contextvars
import functools
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, ParamSpec, TypeVar

if TYPE_CHECKING:
    from pulsetrace.scope import Scope
    from pulsetrace.tracing import Span

P = ParamSpec("P")
R = TypeVar("R")

_SCOPE: contextvars.ContextVar[Scope | None] = contextvars.ContextVar(
    "pulsetrace.scope", default=None
)
_SPAN: contextvars.ContextVar[Span | None] = contextvars.ContextVar("pulsetrace.span", default=None)
# Set inside the transport worker so that the SDK's own HTTP requests are never
# instrumented (which would create telemetry about telemetry, forever).
_SUPPRESS: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "pulsetrace.suppress", default=False
)
# Set while an event is being built, so anything the pipeline triggers (a log
# line in a before_send hook, for example) cannot recursively capture events.
_CAPTURING: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "pulsetrace.capturing", default=False
)


def get_isolation_scope() -> Scope | None:
    """The scope of the current request/task, or ``None`` outside of one."""
    return _SCOPE.get()


def get_current_span() -> Span | None:
    return _SPAN.get()


def is_instrumentation_suppressed() -> bool:
    return _SUPPRESS.get()


@contextmanager
def suppress_instrumentation() -> Iterator[None]:
    """Temporarily disable automatic instrumentation in the current context."""
    token = _SUPPRESS.set(True)
    try:
        yield
    finally:
        _SUPPRESS.reset(token)


def bind_context(fn: Callable[P, R]) -> Callable[P, R]:
    """Snapshot the current context so ``fn`` runs in it, even on another thread.

    Each call runs in a fresh copy of the snapshot: a ``Context`` object cannot
    be entered by two threads at once, and copies keep concurrent calls from
    observing each other's variable changes.

    >>> loop.run_in_executor(None, bind_context(blocking_io))  # doctest: +SKIP
    """
    snapshot = contextvars.copy_context()

    @functools.wraps(fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        return snapshot.copy().run(fn, *args, **kwargs)

    return wrapper
