"""Exception serialisation.

``serialize_exception`` converts a live exception into plain data *at capture
time*, while its traceback frames still exist. Afterwards the SDK holds no
reference to the exception, its traceback or any frame, so captured exceptions
cannot keep request-local objects alive inside the telemetry queue.

The exception graph is flattened into a list of ``ExceptionValue``s:

* ``values[0]`` is the exception that was captured;
* explicit chaining (``raise X from Y``) is recorded with ``relation="cause"``;
* implicit chaining (raising inside ``except``) uses ``relation="context"``,
  unless it was suppressed with ``raise X from None``;
* members of an ``ExceptionGroup`` use ``relation="group"`` plus ``group_index``.

Each value links to its parent through ``parent_id``. The walk is iterative,
detects cycles (``a.__context__ = b; b.__context__ = a`` is legal Python) and is
bounded both in the number of exceptions and in the number of frames per
exception, so a ``RecursionError`` with thousands of frames still produces a
small, useful event.

``traceback.TracebackException`` implements a similar walk, but its frame
summaries do not include module names (needed for in-app detection and stable
grouping), so this module walks tracebacks directly with ``traceback.walk_tb``.
"""

from __future__ import annotations

import linecache
import os
import sysconfig
import traceback
from types import FrameType, TracebackType

from pulsetrace.events import ExceptionRelation, ExceptionValue, FramePayload, StacktracePayload
from pulsetrace.utils import safe_str, truncate

CONTEXT_LINES = 2
MAX_LINE_LENGTH = 256


def _stdlib_prefixes() -> tuple[str, ...]:
    prefixes: set[str] = set()
    paths = sysconfig.get_paths()
    for key in ("stdlib", "platstdlib"):
        path = paths.get(key)
        if path:
            prefixes.add(path.rstrip(os.sep) + os.sep)
            prefixes.add(os.path.realpath(path).rstrip(os.sep) + os.sep)
    return tuple(sorted(prefixes))


_STDLIB_PREFIXES = _stdlib_prefixes()
_THIRD_PARTY_MARKERS = (
    f"{os.sep}site-packages{os.sep}",
    f"{os.sep}dist-packages{os.sep}",
)


def _matches(module: str, prefixes: tuple[str, ...]) -> bool:
    return any(module == prefix or module.startswith(prefix + ".") for prefix in prefixes)


def is_in_app(
    module: str | None,
    abs_path: str,
    include: tuple[str, ...] = (),
    exclude: tuple[str, ...] = (),
) -> bool:
    """Whether a frame belongs to the application rather than a library."""
    if module:
        if _matches(module, include):
            return True
        if _matches(module, exclude) or _matches(module, ("pulsetrace",)):
            return False
    if not abs_path or abs_path.startswith("<"):  # <frozen ...>, <string>, <stdin>
        return False
    if any(marker in abs_path for marker in _THIRD_PARTY_MARKERS):
        return False
    return not abs_path.startswith(_STDLIB_PREFIXES)


def _short_filename(abs_path: str, module: str | None) -> str:
    """Path relative to its import root, e.g. ``payments/api.py``.

    Deriving it from the module name keeps filenames identical across machines
    that install the same code at different absolute paths (``/app`` vs
    ``/srv/app``), which keeps issue grouping stable across deployments.
    """
    if module and module != "__main__" and abs_path.endswith(".py"):
        base = module.replace(".", "/")
        candidate = base + "/__init__.py" if abs_path.endswith("__init__.py") else base + ".py"
        if abs_path.replace(os.sep, "/").endswith(candidate):
            return candidate
    return os.path.basename(abs_path) or abs_path


def _frame_payload(
    tb_frame: FrameType,
    lineno: int | None,
    include: tuple[str, ...],
    exclude: tuple[str, ...],
    checked_files: set[str],
) -> FramePayload:
    code = tb_frame.f_code
    abs_path: str = code.co_filename or "<unknown>"
    module_globals = tb_frame.f_globals
    raw_module = module_globals.get("__name__")
    module = raw_module if isinstance(raw_module, str) else None
    function = code.co_qualname or code.co_name or "<unknown>"

    frame: FramePayload = {
        "filename": _short_filename(abs_path, module),
        "abs_path": abs_path,
        "module": module,
        "function": function,
        "lineno": lineno,
        "in_app": is_in_app(module, abs_path, include, exclude),
    }

    if lineno is not None and lineno > 0 and not abs_path.startswith("<"):
        if abs_path not in checked_files:
            linecache.checkcache(abs_path)  # pick up files edited since they were cached
            checked_files.add(abs_path)
        lines = linecache.getlines(abs_path, module_globals)
        if 0 < lineno <= len(lines):
            index = lineno - 1
            frame["context_line"] = _line(lines[index])
            frame["pre_context"] = [
                _line(text) for text in lines[max(0, index - CONTEXT_LINES) : index]
            ]
            frame["post_context"] = [
                _line(text) for text in lines[index + 1 : index + 1 + CONTEXT_LINES]
            ]
    return frame


def _line(text: str) -> str:
    return truncate(text.rstrip("\r\n"), MAX_LINE_LENGTH)


def extract_stacktrace(
    tb: TracebackType | None,
    *,
    max_frames: int = 100,
    include: tuple[str, ...] = (),
    exclude: tuple[str, ...] = (),
) -> StacktracePayload:
    """Frames oldest-first. Over-long traces keep their head and their tail.

    The head shows the entry point, the tail shows where it crashed; the middle
    of a 1000-frame recursion is the least informative part.
    """
    entries = list(traceback.walk_tb(tb))
    omitted = 0
    if len(entries) > max_frames:
        head = max_frames // 2
        tail = max_frames - head
        omitted = len(entries) - max_frames
        entries = entries[:head] + entries[-tail:]
    checked: set[str] = set()
    frames = [_frame_payload(frame, lineno, include, exclude, checked) for frame, lineno in entries]
    stacktrace: StacktracePayload = {"frames": frames}
    if omitted:
        stacktrace["frames_omitted"] = omitted
    return stacktrace


def serialize_exception(
    exc: BaseException,
    *,
    max_exceptions: int = 16,
    max_frames: int = 100,
    max_message_length: int = 8192,
    in_app_include: tuple[str, ...] = (),
    in_app_exclude: tuple[str, ...] = (),
) -> tuple[list[ExceptionValue], bool]:
    """Serialise ``exc`` and everything chained to it.

    Returns ``(values, truncated)`` where ``truncated`` is true when the graph
    had more than ``max_exceptions`` members.
    """
    values: list[ExceptionValue] = []
    seen: set[int] = set()
    # Depth-first, pre-order: the captured exception, then its cause chain,
    # then (for groups) each member with its own chain.
    stack: list[tuple[BaseException, int | None, ExceptionRelation | None, int | None]] = [
        (exc, None, None, None)
    ]
    while stack and len(values) < max_exceptions:
        current, parent_id, relation, group_index = stack.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        exc_id = len(values)
        values.append(
            _serialize_one(
                current,
                exc_id,
                parent_id,
                relation,
                group_index,
                max_frames=max_frames,
                max_message_length=max_message_length,
                include=in_app_include,
                exclude=in_app_exclude,
            )
        )
        children: list[tuple[BaseException, int | None, ExceptionRelation | None, int | None]] = []
        if isinstance(current, BaseExceptionGroup):
            for index, member in enumerate(current.exceptions):
                children.append((member, exc_id, "group", index))
        cause = current.__cause__
        context = current.__context__
        if cause is not None:
            children.append((cause, exc_id, "cause", None))
        elif context is not None and not current.__suppress_context__:
            children.append((context, exc_id, "context", None))
        stack.extend(reversed(children))  # pop() then yields them in their natural order
    truncated = any(id(item[0]) not in seen for item in stack)
    return values, truncated


def _serialize_one(
    exc: BaseException,
    exc_id: int,
    parent_id: int | None,
    relation: ExceptionRelation | None,
    group_index: int | None,
    *,
    max_frames: int,
    max_message_length: int,
    include: tuple[str, ...],
    exclude: tuple[str, ...],
) -> ExceptionValue:
    exc_type = type(exc)
    value: ExceptionValue = {
        "id": exc_id,
        "parent_id": parent_id,
        "relation": relation,
        "type": exc_type.__qualname__,
        "module": exc_type.__module__,
        "message": safe_str(exc, max_message_length),
        "stacktrace": extract_stacktrace(
            exc.__traceback__, max_frames=max_frames, include=include, exclude=exclude
        ),
    }
    notes = getattr(exc, "__notes__", None)
    if isinstance(notes, list | tuple) and notes:
        value["notes"] = [safe_str(note, 1024) for note in notes[:10]]
    if isinstance(exc, BaseExceptionGroup):
        value["is_group"] = True
    if group_index is not None:
        value["group_index"] = group_index
    return value
