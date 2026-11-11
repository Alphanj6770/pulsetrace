from __future__ import annotations

import json
import sys

import pytest

from pulsetrace.exceptions import is_in_app, serialize_exception


def _raise(exc: BaseException) -> BaseException:
    try:
        raise exc
    except BaseException as caught:
        return caught


def _recurse(depth: int) -> None:
    if depth == 0:
        raise RuntimeError("bottom")
    _recurse(depth - 1)


class PaymentError(Exception):
    pass


def test_serializes_type_message_and_frames() -> None:
    exc = _raise(ValueError("Payment processing failed"))
    values, truncated = serialize_exception(exc)

    assert not truncated
    assert len(values) == 1
    value = values[0]
    assert value["type"] == "ValueError"
    assert value["module"] == "builtins"
    assert value["message"] == "Payment processing failed"
    crash = value["stacktrace"]["frames"][-1]
    assert crash["function"] == "_raise"
    assert crash["module"] == __name__
    assert crash["in_app"] is True
    assert crash["context_line"].strip() == "raise exc"
    assert crash["filename"].endswith("test_exceptions.py")
    assert isinstance(crash["lineno"], int)
    json.dumps(values)  # plain data, no live objects


def test_explicit_cause_is_recorded_after_the_captured_exception() -> None:
    try:
        try:
            {}["card"]
        except KeyError as inner:
            raise PaymentError("charge failed") from inner
    except PaymentError as outer:
        values, _ = serialize_exception(outer)

    assert [v["type"] for v in values] == ["PaymentError", "KeyError"]
    assert values[1]["relation"] == "cause"
    assert values[1]["parent_id"] == values[0]["id"]


def test_implicit_context_is_recorded() -> None:
    try:
        try:
            1 / 0
        except ZeroDivisionError:
            raise PaymentError("while handling")  # noqa: B904 - implicit chaining is the point
    except PaymentError as exc:
        values, _ = serialize_exception(exc)

    assert [v["relation"] for v in values] == [None, "context"]


def test_suppressed_context_is_omitted() -> None:
    try:
        try:
            1 / 0
        except ZeroDivisionError:
            raise PaymentError("clean") from None
    except PaymentError as exc:
        values, _ = serialize_exception(exc)

    assert [v["type"] for v in values] == ["PaymentError"]


def test_exception_group_members_are_flattened_in_order() -> None:
    group = _raise(ExceptionGroup("batch failed", [ValueError("a"), KeyError("b")]))
    values, _ = serialize_exception(group)

    assert values[0]["is_group"] is True
    members = [v for v in values if v["relation"] == "group"]
    assert [m["type"] for m in members] == ["ValueError", "KeyError"]
    assert [m["group_index"] for m in members] == [0, 1]
    assert all(m["parent_id"] == 0 for m in members)


def test_cyclic_chain_terminates() -> None:
    a = ValueError("a")
    b = KeyError("b")
    a.__context__ = b
    b.__context__ = a
    values, _ = serialize_exception(a)
    assert [v["type"] for v in values] == ["ValueError", "KeyError"]


def test_unprintable_exception_does_not_break_capture() -> None:
    class Broken(Exception):
        def __str__(self) -> str:
            raise RuntimeError("no str for you")

    values, _ = serialize_exception(_raise(Broken()))
    assert values[0]["message"] == "<unprintable Broken object>"


def test_long_message_is_truncated() -> None:
    values, _ = serialize_exception(_raise(ValueError("x" * 50_000)), max_message_length=100)
    assert len(values[0]["message"]) == 100


def test_deep_recursion_keeps_head_and_tail_frames() -> None:
    try:
        _recurse(500)
    except RuntimeError as exc:
        values, _ = serialize_exception(exc, max_frames=20)

    stacktrace = values[0]["stacktrace"]
    assert len(stacktrace["frames"]) == 20
    assert stacktrace["frames_omitted"] > 450
    assert stacktrace["frames"][0]["function"] == "test_deep_recursion_keeps_head_and_tail_frames"
    assert stacktrace["frames"][-1]["context_line"].strip() == 'raise RuntimeError("bottom")'


def test_max_exceptions_bounds_long_chains() -> None:
    exc: BaseException = ValueError("root")
    for index in range(40):
        try:
            raise KeyError(index) from exc
        except KeyError as new:
            exc = new
    values, truncated = serialize_exception(exc, max_exceptions=5)
    assert len(values) == 5
    assert truncated is True


def test_notes_are_included() -> None:
    exc = ValueError("bad")
    exc.add_note("order_id=42")
    values, _ = serialize_exception(_raise(exc))
    assert values[0]["notes"] == ["order_id=42"]


def test_unraised_exception_has_no_frames() -> None:
    values, _ = serialize_exception(ValueError("never raised"))
    assert values[0]["stacktrace"]["frames"] == []


def test_serialization_does_not_mutate_the_exception() -> None:
    exc = _raise(ValueError("x"))
    before = (dict(exc.__dict__), exc.args, exc.__traceback__)
    serialize_exception(exc)
    assert (dict(exc.__dict__), exc.args, exc.__traceback__) == before


@pytest.mark.parametrize(
    ("module", "path", "expected"),
    [
        ("app.payments", "/srv/app/payments.py", True),
        ("httpx._client", "/venv/lib/python3.12/site-packages/httpx/_client.py", False),
        ("pulsetrace.client", "/repo/src/pulsetrace/client.py", False),
        ("json.decoder", json.__file__.replace("__init__.py", "decoder.py"), False),
        (None, "<frozen importlib._bootstrap>", False),
    ],
)
def test_in_app_detection(module: str | None, path: str, expected: bool) -> None:
    assert is_in_app(module, path) is expected


def test_in_app_include_and_exclude_override_paths() -> None:
    vendored = "/venv/lib/site-packages/mycompany/lib.py"
    assert is_in_app("mycompany.lib", vendored, include=("mycompany",)) is True
    assert is_in_app("app.legacy", "/srv/app/legacy.py", exclude=("app.legacy",)) is False


def test_serializer_survives_recursion_error() -> None:
    limit = sys.getrecursionlimit()
    try:
        _recurse(limit + 100)
    except RecursionError as exc:
        values, _ = serialize_exception(exc)
    assert values[0]["type"] == "RecursionError"
    assert len(values[0]["stacktrace"]["frames"]) <= 100
