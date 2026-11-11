from __future__ import annotations

import pytest

from collector.grouping import group_event, normalize_message
from tests.helpers import error_event, frame


def fingerprint(**kwargs: object) -> str:
    grouping = group_event(error_event(**kwargs))  # type: ignore[arg-type]
    assert grouping is not None
    return grouping.fingerprint


def test_dynamic_messages_do_not_split_an_issue() -> None:
    assert fingerprint(message="Invalid payment amount: -5") == fingerprint(
        message="Invalid payment amount: -7"
    )


def test_line_number_shifts_do_not_split_an_issue() -> None:
    def frames(offset: int) -> list[dict[str, object]]:
        return [
            frame("checkout", lineno=42 + offset, context_line="    validate(amount)"),
            frame("validate", lineno=17 + offset, context_line="    raise ValueError(msg)"),
        ]

    assert fingerprint(frames=frames(0)) == fingerprint(frames=frames(30))


def test_framework_frames_do_not_matter() -> None:
    app_frames = [frame("checkout"), frame("validate")]
    old = [frame("dispatch", module="starlette.routing", in_app=False), *app_frames]
    new = [
        frame("handle", module="starlette.routing", in_app=False),
        frame("run_endpoint", module="fastapi.routing", in_app=False),
        *app_frames,
    ]
    assert fingerprint(frames=old) == fingerprint(frames=new)


def test_different_call_paths_are_different_issues() -> None:
    via_checkout = [frame("checkout"), frame("validate")]
    via_refund = [frame("refund"), frame("validate")]
    assert fingerprint(frames=via_checkout) != fingerprint(frames=via_refund)


def test_different_raise_sites_in_one_function_are_different_issues() -> None:
    amount = [frame("validate", context_line="    raise ValueError('amount')")]
    currency = [frame("validate", context_line="    raise ValueError('currency')")]
    assert fingerprint(frames=amount) != fingerprint(frames=currency)


def test_exception_type_and_cause_matter() -> None:
    assert fingerprint(exc_type="ValueError") != fingerprint(exc_type="KeyError")
    cause = {
        "id": 1,
        "parent_id": 0,
        "relation": "cause",
        "type": "TimeoutError",
        "module": "builtins",
        "message": "",
        "stacktrace": {"frames": []},
    }
    assert fingerprint() != fingerprint(extra_values=[cause])


def test_issues_are_scoped_per_service() -> None:
    assert fingerprint(service="payment-service") != fingerprint(service="fraud-service")


def test_exceptions_without_frames_fall_back_to_normalized_messages() -> None:
    assert fingerprint(frames=[], message="user 123 not found") == fingerprint(
        frames=[], message="user 456 not found"
    )
    assert fingerprint(frames=[], message="user 123 not found") != fingerprint(
        frames=[], message="card expired"
    )


def test_custom_fingerprint_overrides_everything() -> None:
    a = error_event(exc_type="ValueError")
    b = error_event(exc_type="KeyError")
    a["fingerprint"] = b["fingerprint"] = ["payment-gateway-down"]
    assert group_event(a).fingerprint == group_event(b).fingerprint  # type: ignore[union-attr]


def test_grouping_is_explainable() -> None:
    grouping = group_event(error_event())
    assert grouping is not None
    assert grouping.components[0] == "type:ValueError"
    assert "frame:app.payments:validate" in grouping.components
    assert grouping.culprit == "app.payments in validate"
    assert len(grouping.fingerprint) == 32


def test_transactions_are_not_grouped() -> None:
    assert group_event({"type": "transaction", "service": "svc"}) is None


@pytest.mark.parametrize(
    ("message", "normalized"),
    [
        ("Invalid payment amount: -5.50", "Invalid payment amount: <num>"),
        ("order 4b0d9a30-0f3c-4f2e-9a43-1d2c3b4a5f6e failed", "order <uuid> failed"),
        ("trace 4bf92f3577b34da6a3ce929d0e0e4736 broke", "trace <hex> broke"),
        ("mail to jane.doe+x@example.com bounced", "mail to <email> bounced"),
        ("expired at 2026-11-11T10:30:00Z", "expired at <timestamp>"),
        ("retry   after\n 3 attempts", "retry after <num> attempts"),
        ("http2 handshake", "http2 handshake"),
    ],
)
def test_message_normalization(message: str, normalized: str) -> None:
    assert normalize_message(message) == normalized
