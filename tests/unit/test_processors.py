from __future__ import annotations

import copy

import pytest

from pulsetrace.processors import REDACTED, Scrubber, luhn_valid

scrubber = Scrubber()


@pytest.mark.parametrize(
    "key",
    [
        "Authorization",
        "authorization",
        "Cookie",
        "Set-Cookie",
        "X-API-Key",
        "x_api_key",
        "password",
        "db_password",
        "client_secret",
        "access_token",
        "session_id",
        "cvv",
        "PAN",
    ],
)
def test_sensitive_keys_are_redacted(key: str) -> None:
    assert scrubber.scrub({key: "value"}) == {key: REDACTED}


@pytest.mark.parametrize("key", ["user_agent", "content-type", "company", "amount", "route"])
def test_ordinary_keys_are_kept(key: str) -> None:
    assert scrubber.scrub({key: "value"}) == {key: "value"}


def test_redaction_applies_at_any_depth() -> None:
    event = {
        "request": {"headers": {"authorization": "Bearer abc"}},
        "extra": {"db": {"password": "x"}},
    }
    scrubbed = scrubber.scrub(event)
    assert scrubbed["request"]["headers"]["authorization"] == REDACTED
    assert scrubbed["extra"]["db"]["password"] == REDACTED


@pytest.mark.parametrize(
    "number", ["4111111111111111", "4111 1111 1111 1111", "4111-1111-1111-1111", "378282246310005"]
)
def test_luhn_valid_card_numbers_are_scrubbed_from_text(number: str) -> None:
    assert scrubber.scrub_text(f"charge failed for card {number} today") == (
        f"charge failed for card {REDACTED} today"
    )


def test_numbers_failing_luhn_are_kept() -> None:
    text = "order 4111111111111112 failed"
    assert not luhn_valid("4111111111111112")
    assert scrubber.scrub_text(text) == text


def test_identifiers_are_never_pattern_scrubbed() -> None:
    # This trace id contains a Luhn-valid 16-digit run.
    event = {"trace": {"trace_id": "4111111111111111" + "0" * 16, "span_id": "4111111111111111"}}
    assert scrubber.scrub(event) == event


def test_bearer_tokens_and_url_passwords_are_scrubbed() -> None:
    assert (
        scrubber.scrub_text("header was Bearer eyJhbGciOi.payload.sig")
        == f"header was Bearer {REDACTED}"
    )
    assert (
        scrubber.scrub_text("postgres://app:hunter2@db:5432/x")
        == f"postgres://app:{REDACTED}@db:5432/x"
    )


def test_custom_keys_and_card_scrubbing_switch() -> None:
    custom = Scrubber(extra_keys=["Merchant-Secret-Ref"], scrub_card_numbers=False)
    assert custom.scrub({"merchant_secret_ref": "x"}) == {"merchant_secret_ref": REDACTED}
    assert custom.scrub_text("4111111111111111") == "4111111111111111"


def test_scrubbing_does_not_mutate_its_input() -> None:
    event = {
        "extra": {"password": "p", "note": "card 4111111111111111"},
        "breadcrumbs": [{"data": {"token": "t"}}],
    }
    original = copy.deepcopy(event)
    scrubber.scrub(event)
    assert event == original
