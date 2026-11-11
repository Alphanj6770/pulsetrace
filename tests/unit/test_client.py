from __future__ import annotations

import dataclasses
import inspect
import json
import threading
from typing import Any

import pytest

from pulsetrace import ConfigurationError, Options, PulseTrace, Scope
from pulsetrace.events import Event
from pulsetrace.testing import CapturingTransport
from tests.conftest import ClientFactory


def _fail(message: str = "Payment processing failed") -> None:
    raise ValueError(message)


def test_constructor_exposes_every_option() -> None:
    params = set(inspect.signature(PulseTrace.__init__).parameters)
    assert {field.name for field in dataclasses.fields(Options)} <= params


@pytest.mark.parametrize(
    "kwargs",
    [
        {"traces_sample_rate": 1.5},
        {"error_sample_rate": -0.1},
        {"queue_size": 0},
        {"flush_interval": 0},
        {"max_retries": -1},
        {"endpoint": "localhost:9000"},
        {"overflow_policy": "block"},
        {"initial_backoff": 10, "max_backoff": 1},
    ],
)
def test_invalid_configuration_fails_fast(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ConfigurationError):
        PulseTrace(service_name="svc", **kwargs)


def test_environment_variables_provide_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PULSETRACE_ENDPOINT", "http://collector:9000/v1/events")
    monkeypatch.setenv("PULSETRACE_SERVICE_NAME", "from-env")
    monkeypatch.setenv("PULSETRACE_ENVIRONMENT", "staging")
    client = PulseTrace()
    assert client.options.endpoint == "http://collector:9000/v1/events"
    assert client.options.service_name == "from-env"
    assert client.options.environment == "staging"
    assert client.enabled


def test_without_endpoint_the_client_is_a_noop() -> None:
    client = PulseTrace(service_name="svc")
    assert not client.enabled
    assert client.capture_message("hello") is None


def test_capture_exception_builds_a_complete_event(
    client: PulseTrace, transport: CapturingTransport
) -> None:
    try:
        _fail()
    except ValueError:
        event_id = client.capture_exception()

    (event,) = transport.errors()
    assert event["event_id"] == event_id
    assert event["schema_version"] == 1
    assert event["service"] == "test-service"
    assert event["environment"] == "test"
    assert event["release"] == "1.0.0"
    assert event["timestamp"].endswith("Z")
    assert event["exception"]["mechanism"] == {"type": "manual", "handled": True}
    assert event["exception"]["values"][0]["message"] == "Payment processing failed"
    assert event["sdk"]["name"] == "pulsetrace.python"


def test_capture_exception_outside_except_returns_none(client: PulseTrace) -> None:
    assert client.capture_exception() is None


def test_capture_message_levels(client: PulseTrace, transport: CapturingTransport) -> None:
    client.capture_message("disk almost full", "warn")
    client.capture_message("payment failed", "critical")
    assert [event["level"] for event in transport.messages()] == ["warning", "fatal"]


def test_capture_never_raises_even_if_the_transport_does(make_client: ClientFactory) -> None:
    class ExplodingTransport(CapturingTransport):
        def submit(self, payload: bytes, *, low_priority: bool = False) -> bool:
            raise RuntimeError("transport bug")

    client = make_client(transport=ExplodingTransport())
    assert client.capture_message("hello") is None
    assert client.stats()["events"]["internal_errors"] == 1


def test_scope_data_is_attached(client: PulseTrace, transport: CapturingTransport) -> None:
    with client.isolation_scope():
        client.set_tag("region", "ap-south-1")
        client.set_user({"id": "user-1"})
        client.set_extra("cart", {"items": 3})
        client.add_breadcrumb("Checkout initiated", category="checkout")
        client.capture_message("boom", "error", tags={"attempt": 2})
    (event,) = transport.messages()
    assert event["tags"] == {"region": "ap-south-1", "attempt": "2"}
    assert event["user"] == {"id": "user-1"}
    assert event["extra"] == {"cart": {"items": 3}}
    assert event["breadcrumbs"][0]["message"] == "Checkout initiated"


def test_captured_values_are_detached_copies(
    client: PulseTrace, transport: CapturingTransport
) -> None:
    order: dict[str, Any] = {"total": 10}
    client.capture_message("m", extra={"order": order})
    order["total"] = 99
    assert transport.messages()[0]["extra"]["order"] == {"total": 10}


def test_before_send_can_modify_or_drop(
    make_client: ClientFactory, transport: CapturingTransport
) -> None:
    def hook(event: Event) -> Event | None:
        if event.get("message") == "drop me":
            return None
        event["tags"] = {"edited": "yes"}
        return event

    client = make_client(before_send=hook)
    client.capture_message("keep me")
    client.capture_message("drop me")
    assert [event["message"] for event in transport.events] == ["keep me"]
    assert transport.events[0]["tags"] == {"edited": "yes"}
    assert client.stats()["events"]["dropped_before_send"] == 1


def test_before_send_errors_drop_the_event(
    make_client: ClientFactory, transport: CapturingTransport
) -> None:
    def hook(event: Event) -> Event | None:
        raise RuntimeError("buggy hook")

    client = make_client(before_send=hook)
    assert client.capture_message("m") is None
    assert transport.events == []
    assert client.stats()["events"]["before_send_errors"] == 1


def test_before_send_sees_already_scrubbed_data(make_client: ClientFactory) -> None:
    seen: list[Event] = []

    def hook(event: Event) -> Event | None:
        seen.append(event)
        return event

    client = make_client(before_send=hook)
    client.capture_message("m", extra={"password": "hunter2"})
    assert seen[0]["extra"]["password"] == "[REDACTED]"


def test_error_sample_rate_zero_drops_errors(
    make_client: ClientFactory, transport: CapturingTransport
) -> None:
    client = make_client(error_sample_rate=0.0)
    try:
        _fail()
    except ValueError as exc:
        assert client.capture_exception(exc) is None
    assert transport.events == []
    assert client.stats()["events"]["sampled_out"] == 1


def test_trace_sampling_does_not_suppress_errors(
    make_client: ClientFactory, transport: CapturingTransport
) -> None:
    client = make_client(traces_sample_rate=0.0)
    with client.start_span("txn") as span:
        try:
            _fail()
        except ValueError as exc:
            client.capture_exception(exc)
    (event,) = transport.events
    assert event["type"] == "error"
    assert event["trace"]["trace_id"] == span.trace_id
    assert event["trace"]["sampled"] is False


def test_the_same_exception_is_reported_once_per_request(
    client: PulseTrace, transport: CapturingTransport
) -> None:
    with client.isolation_scope():
        try:
            _fail()
        except ValueError as exc:
            assert client.capture_exception(exc) is not None
            assert client.capture_exception(exc) is None
    assert len(transport.errors()) == 1
    assert client.stats()["events"]["duplicates_suppressed"] == 1


def test_last_event_id(client: PulseTrace) -> None:
    with client.isolation_scope():
        event_id = client.capture_message("m")
        assert client.last_event_id() == event_id


def test_oversized_events_are_shrunk_then_dropped(
    make_client: ClientFactory, transport: CapturingTransport
) -> None:
    client = make_client(max_event_bytes=4_000)
    for index in range(50):
        client.add_breadcrumb("x" * 200, category=f"c{index}")
    assert client.capture_message("fits once breadcrumbs go") is not None
    assert "breadcrumbs" not in transport.messages()[0]
    assert client.capture_message("y" * 8000) is None
    assert client.stats()["events"]["dropped_oversized"] == 1


def test_events_are_valid_json_even_with_hostile_values(
    client: PulseTrace, transport: CapturingTransport
) -> None:
    client.capture_message("\ud800 lone surrogate", extra={"nan": float("nan"), "obj": object()})
    json.dumps(transport.events[0], allow_nan=False)


def test_transactions_are_low_priority(client: PulseTrace, transport: CapturingTransport) -> None:
    with client.start_span("txn"):
        client.capture_message("m")
    assert transport.low_priority == [False, True]


async def test_shutdown_is_idempotent_and_stops_capture(client: PulseTrace) -> None:
    first = await client.shutdown()
    assert await client.shutdown() is first
    assert not client.enabled
    assert client.capture_message("late") is None


def test_scope_fork_isolates_changes() -> None:
    parent = Scope(root=False)
    parent.set_tag("a", 1)
    child = parent.fork()
    child.set_tag("b", 2)
    parent.set_tag("c", 3)
    assert set(child._tags) == {"a", "b"}
    assert set(parent._tags) == {"a", "c"}


def test_request_scopes_do_not_inherit_root_breadcrumbs() -> None:
    root = Scope(root=True)
    root.add_breadcrumb(message="startup", category="boot", level="info")
    request = root.fork()
    assert len(request._breadcrumbs) == 0
    request.add_breadcrumb(message="step", category="x", level="info")
    assert len(request.fork()._breadcrumbs) == 1


def test_breadcrumbs_are_bounded() -> None:
    scope = Scope(max_breadcrumbs=10)
    for index in range(1000):
        scope.add_breadcrumb(message=str(index), category="c", level="info")
    event: Any = {}
    scope.apply(event)
    assert [crumb["message"] for crumb in event["breadcrumbs"]] == [
        str(n) for n in range(990, 1000)
    ]


def test_scope_is_safe_under_concurrent_mutation() -> None:
    scope = Scope(max_breadcrumbs=50)
    errors: list[BaseException] = []

    def writer() -> None:
        for index in range(2000):
            scope.add_breadcrumb(message=str(index), category="c", level="info")
            scope.set_tag(f"t{index % 5}", index)

    def reader() -> None:
        try:
            for _ in range(500):
                event: Any = {}
                scope.apply(event)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=writer) for _ in range(4)] + [
        threading.Thread(target=reader) for _ in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
