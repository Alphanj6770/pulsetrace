from __future__ import annotations

import logging
import threading
from collections.abc import Iterator

import pytest

from pulsetrace import PulseTrace, suppress_instrumentation
from pulsetrace.events import Event
from pulsetrace.integrations.logging import LoggingIntegration
from pulsetrace.testing import CapturingTransport
from tests.conftest import ClientFactory

log = logging.getLogger("payment_service")


@pytest.fixture(autouse=True)
def info_level() -> Iterator[None]:
    previous = log.level
    log.setLevel(logging.INFO)
    yield
    log.setLevel(previous)


@pytest.fixture
def logged(make_client: ClientFactory) -> PulseTrace:
    return make_client(integrations=[LoggingIntegration()])


def test_logs_become_breadcrumbs_on_the_next_error(
    logged: PulseTrace, transport: CapturingTransport
) -> None:
    with logged.isolation_scope():
        log.info("Checkout initiated")
        log.info("Payment request created")
        log.warning("Fraud service timeout")
        try:
            raise ValueError("Payment processing failed")
        except ValueError as exc:
            logged.capture_exception(exc)

    crumbs = transport.errors()[0]["breadcrumbs"]
    assert [crumb["message"] for crumb in crumbs] == [
        "Checkout initiated",
        "Payment request created",
        "Fraud service timeout",
    ]
    assert [crumb["level"] for crumb in crumbs] == ["info", "info", "warning"]
    assert crumbs[0]["category"] == "payment_service"
    assert crumbs[0]["type"] == "log"


def test_logger_exception_becomes_an_exception_event(
    logged: PulseTrace, transport: CapturingTransport
) -> None:
    try:
        {}["card"]
    except KeyError:
        log.exception("Refund failed")
    (event,) = transport.errors()
    assert event["exception"]["values"][0]["type"] == "KeyError"
    assert event["exception"]["mechanism"]["type"] == "logging"
    assert event["extra"]["log_message"] == "Refund failed"


def test_error_logs_become_message_events_grouped_by_template(
    logged: PulseTrace, transport: CapturingTransport
) -> None:
    log.error("Charge %s declined", "pay_1")
    log.error("Charge %s declined", "pay_2")
    messages = transport.messages()
    assert [event["message"] for event in messages] == [
        "Charge pay_1 declined",
        "Charge pay_2 declined",
    ]
    assert (
        messages[0]["fingerprint"]
        == messages[1]["fingerprint"]
        == [
            "log",
            "payment_service",
            "Charge %s declined",
        ]
    )


def test_sdk_logs_never_become_telemetry(logged: PulseTrace, transport: CapturingTransport) -> None:
    logging.getLogger("pulsetrace").error("internal SDK problem")
    logging.getLogger("pulsetrace.transport").error("internal SDK problem")
    assert transport.events == []


def test_logs_from_the_transport_worker_never_become_telemetry(
    logged: PulseTrace, transport: CapturingTransport
) -> None:
    # The worker runs suppressed; httpx logs each delivery request from inside it.
    with suppress_instrumentation():
        logging.getLogger("httpx").error('HTTP Request: POST /v1/events "HTTP/1.1 503"')
    logged.capture_message("later")
    assert len(transport.events) == 1
    assert "breadcrumbs" not in transport.events[0]


def test_a_before_send_that_logs_cannot_recurse(
    make_client: ClientFactory, transport: CapturingTransport
) -> None:
    def noisy(event: Event) -> Event:
        log.error("before_send saw %s", event["event_id"])
        return event

    client = make_client(before_send=noisy, integrations=[LoggingIntegration()])
    log.error("first")
    assert len(transport.messages()) == 1
    assert client.stats()["events"]["internal_errors"] == 0


def test_bad_format_arguments_do_not_raise(
    logged: PulseTrace, transport: CapturingTransport
) -> None:
    # Called directly: pytest's own capture handler (rightly) raises on this record.
    (handler,) = [
        h for h in logging.getLogger().handlers if type(h).__name__ == "PulseTraceLogHandler"
    ]
    record = log.makeRecord(
        log.name, logging.ERROR, __file__, 1, "broken %d format", ("nan",), None
    )
    handler.handle(record)
    assert transport.messages()[0]["message"] == "broken %d format"


def test_logging_from_many_threads(logged: PulseTrace, transport: CapturingTransport) -> None:
    def work(index: int) -> None:
        for _ in range(50):
            log.info("worker %d step", index)
        log.error("worker %d failed", index)

    threads = [threading.Thread(target=work, args=(n,)) for n in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(transport.messages()) == 8


async def test_teardown_removes_the_handler(
    logged: PulseTrace, transport: CapturingTransport
) -> None:
    await logged.shutdown(0.1)
    log.error("after shutdown")
    assert transport.events == []
    assert not any(type(h).__name__ == "PulseTraceLogHandler" for h in logging.getLogger().handlers)
