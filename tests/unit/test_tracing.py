from __future__ import annotations

import asyncio

import pytest

from pulsetrace import PulseTrace, TraceParent
from pulsetrace.context import get_current_span
from pulsetrace.testing import CapturingTransport
from pulsetrace.tracing import status_from_http
from tests.conftest import ClientFactory

VALID = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


def test_parse_valid_traceparent() -> None:
    parsed = TraceParent.parse(VALID)
    assert parsed == TraceParent("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", True)
    assert parsed.to_header() == VALID


def test_unsampled_flag() -> None:
    parsed = TraceParent.parse(VALID[:-2] + "00")
    assert parsed is not None
    assert parsed.sampled is False


def test_future_versions_may_append_fields() -> None:
    parsed = TraceParent.parse(
        "cc-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-what-the-future"
    )
    assert parsed is not None
    assert parsed.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"


@pytest.mark.parametrize(
    "header",
    [
        None,
        "",
        "garbage",
        VALID.upper(),  # uppercase hex is invalid
        "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",  # forbidden version
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01",  # zero trace id
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",  # zero span id
        VALID + "-extra",  # version 00 has exactly four fields
        "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01",  # short trace id
        VALID + "\n",
        "x" * 10_000,
    ],
)
def test_invalid_traceparents_are_rejected(header: str | None) -> None:
    if header == VALID + "\n":
        # Surrounding whitespace is optional whitespace per the spec.
        assert TraceParent.parse(header) is not None
        return
    assert TraceParent.parse(header) is None


@pytest.mark.parametrize(
    ("code", "status"),
    [
        (200, "ok"),
        (302, "ok"),
        (404, "not_found"),
        (429, "resource_exhausted"),
        (418, "invalid_argument"),
        (500, "internal_error"),
        (503, "unavailable"),
    ],
)
def test_http_status_mapping(code: int, status: str) -> None:
    assert status_from_http(code) == status


async def test_nested_spans_form_a_tree_and_reset_context(
    client: PulseTrace, transport: CapturingTransport
) -> None:
    assert get_current_span() is None
    async with client.start_span("job.process", op="job") as root:
        assert get_current_span() is root
        async with client.start_span("payment.authorize") as child:
            with client.start_span("db.query", op="db") as grandchild:
                assert get_current_span() is grandchild
            assert get_current_span() is child
    assert get_current_span() is None

    (event,) = transport.transactions()
    spans = {span["name"]: span for span in event["transaction"]["spans"]}
    assert event["transaction"]["name"] == "job.process"
    assert spans["payment.authorize"]["parent_span_id"] == root.span_id
    assert spans["db.query"]["parent_span_id"] == child.span_id
    assert event["trace"]["trace_id"] == root.trace_id == child.trace_id


async def test_span_marks_failure_and_never_swallows(
    client: PulseTrace, transport: CapturingTransport
) -> None:
    with pytest.raises(KeyError):
        async with client.start_span("txn"):
            with client.start_span("inner"):
                raise KeyError("boom")
    (event,) = transport.transactions()
    assert event["transaction"]["status"] == "internal_error"
    assert event["transaction"]["spans"][0]["status"] == "internal_error"


async def test_cancelled_span_is_marked_cancelled(
    client: PulseTrace, transport: CapturingTransport
) -> None:
    async def work() -> None:
        async with client.start_span("long"):
            await asyncio.sleep(10)

    task = asyncio.create_task(work())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert transport.transactions()[0]["transaction"]["status"] == "cancelled"


def test_continuing_an_incoming_trace(client: PulseTrace) -> None:
    transaction = client.start_transaction("GET /x", traceparent=VALID)
    assert transaction.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert transaction.parent_span_id == "00f067aa0ba902b7"
    assert transaction.traceparent().startswith("00-4bf92f3577b34da6a3ce929d0e0e4736-")


def test_parent_based_sampling_follows_the_upstream_flag(make_client: ClientFactory) -> None:
    client = make_client(traces_sample_rate=0.0, trace_sampling="parent_based")
    assert client.start_transaction("t", traceparent=VALID).sampled is True
    assert client.start_transaction("t", traceparent=VALID[:-2] + "00").sampled is False


def test_consistent_sampling_ignores_the_upstream_flag(make_client: ClientFactory) -> None:
    client = make_client(traces_sample_rate=0.0)
    assert client.start_transaction("t", traceparent=VALID).sampled is False


def test_unsampled_transactions_send_nothing_but_still_propagate(
    make_client: ClientFactory, transport: CapturingTransport
) -> None:
    client = make_client(traces_sample_rate=0.0)
    with client.start_span("txn") as txn, client.start_span("child") as child:
        assert child.traceparent().endswith("-00")
        assert child.trace_id == txn.trace_id
    assert transport.events == []


def test_spans_beyond_the_cap_are_counted_not_kept(
    make_client: ClientFactory, transport: CapturingTransport
) -> None:
    client = make_client(traces_sample_rate=1.0, max_spans_per_transaction=5)
    with client.start_span("txn"):
        for index in range(12):
            with client.start_span(f"child-{index}"):
                pass
    transaction = transport.transactions()[0]["transaction"]
    assert len(transaction["spans"]) == 5
    assert transaction["dropped_spans"] == 7


def test_child_offsets_are_monotonic(client: PulseTrace, transport: CapturingTransport) -> None:
    with client.start_span("txn"):
        for index in range(3):
            with client.start_span(f"step-{index}"):
                pass
    starts = [
        span["start_timestamp"] for span in transport.transactions()[0]["transaction"]["spans"]
    ]
    assert starts == sorted(starts)


def test_disabled_client_spans_are_safe_no_ops(transport: CapturingTransport) -> None:
    client = PulseTrace(
        service_name="svc", transport=transport, enabled=False, traces_sample_rate=1.0
    )
    with client.start_span("txn") as span:
        span.set_data("k", "v")
    assert transport.events == []


def test_finished_transactions_need_no_cycle_collector(client: PulseTrace) -> None:
    """Regression: a span that referenced itself made every request garbage-collector
    work, which showed up as a 3x p99 latency tail under load."""
    import gc

    gc.collect()
    gc.disable()
    try:
        for _ in range(200):
            with client.isolation_scope(), client.start_span("txn"), client.start_span("child"):
                pass
            abandoned = client.start_transaction("never finished")
            with abandoned.start_child("child"):
                pass
            del abandoned
        assert gc.collect() == 0
    finally:
        gc.enable()
