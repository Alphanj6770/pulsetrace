from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine

from pulsetrace.integrations.sqlalchemy import SqlAlchemyIntegration
from pulsetrace.testing import CapturingTransport
from tests.conftest import ClientFactory


def test_queries_become_spans_without_parameter_values(
    make_client: ClientFactory, transport: CapturingTransport
) -> None:
    engine = create_engine("sqlite://")
    client = make_client(traces_sample_rate=1.0, integrations=[SqlAlchemyIntegration(engine)])
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE payments (id INTEGER, card TEXT)"))
        with client.start_span("POST /checkout"):
            conn.execute(
                text("INSERT INTO payments (id, card)\n   VALUES (:id, :card)"),
                {"id": 1, "card": "secret-card-value"},
            )
    (event,) = transport.transactions()
    (span,) = event["transaction"]["spans"]
    assert span["op"] == "db.query"
    assert span["name"] == "INSERT INTO payments (id, card) VALUES (?, ?)"
    assert span["data"]["db.system"] == "sqlite"
    assert "secret-card-value" not in json.dumps(transport.events)
    engine.dispose()


def test_failed_queries_mark_the_span_and_leave_a_breadcrumb(
    make_client: ClientFactory, transport: CapturingTransport
) -> None:
    engine = create_engine("sqlite://")
    client = make_client(traces_sample_rate=1.0, integrations=[SqlAlchemyIntegration(engine)])
    with client.start_span("txn"), engine.connect() as conn:
        with pytest.raises(OperationalError):
            conn.execute(text("SELECT * FROM missing_table"))
        client.capture_message("after failed query")
    span = transport.transactions()[0]["transaction"]["spans"][0]
    assert span["status"] == "internal_error"
    crumb = transport.messages()[0]["breadcrumbs"][-1]
    assert crumb["category"] == "query"
    assert crumb["level"] == "error"
    assert crumb["message"] == "SELECT * FROM missing_table"
    engine.dispose()


async def test_async_engine_spans_attach_to_the_current_request(
    make_client: ClientFactory, transport: CapturingTransport, tmp_path: Path
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'db.sqlite'}")
    client = make_client(traces_sample_rate=1.0, integrations=[SqlAlchemyIntegration(engine)])
    async with client.start_span("GET /payments"), engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    await engine.dispose()
    spans = transport.transactions()[0]["transaction"]["spans"]
    assert [span["name"] for span in spans] == ["SELECT 1"]


async def test_teardown_removes_listeners(
    make_client: ClientFactory, transport: CapturingTransport
) -> None:
    engine = create_engine("sqlite://")
    client = make_client(traces_sample_rate=1.0, integrations=[SqlAlchemyIntegration(engine)])
    await client.shutdown(0.1)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    assert transport.events == []
    engine.dispose()
