"""Read-side queries shared by the JSON API and the HTML viewer."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import func, select

from collector.database import Database
from collector.models import EventRecord, IssueRecord


def event_summary(record: EventRecord) -> dict[str, Any]:
    return {
        "event_id": str(record.event_id),
        "timestamp": record.timestamp.isoformat(),
        "received_at": record.received_at.isoformat(),
        "type": record.type,
        "level": record.level,
        "service": record.service,
        "environment": record.environment,
        "release": record.release,
        "title": record.title,
        "trace_id": record.trace_id,
        "span_id": record.span_id,
        "fingerprint": record.fingerprint,
        "duration_ms": record.duration_ms,
        "status_code": record.status_code,
    }


def issue_summary(record: IssueRecord) -> dict[str, Any]:
    return {
        "fingerprint": record.fingerprint,
        "service": record.service,
        "environment": record.environment,
        "title": record.title,
        "exception_type": record.exception_type,
        "message": record.message,
        "culprit": record.culprit,
        "level": record.level,
        "first_seen": record.first_seen.isoformat(),
        "last_seen": record.last_seen.isoformat(),
        "occurrence_count": record.occurrence_count,
        "last_event_id": str(record.last_event_id),
        "grouping": record.grouping,
    }


async def list_events(
    db: Database,
    *,
    event_type: str | None = None,
    service: str | None = None,
    trace_id: str | None = None,
    fingerprint: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    statement = select(EventRecord).order_by(EventRecord.timestamp.desc()).limit(limit)
    if event_type:
        statement = statement.where(EventRecord.type == event_type)
    if service:
        statement = statement.where(EventRecord.service == service)
    if trace_id:
        statement = statement.where(EventRecord.trace_id == trace_id)
    if fingerprint:
        statement = statement.where(EventRecord.fingerprint == fingerprint)
    async with db.sessions() as session:
        records = (await session.execute(statement)).scalars().all()
    return [event_summary(record) for record in records]


async def get_event(db: Database, event_id: uuid.UUID) -> dict[str, Any] | None:
    async with db.sessions() as session:
        record = await session.get(EventRecord, event_id)
    if record is None:
        return None
    return {**event_summary(record), "payload": record.payload}


async def list_issues(
    db: Database, *, service: str | None = None, limit: int = 50
) -> list[dict[str, Any]]:
    statement = select(IssueRecord).order_by(IssueRecord.last_seen.desc()).limit(limit)
    if service:
        statement = statement.where(IssueRecord.service == service)
    async with db.sessions() as session:
        records = (await session.execute(statement)).scalars().all()
    return [issue_summary(record) for record in records]


async def get_issue(db: Database, fingerprint: str) -> dict[str, Any] | None:
    async with db.sessions() as session:
        record = await session.get(IssueRecord, fingerprint)
        if record is None:
            return None
        latest = await session.get(EventRecord, record.last_event_id)
    recent = await list_events(db, fingerprint=fingerprint, limit=20)
    return {
        **issue_summary(record),
        "latest_event": {**event_summary(latest), "payload": latest.payload} if latest else None,
        "recent_events": recent,
    }


async def counts(db: Database) -> dict[str, int]:
    async with db.sessions() as session:
        events = (await session.execute(select(func.count()).select_from(EventRecord))).scalar_one()
        issues = (await session.execute(select(func.count()).select_from(IssueRecord))).scalar_one()
    return {"events": int(events), "issues": int(issues)}


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


async def get_trace(db: Database, trace_id: str) -> dict[str, Any] | None:
    """Stitch every service's transaction for ``trace_id`` into one span tree."""
    async with db.sessions() as session:
        records = (
            (await session.execute(select(EventRecord).where(EventRecord.trace_id == trace_id)))
            .scalars()
            .all()
        )
    if not records:
        return None

    nodes: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, Any]] = []
    for record in records:
        payload = record.payload
        if record.type == "transaction":
            transaction = payload["transaction"]
            trace = payload["trace"]
            nodes[trace["span_id"]] = {
                "span_id": trace["span_id"],
                "parent_span_id": trace.get("parent_span_id"),
                "name": transaction["name"],
                "op": transaction["op"],
                "service": record.service,
                "start": _parse_ts(transaction["start_timestamp"]),
                "duration_ms": float(transaction["duration_ms"]),
                "status": transaction.get("status", "ok"),
                "event_id": str(record.event_id),
                "is_transaction": True,
            }
            for span in transaction.get("spans", []):
                nodes[span["span_id"]] = {
                    "span_id": span["span_id"],
                    "parent_span_id": span.get("parent_span_id"),
                    "name": span["name"],
                    "op": span["op"],
                    "service": record.service,
                    "start": _parse_ts(span["start_timestamp"]),
                    "duration_ms": float(span["duration_ms"]),
                    "status": span.get("status", "ok"),
                    "event_id": str(record.event_id),
                    "is_transaction": False,
                }
        elif record.type in ("error", "message"):
            errors.append(
                {
                    "event_id": str(record.event_id),
                    "title": record.title,
                    "service": record.service,
                    "span_id": record.span_id,
                    "fingerprint": record.fingerprint,
                }
            )

    children: dict[str | None, list[dict[str, Any]]] = {}
    for node in nodes.values():
        parent = node["parent_span_id"] if node["parent_span_id"] in nodes else None
        children.setdefault(parent, []).append(node)
    for siblings in children.values():
        siblings.sort(key=lambda node: node["start"])

    error_spans = {error["span_id"] for error in errors}
    starts = [node["start"] for node in nodes.values()]
    trace_start = min(starts) if starts else None
    trace_end = max(
        (node["start"].timestamp() * 1000 + node["duration_ms"] for node in nodes.values()),
        default=0.0,
    )
    rows: list[dict[str, Any]] = []

    def visit(node: dict[str, Any], depth: int) -> None:
        assert trace_start is not None
        rows.append(
            {
                **{key: value for key, value in node.items() if key != "start"},
                "start_timestamp": node["start"].isoformat(),
                "offset_ms": round((node["start"] - trace_start).total_seconds() * 1000, 3),
                "depth": depth,
                "has_error": node["span_id"] in error_spans,
            }
        )
        for child in children.get(node["span_id"], []):
            visit(child, depth + 1)

    for root in children.get(None, []):
        visit(root, 0)

    return {
        "trace_id": trace_id,
        "start_timestamp": trace_start.isoformat() if trace_start else None,
        "duration_ms": round(trace_end - trace_start.timestamp() * 1000, 3) if trace_start else 0,
        # Call order (root first), then services that only reported errors.
        "services": list(
            dict.fromkeys(
                [row["service"] for row in rows] + sorted({record.service for record in records})
            )
        ),
        "spans": rows,
        "errors": errors,
    }
