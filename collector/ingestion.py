"""``POST /v1/events``: authenticate, decode, validate, store, group.

Idempotency: events are inserted with ``ON CONFLICT (event_id) DO NOTHING
RETURNING event_id``. Only the ids that come back were new, and only those
increment issue counters - so an SDK retry after a lost acknowledgement changes
nothing, and counts stay exact. Event insert and issue upsert share one
transaction.

Issue rows are upserted in sorted fingerprint order so two concurrent batches
touching the same issues always lock rows in the same order (no deadlocks).
"""

from __future__ import annotations

import hmac
import logging
import math
import random
import uuid
import zlib
from collections import defaultdict
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy import case
from sqlalchemy.exc import SQLAlchemyError

from collector.database import Database
from collector.grouping import Grouping, group_event
from collector.models import EventRecord, IssueRecord
from collector.schemas import BatchIn, EventIn, IngestResponse, RejectedEvent
from collector.settings import Settings

logger = logging.getLogger("collector.ingestion")
router = APIRouter()
_INSERT_CHUNK = 200


@dataclass(slots=True)
class PreparedEvent:
    event: EventIn
    raw: dict[str, Any]
    grouping: Grouping | None


@dataclass(slots=True)
class IngestCounters:
    requests: int = 0
    accepted: int = 0
    duplicates: int = 0
    rejected: int = 0
    rate_limited: int = 0
    unauthorized: int = 0
    storage_errors: int = 0


def authenticate(request: Request, settings: Settings) -> None:
    if not settings.api_keys:
        return
    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    # Constant-time comparison against every key: no timing oracle.
    if (
        scheme.lower() == "bearer"
        and token
        and any(hmac.compare_digest(token.encode(), key.encode()) for key in settings.api_keys)
    ):
        return
    raise HTTPException(status_code=401, detail="missing or invalid API key")


def gunzip_limited(data: bytes, limit: int) -> bytes:
    """Decompress gzip without letting a tiny "zip bomb" expand to gigabytes."""
    decompressor = zlib.decompressobj(wbits=16 + zlib.MAX_WBITS)
    try:
        output = decompressor.decompress(data, limit + 1)
    except zlib.error as exc:
        raise HTTPException(status_code=400, detail="invalid gzip body") from exc
    if len(output) > limit or decompressor.unconsumed_tail:
        raise HTTPException(status_code=413, detail="decompressed body too large")
    if not decompressor.eof:
        raise HTTPException(status_code=400, detail="truncated gzip body")
    return output


async def read_body(request: Request, settings: Settings) -> bytes:
    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > settings.max_body_bytes:
        raise HTTPException(status_code=413, detail="body too large")
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > settings.max_body_bytes:
            raise HTTPException(status_code=413, detail="body too large")
    encoding = request.headers.get("content-encoding", "identity").strip().lower()
    if encoding == "gzip":
        return gunzip_limited(bytes(body), settings.max_decompressed_bytes)
    if encoding in ("", "identity"):
        return bytes(body)
    raise HTTPException(status_code=415, detail=f"unsupported content-encoding {encoding!r}")


def prepare(batch: BatchIn) -> tuple[list[PreparedEvent], list[RejectedEvent], int]:
    """Validate each event independently. Returns (valid, rejected, in-batch duplicates)."""
    valid: list[PreparedEvent] = []
    rejected: list[RejectedEvent] = []
    seen: set[uuid.UUID] = set()
    duplicates = 0
    for index, raw in enumerate(batch.events):
        try:
            if not isinstance(raw, dict):
                raise TypeError("event must be a JSON object")
            event = EventIn.model_validate(raw)
        except (ValidationError, TypeError) as exc:
            event_id = raw.get("event_id") if isinstance(raw, dict) else None
            rejected.append(
                RejectedEvent(
                    index=index,
                    event_id=str(event_id)[:64] if event_id is not None else None,
                    error=_summarize(exc),
                )
            )
            continue
        if event.event_id in seen:
            duplicates += 1
            continue
        seen.add(event.event_id)
        valid.append(PreparedEvent(event=event, raw=raw, grouping=group_event(raw)))
    return valid, rejected, duplicates


def _summarize(exc: Exception) -> str:
    if isinstance(exc, ValidationError):
        first = exc.errors(include_url=False)[0]
        location = ".".join(str(part) for part in first.get("loc", ()))
        return f"{location}: {first.get('msg')}" if location else str(first.get("msg"))
    return str(exc)


def _row(item: PreparedEvent, received_at: datetime) -> dict[str, Any]:
    event = item.event
    request = event.request or {}
    duration: float | None
    if event.transaction is not None:
        title = event.transaction.name
        duration = event.transaction.duration_ms
    else:
        title = item.grouping.title if item.grouping else (event.message or event.type)
        raw_duration = request.get("duration_ms")
        duration = float(raw_duration) if isinstance(raw_duration, int | float) else None
    status = request.get("status_code")
    return {
        "event_id": event.event_id,
        "received_at": received_at,
        "timestamp": event.timestamp,
        "type": event.type,
        "level": event.level,
        "service": event.service,
        "environment": event.environment,
        "release": event.release,
        "trace_id": event.trace.trace_id if event.trace else None,
        "span_id": event.trace.span_id if event.trace else None,
        "fingerprint": item.grouping.fingerprint if item.grouping else None,
        "title": title[:512],
        "duration_ms": duration,
        "status_code": status if isinstance(status, int) else None,
        "payload": item.raw,
    }


async def store(db: Database, items: list[PreparedEvent]) -> tuple[int, int]:
    """Persist events; returns (newly inserted, already present)."""
    if not items:
        return 0, 0
    received_at = datetime.now(UTC)
    rows = [_row(item, received_at) for item in sorted(items, key=lambda i: i.event.event_id)]
    lock = db.write_lock if db.write_lock is not None else nullcontext()
    async with lock, db.sessions() as session, session.begin():
        inserted: set[uuid.UUID] = set()
        for start in range(0, len(rows), _INSERT_CHUNK):
            statement = (
                db.insert(EventRecord)
                .values(rows[start : start + _INSERT_CHUNK])
                .on_conflict_do_nothing(index_elements=["event_id"])
                .returning(EventRecord.event_id)
            )
            inserted.update((await session.execute(statement)).scalars())

        groups: dict[str, list[PreparedEvent]] = defaultdict(list)
        for item in items:
            if item.grouping is not None and item.event.event_id in inserted:
                groups[item.grouping.fingerprint].append(item)
        for fingerprint in sorted(groups):
            await _upsert_issue(session, db, fingerprint, groups[fingerprint])
    return len(inserted), len(items) - len(inserted)


async def _upsert_issue(
    session: Any, db: Database, fingerprint: str, items: list[PreparedEvent]
) -> None:
    first = min(items, key=lambda item: item.event.timestamp)
    last = max(items, key=lambda item: item.event.timestamp)
    grouping = first.grouping
    assert grouping is not None
    statement = db.insert(IssueRecord).values(
        fingerprint=fingerprint,
        service=first.event.service,
        environment=last.event.environment,
        title=grouping.title,
        exception_type=grouping.exception_type,
        message=grouping.message,
        culprit=grouping.culprit,
        level=last.event.level,
        first_seen=first.event.timestamp,
        last_seen=last.event.timestamp,
        occurrence_count=len(items),
        last_event_id=last.event.event_id,
        grouping=grouping.components,
    )
    excluded = statement.excluded
    newer = excluded.last_seen >= IssueRecord.last_seen
    statement = statement.on_conflict_do_update(
        index_elements=[IssueRecord.fingerprint],
        set_={
            "occurrence_count": IssueRecord.occurrence_count + excluded.occurrence_count,
            "first_seen": db.least(IssueRecord.first_seen, excluded.first_seen),
            "last_seen": db.greatest(IssueRecord.last_seen, excluded.last_seen),
            "last_event_id": case((newer, excluded.last_event_id), else_=IssueRecord.last_event_id),
            "level": case((newer, excluded.level), else_=IssueRecord.level),
            "environment": case((newer, excluded.environment), else_=IssueRecord.environment),
        },
    )
    await session.execute(statement)


@router.post("/v1/events", response_model=IngestResponse)
async def ingest(request: Request) -> Any:
    state = request.app.state
    settings: Settings = state.settings
    counters: IngestCounters = state.counters
    counters.requests += 1
    try:
        authenticate(request, settings)
    except HTTPException:
        counters.unauthorized += 1
        raise
    if settings.failure_rate and random.random() < settings.failure_rate:
        return JSONResponse({"detail": "injected failure"}, status_code=503)

    raw = await read_body(request, settings)
    try:
        batch = BatchIn.model_validate_json(raw)
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=f"invalid batch: {_summarize(exc)}") from exc
    if len(batch.events) > settings.max_events_per_batch:
        raise HTTPException(status_code=413, detail="too many events in one batch")

    bucket = state.bucket
    if bucket is not None:
        wait = bucket.try_acquire(len(batch.events))
        if wait is not None:
            counters.rate_limited += 1
            return JSONResponse(
                {"detail": "rate limited"},
                status_code=429,
                headers={"Retry-After": str(max(1, math.ceil(wait)))},
            )

    valid, rejected, in_batch_duplicates = prepare(batch)
    try:
        accepted, duplicates = await store(state.db, valid)
    except (SQLAlchemyError, OSError):
        counters.storage_errors += 1
        logger.exception("failed to store a batch")
        # 503 + Retry-After: the SDK keeps the batch and retries later.
        return JSONResponse(
            {"detail": "storage unavailable"}, status_code=503, headers={"Retry-After": "5"}
        )
    duplicates += in_batch_duplicates
    counters.accepted += accepted
    counters.duplicates += duplicates
    counters.rejected += len(rejected)
    return IngestResponse(
        accepted=accepted, duplicates=duplicates, rejected=len(rejected), errors=rejected[:20]
    )
