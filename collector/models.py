"""Database schema.

``events.event_id`` is the primary key, which makes ingestion idempotent: a
batch retried by the SDK after a lost acknowledgement inserts nothing new.

There is deliberately no foreign key from ``events.fingerprint`` to
``issues``: the issue row is upserted *after* we learn which events were new
(``INSERT ... ON CONFLICT DO NOTHING RETURNING``), inside the same transaction.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import JSON, BigInteger, DateTime, Dialect, Float, Index, Integer, String, Uuid
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import TypeDecorator

JSONType = JSON().with_variant(JSONB(), "postgresql")


class UTCDateTime(TypeDecorator[datetime]):
    """Timezone-aware UTC datetimes on every backend.

    PostgreSQL stores ``timestamptz``. SQLite has no timezone support, so values
    are stored as naive UTC (a fixed-width string that also sorts correctly) and
    re-labelled as UTC when read back.
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("naive datetimes are not allowed")
        value = value.astimezone(UTC)
        return value.replace(tzinfo=None) if dialect.name == "sqlite" else value

    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is not None and value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value


class Base(DeclarativeBase):
    pass


class EventRecord(Base):
    __tablename__ = "events"

    event_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    received_at: Mapped[datetime] = mapped_column(UTCDateTime)
    timestamp: Mapped[datetime] = mapped_column(UTCDateTime)
    type: Mapped[str] = mapped_column(String(16))
    level: Mapped[str] = mapped_column(String(16))
    service: Mapped[str] = mapped_column(String(200))
    environment: Mapped[str] = mapped_column(String(200))
    release: Mapped[str | None] = mapped_column(String(200))
    trace_id: Mapped[str | None] = mapped_column(String(32))
    span_id: Mapped[str | None] = mapped_column(String(16))
    fingerprint: Mapped[str | None] = mapped_column(String(64))
    title: Mapped[str] = mapped_column(String(512))
    duration_ms: Mapped[float | None] = mapped_column(Float)
    status_code: Mapped[int | None] = mapped_column(Integer)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONType)

    __table_args__ = (
        Index("ix_events_trace_id", "trace_id"),
        Index("ix_events_fingerprint_timestamp", "fingerprint", "timestamp"),
        Index("ix_events_service_type_timestamp", "service", "type", "timestamp"),
        Index("ix_events_timestamp", "timestamp"),
    )


class IssueRecord(Base):
    __tablename__ = "issues"

    fingerprint: Mapped[str] = mapped_column(String(64), primary_key=True)
    service: Mapped[str] = mapped_column(String(200))
    environment: Mapped[str] = mapped_column(String(200))
    title: Mapped[str] = mapped_column(String(512))
    exception_type: Mapped[str | None] = mapped_column(String(512))
    message: Mapped[str] = mapped_column(String(2048))
    culprit: Mapped[str | None] = mapped_column(String(512))
    level: Mapped[str] = mapped_column(String(16))
    first_seen: Mapped[datetime] = mapped_column(UTCDateTime)
    last_seen: Mapped[datetime] = mapped_column(UTCDateTime)
    occurrence_count: Mapped[int] = mapped_column(BigInteger)
    last_event_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    # The components that were hashed, so "why were these grouped?" has an answer.
    grouping: Mapped[list[str]] = mapped_column(JSONType)

    __table_args__ = (Index("ix_issues_last_seen", "last_seen"),)
