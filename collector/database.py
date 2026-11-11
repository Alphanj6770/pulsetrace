"""Engine/session setup and the few places where SQL dialects differ.

PostgreSQL is the production target. SQLite (via aiosqlite) is supported so the
test suite and local development need no database server; ingestion uses the
same ``INSERT ... ON CONFLICT`` statements on both.
"""

from __future__ import annotations

import asyncio
from typing import Any

from sqlalchemy import event, func, text
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.sql.elements import ColumnElement

from collector.models import Base


def _sqlite_pragmas(dbapi_connection: Any, _record: Any) -> None:
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.close()


class Database:
    def __init__(self, url: str) -> None:
        parsed = make_url(url)
        self.dialect = parsed.get_backend_name()
        options: dict[str, Any] = {}
        if self.dialect == "sqlite":
            options["connect_args"] = {"timeout": 30}
        else:
            options["pool_pre_ping"] = True
        self.engine: AsyncEngine = create_async_engine(url, **options)
        if self.dialect == "sqlite":
            event.listen(self.engine.sync_engine, "connect", _sqlite_pragmas)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        # SQLite allows a single writer; serialising writes in-process avoids
        # "database is locked" errors. PostgreSQL handles concurrency itself.
        self.write_lock: asyncio.Lock | None = asyncio.Lock() if self.dialect == "sqlite" else None

    def insert(self, table: Any) -> Any:
        if self.dialect == "postgresql":
            return postgresql.insert(table)
        if self.dialect == "sqlite":
            return sqlite.insert(table)
        raise RuntimeError(f"unsupported database dialect: {self.dialect}")

    def greatest(self, a: Any, b: Any) -> ColumnElement[Any]:
        return func.greatest(a, b) if self.dialect == "postgresql" else func.max(a, b)

    def least(self, a: Any, b: Any) -> ColumnElement[Any]:
        return func.least(a, b) if self.dialect == "postgresql" else func.min(a, b)

    async def create_schema(self) -> None:
        # Production would use migrations (Alembic); create_all keeps the demo simple.
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def ping(self) -> bool:
        try:
            async with self.engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    async def dispose(self) -> None:
        await self.engine.dispose()
