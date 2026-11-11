"""Collector application factory.

Run with ``uvicorn collector.main:create_app --factory --port 9000``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from collector import api, ingestion, ui
from collector.database import Database
from collector.ratelimit import TokenBucket
from collector.settings import Settings


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    database = Database(settings.database_url)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        await database.create_schema()
        yield
        await database.dispose()

    app = FastAPI(
        title="PulseTrace Collector",
        version="0.1.0",
        description="Minimal ingestion, deduplication and issue grouping for PulseTrace events.",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.db = database
    app.state.counters = ingestion.IngestCounters()
    app.state.bucket = (
        TokenBucket(settings.rate_limit_per_second, max(settings.rate_limit_burst, 1.0))
        if settings.rate_limit_per_second > 0
        else None
    )
    app.include_router(ingestion.router)
    app.include_router(api.router)
    app.include_router(ui.router)
    return app
