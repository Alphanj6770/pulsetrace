"""Collector fixtures. Every test runs on SQLite, and also on PostgreSQL when
``PULSETRACE_TEST_DATABASE_URL`` is set (CI provides a postgres service)."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from collector.main import create_app
from collector.models import Base
from collector.settings import Settings

POSTGRES_URL = os.environ.get("PULSETRACE_TEST_DATABASE_URL")
BACKENDS = ["sqlite", *(["postgresql"] if POSTGRES_URL else [])]

CollectorFactory = Callable[..., Awaitable[tuple[FastAPI, httpx.AsyncClient]]]


@pytest.fixture(params=BACKENDS)
def database_url(request: pytest.FixtureRequest, tmp_path: Path) -> str:
    if request.param == "sqlite":
        return f"sqlite+aiosqlite:///{tmp_path / 'collector.db'}"
    assert POSTGRES_URL is not None
    return POSTGRES_URL


@pytest.fixture
async def make_collector(database_url: str) -> AsyncIterator[CollectorFactory]:
    apps: list[FastAPI] = []
    clients: list[httpx.AsyncClient] = []

    async def factory(**overrides: Any) -> tuple[FastAPI, httpx.AsyncClient]:
        app = create_app(Settings(database_url=database_url, **overrides))
        async with app.state.db.engine.begin() as conn:  # a clean schema for every test
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://collector"
        )
        apps.append(app)
        clients.append(client)
        return app, client

    yield factory
    for client in clients:
        await client.aclose()
    for app in apps:
        await app.state.db.dispose()


@pytest.fixture
async def collector(make_collector: CollectorFactory) -> httpx.AsyncClient:
    _, client = await make_collector()
    return client
