"""Real processes on real ports: collector, fraud-service, payment-service."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[2]


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class Service:
    def __init__(
        self,
        name: str,
        app: str,
        port: int,
        env: dict[str, str],
        log_dir: Path,
        *,
        factory: bool = False,
    ) -> None:
        self.name = name
        self.app = app
        self.port = port
        self.env = env
        self.factory = factory
        self.log_path = log_dir / f"{name}.log"
        self.process: subprocess.Popen[bytes] | None = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        command = [
            sys.executable,
            "-m",
            "uvicorn",
            self.app,
            "--host",
            "127.0.0.1",
            "--port",
            str(self.port),
            "--log-level",
            "warning",
        ]
        if self.factory:
            command.append("--factory")
        with self.log_path.open("ab") as log:  # the child keeps its own copy of the descriptor
            self.process = subprocess.Popen(
                command,
                cwd=ROOT,
                env={**os.environ, **self.env},
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(
                    f"{self.name} exited early:\n{self.log_path.read_text()[-3000:]}"
                )
            try:
                if httpx.get(f"{self.url}/health", timeout=1).status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
        raise RuntimeError(
            f"{self.name} did not become healthy:\n{self.log_path.read_text()[-3000:]}"
        )

    def stop(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(5)
        self.process = None


@dataclass
class Stack:
    collector: Service
    fraud: Service
    payment: Service

    def collector_get(self, path: str, **params: Any) -> Any:
        return httpx.get(f"{self.collector.url}{path}", params=params, timeout=5).json()


def wait_for(predicate: Callable[[], Any], timeout: float = 20.0, interval: float = 0.2) -> Any:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (httpx.HTTPError, KeyError, ValueError):
            pass
        time.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s (last value: {last!r})")


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Stack]:
    workdir = tmp_path_factory.mktemp("e2e")
    database_url = os.environ.get(
        "PULSETRACE_TEST_DATABASE_URL", f"sqlite+aiosqlite:///{workdir / 'collector.db'}"
    )
    collector = Service(
        "collector",
        "collector.main:create_app",
        free_port(),
        {"DATABASE_URL": database_url},
        workdir,
        factory=True,
    )
    sdk_env = {
        "PULSETRACE_ENDPOINT": f"{collector.url}/v1/events",
        "PULSETRACE_ENVIRONMENT": "e2e",
        "PULSETRACE_FLUSH_INTERVAL": "0.1",
        "PULSETRACE_MAX_RETRIES": "40",
        "PULSETRACE_INITIAL_BACKOFF": "0.1",
        "PULSETRACE_MAX_BACKOFF": "0.5",
    }
    fraud = Service("fraud", "examples.fraud_service.main:app", free_port(), sdk_env, workdir)
    payment = Service(
        "payment",
        "examples.payment_service.main:app",
        free_port(),
        {
            **sdk_env,
            "FRAUD_SERVICE_URL": fraud.url,
            "PAYMENTS_DATABASE_URL": f"sqlite+aiosqlite:///{workdir / 'payments.db'}",
        },
        workdir,
    )
    if database_url.startswith("postgresql"):
        _reset_postgres(database_url)
    started: list[Service] = []
    try:
        for service in (collector, fraud, payment):
            service.start()
            started.append(service)
        yield Stack(collector, fraud, payment)
    finally:
        for service in reversed(started):
            service.stop()


def _reset_postgres(url: str) -> None:
    import asyncio

    from sqlalchemy.ext.asyncio import create_async_engine

    from collector.models import Base

    async def reset() -> None:
        engine = create_async_engine(url)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        await engine.dispose()

    asyncio.run(reset())
