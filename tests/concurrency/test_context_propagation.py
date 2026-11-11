"""Which execution boundaries carry the request context - verified, not assumed."""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading

import anyio.to_thread

from pulsetrace import PulseTrace, bind_context
from pulsetrace.context import get_isolation_scope


def current_request() -> str | None:
    scope = get_isolation_scope()
    return scope._tags.get("request") if scope is not None else None


async def test_await_and_create_task_inherit_the_scope(client: PulseTrace) -> None:
    with client.isolation_scope():
        client.set_tag("request", "A")
        await asyncio.sleep(0)
        assert current_request() == "A"
        assert await asyncio.create_task(asyncio.to_thread(current_request)) == "A"

        async def child() -> str | None:
            return current_request()

        assert await asyncio.create_task(child()) == "A"


async def test_to_thread_and_anyio_threads_inherit_the_scope(client: PulseTrace) -> None:
    with client.isolation_scope():
        client.set_tag("request", "B")
        assert await asyncio.to_thread(current_request) == "B"
        assert await anyio.to_thread.run_sync(current_request) == "B"  # FastAPI `def` endpoints


async def test_run_in_executor_does_not_and_bind_context_fixes_it(client: PulseTrace) -> None:
    loop = asyncio.get_running_loop()
    with client.isolation_scope(), concurrent.futures.ThreadPoolExecutor(1) as pool:
        client.set_tag("request", "C")
        assert await loop.run_in_executor(pool, current_request) is None
        assert await loop.run_in_executor(pool, bind_context(current_request)) == "C"


def test_plain_threads_need_bind_context(client: PulseTrace) -> None:
    results: dict[str, str | None] = {}
    with client.isolation_scope():
        client.set_tag("request", "D")
        bound = bind_context(current_request)
        threads = [
            threading.Thread(target=lambda: results.__setitem__("plain", current_request())),
            threading.Thread(target=lambda: results.__setitem__("bound", bound())),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    assert results == {"plain": None, "bound": "D"}


async def test_scope_is_reset_after_the_block(client: PulseTrace) -> None:
    assert get_isolation_scope() is None
    with client.isolation_scope():
        assert get_isolation_scope() is not None
    assert get_isolation_scope() is None


async def test_sibling_tasks_do_not_share_rebinding(client: PulseTrace) -> None:
    async def request(name: str) -> str | None:
        with client.isolation_scope():
            client.set_tag("request", name)
            await asyncio.sleep(0.001)
            return current_request()

    assert await asyncio.gather(*(request(f"r{n}") for n in range(50))) == [
        f"r{n}" for n in range(50)
    ]
