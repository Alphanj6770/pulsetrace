from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import pytest

from pulsetrace import PulseTrace
from pulsetrace.testing import CapturingTransport

ClientFactory = Callable[..., PulseTrace]


@pytest.fixture
def transport() -> CapturingTransport:
    return CapturingTransport()


@pytest.fixture
def make_client(transport: CapturingTransport) -> Iterator[ClientFactory]:
    """Build clients that report into ``transport``; integrations are removed afterwards."""
    clients: list[PulseTrace] = []

    def factory(**kwargs: Any) -> PulseTrace:
        kwargs.setdefault("service_name", "test-service")
        kwargs.setdefault("environment", "test")
        kwargs.setdefault("release", "1.0.0")
        kwargs.setdefault("transport", transport)
        client = PulseTrace(**kwargs)
        clients.append(client)
        return client

    yield factory
    for client in clients:
        client._teardown_integrations()


@pytest.fixture
def client(make_client: ClientFactory) -> PulseTrace:
    return make_client(traces_sample_rate=1.0)
