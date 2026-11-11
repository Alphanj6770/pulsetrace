"""Helpers for testing code instrumented with PulseTrace.

>>> transport = CapturingTransport()
>>> telemetry = PulseTrace(service_name="svc", transport=transport)
>>> telemetry.capture_message("hello")  # doctest: +ELLIPSIS
'...'
>>> transport.messages()[0]["message"]
'hello'
"""

from __future__ import annotations

import json
import threading
from typing import Any

from pulsetrace.transport.base import ShutdownReport


class CapturingTransport:
    """Keeps every submitted event in memory, decoded from its wire format.

    Decoding the exact bytes the real transport would send means tests also
    prove that every event is valid JSON.
    """

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self.low_priority: list[bool] = []
        self._lock = threading.Lock()
        self.started = False
        self.closed = False

    def submit(self, payload: bytes, *, low_priority: bool = False) -> bool:
        if self.closed:
            return False
        event = json.loads(payload)
        with self._lock:
            self.events.append(event)
            self.low_priority.append(low_priority)
        return True

    async def start(self) -> None:
        self.started = True

    async def flush(self, timeout: float) -> bool:
        return True

    async def shutdown(self, timeout: float) -> ShutdownReport:
        self.closed = True
        return ShutdownReport(delivered=len(self.events), dropped=0, unsent=0, elapsed=0.0)

    def stats(self) -> dict[str, Any]:
        return {"transport": "capturing", "captured": len(self.events)}

    def of_type(self, event_type: str) -> list[dict[str, Any]]:
        with self._lock:
            return [event for event in self.events if event["type"] == event_type]

    def errors(self) -> list[dict[str, Any]]:
        return self.of_type("error")

    def messages(self) -> list[dict[str, Any]]:
        return self.of_type("message")

    def transactions(self) -> list[dict[str, Any]]:
        return self.of_type("transaction")

    def clear(self) -> None:
        with self._lock:
            self.events.clear()
            self.low_priority.clear()
