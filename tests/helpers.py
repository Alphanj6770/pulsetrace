"""Hand-built wire events for collector tests (where exact control over frames matters)."""

from __future__ import annotations

import uuid
from typing import Any


def frame(
    function: str,
    *,
    module: str = "app.payments",
    lineno: int = 10,
    in_app: bool = True,
    context_line: str | None = None,
) -> dict[str, Any]:
    return {
        "filename": module.replace(".", "/") + ".py",
        "abs_path": "/srv/" + module.replace(".", "/") + ".py",
        "module": module,
        "function": function,
        "lineno": lineno,
        "in_app": in_app,
        "context_line": context_line if context_line is not None else f"    {function}()",
    }


def error_event(
    *,
    message: str = "Invalid payment amount: -5",
    exc_type: str = "ValueError",
    frames: list[dict[str, Any]] | None = None,
    service: str = "payment-service",
    timestamp: str = "2026-11-11T10:30:00.000000Z",
    event_id: str | None = None,
    trace_id: str | None = None,
    span_id: str | None = None,
    extra_values: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    values = [
        {
            "id": 0,
            "parent_id": None,
            "relation": None,
            "type": exc_type,
            "module": "builtins",
            "message": message,
            "stacktrace": {
                "frames": frames
                if frames is not None
                else [
                    frame("dispatch", module="starlette.routing", in_app=False),
                    frame("checkout", lineno=42, context_line="    validate(amount)"),
                    frame("validate", lineno=17, context_line="    raise ValueError(msg)"),
                ]
            },
        },
        *(extra_values or []),
    ]
    event: dict[str, Any] = {
        "schema_version": 1,
        "event_id": event_id or uuid.uuid4().hex,
        "timestamp": timestamp,
        "type": "error",
        "level": "error",
        "platform": "python",
        "service": service,
        "environment": "test",
        "release": "1.0.0",
        "sdk": {"name": "pulsetrace.python", "version": "0.1.0"},
        "exception": {"values": values, "mechanism": {"type": "asgi", "handled": False}},
        "request": {
            "method": "POST",
            "route": "/checkout",
            "status_code": 500,
            "duration_ms": 12.5,
        },
    }
    if trace_id:
        event["trace"] = {
            "trace_id": trace_id,
            "span_id": span_id or "a" * 16,
            "parent_span_id": None,
            "sampled": True,
        }
    return event


def transaction_event(
    *,
    trace_id: str,
    span_id: str,
    parent_span_id: str | None = None,
    name: str = "POST /checkout",
    service: str = "payment-service",
    start: str = "2026-11-11T10:30:00.000000Z",
    duration_ms: float = 50.0,
    spans: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "event_id": uuid.uuid4().hex,
        "timestamp": start,
        "type": "transaction",
        "level": "info",
        "platform": "python",
        "service": service,
        "environment": "test",
        "release": "1.0.0",
        "sdk": {"name": "pulsetrace.python", "version": "0.1.0"},
        "trace": {
            "trace_id": trace_id,
            "span_id": span_id,
            "parent_span_id": parent_span_id,
            "sampled": True,
        },
        "transaction": {
            "name": name,
            "op": "http.server",
            "start_timestamp": start,
            "duration_ms": duration_ms,
            "status": "ok",
            "spans": spans or [],
        },
    }
