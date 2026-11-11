"""JSON read API."""

from __future__ import annotations

import uuid
from dataclasses import asdict
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from collector import queries

router = APIRouter()


@router.get("/health")
async def health(request: Request) -> JSONResponse:
    healthy = await request.app.state.db.ping()
    return JSONResponse(
        {"status": "ok" if healthy else "degraded", "database": healthy},
        status_code=200 if healthy else 503,
    )


@router.get("/events")
async def list_events(
    request: Request,
    type: Literal["error", "message", "transaction"] | None = None,
    service: str | None = None,
    trace_id: str | None = None,
    fingerprint: str | None = None,
    limit: int = Query(50, ge=1, le=500),
) -> list[dict[str, Any]]:
    return await queries.list_events(
        request.app.state.db,
        event_type=type,
        service=service,
        trace_id=trace_id,
        fingerprint=fingerprint,
        limit=limit,
    )


@router.get("/events/{event_id}")
async def get_event(request: Request, event_id: uuid.UUID) -> dict[str, Any]:
    event = await queries.get_event(request.app.state.db, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    return event


@router.get("/issues")
async def list_issues(
    request: Request, service: str | None = None, limit: int = Query(50, ge=1, le=500)
) -> list[dict[str, Any]]:
    return await queries.list_issues(request.app.state.db, service=service, limit=limit)


@router.get("/issues/{fingerprint}")
async def get_issue(request: Request, fingerprint: str) -> dict[str, Any]:
    issue = await queries.get_issue(request.app.state.db, fingerprint)
    if issue is None:
        raise HTTPException(status_code=404, detail="issue not found")
    return issue


@router.get("/traces/{trace_id}")
async def get_trace(request: Request, trace_id: str) -> dict[str, Any]:
    trace = await queries.get_trace(request.app.state.db, trace_id.lower())
    if trace is None:
        raise HTTPException(status_code=404, detail="trace not found")
    return trace


@router.get("/stats")
async def stats(request: Request) -> dict[str, Any]:
    state = request.app.state
    return {"ingestion": asdict(state.counters), "stored": await queries.counts(state.db)}
