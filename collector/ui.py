"""A deliberately small server-rendered viewer (the SDK is the product, not the dashboard).

Everything shown here is attacker-controllable input (exception messages,
headers, breadcrumbs), so templates are rendered with autoescaping on.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from jinja2 import Environment, FileSystemLoader, select_autoescape

from collector import queries

router = APIRouter(include_in_schema=False)
_templates = Environment(
    loader=FileSystemLoader(Path(__file__).parent / "templates"),
    autoescape=select_autoescape(default=True, default_for_string=True),
)


def _render(name: str, **context: Any) -> HTMLResponse:
    return HTMLResponse(_templates.get_template(name).render(**context))


def _exception_tree(payload: dict[str, Any]) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = (payload.get("exception") or {}).get("values") or []
    depth: dict[int, int] = {}
    for value in values:
        parent = value.get("parent_id")
        depth[value.get("id", 0)] = depth.get(parent, -1) + 1 if parent is not None else 0
    return [{**value, "depth": depth.get(value.get("id", 0), 0)} for value in values]


@router.get("/")
async def index() -> RedirectResponse:
    return RedirectResponse("/ui/issues")


@router.get("/ui/issues")
async def issues_page(request: Request) -> HTMLResponse:
    issues = await queries.list_issues(request.app.state.db, limit=200)
    return _render("issues.html", issues=issues)


@router.get("/ui/issues/{fingerprint}")
async def issue_page(request: Request, fingerprint: str) -> HTMLResponse:
    issue = await queries.get_issue(request.app.state.db, fingerprint)
    if issue is None:
        raise HTTPException(status_code=404, detail="issue not found")
    latest = issue["latest_event"]
    payload = latest["payload"] if latest else {}
    return _render("issue.html", issue=issue, payload=payload, exceptions=_exception_tree(payload))


@router.get("/ui/events")
async def events_page(request: Request) -> HTMLResponse:
    events = await queries.list_events(request.app.state.db, limit=200)
    return _render("events.html", events=events)


@router.get("/ui/events/{event_id}")
async def event_page(request: Request, event_id: uuid.UUID) -> HTMLResponse:
    event = await queries.get_event(request.app.state.db, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    payload = event["payload"]
    return _render("event.html", event=event, payload=payload, exceptions=_exception_tree(payload))


@router.get("/ui/traces/{trace_id}")
async def trace_page(request: Request, trace_id: str) -> HTMLResponse:
    trace = await queries.get_trace(request.app.state.db, trace_id.lower())
    if trace is None:
        raise HTTPException(status_code=404, detail="trace not found")
    return _render("trace.html", trace=trace)
