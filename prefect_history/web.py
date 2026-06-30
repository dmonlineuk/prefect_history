"""FastAPI web UI for browsing cached Prefect flow-run history."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from prefect_history.config import load_settings
from prefect_history.db import FlowRunDB

_TEMPLATES_DIR = Path(__file__).parent / "templates"

STATE_COLOURS: dict[str, str] = {
    "COMPLETED": "#22c55e",
    "RUNNING": "#06b6d4",
    "SCHEDULED": "#3b82f6",
    "PENDING": "#eab308",
    "FAILED": "#ef4444",
    "CRASHED": "#dc2626",
    "CANCELLED": "#a855f7",
    "CANCELLING": "#a855f7",
    "PAUSED": "#f59e0b",
}

_PER_PAGE = 20


def _get_db(app: FastAPI) -> FlowRunDB:
    return app.state.db


def _get_distinct_values(db: FlowRunDB) -> dict[str, list[str]]:
    """Fetch distinct state types and flow names for filter dropdowns."""
    import sqlite3

    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    try:
        states = [
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT state_type FROM flow_runs "
                "WHERE state_type IS NOT NULL ORDER BY state_type"
            )
        ]
        flows = [
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT flow_name FROM flow_runs "
                "WHERE flow_name IS NOT NULL AND flow_name != '' "
                "ORDER BY flow_name"
            )
        ]
    finally:
        conn.close()
    return {"states": states, "flows": flows}


def create_app(settings_kwargs: dict[str, Any] | None = None) -> FastAPI:
    """Build and return the FastAPI application."""
    settings_kwargs = settings_kwargs or {}
    settings = load_settings(**settings_kwargs)
    db = FlowRunDB(settings.db_path)

    app = FastAPI(title="Prefect History Viewer")
    app.state.db = db
    app.state.db_path = settings.db_path

    templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

    @app.get("/", response_class=HTMLResponse)
    async def index(
        request: Request,
        page: int = Query(1, ge=1),
        state: str | None = Query(None),
        flow: str | None = Query(None),
    ) -> HTMLResponse:
        db = _get_db(app)
        offset = (page - 1) * _PER_PAGE

        total = db.count_flow_runs(state_type=state)
        rows = db.get_all_flow_runs(
            state_type=state,
            flow_name=flow,
            limit=_PER_PAGE,
            offset=offset,
        )
        total_pages = max(1, (total + _PER_PAGE - 1) // _PER_PAGE)

        filters = _get_distinct_values(db)
        last_sync = db.last_successful_sync_time()
        in_flight = len(db.get_in_flight_run_ids())

        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "rows": rows,
                "total": total,
                "page": page,
                "total_pages": total_pages,
                "per_page": _PER_PAGE,
                "state_filter": state or "",
                "flow_filter": flow or "",
                "states": filters["states"],
                "flows": filters["flows"],
                "state_colours": STATE_COLOURS,
                "last_sync": last_sync,
                "in_flight": in_flight,
                "db_path": app.state.db_path,
            },
        )

    @app.get("/summary", response_class=HTMLResponse)
    async def summary_page(
        request: Request,
        since: str | None = Query(None),
        flow: str | None = Query(None),
    ) -> HTMLResponse:
        db = _get_db(app)
        rows = db.get_flow_summary(since=since)
        if flow:
            rows = [r for r in rows if r["flow_name"] == flow]

        filters = _get_distinct_values(db)

        return templates.TemplateResponse(
            request,
            "summary.html",
            {
                "rows": rows,
                "flows": filters["flows"],
                "flow_filter": flow or "",
                "since_filter": since or "",
            },
        )

    @app.get("/run/{run_id}", response_class=HTMLResponse)
    async def run_detail(
        request: Request,
        run_id: str,
    ) -> HTMLResponse:
        db = _get_db(app)
        row = db.get_flow_run_by_id(run_id)

        return templates.TemplateResponse(
            request,
            "run_detail.html",
            {
                "row": row,
                "run_id": run_id,
                "state_colours": STATE_COLOURS,
            },
        )

    @app.get("/rows", response_class=HTMLResponse)
    async def rows_fragment(
        request: Request,
        page: int = Query(1, ge=1),
        state: str | None = Query(None),
        flow: str | None = Query(None),
    ) -> HTMLResponse:
        """HTMX partial: just the table body + pagination."""
        db = _get_db(app)
        offset = (page - 1) * _PER_PAGE

        total = db.count_flow_runs(state_type=state)
        rows = db.get_all_flow_runs(
            state_type=state,
            flow_name=flow,
            limit=_PER_PAGE,
            offset=offset,
        )
        total_pages = max(1, (total + _PER_PAGE - 1) // _PER_PAGE)

        return templates.TemplateResponse(
            request,
            "rows.html",
            {
                "rows": rows,
                "total": total,
                "page": page,
                "total_pages": total_pages,
                "per_page": _PER_PAGE,
                "state_filter": state or "",
                "flow_filter": flow or "",
                "state_colours": STATE_COLOURS,
            },
        )

    return app
