"""Tests for prefect_history.web (FastAPI web UI)."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from prefect_history.db import FlowRunDB
from prefect_history.web import create_app
from tests.conftest import make_flow_run_row


@pytest.fixture()
def seeded_app(tmp_path, monkeypatch):
    """Return a FastAPI app backed by a temp DB with sample data."""
    monkeypatch.setenv("PREFECT_API_URL", "https://api.test.com")
    monkeypatch.setenv("PREFECT_API_KEY", "pnu_test")

    db_path = str(tmp_path / "web_test.db")
    db = FlowRunDB(db_path)

    rows = [
        make_flow_run_row(
            run_id=f"web-{i}",
            flow_name="etl-pipeline",
            state_type="COMPLETED" if i % 3 != 0 else "FAILED",
            state_name="Completed" if i % 3 != 0 else "Failed",
            deployment_name="daily-etl",
            entrypoint="flows/etl.py:run",
            work_pool_type="kubernetes",
            parameters='{"batch_size": 100}',
        )
        for i in range(25)
    ]
    db.upsert_flow_runs(rows)

    # Record a sync so last_sync is set
    log_id = db.start_sync("backfill")
    db.finish_sync(log_id, rows_synced=25)

    app = create_app({"env_file": None, "db_path": db_path})
    return app


@pytest.fixture()
def empty_app(tmp_path, monkeypatch):
    """Return a FastAPI app with an empty DB."""
    monkeypatch.setenv("PREFECT_API_URL", "https://api.test.com")
    monkeypatch.setenv("PREFECT_API_KEY", "pnu_test")

    db_path = str(tmp_path / "empty.db")
    app = create_app({"env_file": None, "db_path": db_path})
    return app


class TestIndexPage:
    @pytest.mark.asyncio
    async def test_index_returns_html(self, seeded_app):
        transport = ASGITransport(app=seeded_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Prefect History Viewer" in resp.text

    @pytest.mark.asyncio
    async def test_index_shows_stats(self, seeded_app):
        transport = ASGITransport(app=seeded_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/")
        assert "25" in resp.text  # total runs
        assert "Last Sync" in resp.text

    @pytest.mark.asyncio
    async def test_index_shows_flow_runs(self, seeded_app):
        transport = ASGITransport(app=seeded_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/")
        assert "etl-pipeline" in resp.text

    @pytest.mark.asyncio
    async def test_index_shows_deployment_and_entrypoint(self, seeded_app):
        transport = ASGITransport(app=seeded_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/")
        assert "daily-etl" in resp.text
        assert "flows/etl.py:run" in resp.text
        assert "kubernetes" in resp.text

    @pytest.mark.asyncio
    async def test_empty_db(self, empty_app):
        transport = ASGITransport(app=empty_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/")
        assert resp.status_code == 200
        assert "No flow runs found" in resp.text


class TestStateFilter:
    @pytest.mark.asyncio
    async def test_filter_by_state(self, seeded_app):
        transport = ASGITransport(app=seeded_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/", params={"state": "FAILED"})
        assert resp.status_code == 200
        # Should only show FAILED runs (every 3rd = 9 runs: 0,3,6,9,12,15,18,21,24)
        assert "Failed" in resp.text


class TestPagination:
    @pytest.mark.asyncio
    async def test_page_2(self, seeded_app):
        transport = ASGITransport(app=seeded_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/", params={"page": 2})
        assert resp.status_code == 200
        assert "21-25 of 25" in resp.text

    @pytest.mark.asyncio
    async def test_rows_fragment(self, seeded_app):
        transport = ASGITransport(app=seeded_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/rows", params={"page": 1})
        assert resp.status_code == 200
        assert "<table>" in resp.text
        # Should not contain the full page chrome
        assert "Prefect History Viewer" not in resp.text


class TestFlowFilter:
    @pytest.mark.asyncio
    async def test_filter_by_flow(self, seeded_app):
        transport = ASGITransport(app=seeded_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/", params={"flow": "etl-pipeline"})
        assert resp.status_code == 200
        assert "etl-pipeline" in resp.text

    @pytest.mark.asyncio
    async def test_filter_nonexistent_flow(self, seeded_app):
        transport = ASGITransport(app=seeded_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/", params={"flow": "nonexistent"})
        assert resp.status_code == 200
        assert "No flow runs found" in resp.text


class TestSummaryPage:
    @pytest.mark.asyncio
    async def test_summary_returns_html(self, seeded_app):
        transport = ASGITransport(app=seeded_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/summary")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Flow Summary" in resp.text

    @pytest.mark.asyncio
    async def test_summary_shows_flow_name(self, seeded_app):
        transport = ASGITransport(app=seeded_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/summary")
        assert "etl-pipeline" in resp.text

    @pytest.mark.asyncio
    async def test_summary_shows_rag_badge(self, seeded_app):
        transport = ASGITransport(app=seeded_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/summary")
        assert "rag-badge" in resp.text

    @pytest.mark.asyncio
    async def test_summary_empty_db(self, empty_app):
        transport = ASGITransport(app=empty_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/summary")
        assert resp.status_code == 200
        assert "No flow runs found" in resp.text

    @pytest.mark.asyncio
    async def test_summary_filter_by_flow(self, seeded_app):
        transport = ASGITransport(app=seeded_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/summary", params={"flow": "nonexistent"})
        assert resp.status_code == 200
        assert "No flow runs found" in resp.text

    @pytest.mark.asyncio
    async def test_summary_navigation_link(self, seeded_app):
        transport = ASGITransport(app=seeded_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/")
        assert 'href="/summary"' in resp.text


class TestRunDetail:
    @pytest.mark.asyncio
    async def test_detail_returns_html(self, seeded_app):
        transport = ASGITransport(app=seeded_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/run/web-0")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Flow Run Detail" in resp.text

    @pytest.mark.asyncio
    async def test_detail_shows_fields(self, seeded_app):
        transport = ASGITransport(app=seeded_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/run/web-0")
        assert "etl-pipeline" in resp.text
        assert "daily-etl" in resp.text
        assert "flows/etl.py:run" in resp.text
        assert "kubernetes" in resp.text
        assert "batch_size" in resp.text

    @pytest.mark.asyncio
    async def test_detail_not_found(self, seeded_app):
        transport = ASGITransport(app=seeded_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/run/nonexistent-id")
        assert resp.status_code == 200
        assert "not found" in resp.text.lower()

    @pytest.mark.asyncio
    async def test_runs_table_links_to_detail(self, seeded_app):
        transport = ASGITransport(app=seeded_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/")
        assert "/run/web-" in resp.text


class TestIndexNavigation:
    @pytest.mark.asyncio
    async def test_index_has_nav(self, seeded_app):
        transport = ASGITransport(app=seeded_app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.get("/")
        assert 'class="nav"' in resp.text
        assert 'href="/"' in resp.text
        assert 'href="/summary"' in resp.text
