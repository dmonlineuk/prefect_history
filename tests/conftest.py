"""Shared fixtures for prefect_history tests."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from prefect_history.config import Settings
from prefect_history.db import FlowRunDB


@pytest.fixture()
def tmp_db(tmp_path):
    """Return a ``FlowRunDB`` backed by a temporary SQLite file."""
    db_path = str(tmp_path / "test.db")
    return FlowRunDB(db_path)


@pytest.fixture()
def settings(tmp_path):
    """Return a ``Settings`` instance pointing at a temp DB."""
    return Settings(
        prefect_api_url="https://api.prefect.cloud/api/accounts/test/workspaces/test",
        prefect_api_key="pnu_test_key_1234567890",
        db_path=str(tmp_path / "test.db"),
        backfill_months=2,
        page_size=50,
    )


def make_flow_run_row(
    *,
    run_id: str | None = None,
    flow_id: str | None = None,
    flow_name: str = "my-flow",
    name: str = "run-1",
    state_type: str = "COMPLETED",
    state_name: str = "Completed",
    start_time: str | None = None,
    end_time: str | None = None,
    expected_start_time: str | None = None,
) -> dict:
    """Build a minimal flow_run row dict for testing."""
    now = datetime.now(UTC).isoformat()
    return {
        "id": run_id or str(uuid4()),
        "flow_id": flow_id or str(uuid4()),
        "flow_name": flow_name,
        "name": name,
        "deployment_id": None,
        "deployment_version": None,
        "work_pool_name": None,
        "work_queue_name": None,
        "state_type": state_type,
        "state_name": state_name,
        "state_message": None,
        "start_time": start_time or now,
        "end_time": end_time or now,
        "expected_start_time": expected_start_time or now,
        "total_run_time_s": 42.0,
        "created": now,
        "updated": now,
        "tags": "[]",
        "parameters": "{}",
        "parent_task_run_id": None,
        "auto_scheduled": 0,
        "run_count": 1,
    }
