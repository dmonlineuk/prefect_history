"""Tests for prefect_history.sync."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from prefect_history.db import FlowRunDB
from prefect_history.sync import _months_ago, backfill, incremental
from tests.conftest import make_flow_run_row


class TestMonthsAgo:
    def test_returns_past_datetime(self):
        result = _months_ago(2)
        now = datetime.now(UTC)
        delta = now - result
        assert 55 <= delta.days <= 65

    def test_zero_months(self):
        result = _months_ago(0)
        now = datetime.now(UTC)
        assert abs((now - result).total_seconds()) < 2


class TestBackfill:
    def test_backfill_upserts_rows(self, settings):
        fake_rows = [make_flow_run_row(run_id=f"bf-{i}") for i in range(5)]

        with patch(
            "prefect_history.sync.fetch_flow_runs_since",
            new=AsyncMock(return_value=fake_rows),
        ):
            count = backfill(settings)

        assert count == 5
        db = FlowRunDB(settings.db_path)
        assert db.count_flow_runs() == 5
        assert db.has_prior_sync()

    def test_backfill_custom_months(self, settings):
        mock_fetch = AsyncMock(return_value=[])
        with patch(
            "prefect_history.sync.fetch_flow_runs_since",
            new=mock_fetch,
        ):
            backfill(settings, months=6)

        call_kwargs = mock_fetch.call_args.kwargs
        since = call_kwargs["since"]
        now = datetime.now(UTC)
        delta = now - since
        assert 175 <= delta.days <= 185

    def test_backfill_records_failure_on_error(self, settings):
        with patch(
            "prefect_history.sync.fetch_flow_runs_since",
            new=AsyncMock(side_effect=RuntimeError("API down")),
        ):
            with pytest.raises(RuntimeError, match="API down"):
                backfill(settings)

        db = FlowRunDB(settings.db_path)
        log = db.get_sync_log(limit=1)
        assert log[0]["status"] == "failed"
        assert db.has_prior_sync() is False


class TestIncremental:
    def test_falls_back_to_backfill_on_first_run(self, settings):
        fake_rows = [make_flow_run_row(run_id="inc-1")]

        with patch(
            "prefect_history.sync.fetch_flow_runs_since",
            new=AsyncMock(return_value=fake_rows),
        ):
            count = incremental(settings)

        assert count == 1
        db = FlowRunDB(settings.db_path)
        log = db.get_sync_log()
        assert any(e["sync_type"] == "backfill" for e in log)

    def test_incremental_fetches_new_and_rechecks_inflight(self, settings):
        db = FlowRunDB(settings.db_path)

        # Seed a prior sync + an in-flight run
        log_id = db.start_sync("backfill")
        db.finish_sync(log_id, rows_synced=1)
        inflight_row = make_flow_run_row(
            run_id="inflight-1", state_type="RUNNING", state_name="Running"
        )
        db.upsert_flow_runs([inflight_row])

        new_rows = [make_flow_run_row(run_id="new-1")]
        refreshed_rows = [
            make_flow_run_row(
                run_id="inflight-1", state_type="COMPLETED", state_name="Completed"
            )
        ]

        mock_by_ids = AsyncMock(return_value=refreshed_rows)
        with (
            patch(
                "prefect_history.sync.fetch_flow_runs_since",
                new=AsyncMock(return_value=new_rows),
            ),
            patch(
                "prefect_history.sync.fetch_flow_runs_by_ids",
                new=mock_by_ids,
            ),
        ):
            count = incremental(settings)

        # Should have called fetch_by_ids with the in-flight run ID
        mock_by_ids.assert_called_once()
        call_kwargs = mock_by_ids.call_args.kwargs
        assert "inflight-1" in call_kwargs["run_ids"]

        assert count == 2
        runs = db.get_all_flow_runs()
        inflight_updated = [r for r in runs if r["id"] == "inflight-1"]
        assert inflight_updated[0]["state_type"] == "COMPLETED"

    def test_incremental_no_inflight(self, settings):
        db = FlowRunDB(settings.db_path)
        log_id = db.start_sync("backfill")
        db.finish_sync(log_id, rows_synced=0)

        new_rows = [make_flow_run_row(run_id="new-only")]
        mock_by_ids = AsyncMock(return_value=[])

        with (
            patch(
                "prefect_history.sync.fetch_flow_runs_since",
                new=AsyncMock(return_value=new_rows),
            ),
            patch(
                "prefect_history.sync.fetch_flow_runs_by_ids",
                new=mock_by_ids,
            ),
        ):
            count = incremental(settings)

        mock_by_ids.assert_not_called()
        assert count == 1

    def test_incremental_records_failure(self, settings):
        db = FlowRunDB(settings.db_path)
        log_id = db.start_sync("backfill")
        db.finish_sync(log_id, rows_synced=0)

        with patch(
            "prefect_history.sync.fetch_flow_runs_since",
            new=AsyncMock(side_effect=RuntimeError("timeout")),
        ):
            with pytest.raises(RuntimeError, match="timeout"):
                incremental(settings)

        log = db.get_sync_log(limit=1)
        assert log[0]["status"] == "failed"
