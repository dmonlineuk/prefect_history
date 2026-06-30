"""Tests for prefect_history.db."""

from __future__ import annotations

from datetime import datetime

import pytest

from prefect_history.db import FlowRunDB
from tests.conftest import make_flow_run_row


class TestSchema:
    def test_creates_tables(self, tmp_db):
        with tmp_db._connect() as conn:
            tables = [
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            ]
        assert "flow_runs" in tables
        assert "sync_log" in tables

    def test_idempotent_schema_creation(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        FlowRunDB(db_path)
        db2 = FlowRunDB(db_path)
        assert db2.count_flow_runs() == 0


class TestUpsertFlowRuns:
    def test_insert_single_row(self, tmp_db):
        row = make_flow_run_row(run_id="r1")
        count = tmp_db.upsert_flow_runs([row])
        assert count == 1
        assert tmp_db.count_flow_runs() == 1

    def test_upsert_updates_existing(self, tmp_db):
        row = make_flow_run_row(run_id="r1", state_type="RUNNING", state_name="Running")
        tmp_db.upsert_flow_runs([row])

        row["state_type"] = "COMPLETED"
        row["state_name"] = "Completed"
        tmp_db.upsert_flow_runs([row])

        assert tmp_db.count_flow_runs() == 1
        runs = tmp_db.get_all_flow_runs()
        assert runs[0]["state_type"] == "COMPLETED"

    def test_empty_list_returns_zero(self, tmp_db):
        assert tmp_db.upsert_flow_runs([]) == 0

    def test_bulk_insert(self, tmp_db):
        rows = [make_flow_run_row(run_id=f"r{i}") for i in range(50)]
        count = tmp_db.upsert_flow_runs(rows)
        assert count == 50
        assert tmp_db.count_flow_runs() == 50


class TestGetInFlightRunIds:
    def test_returns_non_terminal_runs(self, tmp_db):
        terminal = make_flow_run_row(run_id="done", state_type="COMPLETED")
        running = make_flow_run_row(run_id="active", state_type="RUNNING")
        scheduled = make_flow_run_row(run_id="queued", state_type="SCHEDULED")
        pending = make_flow_run_row(run_id="wait", state_type="PENDING")
        cancelling = make_flow_run_row(run_id="stopping", state_type="CANCELLING")
        paused = make_flow_run_row(run_id="paused", state_type="PAUSED")
        failed = make_flow_run_row(run_id="failed", state_type="FAILED")
        crashed = make_flow_run_row(run_id="crashed", state_type="CRASHED")
        cancelled = make_flow_run_row(run_id="cancelled", state_type="CANCELLED")

        tmp_db.upsert_flow_runs(
            [
                terminal,
                running,
                scheduled,
                pending,
                cancelling,
                paused,
                failed,
                crashed,
                cancelled,
            ]
        )

        in_flight = set(tmp_db.get_in_flight_run_ids())
        assert in_flight == {"active", "queued", "wait", "stopping", "paused"}

    def test_empty_db_returns_empty(self, tmp_db):
        assert tmp_db.get_in_flight_run_ids() == []


class TestGetAllFlowRuns:
    def test_unfiltered(self, tmp_db):
        tmp_db.upsert_flow_runs([make_flow_run_row(run_id=f"r{i}") for i in range(3)])
        assert len(tmp_db.get_all_flow_runs()) == 3

    def test_filter_by_state_type(self, tmp_db):
        tmp_db.upsert_flow_runs(
            [
                make_flow_run_row(run_id="r1", state_type="COMPLETED"),
                make_flow_run_row(run_id="r2", state_type="RUNNING"),
                make_flow_run_row(run_id="r3", state_type="COMPLETED"),
            ]
        )
        results = tmp_db.get_all_flow_runs(state_type="COMPLETED")
        assert len(results) == 2
        assert all(r["state_type"] == "COMPLETED" for r in results)

    def test_filter_by_flow_name(self, tmp_db):
        tmp_db.upsert_flow_runs(
            [
                make_flow_run_row(run_id="r1", flow_name="alpha"),
                make_flow_run_row(run_id="r2", flow_name="beta"),
            ]
        )
        results = tmp_db.get_all_flow_runs(flow_name="alpha")
        assert len(results) == 1
        assert results[0]["flow_name"] == "alpha"

    def test_limit_and_offset(self, tmp_db):
        tmp_db.upsert_flow_runs([make_flow_run_row(run_id=f"r{i}") for i in range(10)])
        page = tmp_db.get_all_flow_runs(limit=3, offset=0)
        assert len(page) == 3
        page2 = tmp_db.get_all_flow_runs(limit=3, offset=3)
        assert len(page2) == 3
        assert {r["id"] for r in page} & {r["id"] for r in page2} == set()


class TestCountFlowRuns:
    def test_count_all(self, tmp_db):
        tmp_db.upsert_flow_runs([make_flow_run_row(run_id=f"r{i}") for i in range(5)])
        assert tmp_db.count_flow_runs() == 5

    def test_count_by_state(self, tmp_db):
        tmp_db.upsert_flow_runs(
            [
                make_flow_run_row(run_id="r1", state_type="COMPLETED"),
                make_flow_run_row(run_id="r2", state_type="FAILED"),
                make_flow_run_row(run_id="r3", state_type="COMPLETED"),
            ]
        )
        assert tmp_db.count_flow_runs(state_type="COMPLETED") == 2
        assert tmp_db.count_flow_runs(state_type="FAILED") == 1
        assert tmp_db.count_flow_runs(state_type="RUNNING") == 0


class TestSyncLog:
    def test_start_and_finish_sync(self, tmp_db):
        log_id = tmp_db.start_sync("backfill")
        assert isinstance(log_id, int)

        tmp_db.finish_sync(log_id, rows_synced=42)
        log = tmp_db.get_sync_log(limit=1)
        assert len(log) == 1
        assert log[0]["sync_type"] == "backfill"
        assert log[0]["rows_synced"] == 42
        assert log[0]["status"] == "completed"
        assert log[0]["finished_at"] is not None

    def test_failed_sync(self, tmp_db):
        log_id = tmp_db.start_sync("incremental")
        tmp_db.finish_sync(log_id, rows_synced=0, status="failed")
        log = tmp_db.get_sync_log(limit=1)
        assert log[0]["status"] == "failed"

    def test_last_successful_sync_time(self, tmp_db):
        assert tmp_db.last_successful_sync_time() is None

        log_id = tmp_db.start_sync("backfill")
        tmp_db.finish_sync(log_id, rows_synced=10)
        ts = tmp_db.last_successful_sync_time()
        assert isinstance(ts, datetime)

    def test_has_prior_sync(self, tmp_db):
        assert tmp_db.has_prior_sync() is False
        log_id = tmp_db.start_sync("backfill")
        tmp_db.finish_sync(log_id, rows_synced=1)
        assert tmp_db.has_prior_sync() is True

    def test_failed_sync_not_counted_as_prior(self, tmp_db):
        log_id = tmp_db.start_sync("backfill")
        tmp_db.finish_sync(log_id, rows_synced=0, status="failed")
        assert tmp_db.has_prior_sync() is False

    def test_sync_log_ordering(self, tmp_db):
        for i in range(5):
            log_id = tmp_db.start_sync("backfill")
            tmp_db.finish_sync(log_id, rows_synced=i)
        log = tmp_db.get_sync_log(limit=3)
        assert len(log) == 3
        assert log[0]["id"] > log[1]["id"] > log[2]["id"]


class TestGetFlowRunById:
    def test_found(self, tmp_db):
        tmp_db.upsert_flow_runs([make_flow_run_row(run_id="lookup-1", flow_name="etl")])
        row = tmp_db.get_flow_run_by_id("lookup-1")
        assert row is not None
        assert row["id"] == "lookup-1"
        assert row["flow_name"] == "etl"

    def test_not_found(self, tmp_db):
        assert tmp_db.get_flow_run_by_id("nonexistent") is None

    def test_returns_all_fields(self, tmp_db):
        tmp_db.upsert_flow_runs(
            [
                make_flow_run_row(
                    run_id="detail-1",
                    flow_name="etl",
                    deployment_name="daily-etl",
                    entrypoint="flows/etl.py:run",
                    work_pool_type="kubernetes",
                    parameters='{"batch": 100}',
                )
            ]
        )
        row = tmp_db.get_flow_run_by_id("detail-1")
        assert row["deployment_name"] == "daily-etl"
        assert row["entrypoint"] == "flows/etl.py:run"
        assert row["work_pool_type"] == "kubernetes"
        assert row["parameters"] == '{"batch": 100}'


class TestGetFlowSummary:
    def test_empty_db(self, tmp_db):
        assert tmp_db.get_flow_summary() == []

    def test_single_flow(self, tmp_db):
        tmp_db.upsert_flow_runs(
            [
                make_flow_run_row(run_id="s1", flow_name="etl", state_type="COMPLETED"),
                make_flow_run_row(run_id="s2", flow_name="etl", state_type="COMPLETED"),
                make_flow_run_row(run_id="s3", flow_name="etl", state_type="FAILED"),
            ]
        )
        summary = tmp_db.get_flow_summary()
        assert len(summary) == 1
        row = summary[0]
        assert row["flow_name"] == "etl"
        assert row["total_runs"] == 3
        assert row["completed"] == 2
        assert row["failed"] == 1
        assert row["crashed"] == 0
        assert row["success_rate"] == pytest.approx(66.7, abs=0.1)

    def test_multiple_flows_ordered_by_total(self, tmp_db):
        tmp_db.upsert_flow_runs(
            [
                make_flow_run_row(
                    run_id="a1", flow_name="alpha", state_type="COMPLETED"
                ),
                make_flow_run_row(
                    run_id="b1", flow_name="beta", state_type="COMPLETED"
                ),
                make_flow_run_row(
                    run_id="b2", flow_name="beta", state_type="COMPLETED"
                ),
                make_flow_run_row(run_id="b3", flow_name="beta", state_type="CRASHED"),
            ]
        )
        summary = tmp_db.get_flow_summary()
        assert len(summary) == 2
        assert summary[0]["flow_name"] == "beta"
        assert summary[0]["total_runs"] == 3
        assert summary[1]["flow_name"] == "alpha"
        assert summary[1]["total_runs"] == 1

    def test_since_filter(self, tmp_db):
        tmp_db.upsert_flow_runs(
            [
                make_flow_run_row(
                    run_id="old",
                    flow_name="etl",
                    state_type="COMPLETED",
                    start_time="2025-01-01T00:00:00",
                ),
                make_flow_run_row(
                    run_id="new",
                    flow_name="etl",
                    state_type="FAILED",
                    start_time="2026-06-01T00:00:00",
                ),
            ]
        )
        summary = tmp_db.get_flow_summary(since="2026-01-01T00:00:00")
        assert len(summary) == 1
        assert summary[0]["total_runs"] == 1
        assert summary[0]["failed"] == 1

    def test_duration_stats(self, tmp_db):
        rows = [
            make_flow_run_row(run_id="d1", flow_name="etl", state_type="COMPLETED"),
            make_flow_run_row(run_id="d2", flow_name="etl", state_type="COMPLETED"),
        ]
        rows[0]["total_run_time_s"] = 10.0
        rows[1]["total_run_time_s"] = 30.0
        tmp_db.upsert_flow_runs(rows)

        summary = tmp_db.get_flow_summary()
        row = summary[0]
        assert row["avg_duration_s"] == 20.0
        assert row["min_duration_s"] == 10.0
        assert row["max_duration_s"] == 30.0

    def test_in_flight_count(self, tmp_db):
        tmp_db.upsert_flow_runs(
            [
                make_flow_run_row(run_id="r1", flow_name="etl", state_type="RUNNING"),
                make_flow_run_row(run_id="r2", flow_name="etl", state_type="PENDING"),
                make_flow_run_row(run_id="r3", flow_name="etl", state_type="COMPLETED"),
            ]
        )
        summary = tmp_db.get_flow_summary()
        assert summary[0]["in_flight"] == 2

    def test_success_rate_all_completed(self, tmp_db):
        tmp_db.upsert_flow_runs(
            [
                make_flow_run_row(
                    run_id="c1", flow_name="perfect", state_type="COMPLETED"
                ),
                make_flow_run_row(
                    run_id="c2", flow_name="perfect", state_type="COMPLETED"
                ),
            ]
        )
        summary = tmp_db.get_flow_summary()
        assert summary[0]["success_rate"] == 100.0
