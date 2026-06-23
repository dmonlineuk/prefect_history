"""Tests for prefect_history.client."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
from prefect.client.schemas.objects import FlowRun, State, StateType

from prefect_history.client import (
    NON_TERMINAL_STATES,
    TERMINAL_STATES,
    _dt_iso,
    _resolve_deployments,
    _resolve_flow_names,
    _resolve_work_pool_types,
    fetch_flow_runs_by_ids,
    fetch_flow_runs_since,
    flow_run_to_row,
)


def _make_mock_flow_run(
    *,
    run_id: UUID | None = None,
    flow_id: UUID | None = None,
    name: str = "test-run",
    state_type: StateType = StateType.COMPLETED,
    state_name: str = "Completed",
    state_message: str | None = None,
    tags: list[str] | None = None,
    parameters: dict | None = None,
    start_time: datetime | None = None,
) -> FlowRun:
    """Build a ``FlowRun`` instance for testing."""
    now = datetime.now(UTC)
    return FlowRun(
        id=run_id or uuid4(),
        flow_id=flow_id or uuid4(),
        name=name,
        state_type=state_type,
        state_name=state_name,
        state=State(type=state_type, name=state_name, message=state_message),
        tags=tags or [],
        parameters=parameters or {},
        start_time=start_time or now,
        end_time=now,
        expected_start_time=now - timedelta(seconds=10),
        total_run_time=timedelta(seconds=120),
        created=now,
        updated=now,
        auto_scheduled=False,
        run_count=1,
    )


class TestDtIso:
    def test_none_returns_none(self):
        assert _dt_iso(None) is None

    def test_datetime_returns_isoformat(self):
        dt = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)
        assert _dt_iso(dt) == "2025-01-15T12:00:00+00:00"


class TestFlowRunToRow:
    def test_basic_conversion(self):
        fid = uuid4()
        rid = uuid4()
        run = _make_mock_flow_run(run_id=rid, flow_id=fid, name="my-run")
        row = flow_run_to_row(run, flow_name="etl-pipeline")

        assert row["id"] == str(rid)
        assert row["flow_id"] == str(fid)
        assert row["flow_name"] == "etl-pipeline"
        assert row["name"] == "my-run"
        assert row["state_type"] == "COMPLETED"
        assert row["state_name"] == "Completed"
        assert row["total_run_time_s"] == 120.0
        assert row["auto_scheduled"] == 0
        assert row["run_count"] == 1

    def test_tags_serialized_as_json(self):
        run = _make_mock_flow_run(tags=["prod", "etl"])
        row = flow_run_to_row(run)
        assert json.loads(row["tags"]) == ["prod", "etl"]

    def test_parameters_serialized_as_json(self):
        run = _make_mock_flow_run(parameters={"batch_size": 1000})
        row = flow_run_to_row(run)
        assert json.loads(row["parameters"]) == {"batch_size": 1000}

    def test_empty_tags_and_params(self):
        run = _make_mock_flow_run(tags=[], parameters={})
        row = flow_run_to_row(run)
        assert row["tags"] == "[]"
        assert row["parameters"] == "{}"

    def test_state_message_captured(self):
        run = _make_mock_flow_run(state_message="All tasks succeeded")
        row = flow_run_to_row(run)
        assert row["state_message"] == "All tasks succeeded"

    def test_no_flow_name_defaults_to_empty(self):
        run = _make_mock_flow_run()
        row = flow_run_to_row(run, flow_name=None)
        assert row["flow_name"] == ""

    def test_deployment_info_captured(self):
        run = _make_mock_flow_run()
        dep_info = {"name": "daily-etl", "entrypoint": "flows/etl.py:run"}
        row = flow_run_to_row(run, deployment_info=dep_info)
        assert row["deployment_name"] == "daily-etl"
        assert row["entrypoint"] == "flows/etl.py:run"

    def test_work_pool_type_captured(self):
        run = _make_mock_flow_run()
        row = flow_run_to_row(run, work_pool_type="kubernetes")
        assert row["work_pool_type"] == "kubernetes"

    def test_created_by_fields(self):
        run = _make_mock_flow_run()
        cb = MagicMock()
        cb.type = "DEPLOYMENT"
        cb.id = uuid4()
        cb.display_value = "daily-etl"
        run.created_by = cb
        row = flow_run_to_row(run)
        assert row["created_by_type"] == "DEPLOYMENT"
        assert row["created_by_display"] == "daily-etl"

    def test_infrastructure_pid_captured(self):
        run = _make_mock_flow_run()
        run.infrastructure_pid = "k8s-pod-abc123"
        row = flow_run_to_row(run)
        assert row["infrastructure_pid"] == "k8s-pod-abc123"


class TestStateConstants:
    def test_terminal_states(self):
        assert StateType.COMPLETED in TERMINAL_STATES
        assert StateType.FAILED in TERMINAL_STATES
        assert StateType.CANCELLED in TERMINAL_STATES
        assert StateType.CRASHED in TERMINAL_STATES

    def test_non_terminal_states(self):
        assert StateType.RUNNING in NON_TERMINAL_STATES
        assert StateType.PENDING in NON_TERMINAL_STATES
        assert StateType.SCHEDULED in NON_TERMINAL_STATES
        assert StateType.CANCELLING in NON_TERMINAL_STATES
        assert StateType.PAUSED in NON_TERMINAL_STATES

    def test_all_states_covered(self):
        all_covered = TERMINAL_STATES | NON_TERMINAL_STATES
        for st in StateType:
            assert st in all_covered, f"{st} not in TERMINAL or NON_TERMINAL"


class TestFetchFlowRunsSince:
    @pytest.mark.asyncio
    async def test_single_page(self):
        fid = uuid4()
        runs = [_make_mock_flow_run(flow_id=fid, name=f"run-{i}") for i in range(3)]
        mock_flow = MagicMock()
        mock_flow.id = fid
        mock_flow.name = "test-flow"

        mock_client = AsyncMock()
        mock_client.read_flow_runs = AsyncMock(side_effect=[runs, []])
        mock_client.read_flows = AsyncMock(return_value=[mock_flow])
        mock_client.read_deployment = AsyncMock(side_effect=Exception("no dep"))
        mock_client.read_work_pool = AsyncMock(side_effect=Exception("no pool"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("prefect_history.client.get_client", return_value=mock_client):
            rows = await fetch_flow_runs_since(
                since=datetime.now(UTC) - timedelta(days=60),
                page_size=50,
            )

        assert len(rows) == 3
        assert all(r["flow_name"] == "test-flow" for r in rows)

    @pytest.mark.asyncio
    async def test_pagination(self):
        fid = uuid4()
        page1 = [_make_mock_flow_run(flow_id=fid) for _ in range(2)]
        page2 = [_make_mock_flow_run(flow_id=fid)]

        mock_flow = MagicMock()
        mock_flow.id = fid
        mock_flow.name = "paginated-flow"

        mock_client = AsyncMock()
        mock_client.read_flow_runs = AsyncMock(side_effect=[page1, page2, []])
        mock_client.read_flows = AsyncMock(return_value=[mock_flow])
        mock_client.read_deployment = AsyncMock(side_effect=Exception("no dep"))
        mock_client.read_work_pool = AsyncMock(side_effect=Exception("no pool"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("prefect_history.client.get_client", return_value=mock_client):
            rows = await fetch_flow_runs_since(
                since=datetime.now(UTC) - timedelta(days=60),
                page_size=2,
            )

        assert len(rows) == 3

    @pytest.mark.asyncio
    async def test_empty_result(self):
        mock_client = AsyncMock()
        mock_client.read_flow_runs = AsyncMock(return_value=[])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("prefect_history.client.get_client", return_value=mock_client):
            rows = await fetch_flow_runs_since(
                since=datetime.now(UTC),
            )

        assert rows == []


class TestFetchFlowRunsByIds:
    @pytest.mark.asyncio
    async def test_fetches_by_ids(self):
        rid = uuid4()
        fid = uuid4()
        run = _make_mock_flow_run(run_id=rid, flow_id=fid)

        mock_flow = MagicMock()
        mock_flow.id = fid
        mock_flow.name = "recheck-flow"

        mock_client = AsyncMock()
        mock_client.read_flow_runs = AsyncMock(return_value=[run])
        mock_client.read_flows = AsyncMock(return_value=[mock_flow])
        mock_client.read_deployment = AsyncMock(side_effect=Exception("no dep"))
        mock_client.read_work_pool = AsyncMock(side_effect=Exception("no pool"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("prefect_history.client.get_client", return_value=mock_client):
            rows = await fetch_flow_runs_by_ids(
                run_ids=[str(rid)],
            )

        assert len(rows) == 1
        assert rows[0]["id"] == str(rid)
        assert rows[0]["flow_name"] == "recheck-flow"

    @pytest.mark.asyncio
    async def test_empty_ids_returns_empty(self):
        rows = await fetch_flow_runs_by_ids(
            run_ids=[],
        )
        assert rows == []


class TestResolveFlowNames:
    @pytest.mark.asyncio
    async def test_resolves_names(self):
        fid1 = uuid4()
        fid2 = uuid4()

        mock_flow1 = MagicMock()
        mock_flow1.id = fid1
        mock_flow1.name = "flow-alpha"
        mock_flow2 = MagicMock()
        mock_flow2.id = fid2
        mock_flow2.name = "flow-beta"

        mock_client = AsyncMock()
        mock_client.read_flows = AsyncMock(return_value=[mock_flow1, mock_flow2])

        result = await _resolve_flow_names(mock_client, {str(fid1), str(fid2)})
        assert result[str(fid1)] == "flow-alpha"
        assert result[str(fid2)] == "flow-beta"

    @pytest.mark.asyncio
    async def test_empty_set_returns_empty(self):
        mock_client = AsyncMock()
        result = await _resolve_flow_names(mock_client, set())
        assert result == {}


class TestResolveDeployments:
    @pytest.mark.asyncio
    async def test_resolves_deployment(self):
        dep_id = uuid4()
        mock_dep = MagicMock()
        mock_dep.name = "daily-etl"
        mock_dep.entrypoint = "flows/etl.py:run"

        mock_client = AsyncMock()
        mock_client.read_deployment = AsyncMock(return_value=mock_dep)

        result = await _resolve_deployments(mock_client, {str(dep_id)})
        assert result[str(dep_id)]["name"] == "daily-etl"
        assert result[str(dep_id)]["entrypoint"] == "flows/etl.py:run"

    @pytest.mark.asyncio
    async def test_empty_set_returns_empty(self):
        mock_client = AsyncMock()
        result = await _resolve_deployments(mock_client, set())
        assert result == {}

    @pytest.mark.asyncio
    async def test_handles_error_gracefully(self):
        dep_id = uuid4()
        mock_client = AsyncMock()
        mock_client.read_deployment = AsyncMock(side_effect=Exception("not found"))

        result = await _resolve_deployments(mock_client, {str(dep_id)})
        assert result[str(dep_id)] == {"name": None, "entrypoint": None}


class TestResolveWorkPoolTypes:
    @pytest.mark.asyncio
    async def test_resolves_pool_type(self):
        mock_pool = MagicMock()
        mock_pool.type = "kubernetes"

        mock_client = AsyncMock()
        mock_client.read_work_pool = AsyncMock(return_value=mock_pool)

        result = await _resolve_work_pool_types(mock_client, {"my-k8s-pool"})
        assert result["my-k8s-pool"] == "kubernetes"

    @pytest.mark.asyncio
    async def test_empty_set_returns_empty(self):
        mock_client = AsyncMock()
        result = await _resolve_work_pool_types(mock_client, set())
        assert result == {}

    @pytest.mark.asyncio
    async def test_handles_error_gracefully(self):
        mock_client = AsyncMock()
        mock_client.read_work_pool = AsyncMock(side_effect=Exception("not found"))

        result = await _resolve_work_pool_types(mock_client, {"missing-pool"})
        assert result["missing-pool"] == ""
