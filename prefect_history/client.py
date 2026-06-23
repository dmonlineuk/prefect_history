"""Prefect API client wrapper for fetching flow-run history."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterExpectedStartTime,
    FlowRunFilterId,
)
from prefect.client.schemas.objects import FlowRun, StateType
from prefect.client.schemas.sorting import FlowRunSort

if TYPE_CHECKING:
    from prefect.client.orchestration import PrefectClient

logger = logging.getLogger(__name__)

TERMINAL_STATES = frozenset(
    {
        StateType.COMPLETED,
        StateType.FAILED,
        StateType.CANCELLED,
        StateType.CRASHED,
    }
)
NON_TERMINAL_STATES = frozenset(
    {
        StateType.SCHEDULED,
        StateType.PENDING,
        StateType.RUNNING,
        StateType.CANCELLING,
        StateType.PAUSED,
    }
)


def _dt_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.isoformat()


def flow_run_to_row(
    run: FlowRun,
    flow_name: str | None = None,
    deployment_info: dict | None = None,
    work_pool_type: str | None = None,
) -> dict:
    """Convert a Prefect ``FlowRun`` object into a flat dict for SQLite.

    Parameters
    ----------
    run:
        The FlowRun object from the Prefect API.
    flow_name:
        Resolved human-readable flow name.
    deployment_info:
        Dict with ``name`` and ``entrypoint`` keys from the deployment.
    work_pool_type:
        The type of the work pool (e.g. 'kubernetes', 'process').
    """
    total_seconds = run.total_run_time.total_seconds() if run.total_run_time else 0.0
    dep = deployment_info or {}
    created_by = run.created_by
    return {
        "id": str(run.id),
        "flow_id": str(run.flow_id),
        "flow_name": flow_name or "",
        "name": run.name,
        "deployment_id": str(run.deployment_id) if run.deployment_id else None,
        "deployment_name": dep.get("name"),
        "deployment_version": run.deployment_version,
        "entrypoint": dep.get("entrypoint"),
        "work_pool_name": run.work_pool_name,
        "work_pool_type": work_pool_type,
        "work_queue_name": run.work_queue_name,
        "infrastructure_pid": run.infrastructure_pid,
        "created_by_type": created_by.type if created_by else None,
        "created_by_id": str(created_by.id) if created_by and created_by.id else None,
        "created_by_display": (created_by.display_value if created_by else None),
        "state_type": run.state_type.value if run.state_type else None,
        "state_name": run.state_name,
        "state_message": (
            run.state.message if run.state and run.state.message else None
        ),
        "start_time": _dt_iso(run.start_time),
        "end_time": _dt_iso(run.end_time),
        "expected_start_time": _dt_iso(run.expected_start_time),
        "total_run_time_s": total_seconds,
        "created": _dt_iso(run.created),
        "updated": _dt_iso(run.updated),
        "tags": json.dumps(run.tags) if run.tags else "[]",
        "parameters": json.dumps(run.parameters) if run.parameters else "{}",
        "parent_task_run_id": (
            str(run.parent_task_run_id) if run.parent_task_run_id else None
        ),
        "auto_scheduled": int(run.auto_scheduled),
        "run_count": run.run_count,
    }


async def _resolve_flow_names(
    client: PrefectClient,
    flow_ids: set[str],
) -> dict[str, str]:
    """Resolve a set of flow IDs to their human-readable names."""
    from prefect.client.schemas.filters import FlowFilter, FlowFilterId

    if not flow_ids:
        return {}

    uuids = [UUID(fid) for fid in flow_ids]
    flows = await client.read_flows(
        flow_filter=FlowFilter(id=FlowFilterId(any_=uuids)),
    )
    return {str(f.id): f.name for f in flows}


async def _resolve_deployments(
    client: PrefectClient,
    deployment_ids: set[str],
) -> dict[str, dict]:
    """Resolve deployment IDs to {name, entrypoint} dicts."""
    if not deployment_ids:
        return {}

    result: dict[str, dict] = {}
    for dep_id in deployment_ids:
        try:
            dep = await client.read_deployment(UUID(dep_id))
            result[dep_id] = {
                "name": dep.name,
                "entrypoint": dep.entrypoint,
            }
        except Exception:
            logger.debug("Could not resolve deployment %s", dep_id)
            result[dep_id] = {"name": None, "entrypoint": None}
    return result


async def _resolve_work_pool_types(
    client: PrefectClient,
    pool_names: set[str],
) -> dict[str, str]:
    """Resolve work pool names to their type (e.g. 'kubernetes')."""
    if not pool_names:
        return {}

    result: dict[str, str] = {}
    for name in pool_names:
        try:
            pool = await client.read_work_pool(name)
            result[name] = pool.type
        except Exception:
            logger.debug("Could not resolve work pool %s", name)
            result[name] = ""
    return result


async def fetch_flow_runs_since(
    *,
    since: datetime,
    page_size: int = 200,
) -> list[dict]:
    """Fetch all flow runs whose ``expected_start_time >= since``.

    Handles pagination automatically and resolves flow names,
    deployment details, and work pool types.
    The Prefect client reads ``PREFECT_API_URL`` and ``PREFECT_API_KEY``
    from environment variables (set by ``load_settings``).
    Returns a list of row dicts ready for ``FlowRunDB.upsert_flow_runs``.
    """
    rows: list[dict] = []
    flow_name_cache: dict[str, str] = {}
    deployment_cache: dict[str, dict] = {}
    pool_type_cache: dict[str, str] = {}

    async with get_client() as client:
        offset = 0
        while True:
            batch = await client.read_flow_runs(
                flow_run_filter=FlowRunFilter(
                    expected_start_time=FlowRunFilterExpectedStartTime(
                        after_=since,
                    ),
                ),
                sort=FlowRunSort.EXPECTED_START_TIME_ASC,
                limit=page_size,
                offset=offset,
            )
            if not batch:
                break

            new_flow_ids = {str(r.flow_id) for r in batch} - flow_name_cache.keys()
            if new_flow_ids:
                names = await _resolve_flow_names(client, new_flow_ids)
                flow_name_cache.update(names)

            new_dep_ids = {
                str(r.deployment_id) for r in batch if r.deployment_id
            } - deployment_cache.keys()
            if new_dep_ids:
                deps = await _resolve_deployments(client, new_dep_ids)
                deployment_cache.update(deps)

            new_pools = {
                r.work_pool_name for r in batch if r.work_pool_name
            } - pool_type_cache.keys()
            if new_pools:
                types = await _resolve_work_pool_types(client, new_pools)
                pool_type_cache.update(types)

            for run in batch:
                dep_id = str(run.deployment_id) if run.deployment_id else None
                rows.append(
                    flow_run_to_row(
                        run,
                        flow_name=flow_name_cache.get(str(run.flow_id)),
                        deployment_info=(
                            deployment_cache.get(dep_id) if dep_id else None
                        ),
                        work_pool_type=pool_type_cache.get(run.work_pool_name or ""),
                    )
                )

            logger.info("Fetched page at offset %d (%d runs)", offset, len(batch))
            offset += len(batch)
            if len(batch) < page_size:
                break

    logger.info("Total flow runs fetched: %d", len(rows))
    return rows


async def fetch_flow_runs_by_ids(
    *,
    run_ids: list[str],
    page_size: int = 200,
) -> list[dict]:
    """Re-fetch specific flow runs by their IDs (for in-flight re-checks).

    The Prefect client reads ``PREFECT_API_URL`` and ``PREFECT_API_KEY``
    from environment variables (set by ``load_settings``).
    Returns a list of row dicts ready for ``FlowRunDB.upsert_flow_runs``.
    """
    if not run_ids:
        return []

    rows: list[dict] = []
    flow_name_cache: dict[str, str] = {}
    deployment_cache: dict[str, dict] = {}
    pool_type_cache: dict[str, str] = {}

    async with get_client() as client:
        for i in range(0, len(run_ids), page_size):
            chunk = run_ids[i : i + page_size]
            uuids = [UUID(rid) for rid in chunk]
            batch = await client.read_flow_runs(
                flow_run_filter=FlowRunFilter(
                    id=FlowRunFilterId(any_=uuids),
                ),
            )

            new_flow_ids = {str(r.flow_id) for r in batch} - flow_name_cache.keys()
            if new_flow_ids:
                names = await _resolve_flow_names(client, new_flow_ids)
                flow_name_cache.update(names)

            new_dep_ids = {
                str(r.deployment_id) for r in batch if r.deployment_id
            } - deployment_cache.keys()
            if new_dep_ids:
                deps = await _resolve_deployments(client, new_dep_ids)
                deployment_cache.update(deps)

            new_pools = {
                r.work_pool_name for r in batch if r.work_pool_name
            } - pool_type_cache.keys()
            if new_pools:
                types = await _resolve_work_pool_types(client, new_pools)
                pool_type_cache.update(types)

            for run in batch:
                dep_id = str(run.deployment_id) if run.deployment_id else None
                rows.append(
                    flow_run_to_row(
                        run,
                        flow_name=flow_name_cache.get(str(run.flow_id)),
                        deployment_info=(
                            deployment_cache.get(dep_id) if dep_id else None
                        ),
                        work_pool_type=pool_type_cache.get(run.work_pool_name or ""),
                    )
                )

            logger.info(
                "Re-checked chunk %d-%d (%d runs returned)",
                i,
                i + len(chunk),
                len(batch),
            )

    logger.info("Total in-flight runs re-checked: %d", len(rows))
    return rows
