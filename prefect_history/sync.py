"""Synchronisation orchestration: backfill, incremental, and in-flight re-check."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from prefect_history.client import fetch_flow_runs_by_ids, fetch_flow_runs_since
from prefect_history.config import Settings
from prefect_history.db import FlowRunDB

logger = logging.getLogger(__name__)


def _months_ago(months: int) -> datetime:
    """Return a UTC datetime *months* months before now (approximate)."""
    now = datetime.now(UTC)
    return now - timedelta(days=30 * months)


def _run_async(coro):
    """Run an async coroutine from synchronous code."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, coro).result()
    return asyncio.run(coro)


# ------------------------------------------------------------------
# Public sync functions
# ------------------------------------------------------------------


def backfill(settings: Settings, *, months: int | None = None) -> int:
    """Pull all flow runs from the last *months* months into the cache.

    Parameters
    ----------
    settings:
        Runtime configuration (API credentials, DB path, etc.).
    months:
        Override for the backfill window. Falls back to
        ``settings.backfill_months``.

    Returns
    -------
    int
        Number of rows upserted.
    """
    months = months or settings.backfill_months
    since = _months_ago(months)
    db = FlowRunDB(settings.db_path)
    log_id = db.start_sync("backfill")
    logger.info("Starting backfill from %s (%d months)", since.isoformat(), months)

    try:
        rows = _run_async(
            fetch_flow_runs_since(
                api_url=settings.prefect_api_url,
                api_key=settings.prefect_api_key,
                since=since,
                page_size=settings.page_size,
            )
        )
        count = db.upsert_flow_runs(rows)
        db.finish_sync(log_id, rows_synced=count, status="completed")
        logger.info("Backfill complete: %d rows upserted", count)
        return count
    except Exception:
        db.finish_sync(log_id, rows_synced=0, status="failed")
        raise


def incremental(settings: Settings) -> int:
    """Pull flow runs created or updated since the last successful sync.

    If no previous sync exists, falls back to a full backfill.

    Returns
    -------
    int
        Number of rows upserted (new + updated in-flight).
    """
    db = FlowRunDB(settings.db_path)
    last_sync = db.last_successful_sync_time()

    if last_sync is None:
        logger.info("No prior sync found; falling back to backfill")
        return backfill(settings)

    log_id = db.start_sync("incremental")
    logger.info("Starting incremental sync since %s", last_sync.isoformat())

    try:
        # 1) Fetch new runs since last sync
        new_rows = _run_async(
            fetch_flow_runs_since(
                api_url=settings.prefect_api_url,
                api_key=settings.prefect_api_key,
                since=last_sync,
                page_size=settings.page_size,
            )
        )

        # 2) Re-check in-flight runs that may have completed
        in_flight_ids = db.get_in_flight_run_ids()
        refreshed_rows: list[dict] = []
        if in_flight_ids:
            logger.info(
                "Re-checking %d in-flight runs for state changes",
                len(in_flight_ids),
            )
            refreshed_rows = _run_async(
                fetch_flow_runs_by_ids(
                    api_url=settings.prefect_api_url,
                    api_key=settings.prefect_api_key,
                    run_ids=in_flight_ids,
                    page_size=settings.page_size,
                )
            )

        # Merge: refreshed_rows may overlap with new_rows (same ID),
        # but the upsert handles de-duplication.
        all_rows = new_rows + refreshed_rows
        count = db.upsert_flow_runs(all_rows)
        db.finish_sync(log_id, rows_synced=count, status="completed")
        logger.info(
            "Incremental sync complete: %d new + %d refreshed = %d upserted",
            len(new_rows),
            len(refreshed_rows),
            count,
        )
        return count
    except Exception:
        db.finish_sync(log_id, rows_synced=0, status="failed")
        raise
