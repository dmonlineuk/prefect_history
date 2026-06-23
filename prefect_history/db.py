"""SQLite persistence layer for cached Prefect flow-run data."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS flow_runs (
    id                  TEXT PRIMARY KEY,
    flow_id             TEXT NOT NULL,
    flow_name           TEXT,
    name                TEXT NOT NULL,
    deployment_id       TEXT,
    deployment_version  TEXT,
    work_pool_name      TEXT,
    work_queue_name     TEXT,
    state_type          TEXT,
    state_name          TEXT,
    state_message       TEXT,
    start_time          TEXT,
    end_time            TEXT,
    expected_start_time TEXT,
    total_run_time_s    REAL,
    created             TEXT,
    updated             TEXT,
    tags                TEXT,          -- JSON array
    parameters          TEXT,          -- JSON object
    parent_task_run_id  TEXT,
    auto_scheduled      INTEGER,
    run_count           INTEGER
);

CREATE INDEX IF NOT EXISTS idx_flow_runs_state_type
    ON flow_runs(state_type);
CREATE INDEX IF NOT EXISTS idx_flow_runs_start_time
    ON flow_runs(start_time);
CREATE INDEX IF NOT EXISTS idx_flow_runs_updated
    ON flow_runs(updated);
CREATE INDEX IF NOT EXISTS idx_flow_runs_flow_id
    ON flow_runs(flow_id);

CREATE TABLE IF NOT EXISTS sync_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    sync_type   TEXT NOT NULL,       -- 'backfill' | 'incremental'
    rows_synced INTEGER DEFAULT 0,
    status      TEXT DEFAULT 'running'  -- 'running' | 'completed' | 'failed'
);
"""

_UPSERT_SQL = """\
INSERT INTO flow_runs (
    id, flow_id, flow_name, name,
    deployment_id, deployment_version,
    work_pool_name, work_queue_name,
    state_type, state_name, state_message,
    start_time, end_time, expected_start_time,
    total_run_time_s, created, updated,
    tags, parameters, parent_task_run_id,
    auto_scheduled, run_count
) VALUES (
    :id, :flow_id, :flow_name, :name,
    :deployment_id, :deployment_version,
    :work_pool_name, :work_queue_name,
    :state_type, :state_name, :state_message,
    :start_time, :end_time, :expected_start_time,
    :total_run_time_s, :created, :updated,
    :tags, :parameters, :parent_task_run_id,
    :auto_scheduled, :run_count
)
ON CONFLICT(id) DO UPDATE SET
    flow_id            = excluded.flow_id,
    flow_name          = excluded.flow_name,
    name               = excluded.name,
    deployment_id      = excluded.deployment_id,
    deployment_version = excluded.deployment_version,
    work_pool_name     = excluded.work_pool_name,
    work_queue_name    = excluded.work_queue_name,
    state_type         = excluded.state_type,
    state_name         = excluded.state_name,
    state_message      = excluded.state_message,
    start_time         = excluded.start_time,
    end_time           = excluded.end_time,
    expected_start_time = excluded.expected_start_time,
    total_run_time_s   = excluded.total_run_time_s,
    created            = excluded.created,
    updated            = excluded.updated,
    tags               = excluded.tags,
    parameters         = excluded.parameters,
    parent_task_run_id = excluded.parent_task_run_id,
    auto_scheduled     = excluded.auto_scheduled,
    run_count          = excluded.run_count
"""


class FlowRunDB:
    """Thin wrapper around a SQLite database for flow-run caching."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._ensure_schema()

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(_SCHEMA_SQL)

    # ------------------------------------------------------------------
    # Flow run CRUD
    # ------------------------------------------------------------------

    def upsert_flow_runs(self, rows: list[dict]) -> int:
        """Insert or update flow runs. Returns the number of rows affected."""
        if not rows:
            return 0
        with self._connect() as conn:
            conn.executemany(_UPSERT_SQL, rows)
            return len(rows)

    def get_in_flight_run_ids(self) -> list[str]:
        """Return IDs of cached flow runs still in a non-terminal state."""
        non_terminal = (
            "SCHEDULED",
            "PENDING",
            "RUNNING",
            "CANCELLING",
            "PAUSED",
        )
        placeholders = ",".join("?" for _ in non_terminal)
        sql = f"SELECT id FROM flow_runs WHERE state_type IN ({placeholders})"
        with self._connect() as conn:
            return [row["id"] for row in conn.execute(sql, non_terminal)]

    def get_all_flow_runs(
        self,
        *,
        state_type: str | None = None,
        flow_name: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict]:
        """Query cached flow runs with optional filters."""
        clauses: list[str] = []
        params: list[str | int] = []
        if state_type:
            clauses.append("state_type = ?")
            params.append(state_type)
        if flow_name:
            clauses.append("flow_name = ?")
            params.append(flow_name)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT * FROM flow_runs{where} ORDER BY start_time DESC"
        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
        with self._connect() as conn:
            return [dict(row) for row in conn.execute(sql, params)]

    def count_flow_runs(self, *, state_type: str | None = None) -> int:
        """Count cached flow runs, optionally filtered by state_type."""
        if state_type:
            sql = "SELECT COUNT(*) AS cnt FROM flow_runs WHERE state_type = ?"
            params: tuple = (state_type,)
        else:
            sql = "SELECT COUNT(*) AS cnt FROM flow_runs"
            params = ()
        with self._connect() as conn:
            row = conn.execute(sql, params).fetchone()
            return row["cnt"] if row else 0

    # ------------------------------------------------------------------
    # Sync log helpers
    # ------------------------------------------------------------------

    def start_sync(self, sync_type: str) -> int:
        """Record the start of a sync operation. Returns the log ID."""
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO sync_log (started_at, sync_type) VALUES (?, ?)",
                (now, sync_type),
            )
            return cur.lastrowid  # type: ignore[return-value]

    def finish_sync(
        self, log_id: int, *, rows_synced: int, status: str = "completed"
    ) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            conn.execute(
                "UPDATE sync_log "
                "SET finished_at = ?, rows_synced = ?, status = ? "
                "WHERE id = ?",
                (now, rows_synced, status, log_id),
            )

    def last_successful_sync_time(self) -> datetime | None:
        """Return the ``finished_at`` of the most recent successful sync."""
        sql = (
            "SELECT finished_at FROM sync_log "
            "WHERE status = 'completed' "
            "ORDER BY finished_at DESC LIMIT 1"
        )
        with self._connect() as conn:
            row = conn.execute(sql).fetchone()
            if row and row["finished_at"]:
                return datetime.fromisoformat(row["finished_at"])
            return None

    def has_prior_sync(self) -> bool:
        """Return ``True`` if at least one successful sync exists."""
        return self.last_successful_sync_time() is not None

    def get_sync_log(self, *, limit: int = 20) -> list[dict]:
        """Return recent sync log entries."""
        sql = "SELECT * FROM sync_log ORDER BY id DESC LIMIT ?"
        with self._connect() as conn:
            return [dict(row) for row in conn.execute(sql, (limit,))]

    # ------------------------------------------------------------------
    # Summary / aggregation
    # ------------------------------------------------------------------

    def get_flow_summary(self, *, since: str | None = None) -> list[dict]:
        """Per-flow aggregation with run counts, state breakdown, and duration stats.

        Parameters
        ----------
        since:
            ISO-format datetime string. If provided, only runs with
            ``start_time >= since`` are included.

        Returns a list of dicts sorted by total runs descending.
        """
        where = "WHERE start_time >= ?" if since else ""
        params: tuple = (since,) if since else ()

        sql = f"""
            SELECT
                flow_name,
                COUNT(*)                                       AS total_runs,
                SUM(CASE WHEN state_type = 'COMPLETED' THEN 1 ELSE 0 END) AS completed,
                SUM(CASE WHEN state_type = 'FAILED' THEN 1 ELSE 0 END)    AS failed,
                SUM(CASE WHEN state_type = 'CRASHED' THEN 1 ELSE 0 END)   AS crashed,
                SUM(CASE WHEN state_type = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled,
                SUM(CASE WHEN state_type IN (
                    'RUNNING','PENDING','SCHEDULED','CANCELLING','PAUSED'
                ) THEN 1 ELSE 0 END)                   AS in_flight,
                ROUND(AVG(total_run_time_s), 1)                AS avg_duration_s,
                ROUND(MIN(total_run_time_s), 1)                AS min_duration_s,
                ROUND(MAX(total_run_time_s), 1)                AS max_duration_s,
                MAX(start_time)                                AS last_run,
                SUM(CASE WHEN start_time >= datetime('now', '-1 day')
                    THEN 1 ELSE 0 END)                         AS runs_24h,
                SUM(CASE WHEN start_time >= datetime('now', '-7 days')
                    THEN 1 ELSE 0 END)                         AS runs_7d
            FROM flow_runs
            {where}
            GROUP BY flow_name
            ORDER BY total_runs DESC
        """
        with self._connect() as conn:
            rows = [dict(row) for row in conn.execute(sql, params)]

        for row in rows:
            total = row["total_runs"]
            completed = row["completed"]
            row["success_rate"] = round(completed / total * 100, 1) if total else 0.0

        return rows
