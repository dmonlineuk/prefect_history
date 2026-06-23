"""CLI entry-point: ``python -m prefect_history``."""

from __future__ import annotations

import argparse
import logging
import sys

from rich.console import Console
from rich.table import Table

from prefect_history.config import load_settings
from prefect_history.db import FlowRunDB
from prefect_history.sync import backfill, incremental

_STATE_COLOURS: dict[str, str] = {
    "COMPLETED": "green",
    "RUNNING": "cyan",
    "SCHEDULED": "blue",
    "PENDING": "yellow",
    "FAILED": "red",
    "CRASHED": "bold red",
    "CANCELLED": "magenta",
    "CANCELLING": "magenta",
    "PAUSED": "yellow",
}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="prefect_history",
        description="Cache Prefect flow-run history in a local SQLite database.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to .env file (default: .env).",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="SQLite database path (default: prefect_history.db).",
    )

    sub = parser.add_subparsers(dest="command")

    # -- backfill ---------------------------------------------------
    bf = sub.add_parser(
        "backfill",
        help="Initial pull of flow runs (default: last 2 months).",
    )
    bf.add_argument(
        "-m",
        "--months",
        type=int,
        default=None,
        help="Number of months to look back (overrides PH_BACKFILL_MONTHS).",
    )

    # -- sync -------------------------------------------------------
    sub.add_parser(
        "sync",
        help="Incremental sync: new runs + re-check in-flight.",
    )

    # -- status -----------------------------------------------------
    sub.add_parser(
        "status",
        help="Show cache statistics and recent sync log.",
    )

    # -- list -------------------------------------------------------
    ls = sub.add_parser(
        "list",
        help="Display cached flow runs in a table.",
    )
    ls.add_argument(
        "-n",
        "--limit",
        type=int,
        default=20,
        help="Number of rows to display (default: 20).",
    )
    ls.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Row offset for pagination (default: 0).",
    )
    ls.add_argument(
        "--state",
        default=None,
        help="Filter by state_type (e.g. COMPLETED, FAILED, RUNNING).",
    )
    ls.add_argument(
        "--flow",
        default=None,
        help="Filter by flow name.",
    )

    # -- summary ----------------------------------------------------
    sm = sub.add_parser(
        "summary",
        help="Per-flow aggregation with stats and success rate.",
    )
    sm.add_argument(
        "--since",
        default=None,
        help="Only include runs after this ISO datetime (e.g. 2026-01-01).",
    )
    sm.add_argument(
        "--flow",
        default=None,
        help="Filter to a single flow name.",
    )

    # -- serve ------------------------------------------------------
    sv = sub.add_parser(
        "serve",
        help="Launch the web UI for browsing cached flow runs.",
    )
    sv.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind to (default: 127.0.0.1).",
    )
    sv.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to listen on (default: 8000).",
    )

    return parser


def _cmd_status(settings_kwargs: dict) -> None:
    settings = load_settings(**settings_kwargs)
    db = FlowRunDB(settings.db_path)

    total = db.count_flow_runs()
    last = db.last_successful_sync_time()
    in_flight = len(db.get_in_flight_run_ids())

    print(f"Database      : {settings.db_path}")
    print(f"Total runs    : {total}")
    print(f"In-flight     : {in_flight}")
    print(f"Last sync     : {last.isoformat() if last else 'never'}")
    print()

    log = db.get_sync_log(limit=10)
    if log:
        print("Recent sync log:")
        print(f"  {'ID':>4}  {'Type':<12} {'Status':<10} {'Rows':>6}  Started")
        for entry in log:
            print(
                f"  {entry['id']:>4}  {entry['sync_type']:<12} "
                f"{entry['status']:<10} {entry['rows_synced'] or 0:>6}  "
                f"{entry['started_at']}"
            )


def _cmd_list(
    settings_kwargs: dict,
    *,
    limit: int,
    offset: int,
    state: str | None,
    flow: str | None,
) -> None:
    settings = load_settings(**settings_kwargs)
    db = FlowRunDB(settings.db_path)

    total = db.count_flow_runs(state_type=state)
    rows = db.get_all_flow_runs(
        state_type=state,
        flow_name=flow,
        limit=limit,
        offset=offset,
    )

    console = Console()
    table = Table(
        title=f"Flow Runs ({offset + 1}-{offset + len(rows)} of {total})",
        show_lines=True,
    )
    table.add_column("Name", style="bold")
    table.add_column("Flow")
    table.add_column("Deployment")
    table.add_column("Entrypoint")
    table.add_column("State", justify="center")
    table.add_column("Pool Type")
    table.add_column("Start Time")
    table.add_column("Duration (s)", justify="right")

    for row in rows:
        st = row.get("state_type") or ""
        colour = _STATE_COLOURS.get(st, "white")
        state_display = f"[{colour}]{row.get('state_name', st)}[/{colour}]"

        duration = row.get("total_run_time_s")
        dur_str = f"{duration:.1f}" if duration is not None else ""

        table.add_row(
            row.get("name", ""),
            row.get("flow_name", ""),
            row.get("deployment_name", "") or "",
            row.get("entrypoint", "") or "",
            state_display,
            row.get("work_pool_type", "") or "",
            (row.get("start_time") or "")[:19],
            dur_str,
        )

    console.print(table)

    if offset + limit < total:
        console.print(f"  [dim]Next page: --offset {offset + limit}[/dim]")


def _rag_colour(rate: float) -> str:
    """Return a rich colour string based on success rate (RAG)."""
    if rate >= 95.0:
        return "green"
    if rate >= 80.0:
        return "yellow"
    return "red"


def _cmd_summary(
    settings_kwargs: dict,
    *,
    since: str | None,
    flow: str | None,
) -> None:
    settings = load_settings(**settings_kwargs)
    db = FlowRunDB(settings.db_path)

    rows = db.get_flow_summary(since=since)
    if flow:
        rows = [r for r in rows if r["flow_name"] == flow]

    if not rows:
        print("No flow runs found.")
        return

    console = Console()
    table = Table(title="Flow Summary", show_lines=True)
    table.add_column("Flow Name", style="bold")
    table.add_column("Runs", justify="right")
    table.add_column("Completed", justify="right", style="green")
    table.add_column("Failed", justify="right", style="red")
    table.add_column("Crashed", justify="right", style="bold red")
    table.add_column("Cancelled", justify="right", style="magenta")
    table.add_column("In-Flight", justify="right", style="cyan")
    table.add_column("Success %", justify="center")
    table.add_column("Avg (s)", justify="right")
    table.add_column("Min (s)", justify="right")
    table.add_column("Max (s)", justify="right")
    table.add_column("Last Run")
    table.add_column("24h", justify="right")
    table.add_column("7d", justify="right")

    for row in rows:
        rate = row["success_rate"]
        colour = _rag_colour(rate)
        rate_display = f"[{colour}]{rate:.1f}%[/{colour}]"

        table.add_row(
            row["flow_name"] or "(unknown)",
            str(row["total_runs"]),
            str(row["completed"]),
            str(row["failed"]),
            str(row["crashed"]),
            str(row["cancelled"]),
            str(row["in_flight"]),
            rate_display,
            str(row["avg_duration_s"] or ""),
            str(row["min_duration_s"] or ""),
            str(row["max_duration_s"] or ""),
            (row["last_run"] or "")[:19],
            str(row["runs_24h"]),
            str(row["runs_7d"]),
        )

    console.print(table)


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        level=level,
    )

    settings_kwargs: dict = {"env_file": args.env_file}
    if args.db:
        settings_kwargs["db_path"] = args.db

    if args.command == "backfill":
        settings = load_settings(**settings_kwargs)
        count = backfill(settings, months=args.months)
        print(f"Backfill complete: {count} flow runs cached.")

    elif args.command == "sync":
        settings = load_settings(**settings_kwargs)
        count = incremental(settings)
        print(f"Sync complete: {count} flow runs upserted.")

    elif args.command == "status":
        _cmd_status(settings_kwargs)

    elif args.command == "list":
        _cmd_list(
            settings_kwargs,
            limit=args.limit,
            offset=args.offset,
            state=args.state,
            flow=args.flow,
        )

    elif args.command == "summary":
        _cmd_summary(settings_kwargs, since=args.since, flow=args.flow)

    elif args.command == "serve":
        from prefect_history.web import create_app

        app = create_app(settings_kwargs)
        import uvicorn

        uvicorn.run(app, host=args.host, port=args.port)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
