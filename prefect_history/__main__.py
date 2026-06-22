"""CLI entry-point: ``python -m prefect_history``."""

from __future__ import annotations

import argparse
import logging
import sys

from prefect_history.config import load_settings
from prefect_history.db import FlowRunDB
from prefect_history.sync import backfill, incremental


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

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
