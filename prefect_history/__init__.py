"""prefect_history -- cache and query Prefect flow-run history locally.

Quick-start
-----------
::

    from prefect_history import load_settings, backfill, incremental

    settings = load_settings()       # reads .env
    backfill(settings)               # initial 2-month pull
    incremental(settings)            # daily delta + in-flight re-check

See ``prefect_history.sync`` for full details.
"""

from prefect_history.config import Settings, load_settings
from prefect_history.db import FlowRunDB
from prefect_history.sync import backfill, incremental

__all__ = [
    "Settings",
    "FlowRunDB",
    "backfill",
    "incremental",
    "load_settings",
]
