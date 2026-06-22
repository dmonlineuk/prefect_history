"""Configuration for the prefect_history module.

Loads settings from .env and exposes them as a typed dataclass.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

_DEFAULT_DB_PATH = "prefect_history.db"
_DEFAULT_BACKFILL_MONTHS = 2
_DEFAULT_PAGE_SIZE = 200


@dataclass(frozen=True)
class Settings:
    """Immutable runtime settings."""

    prefect_api_url: str
    prefect_api_key: str
    db_path: str = _DEFAULT_DB_PATH
    backfill_months: int = _DEFAULT_BACKFILL_MONTHS
    page_size: int = _DEFAULT_PAGE_SIZE


def load_settings(
    env_file: str | Path | None = ".env",
    *,
    db_path: str | None = None,
    backfill_months: int | None = None,
    page_size: int | None = None,
) -> Settings:
    """Build a ``Settings`` instance from environment variables.

    Parameters
    ----------
    env_file:
        Path to the ``.env`` file. Pass ``None`` to skip loading a file
        (useful when variables are already exported).
    db_path:
        Override for the SQLite database path.
    backfill_months:
        Override for the initial backfill window (in months).
    page_size:
        Override for the Prefect API pagination size.
    """
    if env_file is not None:
        load_dotenv(env_file, override=False)

    api_url = os.getenv("PREFECT_API_URL", "")
    api_key = os.getenv("PREFECT_API_KEY", "")

    if not api_url:
        raise ValueError(
            "PREFECT_API_URL is not set. "
            "Add it to your .env file or export it as an environment variable."
        )
    if not api_key:
        raise ValueError(
            "PREFECT_API_KEY is not set. "
            "Add it to your .env file or export it as an environment variable."
        )

    # Ensure the env vars are set so Prefect's get_client() picks them up.
    # load_dotenv(override=False) only writes to os.environ if the key is
    # missing, but if the user constructed Settings directly we still need
    # these exported for the Prefect SDK.
    os.environ.setdefault("PREFECT_API_URL", api_url)
    os.environ.setdefault("PREFECT_API_KEY", api_key)

    return Settings(
        prefect_api_url=api_url,
        prefect_api_key=api_key,
        db_path=db_path or os.getenv("PH_DB_PATH", _DEFAULT_DB_PATH),
        backfill_months=backfill_months
        or int(os.getenv("PH_BACKFILL_MONTHS", str(_DEFAULT_BACKFILL_MONTHS))),
        page_size=page_size or int(os.getenv("PH_PAGE_SIZE", str(_DEFAULT_PAGE_SIZE))),
    )
