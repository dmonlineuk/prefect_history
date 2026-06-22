# Prefect History

Cache and browse Prefect flow-run history locally using SQLite.

## Installation

[uv](https://docs.astral.sh/uv/getting-started/installation) is recommended.

```shell
git clone https://github.com/dmonlineuk/prefect_history
cd prefect_history
uv venv
. .venv/Scripts/activate    # Windows
# or . .venv/bin/activate   # Linux / Mac
uv sync
```

## Configuration

Create a `.env` file in the project root:

```env
PREFECT_API_URL=https://api.prefect.cloud/api/accounts/<ACCOUNT-ID>/workspaces/<WORKSPACE-ID>
PREFECT_API_KEY=pnu_...

# Optional
PH_DB_PATH=prefect_history.db
PH_BACKFILL_MONTHS=2
PH_PAGE_SIZE=200
```

## CLI Usage

After installation, the `prefect-history` command is available:

```shell
prefect-history --help
```

### Sync Commands

```shell
# Initial backfill (default: last 2 months)
prefect-history backfill
prefect-history backfill -m 6          # 6-month backfill

# Incremental sync (new runs + re-check in-flight)
prefect-history sync

# Cache statistics and recent sync log
prefect-history status
```

### Browsing Flow Runs (CLI)

```shell
# List cached flow runs in a colour-coded table
prefect-history list
prefect-history list -n 50             # show 50 rows
prefect-history list --state FAILED    # filter by state
prefect-history list --flow etl-pipeline   # filter by flow name
prefect-history list --offset 20       # pagination (skip first 20)
```

### Web Dashboard

```shell
# Launch the web UI at http://127.0.0.1:8000 (default)
prefect-history serve

# Custom host/port (e.g. expose on all interfaces, port 9000)
prefect-history serve --host 0.0.0.0 --port 9000
```

The web dashboard provides:
- Stats overview (total runs, in-flight, last sync time)
- Filter dropdowns for state type and flow name
- Paginated table with colour-coded state badges
- Smooth navigation via HTMX partial updates

### Global Options

```shell
prefect-history --env-file /path/to/.env list   # custom .env location
prefect-history --db /path/to/cache.db list     # custom database path
prefect-history -v sync                         # verbose/debug logging
```

## Python API

```python
from prefect_history import load_settings, backfill, incremental

settings = load_settings()       # reads .env
backfill(settings)               # initial 2-month pull
incremental(settings)            # daily delta + in-flight re-check
```

## Contributions

Please use linter, formatter, and run pytest before committing.

```shell
uv sync --extra dev
isort .
black .
ruff check .
pytest
```
