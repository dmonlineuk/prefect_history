# pp_testing

## Dependencies

uv is recommended, either system-wide or it needs installing in the activated environment.

Follow the instructions at https://docs.astral.sh/uv/getting-started/installation if installing uv system-wide.

### uv System-wide

```shell
git clone https://github.com/dmonlineuk/pp_testing
cd pp_testing
uv venv
. .venv/Scripts/activate # or . .venv/bin/activate on Linux/Mac
uv sync
```

### uv in Virtual Environment

```shell
git clone https://github.com/dmonlineuk/pp_testing
cd pp_testing
python -m venv .venv
. .venv/Scripts/Activate.ps1 # or . .venv/bin/activate on Linux/Mac
python -m pip install uv
uv sync
```

## Using the package

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

