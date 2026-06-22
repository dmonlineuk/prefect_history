# pp_testing

## Dependencies

uv is recommended, either systemwide or it needs installing in the activated environment.

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
. .venv/Scripts/activate # or . .venv/bin/activate on Linux/Mac
python -m pip install uv
uv sync
```

## Getting Started



## Contributions

```shell
uv sync --extra dev
isort .
black .
ruff check .
pytest
```
