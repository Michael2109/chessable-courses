# chessable-courses

Standard Python project scaffold managed with `uv`.

## Setup

Create/refresh the environment and install deps (dev included by default):

```bash
uv sync
```

## Run tests

```bash
uv run pytest -q
```

## Project layout

- `app/`: main application code
- `tests/`: pytest tests
- `pyproject.toml`: project configuration and dependencies

uv run python -m app.main --csv-path lichess_db_puzzle.csv --min-rating 200 --max-rating 1500 --per-theme 100