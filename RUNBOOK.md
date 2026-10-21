# Runbook

Procedures for EnergyPulse. `README.md` describes what each command does; this file says when
to run it and in what order.

## On open

```bash
git status --short
git log -3 --oneline
```

## On close

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src/
uv run pytest -q
```

These need the `dev` extra: `uv sync --all-extras` once per checkout.

## Run

`uv run energypulse run` fills `data/energypulse.duckdb` for one city, then
`uv run streamlit run src/energypulse/dashboard/app.py` opens the dashboard on it.
`uv run energypulse status` says what the database holds without fetching anything.

## Refresh the data

`uv run energypulse ingest` per city with `--days` for the window, then
`uv run energypulse quality`. Re-ingesting upserts by timestamp and location, so overlapping
windows do not duplicate rows; the database is rebuildable from Open-Meteo at any time, so
deleting `data/` is safe.

## Release

1. Bump `version` in `pyproject.toml` and `__version__` in `src/energypulse/__init__.py`,
   then `uv lock` (CI installs with `--locked`).
2. Commit `chore: Release vX.Y.Z`, tag `git tag -a vX.Y.Z`, push `main` and the tag.
3. `gh release create vX.Y.Z` with handwritten notes; mark it latest.

## Deploy

Not deployed. The dashboard runs locally over the local database; there is no hosted copy.

## Restore on a new machine

1. Clone the repo, `uv sync --all-extras`.
2. `uv run pytest`, then `uv run energypulse run` to rebuild the database. No keys.

## Validate

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy src/
uv run pytest
uv run energypulse status
```
