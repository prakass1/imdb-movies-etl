# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Current state (scaffold only)
- Python project; [`pyproject.toml`](pyproject.toml), [`movies_etl/movies_job.py`](movies_etl/movies_job.py) and [`movies_etl/commons/helpers.py`](movies_etl/commons/helpers.py) are all **empty** files.
- No build backend, dependencies, lint/format config (ruff/black/flake8/mypy), tests, git repo, or existing AI rules (CLAUDE.md, .cursorrules, copilot-instructions) exist yet.
- Therefore there are no verified build/lint/test commands, and no single-test command. Do not assume any; add the tooling to `pyproject.toml` first (e.g. a `[tool.pytest.ini_options]` section) and then update this file.

## Layout
- `movies_etl/movies_job.py` – intended ETL job entry point (by name).
- `movies_etl/commons/helpers.py` – intended shared helpers.
- Neither `movies_etl/` nor `movies_etl/commons/` has an `__init__.py`; add them if importing as a package (`from movies_etl.commons.helpers import ...`).

## Conventions
- None established. When adding code, define them in `pyproject.toml` (formatter, linter, type checker) and record the non-obvious ones here.
