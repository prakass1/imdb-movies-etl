# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Commands
- Python/deps come from `pyproject.toml` (`>=3.11`, `pyspark==4.1.0`, `pydantic==2.14.1`); use `uv run ...`.
- Tests: `uv run pytest` · single test: `uv run pytest tests/test_helpers_and_iceberg.py::test_iceberg_ddl_generation`
- Ruff is NOT a project dependency (`uv run ruff` fails); required by `.bob/rules/basic_rules.md` after accepted changes: `uvx ruff format . && uvx ruff check .`
- Type checker is pyrefly (`[tool.pyrefly]` in `pyproject.toml`), no ruff/pytest config exists.
- Tests mock SparkSession (`MagicMock`); no Spark/Docker needed. Known failing: `test_load_to_ldg` (calls `load_to_ldg(mock_spark)` without the required `load_file` arg, and asserts `titles` instead of `spark_catalog.db.titles`).
- Jobs (`jobs/*.py`) run inside the `spark-iceberg` container of `docker-compose.yaml` (REST catalog + RustFS S3), which mounts `movies_etl/` and the two job files into `/home/iceberg/`. Run order: `movies_ldg_job.py` (CDC CSV -> `spark_catalog.db.titles`) then `movies_stg_job.py` (CTAS tables).
- Env vars read by `get_spark_session`: `ICEBERG_REST_URI`, `S3_ENDPOINT`, `AWS_REGION`; `LOG_LEVEL` for logger.

## Architecture (non-obvious)
- Layers: `ldg` (landing, Pydantic model -> Iceberg table + MERGE of CDC rows) -> `stg` (CTAS from `.sql` files).
- Pydantic models in `movies_etl/db/ldg/` are the single source of truth for schema: `commons/helpers.py` derives Iceberg DDL, Spark DDL and Spark `StructType` from `model_json_schema()`. Add a column by editing the model only. `X | None` => nullable; `Decimal` => `decimal(18, 2)`; multi-type unions raise `ValueError`.
- CDC CSVs (`movies_etl/cdc_events/LOAD*.csv`) have an extra `op` column (`I`/`U`) appended to the schema in `wrapper_movies_ldg.py`; `merge_into` defaults to `join_keys="movieid"`, and updates only when `source.op = 'U'`, inserts only when `source.op = 'I'` (as configured by the wrapper).
- Adding an STG table: add a `.sql` in `movies_etl/db/stg/` AND register it in `STG_TABLE_CONFIG` in `wrapper_movies_stg.py`. SQL is rendered with `str.format`, so literal `{`/`}` in SQL must be doubled; only `{catalog_db_table}` is a placeholder.
- All tables are addressed as `spark_catalog.db.<table>` (the catalog is named `spark_catalog` but is a plain `SparkCatalog` of type `rest`). Do NOT switch to `SparkSessionCatalog` (CREATE NAMESPACE then fails) — see comments in `commons/spark_session.py`. `-Daws.region` JVM options there are required, not redundant.
- Job scripts run top-level code on import (no `main()`); don't import them in tests.
- `*.pyc` for cpython-310/315 exist locally; `warehouse/`, `spark-warehouse/`, `metastore_db/`, `derby.log` are git-ignored artifacts.

## Code style
- Imports: absolute from package root (`from movies_etl.commons.logger import get_logger`); no import-sorting config enforced (existing files are mixed — ruff will reorder only if the `I` rule is enabled).
- Typing: modern syntax (`str | None`, `dict[str, str]`, `type[BaseModel]`); public functions have type hints and Google-style docstrings (`Args:`/`Returns:`/`Raises:`).
- Logging: always `logger = get_logger(__name__)` from `commons/logger.py` (JSON to stdout); pass structured data via `extra={...}`, never `print` or f-string secrets. Keys in `extra` must not collide with LogRecord attributes (e.g. `message`, `name`).
- Errors: raise specific exceptions with descriptive messages (`ValueError` for bad args, `KeyError` for unregistered STG table); no blanket try/except.
- Naming: snake_case; wrapper modules named `wrapper_movies_<layer>.py`; Pydantic model/column names mirror the CSV headers in lowercase snake_case (`movieid`, `no_of_votes`); SQL genre columns use `Film_Noir`/`Sci_Fi` style (hyphens replaced by underscores).
- SQL: built via f-strings in `helpers.py`/`iceberg_operations.py` from trusted internal names only — never interpolate user input.
- Legacy aliases at the bottom of `commons/helpers.py` (`map_pydantic_to_iceberg_type`, `get_iceberg_map_type_from_pydantic`) exist for backward compatibility; don't use them in new code.
