# Schema Drift Automation Plan

## Top-Level Overview

**Goal:** When the database team changes a table definition in `movies_db` (new column, changed type, changed nullability, new setting), detect it automatically, have AI propose the matching change in `movies_etl`, raise a merge request, and, only after it is merged, drop and fully reload the affected tables.

**Trigger / input:** A `git diff` of the `.sql` DDL files in `movies_db` between two commits. This diff acts as the changelog. `movies_db` is outside this workspace, so the diff is passed to the tooling as input (a diff file or text), not read directly by Bob.

**Scope (what can drift):**
- **Landing (ldg):** the Pydantic model `Titles` in `movies_etl/db/ldg/titles.py` is the single source of truth. `commons/helpers.py` derives the Iceberg DDL, Spark DDL and Spark `StructType` from it, so a column change is a one-model edit.
- **Staging (stg):** each CTAS `.sql` file in `movies_etl/db/stg/` lists its columns by hand (`s_movies_genres.sql` also hardcodes the genre pivot list). These need an AI-drafted edit and a human review. Tables are registered in `STG_TABLE_CONFIG` in `wrapper_movies_stg.py`.

**Approach:** AI detects and proposes; a human approves the MR; the reload runs only after the merge. The pipeline does not drop or reload anything before approval.

**Non-goals:**
- AI does not auto-merge.
- AI does not drop tables before the MR is merged.
- No changes to the Spark session or catalog configuration (see `AGENTS.md`).
- No support yet for tables other than `titles` and its stg derivatives. The design should not preclude them.

**Flow (text form):**
1. `movies_db` commit lands, producing a DDL diff.
2. Parse the diff into an old and a new normalised schema.
3. Compare the new schema against `Titles` and against the columns referenced by each stg SQL file.
4. Classify the impact: ldg change, stg change, or no ETL impact.
5. AI drafts edits to the `Titles` model and stg SQL, plus an MR description.
6. Pause the pipeline, open the MR, and wait for review and merge.
7. After merge: drop the ldg and stg tables, replay all CDC files in order, re-run all stg CTAS.
8. Validate that the loaded table schema matches the model and the row counts are sane.

## Facts Established From the Codebase

- `load_to_ldg(spark, load_file)` in `movies_etl/wrapper_movies_ldg.py` loads a single CSV per call, builds the read schema as `map_pydantic_to_spark_struct(Titles)` plus an `op` string field, then calls `create_table` and `merge_into` (`join_keys=["movieid"]`, update when `source.op = 'U'`, insert when `source.op = 'I'`).
- `jobs/movies_ldg_job.py` loads only `LOAD1.csv`. The `LOAD2.csv` call is commented out. A full reload therefore needs an ordered replay of all `LOAD*.csv` files, which does not exist yet.
- The CDC CSV header contains `transaction_ts`, which is not a field of `Titles`. The existing code reads the CSV with an explicit schema, so this is already a small drift between `movies_db` and `movies_etl`, and the first diff run should surface it.
- `run_all_stg` iterates `STG_TABLE_CONFIG` and runs `CREATE OR REPLACE TABLE ... USING iceberg AS (...)`, so stg tables are rebuilt in place and a stg drop is not strictly required for CTAS.
- There is no existing drop-table helper and no schema comparison utility.
- `helpers.py` raises `ValueError` for multi-type unions. `X | None` maps to nullable and `Decimal` maps to `decimal(18, 2)`.
- Tests mock `SparkSession`, so new logic should be testable without Spark or Docker. `test_load_to_ldg` is a known failing test unrelated to this work.

## Cross-Cutting Constraints

- Follow `AGENTS.md` and `.bob/rules/basic_rules.md`: use dependencies and the Python version from `pyproject.toml`, and run `uvx ruff format . && uvx ruff check .` after accepted changes.
- Security rules apply:
  - No secrets in code. The MR platform token comes from environment variables.
  - Identifiers taken from the DDL diff must be validated against an allowlist pattern before being used in any SQL string.
  - Structured logging only, with no sensitive data.
  - Any new dependency must be actively maintained and the latest stable version.
- Jobs under `jobs/` run top-level code on import, so new orchestration logic belongs in `movies_etl/` modules and not in the job scripts.

---

## Sub-Tasks

### Sub-Task 1: Define the Changelog Input Contract

**Intent:** Fix exactly what the automation receives, so the later steps have a stable interface.

**Expected Outcomes:**
- A documented contract: the input is a unified `git diff` of `movies_db/**/*.sql` between two commits, plus the two commit identifiers.
- Sample diffs covering: add column, drop column, change type, change nullability, rename column, new table property.
- A documented list of which change types are in scope for v1 and which are rejected or flagged for manual handling.

**Todo List:**
- [ ] Decide the in-scope change types for v1 (add column, type change, nullability change) and the out-of-scope ones (drop and rename flagged for a human).
- [ ] Write the contract: input format, output format of the detection step, error behavior on unparsable input.
- [ ] Create sample diff fixtures under `tests/` for each change type.
- [ ] Record the contract in a short doc in the repo.

**Relevant Context:** `movies_db` DDL files (outside this workspace, so the user supplies samples); `movies_etl/cdc_events/LOAD*.csv` for the CDC header shape.

**Status:** `[ ] pending`

---

### Sub-Task 2: DDL Diff Parser

**Intent:** Turn the raw DDL diff into a normalised, comparable representation of the old and new table schema.

**Expected Outcomes:**
- A module that parses `CREATE TABLE` DDL (before and after) into column name, type, nullability and table-level settings.
- Type normalisation from the source SQL dialect to the types used by `helpers.py` (`str`, `int`, `float`, `Decimal`, `datetime` and so on).
- Unsupported constructs raise a clear `ValueError` and are never silently ignored.

**Todo List:**
- [ ] Confirm the source SQL dialect with sample DDL from `movies_db`.
- [ ] Decide whether to hand-roll a restricted parser or use a maintained SQL parsing library (check maintenance status and licence first).
- [ ] Implement parsing of the old and new DDL from the diff hunks, reconstructing both full file versions where needed.
- [ ] Validate identifiers against an allowlist pattern.
- [ ] Unit tests using the fixtures from Sub-Task 1.

**Relevant Context:** `movies_etl/commons/helpers.py` (type mapping conventions and the `ValueError` pattern); `AGENTS.md` code style (Google-style docstrings, absolute imports, typing).

**Status:** `[ ] pending`

---

### Sub-Task 3: Schema Differ and Impact Classifier

**Intent:** Compare the new `movies_db` schema against the ETL representation and decide which ETL artifacts are affected.

**Expected Outcomes:**
- A function that produces a structured diff report: added, removed, retyped and nullability-changed columns, plus setting changes.
- The ldg comparison uses `Titles.model_json_schema()`, the same source the helpers use.
- The stg comparison scans each registered stg SQL file for the columns it references, using `STG_TABLE_CONFIG` to find them.
- Each change is classified as: ldg impact, stg impact, both, or none.
- Pre-existing drift (such as `transaction_ts` in the CSV) is reported distinctly from the new change.

**Todo List:**
- [ ] Define the report data model (a Pydantic model, consistent with the project's use of Pydantic).
- [ ] Implement the ldg comparison against the model schema.
- [ ] Implement the stg column-reference detection and map it to the affected files.
- [ ] Implement the classification rules.
- [ ] Unit tests for every change type.

**Relevant Context:** `movies_etl/db/ldg/titles.py`; `movies_etl/wrapper_movies_stg.py` (`STG_TABLE_CONFIG`); `movies_etl/db/stg/*.sql`; `commons/helpers.py` (`map_pydantic_to_iceberg_schema` and related functions).

**Status:** `[ ] pending`

---

### Sub-Task 4: AI Proposal Step

**Intent:** Use AI to draft the concrete ETL edits and the MR description from the diff report. The edits are proposals only and are never applied automatically.

**Expected Outcomes:**
- For ldg impact: a drafted edit to the `Titles` model (field name, type, `| None` for nullable) that follows the project conventions.
- For stg impact: drafted edits to the affected `.sql` files, including any change to the hardcoded lists (for example the genre pivot) and new column pass-through in the final `SELECT`.
- An MR description containing the source `movies_db` commits, the diff report, the proposed changes, risk notes, and the required reload steps.
- Constraints enforced on the draft: literal `{` and `}` in SQL are doubled because the SQL is rendered with `str.format`, and only `{catalog_db_table}` is a placeholder.

**Todo List:**
- [ ] Decide the form of the AI step: a Bob skill and rule set run in a Bob session, or a scripted LLM call. Confirm with the user.
- [ ] Write the prompt or skill instructions, including the `AGENTS.md` conventions and the `str.format` brace rule.
- [ ] Define a validation gate on the drafted output: it must parse, `Titles` must still build the DDL without error, and the stg SQL must render through `_load_sql`.
- [ ] Define the fallback when the AI proposal fails validation: report the diff only and flag for a human.

**Relevant Context:** `AGENTS.md` (architecture and style sections); `movies_etl/wrapper_movies_stg.py` (`_load_sql`); `.bob/` for where the Bob skill or rules would live.

**Status:** `[ ] pending`

---

### Sub-Task 5: Pause Pipeline and Open Merge Request

**Intent:** Stop the pipeline when drift is detected and route the proposal through a reviewable MR.

**Expected Outcomes:**
- A drift-detected signal that blocks the next ldg and stg run until the MR is resolved.
- A new branch in `movies_etl` containing the proposed edits, with a pushed commit and an opened MR carrying the generated description.
- No credentials in code; the token comes from environment variables.

**Todo List:**
- [ ] Confirm the MR platform. The repo has an `origin` remote, but the platform has not been verified, and the available PR workflow targets GitHub.
- [ ] Decide how the pause is implemented (a marker/lock file, a job exit code, or an orchestrator gate).
- [ ] Implement branch, commit, push and MR creation.
- [ ] Run `uvx ruff format . && uvx ruff check .` on the proposed changes before committing.
- [ ] Tests for the pause logic with the git and platform calls mocked.

**Relevant Context:** `jobs/movies_ldg_job.py` and `jobs/movies_stg_job.py` (where a gate would hook in, noting that they run top-level code on import); the existing `create_pr_workflow` Bob workflow.

**Status:** `[ ] pending`

---

### Sub-Task 6: Post-Merge Drop and Full Reload Runner

**Intent:** After the MR is merged, rebuild the tables from scratch so the new definition is applied.

**Expected Outcomes:**
- A drop-table helper for the ldg table, with the table name validated and the table fully qualified as `spark_catalog.db.<table>`.
- An ordered replay of every `movies_etl/cdc_events/LOAD*.csv` file (`I` then `U`), so the end state equals the normal incremental path.
- Re-run of `run_all_stg` after the ldg reload.
- The runner only starts after merge. It refuses to run while the drift gate from Sub-Task 5 is still active.

**Todo List:**
- [ ] Add a `drop_table` helper to `commons/iceberg_operations.py`, following its existing patterns (a trusted-identifier f-string, structured logging).
- [ ] Decide how the CDC files are ordered (filename order, or `transaction_ts`) and how a missing or empty directory is handled.
- [ ] Implement the reload orchestration in a new `movies_etl` module and keep the job scripts thin.
- [ ] Decide whether stg tables are explicitly dropped or just rebuilt by `CREATE OR REPLACE`. The default is rebuild only, since `run_stg` already replaces them.
- [ ] Unit tests with a mocked SparkSession.

**Relevant Context:** `movies_etl/wrapper_movies_ldg.py` (`load_to_ldg`); `movies_etl/commons/iceberg_operations.py` (`create_table`, `merge_into`); `jobs/movies_ldg_job.py`; `AGENTS.md` (the ldg then stg run order and the catalog naming).

**Status:** `[ ] pending`

---

### Sub-Task 7: Post-Reload Validation

**Intent:** Prove the reload produced the intended result before the pipeline is released.

**Expected Outcomes:**
- Schema check: the loaded Iceberg table columns and types equal what `Titles` derives, including the new or changed column.
- Row-count check: the ldg count matches the distinct `movieid` count implied by the CDC files, and each stg table is non-empty.
- A pass or fail result is logged in structured form; failure keeps the gate closed.

**Todo List:**
- [ ] Implement the schema comparison between `spark.table(...)` and the model-derived `StructType`.
- [ ] Implement the row-count checks.
- [ ] Wire the result to the gate from Sub-Task 5.
- [ ] Unit tests with a mocked SparkSession.

**Relevant Context:** `commons/helpers.py` (`map_pydantic_to_spark_struct`); `movies_etl/wrapper_movies_stg.py` (`STG_TABLE_CONFIG` for the list of tables to check).

**Status:** `[ ] pending`

---

### Sub-Task 8: Tests, Documentation and Bob Assets

**Intent:** Lock in the behavior and make the workflow repeatable for future drift use cases.

**Expected Outcomes:**
- Tests covering Sub-Tasks 2, 3, 6 and 7, in the existing mocked-Spark style under `tests/`.
- A short runbook describing the end-to-end flow and the manual fallbacks.
- Bob assets in `.bob/` (a skill and/or rules) that encode the detect-and-propose workflow, so other drift cases (for example a new stg table) can reuse it.
- `AGENTS.md` updated with the new modules and the new constraints.

**Todo List:**
- [ ] Review test coverage against the earlier sub-tasks and fill the gaps.
- [ ] Write the runbook.
- [ ] Create the Bob skill and/or rules for the workflow.
- [ ] Update `AGENTS.md`.
- [ ] Run `uv run pytest` and `uvx ruff format . && uvx ruff check .`; the only acceptable failure is the known `test_load_to_ldg`.

**Relevant Context:** `tests/test_helpers_and_iceberg.py`; `.bob/rules/basic_rules.md`; `AGENTS.md`.

**Status:** `[ ] pending`

---

## Open Questions

- Which SQL dialect do the `movies_db` DDL files use, and can sample files be supplied?
- Which platform hosts the merge requests (verify the `origin` remote)?
- How should the pipeline pause be enforced (lock file, exit code, orchestrator)?
- Should the AI proposal step run inside a Bob session, or as a scripted LLM call in CI?
- How should `transaction_ts` be handled: add it to `Titles`, or intentionally exclude it?
