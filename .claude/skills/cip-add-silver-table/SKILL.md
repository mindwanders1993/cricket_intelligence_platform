---
name: cip-add-silver-table
description: "Scaffold a new Bronze → Silver table in the Cricket Intelligence Platform: register the FQN in naming.py, add a transform stub, add a DQ check stub, and wire a task into the relevant DAG. Use when the user says 'add silver table <name>' or 'I need a new entity from the match JSON'."
trigger: /cip-add-silver-table
---

# /cip-add-silver-table

Scaffolds the **boilerplate** for a new Silver-layer table. **Does not** write the actual transform logic — that requires understanding the source data. The skill produces a runnable skeleton with explicit `TODO:` markers; the user (or you, on the next turn) fills in the column extraction, dedup keys, and tests.

## Usage

```
/cip-add-silver-table <bronze_source> <silver_target>     # scaffold pipeline (Polars by default)
/cip-add-silver-table <bronze_source> <silver_target> --engine spark   # for Match-pipeline-style heavy lifts
/cip-add-silver-table <bronze_source> <silver_target> --grain "<key1>,<key2>"   # declare grain upfront
```

Example: `/cip-add-silver-table match_data umpires --grain "match_id,umpire_name"` scaffolds `silver.umpires` from `bronze.match_data`.

## What You Must Do When Invoked

If invoked with `--help` or `-h`, print Usage and stop.

### Step 1 — Validate inputs and auto-pick the pipeline

- `<bronze_source>` must already exist in `TableName.BRONZE_TABLES`. If not, fail with the list of valid Bronze tables.
- `<silver_target>` must NOT already exist in `TableName.SILVER_TABLES`. If it does, fail — adding a duplicate is a different workflow.
- `--engine` defaults to `polars`. Use `spark` only if the user explicitly asked or the bronze source is `match_data` (heavy join/explode work).
- **Auto-pick `<PIPELINE>`** from the bronze source (do NOT ask the user):
  - `people`, `people_identifiers`, `name_variations` → `PIPELINE = people_and_names`
  - `match_data` → `PIPELINE = match_data`

`<PIPELINE>` is then used to resolve the exact files in Step 3 (`build_silver_<PIPELINE>.py`, `dag_build_silver_<PIPELINE>.py`).

### Step 2 — Show the plan, get confirmation

Use `AskUserQuestion` to confirm the scope. This skill **edits multiple files** — surface them first:

```
About to scaffold silver.<target>:
  1. Register name in src/cip/common/contracts/naming.py (SILVER_TABLES frozenset)
  2. Create src/cip/transform/<engine>/silver/<target>.py (transform stub)
  3. Create tests/unit/transform/<engine>/silver/test_<target>.py (test stub)
  4. Add DQ check stub in src/cip/quality/checks/<target>_dq.py
  5. Add a task to orchestration/airflow/dags/dag_build_silver_<pipeline>.py
  6. Append the new task to src/cip/ingestion/jobs/build_silver_<pipeline>.py

Proceed?
```

If the user says no, stop.

### Step 3 — Edit each file with surgical changes

For each file, follow the **Karpathy Surgical Changes** principle: insert the minimum to make the table appear; leave a `TODO:` for actual logic.

**1. `naming.py`** — locate `SILVER_TABLES = frozenset(`, add `"<target>",` to the set alphabetically.

**2. Transform module** — write a class skeleton matching the existing pattern. For Polars (mirror `src/cip/transform/polars/silver/persons.py`):

```python
from __future__ import annotations
import polars as pl
from cip.common.logging import get_logger
from cip.transform.shared.readers import PolarsIcebergReader
from cip.transform.shared.writers import PolarsIcebergWriter

logger = get_logger(__name__)


class Polars<TARGET_PASCAL>SilverTransform:
    """Build silver.<target> from bronze.<source>."""

    def transform(self, snapshot_date: str) -> pl.DataFrame:
        reader = PolarsIcebergReader.from_settings()
        bronze = reader.read("bronze.<source>").filter(
            pl.col("_snapshot_date") == snapshot_date
        )

        # TODO: dedup on grain keys, cast types, explode nested fields.
        # Grain: (<grain_keys>). One row per <natural unit>.
        # Watch out for: <list any non-obvious source quirks>.
        silver = bronze.select(
            # TODO: project columns
        )
        return silver.collect()

    def write(self, df: pl.DataFrame, snapshot_date: str, pipeline_run_id: str) -> None:
        writer = PolarsIcebergWriter.from_settings()
        writer.overwrite_partition(
            df=df,
            table_fqn="silver.<target>",
            partition_cols=["_snapshot_date"],
            pipeline_run_id=pipeline_run_id,
        )
```

For Spark, mirror `src/cip/transform/spark/silver/persons.py` (uses `SparkIcebergWriter.dynamic_overwrite()`).

**3. Test stub** — a single test that the transform produces a non-empty frame for a small fixture. Mark with `@pytest.mark.skip("scaffold — fill in fixture")` so the suite stays green until the user writes it.

**4. DQ check stub** — pattern after `src/cip/quality/checks/people_and_names_dq.py`. Include placeholder checks for: row count > 0, primary key uniqueness on declared grain, no nulls on grain columns.

**5. DAG task** — `orchestration/airflow/dags/dag_build_silver_<pipeline>.py`. Add a `PythonOperator` that calls the new job function; set its dependency to run after the existing Silver tasks (`>>`).

**6. Job wrapper** — `src/cip/ingestion/jobs/build_silver_<pipeline>.py`. Add a `task_build_<target>(snapshot_date, pipeline_run_id, force, **context)` function that instantiates the transform and writes.

### Step 4 — Verify the scaffold compiles

```bash
poetry run python -c "
from cip.common.contracts.naming import TableName
assert '<target>' in TableName.SILVER_TABLES, 'naming registration failed'
from cip.transform.<engine>.silver.<target> import Polars<TARGET_PASCAL>SilverTransform
print('Scaffold imports OK')
"
make dag-validate    # asserts DAG still parses
```

### Step 5 — Tell the user what's left

Print a checklist of **TODOs** the user must finish:

```
Scaffold complete. Files added/modified:
  ✓ naming.py — registered silver.<target>
  ✓ src/cip/transform/<engine>/silver/<target>.py — transform stub
  ✓ tests/unit/transform/<engine>/silver/test_<target>.py — test stub (currently skipped)
  ✓ src/cip/quality/checks/<target>_dq.py — DQ stub
  ✓ orchestration/airflow/dags/dag_build_silver_<pipeline>.py — DAG task wired
  ✓ src/cip/ingestion/jobs/build_silver_<pipeline>.py — job wrapper

TODO:
  1. Fill in transform.select(...) — project the actual columns.
  2. Verify grain uniqueness — recall the fact_delivery multi-wicket lesson.
  3. Write the test fixture and remove the @pytest.mark.skip.
  4. Add dbt source + staging model under models/dbt/models/staging/silver/.
  5. Add the new table to bronze→silver inspect modules in validation/modules/14_silver_inspect.sh.
  6. Run /cip-pipeline-run people-silver|match-silver --task silver to materialise.
```

## Honesty rules

- **Do not** invent the transform logic. The scaffold's body is `TODO:` — that's intentional. Wrong column extraction silently produces broken Silver data.
- **Do not** skip Step 2's confirmation. This touches 5+ files; the user needs to know before changes land.
- If the Bronze source has explode-able nested fields, mention them in the TODO comments but don't try to guess the structure.
- If the user asks you to "also write the transform", point them at `/cip-inspect-table bronze.<source>` first to see the actual schema — guessing produces wrong joins.
