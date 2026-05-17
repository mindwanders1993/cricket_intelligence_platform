---
name: cip-gold-refresh
description: "Refresh the Gold layer end-to-end: release the DuckDB UI file lock if held, rebuild Bronze+Silver DuckDB tables from Iceberg, then run the dbt models + tests. Use when the user wants the Gold marts re-materialised after Silver changes — e.g. 'refresh gold', 'rebuild marts', 'rerun dbt'."
trigger: /cip-gold-refresh
---

# /cip-gold-refresh

One command that does the three-step Gold dance (stop UI → run refresh job → tell user how to reopen UI). Encodes the non-obvious DuckDB write-lock gotcha that broke us before.

## Why this skill exists

`storage/duckdb/cricket.duckdb` allows a single writer. When `make duckdb-ui` is running, the UI holds a write lock for its internal `_ui` catalog. Any Gold refresh that tries to materialise Bronze/Silver tables fails with a file-lock error until the UI is stopped. This skill takes care of the dance.

## Usage

```
/cip-gold-refresh                          # full refresh: bootstrap + Bronze + Silver tables + dbt run + dbt test
/cip-gold-refresh --no-test                # skip dbt tests (faster)
/cip-gold-refresh --select <dbt_selector>  # run only matching dbt models (e.g. tag:gold or marts.dimensions)
/cip-gold-refresh --no-stop-ui             # error out instead of stopping the UI (paranoid mode)
```

## What You Must Do When Invoked

If invoked with `--help` or `-h` (no other args), print the Usage block above and stop.

### Step 1 — Check for the DuckDB UI lock

```bash
pgrep -f "duckdb .*-ui" >/dev/null 2>&1 && echo "ui-running" || echo "ui-not-running"
```

- If `ui-not-running`: continue to Step 3.
- If `ui-running` and `--no-stop-ui` was passed: stop with an error explaining the user must run `make duckdb-stop` themselves first.
- If `ui-running` and `--no-stop-ui` was NOT passed: tell the user "DuckDB UI is running — stopping it to release the file lock" and proceed to Step 2.

### Step 2 — Stop the DuckDB UI

```bash
make duckdb-stop
```

### Step 3 — Run the Gold refresh

The Gold job module (`cip.ingestion.jobs.run_gold_dbt_models`) has no `__main__` block — it's Airflow-callable only. Invoke its two task functions via `python -c`:

```bash
ICEBERG_REST_URI=http://localhost:8181 \
MINIO_S3_ENDPOINT=http://localhost:9000 \
POSTGRES_HOST=localhost \
poetry run python -c "
from cip.ingestion.jobs.run_gold_dbt_models import task_refresh_duckdb_views, task_run_dbt
task_refresh_duckdb_views()
task_run_dbt(dbt_select=<SELECTOR_OR_NONE>, dbt_test=<TEST_FLAG>)
"
```

Where:
- `<SELECTOR_OR_NONE>` = the value of `--select` if given, else `None` (literal Python None, no quotes).
- `<TEST_FLAG>` = `False` if `--no-test` was passed, else `True`.

Run in the foreground — users want to watch dbt progress live. Time budget: ~30–60s on this corpus.

### Step 4 — Report and offer to reopen the UI

On success, print a short summary:
```
Gold refresh complete.
  - DuckDB tables rebuilt: bronze.*, silver.*
  - dbt run: <N models materialised>
  - dbt test: <PASS|N failed>
```
(Extract the model/test counts from the dbt stdout — they're in the summary lines.)

Then offer: "Reopen the DuckDB UI with `make duckdb-ui`?"

Do NOT spawn `make duckdb-ui` yourself — it's an interactive command that blocks the terminal, and the user may want to do other things first.

On failure: surface the full traceback and stop. Common failure modes to diagnose for the user:
- `IO Error: Could not set lock` — the UI wasn't actually stopped, or another process holds it. Suggest `lsof storage/duckdb/cricket.duckdb`.
- `CatalogException: Table ... not found` — Silver Iceberg is empty for the latest snapshot. Suggest running `/cip-pipeline-run people-silver` or `match-silver` first.
- `dbt: command not found` — run `poetry install` to fetch dbt-duckdb.

## Pre-flight checks

Before Step 3, verify:
- `docker ps --filter name=compose-iceberg-rest-1 --filter status=running -q` is non-empty. The refresh reads Iceberg via the REST catalog; if it's down, fail fast.
- `storage/duckdb/` directory exists (create with `mkdir -p storage/duckdb` if not — empty is fine, refresh creates the file).

## Honesty rules

- Always tell the user before stopping their UI session. Stopping closes any open SQL editor tabs in the browser.
- Never assume the previous refresh succeeded. Always run both `task_refresh_duckdb_views` and `task_run_dbt`.
- Do not auto-restart the UI. Hand control back to the user.
