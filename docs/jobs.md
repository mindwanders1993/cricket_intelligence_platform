# Job Module Reference

All pipeline entry points live in `src/cip/ingestion/jobs/`. Each module is invoked via `python -m cip.ingestion.jobs.<module>` with a `--task` flag. Airflow-callable functions are thin wrappers around the same logic.

---

## 1. `full_load_match_data` — Monthly full backfill (Bronze)

**Source:** `all_json.zip` (~21,600+ matches)
**DAG:** `ingest_all_match_data_bronze` → auto-triggers `ingest_all_match_data_silver` → auto-triggers `ingest_all_match_data_gold`
**Schedule:** None (manual)

| Task | What it does |
|---|---|
| `download` | Downloads `all_json.zip` from Cricsheet to MinIO landing zone. Streams with HTTP Range resume + 5-retry exponential backoff. |
| `extract` | Extracts JSON files from ZIP into `match_data/json/snapshot_date={date}/archive=all_json/` in MinIO. |
| `bronze` | Reads extracted JSONs from MinIO, stamps meta columns, appends to `bronze.match_data` Iceberg table with revision tracking. |
| `dq` | Runs DQ checks on the Bronze `match_data` snapshot (row counts, nulls, expected columns). |
| `silver` | Idempotent guard only — actual Silver build is handled by `ingest_all_match_data_silver` DAG. |
| `all` | Runs all tasks in sequence. |

**Manual run:**
```bash
ICEBERG_REST_URI=http://localhost:8181 MINIO_S3_ENDPOINT=http://localhost:9000 POSTGRES_HOST=localhost \
  poetry run python -m cip.ingestion.jobs.full_load_match_data --task all
```

---

## 2. `incremental_match_data` — Daily incremental (Bronze)

**Source:** `recently_added_2_json.zip` (~30 matches added in the last 2 days)
**DAG:** `ingest_two_day_match_data_bronze` → auto-triggers `ingest_two_day_match_data_silver` → auto-triggers `ingest_two_day_match_data_gold`
**Schedule:** Daily 02:00 UTC

| Task | What it does |
|---|---|
| `download` | Downloads `recently_added_2_json.zip` from Cricsheet to MinIO landing zone. |
| `extract` | Extracts JSON files into `match_data/json/snapshot_date={date}/archive=recently_added_2_json/`. |
| `bronze` | Appends extracted JSONs to `bronze.match_data`; same revision-tracking logic as full load. |
| `dq` | DQ checks on the incremental Bronze snapshot. |
| `silver` | Idempotent guard only — actual Silver build is handled by `ingest_two_day_match_data_silver` DAG. |
| `all` | Runs all tasks in sequence. |

**Manual run:**
```bash
ICEBERG_REST_URI=http://localhost:8181 MINIO_S3_ENDPOINT=http://localhost:9000 POSTGRES_HOST=localhost \
  poetry run python -m cip.ingestion.jobs.incremental_match_data --task all
```

---

## 3. `build_silver_match_data` — Bronze → Silver (match pipeline)

**Engine:** PySpark + Iceberg
**DAG:** `ingest_all_match_data_silver` / `ingest_two_day_match_data_silver`
**Schedule:** Triggered automatically by the respective bronze DAG, or run manually.

| Task | What it does |
|---|---|
| `check_bronze_ready` | Verifies `bronze.match_data` has rows for the target `snapshot_date` before starting Spark. |
| `build_silver` | Spark job that reads `bronze.match_data`, deduplicates by `MAX(revision) per match_id`, explodes nested JSON into typed Silver tables: `silver.matches`, `silver.deliveries`, `silver.wickets`, `silver.match_players`, etc. Inserts pending match_ids into `control.match_file_audit`. |
| `dq` | DQ checks on all Silver match tables (row count, grain uniqueness, null checks). Writes results to `control.silver_dq_log`. |

**Manual run:**
```bash
ICEBERG_REST_URI=http://localhost:8181 MINIO_S3_ENDPOINT=http://localhost:9000 POSTGRES_HOST=localhost \
  SPARK_DRIVER_MEMORY=8g SPARK_MASTER=local[2] \
  poetry run python -m cip.ingestion.jobs.build_silver_match_data --snapshot-date 2026-05-01 --task all
```

---

## 4. `ingest_people_and_names` — Register pipeline (Landing → Bronze)

**Source:** `people.csv` + `names.csv` from Cricsheet
**Engine:** Polars + PyIceberg
**DAG:** `ingest_people_and_names_bronze` → auto-triggers `ingest_people_and_names_silver`
**Schedule:** Weekly Sun 00:30 UTC

| Task | What it does |
|---|---|
| `download` | Downloads `people.csv` and `names.csv` to MinIO landing zone. |
| `bronze` | Reads CSVs as all-string Polars DataFrames, stamps meta columns, writes to `bronze.people` and `bronze.names` Iceberg tables. Unpivots `key_*` columns in `people.csv` to long-form `bronze.people_identifiers`. |
| `all` | Runs download → bronze in sequence. |

**Manual run:**
```bash
poetry run python -m cip.ingestion.jobs.ingest_people_and_names --task all
poetry run python -m cip.ingestion.jobs.ingest_people_and_names --snapshot-date 2026-05-11 --task download
poetry run python -m cip.ingestion.jobs.ingest_people_and_names --snapshot-date 2026-05-10 --task bronze --force
```

---

## 5. `build_silver_people_and_names` — Register pipeline (Bronze → Silver)

**Engine:** Polars + PyIceberg
**DAG:** `ingest_people_and_names_silver`
**Schedule:** Weekly Sun 01:30 UTC (1h after bronze DAG), or triggered automatically by `ingest_people_and_names_bronze`

| Task | What it does |
|---|---|
| `silver` | Reads `bronze.people` + `bronze.names`, applies type coercion and normalisation, writes to `silver.people` + `silver.names` Iceberg tables via `PolarsIcebergWriter.overwrite_partition()`. |
| `dq` | DQ checks on `silver.people` + `silver.names`: row counts, null checks on `person_id`, name uniqueness. Writes to `control.silver_dq_log`. |
| `dbt` | Runs `dbt run --select dim_player player_display_names` to refresh Gold player dimension from updated Silver. |
| `all` | Runs silver → dq → dbt in sequence. |

**Manual run:**
```bash
poetry run python -m cip.ingestion.jobs.build_silver_people_and_names --task all
poetry run python -m cip.ingestion.jobs.build_silver_people_and_names --snapshot-date 2026-05-11 --task silver
poetry run python -m cip.ingestion.jobs.build_silver_people_and_names --snapshot-date 2026-05-11 --task dq
```

---

## 6. `run_gold_dbt_models` — Gold layer (DuckDB + dbt)

**DAGs:** `ingest_two_day_match_data_gold` (incremental) · `ingest_all_match_data_gold` (full rebuild)
**Schedule:** Both `None` — triggered automatically by their respective silver DAGs, or manually.
**Lock note:** Gold writes to DuckDB. Stop Metabase before triggering: `docker stop compose-metabase-1`

These are Airflow-callable functions, not a `--task` CLI. Each function is a `PythonOperator` task in one or both Gold DAGs.

| Function | DAGs | What it does |
|---|---|---|
| `task_refresh_duckdb_views` | Both | Calls `DuckDBRefresh.bootstrap()`, then materialises Bronze + Silver + control tables in DuckDB as native tables from Iceberg snapshots filtered to `MAX(_snapshot_date)`. |
| `task_run_dbt_incremental` | `ingest_two_day_match_data_gold` | Runs `dbt seed` then `dbt run` (incremental). dbt's `is_incremental()` filters dims/facts to `control.match_file_audit WHERE gold_loaded_at IS NULL`. |
| `task_run_dbt_full_refresh` | `ingest_all_match_data_gold` | Runs `dbt seed` then `dbt run --full-refresh`. Rebuilds all Gold tables from scratch. |
| `task_run_dbt_test` | Both | Runs `dbt test` against all Gold models (50 tests as of 2026-05-19). Fails the DAG if any test fails. |
| `task_mark_gold_loaded_pending` | `ingest_two_day_match_data_gold` | Stamps `gold_loaded_at = NOW()` in `control.match_file_audit` for all match_ids that had `gold_loaded_at IS NULL` at the start of the run. |
| `task_mark_gold_loaded_all_silver` | `ingest_all_match_data_gold` | Stamps `gold_loaded_at = NOW()` for all Silver match_ids (full-refresh catch-all). |

**Manual trigger (Airflow CLI):**
```bash
# Stop Metabase first
docker stop compose-metabase-1

# Incremental (normal after Silver build):
airflow dags trigger ingest_two_day_match_data_gold

# Full rebuild (after schema changes or volume wipe):
airflow dags trigger ingest_all_match_data_gold

# After DAG completes, restart Metabase:
docker start compose-metabase-1
```

**Manual run (no Airflow):**
```bash
cd models/dbt && poetry run dbt seed
cd models/dbt && poetry run dbt run
cd models/dbt && poetry run dbt test
```

---

## Pipeline auto-trigger chains

```
ingest_people_and_names_bronze  (Sun 00:30 UTC)
  └─► ingest_people_and_names_silver  (or Sun 01:30 UTC scheduled)

ingest_all_match_data_bronze  (manual)
  └─► ingest_all_match_data_silver  (manual / triggered)
        └─► ingest_all_match_data_gold  (manual / triggered)

ingest_two_day_match_data_bronze  (daily 02:00 UTC)
  └─► ingest_two_day_match_data_silver  (triggered)
        └─► ingest_two_day_match_data_gold  (triggered)
```

All triggers use `wait_for_completion=False` — each DAG shows its own success/failure independently and can be re-triggered standalone.

---

## Quick reference

| Module | Layer | Engine | DAG(s) |
|---|---|---|---|
| `full_load_match_data` | Landing → Bronze | Polars + PyIceberg | `ingest_all_match_data_bronze` |
| `incremental_match_data` | Landing → Bronze | Polars + PyIceberg | `ingest_two_day_match_data_bronze` |
| `build_silver_match_data` | Bronze → Silver | PySpark + Iceberg | `ingest_all_match_data_silver` / `ingest_two_day_match_data_silver` |
| `ingest_people_and_names` | Landing → Bronze | Polars + PyIceberg | `ingest_people_and_names_bronze` |
| `build_silver_people_and_names` | Bronze → Silver | Polars + PyIceberg | `ingest_people_and_names_silver` |
| `run_gold_dbt_models` | Silver → Gold | DuckDB + dbt | `ingest_all_match_data_gold` / `ingest_two_day_match_data_gold` |
