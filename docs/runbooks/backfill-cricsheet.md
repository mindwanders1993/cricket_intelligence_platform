# Cricsheet Backfill Runbook

> Full historical match backfill via `ingest_all_match_data_bronze` (~21,600 matches in `all_json.zip`).
> Pair with `docs/runbooks/local-bootstrap.md` (first-time setup) and `docs/jobs.md` (module-level reference).

---

## When to run

- After a fresh `make bootstrap` (no `bronze.match_data` rows yet).
- After a schema change that requires full Silver/Gold rebuild.
- After a Bronze data-quality issue that needs full re-ingestion (use `force=True`).
- Periodically (monthly) to catch retroactive corrections published by Cricsheet.

The daily incremental DAG (`ingest_two_day_match_data_bronze`) covers normal new-match flow — backfill is a heavier operation.

---

## Expected runtime + resource footprint

| Phase | Wall time | Notes |
|---|---|---|
| `download` | ~5–10 min | Network-bound; HTTP Range resume + 5-retry backoff |
| `extract` | ~5–10 min | CPU-bound; ZIP → ~21k JSON files in MinIO |
| `bronze` | ~10–15 min | Polars + audit-driven dedup; appends to `bronze.match_data` |
| `dq` | ~1 min | Row count + structural checks |
| Auto-triggered Silver (full) | ~20–30 min | PySpark explosion to 11 Silver tables |
| Auto-triggered Gold (full-refresh) | ~5–10 min | dbt + 50 tests |
| **Total end-to-end** | **~45–75 min** | Watch via Airflow Grid view |

Disk usage:
- `cricket-source-files/match_data/zip/snapshot_date=<date>/all_json.zip` — ~150 MB
- `cricket-source-files/match_data/json/snapshot_date=<date>/archive=all_json/*.json` — ~3 GB
- `cricket-lakehouse/bronze/match_data/` — ~600 MB Parquet
- DuckDB file after Gold — ~500 MB

RAM:
- Bronze: Polars memory peaks around 4 GB during JSON parse
- Silver: PySpark — set `SPARK_DRIVER_MEMORY=8g` (already in `compose.dev.yml`)

---

## Pre-flight checklist

```bash
# 1. Stack must be up + healthy
docker ps --filter "name=compose-" --format "table {{.Names}}\t{{.Status}}"

# 2. Custom Airflow image must be built
docker images | grep airflow

# 3. DAGs must import clean
make dag-validate

# 4. Stop Metabase before backfill — the auto-trigger chain reaches Gold which needs the DuckDB write lock
docker stop compose-metabase-1

# 5. (Optional) Stop DuckDB UI if running
make duckdb-stop
```

---

## Phase 1 — Trigger the backfill

### From Airflow UI

1. http://localhost:8080 → log in.
2. `ingest_all_match_data_bronze` → unpause if needed.
3. ▶ **Trigger DAG w/ config**:
   ```json
   {"snapshot_date": "2026-05-17"}
   ```
   (Use today's date or any past date — backfill is date-agnostic; the date stamps `_snapshot_date` on Bronze rows.)
4. Click Trigger.

### From the shell (alternative)

```bash
docker exec compose-airflow-scheduler-1 \
  airflow dags trigger ingest_all_match_data_bronze \
  --conf '{"snapshot_date": "2026-05-17"}'
```

---

## Phase 2 — Watch progress

### Airflow Grid view

- Bronze DAG: 4 tasks (`download → extract → bronze → dq`) + `trigger_silver`.
- Silver DAG: appears in Grid once trigger_silver fires (within ~1 min of Bronze end).
- Gold DAG: appears once Silver finishes.

Failed task → click red square → **Logs** tab.

### Control schema (pgAdmin / SQL)

While the DAG runs, useful queries via Tools → Query Tool:

```sql
-- Current archive download state
SELECT * FROM control.archive_download_log ORDER BY started_at DESC LIMIT 5;

-- Bronze ingestion progress
SELECT
    archive_file,
    started_at,
    finished_at,
    rows_loaded,
    status
FROM control.bronze_match_ingestion_log
ORDER BY started_at DESC
LIMIT 5;

-- Per-file audit (which match_ids have been Bronze-loaded)
SELECT
    status,
    COUNT(*) AS files,
    COUNT(DISTINCT match_id) AS distinct_matches
FROM control.match_file_audit
WHERE dag_id = 'ingest_all_match_data_bronze'
GROUP BY status;

-- DQ failures (should be empty during a clean run)
SELECT * FROM control.v_dq_failures ORDER BY started_at DESC;
```

### MinIO

Browse `cricket-source-files/match_data/json/snapshot_date=<date>/archive=all_json/` — file count should climb during `extract` and stop at ~21,600.

---

## Phase 3 — Post-backfill verification

After the entire chain (Bronze → Silver → Gold) finishes green:

### Row counts

In **DuckDB UI** (`make duckdb-ui`):

```sql
SELECT 'bronze.match_data'   AS t, COUNT(*) FROM bronze.match_data
UNION ALL SELECT 'silver.matches',     COUNT(*) FROM silver.matches
UNION ALL SELECT 'silver.deliveries',  COUNT(*) FROM silver.deliveries
UNION ALL SELECT 'silver.wickets',     COUNT(*) FROM silver.wickets
UNION ALL SELECT 'gold.fact_delivery', COUNT(*) FROM gold.fact_delivery
UNION ALL SELECT 'gold.dim_player',    COUNT(*) FROM gold.dim_player;
```

Expected magnitudes (will drift over time as Cricsheet adds matches):

| Table | ~Rows |
|---|---|
| `bronze.match_data` | ~21,600 (= match count for that snapshot) |
| `silver.matches` | ~21,600 |
| `silver.deliveries` | ~11M |
| `silver.wickets` | ~430k |
| `gold.fact_delivery` | ~11M |
| `gold.dim_player` | ~12k |

### Validation harness

Paste `analysis/validation_queries.sql` into the DuckDB UI (9 sections, ~30 queries). Section 7.4 expects a small non-zero wicket diff (10 multi-wicket deliveries with 2 wickets + 1 with 10 wickets — see CLAUDE.md "Edge cases to know").

Make sure to **close the DuckDB UI** (`make duckdb-stop`) before triggering anything else.

### Restart Metabase

```bash
docker start compose-metabase-1
```

Then http://localhost:3000 — verify the "Cricket Universe" dashboard counters reflect the new totals.

---

## Failure modes + recovery

### Download fails (network)

```
control.archive_download_log → status=FAILED
control.bronze_match_ingestion_log → empty (download didn't reach Bronze)
```

Just re-trigger the DAG. Download supports HTTP Range resume + 5-retry exponential backoff. If it still fails after the second attempt, check Cricsheet.org manually.

### Extract fails mid-zip

```
MinIO has partial JSON files at archive=all_json/
control.archive_download_log → status=SUCCESS (download done)
control.bronze_match_ingestion_log → empty
```

Re-trigger with `{"force": true}`. The extract task overwrites the existing prefix.

### Bronze fails mid-batch

```
control.match_file_audit → some rows status=SUCCESS, some empty
Bronze Iceberg table → partial appends committed (atomic per-batch)
```

Re-trigger the DAG (without `force`). The `match_file_audit` lookup skips already-loaded files and continues from where it stopped. **No need for `force`** — append-only Bronze + audit dedup is idempotent.

### Silver Spark OOM

```
silver.* Iceberg tables → partial snapshots may exist
```

Increase `SPARK_DRIVER_MEMORY` in `compose.dev.yml` (default 8g → try 12g if you have RAM). Re-trigger the silver DAG; `dynamic_overwrite` replaces affected partitions atomically — idempotent.

### Gold DuckDB lock conflict

```
Gold DAG fails on refresh_duckdb_views task: "file is locked"
```

Stop the offender:
```bash
docker stop compose-metabase-1   # Metabase reader
make duckdb-stop                  # DuckDB UI
# stop dashboard dev server with Ctrl-C
```

Re-trigger the Gold DAG (or just `make refresh-gold` which encapsulates this).

### dbt test fails

```
Gold DAG fails on dbt_test task
```

Click into the failed task logs. Common causes:
- Bronze had bad data → trace upstream via `control.dq_results`
- Silver had grain drift (multi-wicket delivery) → run `analysis/validation_queries.sql` section 5 to find the bad rows
- Custom test `fact_player_of_match_unique_grain` fails → look for duplicate player names in `player_of_match` array (data artefact in source JSON)

Use the `/cip-diagnose-dag` skill for a structured analysis.

---

## Forcing a full re-ingestion

If you need to wipe and re-ingest (schema change, data fix, debugging):

```json
{"snapshot_date": "2026-05-17", "force": true}
```

`force=True` propagates via Jinja from `dag_run.conf` to every task callable. Download re-downloads, extract overwrites, Bronze ignores existing audit rows (re-INSERTs with revision bump), Silver `dynamic_overwrite` replaces partitions, Gold full-refresh rebuilds.

Be patient — full-force takes the upper end of the runtime range (75 min).

---

## After the backfill

- Resume the daily incremental DAG: `ingest_two_day_match_data_bronze` should run automatically every 02:00 UTC.
- Add the date to `docs/runbooks/last-backfill.md` (informal log; create if missing).
- If schema changed, re-run dbt docs: `cd models/dbt && dbt docs generate`.

---

## References

- `docs/jobs.md` — per-module CLI invocations
- `docs/runbooks/full-rebuild.md` — wipe + reboot the whole stack
- `docs/runbooks/recover-failed-dag.md` — detailed recovery procedures
- `docs/runbooks/dashboard.md` — DuckDB lock coordination protocol
- `CLAUDE.md` — match ingestion pipeline gotchas
- The `/cip-diagnose-dag` skill for structured failure analysis
