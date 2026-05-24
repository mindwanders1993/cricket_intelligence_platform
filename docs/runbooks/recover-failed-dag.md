# Recover a Failed DAG — Cricket Intelligence Platform

> Procedures for diagnosing and recovering from a failed Airflow DAG. The `/cip-diagnose-dag` skill automates most of step 2.

---

## When this applies

- Airflow Grid view shows a red task square.
- Auto-trigger chain stops (Bronze done, Silver/Gold didn't fire — usually means Bronze's trigger task failed silently).
- A schedule was missed (DAG paused or scheduler stuck).
- DQ check raised BLOCK severity, halting downstream.

---

## Step 1 — Identify the failing run

In Airflow UI (http://localhost:8080):

1. Find the DAG → click into its **Grid** view.
2. The latest failed run shows a red square. Click the failed task.
3. Note the **run_id** (`scheduled__2026-05-17T...` or `manual__2026-05-17T...`).

From the shell:

```bash
# Most recent failed runs
docker exec compose-airflow-scheduler-1 \
  airflow dags list-runs -d <dag_id> --state failed --limit 5
```

---

## Step 2 — Use `/cip-diagnose-dag` (fast path)

If you have the project skill enabled, just say in chat:
> `/cip-diagnose-dag <dag_id> <run_id>`

The skill pulls:
- Task states for that run
- Filesystem logs for failed tasks
- The matching row in `control.<dag>_log` (audit table)
- Landing artifacts in MinIO for the snapshot
- A pattern match against known failure modes

It returns a structured root-cause + retry strategy. Stop here if the skill identified the issue.

---

## Step 3 — Manual diagnosis (when the skill isn't enough)

### 3a. Pull task logs

```bash
# Show logs for the failed task
docker exec compose-airflow-scheduler-1 \
  airflow tasks logs <dag_id> <task_id> <run_id>
```

Or in the UI: click the failed task → **Logs** tab.

### 3b. Check the matching audit row

Open pgAdmin (http://localhost:5050) → Query Tool. Lookup table depends on which DAG failed:

| DAG | Lookup table | Useful filter |
|---|---|---|
| `ingest_people_and_names_bronze` | `control.register_ingestion_log` | `WHERE pipeline_run_id = '<run_id>'` |
| `ingest_all_match_data_bronze` / `ingest_two_day_match_data_bronze` | `control.archive_download_log`, `control.bronze_match_ingestion_log`, `control.match_file_audit` | `WHERE pipeline_run_id = '<run_id>'` |
| `ingest_*_silver` | `silver_dq_log` (in same control schema) | `WHERE pipeline_run_id = '<run_id>'` |
| `ingest_*_gold` | `control.dq_results` (dbt-test failures land here) | `WHERE pipeline_run_id = '<run_id>'` |
| (all) | `control.v_dq_failures` view | latest failures cross-table |

### 3c. Check the landing artifacts

In MinIO Console (http://localhost:9001), browse the snapshot's prefix:

```
cricket-source-files/match_data/json/snapshot_date=<date>/archive=<archive_stem>/
```

Expect ~30 JSON files (incremental) or ~21,000 (full backfill). If the files aren't there → download/extract task failed.

---

## Step 4 — Pattern match against known failure modes

| Symptom | Likely cause | Fix |
|---|---|---|
| Download task → "Connection reset" / "503" | Cricsheet flaky | Re-run DAG — retry is built in |
| Extract task → "BadZipFile" | Download was truncated | Re-run DAG with `{"force": true}` — overwrites downloaded artifact |
| Bronze task → "Maven download timeout" | Iceberg JARs not in custom image cache | Rebuild custom image: `make build-airflow` |
| Bronze task → "Iceberg REST 500" | Iceberg REST catalog flaky | `docker restart compose-iceberg-rest-1` then re-run DAG |
| Bronze task → "control.match_file_audit constraint violation" | Re-running over partial Bronze load | This is fine — audit dedup handles it; check `match_file_audit` for orphan rows |
| Silver task → "java.lang.OutOfMemoryError" | Spark driver OOM | Increase `SPARK_DRIVER_MEMORY` in `compose.dev.yml` (8g → 12g); re-trigger silver DAG |
| Silver task → "schema mismatch" | Bronze had new column not yet seen in Silver | Silver `dynamic_overwrite` should handle in-place; re-run; if persistent, re-run with `force=True` |
| Silver task → "MAX(revision) returns null" | No Bronze data for that snapshot_date | Re-run Bronze first |
| Gold `refresh_duckdb_views` → "file is locked" | Metabase / DuckDB UI / dashboard holds the lock | `docker stop compose-metabase-1 ; make duckdb-stop` ; re-trigger Gold |
| Gold `dbt_run` → "model not found" | Source not refreshed in DuckDB | `refresh_duckdb_views` task didn't complete — check its log; re-run Gold |
| Gold `dbt_test` → uniqueness fail on `fact_delivery` | Multi-wicket delivery + un-deduped wickets CTE | Check `analysis/validation_queries.sql` section 5; likely a Silver bug |
| Gold `dbt_test` → "fact_player_of_match_unique_grain" fail | Duplicate names in source `player_of_match` array | Data-quality artifact; dbt QUALIFY ROW_NUMBER() in the model should dedup — check model file |
| Trigger Silver → "DagRunNotFound" | Silver DAG not unpaused | Unpause Silver DAG in UI; manual trigger |
| Anything `pydantic.ValidationError` | Settings drift after `.env` change | `make down ; make up` to re-read env |
| Anything `MinIO returns 403` | Credentials drift | Verify `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` in `.env` match container env |

---

## Step 5 — Retry strategy

Most pipelines are idempotent — re-trigger and the audit-driven dedup handles partial state.

### Standard re-trigger

```
Airflow UI → DAG → ▶ Trigger DAG w/ config → {"snapshot_date": "<original-date>"}
```

Or from the shell:

```bash
docker exec compose-airflow-scheduler-1 \
  airflow dags trigger <dag_id> --conf '{"snapshot_date": "<date>"}'
```

### Force re-trigger (bypass idempotency)

Use when you need to re-process from scratch (e.g., schema fix, bad data):

```json
{"snapshot_date": "<date>", "force": true}
```

`force=True` propagates via Jinja from `dag_run.conf` to every task callable.

### Clear & re-run a specific task (Airflow UI)

If only one task failed and downstream tasks shouldn't re-run:

1. Grid view → click failed task → **Clear** button.
2. Choose "Past" only if you want to wipe state for earlier runs (rare).
3. Click Clear → task is reset to "no_status" → scheduler picks it up.

---

## Step 6 — Special recovery scenarios

### 6a. Stale `control.match_file_audit` after manual deletion of Bronze data

If you manually deleted rows from `bronze.match_data` (e.g., via DuckDB UI or Iceberg snapshot rollback), the audit table thinks those files are still loaded. Recovery:

```sql
-- Identify the orphan rows
SELECT match_id, file_sha256, revision, status, bronze_loaded_at
FROM control.match_file_audit
WHERE dag_id = 'ingest_all_match_data_bronze'
  AND match_id NOT IN (SELECT DISTINCT match_id FROM /* DuckDB bronze.match_data */)
LIMIT 20;

-- Reset them (so Bronze re-ingests)
UPDATE control.match_file_audit
SET status = 'PENDING', bronze_loaded_at = NULL
WHERE match_id IN (<list>);
```

Then re-trigger the Bronze DAG.

### 6b. DuckDB file corruption

Symptoms: dbt errors with "Catalog Exception" or unexpected NULLs.

```bash
make duckdb-stop                            # release the file
mv storage/duckdb/cricket.duckdb storage/duckdb/cricket.duckdb.bad
# Trigger ingest_all_match_data_gold — it will refresh the DuckDB tables from Iceberg
```

The DuckDB file is regenerable from Iceberg snapshots — losing it costs ~10 minutes, no data loss.

### 6c. Schema drift in Cricsheet (new `key_*` column)

The `ingest_people_and_names_bronze` DAG has a `schema_drift_check` task that emits informational alerts but never blocks. After confirming the drift:

```sql
SELECT * FROM control.register_schema_versions
ORDER BY detected_at DESC LIMIT 5;
```

The Bronze loader handles new `key_*` columns automatically (unpivots to long-form). Silver dbt models may need a one-line update if they explicitly reference the new column.

### 6d. Iceberg snapshot bloat / "too many snapshots"

Run the maintenance script:

```bash
poetry run python scripts/cleanup_silver_stale_snapshots.py --table silver.deliveries --keep-last 10
```

(Iceberg REST + PostgreSQL keeps metadata for every snapshot. Cleanup is safe — old snapshots become unreadable but the current snapshot is unaffected.)

---

## Step 7 — Post-recovery checklist

After a successful re-run:

- [ ] Failed task square → green in Grid view.
- [ ] `control.<table>_log` row for the run shows `status='SUCCESS'`.
- [ ] `control.v_dq_failures` shows no new failures for the run.
- [ ] Downstream DAG (Silver or Gold) auto-triggered (if applicable).
- [ ] Metabase / dashboard restarted if Gold ran (lock released).
- [ ] Add an entry to `local/scratch/incident-log.md` (informal — create if missing).

---

## When to escalate (escalate-to-yourself)

If the same DAG fails 3+ times in a row with the same root cause:

1. Open an issue or note in `local/scratch/known-issues.md`.
2. Add a regression test under `tests/unit/` or `tests/integration/`.
3. Pattern-match: is this a data-quality issue (fix Silver), a contract issue (fix Bronze), or an infra issue (fix Compose)?
4. Update this runbook's pattern-match table (§4) with the new symptom.

---

## References

- `docs/jobs.md` — per-module CLI invocations for manual retries
- `docs/runbooks/full-rebuild.md` — when everything is broken and you'd rather start over
- `docs/runbooks/dashboard.md` — DuckDB lock coordination
- `docs/architecture/data-flow.md` §5 — failure-mode flows
- `/cip-diagnose-dag` skill — automated diagnosis
- `analysis/validation_queries.sql` — end-to-end correctness queries
