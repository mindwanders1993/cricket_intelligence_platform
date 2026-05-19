# Full Teardown & UI-Driven Rebuild — Cricket Intelligence Platform

## Context

Wipe the platform to zero and rebuild it end-to-end. Terminal is only used to **start / stop / refresh** services via `make` targets. All pipeline runs and inspection happen in the web UIs:

| Step | UI | URL | Username | Password |
|---|---|---|---|---|
| Trigger pipelines | **Airflow** | http://localhost:8080 | `admin` | `$AIRFLOW_ADMIN_PASSWORD` (from `.env`) |
| Verify landed files | **MinIO Console** | http://localhost:9001 | `$MINIO_ROOT_USER` (from `.env`) | `$MINIO_ROOT_PASSWORD` (from `.env`) |
| Check control tables | **pgAdmin** | http://localhost:5050 | `admin@cricket-platform.local` | `admin123` |
| Inspect Bronze/Silver/Gold data | **DuckDB UI** | http://localhost:4213 | — (no auth) | — |
| View dashboards | **Metabase** | http://localhost:3000 | `admin@cricket-platform.local` | `Cricket2026!` |

## Make targets used in this runbook

| Target | What it does |
|---|---|
| `make nuke` | Stop UI, drop all containers + volumes, clear DuckDB + dbt host state. |
| `make rebuild` | Build Airflow image, start services, bootstrap MinIO + Postgres, validate DAGs. |
| `make refresh-gold` | Release DuckDB locks, trigger Gold DAG via CLI, wait, restart Metabase. |
| `make provision-metabase` | First-boot Metabase setup (if needed) + dashboard provisioning. |
| `make duckdb-ui` / `make duckdb-stop` | Open / close the DuckDB web UI. |

## Confirmed DAG IDs (Airflow UI)

| DAG ID | Purpose | Auto-trigger |
|---|---|---|
| `ingest_people_and_names_bronze` | people.csv + names.csv → Bronze | → silver |
| `ingest_people_and_names_silver` | Bronze register → Silver + DQ | — |
| `ingest_all_match_data_bronze` | all_json.zip → Bronze match_data (full backfill, ~21k matches) | → silver |
| `ingest_all_match_data_silver` | Bronze match → Silver deliveries/wickets/innings | → gold |
| `ingest_all_match_data_gold` | DuckDB refresh + dbt full-refresh + 50 tests | — |
| `ingest_two_day_match_data_bronze` | recently_added_2_json.zip → Bronze match_data | → silver |
| `ingest_two_day_match_data_silver` | Bronze match → Silver (incremental, pending match_ids) | → gold |
| `ingest_two_day_match_data_gold` | DuckDB refresh + dbt incremental + 50 tests | — |

---

## Phase 1 — Teardown

```bash
cd /Users/mrrobot/Desktop/Projects/cricket_intelligence_platform
make nuke
```

That stops the UI, drops every container + named volume, and clears `storage/duckdb/cricket.duckdb` + `models/dbt/target/`.

---

## Phase 2 — Bring everything up

```bash
make rebuild
```

That builds the custom Airflow image, starts every service, waits for healthchecks, bootstraps MinIO + Postgres, and validates DAGs. Expected final line: `✓ Rebuild complete — Airflow at http://localhost:8080`.

If Airflow login fails (password mismatch after a `.env` change):

```bash
docker exec compose-airflow-scheduler-1 \
  airflow users reset-password --username admin \
  --password "$(grep AIRFLOW_ADMIN_PASSWORD .env | cut -d= -f2)"
```

---

## Phase 3 — Airflow UI: Run Bronze + Silver

Open **http://localhost:8080** → log in.

For each DAG below: unpause it (left toggle), click ▶ **Trigger DAG w/ config**, paste this JSON, click Trigger:

```json
{"snapshot_date": "2026-05-17"}
```

Watch the Grid view turn green. Click any failed square → **Logs** tab to inspect.

### Run order

| # | DAG | Notes |
|---|---|---|
| 1 | `ingest_people_and_names_bronze` | ~1–2 min. Auto-triggers silver. |
| 2 | `ingest_people_and_names_silver` | ~1 min. Triggered automatically by step 1, or run standalone. |
| 3 | **Pick one:** `ingest_two_day_match_data_bronze` (smoke test, ~30 matches) **or** `ingest_all_match_data_bronze` (full backfill ~21k matches) | Incremental ~5 min; backfill 20–40 min. Both auto-trigger silver → gold. |

**Stop Metabase before step 3** so the auto-triggered Gold DAG can acquire the DuckDB write lock:
```bash
docker stop compose-metabase-1
```
Then trigger step 3 from the UI. After the Gold DAG completes, restart Metabase with `docker start compose-metabase-1`.

Don't trigger the Gold DAGs directly from the UI without stopping Metabase first — the `refresh_duckdb_views` task will fail with a file-lock error.

---

## Phase 4 — Gold refresh + Metabase

```bash
make refresh-gold
make provision-metabase
```

`make refresh-gold` stops Metabase, triggers the Gold DAG, polls until it finishes, then restarts Metabase. Watch progress in the Airflow UI. If the auto-trigger chain from step 3 already ran Gold successfully, this step is optional.

`make provision-metabase` runs the dashboard provisioner — and if Metabase has no admin yet (fresh after `make nuke`), it bootstraps the admin user + DuckDB connection first. Idempotent on re-runs.

---

## Phase 5 — UI verification

### 5a. MinIO Console — verify landing files

Browse and confirm files exist:

| Bucket / prefix | Expect |
|---|---|
| `cricket-source-files/people_and_names/csv/snapshot_date=2026-05-17/` | `people.csv`, `names.csv` |
| `cricket-source-files/match_data/zip/snapshot_date=2026-05-17/` | `all_json.zip` or `recently_added_2_json.zip` |
| `cricket-source-files/match_data/json/snapshot_date=2026-05-17/archive=all_json/` | thousands of `.json` files (full backfill only) |
| `cricket-source-files/match_data/json/snapshot_date=2026-05-17/archive=recently_added_2_json/` | ~30 `.json` files (incremental only) |
| `cricket-lakehouse/bronze/` | Iceberg metadata + Parquet |
| `cricket-lakehouse/silver/` | Iceberg metadata + Parquet |

### 5b. pgAdmin — verify control tables

Navigate **Servers → Cricket Platform → Databases → cricket_platform → Schemas → control → Tables**. Right-click any table → **View/Edit Data → All Rows**.

| Table | Expect rows for |
|---|---|
| `control.register_ingestion_log` | people.csv + names.csv ingest |
| `control.archive_download_log` | archive ZIP downloads |
| `control.bronze_match_ingestion_log` | Bronze match load |
| `control.dq_results` | DQ check results (status=`PASSED` mostly) |
| `control.register_schema_versions` | Schema fingerprints |

Useful queries (Tools → Query Tool):

```sql
SELECT * FROM control.v_dq_failures;
SELECT * FROM control.v_latest_register_snapshot;
SELECT * FROM control.v_latest_archive_snapshot;
```

### 5c. DuckDB UI — verify Bronze/Silver/Gold

```bash
make duckdb-ui
```

Opens **http://localhost:4213**. Browse `bronze`, `silver`, `gold` schemas in the left panel. Or run:

```sql
SELECT 'bronze.match_data'           AS table_name, COUNT(*) FROM bronze.match_data
UNION ALL SELECT 'silver.deliveries',  COUNT(*) FROM silver.deliveries
UNION ALL SELECT 'silver.wickets',     COUNT(*) FROM silver.wickets
UNION ALL SELECT 'gold.fact_delivery', COUNT(*) FROM gold.fact_delivery
UNION ALL SELECT 'gold.dim_player',    COUNT(*) FROM gold.dim_player;
```

For end-to-end checks paste `analysis/validation_queries.sql` into the UI (9 sections — row counts, integrity, grain uniqueness, referential integrity, business rules, freshness).

**Stop the UI before going to Metabase** (Metabase needs the read lock):

```bash
make duckdb-stop
```

### 5d. Metabase — verify dashboards

Open **http://localhost:3000** → log in.

| Dashboard | What to check |
|---|---|
| Cricket Universe | Counter cards populated (~21K matches, ~11M deliveries). |
| Player Spotlight | Drop-down has players; cards render. (Cricsheet uses initials — search `V Kohli` not `Virat Kohli`.) |
| Match Centre | Match dropdown populated; ball-by-ball cards render. |
| Matchup Explorer | Top matchups leaderboard returns 25 rows. |

If anything is empty: re-run `make provision-metabase`. If SQL errors point to missing tables: the Gold DAG didn't finish — re-run `make refresh-gold`.

---

## Verification checklist

- [ ] `make dag-validate` → 0 import errors, all 6 DAGs listed.
- [ ] All Bronze/Silver DAG runs green in Airflow UI.
- [ ] MinIO Console shows landing files + Iceberg metadata.
- [ ] pgAdmin shows rows in `control.register_ingestion_log`, `control.bronze_match_ingestion_log`, `control.dq_results`.
- [ ] DuckDB UI: row counts non-zero across `bronze.*`, `silver.*`, `gold.*`.
- [ ] Metabase dashboards load with data, no SQL errors.

## Rollback / iteration notes

- Re-running a DAG for the same `snapshot_date` is a no-op (idempotency guards). Pass `{"snapshot_date": "...", "force": true}` to override.
- If a DAG fails: open the failed task in Grid view → **Logs** tab. The `/cip-diagnose-dag` skill correlates task state + control schema + landing artifacts.
- DuckDB lock errors during the Gold DAG → `make refresh-gold` handles this automatically by stopping Metabase first.
