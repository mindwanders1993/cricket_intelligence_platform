# Data Flow — Cricket Intelligence Platform

> **Companion to** `docs/architecture/hld-hla.md` (target state) and `docs/architecture/as-built.md` (current state).
> Shows how a single record (or event) travels through the platform, who triggers each step, and what side-effects (lineage, telemetry, audit, cost) each step emits.

---

## 1. End-to-end pipeline shape

```
   ┌───────────┐
   │ Cricsheet │
   └─────┬─────┘
         │ HTTP GET (Airflow scheduler triggers download task)
         ▼
   ┌─────────────────────────────────────────────────────────┐
   │ LANDING (MinIO buckets / cricket-source-files)          │
   │  • match_data/zip/snapshot_date=…/<archive>.zip         │
   │  • match_data/json/snapshot_date=…/archive=…/*.json     │
   │  • people_and_names/csv/snapshot_date=…/{people,names}.csv │
   └─────┬───────────────────────────────────────────────────┘
         │ Polars reads bytes; computes _row_hash; stamps meta
         ▼
   ┌─────────────────────────────────────────────────────────┐
   │ BRONZE (Iceberg / cricket-lakehouse/bronze/<table>)     │
   │  match_data ← (match_id, revision)                      │
   │  people, people_identifiers, name_variations            │
   │  → control.match_file_audit row INSERT (revision bump)  │
   │  → control.{archive_download,bronze_match_ingestion}_log│
   │  → OpenLineage RunEvent + OTEL span + cost_emission     │
   └─────┬───────────────────────────────────────────────────┘
         │ Reads MAX(revision) per match_id
         │ Polars (register) or PySpark (match) explodes
         ▼
   ┌─────────────────────────────────────────────────────────┐
   │ SILVER (Iceberg / cricket-lakehouse/silver/<table>)     │
   │  matches, innings, deliveries, wickets, match_players,  │
   │  match_officials, teams, venues, competitions           │
   │  persons, person_identifiers, name_variations           │
   │  → overwrite_partition / dynamic_overwrite              │
   │  → DQ checks → control.dq_results + Soda Core scan      │
   │  → OpenLineage event + OTEL span + cost_emission        │
   └─────┬───────────────────────────────────────────────────┘
         │
         ├──────────────────────────┐
         ▼                          ▼
   ┌──────────────────────┐  ┌──────────────────────────────┐
   │ Iceberg → DuckDB     │  │ Iceberg → BigQuery sync      │
   │ DuckDBRefresh        │  │ scripts/sync_silver_to_bq.py │
   │ materialise() filter │  │ PolarsIcebergReader → BQ     │
   │ to MAX(_snapshot_date)│  │ load jobs (chunked)          │
   └────────┬─────────────┘  └────────┬─────────────────────┘
            ▼                          ▼
   ┌──────────────────────┐  ┌──────────────────────────────┐
   │ DuckDB (cricket.duckdb)│  │ BigQuery cricket_silver.*  │
   │  schemas: bronze / silver / control / gold              │
   └────────┬─────────────┘  └────────┬─────────────────────┘
            │                          │
            ▼                          ▼
   ┌─────────────────────────────────────────────────────────┐
   │ GOLD via dbt (target=dev DuckDB / target=bq_dev BQ)     │
   │  Dimensions (SCD2 dim_player + 6 others)                │
   │  Facts (incremental fact_delivery, fact_player_match…)  │
   │  Marts (incl. mart_pipeline_cost_daily)                 │
   │  MetricFlow semantic models + 5+ metrics + exposures    │
   │  Soda Core scans on Gold + dbt tests (50+)              │
   │  → OpenLineage facets per model via dbt-ol              │
   │  → control.match_file_audit.gold_loaded_at stamp        │
   └────────┬───────────┬────────────┬─────────────┬─────────┘
            │           │            │             │
   DuckDB direct        │   FastAPI gateway  Observable build
   (read-only)          │   /query /chat     Python loaders
            │           │            │             │
            ▼           ▼            ▼             ▼
   ┌──────────┐  ┌──────────┐  ┌─────────┐  ┌──────────────┐
   │Metabase  │  │Lightdash │  │Chainlit │  │Observable    │
   │SQL cards │  │MetricFlow│  │chat UI  │  │Framework     │
   │exec+anal │  │ops+FinOps│  │AI agent │  │player portfo │
   └──────────┘  └──────────┘  └─────────┘  └──────────────┘
```

---

## 2. Concrete trace — a single new match record

This is the journey of one Cricsheet match JSON (say `1426001.json` for IND vs AUS, 2026-05-10) from Cricsheet to the Player Portfolio dashboard.

### Step 1 — Cricsheet → Landing (Airflow DAG `ingest_two_day_match_data_bronze`, 02:00 UTC daily)

| Action | Where | Side-effect |
|---|---|---|
| `MatchDataDownloader.run(snapshot_date)` fetches `recently_added_2_json.zip` | Airflow worker | New object at `s3://cricket-source-files/match_data/zip/snapshot_date=2026-05-10/recently_added_2_json.zip` |
| SHA-256 checksum computed | Airflow worker | `control.archive_download_log` row |
| `MatchDataExtractor.run(...)` unzips JSON files | Airflow worker | Objects at `s3://cricket-source-files/match_data/json/snapshot_date=2026-05-10/archive=recently_added_2_json/1426001.json` (+~29 others) |
| Manifest written | Airflow worker | `_manifest.json` next to the extracted JSONs |

### Step 2 — Landing → Bronze (`MatchBronzeLoader`, instrumented)

| Action | Where | Side-effect |
|---|---|---|
| Loader scans archive-scoped prefix, reads each JSON via MinIO | Polars in Airflow worker | — |
| For `1426001.json`: file SHA computed; `control.match_file_audit` queried | — | If `(match_id, file_sha256)` exists with status=SUCCESS → skip; else INSERT with revision=MAX+1 |
| Polars DataFrame stamped with `_snapshot_date=2026-05-10`, `_pipeline_run_id`, `_ingested_at`, `_row_hash`, `_source_file=1426001.json` | Polars | — |
| `PolarsIcebergWriter.create_and_append(df, fqn=bronze.match_data)` | Iceberg REST + MinIO | New snapshot of `bronze.match_data`; new Parquet under `cricket-lakehouse/bronze/match_data/data/` |
| `cost_emission.record(rows=1, bytes=12345, executor_seconds=0.04, wall_time=0.6, target="bronze.match_data")` | Postgres | Row INSERT into `control.pipeline_cost_event` |
| OTEL span `bronze.match_data.write` finishes; OpenLineage `COMPLETE` event emitted | OTEL Collector + Marquez | — |
| `control.bronze_match_ingestion_log` row written | Postgres | — |
| `TriggerDagRunOperator` fires `ingest_two_day_match_data_silver` | Airflow scheduler | Silver DAG kicked off with `dag_run.conf = {"snapshot_date": "2026-05-10", ...}` |

### Step 3 — Bronze → Silver (PySpark, `ingest_two_day_match_data_silver`)

| Action | Where | Side-effect |
|---|---|---|
| Spark reads `bronze.match_data WHERE _snapshot_date=2026-05-10 AND match_id IN (pending_ids)` then dedups on `MAX(revision) per match_id` | Spark in Airflow container | — |
| JSON exploded into 11 Silver tables (matches, innings, deliveries, wickets, etc.) | Spark | — |
| Each Silver write: `SparkIcebergWriter.dynamic_overwrite(df, fqn=silver.<table>)` | Iceberg REST + MinIO | One snapshot per Silver table |
| OTEL spans `silver.<table>.write` finish; OpenLineage events emitted with `inputs=[bronze.match_data], outputs=[silver.<table>]` | Marquez | Lineage graph updated |
| `cost_emission.record(...)` for each Silver write | Postgres | Multiple rows in `control.pipeline_cost_event` |
| Per-table DQ checks (`silver_dq_log` + Soda Core) | Postgres + soda-core | If any check fails BLOCK, DAG fails; else `dq_results` rows written |
| `control.match_file_audit` set `silver_loaded_at = NOW()` for processed match_ids | Postgres | — |
| `TriggerDagRunOperator` fires `ingest_two_day_match_data_gold` | Airflow | — |

### Step 4 — Silver → DuckDB serving + Gold (`ingest_two_day_match_data_gold`)

| Action | Where | Side-effect |
|---|---|---|
| `make duckdb-stop` semantics — Gold task acquires write lock | DuckDB | — |
| `DuckDBRefresh.materialise()` reads Iceberg via PyIceberg; rebuilds `bronze.*`, `silver.*`, `control.*` as native DuckDB tables filtered to `MAX(_snapshot_date)` | DuckDB process | — |
| `dbt run` (incremental) — `is_incremental()` filter applies on `control.match_file_audit WHERE gold_loaded_at IS NULL` | DuckDB | New rows in `fact_delivery`, `fact_player_match`, `dim_player_scd2`, dependent marts |
| `mart_pipeline_cost_daily` rebuilt — picks up the new `pipeline_cost_event` rows | DuckDB | — |
| `dbt test` runs 50+ tests | DuckDB | Failures break the DAG |
| dbt-ol emits OpenLineage events per model | Marquez | Cross-tool lineage graph stitched |
| `control.match_file_audit.gold_loaded_at = NOW()` for processed match_ids | Postgres | — |

### Step 5 — Sync to BigQuery (`sync_silver_to_bigquery`, daily, downstream of Silver)

| Action | Where | Side-effect |
|---|---|---|
| For each Silver table: `PolarsIcebergReader.scan(fqn).filter(_snapshot_date=ds)` → Polars frame | Airflow worker | — |
| `google.cloud.bigquery.load_table_from_dataframe(...)` writes to `cricket_silver.<table>` | BigQuery | — |
| `dbt build --target bq_dev` reruns Gold models against BigQuery | BigQuery | Parallel Gold materialisation |
| Row-count parity check: assert `COUNT(*)` from DuckDB == BigQuery for every Gold table | DuckDB + BigQuery | DAG fails if mismatch |

### Step 6 — Consumers see the new match

| Consumer | How it picks up the new row |
|---|---|
| Metabase | Next query against `gold.fact_delivery` includes the new rows (DuckDB caching) |
| Lightdash | Same — semantic-layer SQL evaluated against the same DuckDB tables |
| Observable Framework | Next `npm run build` (or dev-mode auto-refresh) re-runs Python loaders → CSV cache rebuilt → charts re-render |
| FastAPI `/query` | Returns updated metrics next call |
| AI agent | Next chat turn that calls `query_metric` picks up new data |

---

## 3. Side-channel flows

### 3.1 Lineage (OpenLineage)

```
PolarsIcebergWriter ──┐
SparkIcebergWriter ───┼─→ HTTP POST → Marquez (localhost:5000) → UI graph
dbt-ol (per model) ───┤    + same events stitched together by Marquez
Airflow-ol listener ──┘
```

Every event has `job.namespace=cricket`, `job.name=<layer>.<table>.write` (or dbt model name), and facets for schema + `_snapshot_date` + `_row_hash` aggregate.

### 3.2 Metrics & traces (OpenTelemetry)

```
writers / FastAPI / agent → OTLP/gRPC → OTEL Collector
                                            ├─→ Prometheus (metrics)
                                            ├─→ Tempo (traces)
                                            └─→ (future) Loki (logs)
                                                  │
                                                  ▼
                                              Grafana (datasource for all three)
```

Trace propagation: Airflow → writer → OpenLineage emitter share `_pipeline_run_id` as the trace ID prefix.

### 3.3 Cost telemetry

```
writer  ──→ cost_emission.record(...)
                  │
                  ▼
   control.pipeline_cost_event  ──→ dbt staging  ──→ mart_pipeline_cost_daily
                                                          │
                                                          ▼
                                              Lightdash + Metabase + Grafana panels
```

Cost constants (`$/executor-second`, `$/TB-written`, `$/row-written`) live in `models/dbt/dbt_project.yml` `vars:` so they're easy to tune.

### 3.4 Audit trail (idempotency)

```
download task  ──→ control.archive_download_log     (file_sha256, snapshot_date, status)
bronze task    ──→ control.match_file_audit         (match_id, file_sha256, revision, status)
silver task    ──→ control.match_file_audit.silver_loaded_at
gold task      ──→ control.match_file_audit.gold_loaded_at
register task  ──→ control.register_ingestion_log   (source_file, snapshot_date)
schema drift   ──→ control.register_schema_versions
DQ runs        ──→ control.dq_results               (run_id, check_name, status, severity)
```

Re-runs short-circuit by checking these tables. `force=True` bypasses.

### 3.5 AI metadata refresh (Sprint 2+)

```
dag_refresh_ai_metadata (nightly)
   │
   ├─→ src/cip/serving/ai/retrieval/embed_dbt_docs.py
   │      reads models/dbt/target/manifest.json + catalog.json
   │      generates nomic-embed-text embeddings via Ollama
   │      upserts to Qdrant collection "dbt_docs"
   │
   ├─→ src/cip/serving/ai/jobs/refresh_ai_metadata.py
   │      pulls MetricFlow metric catalog
   │      writes to control.ai_metadata_refresh_log
   │
   └─→ stamps control.ai_metadata_refresh_log.last_refreshed_at
```

---

## 4. Sequence diagram — a chat turn

```
User → Chainlit → FastAPI /chat (SSE) → LangGraph agent
                                          │
                                          ├─ system prompt + few-shots loaded from prompt_registry
                                          │
                                          ├─ tool: search_metrics("batting average since 2020")
                                          │       └─ Qdrant similarity → returns [batting_average, …]
                                          │
                                          ├─ tool: lookup_player("Virat Kohli")
                                          │       └─ DuckDB query against gold.player_display_names
                                          │            returns canonical name "V Kohli" + person_id
                                          │
                                          ├─ tool: query_metric({metric: "batting_average",
                                          │                     dimensions: ["dim_player__full_name"],
                                          │                     filters: {"player": "V Kohli",
                                          │                               "match_type": "ODI",
                                          │                               "season_year__gte": 2020}})
                                          │       └─ FastAPI /query → MetricFlow → DuckDB → result rows
                                          │
                                          ├─ LLM synthesises natural-language response
                                          │       └─ Optional: tool: generate_chart_spec(rows, "bar")
                                          │              returns Vega-Lite JSON
                                          │
                                          └─ SSE stream → Chainlit UI

  Side-effects:
   - OTEL trace per agent turn (visible in Tempo)
   - OpenLineage event (job.name=ai.chat.turn) optional
   - control.ai_chat_log (future, Sprint 2.1) row
```

---

## 5. Pipeline failure-mode flows

### 5.1 Bronze load fails midway

Failure during `bronze_load` task (e.g. Iceberg REST timeout after 30 of 100 files written):

```
control.match_file_audit  ← 30 rows already INSERTed with status=SUCCESS
control.bronze_match_ingestion_log  ← run row with status=FAILED
Bronze Iceberg table  ← snapshot committed for first batch; partial files appended
Re-run with same snapshot_date  → audit_lookup says 30 done, skips them, processes remaining 70
                                  no `force=True` needed
```

Append-only Bronze + audit-driven dedup makes re-run idempotent. No `MERGE INTO`, no rollback.

### 5.2 Silver Spark OOM

Failure during PySpark Silver build:

```
silver.* Iceberg tables  ← partial commits possible (Spark may commit per-stage)
control.dq_results       ← failures logged
Re-run with same snapshot_date → SparkIcebergWriter.dynamic_overwrite() replaces the snapshot's partitions
                                 → idempotent
```

If OOM is fundamental: increase `SPARK_DRIVER_MEMORY` in `compose.dev.yml` (default 8g) or shrink the per-batch slice.

### 5.3 DuckDB lock conflict

Gold DAG task `refresh_duckdb_views` fails because another reader (Metabase / dashboard / DuckDB UI) holds the file lock:

```
DAG fails fast with "file is locked"
Operator action: docker stop compose-metabase-1 ; make duckdb-stop ; stop dashboard dev server
Re-run the Gold DAG → succeeds
```

The `make refresh-gold` target encapsulates this stop-trigger-restart sequence.

### 5.4 BigQuery sync hits free-tier limit

Sync DAG fails when BQ rejects load job due to slot/quota:

```
Sync DAG fails; DuckDB target unaffected
sync_silver_to_bq.py logs the BQ error code; ADR 0005 documents the limit
Operator can either: (a) defer sync, (b) shrink data via WHERE filters, (c) move to paid tier
```

### 5.5 OpenLineage emitter offline (Marquez down)

```
OpenLineage emit is fire-and-forget with retry; failure logged at WARN
Pipeline continues normally — no data plane impact
Marquez restart auto-recovers; missed events are lost (acceptable in dev)
```

For production: use OTEL Collector buffering or DataHub which supports durable ingest.

---

## 6. Refresh cadence summary

| Job | Trigger | Cadence | Downstream |
|---|---|---|---|
| `ingest_people_and_names_bronze` | Schedule | Sun 00:30 UTC | → silver |
| `ingest_people_and_names_silver` | Trigger / schedule | Sun 01:30 UTC | (none) |
| `ingest_two_day_match_data_bronze` | Schedule | Daily 02:00 UTC | → silver |
| `ingest_two_day_match_data_silver` | Trigger | (within ~5 min) | → gold |
| `ingest_two_day_match_data_gold` | Trigger | (within ~10 min) | (none) |
| `sync_silver_to_bigquery` *(Sprint 3)* | Trigger | Daily after silver | → bq_dev dbt |
| `dq_soda` *(Sprint 0)* | Schedule | Daily 03:00 UTC | (control.dq_results) |
| `dag_refresh_ai_metadata` *(Sprint 2)* | Schedule | Daily 04:00 UTC | (Qdrant + cache) |
| `ingest_all_match_data_*` | Manual | Monthly (or on schema change) | full Silver+Gold rebuild |
