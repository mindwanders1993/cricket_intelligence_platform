# As-Built Architecture — Cricket Intelligence Platform

> What is **actually built** in `main`, not what is planned.
> Last verified: 2026-05-19 (Bronze + Silver + Gold all live; Metabase BI provisioned; 8-DAG Airflow layout in place).

---

## Data flow overview

```
cricsheet.org
    │
    ├── people.csv ─────────────────────────────────────────────────┐
    ├── names.csv ──────────────────────────────────────────────────┤
    │                                                               │
    │   PeopleAndNamesDownloader  →  s3://cricket-source-files/people_and_names/
    │   PeopleAndNamesBronzeLoader (Polars)                         ▼
    │                                                bronze.people / bronze.people_identifiers
    │                                                bronze.name_variations
    │                                                               │
    │   PolarsPeopleAndNamesSilverTransform                         ▼
    │                                                silver.persons / silver.person_identifiers
    │                                                silver.name_variations
    │
    ├── all_json.zip ──────── (monthly full backfill)
    └── recently_added_2_json.zip ──── (daily incremental)
            │
            ▼
        MatchDataDownloader  → s3://cricket-source-files/match_data/zip/
        MatchDataExtractor   → s3://cricket-source-files/match_data/json/snapshot_date=…/archive=…/
        MatchBronzeLoader (Polars + audit-driven dedup via control.match_file_audit)
            │
            ▼
        bronze.match_data  (append-only, primary key (match_id, revision))
            │
            ▼
        SparkSilverPipeline  (PySpark + Iceberg, MAX(revision) per match_id)
            │
            ▼
        silver.matches / silver.innings / silver.deliveries / silver.wickets
        silver.match_players / silver.match_officials
        silver.teams / silver.venues / silver.competitions
            │
            ▼
        DuckDBRefresh.materialise() — Iceberg → DuckDB bronze/silver tables
            │
            ▼
        dbt run (target=dev)  → gold.* dims, facts, marts (DuckDB)
            │
            ▼
        Metabase (read-only) + DuckDB UI + FastAPI (deferred) + AI assistant (deferred)
```

---

## Infrastructure services (Docker Compose)

Containers are named `compose-<service>-1` because the compose project folder is `infra/compose`.

| Container | Image | Port (host) | Role |
|-----------|-------|-------------|------|
| `compose-minio-1` | `minio/minio` | 9000 (API), 9001 (console) | Object storage (source files, lakehouse, ML models) |
| `compose-postgres-1` | `postgres:15` | 5432 | Iceberg metastore + control schema + Airflow metadata |
| `compose-iceberg-rest-1` | `tabulario/iceberg-rest` | 8181 (API only — no UI) | Iceberg REST catalog |
| `compose-airflow-init-1` | custom (see `infra/docker/airflow/Dockerfile`) | — | One-shot DB init + admin user creation |
| `compose-airflow-webserver-1` | custom Airflow image | 8080 | Airflow UI |
| `compose-airflow-scheduler-1` | custom Airflow image | — | DAG scheduling + execution |
| `compose-mlflow-1` | `ghcr.io/mlflow/mlflow` | 5001 | ML experiment tracking (SQLite backend in dev) |
| `compose-pgadmin-1` | `dpage/pgadmin4` | 5050 | PostgreSQL UI (auto-connects via libpq passfile) |
| `compose-metabase-1` | custom (Temurin 21 + DuckDB driver) | 3000 | BI dashboards (read-only on DuckDB file) |

The custom Airflow image bakes PySpark, JDK 17, Iceberg JAR cache, and `pydantic_settings` — run `make build-airflow` once after clone or after Dockerfile changes.

**MinIO buckets** (created by `make bootstrap`):

| Bucket | Contents |
|--------|----------|
| `cricket-source-files` | Raw downloads — ZIPs, CSVs, extracted JSONs (never modified) |
| `cricket-lakehouse` | All Iceberg table data, layer-first layout: `bronze/{table}/`, `silver/{table}/`, `gold/{table}/` |
| `cricket-ml-models` | MLflow run artifacts |

The Iceberg catalog is named `cricket`. Table FQNs are 2-segment `<layer>.<entity>` (e.g. `bronze.people`, `silver.deliveries`) — the catalog name is **not** in the FQN.

---

## Module layout (`src/cip/`)

```
src/cip/
├── common/
│   ├── settings.py             PlatformSettings + get_settings() singleton
│   ├── logging.py              structlog wrapper (get_logger, bind_context)
│   ├── exceptions.py           IcebergError + platform exception hierarchy
│   └── contracts/
│       ├── enums.py            StrEnum: Layer, MatchType, WicketKind, ExtraType, …
│       └── naming.py           TableName, PathBuilder, META, DagNames, IcebergProperties
│
├── ingestion/
│   ├── io/
│   │   └── minio.py            MinIOClient (from_settings, health_check, upload_to_source_files, read_object)
│   ├── people_and_names/
│   │   ├── download.py         PeopleAndNamesDownloader (HTTP fetch + control schema audit)
│   │   └── …                   parsing + Bronze writers
│   ├── match_data/
│   │   ├── download.py         MatchDataDownloader (ZIP fetch, checksum, MinIO upload)
│   │   ├── extract.py          MatchDataExtractor (ZIP → JSON files, archive-scoped MinIO prefix)
│   │   └── checksum.py         sha256_bytes / sha256_file utilities
│   └── jobs/                   Thin Airflow-callable wrappers + CLI entrypoints
│       ├── ingest_people_and_names.py
│       ├── build_silver_people_and_names.py
│       ├── full_load_match_data.py
│       ├── incremental_match_data.py
│       ├── build_silver_match_data.py
│       └── run_gold_dbt_models.py
│
├── transform/
│   ├── polars/
│   │   ├── bronze/             match_data + people_and_names Bronze writers
│   │   └── silver/persons.py   PolarsPeopleAndNamesSilverTransform
│   ├── spark/
│   │   └── silver/             PySpark match-data Silver pipeline (matches, innings, deliveries, …)
│   └── shared/
│       ├── writers.py          PolarsIcebergWriter (Bronze + Silver Polars), SparkIcebergWriter
│       ├── readers.py          PolarsIcebergReader + Spark-Iceberg session bootstrap
│       └── partitioning.py     PartitionStrategy registry
│
├── serving/
│   ├── api/main.py             FastAPI app (deferred)
│   ├── duckdb/refresh.py       DuckDBRefresh — Iceberg → DuckDB tables (bronze/silver schemas)
│   └── ai/                     LLM chains, semantic layer (deferred)
│
├── quality/checks/             DQ checkers per pipeline (control.dq_results)
└── ml/                         Feature eng + training + MLflow (deferred)
```

---

## Iceberg catalog layout

```
cricket (catalog)
├── bronze
│   ├── people                       ✅
│   ├── people_identifiers           ✅
│   ├── name_variations              ✅
│   └── match_data                   ✅ (append-only, PK = (match_id, revision))
├── silver
│   ├── persons                      ✅
│   ├── person_identifiers           ✅
│   ├── name_variations              ✅
│   ├── matches                      ✅
│   ├── innings                      ✅
│   ├── deliveries                   ✅
│   ├── wickets                      ✅
│   ├── match_players                ✅
│   ├── match_officials              ✅
│   ├── teams                        ✅
│   ├── venues                       ✅
│   └── competitions                 ✅
└── gold  (materialised in DuckDB via dbt; Iceberg gold deferred)
    ├── dim_match / dim_player / dim_team / dim_venue / dim_competition / dim_official  ✅
    ├── fact_delivery / fact_innings / fact_match_result / fact_player_match / fact_player_of_match  ✅
    └── mart_player_batting_career / _season, mart_player_bowling_career / _season  ✅
```

MinIO physical layout is **layer-first** (Option A): `cricket-lakehouse/bronze/{table}/`, `cricket-lakehouse/silver/{table}/`. Match JSON files in MinIO are partitioned by archive segment: `match_data/json/snapshot_date={date}/archive={stem}/` — required to keep the monthly full backfill and the daily incremental from cross-reading each other's files.

---

## Control schema (PostgreSQL `control.*`)

| Object | Purpose | Status |
|--------|---------|--------|
| `control.register_ingestion_log` | Per-file landing audit (People & Names) | ✅ |
| `control.register_schema_versions` | Column fingerprint + drift detection (People & Names) | ✅ |
| `control.register_change_log` | Row-count deltas between snapshots (People & Names) | ✅ |
| `control.archive_download_log` | Per-archive download audit (match_data) | ✅ |
| `control.bronze_match_ingestion_log` | Per-run Bronze match-data load metrics | ✅ |
| `control.match_file_audit` | Per-file `(match_id, file_sha256, revision)` ledger — drives Bronze dedup | ✅ |
| `control.dq_results` | Central DQ result store across all layers | ✅ |
| `control.v_latest_register_snapshot` | View: latest successful Register snapshot per file | ✅ |
| `control.v_dq_failures` | View: all DQ failures, newest first | ✅ |
| `control.v_latest_archive_snapshot` | View: latest successful archive download per file | ✅ |

The `control.match_file_audit` ledger (added 2026-05-17) is what makes Bronze idempotent across re-runs: the loader skips files whose `file_sha256` is already SUCCESS for the current `dag_id`, and bumps `revision` when the same `match_id` reappears with a different content hash.

---

## Airflow DAG inventory (8 DAGs, post-rename)

Naming convention: `ingest_<scope>_<dataset>_<layer>`. The `dag_` prefix is gone (commit `f1ca548`, 2026-05-19).

| DAG ID | Schedule | Status | Description |
|--------|----------|--------|-------------|
| `ingest_people_and_names_bronze` | `30 0 * * 0` (Sun 00:30 UTC) | ✅ | Download CSVs + land Bronze; schema drift check |
| `ingest_people_and_names_silver` | `30 1 * * 0` (Sun 01:30 UTC) | ✅ | Polars Silver transform + DQ |
| `ingest_all_match_data_bronze` | None (manual) | ✅ | `all_json.zip` → extract → Bronze + DQ; auto-triggers silver |
| `ingest_all_match_data_silver` | None (manual / triggered) | ✅ | PySpark full Silver rebuild; auto-triggers gold |
| `ingest_all_match_data_gold` | None (manual) | ✅ | DuckDB refresh + `dbt run --full-refresh` + tests |
| `ingest_two_day_match_data_bronze` | `0 2 * * *` (daily 02:00 UTC) | ✅ | `recently_added_2_json.zip` → Bronze; auto-triggers silver |
| `ingest_two_day_match_data_silver` | None (triggered / manual) | ✅ | Incremental Silver for last-2-day match IDs; auto-triggers gold |
| `ingest_two_day_match_data_gold` | None (manual / triggered) | ✅ | Incremental DuckDB refresh + `dbt run` + tests |

Bronze → Silver → Gold linkage uses `TriggerDagRunOperator` with `wait_for_completion=False` so each DAG owns its success/failure state independently.

Future / placeholder DAGs (not yet implemented): `dag_parse_bronze_match_data`, `dag_run_quality_checks`, `dag_refresh_serving_layer`, `dag_train_ml_model`, `dag_refresh_ai_metadata`. IDs retained in `DagNames` for reference.

### Bronze auto-trigger pattern

```python
trigger_silver = TriggerDagRunOperator(
    task_id="trigger_silver",
    trigger_dag_id=DagNames.INGEST_ALL_MATCH_DATA_SILVER,  # or TWO_DAY variant
    wait_for_completion=False,
    reset_dag_run=False,
    conf={"snapshot_date": "{{ ds }}", "pipeline_run_id": "{{ run_id }}"},
    execution_timeout=timedelta(minutes=2),
)
```

Silver DAGs read these XCom values from `dag_run.conf`, falling back to `ds` / `run_id` when triggered standalone.

---

## Settings hierarchy

```
PlatformSettings
  ├── storage:  StorageSettings   (env prefix: MINIO_)
  ├── iceberg:  IcebergSettings   (env prefix: ICEBERG_)
  ├── postgres: PostgresSettings  (env prefix: POSTGRES_)
  ├── airflow:  AirflowSettings   (env prefix: AIRFLOW_)
  ├── spark:    SparkSettings     (env prefix: SPARK_)
  ├── polars:   PolarsSettings    (env prefix: POLARS_)
  ├── duckdb:   DuckDBSettings    (env prefix: DUCKDB_)
  ├── dbt:      DbtSettings       (env prefix: DBT_)
  ├── mlflow:   MLflowSettings    (env prefix: MLFLOW_)
  ├── ai:       AISettings        (env prefix: AI_)
  └── paths:    PathSettings      (env prefix: PATH_)
```

Resolution order: env vars > `.env` file > `conf/base/*.yaml` > Pydantic defaults. Always `get_settings()`; never instantiate `PlatformSettings` directly.

---

## Gold layer (dbt + DuckDB)

- dbt project at `models/dbt/`, profile `cricket`, target `dev`.
- Sources are Silver Iceberg tables surfaced via DuckDB's `silver` schema.
- `DuckDBRefresh` materialises **native DuckDB tables** (not views) for the `bronze` and `silver` schemas, filtered to `MAX(_snapshot_date)` so dim/fact PKs remain unique across Silver re-runs.
- 40 dbt tests guard grain, PK uniqueness, FK referential integrity, and business rules. Custom test `fact_player_of_match_unique_grain` enforces the `(match_id, player_name)` bridge grain.

---

## BI layer (Metabase)

- Metabase v0.60.6 OSS + `metabase_duckdb_driver` 1.5.2.0, custom Temurin-21 image (Alpine causes JNI segfaults — do not switch back).
- Reads `storage/duckdb/cricket.duckdb` **read-only**.
- Dashboards provisioned via `scripts/provision_metabase_dashboards.py` (idempotent — re-run after volume wipe or SQL change).
- DuckDB write lock: stop Metabase (`docker stop compose-metabase-1`) before triggering any `*_gold` DAG.

---

## Test coverage snapshot (2026-05-19)

| Area | Count | Notes |
|---|---|---|
| Settings / config | 16 | Repo root, DSNs, env overrides |
| People & Names ingestion (download, normalize, parse, Bronze, Silver) | ~70 | Schema drift, all-string ingestion, row-hash |
| Match-data ingestion (download, extract, Bronze loader, audit dedup) | ~50 | Per-archive prefix isolation, idempotency, revision bump |
| Spark Silver transforms | ~40 | matches/innings/deliveries/wickets grain + dedup |
| dbt | 40 | `poetry run dbt test` from `models/dbt/` |

All unit tests run without external dependencies (I/O mocked).

---

## Revamp v2 — additions in flight

The original roadmap (`README.md` §20) called for Phases 5 (AI/MLOps), 6 (AWS cloud), and 7 (Portfolio hardening) which were deferred after Phase 4 shipped. Those are now being executed together as **revamp v2** with open-standards extensions. This `as-built.md` document will be updated as each sprint lands.

| Sprint | Adds to this snapshot | See |
|---|---|---|
| **Sprint 0** — Observability + dbt depth | `src/cip/observability/{lineage,telemetry,cost_emission}.py`; `compose.observability.yml` (Marquez + Grafana + Prometheus + Tempo + OTEL Collector); `control.pipeline_cost_event`; dbt SCD2 (`dim_player_scd2`) + incremental facts + MetricFlow semantic models + 5 metrics + exposures; Soda Core baseline; ADRs 0001–0004 | `docs/planning.md` → Sprint 0 |
| **Sprint 1** — FastAPI + FinOps + Lightdash | `src/cip/serving/api/` (FastAPI gateway with `/health`, `/metrics`, `/query`, `/explain`, `/catalog/*`); `mart_pipeline_cost_daily` + `mart_top_expensive_tasks` + `mart_data_freshness`; Lightdash dashboards (config-as-code); ADRs 0006–0008 | `docs/planning.md` → Sprint 1 |
| **Sprint 2** — Agentic AI assistant | `src/cip/serving/ai/{chains,tools,prompt_registry,retrieval,jobs}/`; Chainlit chat UI in `apps/ai-studio/`; `dag_refresh_ai_metadata` becomes real; ADR 0009 | `docs/planning.md` → Sprint 2 |
| **Sprint 3** — BigQuery target + Terraform | `bq_dev` dbt target; `scripts/sync_silver_to_bq.py`; `sync_silver_to_bigquery` DAG; `infra/terraform/{bigquery,aws}/`; ADR 0005 | `docs/planning.md` → Sprint 3 |
| **Sprint 4** — Scale + dashboard + polish | `scripts/synth/generate_synthetic_deliveries.py` (100M rows); `docs/perf/scale_test.md`; Observable dashboard M3–M22 (player portfolio + embedded AI chat); ADR 0010 | `docs/planning.md` → Sprint 4 |

**Reference docs (target state):**
- `docs/architecture/hld-hla.md` — full target architecture
- `docs/architecture/data-flow.md` — per-record flow + side-channel emissions
- `docs/architecture/service-interactions.md` — port + lock + auth matrix
- `docs/architecture/repo-structure.md` — codebase navigation

When a sprint ships, update the relevant table sections above (DAG inventory, Iceberg catalog layout, control schema, BI layer) inline and append the change to this section's row with a ✅.
