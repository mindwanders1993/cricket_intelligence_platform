# As-Built Architecture — Cricket Intelligence Platform

> This documents what is **actually built**, not what is planned.
> Last verified: 2026-05-11 (Big Task 1 + 2 done, Big Task 3 Bronze complete).

---

## Data flow overview

```
cricsheet.org
    │
    ├── people.csv ──────────────────────────────────┐
    ├── names.csv ───────────────────────────────────┤
    │                                                │
    │   RegisterDownloader                           │
    │   (download, checksum, land)                   │
    │                                                ▼
    │                               s3://cricket-landing/register_csv/
    │                                                │
    │                               RegisterNormalizer  (all-string Polars)
    │                               RegisterParser     (split into 3 frames)
    │                               RegisterLoader     (PolarsIcebergWriter)
    │                                                │
    │                               ┌───────────────┼───────────────┐
    │                               ▼               ▼               ▼
    │                    bronze.register_people  bronze.register_  bronze.register_
    │                                           identifiers       name_variations
    │
    └── all_matches.zip ──── (Big Task 4, not yet built)
            │
            ▼
        s3://cricket-landing/extracted_json/
            │
            ▼
        bronze.match_documents
            │
            ▼
        silver.*  (Big Task 5, PySpark)
            │
            ▼
        gold.*  (Big Task 7-8, dbt)
            │
            ▼
        DuckDB serving layer
```

---

## Infrastructure services (Docker Compose)

| Container | Image | Port | Role |
|-----------|-------|------|------|
| `cip-minio` | `minio/minio` | 9000 (API), 9001 (console) | Object storage (Landing, Bronze, Silver, Gold, Iceberg) |
| `cip-postgres` | `postgres:15` | 5432 | Iceberg metastore + control schema |
| `cip-iceberg-rest` | `tabulario/iceberg-rest` | 8181 | Iceberg REST catalog |
| `cip-airflow-init` | `apache/airflow:2.9` | — | One-shot DB init |
| `cip-airflow-webserver` | `apache/airflow:2.9` | 8080 | Airflow UI |
| `cip-airflow-scheduler` | `apache/airflow:2.9` | — | DAG scheduling |

**MinIO buckets** (created by `make bootstrap`):

| Bucket | Contents |
|--------|----------|
| `cricket-landing` | Raw downloads (ZIPs, CSVs, extracted JSONs) |
| `cricket-bronze` | (reserved for direct-write Bronze staging) |
| `cricket-silver` | (reserved) |
| `cricket-gold` | (reserved) |
| `iceberg-warehouse` | All Iceberg table data (Bronze + Silver + Gold) |
| `mlflow-artifacts` | MLflow experiment artifacts |

---

## Module layout (`src/cip/`)

```
src/cip/
├── common/
│   ├── settings.py          PlatformSettings + get_settings() singleton
│   ├── logging.py           structlog wrapper (get_logger, bind_context, get_context)
│   ├── exceptions.py        Platform exception hierarchy
│   └── contracts/
│       ├── enums.py         StrEnum for Layer, MatchType, WicketKind, ExtraType, etc.
│       └── naming.py        TableName, PathBuilder, META, DagNames, IcebergProperties
│
├── ingestion/
│   ├── io/
│   │   └── minio.py         MinIOClient (from_settings, health_check, read_object, upload...)
│   ├── register/
│   │   ├── download.py      RegisterDownloader — download + land + control DB write
│   │   ├── normalize.py     RegisterNormalizer — MinIO read → all-string Polars + metadata
│   │   └── parse.py         RegisterParser — split normalized frame into 3 Bronze-shaped frames
│   ├── cricsheet/           (Big Task 4 — not yet built)
│   │   ├── download.py
│   │   └── extract.py
│   └── jobs/
│       └── ingest_people_and_names.py   Airflow callables + CLI entrypoint
│
├── transform/
│   ├── polars/
│   │   └── bronze/
│   │       └── register_loader.py   RegisterLoader — writes 3 Bronze Iceberg tables
│   ├── spark/               (Big Task 5 — not yet built)
│   │   └── silver/
│   └── shared/
│       ├── writers.py       PolarsIcebergWriter, SparkIcebergWriter
│       ├── readers.py       PolarsIcebergReader, SparkIcebergReader, DuckDBIcebergReader
│       └── partitioning.py  PartitionStrategy registry
│
├── serving/                 (deferred — post Gold)
│   ├── api/
│   ├── duckdb/
│   └── ai/
│
├── quality/                 (Big Task 6 — not yet built)
└── ml/                      (deferred — post Gold)
```

---

## Iceberg catalog layout

```
cricket (catalog)
├── bronze
│   ├── register_people           ✅ built
│   ├── register_identifiers      ✅ built
│   ├── register_name_variations  ✅ built
│   └── match_documents           ⬜ Big Task 4
├── silver
│   ├── matches                   ⬜ Big Task 5
│   ├── innings                   ⬜ Big Task 5
│   ├── deliveries                ⬜ Big Task 5
│   ├── wickets                   ⬜ Big Task 5
│   ├── teams                     ⬜ Big Task 5
│   ├── venues                    ⬜ Big Task 5
│   ├── competitions              ⬜ Big Task 5
│   ├── persons                   ⬜ Big Task 3 (Silver)
│   ├── person_identifiers        ⬜ Big Task 3 (Silver)
│   ├── match_players             ⬜ Big Task 5
│   └── match_officials           ⬜ Big Task 5
└── gold
    ├── dim_player                ⬜ Big Task 7
    ├── dim_match                 ⬜ Big Task 7
    ├── dim_team                  ⬜ Big Task 7
    ├── dim_venue                 ⬜ Big Task 7
    ├── dim_competition           ⬜ Big Task 7
    ├── dim_date                  ⬜ Big Task 7
    ├── fact_delivery             ⬜ Big Task 7
    ├── fact_innings              ⬜ Big Task 7
    ├── fact_match_result         ⬜ Big Task 7
    ├── fact_player_match         ⬜ Big Task 7
    ├── mart_player_batting       ⬜ Big Task 8
    ├── mart_player_bowling       ⬜ Big Task 8
    ├── mart_team_performance     ⬜ Big Task 8
    ├── mart_venue_dna            ⬜ Big Task 8
    ├── mart_phase_scoring        ⬜ Big Task 8
    ├── mart_toss_outcome         ⬜ Big Task 8
    └── mart_matchup_analysis     ⬜ Big Task 8
```

---

## Control schema (PostgreSQL `control.*`)

| Table | Purpose | Status |
|-------|---------|--------|
| `control.register_ingestion_log` | Per-file landing audit for Register pipeline | ✅ |
| `control.register_schema_versions` | Column fingerprint + drift detection | ✅ |
| `control.register_change_log` | Delta row counts between Register snapshots | ✅ |
| `control.archive_download_log` | Per-archive landing audit for match pipeline | ✅ |
| `control.bronze_match_ingestion_log` | Per-run Bronze load metrics | ✅ |
| `control.dq_results` | Central DQ result store (all layers) | ✅ |
| `control.v_latest_register_snapshot` | Latest successful Register snapshot per file | ✅ |
| `control.v_dq_failures` | All DQ failures ordered newest-first | ✅ |
| `control.v_latest_archive_snapshot` | Latest successful archive download per file | ✅ |

---

## Airflow DAG inventory

| DAG ID | Schedule | Status | Description |
|--------|----------|--------|-------------|
| `dag_ingest_people_and_names` | Sun 00:30 UTC | ✅ Built | Register download + Bronze load |
| `dag_ingest_all_match_data` | TBD | ⬜ Big Task 4 | Match archive download + extract + Bronze |
| `dag_parse_bronze_match_documents` | TBD | ⬜ Big Task 4 | JSON → Bronze Iceberg |
| `dag_build_silver_match_data` | TBD | ⬜ Big Task 5 | PySpark Silver transforms |
| `dag_run_gold_dbt_models` | TBD | ⬜ Big Task 7 | dbt Gold layer |
| `dag_run_quality_checks` | TBD | ⬜ Big Task 6 | DQ across all layers |
| `dag_refresh_serving_layer` | TBD | Deferred | DuckDB + FastAPI refresh |
| `dag_train_ml_model` | TBD | Deferred | MLflow training |
| `dag_refresh_ai_metadata` | TBD | Deferred | LLM semantic layer refresh |

### `dag_ingest_people_and_names` — task graph

```
check_infra
    └─► download_and_land
          ├─► schema_drift_check ──(drift detected only)──► schema_drift_alert
          └─► load_bronze  (trigger_rule=all_done — always runs)
                └─► done
```

- `check_infra` — verifies MinIO health + PostgreSQL `control.register_ingestion_log` reachable
- `download_and_land` → `task_download_and_land()` — download, checksum, MinIO upload, control DB write
- `schema_drift_check` — ShortCircuitOperator; reads XCom from `download_and_land`, skips alert if no drift
- `schema_drift_alert` — logs structured WARNING per drifted file (wire to Slack/PagerDuty in prod)
- `load_bronze` → `task_load_bronze()` — normalize, parse, write 3 Iceberg tables

---

## Register pipeline — data flow detail

```
cricsheet.org/register/people.csv
cricsheet.org/register/names.csv
        │
        ▼
RegisterDownloader.run(snapshot_date, pipeline_run_id)
  ├── HTTP GET + SHA-256 checksum
  ├── Upload → s3://cricket-landing/register_csv/snapshot_date=YYYY-MM-DD/{file}
  ├── Write control.register_ingestion_log row (status=RUNNING → SUCCESS)
  └── Write control.register_schema_versions row (column fingerprint + drift detection)
        │
        ▼
RegisterNormalizer.run(snapshot_date, pipeline_run_id)
  ├── MinIOClient.read_object() → raw bytes
  ├── pl.read_csv(infer_schema_length=0, null_values=[""])  ← all columns = Utf8
  └── _attach_metadata(): _row_hash (SHA-256) + _snapshot_date + _ingested_at + _pipeline_run_id
  → NormalizedRegister(people=LazyFrame, names=LazyFrame)
        │
        ▼
RegisterParser.parse(normalized)
  ├── _parse_persons()          → select core cols (identifier, name, unique_name) + meta
  ├── _parse_person_identifiers() → unpivot key_* → (identifier, key_source, key_value) + meta
  └── _parse_name_variations()  → select (identifier, name) + meta, drop nulls, dedup
  → ParsedRegister(persons, person_identifiers, name_variations)
        │
        ▼
RegisterLoader.load(parsed)  OR  .overwrite_snapshot(parsed)
  └── PolarsIcebergWriter.create_and_append() × 3
        ├── inject meta cols (_snapshot_date as Date, _ingested_at, _dag_run_id, _source_file, _source_url)
        ├── create table if not exists (schema inferred from DataFrame)
        ├── build PartitionSpec (_snapshot_date, IdentityTransform)
        └── PyIceberg append → Iceberg REST catalog → MinIO Parquet files
```

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

Resolution order: env vars > `.env` file > `conf/base/*.yaml` > Pydantic defaults.

---

## Test coverage (as of 2026-05-11)

| Test file | Tests | Coverage area |
|-----------|-------|---------------|
| `tests/unit/test_settings.py` | 16 | Repo root resolution, Docker service name defaults, DSN format, env override, S3 paths |
| `tests/unit/test_example.py` | 1 | Placeholder |
| `tests/unit/ingestion/register/test_normalize.py` | 18 | All-string schema, metadata cols, row hash, error handling, schema drift |
| `tests/unit/ingestion/register/test_parse.py` | 4 | `parse_from_dfs()` round-trip equivalence |
| `tests/unit/transform/polars/bronze/test_register_loader.py` | 24 | Table FQNs, write delegation, empty frames, overwrite_snapshot, from_settings |
| **Total** | **63** | |

All tests run in < 1 second with no external dependencies (all I/O mocked).
