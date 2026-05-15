# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Infrastructure
make build-airflow   # Build custom Airflow image (Java + PySpark baked in) — run once after clone or Dockerfile change
make up              # Start all services (requires .env — copy from .env.example)
make down            # Stop all services
make bootstrap       # Create MinIO buckets + run PostgreSQL control schema DDL
make dag-validate    # List Airflow DAG import errors and verify expected DAGs exist

# Tests
poetry run pytest                                          # All tests
poetry run pytest tests/unit/test_settings.py             # Single file
poetry run pytest tests/unit/transform/polars/bronze/     # Directory
poetry run pytest -k "test_add_row_hash"                  # Single test by name

# Lint & format
poetry run ruff check .
poetry run ruff check --fix .          # auto-fix (handles I001 import order too)
poetry run black --check .
poetry run isort --check-only .
poetry run pre-commit run --all-files

# Note: ruff's I001 rule differs from isort — always use `ruff --fix` (not isort) to
# resolve import-sort errors reported by ruff. Run isort separately only if needed.

# Manual pipeline run (dev, no Airflow needed)
# People & Names — Landing → Bronze
poetry run python -m cip.ingestion.jobs.ingest_people_and_names --task all
poetry run python -m cip.ingestion.jobs.ingest_people_and_names --snapshot-date 2026-05-11 --task download
poetry run python -m cip.ingestion.jobs.ingest_people_and_names --snapshot-date 2026-05-10 --task bronze --force
# People & Names — Bronze → Silver + DQ
poetry run python -m cip.ingestion.jobs.build_silver_people_and_names --task all
poetry run python -m cip.ingestion.jobs.build_silver_people_and_names --snapshot-date 2026-05-11 --task silver
poetry run python -m cip.ingestion.jobs.build_silver_people_and_names --snapshot-date 2026-05-11 --task dq
# Match data — All match data Silver build
ICEBERG_REST_URI=http://localhost:8181 MINIO_S3_ENDPOINT=http://localhost:9000 POSTGRES_HOST=localhost SPARK_DRIVER_MEMORY=8g SPARK_MASTER=local[2] poetry run python -m cip.ingestion.jobs.build_silver_match_data --snapshot-date 2026-05-01 --task all
```

Line length is 120 for ruff, black, and isort.

## Architecture Overview

This is a cricket data lakehouse — a **medallion architecture** (Landing → Bronze → Silver → Gold) that ingests Cricsheet data, transforms it through layers, and serves it via DuckDB, a FastAPI, and an AI assistant.

### Data layers

| Layer | Storage | Engine | Description |
|-------|---------|--------|-------------|
| Source files | MinIO (`cricket-source-files`) | — | Raw downloads (ZIPs, CSVs, extracted JSONs), never modified |
| Bronze | Iceberg (`cricket-lakehouse/bronze/`) | Polars + PyIceberg | All-string ingestion, source-faithful |
| Silver | Iceberg (`cricket-lakehouse/silver/`) | PySpark + Polars | Typed, exploded, deduplicated |
| Gold | Iceberg (`cricket-lakehouse/gold/`) + DuckDB | dbt | Star schema dims/facts/marts |
| ML models | MinIO (`cricket-ml-models`) | MLflow | Trained models, run artifacts |
| Control | PostgreSQL (`control` schema) | psycopg2 | Audit logs, DQ results, schema versions |

### Key source data

- `SRC-001`: `all_matches.zip` — 21,600+ match JSON files (primary backfill source)
- `SRC-002/003`: `people.csv` / `names.csv` — player identity register (must be loaded before Silver match transforms)

**Bronze rule**: All CSV/JSON columns ingested as strings (`infer_schema_length=0`). Type casting is deferred entirely to Silver dbt models.

**Dedup rule**: Bronze primary key for matches is `(match_id, revision)` — corrections are appended as new rows; Silver reads only `MAX(revision)` per `match_id`.

### Iceberg catalog

Catalog name: `cricket` (REST catalog). Table FQNs are 2-segment `<layer>.<entity>` (e.g. `bronze.people`, `silver.deliveries`) — the catalog name is **not** in the FQN. MinIO physical layout is layer-first (Option A): `cricket-lakehouse/bronze/{table}/`, `cricket-lakehouse/silver/{table}/`. The REST catalog runs at `http://iceberg-rest:8181` backed by PostgreSQL + MinIO.

### Module layout (`src/cip/`)

```
common/
  settings.py        — PlatformSettings singleton; call get_settings() everywhere, never instantiate directly
  contracts/
    enums.py         — StrEnum for all magic strings (Layer, MatchType, WicketKind, etc.)
    naming.py        — TableName, PathBuilder, META, DagNames — all table FQNs and S3 paths built here
  exceptions.py      — IcebergError and other platform exceptions
  logging.py         — structlog wrapper; use get_logger(__name__)

ingestion/
  match_data/        — ZIP download, checksum, extraction (Cricsheet all_matches.zip)
  people_and_names/  — people.csv + names.csv download, parse, normalize
  io/
    minio.py         — MinIOClient with health_check(); use from_settings() factory
  jobs/              — Thin Airflow-callable wrappers (one function per DAG task)

transform/
  polars/bronze/     — Polars → PyIceberg Bronze writers
  polars/silver/     — Polars → PyIceberg Silver transforms (Register pipeline)
  spark/silver/      — PySpark → Iceberg Silver transforms (Match pipeline; kept for reference)
  shared/
    writers.py       — PolarsIcebergWriter (Bronze + Silver Register) and SparkIcebergWriter (Silver Match+)
    readers.py       — PyIceberg catalog helpers; PolarsIcebergReader for Silver reads
    partitioning.py  — Iceberg partition spec builders

serving/
  api/main.py        — FastAPI app
  duckdb/            — DuckDB refresh job (reads Iceberg, serves Gold)
  ai/                — LLM chains, semantic layer, prompt registry

quality/             — DQ checks per layer, reconciliation, reporting
ml/                  — Feature engineering, training, scoring, MLflow tracking
```

### Infrastructure services (Docker Compose)

`infra/compose/compose.base.yml` defines: MinIO, PostgreSQL, Iceberg REST catalog, Airflow (init + webserver + scheduler). Extend with `compose.dev.yml`. Container prefix is `compose-` (Docker names containers from the folder name `infra/compose`).

### Settings

`get_settings()` returns a cached `PlatformSettings` singleton. Sub-settings are accessed as:

```python
cfg = get_settings()
cfg.storage.endpoint          # MinIO
cfg.iceberg.rest_uri          # Iceberg REST catalog
cfg.postgres.dsn              # SQLAlchemy DSN
cfg.spark.master              # Spark master URL
```

Resolution order: env vars > `.env` file > `conf/base/*.yaml` > Pydantic defaults. Call `invalidate_settings_cache()` in tests between env mutations (already wired in `tests/unit/conftest.py`).

### Naming conventions

Always use the provided builders — no raw f-string path construction:

```python
from cip.common.contracts.naming import TableName, PathBuilder, META, DagNames

TableName.bronze("match_data")                  # → "bronze.match_data"
PathBuilder.landing_register_csv("people.csv", "2026-05-11")
META.SNAPSHOT_DATE                              # → "_snapshot_date"
```

New table names must be added to `TableName.BRONZE_TABLES` / `SILVER_TABLES` / `GOLD_TABLES` before use (strict validation by default).

### Idempotency pattern

Every pipeline task guards against re-runs via `control.register_ingestion_log` (register pipeline) or `control.bronze_ingestion_log` (match pipeline). Pass `force=True` / `--force` to bypass. DAG conf `{"force": true}` propagates via Jinja to all tasks.

### Writers

- **`PolarsIcebergWriter.create_and_append()`** — for Bronze Polars jobs. Creates table on first run, appends thereafter. Always call with `layer=Layer.BRONZE` and `partition_cols=["_snapshot_date"]`.
- **`PolarsIcebergWriter.overwrite_partition()`** — for Silver Polars jobs (Register pipeline). Creates the table if absent, then **replaces only the `_snapshot_date` partition** in the incoming DataFrame — idempotent for re-runs. Always pass `partition_cols=["_snapshot_date"]`.
- **`SparkIcebergWriter.dynamic_overwrite()`** — standard write mode for Silver PySpark jobs (Match pipeline). Accepts optional `partition_cols`; auto-creates the table via `_ensure_table_exists()` on first run, then overwrites only the partitions present in the DataFrame.
- Both writers call `_inject_meta_polars`/`_inject_meta_spark` to stamp mandatory `_snapshot_date`, `_ingested_at`, `_pipeline_run_id`, `_row_hash`, `_source_file`, `_source_url` columns.

**Silver Register uses Polars, not PySpark.** The People & Names Silver build (`build_silver_people_and_names.py`) instantiates `PolarsPeopleAndNamesSilverTransform` (from `transform/polars/silver/persons.py`). PySpark is not required for the People & Names pipeline.

**Spark JAR packages:** `_build_spark_iceberg_conf()` in `readers.py` injects `spark.jars.packages` with the Iceberg runtime + `hadoop-aws` + `aws-java-sdk-bundle`. These are downloaded from Maven Central on first run (requires internet). Versions are controlled by `SparkSettings.iceberg_version`, `.hadoop_aws_version`, `.aws_java_sdk_version` (via env or conf/base/spark.yaml). The catalog config also sets `s3.access-key-id` / `s3.secret-access-key` directly on the Iceberg catalog (not only the Hadoop `fs.s3a.*` keys) — both are required.

**PyIceberg schema conversion (writers.py detail):** `create_and_append()` and `overwrite_partition()` convert PyArrow → PyIceberg schema using private `_pyarrow_to_schema_without_ids()` + public `assign_fresh_schema_ids()` because PyIceberg 0.11.1's `pyarrow_to_schema()` requires pre-existing field IDs. `_build_partition_spec()` receives the **PyIceberg schema** (not the raw PyArrow schema) and resolves field IDs from it. Do not revert to `schema_to_pyiceberg()` — that function was removed in 0.11.x.

**`make dag-validate` requires Docker running.** Run `make up` first; otherwise the Airflow scheduler container won't exist.

### Service URLs and credentials (dev)

| Service | URL | User | Password |
|---|---|---|---|
| Airflow | http://localhost:8080 | `admin` | `AIRFLOW_ADMIN_PASSWORD` from `.env` |
| MinIO Console | http://localhost:9001 | `MINIO_ROOT_USER` | `MINIO_ROOT_PASSWORD` |
| pgAdmin | http://localhost:5050 | `admin@cricket-platform.local` | `admin123` |
| MLflow | http://localhost:5001 | — | — |
| Iceberg REST | http://localhost:8181 (API only — no UI) | — | — |

If the Airflow login fails after a password change in `.env`, reset it in the running container:

```bash
docker exec compose-airflow-scheduler-1 \
  airflow users reset-password --username admin --password <new-password>
```

The admin user is only **created** by `airflow-init` on first boot; subsequent changes to `AIRFLOW_ADMIN_PASSWORD` in `.env` do not propagate to the DB.

### pgAdmin auto-connect

`infra/pgadmin/setup-pgpass.sh` writes a libpq passfile at `/pgpassfile` inside the container at startup, using `POSTGRES_PASSWORD` from the env. `infra/pgadmin/servers.json` references this passfile and connects as `postgres` to the `cricket_platform` DB on host `postgres` without prompting. If you change `POSTGRES_PASSWORD`, recreate the pgAdmin container (`docker compose up -d pgadmin`).

### Container naming

Docker Compose names containers `compose-<service>-<n>` because the project folder is `infra/compose`. The `make dag-validate` target uses these names directly — do not change them without updating the Makefile.

### MLflow dev backend

MLflow runs with a SQLite backend (`sqlite:////mlflow/mlflow.db` on the `mlflow_data` volume) instead of PostgreSQL. Reason: the pre-built `ghcr.io/mlflow/mlflow` image lacks `psycopg2`, and sharing the Airflow PostgreSQL DB caused Alembic revision conflicts. SQLite has no auth, no schema collisions, and zero startup latency — appropriate for dev. Production should use a dedicated Postgres DB with `psycopg2` pre-installed (custom image).

### Airflow DAG pattern

DAG files in `orchestration/airflow/dags/` are thin wrappers. All business logic lives in `src/cip/ingestion/jobs/` or `src/cip/transform/polars|spark/jobs/`. DAG task callables receive `snapshot_date`, `pipeline_run_id`, and `force` as `op_kwargs` via Jinja templates. XCom payloads are plain JSON-serialisable dicts of primitives only — never DataFrames.

### Metadata columns (system columns)

Every Bronze and Silver Iceberg table carries: `_snapshot_date`, `_ingested_at`, `_pipeline_run_id`, `_dag_run_id`, `_source_file`, `_source_url`, `_row_hash`. Silver/Gold also adds SCD2 columns: `_is_current`, `_valid_from`, `_valid_to`. Use `META.*` constants from `naming.py` — never hardcode column name strings.

### Edge cases to know

- `season` field in match JSON can be a string `"2011/12"`, string `"2026"`, or integer `2007` — normalise in Silver.
- `wickets[n].player_out` is the authoritative dismissal subject, not `batter` (they differ on run-outs).
- `key_*` columns in `people.csv` are unpivoted to long-form in Bronze (`bronze.people_identifiers` table) — new `key_*` columns flow through automatically without code changes.
- YAML match files are intentionally skipped at Bronze; only `.json` files are processed.

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

Rules:
- ALWAYS read graphify-out/GRAPH_REPORT.md before reading any source files, running grep/glob searches, or answering codebase questions. The graph is your primary map of the codebase.
- IF graphify-out/wiki/index.md EXISTS, navigate it instead of reading raw files
- For cross-module "how does X relate to Y" questions, prefer `graphify query "<question>"`, `graphify path "<A>" "<B>"`, or `graphify explain "<concept>"` over grep — these traverse the graph's EXTRACTED + INFERRED edges instead of scanning files
- After modifying code, run `poetry run graphify update .` to keep the graph current (AST-only, no API cost).
- `graphify` is not on the system PATH — always invoke via `poetry run graphify`.
