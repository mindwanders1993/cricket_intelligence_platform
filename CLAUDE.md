# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working agreement (Karpathy skills)

Behavioural guardrails for agent-assisted edits in this repo. Adapted from [forrestchang/andrej-karpathy-skills](https://github.com/forrestchang/andrej-karpathy-skills). These bias toward caution over speed — use judgement on trivial tasks.

### 1. Think before coding
*Don't assume. Don't hide confusion. Surface tradeoffs.*

- State assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them — don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

*Applied here:* Data-pipeline invariants (grain, partition keys, snapshot semantics) are easy to assume wrong. Before writing a JOIN against a Silver table, verify the right-side key is actually unique at that grain. Multi-wicket deliveries broke `fact_delivery` exactly this way — a 30-second "is this key unique?" check would have caught it.

### 2. Simplicity first
*Minimum code that solves the problem. Nothing speculative.*

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.
- Sanity check: *"Would a senior engineer say this is overcomplicated?"*

*Applied here:* Writers are intentionally thin (`PolarsIcebergWriter`, `SparkIcebergWriter`). Don't wrap PyIceberg/Spark calls in defensive try/except for exceptions that can't fire. Don't introduce a Pydantic model for a one-shot DAG payload — XCom takes plain dicts. Don't add a `force` flag to functions that already get one from upstream.

### 3. Surgical changes
*Touch only what you must. Clean up only your own mess.*

- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it — don't delete it.
- Remove imports/variables/functions YOUR changes made unused; don't remove pre-existing dead code unless asked.
- The test: every changed line should trace directly to the user's request.

*Applied here:* This is a contract graph (Bronze → Silver → Gold → DuckDB → dbt → validation). An "improvement" to `naming.py`, `META`, or a writer signature can silently break every downstream consumer. Keep changes local to the task; raise concerns about adjacent code in chat rather than editing it.

### 4. Goal-driven execution
*Define success criteria. Loop until verified.*

Transform vague tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, write a brief plan with a verify-check per step.

*Applied here:* This repo already has strong verify-loops — use them as success criteria up front, not as afterthoughts:
- Gold/dbt change → success = `poetry run dbt test` (40 tests) passes + relevant section of `analysis/validation_queries.sql` returns expected counts.
- DAG change → success = `make dag-validate` clean + the DAG runs green end-to-end.
- Bronze/Silver writer change → success = `poetry run pytest tests/unit/transform/` + a real snapshot write to MinIO that reads back correctly.

**These guidelines are working if:** fewer unnecessary lines in diffs, fewer rewrites due to overcomplication, and clarifying questions come *before* implementation rather than after a broken pipeline run.

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

# Gold — refresh DuckDB views from Iceberg + run dbt star schema build
poetry run python -m cip.ingestion.jobs.run_gold_dbt_models   # refresh views + run dbt (used by DAG)
cd models/dbt && poetry run dbt run                            # dbt build only
cd models/dbt && poetry run dbt test                           # dbt tests (40 tests)

# DuckDB serving UI (browser at http://localhost:4213)
make duckdb-ui     # opens the DuckDB built-in web UI on storage/duckdb/cricket.duckdb
make duckdb-stop   # release the file lock BEFORE running dag_run_gold_dbt_models

# Metabase — BI dashboards
python scripts/provision_metabase_dashboards.py  # (re)provision Cricket dashboards + filters via Metabase API
# Note: Metabase must be running (make up) before provisioning. Safe to re-run — uses upsert semantics.
```

Line length is 120 for ruff, black, and isort.

## Project skills

User-invocable workflows in `.claude/skills/` — type `/<name>` to trigger. Each skill's `SKILL.md` is the source of truth for what it does and how it composes existing scripts/jobs.

| Skill | Purpose |
|---|---|
| `/cip-pipeline-run` | Run any of the 4 ingest/Silver jobs locally with the right env-var profile (Polars vs Spark). Maps natural language → pipeline + task + date. |
| `/cip-gold-refresh` | Release the DuckDB UI file lock if held, then rebuild Bronze+Silver DuckDB tables + run `dbt run` + `dbt test`. Hands UI restart back to the user. |
| `/cip-validate` | Auto-picks `pre-push` / `pre-pr` / `milestone` mode for `validation/run.sh` based on git state. Confirms cost before running milestone (~$1–2). |
| `/cip-inspect-table` | Uniform inspection of any Bronze/Silver/Gold table — row count, snapshot histogram, schema, sample rows. Bronze/Silver via PyIceberg+Polars; Gold via DuckDB. |
| `/cip-diagnose-dag` | Pulls task states + filesystem logs + `control.*_ingestion_log` row + landing artifacts for a failed Airflow run; pattern-matches the error against known failure modes. |
| `/cip-add-silver-table` | Scaffolds a new Bronze→Silver entity across 6 files (naming.py, transform, test, DQ, DAG, job). Stubs only — does not invent transform logic. |

All skills support `--help`. Project-scoped (committed in `.claude/skills/`) so the toolkit travels with the repo.

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
| Metabase | http://localhost:3000 | `admin@cricket-platform.local` | `Cricket2026!` |

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

### Gold layer (dbt + DuckDB)

dbt project lives at `models/dbt/` (profile `cricket`, target `dev`). The Gold layer is a star schema materialised in **both** DuckDB (for serving) and Iceberg (via the future `bronze`/`silver`/`gold` external materialisations — currently DuckDB-only).

- **Sources** (`models/dbt/models/sources.yml`): Silver Iceberg tables exposed via the `silver` schema in DuckDB.
- **Staging** (`models/dbt/models/staging/`): one `stg_silver_*` per source — minor renames + type coercion only.
- **Marts**:
  - Dimensions (`models/dbt/models/marts/dimensions/`): `dim_match`, `dim_player`, `dim_team`, `dim_venue`, `dim_competition`, `dim_official`.
  - Facts (`models/dbt/models/marts/facts/`): `fact_delivery`, `fact_innings`, `fact_match_result`, `fact_player_match`, `fact_player_of_match`.
  - Marts/aggregates (`models/dbt/models/marts/aggregates/`): batting/bowling career & season summaries.

**`fact_player_of_match` grain:** one row per `(match_id, player_name)`. Bridge table that explodes the `player_of_match` array from `stg_silver_matches`. Tied matches (EC-006) produce multiple rows. `QUALIFY ROW_NUMBER() OVER (PARTITION BY match_id, player_name ORDER BY _snapshot_date) = 1` guards against duplicate names within the same source array (data-quality artifact in source JSON). Custom dbt test `fact_player_of_match_unique_grain` enforces the grain.

**`fact_delivery` grain rule:** one row per ball. The wickets and match_players CTEs are deduped with `QUALIFY ROW_NUMBER()` **before** the LEFT JOIN — otherwise multi-wicket deliveries (e.g. caught + non-striker run-out on the same ball) inflate the fact. The wickets dedup prefers bowler-credited kinds (`caught`, `bowled`, `lbw`, `stumped`, `caught and bowled`, `hit wicket`) so the surfaced `dismissal_kind` matches scoring convention.

**`fact_player_match.person_id` is sparse (~1.27%)** — Cricsheet rarely includes registry IDs in match JSONs. No `not_null` test; name-based joins on `dim_player.full_name` close the gap at query time.

### DuckDB serving layer (`src/cip/serving/duckdb/`)

`DuckDBRefresh` (`refresh.py`) materialises **native DuckDB tables** from Iceberg under the `bronze` and `silver` schemas before dbt runs. **Not views** — the DuckDB UI opens fresh connections that don't inherit session-scoped settings like `unsafe_enable_version_guessing`, so Iceberg-scan views fail when queried from the UI. Materialising as tables sidesteps this entirely.

- `create_bronze_views()` / `create_silver_views()` (misnomers — both create TABLES) filter `WHERE _snapshot_date = (SELECT MAX(_snapshot_date) FROM iceberg_scan(...))`. Silver Iceberg accumulates partitions across re-runs; the filter ensures dim/fact PKs stay unique.
- `_drop_if_view()` handles legacy view-to-table migration by inspecting `information_schema.tables` before dropping (DROP VIEW IF EXISTS is type-strict in DuckDB).

**DuckDB UI workflow (`make duckdb-ui`):** DuckDB has single-writer + multiple-reader semantics, but the UI itself holds a write lock for its `_ui` internal catalog. **Always run `make duckdb-stop` before triggering `dag_run_gold_dbt_models`**, otherwise the DAG's `refresh_duckdb_views` task fails with a file-lock error. Re-launching the UI afterwards is one command.

The DB file lives at `storage/duckdb/cricket.duckdb` (bind-mounted into the Airflow containers via `compose.dev.yml`, so the host CLI and the DAG share the same file). Bind mount, **not** a Docker named volume — named volumes have stricter lock semantics across host/container processes.

### Metabase BI layer

Metabase v0.60.6 (OSS) + community DuckDB driver (`motherduckdb/metabase_duckdb_driver 1.5.2.0`). Runs as `compose-metabase-1` on port 3000. Reads `storage/duckdb/cricket.duckdb` in **read-only mode** — it never writes to DuckDB.

- Custom Docker image (`infra/docker/metabase/Dockerfile`) based on `eclipse-temurin:21-jre-jammy` (Ubuntu glibc). The official Alpine image causes JNI segfaults with jemalloc — do not switch back to Alpine.
- Dashboards are provisioned via `scripts/provision_metabase_dashboards.py`. Re-run after a volume wipe or to push SQL changes.
- **DuckDB lock with Metabase:** Metabase holds a read connection at all times. Before running `dag_run_gold_dbt_models`, you must stop Metabase (`docker stop compose-metabase-1`), run the DAG, then restart. Or use `make duckdb-stop` for host-side lock release — but that doesn't release Metabase's connection. See `docs/runbooks/dashboard.md` §5.
- **Metabase field filter + table aliases:** Metabase dimension field filters emit fully qualified column references (`"gold"."fact_delivery"."batter" = ?`). DuckDB resolves this against the physical table name — if the SQL query uses a table alias (`fd` instead of `gold.fact_delivery`), DuckDB raises `"Referenced table 'gold.fact_delivery' not found! Candidate tables: 'fd'"`. Fix: **never use table aliases in Player Spotlight SQL cards**. Use `gold.fact_delivery.column` throughout.
- **Cricsheet player names:** Cricsheet uses abbreviated initials (`V Kohli`, `RG Sharma`). Dropdown search for `Virat Kohli` returns nothing. Planned fix: `scripts/data/player_aliases.csv` seed → `gold.player_display_names` table. See `docs/runbooks/dashboard.md` §12.
- For admin password recovery or full re-provision instructions, see `docs/runbooks/dashboard.md`.

### Validation harness

`analysis/validation_queries.sql` is a hand-curated 9-section suite (~30 queries) for end-to-end lakehouse correctness checks: row counts across all 33 tables, Bronze integrity, Silver grain uniqueness, Gold dim PKs, fact↔dim referential integrity, cross-layer reconciliation (deliveries/wickets/matches), business rules, mart sanity, freshness. Run by pasting into `make duckdb-ui` — section 7.4 expects a small non-zero wicket diff (multi-wicket deliveries: 10 with 2 wickets, 1 with 10 wickets in the current snapshot).

### Edge cases to know

- `season` field in match JSON can be a string `"2011/12"`, string `"2026"`, or integer `2007` — normalise in Silver.
- `wickets[n].player_out` is the authoritative dismissal subject, not `batter` (they differ on run-outs).
- `key_*` columns in `people.csv` are unpivoted to long-form in Bronze (`bronze.people_identifiers` table) — new `key_*` columns flow through automatically without code changes.
- YAML match files are intentionally skipped at Bronze; only `.json` files are processed.
- Silver Iceberg tables accumulate `_snapshot_date` partitions across re-runs; any consumer (dbt staging, DuckDB materialisation) must filter to `MAX(_snapshot_date)` or it will see duplicates.
- Multi-wicket deliveries exist in the source — never assume `(match_id, innings_number, over_number, delivery_number)` is unique in `silver.wickets`.
- Cricsheet player names are abbreviated initials (`V Kohli`, not `Virat Kohli`). Any consumer that needs display names must maintain a separate alias table — do not assume full names exist in `gold.fact_delivery.batter` or `gold.mart_player_batting.player_name`.
- `player_of_match` in match JSON is an array and can contain the same name twice (data artefact). Always dedup with `QUALIFY ROW_NUMBER() OVER (PARTITION BY match_id, player_name ...)` when exploding this array.

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

Rules:
- ALWAYS read graphify-out/GRAPH_REPORT.md before reading any source files, running grep/glob searches, or answering codebase questions. The graph is your primary map of the codebase.
- IF graphify-out/wiki/index.md EXISTS, navigate it instead of reading raw files
- For cross-module "how does X relate to Y" questions, prefer `graphify query "<question>"`, `graphify path "<A>" "<B>"`, or `graphify explain "<concept>"` over grep — these traverse the graph's EXTRACTED + INFERRED edges instead of scanning files
- After modifying code, run `poetry run graphify update .` to keep the graph current (AST-only, no API cost).
- `graphify` is not on the system PATH — always invoke via `poetry run graphify`.
