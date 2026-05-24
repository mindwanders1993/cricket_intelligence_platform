# Developer Quick Reference — Cricket Intelligence Platform

> Day-to-day commands, conventions, and decisions. Keep this open while coding.
> Sections below cover the **as-built** platform; revamp v2 additions are flagged inline.

---

## Revamp v2 quick links

| Need | Doc |
|---|---|
| What's the canonical plan? | `docs/planning.md` |
| Personal scratch plan | `~/.claude/plans/hi-soft-prism.md` |
| Target architecture | `docs/architecture/hld-hla.md` |
| Per-record data flow | `docs/architecture/data-flow.md` |
| Service interactions + ports + locks | `docs/architecture/service-interactions.md` |
| Repo navigation | `docs/architecture/repo-structure.md` |
| Founding principle | `docs/adr/0004-open-standards-first.md` |
| All ADRs | `docs/adr/README.md` |

---

## Daily commands

```bash
# Start / stop everything
make up           # starts MinIO, PostgreSQL, Iceberg REST, Airflow
make down         # stops all containers

# Bootstrap (run once after first `make up`)
make bootstrap    # creates MinIO buckets + runs init-metastore.sql

# Tests
poetry run pytest                                              # all tests
poetry run pytest tests/unit/                                  # unit tests only
poetry run pytest -k "test_row_hash"                           # single test by name
poetry run pytest tests/unit/transform/polars/bronze/ -v       # one directory

# Lint (must be clean before every commit)
poetry run ruff check --exclude ".claude/" .                   # check
poetry run ruff check --fix --exclude ".claude/" .             # auto-fix
poetry run black --exclude ".claude/" .                        # format
# Run ruff --fix AFTER black to resolve I001 conflicts

# Manual pipeline run (no Airflow needed)
poetry run python -m cip.ingestion.jobs.ingest_people_and_names --task all
poetry run python -m cip.ingestion.jobs.ingest_people_and_names --snapshot-date 2026-05-11 --task download
poetry run python -m cip.ingestion.jobs.ingest_people_and_names --snapshot-date 2026-05-11 --task bronze --force

# Airflow DAG validation
make dag-validate
```

---

## Module paths (`src/cip/`)

```
common/
  settings.py         → get_settings()  ← call this, never instantiate PlatformSettings directly
  logging.py          → get_logger(__name__), bind_context(), get_context()
  exceptions.py       → IcebergError, StorageError, IngestionError, TransformError ...
  contracts/
    enums.py          → Layer, MatchType, WicketKind, ExtraType, Gender, IngestionStatus ...
    naming.py         → TableName, PathBuilder, META, DagNames, IcebergProperties

ingestion/
  io/minio.py         → MinIOClient.from_settings() — health_check(), read_object(), upload ...
  register/
    download.py       → RegisterDownloader.from_settings().run(snapshot_date, pipeline_run_id)
    normalize.py      → RegisterNormalizer.from_settings().run(snapshot_date, pipeline_run_id)
    parse.py          → RegisterParser.parse(normalized) → ParsedRegister
  jobs/
    ingest_people_and_names.py → task_download_and_land(), task_load_bronze()  (Airflow callables)

transform/
  polars/bronze/
    register_loader.py → RegisterLoader.from_settings().load(parsed) / .overwrite_snapshot(parsed)
  shared/
    writers.py         → PolarsIcebergWriter, SparkIcebergWriter
    readers.py         → PolarsIcebergReader, SparkIcebergReader, DuckDBIcebergReader
    partitioning.py    → PartitionStrategy.for_table(fqn)
```

---

## Invariants — never break these

### 1. Bronze is all-string
Every CSV and JSON column arrives at Bronze as `Utf8`. No type casting in Bronze. Casting lives in Silver dbt models only.

```python
# correct
df = pl.read_csv(f, infer_schema_length=0, null_values=[""])

# wrong — breaks source fidelity
df = pl.read_csv(f)  # Polars will infer integers, floats, dates
```

### 2. No raw f-string paths or table names
```python
# correct
fqn = TableName.bronze("register_people")
path = PathBuilder.landing_register_csv("people.csv", snapshot_date)

# wrong
fqn = f"cricket.bronze.register_people"
path = f"s3://cricket-landing/register_csv/{snapshot_date}/people.csv"
```

New tables must be added to `TableName.BRONZE_TABLES` / `SILVER_TABLES` / `GOLD_TABLES` before use.

### 3. Metadata columns via META constants
```python
# correct
META.SNAPSHOT_DATE   # → "_snapshot_date"
META.ROW_HASH        # → "_row_hash"

# wrong
"_snapshot_date"
```

### 4. XCom payloads: primitives only
No DataFrames, no datetime objects, no Polars/Spark objects in XCom. Plain JSON-serialisable dicts.

### 5. get_settings() is the singleton
```python
from cip.common.settings import get_settings
cfg = get_settings()
```
Never `PlatformSettings()` directly in application code.

### 6. Idempotency pattern
Every task checks the control DB before writing. `force=True` bypasses.
- Register: `control.register_ingestion_log` (source_file, snapshot_date)
- Match: `control.archive_download_log` (archive_file, snapshot_date)
- Bronze match: `control.bronze_match_ingestion_log` (archive_file, snapshot_date)

---

## Table naming

| Layer | Pattern | Example |
|-------|---------|---------|
| Bronze | `cricket.bronze.<entity>` | `cricket.bronze.register_people` |
| Silver | `cricket.silver.<entity>` | `cricket.silver.deliveries` |
| Gold | `cricket.gold.<entity>` | `cricket.gold.fact_delivery` |

### Known tables

**Bronze:** `match_documents`, `register_people`, `register_identifiers`, `register_name_variations`

**Silver:** `matches`, `innings`, `deliveries`, `wickets`, `teams`, `venues`, `competitions`, `persons`, `person_identifiers`, `match_players`, `match_officials`

**Gold:** `dim_player`, `dim_match`, `dim_team`, `dim_venue`, `dim_competition`, `dim_date`, `fact_delivery`, `fact_innings`, `fact_match_result`, `fact_player_match`, `mart_player_batting`, `mart_player_bowling`, `mart_team_performance`, `mart_venue_dna`, `mart_phase_scoring`, `mart_toss_outcome`, `mart_matchup_analysis`

---

## MinIO paths (landing)

| Data | Path |
|------|------|
| Match archive ZIPs | `s3://cricket-landing/raw_zips/snapshot_date=YYYY-MM-DD/<archive>.zip` |
| Extracted JSON files | `s3://cricket-landing/extracted_json/snapshot_date=YYYY-MM-DD/<match_id>.json` |
| Register CSVs | `s3://cricket-landing/register_csv/snapshot_date=YYYY-MM-DD/people.csv` |
| Iceberg warehouse | `s3://iceberg-warehouse/<layer>/<table>/` |

---

## Iceberg catalog

- Catalog name: `cricket`
- REST endpoint: `http://iceberg-rest:8181` (inside Docker) / `http://localhost:8181` (host)
- Backed by: PostgreSQL metastore + MinIO object store
- Namespaces: `bronze`, `silver`, `gold`

---

## Settings resolution order (highest wins)

1. Real environment variables (CI, Docker, shell exports)
2. `.env` file at repo root
3. `conf/base/*.yaml` files
4. Pydantic field defaults

Key env vars:

| Var | Default | Description |
|-----|---------|-------------|
| `MINIO_S3_ENDPOINT` | `http://minio:9000` | MinIO API endpoint |
| `POSTGRES_HOST` | `postgres` | PostgreSQL service name |
| `POSTGRES_USER` | `cricket_user` | |
| `POSTGRES_PASSWORD` | `cricket_pass` | |
| `POSTGRES_DB` | `cricket_platform` | |
| `ICEBERG_REST_URI` | `http://iceberg-rest:8181` | |
| `LOG_LEVEL` | `INFO` | |
| `ENV_NAME` | `dev` | `dev` or `prod` |
| `CIP_REPO_ROOT` | (derived from `__file__`) | Override repo root for CI |

---

## Writer cheatsheet

```python
# Bronze (Polars → PyIceberg)
writer = PolarsIcebergWriter.from_settings()
rows = writer.create_and_append(
    df=df,
    fqn=TableName.bronze("register_people"),
    snapshot_date="2026-05-11",
    layer=Layer.BRONZE,
    partition_cols=[META.SNAPSHOT_DATE],   # always for Bronze
    pipeline_run_id=run_id,
    source_file="people.csv",
    source_url="https://cricsheet.org/register/",
)

# Silver (Spark → Iceberg)
writer = SparkIcebergWriter.from_spark(spark)
writer.dynamic_overwrite(
    df=silver_df,
    fqn=TableName.silver("deliveries"),
    snapshot_date="2026-05-11",
    pipeline_run_id=run_id,
)
```

---

## Test conventions

- Unit tests mock all I/O — no MinIO, no Iceberg catalog, no PostgreSQL
- `PolarsIcebergWriter` is always injected via constructor, never instantiated inside tested code
- `conftest.py` has `clear_settings_cache` (autouse) — every test starts with clean settings
- Call `invalidate_settings_cache()` explicitly when mutating env vars mid-test
- Test files contain **only** imports, fixtures, and test classes — never paste source code into test files

---

## Lint quick rules

- Line length: **120** (ruff, black, isort all set to 120)
- Import order: ruff is the authority — run `ruff --fix` after `black` to resolve any I001 conflicts
- Always exclude `.claude/` when running lint (agent worktrees live there)
- Unused imports: ruff F401 — auto-fixable with `--fix`

---

## Known gotchas in source data

| Gotcha | Detail |
|--------|--------|
| `season` is polymorphic | String `"2011/12"`, string `"2026"`, or integer `2007` — normalise in Silver |
| Dismissal subject | `wickets[].player_out` is correct, NOT `batter` — they differ on run-outs |
| YAML match files | Present in archive but intentionally skipped — only `.json` files processed |
| Empty key_* cells | Cricsheet uses empty string for absent external IDs — must use `null_values=[""]` |
| `gender`/`dob` in people.csv | Were present in old formats; absent from current Cricsheet register — do not assume |
| Match `registry.people` | Maps display name → Cricsheet identifier — Silver must use this dict for identity resolution, not fuzzy name match |
| No `event` block | Some friendly/unofficial matches have no `event` block |
| No `outcome.by` | No-result and tie matches have no `by` sub-block |

---

## Revamp v2 — new conventions (Sprints 0–4)

These conventions kick in as their sprint lands. Until then, treat them as "what's coming".

### Observability emission (Sprint 0)

Every writer call (Polars + Spark) is wrapped with three side-effects in this order:

```python
with telemetry.span("bronze.match_data.write") as span:
    span.set_attribute("snapshot_date", snapshot_date)
    span.set_attribute("pipeline_run_id", pipeline_run_id)

    lineage.emit_run_event(
        event_type="START",
        run_id=pipeline_run_id,
        job_name="bronze.match_data.write",
        inputs=[...], outputs=[...],
    )

    rows_written, bytes_written = writer.create_and_append(...)

    cost_emission.record(
        pipeline_run_id=pipeline_run_id,
        target_table="bronze.match_data",
        rows_written=rows_written,
        bytes_written=bytes_written,
        executor_seconds=span.elapsed_seconds(),
        wall_time_seconds=span.elapsed_seconds(),
    )

    lineage.emit_run_event(event_type="COMPLETE", ...)
```

Helpers live in `src/cip/observability/{telemetry,lineage,cost_emission}.py` — don't reinvent.

### dbt SCD2 + incremental pattern (Sprint 0)

- SCD2 → snapshot in `models/dbt/snapshots/<entity>_snapshot.sql`; view in `models/dbt/models/marts/dimensions/<entity>_scd2.sql`; current-row view replaces the original `dim_<entity>` model.
- Incremental → `{{ config(materialized='incremental', unique_key='delivery_uid', on_schema_change='append_new_columns', incremental_strategy='delete+insert') }}` (DuckDB) / `'merge'` (BigQuery).
- Custom test: `models/dbt/macros/test_grain_uniqueness.sql` — call it from any model that has a composite grain.

### MetricFlow semantic layer (Sprint 0)

- Semantic models in `models/dbt/models/semantic_models/<entity>.yml`.
- Metrics in `models/dbt/models/metrics/<name>.yml`.
- `mf list metrics` to verify.
- `mf query --metrics batting_average --group-by player__full_name` for ad-hoc.

### FastAPI conventions (Sprint 1)

- Router per concern under `src/cip/serving/api/routers/`.
- All routes OTEL-instrumented automatically via middleware.
- Pydantic models for every request/response (no untyped JSON).
- SQL queries go through `services/sql_guardrails.py` AST walker before execution.
- `services/metricflow_client.py` resolves metric requests; never bypass it for chat-issued queries.

### AI agent conventions (Sprint 2)

- Agent has **no raw SQL ability** — only tools.
- Tools live in `src/cip/serving/ai/tools/<tool>.py`, one file per tool.
- Each tool returns a Pydantic model — no raw dicts.
- System prompts in `prompt_registry/system_*.md`; tool docstrings in `prompt_registry/tool_*.md`; few-shot examples in `prompt_registry/few_shot/`.
- Golden eval in `apps/ai-studio/evaluation/eval_questions.yml` — must stay ≥80% pass rate to merge.
- LLM provider switchable via `AISettings.llm_provider`: `"ollama"` (local) or `"bedrock"` (cloud).

### BigQuery target switching (Sprint 3)

- `cd models/dbt && dbt build --target dev` → DuckDB
- `cd models/dbt && dbt build --target bq_dev` → BigQuery
- Both must succeed before a Sprint 3 PR merges.
- `make bq-sync` populates BigQuery from local Iceberg.
- Models use `{{ target.name }}` Jinja to switch source schemas + materialization config.

### Soda Core DQ (Sprint 0)

- Checks in `quality/soda/checks/<table>.yml`.
- Datasource config in `quality/soda/configuration.yml`.
- `make soda-scan` to run all checks.
- Failures land in `control.dq_results` alongside dbt-test failures.

### New control schema tables (Sprint 0+)

| Table | Sprint | Purpose |
|---|---|---|
| `control.pipeline_cost_event` | 0 | Per-task rows/bytes/seconds → FinOps mart |
| `control.ai_metadata_refresh_log` | 2 | When was the dbt-docs embedding cache last rebuilt |

### Updated tables list (after revamp v2)

**Bronze (unchanged):** `match_data`, `people`, `people_identifiers`, `name_variations`

**Silver (unchanged):** `matches`, `innings`, `deliveries`, `wickets`, `teams`, `venues`, `competitions`, `persons`, `person_identifiers`, `name_variations`, `match_players`, `match_officials`. **+ Sprint 4:** `deliveries_synth` (100M-row synthetic table for perf testing)

**Gold (additions):**
- Sprint 0: `dim_player_scd2`
- Sprint 1: `mart_pipeline_cost_daily`, `mart_top_expensive_tasks`, `mart_data_freshness`
- Sprint 3: same tables built in `cricket_silver_*` BigQuery dataset

### Make-target additions (per sprint)

| Sprint | Targets added |
|---|---|
| 0 | `obs-up`, `obs-down`, `soda-scan` |
| 1 | `api-up`, `api-down`, `lightdash-up` |
| 2 | `ai-up`, `ai-down`, `ai-eval` |
| 3 | `bq-sync`, `bq-build`, `tf-plan-bq`, `tf-plan-aws` |
| 4 | `synth-100m` |

---

## Open-standards-first checklist (every new dependency)

Before adding any new dependency:

1. What open protocol does it speak? *(S3 API, Iceberg REST, OpenLineage, OTEL, ANSI SQL, dbt manifest, OpenAPI, etc.)*
2. What's the managed cloud cousin? *(AWS S3 / Datadog / DataHub / etc.)*
3. Is the only delta to migrate **endpoint config**, not code?

If any answer is "no" or unclear → use a different tool, or write a one-paragraph ADR waiving the principle.

See `docs/adr/0004-open-standards-first.md` for worked examples.
