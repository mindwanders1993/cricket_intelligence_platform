# Cricket Intelligence Platform — Development Plan
## Scope: Source → Landing → Bronze → Silver → Gold (Data Warehouse)

> Build in **vertical slices**. The Register identity model affects Silver joins and Gold dimensions.
> Every big task produces a working, tested slice — not isolated tooling.

---

## Status legend
- ✅ Done — built, tested, lint-clean
- 🔄 In progress — partially built
- ⬜ Not started

---

## Big Task 1 — Project and Environment Foundation ✅

> Make the repo runnable, reproducible, and aligned with the HLD.

### Repo essentials ✅
- [x] `.env.example` with all MinIO / PostgreSQL / Airflow / Iceberg vars
- [x] `pyproject.toml` with all runtime + dev deps (polars, pyiceberg, pydantic-settings, airflow, etc.)
- [x] `Makefile` — `up`, `down`, `bootstrap`, `test`, `lint`, `dag-validate`
- [x] `.pre-commit-config.yaml` — ruff, black, isort, pytest hook

### Local infrastructure ✅
- [x] `infra/compose/compose.base.yml` — MinIO, PostgreSQL, Iceberg REST, Airflow (init + webserver + scheduler)
- [x] `infra/compose/compose.dev.yml` — dev overrides
- [x] `infra/bootstrap/create-buckets.sh` — creates all MinIO buckets
- [x] `infra/bootstrap/init-metastore.sql` — control schema DDL (idempotent)

### Control schema tables ✅ (in `init-metastore.sql`)
- [x] `control.register_ingestion_log` — per-file landing audit for Register pipeline
- [x] `control.register_schema_versions` — column fingerprint + drift detection
- [x] `control.register_change_log` — delta row count tracking between snapshots
- [x] `control.archive_download_log` — per-archive landing audit for match pipeline
- [x] `control.bronze_match_ingestion_log` — per-run Bronze load metrics
- [x] `control.dq_results` — central DQ result store (all layers)
- [x] `control.v_latest_register_snapshot` — convenience view
- [x] `control.v_dq_failures` — convenience view
- [x] `control.v_latest_archive_snapshot` — convenience view

### Shared platform modules ✅ (all under `src/cip/`)
- [x] `common/settings.py` — `PlatformSettings` singleton + `get_settings()`
- [x] `common/logging.py` — structlog wrapper (`get_logger`, `get_context`, `bind_context`)
- [x] `common/exceptions.py` — platform exception hierarchy
- [x] `common/contracts/enums.py` — all `StrEnum` for Layer, MatchType, WicketKind, etc.
- [x] `common/contracts/naming.py` — `TableName`, `PathBuilder`, `META`, `DagNames`, `IcebergProperties`
- [x] `ingestion/io/minio.py` — `MinIOClient` with `health_check()`, `from_settings()`
- [x] `transform/shared/readers.py` — `PolarsIcebergReader`, `SparkIcebergReader`, `DuckDBIcebergReader`
- [x] `transform/shared/writers.py` — `PolarsIcebergWriter`, `SparkIcebergWriter`
- [x] `transform/shared/partitioning.py` — `PartitionStrategy` registry

### Tests ✅
- [x] `tests/unit/test_settings.py` — 16 tests: repo root, Docker defaults, DSN, env override, S3 paths
- [x] `tests/unit/conftest.py` — `clear_settings_cache` fixture (autouse)

---

## Big Task 2 — Source Understanding and Contracts ✅

> Document source behavior before building pipelines. See `source_contracts.md` and `architecture.md`.

- [x] Source inventory documented (SRC-001, SRC-002, SRC-003)
- [x] Match JSON schema documented (meta, info, innings, deliveries, wickets)
- [x] Register CSV schemas documented (people.csv, names.csv)
- [x] All 14 known edge cases recorded
- [x] Warehouse contract defined: metadata columns, partition strategy, idempotency rules
- [x] Naming standards enforced via `TableName`, `PathBuilder`, `META` in code

---

## Big Task 3 — Register Pipeline (first vertical slice) 🔄

> Register = identity backbone. Must be stable before Silver match joins.

### Landing ingestion ✅
- [x] `ingestion/register/download.py` — `RegisterDownloader` downloads people.csv + names.csv
- [x] SHA-256 checksum validation
- [x] Date-partitioned MinIO landing: `s3://cricket-landing/register_csv/snapshot_date=YYYY-MM-DD/`
- [x] Audit row written to `control.register_ingestion_log`
- [x] Column fingerprint + drift detection written to `control.register_schema_versions`

### Bronze load ✅
- [x] `ingestion/register/normalize.py` — `RegisterNormalizer` reads landing CSVs, all-string Polars, attaches `_row_hash` + metadata
- [x] `ingestion/register/parse.py` — `RegisterParser` splits normalized frame into 3 shaped frames
- [x] `transform/polars/bronze/register_loader.py` — `RegisterLoader` writes 3 Iceberg Bronze tables
- [x] Target tables (via `TableName.bronze()`):
  - `cricket.bronze.register_people` — one row per person (identifier, name, unique_name + meta)
  - `cricket.bronze.register_identifiers` — unpivoted key_* cols (identifier, key_source, key_value + meta)
  - `cricket.bronze.register_name_variations` — one row per alias from names.csv (identifier, name + meta)
- [x] Partition: `_snapshot_date` (IdentityTransform)
- [x] `load()` → append-only; `overwrite_snapshot()` → delete partition then append

### Airflow DAG ✅
- [x] `orchestration/airflow/dags/dag_ingest_people_and_names.py`
- [x] Schedule: Sunday 00:30 UTC (06:00 IST)
- [x] Tasks: `check_infra` → `download_and_land` → (`schema_drift_check` → `schema_drift_alert`) + `load_bronze` → `done`
- [x] `force=True` propagation via Jinja from `dag_run.conf`
- [x] Schema drift short-circuit branch (informational, never blocks Bronze load)

### Job entrypoint ✅
- [x] `ingestion/jobs/ingest_people_and_names.py` — CLI + Airflow callable wrappers
- [x] `--task all/download/bronze/dbt` + `--force` + `--snapshot-date`

### Tests ✅ (63 unit tests, all passing)
- [x] `tests/unit/ingestion/register/test_normalize.py` — 18 tests
- [x] `tests/unit/ingestion/register/test_parse.py` — 4 tests
- [x] `tests/unit/transform/polars/bronze/test_register_loader.py` — 24 tests

### Register Silver ⬜
- [ ] `silver_persons` — typed, SCD2-tracked persons dimension
- [ ] `silver_person_identifiers` — one row per (identifier, key_source)
- [ ] `silver_name_variations` — deduplicated aliases

### Register DQ ⬜
- [ ] Null identifier checks
- [ ] Duplicate identifier checks in people.csv
- [ ] Orphan check: names.csv identifiers not in people.csv
- [ ] Row count threshold checks (>2% drop = anomaly)
- [ ] Persist to `control.dq_results`

---

## Big Task 4 — Match Source Ingestion and Bronze ⬜

> Historical backfill from `all_matches.zip` (~21,600 JSON match files).

### Match downloader ⬜
- [ ] `ingestion/cricsheet/download.py` — download `all_matches.zip` from cricsheet.org
- [ ] Store raw zip at `s3://cricket-landing/raw_zips/snapshot_date=.../all_matches.zip`
- [ ] SHA-256 checksum + `control.archive_download_log` row
- [ ] `ingestion/cricsheet/extract.py` — extract JSON files to `s3://cricket-landing/extracted_json/snapshot_date=.../`
- [ ] Track file inventory (count, names) in log

### Bronze match documents ⬜
- [ ] `transform/polars/bronze/match_loader.py`
- [ ] Read each JSON file, flatten top-level `meta` + `info` + raw `innings` blob
- [ ] All-string ingestion for document-level fields
- [ ] Target: `cricket.bronze.match_documents` — one row per (match_id, revision)
- [ ] Partition: `_snapshot_date`
- [ ] Dedup key: `(match_id, revision)` — corrections append as new revision rows
- [ ] Log to `control.bronze_match_ingestion_log`

### Airflow DAG ⬜
- [ ] `dag_ingest_all_match_data`
- [ ] Tasks: `check_infra` → `download_archive` → `extract_json` → `load_bronze` → `dq_check` → `done`
- [ ] Skip already-processed snapshots via `control.archive_download_log`

---

## Big Task 5 — Match Silver Core Entities ⬜

> PySpark explodes nested JSON into conformed relational tables.

### Spark environment ⬜
- [ ] Spark Docker image with Iceberg runtime jars
- [ ] MinIO S3 credentials config in SparkSession
- [ ] Verify Spark reads Bronze and writes Silver Iceberg tables

### Core Silver tables ⬜ (all via `SparkIcebergWriter.dynamic_overwrite()`)
- [ ] `cricket.silver.matches` — one row per match, typed fields
- [ ] `cricket.silver.innings` — one row per innings
- [ ] `cricket.silver.deliveries` — one row per ball
- [ ] `cricket.silver.wickets` — one row per wicket (from `deliveries[].wickets[]`)
- [ ] `cricket.silver.teams` — distinct teams
- [ ] `cricket.silver.venues` — distinct venues
- [ ] `cricket.silver.competitions` — distinct events/competitions

### Identity resolution ⬜
- [ ] Extract `info.registry.people` dict (name → identifier) from each match
- [ ] `cricket.silver.match_players` — join registry → `bronze.register_people` → resolved `identifier`
- [ ] `cricket.silver.match_officials` — umpires, tv_umpires, match_referees
- [ ] Unmatched name audit table for any player not in registry

### Key Silver design rules
- `season` must be normalised: string "2011/12" / string "2026" / integer 2007 → all to string "2011/12" or "2026"
- `wickets[].player_out` is the authoritative dismissal subject — NOT `batter` (differ on run-outs)
- Silver reads `MAX(revision)` per `match_id` from Bronze — corrections handled automatically
- SCD2 columns: `_is_current`, `_valid_from`, `_valid_to` on all Silver dimension tables

---

## Big Task 6 — Silver DQ and Reconciliation ⬜

### Structural checks ⬜
- [ ] Null PK checks on all Silver tables
- [ ] Duplicate grain checks
- [ ] Accepted-value checks (match_type, gender, toss decision, wicket kind, extra type)
- [ ] Referential integrity: deliveries → innings → matches

### Cricket-specific reconciliation ⬜
- [ ] Innings total = sum of delivery totals
- [ ] Wicket count ≤ 10 per innings
- [ ] Over/ball sequence is contiguous (no gaps, no duplicates)
- [ ] Winner in outcome consistent with by-runs or by-wickets margin
- [ ] No-result / tie matches have no `by` block

### Identity quality ⬜
- [ ] % of match players resolved to a Register identifier
- [ ] Names present in matches but absent from Register → audit table
- [ ] Persist run-level summary to `control.dq_results`

---

## Big Task 7 — Gold Warehouse Foundation ⬜

### dbt project ⬜
- [ ] `models/dbt/dbt_project.yml` + `profiles.yml`
- [ ] Sources defined over all Silver Iceberg tables
- [ ] Folder structure: `staging/`, `intermediate/`, `dimensions/`, `facts/`, `marts/`

### Dimensions ⬜
- [ ] `dim_player` — from `silver.persons` + `silver.person_identifiers`
- [ ] `dim_match` — from `silver.matches`
- [ ] `dim_team` — from `silver.teams`
- [ ] `dim_venue` — from `silver.venues`
- [ ] `dim_competition` — from `silver.competitions`
- [ ] `dim_date` — generated date spine

### Facts ⬜
- [ ] `fact_delivery` — grain: one row per ball
- [ ] `fact_innings` — grain: one row per innings
- [ ] `fact_match_result` — grain: one row per match
- [ ] `fact_player_match` — grain: one row per (player, match)

---

## Big Task 8 — Gold Marts and Warehouse Validation ⬜

### Marts ⬜
- [ ] `mart_player_batting` — career and match-level batting stats
- [ ] `mart_player_bowling` — career and match-level bowling stats
- [ ] `mart_team_performance` — win/loss/NR by format, venue, season
- [ ] `mart_venue_dna` — scoring patterns by ground
- [ ] `mart_phase_scoring` — powerplay / middle / death run rates
- [ ] `mart_toss_outcome` — toss decision win correlation
- [ ] `mart_matchup_analysis` — batter vs bowler head-to-head

### dbt quality ⬜
- [ ] `not_null` and `unique` tests on all dimension PKs
- [ ] Relationship tests: facts → dims
- [ ] Accepted-values tests on categorical columns
- [ ] Source freshness checks

### Warehouse validation ⬜
- [ ] DuckDB can query Gold tables via MinIO/Iceberg extension
- [ ] Row counts Silver → Gold reconcile
- [ ] Sample match calculations validated against known results

---

## Execution order summary

| # | Big Task | Status | Output |
|---|----------|--------|--------|
| 1 | Foundation | ✅ Done | Runnable platform skeleton, 63 passing tests |
| 2 | Source contracts | ✅ Done | `source_contracts.md`, `architecture.md` |
| 3 | Register pipeline | 🔄 Bronze done, Silver ⬜ | Register landing → Bronze Iceberg |
| 4 | Match ingestion + Bronze | ⬜ | Archive download → Bronze match_documents |
| 5 | Match Silver | ⬜ | Conformed relational Silver layer |
| 6 | Silver DQ | ⬜ | Reconciliation + trust layer |
| 7 | Gold foundation | ⬜ | dbt dims + facts |
| 8 | Gold marts + validation | ⬜ | Queryable warehouse |
