<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# Hi create a planning step by step development. I will be focusing on building till Source to Data Warehouse. I want to break the tasks to very small tasks.

Create a planning with bigger task and inside small small subtask.

Yes — the right scope is to stop at **Source → Landing/Bronze → Silver → Gold/Data Warehouse** and postpone BI, AI, and MLOps until the warehouse is stable. Your existing direction already supports this: a modular monolith repo, medallion architecture, Polars for ingestion, PySpark for heavier normalization, and dbt + DuckDB for Gold marts.[^1][^2]

## Planning rules

Build this in **vertical slices**, not as isolated tools, because the Register identity model affects Silver joins and Gold dimensions later.[^1][^2]
For your platform, the cleanest order is: foundation, source ingestion, Register pipeline, match Bronze, Silver normalization, quality/reconciliation, then Gold marts.[^2][^3][^1]

## 1) Foundation setup

### Big task 1 — Project and environment foundation

This phase exists to make the repo runnable, reproducible, and aligned with the architecture you already defined in the HLD.[^3][^2]

- Create and verify repo essentials:
    - Finalize `.env.example`.
    - Finalize `pyproject.toml` or `requirements.txt`.
    - Add `Makefile` commands: `make up`, `make down`, `make bootstrap`, `make test`, `make lint`.
    - Add `.pre-commit-config.yaml`.
    - Add `ruff`, `pytest`, and formatting setup.[^2][^3]
- Prepare local infrastructure:
    - Write `infra/compose/compose.base.yml`.
    - Write `infra/compose/compose.dev.yml`.
    - Start MinIO, PostgreSQL, Airflow, and Iceberg catalog locally.
    - Verify ports and health checks.[^2]
- Prepare bootstrap assets:
    - Write `infra/bootstrap/create-buckets.sh`.
    - Write `infra/bootstrap/init-metastore.sql`.
    - Create buckets/prefixes for landing, bronze, silver, and gold.
    - Create control schema tables for ingestion log, DQ results, and schema versions.[^3][^2]
- Prepare shared platform modules:
    - `platform/common/settings.py`
    - `platform/common/logging.py`
    - `platform/common/exceptions.py`
    - `platform/common/contracts/enums.py`
    - `platform/common/contracts/naming.py`
    - `platform/ingestion/io/minio.py`
    - `platform/transform/shared/readers.py`
    - `platform/transform/shared/writers.py`[^2]


### Big task 2 — Source understanding and contracts

Because Cricsheet has both match archives and the Register, you should document source behavior before building pipelines.[^1][^3][^2]

- Document source contracts:
    - List all source URLs you will use.
    - Document archive types: full archive, competition-specific zips, Register CSVs.
    - Document expected file formats: JSON primary, YAML legacy, CSV for Register.
    - Record refresh/update assumptions.[^1][^3][^2]
- Define warehouse contracts:
    - Define naming standards for landing, bronze, silver, and gold.
    - Define metadata columns such as `_snapshot_date`, `_ingested_at`, `_pipeline_run_id`, `_row_hash`.
    - Define partition strategy for each layer.
    - Define idempotency rules for reruns.[^2]


## 2) Source and Bronze

### Big task 3 — Register pipeline first

The Register is the identity backbone, so it should be your first complete production slice before match parsing.[^1][^2]

- Build landing ingestion for Register:
    - Download `people.csv`.
    - Download `names.csv`.
    - Calculate checksum.
    - Store files under date-partitioned landing paths.
    - Log source URL, row count, file size, checksum, and snapshot date in PostgreSQL.[^2]
- Build Register Bronze load:
    - Read CSVs with Polars using all-string ingestion for source fidelity.
    - Add ingestion metadata columns.
    - Write append-only Iceberg Bronze tables.
    - Add idempotency check so same snapshot is not loaded twice.[^2]
- Build Register Silver models:
    - Create `silver_persons`.
    - Create `silver_person_identifiers` by unpivoting `key_*` columns.
    - Create `silver_name_variations`.
    - Keep schema drift handling for new `key_*` columns.[^2]
- Add Register DQ:
    - Null and duplicate identifier checks.
    - Row count threshold checks.
    - Cross-file orphan checks between `people.csv` and `names.csv`.
    - Persist all DQ results in control tables.[^3][^2]


### Big task 4 — Match source ingestion and Bronze

After the Register is stable, start the historical match ingestion path from downloads into landing and Bronze.[^3][^1][^2]

- Build match downloader:
    - Download full archive zip first.
    - Store raw zip in landing.
    - Extract JSON files to landing extracted path.
    - Capture file inventory and extraction metadata.[^3][^2]
- Build ingestion metadata:
    - Store one run record per archive download.
    - Store one extracted-file record per match file.
    - Track checksum, file count, extracted path, and run status.
    - Add rerun logic to skip already-processed files.[^3][^2]
- Build Bronze match documents:
    - Read extracted JSON files with Polars.
    - Preserve raw document or structured raw payload.
    - Flatten only top-level metadata needed for discoverability.
    - Write `bronze_match_documents` as append-only Iceberg.[^1][^2]
- Add Bronze DQ:
    - Validate file parseability.
    - Validate expected archive structure.
    - Validate duplicate match IDs.
    - Validate Bronze row count against extracted file inventory.[^3][^2]


## 3) Silver normalization

### Big task 5 — Match Silver core entities

This is the heavy normalization phase where nested JSON becomes trusted relational tables, and this is where PySpark adds the most value.[^1][^2]

- Prepare Spark execution:
    - Create Spark image/config.
    - Configure Iceberg catalog access.
    - Configure MinIO/S3 access.
    - Verify Spark can read Bronze and write Silver.[^2]
- Build core Silver entity tables:
    - `silver_matches`
    - `silver_innings`
    - `silver_deliveries`
    - `silver_wickets`
    - `silver_teams`
    - `silver_venues`
    - `silver_competitions`[^2]
- Build match-participant tables:
    - `silver_match_players`
    - `silver_match_officials`
    - Optional match registry mapping table for raw file registry references.[^2]
- Implement identity resolution:
    - Extract person names from match registry blocks.
    - Join to `silver_persons` and `silver_name_variations`.
    - Resolve canonical `person_id` where possible.
    - Write unmatched-person audit table for failures.[^1][^2]


### Big task 6 — Silver DQ and reconciliation

This is what makes the warehouse trustworthy and interview-grade, especially for cricket score integrity.[^1][^3][^2]

- Add structural checks:
    - Null PK checks on all Silver tables.
    - Duplicate grain checks.
    - Accepted-value checks for match result fields.
    - Referential integrity checks across tables.[^2]
- Add cricket-specific reconciliation:
    - Validate innings totals against delivery totals.
    - Validate wicket counts.
    - Validate over and ball sequencing logic.
    - Validate winner and margin consistency where applicable.[^3][^2]
- Add identity-quality checks:
    - Measure unmatched person rate.
    - Measure registry coverage.
    - Flag names present in matches but absent in Register.
    - Persist run-level DQ summary.[^1][^3][^2]


## 4) Data warehouse

### Big task 7 — Gold warehouse foundation

Gold is the governed analytical contract, so treat it as the product surface for downstream analytics rather than just another transform step.[^2]

- Set up dbt project:
    - Finalize `models/dbt/dbt_project.yml`.
    - Configure `profiles.yml`.
    - Define sources over Silver tables.
    - Create folders for staging, intermediate, marts, tests, and docs.[^3][^2]
- Build staging layer:
    - Create `stg_matches`.
    - Create `stg_deliveries`.
    - Create `stg_innings`.
    - Create `stg_players`.
    - Create `stg_venues`.
    - Create `stg_competitions`.[^3][^2]
- Build dimensions:
    - `dim_player`
    - `dim_match`
    - `dim_team`
    - `dim_venue`
    - `dim_competition`
    - `dim_date`[^2]
- Build facts:
    - `fact_delivery`
    - `fact_innings`
    - `fact_match_result`
    - `fact_player_match`[^2]


### Big task 8 — First Gold marts and warehouse validation

Your first warehouse deliverable should be a small but strong set of marts that prove the full path from source to trusted analytics.[^1][^3][^2]

- Build first marts:
    - `mart_team_performance`
    - `mart_player_batting`
    - `mart_player_bowling`
    - `mart_venue_dna`
    - `mart_phase_scoring`
    - `mart_toss_outcome`
    - `mart_matchup_analysis`[^3][^2]
- Add dbt quality layer:
    - Source freshness where possible.
    - `not_null` tests.
    - `unique` tests.
    - Relationship tests.
    - Accepted-values tests.[^3][^2]
- Validate warehouse readiness:
    - Query response from DuckDB.
    - Validate row counts from Silver to Gold.
    - Validate business calculations on sample matches.
    - Document grain, keys, and refresh logic for every Gold model.[^2]


## Suggested execution order

Use this exact order so you always have a working slice before moving on.[^1][^3][^2]


| Order | Big task | Output |
| :-- | :-- | :-- |
| 1 | Foundation setup | Running local platform skeleton [^2] |
| 2 | Source contracts | Documented inputs and naming rules [^3][^2] |
| 3 | Register pipeline | First end-to-end landing → Bronze → Silver slice [^1][^2] |
| 4 | Match landing/Bronze | Raw archive ingestion and Bronze documents [^3][^2] |
| 5 | Match Silver | Trusted conformed relational layer [^2] |
| 6 | Silver DQ | Reconciliation and trust layer [^3][^2] |
| 7 | Gold warehouse | Dimensions, facts, marts via dbt [^3][^2] |
| 8 | Warehouse hardening | dbt tests, docs, refresh validation [^3][^2] |

## First 10 tasks to start tomorrow

These are the smallest practical starting tasks for your repo and current scope.[^3][^2]

1. Fill `.env.example` with MinIO, PostgreSQL, Airflow, and Iceberg variables.[^2]
2. Write `infra/compose/compose.base.yml`.[^2]
3. Write `infra/bootstrap/create-buckets.sh`.[^2]
4. Write `infra/bootstrap/init-metastore.sql`.[^2]
5. Write `platform/common/settings.py`.[^2]
6. Write `platform/ingestion/io/minio.py`.[^2]
7. Write `platform/ingestion/register/download.py`.[^2]
8. Write `platform/ingestion/register/parse.py`.[^2]
9. Write `platform/ingestion/register/normalize.py`.[^2]
10. Write `orchestration/airflow/dags/dag_ingest_cricsheet_register.py` skeleton with empty tasks.[^2]

Would you like me to turn this into a **12-week execution plan** with weekly milestones and daily deliverables?

<div align="center">⁂</div>

[^1]: Development-Phases-Perplexity.pdf

[^2]: HLD-or-HLA-Perplexity.pdf

[^3]: Project-Planning-Perplexity.pdf

