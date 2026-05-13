# Task 2 — Source & Warehouse Contracts
# Cricket Intelligence Platform

> Document type: Engineering Contract
> Layer: Cross-cutting / Foundation
> Status: Baseline v1.0
> Last updated: 2026-05-10
> Owner: Platform Engineering (solo-operator)

---

## Part A — Source Contracts

---

### A.1 Source Inventory

| Source ID | Name | Type | Format | URL |
|---|---|---|---|---|
| SRC-001 | Cricsheet Full Archive | Bulk ZIP | JSON (primary) | `https://cricsheet.org/downloads/all_matches.zip` |
| SRC-002 | Cricsheet Register — People | Flat CSV | CSV | `https://cricsheet.org/register/people.csv` |
| SRC-003 | Cricsheet Register — Names | Flat CSV | CSV | `https://cricsheet.org/register/names.csv` |
| SRC-004 | Cricsheet T20 Archive | Competition ZIP | JSON | `https://cricsheet.org/downloads/t20s.zip` |
| SRC-005 | Cricsheet ODI Archive | Competition ZIP | JSON | `https://cricsheet.org/downloads/odis.zip` |
| SRC-006 | Cricsheet Test Archive | Competition ZIP | JSON | `https://cricsheet.org/downloads/tests.zip` |
| SRC-007 | Cricsheet IPL Archive | Competition ZIP | JSON | `https://cricsheet.org/downloads/ipl.zip` |
| SRC-008 | Cricsheet IT20 Archive | Competition ZIP | JSON | `https://cricsheet.org/downloads/it20s.zip` |

**Design note:** SRC-001 is the primary ingestion source for the full historical backfill.
SRC-004 through SRC-008 are used for competition-scoped incremental refreshes and
validation cross-checks. SRC-002 and SRC-003 are ingested independently as the
identity backbone — they must be ingested and validated before any match Silver transform runs.

---

### A.2 Archive Type Definitions

| Archive Type | Description | Scope | Filename Pattern |
|---|---|---|---|
| Full archive | All 21,600+ matches in a single ZIP | All formats, all genders, all time | `all_matches.zip` |
| Competition-specific ZIP | Subset by match type or competition name | One format or tournament | `ipl.zip`, `tests.zip`, `odis.zip` |
| Register CSV — People | One row per unique person; canonical identities and external source IDs | All persons ever in Cricsheet | `people.csv` |
| Register CSV — Names | One row per name alias; many rows per person | All known name variations | `names.csv` |
| YAML legacy | Older match format; deprecated by Cricsheet | Pre-2017 subset | `{match_id}.yaml` |

**Handling rule:** YAML files are ignored in v1. The Polars ingestion job will skip
any file whose extension is not `.json`. If YAML coverage becomes a requirement,
a separate YAML-to-JSON normalisation step will be introduced as a pre-Bronze stage.

---

### A.3 Match JSON Schema Contract (data_version 1.1.0)

**Top-level keys (always present):** `meta`, `info`, `innings`

#### A.3.1 `meta` block

| Field | Type | Nullable | Notes |
|---|---|---|---|
| `data_version` | string | No | `"1.0.0"` or `"1.1.0"` — schema version gate |
| `created` | date string (YYYY-MM-DD) | No | File creation date by Cricsheet |
| `revision` | integer | No | Increments on Cricsheet correction. Dedup key component |

**Dedup rule:** Bronze primary key = `(match_id, revision)`. A `revision=2` file is a
correction and must be written as a new Iceberg snapshot, not silently dropped.
Prior revision rows are retained in Bronze (append-only). Silver reads only the
latest revision per `match_id` via a `MAX(revision)` filter.

#### A.3.2 `info` block — field catalogue

| Field | Type | Nullable | Notes |
|---|---|---|---|
| `balls_per_over` | integer | No | Usually 6; historic Australian cricket used 8 |
| `city` | string | **Yes** | Absent in some older files; use `venue` as fallback |
| `dates` | list[date] | No | Multi-day (Test) has multiple entries; `dates[0]` = match start |
| `event.name` | string | No | Tournament / series name |
| `event.match_number` | integer | **Yes** | Absent on standalone matches |
| `event.group` | string | **Yes** | Group stage label (e.g. `"B"`) |
| `event.stage` | string | **Yes** | Knockout stage label (e.g. `"Final"`) |
| `gender` | string | No | `"male"` / `"female"` |
| `match_type` | string | No | `"Test"`, `"ODI"`, `"T20"`, `"IT20"`, `"ODM"` observed |
| `match_type_number` | integer | **Yes** | Global sequential number per format; absent on club matches |
| `missing[]` | array | **Yes** | Flags incomplete data (e.g. powerplay fielding info absent) |
| `officials.umpires` | list[string] | No | On-field umpires; always 2 |
| `officials.tv_umpires` | list[string] | **Yes** | |
| `officials.reserve_umpires` | list[string] | **Yes** | |
| `officials.match_referees` | list[string] | **Yes** | |
| `overs` | integer | **Yes** | Max scheduled overs; present on limited-overs formats; absent on Tests |
| `outcome.winner` | string | **Yes** | Absent on draws, ties, no-results |
| `outcome.result` | string | **Yes** | `"draw"`, `"tie"`, `"no result"`; mutually exclusive with `outcome.winner` |
| `outcome.by.runs` | integer | **Yes** | Win margin in runs |
| `outcome.by.wickets` | integer | **Yes** | Win margin in wickets |
| `outcome.by.innings` | integer | **Yes** | Test only; win by an innings |
| `outcome.method` | string | **Yes** | `"D/L"` when Duckworth-Lewis applied |
| `player_of_match` | list[string] | **Yes** | Absent on abandoned matches; can have 2 entries on ties |
| `players` | dict[team_name → list[string]] | No | Display names; always 2 teams |
| `registry.people` | dict[display_name → identifier] | No | Identity bridge; covers all players + officials in file |
| `season` | string OR integer | No | Three observed formats: `"2026"`, `"2011/12"`, integer `2007`. Normalise in Silver |
| `team_type` | string | No | `"international"` / `"club"` |
| `teams` | list[string] | No | Always exactly 2 entries |
| `toss.winner` | string | No | Team name |
| `toss.decision` | string | No | `"bat"` / `"field"` |
| `venue` | string | No | Full venue name |

#### A.3.3 `innings[]` block — field catalogue

| Field | Type | Nullable | Notes |
|---|---|---|---|
| `innings[n].team` | string | No | Batting team name |
| `innings[n].overs[].over` | integer | No | 0-indexed over number |
| `innings[n].powerplays[]` | array | **Yes** | Each entry: `from`, `to`, `type` (`mandatory`/`fielding`/`batting`) |
| `innings[n].target.runs` | integer | **Yes** | Second innings only; DLS-adjusted if `method=D/L` |
| `innings[n].target.overs` | integer | **Yes** | Second innings only |
| `innings[n].miscounted_overs` | dict | **Yes** | Rare; records overs with wrong ball counts; preserve in Bronze |
| **Delivery fields** | | | |
| `batter` | string | No | |
| `bowler` | string | No | |
| `non_striker` | string | No | |
| `runs.batter` | integer | No | |
| `runs.extras` | integer | No | |
| `runs.total` | integer | No | |
| `extras.wides` | integer | **Yes** | |
| `extras.noballs` | integer | **Yes** | |
| `extras.byes` | integer | **Yes** | |
| `extras.legbyes` | integer | **Yes** | |
| `extras.penalty` | integer | **Yes** | Rare; awarded penalty runs |
| `wickets[]` | array | **Yes** | Present only on dismissal deliveries |
| `wickets[n].player_out` | string | No | **Authoritative** dismissal subject; may differ from `batter` on run-outs |
| `wickets[n].kind` | string | No | `"caught"`, `"bowled"`, `"lbw"`, `"run out"`, `"stumped"`, `"hit wicket"`, `"retired hurt"`, `"absent hurt"`, `"obstructing the field"` |
| `wickets[n].fielders[]` | array | **Yes** | Each entry: `{name, substitute?}` |
| `replacements.role[]` | array | **Yes** | Concussion/injury substitute: `{in, out, reason, role}` |

---

### A.4 Register CSV Schema Contract

#### A.4.1 `people.csv`

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `identifier` | string (8-char hex) | No | **Primary key** — canonical person ID; join target from `registry.people` |
| `name` | string | No | Primary display name |
| `unique_name` | string | **Yes** | Disambiguated name; populated when `name` is ambiguous |
| `key_bcci` | string | **Yes** | BCCI ID |
| `key_cricinfo` | string | **Yes** | ESPNcricinfo player ID |
| `key_cricketarchive` | string | **Yes** | CricketArchive ID |
| `key_cricbuzz` | string | **Yes** | Cricbuzz ID |
| `key_nvplay` | string | **Yes** | NVPlay ID |
| `key_opta` | string | **Yes** | Opta ID |
| `key_pulse` | string | **Yes** | Pulse ID |
| `key_bigbash` | string | **Yes** | BBL ID |
| ... (10+ additional `key_*` columns) | string | **Yes** | Sparsely populated for domestic players |

**Normalisation rule:** `key_*` columns are **vertically unpivoted** in Silver to
`silver_person_identifiers(person_id, source_system, source_id)`.
Never store 20 sparse columns flat in Silver.

#### A.4.2 `names.csv`

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `identifier` | string (8-char hex) | No | FK to `people.csv.identifier`; repeats per alias |
| `name` | string | No | One name alias per row |

**Usage rule:** `names.csv` is the **fuzzy-match fallback** only. The primary
identity resolution path is `registry.people` (in each match JSON) → `people.csv`.
`names.csv` is consulted only when a display name cannot be resolved via `registry.people`.

---

### A.5 Source Refresh Assumptions

| Source | Expected Update Cadence | Update Type | Pipeline Action |
|---|---|---|---|
| `all_matches.zip` | ~Weekly (new matches added post-event) | Additive + occasional corrections | Differential download; checksum compare; reprocess only changed files |
| `people.csv` | ~Weekly (new players; identifier additions) | Additive + row-level updates | Full reload per run; schema drift check; row-count delta logged |
| `names.csv` | ~Weekly (new aliases) | Additive | Full reload per run; row-count delta logged |
| Competition ZIPs | ~Weekly per active competition | Additive | Same as full archive; used for targeted refresh |

**No SLA exists.** Cricsheet is a volunteer-maintained project. The pipeline must be
tolerant of zero-delta runs (no new files = valid state). All download failures are
retried 3× before raising a BLOCK-level DQ alert.

**Checksum strategy:** SHA-256 hash of each ZIP is stored in `control.source_download_log`
on every run. If the hash matches the prior run, file extraction is skipped.
Individual JSON files within a ZIP are hashed by `(match_id, revision)` — if neither
changes, Bronze write is skipped (idempotent).

---

## Part B — Warehouse Contracts

---

### B.1 Naming Standards

#### B.1.1 MinIO Bucket Layout

```
s3://cricket-platform/
├── landing/
│   ├── raw_zips/                    # Original downloaded ZIPs (never modified)
│   ├── extracted_json/              # Exploded individual match JSON files
│   └── register_csv/                # people.csv, names.csv as downloaded
│
├── warehouse/
│   ├── bronze/                      # Iceberg-managed Bronze tables
│   ├── silver/                      # Iceberg-managed Silver tables
│   └── gold/                        # dbt-materialized Gold tables (DuckDB reads Iceberg)
│
└── archive/
    ├── raw_zips/                    # Historical ZIP snapshots (date-partitioned)
    └── register_csv/                # Historical Register snapshots (date-partitioned)
```

**Archive rule:** Every downloaded ZIP is copied to `archive/raw_zips/YYYY/MM/DD/`
before extraction. Every Register CSV is copied to `archive/register_csv/YYYY/MM/DD/`.
This enables point-in-time reprocessing without re-downloading from Cricsheet.

#### B.1.2 Iceberg Namespace Layout

```
catalog: cricket
├── namespace: bronze
│   ├── bronze.match_documents
│   ├── bronze.register_people
│   ├── bronze.register_identifiers
│   └── bronze.register_name_variations
│
├── namespace: silver
│   ├── silver.matches
│   ├── silver.innings
│   ├── silver.deliveries
│   ├── silver.wickets
│   ├── silver.teams
│   ├── silver.venues
│   ├── silver.competitions
│   ├── silver.persons
│   ├── silver.person_identifiers
│   ├── silver.name_variations
│   ├── silver.match_players
│   └── silver.match_officials
│
└── namespace: gold
    ├── gold.dim_player
    ├── gold.dim_match
    ├── gold.dim_team
    ├── gold.dim_venue
    ├── gold.dim_competition
    ├── gold.dim_date
    ├── gold.fact_delivery
    ├── gold.fact_innings
    ├── gold.fact_match_result
    ├── gold.fact_player_match
    ├── gold.mart_player_batting
    ├── gold.mart_player_bowling
    ├── gold.mart_team_performance
    ├── gold.mart_venue_dna
    ├── gold.mart_phase_scoring
    ├── gold.mart_toss_outcome
    └── gold.mart_matchup_analysis
```

#### B.1.3 Table Naming Rules

| Layer | Pattern | Example |
|---|---|---|
| Bronze | `bronze.<entity>` | `bronze.match_documents` |
| Silver | `silver.<entity>` | `silver.deliveries` |
| Gold — Dim | `gold.dim_<entity>` | `gold.dim_player` |
| Gold — Fact | `gold.fact_<entity>` | `gold.fact_delivery` |
| Gold — Mart | `gold.mart_<topic>` | `gold.mart_phase_scoring` |
| Control | `control.<log_type>` | `control.dq_results` |

**Rules:**
- All names are `snake_case`, lowercase only.
- No abbreviations unless universally understood (`dq`, `dim`, `fact`, `mart`).
- No layer prefix in Silver column names (`match_id`, not `silver_match_id`).

#### B.1.4 Python Module Naming

```
platform/
├── ingestion/cricsheet/        # download + extract
├── ingestion/register/         # people.csv, names.csv
├── transform/polars/bronze/    # match JSON → bronze Iceberg (Polars)
├── transform/polars/silver/    # Register CSV → silver Iceberg (Polars)
├── transform/spark/silver/     # match bronze → silver exploded (PySpark)
├── quality/dq_landing.py
├── quality/dq_bronze.py
└── quality/dq_silver.py
```

---

### B.2 Metadata Columns (System Columns)

These columns are injected by the pipeline — never present in the source data.
They are present in **every Bronze and Silver Iceberg table**.

| Column Name | Type | Layer | Description |
|---|---|---|---|
| `_ingested_at` | TIMESTAMP WITH TIMEZONE | Bronze, Silver | UTC timestamp when this row was written |
| `_snapshot_date` | DATE | Bronze, Silver | Calendar date of the pipeline run (used for partitioning and auditing) |
| `_pipeline_run_id` | STRING | Bronze, Silver | Airflow `run_id` (e.g. `scheduled__2026-05-10T00:00:00+00:00`) |
| `_row_hash` | STRING (SHA-256, 64 hex chars) | Bronze, Silver | SHA-256 hash of all **source** columns (excludes system columns) |
| `_source_file` | STRING | Bronze only | Originating filename (e.g. `64023.json`) |
| `_data_version` | STRING | Bronze only | `meta.data_version` from source JSON |
| `_revision` | INTEGER | Bronze only | `meta.revision` from source JSON |

**`_row_hash` computation rule:**
- Hash input: concatenation of all non-system column values, sorted by column name, null-safe (nulls → empty string).
- Algorithm: SHA-256.
- Usage: change detection between runs; idempotency guard (skip write if hash already exists in Iceberg for same PK).

**`_pipeline_run_id` rule:**
- Populated from Airflow context: `context['run_id']`.
- In standalone/manual runs: `manual__{ISO8601_timestamp}`.

---

### B.3 Partition Strategy

| Table | Partition Key(s) | Strategy | Rationale |
|---|---|---|---|
| `bronze.match_documents` | `_snapshot_date` (month) | `MONTH(_snapshot_date)` | Groups ingestion batches; enables partition pruning on reruns |
| `bronze.register_persons` | `_snapshot_date` (month) | `MONTH(_snapshot_date)` | Register is small (~17k rows); monthly is sufficient |
| `bronze.register_identifiers` | `_snapshot_date` (month) | `MONTH(_snapshot_date)` | Same as persons |
| `bronze.register_name_variations` | `_snapshot_date` (month) | `MONTH(_snapshot_date)` | Same as persons |
| `silver.matches` | `match_type`, `season_year` | `IDENTITY(match_type)`, `IDENTITY(season_year)` | Common filter patterns in downstream queries |
| `silver.innings` | `match_type`, `season_year` | Same as silver.matches | Follows parent join key |
| `silver.deliveries` | `match_type`, `season_year` | Same as silver.matches | ~40-60M rows; partition pruning is critical |
| `silver.wickets` | `match_type`, `season_year` | Same as silver.deliveries | Subset of deliveries |
| `silver.persons` | _(none — ~17k rows)_ | No partition | Small table; full scan is fast |
| `silver.person_identifiers` | `source_system` | `IDENTITY(source_system)` | Lookup by source system is common |
| `silver.match_players` | `match_type`, `season_year` | Same as silver.matches | Follows parent |
| `silver.match_officials` | `match_type` | `IDENTITY(match_type)` | Small; format-level filtering is enough |
| `gold.*` | _(managed by dbt)_ | dbt `partition_by` config per model | Gold is DuckDB-served; partitioning is advisory |

**Iceberg partition evolution rule:** Partition specs can be changed without rewriting
data (Iceberg partition evolution). Document any partition spec change in an ADR before
applying. Never change a partition spec during an active pipeline run.

---

### B.4 Idempotency Rules

Idempotency means: running the same pipeline job twice produces the same final state —
no duplicate rows, no missing rows, no side effects.

#### B.4.1 Landing Layer

| Rule ID | Rule | Implementation |
|---|---|---|
| LND-IDEM-001 | Re-downloading the same ZIP must not overwrite the archive copy | Archive write is skipped if SHA-256 hash matches prior download in `control.source_download_log` |
| LND-IDEM-002 | Re-extracting a ZIP must not create duplicate JSON files in `landing/extracted_json/` | Extraction target path includes `{match_id}.json`; file is overwritten (last-write-wins) |
| LND-IDEM-003 | Register CSV re-download must overwrite the landing copy only | Landing CSV is always replaced; archive copy is date-partitioned and never overwritten |

#### B.4.2 Bronze Layer

| Rule ID | Rule | Implementation |
|---|---|---|
| BRZ-IDEM-001 | Re-ingesting a match file with same `(match_id, revision)` must produce no new rows | Before write: check `EXISTS (SELECT 1 FROM bronze.match_documents WHERE match_id = ? AND _revision = ?)`. Skip if found. |
| BRZ-IDEM-002 | Re-ingesting a match file with a new `revision` must append a new row (retain history) | `(match_id, revision)` is unique in Bronze; append is always safe for a new revision |
| BRZ-IDEM-003 | Register full reload on re-run must not duplicate rows | Bronze register tables use `overwrite_partition` (delete-partition-then-append) — full replace per snapshot date |
| BRZ-IDEM-004 | `_row_hash` guard | Even without explicit dedup logic, identical `_row_hash` + PK = no-write signal |

#### B.4.3 Silver Layer

| Rule ID | Rule | Implementation |
|---|---|---|
| SLV-IDEM-001 | Silver reads only the latest revision per `match_id` | PySpark job: `WHERE _revision = MAX(_revision) OVER (PARTITION BY match_id)` before any transformation |
| SLV-IDEM-002 | Silver write mode is `OVERWRITE` scoped to the partition being processed | Iceberg `overwritePartitions()` — only the partition being reprocessed is replaced; other partitions untouched |
| SLV-IDEM-003 | Silver `match_id` + entity PK must be unique | dbt `unique` test on Silver PKs; PySpark write fails if duplicates detected in output DataFrame before write |
| SLV-IDEM-004 | Register Silver re-run must be fully idempotent | `silver.persons` is written as `OVERWRITE` (full table replace); no partition dependency needed at 17k rows |

#### B.4.4 Gold Layer

| Rule ID | Rule | Implementation |
|---|---|---|
| GLD-IDEM-001 | dbt full-refresh is idempotent | `dbt run --full-refresh` drops and recreates; safe by design |
| GLD-IDEM-002 | dbt incremental models deduplicate on surrogate key | All incremental models use `unique_key` config; dbt generates `MERGE` not `INSERT` |
| GLD-IDEM-003 | Re-running Gold after a Silver correction must reflect updated data | Incremental models check `_ingested_at > last_gold_run_timestamp`; corrections trigger re-processing |

---

### B.5 Control Metadata Schema (PostgreSQL `control` schema)

The following tables are created once via `infra/bootstrap/init-metastore.sql`:

```sql
-- Tracks every source download attempt
CREATE TABLE control.source_download_log (
    id              SERIAL PRIMARY KEY,
    source_id       VARCHAR(20)   NOT NULL,   -- e.g. SRC-001
    source_url      TEXT          NOT NULL,
    download_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    file_size_bytes BIGINT,
    sha256_hash     CHAR(64),
    http_status     INTEGER,
    status          VARCHAR(20),               -- SUCCESS / FAILED / SKIPPED
    pipeline_run_id VARCHAR(250)
);

-- Tracks every Bronze ingestion batch
CREATE TABLE control.register_ingestion_log (
    id              SERIAL PRIMARY KEY,
    table_name      VARCHAR(100)  NOT NULL,
    pipeline_run_id VARCHAR(250)  NOT NULL,
    snapshot_date   DATE          NOT NULL,
    files_attempted INTEGER,
    files_written   INTEGER,
    files_skipped   INTEGER,
    rows_written    BIGINT,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    status          VARCHAR(20)
);

-- Tracks schema fingerprints for drift detection
CREATE TABLE control.register_schema_versions (
    id              SERIAL PRIMARY KEY,
    source_id       VARCHAR(20)   NOT NULL,
    snapshot_date   DATE          NOT NULL,
    column_count    INTEGER,
    column_names_hash CHAR(64),
    sampled_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- All DQ check results (every layer)
CREATE TABLE control.dq_results (
    id              SERIAL PRIMARY KEY,
    check_id        VARCHAR(20)   NOT NULL,   -- e.g. BRZ-001
    layer           VARCHAR(20)   NOT NULL,   -- LANDING / BRONZE / SILVER / GOLD
    table_name      VARCHAR(100),
    pipeline_run_id VARCHAR(250)  NOT NULL,
    severity        VARCHAR(10)   NOT NULL,   -- BLOCK / WARN / ALERT / LOG
    status          VARCHAR(10)   NOT NULL,   -- PASS / FAIL
    metric_value    NUMERIC,
    threshold       NUMERIC,
    message         TEXT,
    evaluated_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Register change delta tracking
CREATE TABLE control.register_change_log (
    id                  SERIAL PRIMARY KEY,
    snapshot_date       DATE          NOT NULL,
    pipeline_run_id     VARCHAR(250),
    people_row_count    INTEGER,
    names_row_count     INTEGER,
    people_delta_rows   INTEGER,
    names_delta_rows    INTEGER,
    schema_drift_flag   BOOLEAN       DEFAULT FALSE,
    logged_at           TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
```

---

### B.6 Data Contract YAML (machine-readable)

Stored at `platform/common/contracts/source_cricsheet_matches.yaml`:

```yaml
contract_id: SRC-MATCH-001
version: "1.0"
source: cricsheet_match_json
format: json
data_version_supported:
  - "1.0.0"
  - "1.1.0"
primary_key:
  - match_id
  - _revision
dedup_key:
  - match_id
  - _revision
nullable_fields:
  - city
  - event.match_number
  - event.group
  - event.stage
  - outcome.winner
  - outcome.result
  - outcome.by.runs
  - outcome.by.wickets
  - outcome.by.innings
  - outcome.method
  - overs
  - match_type_number
  - player_of_match
  - missing
season_normalisation: required          # string|int → silver standard
dq_checks:
  - check_id: BRZ-001
    description: match_id is not null
    severity: BLOCK
  - check_id: BRZ-002
    description: (match_id, revision) is unique within run
    severity: BLOCK
  - check_id: BRZ-003
    description: data_version in supported list
    severity: WARN
  - check_id: BRZ-004
    description: innings array is not empty
    severity: WARN
  - check_id: BRZ-005
    description: registry.people covers all players in players{}
    severity: WARN
refresh_cadence: weekly
archive_policy: retain_all_revisions
```

---

## Part C — Known Edge Cases

| ID | Edge Case | Layer | Handling Rule |
|---|---|---|---|
| EC-001 | `revision > 1` — corrected match file | Bronze | Append new row; Silver reads MAX(revision) only |
| EC-002 | `season` as integer (e.g. `2007`) | Silver | Cast to string `"2007"` in Polars reader before Bronze write |
| EC-003 | `season` as `"2002/03"` split-year format | Silver | Extract `season_year` = `2002` (start year); retain `season_label` = `"2002/03"` |
| EC-004 | `city` is absent | Silver | `silver.matches.city = NULL`; `dim_venue` uses `venue` string as PK |
| EC-005 | `outcome.winner` absent (draw/tie/no result) | Silver | `outcome_winner = NULL`; `outcome_result` populated |
| EC-006 | `player_of_match` has 2 entries (tied match) | Silver | Explode to 2 rows in `silver_player_of_match` bridge table |
| EC-007 | `wickets[n].player_out` ≠ `batter` (run out) | Silver | `silver.wickets.player_out` always from `wickets[].player_out`, never from `batter` |
| EC-008 | `miscounted_overs` present | Bronze | Preserve as raw JSON column; DQ check BRZ-007 flags it as WARN |
| EC-009 | `missing[]` array present | Silver | Set `has_missing_data = TRUE` in `silver.matches`; log which fields |
| EC-010 | `replacements.role[]` (injury sub) | Silver | Map to `silver.deliveries.replacement_in`, `replacement_out`, `replacement_reason` |
| EC-011 | `data_version = "1.0.0"` (older schema) | Bronze | Write as-is; Silver transform handles version-gated field presence |
| EC-012 | `balls_per_over = 8` (historic Australia) | Silver | Preserve in `silver.matches.balls_per_over`; affects economy/SR calculations in Gold |
| EC-013 | `event.group` present (group-stage match) | Silver | Map to `silver.competitions.group_label` |
| EC-014 | `fielders[n].substitute = true` | Silver | Map to `silver.wickets.fielder_is_substitute` boolean |

---

_End of Task 2 Source & Warehouse Contracts_
_Next: Task 3 — Bronze Ingestion Pipeline (Polars + Iceberg)_
