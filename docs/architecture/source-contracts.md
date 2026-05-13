# Source Contracts — Cricket Intelligence Platform

> Authoritative reference for all Cricsheet source files.
> Verified against live files in `support_docs/source_files/` on 2026-05-11.

---

## Source inventory

| ID | File / Archive | URL | Format | Refresh | Bronze table |
|----|----------------|-----|--------|---------|--------------|
| SRC-001 | `all_matches.zip` | cricsheet.org/downloads/ | ZIP → JSON | Weekly | `cricket.bronze.match_documents` |
| SRC-002 | `people.csv` | cricsheet.org/register/people.csv | CSV | Weekly | `cricket.bronze.register_people` + `cricket.bronze.register_identifiers` |
| SRC-003 | `names.csv` | cricsheet.org/register/names.csv | CSV | Weekly | `cricket.bronze.register_name_variations` |

---

## SRC-002: `people.csv` — Player Identity Register

### Schema (verified 2026-05-11)

| Column | Type (Bronze) | Description |
|--------|--------------|-------------|
| `identifier` | Utf8 | Cricsheet 8-char hex UUID — **primary key across all match data**. Stable — never changes for a person. |
| `name` | Utf8 | Canonical display name (e.g. `Virat Kohli`) |
| `unique_name` | Utf8 | Same as `name` in current format; was distinct in older releases |
| `key_bcci` | Utf8 | BCCI registry ID (sparse) |
| `key_bcci_2` | Utf8 | BCCI alternate ID (sparse) |
| `key_bigbash` | Utf8 | BigBash League ID (sparse) |
| `key_cricbuzz` | Utf8 | Cricbuzz player ID (sparse) |
| `key_cricheroes` | Utf8 | CricHeroes ID (sparse) |
| `key_crichq` | Utf8 | CricHQ ID (sparse) |
| `key_cricinfo` | Utf8 | ESPNcricinfo player ID — most densely populated key column |
| `key_cricinfo_2` | Utf8 | Alternate cricinfo ID (sparse) |
| `key_cricinfo_3` | Utf8 | Alternate cricinfo ID (sparse) |
| `key_cricingif` | Utf8 | CricingIF ID (sparse) |
| `key_cricketarchive` | Utf8 | CricketArchive ID (sparse) |
| `key_cricketarchive_2` | Utf8 | CricketArchive alternate ID (sparse) |
| `key_cricketworld` | Utf8 | CricketWorld ID (sparse) |
| `key_nvplay` | Utf8 | NVPlay ID (sparse) |
| `key_nvplay_2` | Utf8 | NVPlay alternate ID (sparse) |
| `key_opta` | Utf8 | Opta ID (sparse) |
| `key_opta_2` | Utf8 | Opta alternate ID (sparse) |
| `key_pulse` | Utf8 | BCCI Pulse ID (sparse) |
| `key_pulse_2` | Utf8 | BCCI Pulse alternate ID (sparse) |

**Total columns (2026-05-11):** 22

### Critical notes

- `gender` and `dob` columns existed in older Cricsheet formats but are **not present** in the current register. Do not add them to the Bronze schema without confirming they have returned.
- `key_*` columns are extremely sparse — the majority of cells are empty. Empty string → null via `null_values=[""]` in Polars.
- New `key_*` columns are added by Cricsheet when new external registries are integrated. The pipeline auto-detects them — no code change needed.
- `identifier` values appear verbatim in match JSON `info.registry.people` dicts, linking match players to this register.

### Bronze split

People.csv feeds two Bronze tables:

**`cricket.bronze.register_people`** — one row per person
```
identifier | name | unique_name | _snapshot_date | _ingested_at | _pipeline_run_id | _row_hash | _source_file | _source_url
```

**`cricket.bronze.register_identifiers`** — unpivoted key_* columns, one row per (identifier, external registry)
```
identifier | key_source | key_value | _snapshot_date | _ingested_at | _pipeline_run_id | _row_hash | _source_file | _source_url
```
- `key_source` = column name with `key_` prefix stripped (e.g. `"cricinfo"`)
- `key_value` = the external ID string
- Rows where `key_value IS NULL` are dropped
- Schema drift (new `key_*` columns) flows automatically — no code change needed

---

## SRC-003: `names.csv` — Alternative Name Register

### Schema (verified 2026-05-11)

| Column | Type (Bronze) | Description |
|--------|--------------|-------------|
| `identifier` | Utf8 | Cricsheet UUID — foreign key to `people.csv.identifier` |
| `name` | Utf8 | Alternate name (alias, transliteration, shortened form) |

**Total columns:** 2

### Behaviour
- One person can have zero, one, or many rows — the file is purely additive
- Used for: aliases (`Allan Donald`), shortened forms (`A Donald`), transliterations
- Deduplication applied on `(identifier, name)` within each snapshot before Bronze write
- Rows with null `identifier` or null `name` are dropped at Bronze parse time

### Bronze table

**`cricket.bronze.register_name_variations`** — one row per (identifier, alternate name)
```
identifier | name | _snapshot_date | _ingested_at | _pipeline_run_id | _row_hash | _source_file | _source_url
```

---

## SRC-001: Match JSON files

### Archive types

| Archive | Contents | Scope |
|---------|----------|-------|
| `all_matches.zip` | All formats, all eras | Primary backfill source |
| `tests.zip` | Test matches only | Format-specific alternative |
| `odis.zip` | ODI matches only | Format-specific alternative |
| `t20s.zip` | T20I matches only | Format-specific alternative |
| `wi_*.zip` | Women's Internationals | Gender-specific |

> Backfill strategy: download `all_matches.zip` first. Subsequent weekly runs can use the smaller weekly delta archive.

### File naming

- Filename = `<match_id>.json` (e.g. `258464.json`)
- `match_id` = Cricsheet's integer match ID — stable, unique across all formats
- Some Women's International files use prefix `wi_` (e.g. `wi_212062.json`)
- **YAML files are also present in archives — intentionally skipped**. Only `.json` files are processed.

### Top-level document structure

```json
{
  "meta": { ... },
  "info": { ... },
  "innings": [ ... ]
}
```

---

### `meta` block

| Field | Type | Notes |
|-------|------|-------|
| `data_version` | string | `"1.0.0"` or `"1.1.0"` — schema version |
| `created` | string | ISO date when Cricsheet created this file (e.g. `"2013-02-18"`) |
| `revision` | integer | Starts at 1. Corrections increment this. Bronze dedup key is `(match_id, revision)`. Silver reads only `MAX(revision)` per `match_id`. |

---

### `info` block — field catalogue

| Field | Type | Notes |
|-------|------|-------|
| `balls_per_over` | integer | Usually 6. Rare historical matches used 8. |
| `city` | string | Nullable. Absent for some matches. |
| `dates` | string[] | List of ISO date strings. Single-day matches have 1. Test matches have up to 5. |
| `event.name` | string | Competition name (e.g. `"IPL"`, `"ICC World Cup"`). |
| `event.match_number` | integer | Match number within the competition. Present on most but not all matches. |
| `event.group` | string | Group stage identifier (e.g. `"B"`). Present on group-stage matches only. |
| `event.stage` | string | Stage name (e.g. `"Final"`, `"Semi Final"`). Present on knockout matches. |
| `gender` | string | `"male"` or `"female"` |
| `match_type` | string | `"Test"`, `"ODI"`, `"T20"`, `"IT20"`, `"MDM"`, `"OTH"` |
| `match_type_number` | integer | Match number of this type (e.g. T20 #16 ever played). |
| `officials.umpires` | string[] | On-field umpires |
| `officials.tv_umpires` | string[] | TV umpires. Nullable. |
| `officials.reserve_umpires` | string[] | Reserve umpires. Nullable. |
| `officials.match_referees` | string[] | Match referees. Nullable. |
| `outcome.winner` | string | Winning team name. Absent on no-result/tie. |
| `outcome.by.runs` | integer | Win margin in runs. Present when winner won by runs. |
| `outcome.by.wickets` | integer | Win margin in wickets. Present when winner won by wickets. |
| `outcome.by.innings` | integer | Win by innings (Test only). |
| `outcome.result` | string | `"no result"`, `"tie"`, `"draw"`. Present when no winner. Mutually exclusive with `outcome.winner`. |
| `overs` | integer | Scheduled overs per innings (e.g. 20 for T20). |
| `player_of_match` | string[] | Player(s) of the match. May be empty or absent. |
| `players` | object | Dict of `team_name → [player_name, ...]`. Player names are display names. |
| `registry.people` | object | Dict of `player_name → cricsheet_identifier`. Links every person named in the match to the Register. **Authoritative for identity resolution.** |
| `season` | string \| integer | **Polymorphic.** Can be string `"2011/12"`, string `"2026"`, or integer `2007`. Normalise in Silver. |
| `team_type` | string | `"international"` or `"club"` |
| `teams` | string[] | The two competing teams |
| `toss.winner` | string | Team that won the toss |
| `toss.decision` | string | `"bat"` or `"field"` |
| `venue` | string | Ground name (e.g. `"Lord's"`, `"Eden Gardens"`) |

---

### `innings[]` block — field catalogue

Each innings object:

| Field | Type | Notes |
|-------|------|-------|
| `team` | string | Batting team name (matches `info.teams` and `info.players`) |
| `overs[]` | array | List of over objects |

Each over object:

| Field | Type | Notes |
|-------|------|-------|
| `over` | integer | 0-indexed over number (over 0 = first over) |
| `deliveries[]` | array | List of delivery objects |

Each delivery object:

| Field | Type | Notes |
|-------|------|-------|
| `batter` | string | Batter facing the delivery |
| `bowler` | string | Bowler delivering |
| `non_striker` | string | Non-striking batter |
| `runs.batter` | integer | Runs scored by the batter (excludes extras) |
| `runs.extras` | integer | Extras on this delivery |
| `runs.total` | integer | Total runs (batter + extras) |
| `extras` | object | Optional. Keys: `wides`, `noballs`, `byes`, `legbyes`, `penalty`. Each is the run value. |
| `wickets[]` | array | Optional. Present only on dismissal deliveries. One entry per wicket (run-outs can dismiss two). |

Each wicket object:

| Field | Type | Notes |
|-------|------|-------|
| `player_out` | string | **The dismissed batter — use this, not the delivery's `batter` field.** They differ on run-outs. |
| `kind` | string | Dismissal type. Values: `bowled`, `caught`, `caught and bowled`, `lbw`, `run out`, `stumped`, `hit wicket`, `handled the ball`, `obstructing the field`, `timed out`, `hit the ball twice`, `retired hurt`, `retired out` |
| `fielders[]` | array | Optional. List of `{name: string}` objects. Present on caught, stumped, run-out. |

---

## Metadata columns (all Bronze + Silver + Gold tables)

| Column | Type | Injected by | Description |
|--------|------|-------------|-------------|
| `_snapshot_date` | Date | `PolarsIcebergWriter` / `SparkIcebergWriter` | Logical processing date — partition key |
| `_ingested_at` | Timestamptz | same | UTC wall-clock time of this load |
| `_pipeline_run_id` | Utf8 | same | Airflow `run_id` or manual UUID |
| `_dag_run_id` | Utf8 | same | Airflow `dag_run_id` (empty on manual runs) |
| `_source_file` | Utf8 | same | Originating filename (e.g. `people.csv`, `258464.json`) |
| `_source_url` | Utf8 | same | Download URL |
| `_row_hash` | Utf8 | `RegisterNormalizer._attach_metadata()` | SHA-256 of all business-key columns — row-level dedup key |

Silver/Gold additionally carry:

| Column | Type | Description |
|--------|------|-------------|
| `_is_current` | Boolean | SCD2 flag |
| `_valid_from` | Timestamptz | SCD2 effective start |
| `_valid_to` | Timestamptz | SCD2 effective end (NULL = current) |
| `_bronze_loaded_at` | Timestamptz | When row entered Bronze |
| `_silver_loaded_at` | Timestamptz | When row entered Silver |

---

## Partition strategy

| Layer | Table | Partition columns |
|-------|-------|------------------|
| Bronze | All tables | `_snapshot_date` (IdentityTransform) |
| Silver | `matches`, `deliveries`, etc. | `_snapshot_date`, `match_type` |
| Gold | Facts | `season`, `match_type` |

---

## Idempotency rules

| Stage | Guard table | Guard columns | Bypass |
|-------|------------|---------------|--------|
| Register download | `control.register_ingestion_log` | `(source_file, snapshot_date, status='SUCCESS')` | `force=True` |
| Register Bronze | Partition delete + append | `_snapshot_date` partition | `force=True` → `overwrite_snapshot()` |
| Match download | `control.archive_download_log` | `(archive_file, snapshot_date, status='SUCCESS')` | `force=True` |
| Match Bronze | `control.bronze_match_ingestion_log` | `(archive_file, snapshot_date, status='SUCCESS')` | `force=True` |

---

## Known edge cases and handling

| # | Edge case | Location | Handling |
|---|-----------|----------|---------|
| 1 | `season` is string `"2011/12"` | `info.season` | Normalise to string in Silver; never cast to int |
| 2 | `season` is integer `2007` | `info.season` | Cast to string in Silver |
| 3 | `wickets[].player_out` ≠ `batter` | Any dismissal | Use `player_out` as authoritative dismissed batter |
| 4 | `wickets[]` has 2 entries | Run-out deliveries | Both dismissals must be recorded; do not assume 1 wicket = 1 entry |
| 5 | No `event` block | Friendly/unofficial matches | Treat as nullable; do not fail parse |
| 6 | No `outcome.by` block | No-result, tie, draw | `outcome.result` is present instead |
| 7 | No `outcome.winner` | No-result, tie, draw | Derive from `outcome.result` |
| 8 | YAML files in archive | Every archive ZIP | Skip — only `.json` files are processed |
| 9 | `key_*` columns are empty string | people.csv | `null_values=[""]` in Polars read — empty → null |
| 10 | New `key_*` column added by Cricsheet | people.csv | Auto-detected by `RegisterParser` — flows to Bronze without code change |
| 11 | `gender` / `dob` absent from people.csv | Register | Not present in current format — do not add to `_PEOPLE_CORE_COLS` without confirming |
| 12 | Match `registry.people` is the ID bridge | info.registry | Use this dict to resolve name → identifier; do not fuzzy-match against people.csv directly |
| 13 | `match_type_number` absent | Some older matches | Nullable; do not fail if missing |
| 14 | Multiple `dates` (Test matches) | info.dates | First date = match start date for partitioning |
