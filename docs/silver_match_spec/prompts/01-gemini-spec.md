# Stage A — Spec design prompt (Gemini CLI)

Paste the block below into a `gemini` session opened at the project root.

---

```
@CLAUDE.md
@src/cip/common/contracts/naming.py
@src/cip/common/contracts/enums.py
@src/cip/transform/spark/silver/persons.py
@src/cip/transform/polars/silver/persons.py
@src/cip/transform/polars/bronze/match_documents.py
@src/cip/transform/shared/writers.py
@src/cip/transform/shared/readers.py
@src/cip/quality/checks/match_bronze_dq.py
@src/cip/quality/checks/register_dq.py
@orchestration/airflow/dags/dag_ingest_all_match_data.py
@orchestration/airflow/dags/dag_ingest_people_and_names.py

You are helping design Big Task 5 — the PySpark Silver pipeline for Cricsheet match data.

## Source

Bronze table: `cricket.bronze.match_documents`
Columns:
- match_id (Utf8), revision (Utf8 - "1", "2", ...)
- match_type, gender, season, match_date, team_a, team_b, venue, city (header projection cols, Utf8)
- raw_json (Utf8) — full Cricsheet match JSON document
- _snapshot_date (Utf8) + metadata columns

Silver reads only MAX(revision) per match_id across all snapshots.

## Deliverable

Produce a complete markdown spec at `docs/silver_match_spec/spec.md` with the following exact sections.

### Section 1 — PySpark StructType

A complete `MATCH_JSON_SCHEMA: StructType` definition that covers every field
in the Cricsheet match JSON, including:

- `meta`: data_version, created, revision
- `info`: balls_per_over, city, dates[], event{name, match_number, group, stage, sub_name}, gender, match_type, match_type_number, missing[], officials{match_referees[], reserve_umpires[], tv_umpires[], umpires[]}, outcome{by{innings, runs, wickets}, winner, result, method, eliminator, bowl_out}, overs, player_of_match[], players{<team>: [...]}, registry{people: {name: id}}, season (handle int/string/slash), supersubs, teams[], team_type, toss{decision, winner, uncontested}, venue
- `innings`: list of {team, overs[{over (int), deliveries[{batter, bowler, non_striker, runs{batter, extras, total, non_boundary}, extras{wides, noballs, byes, legbyes, penalty}, wickets[{player_out, kind, fielders[{name, substitute}]}], replacements{role[], match[]}}]}, absent_hurt[], penalty_runs{pre, post}, declared, forfeited, target{overs, runs}, super_over (bool), powerplays[{from, to, type}], miscounted_overs}

Return as actual Python code I can paste into `_shared.py`.

### Section 2 — Silver table schemas (12 tables)

For each of these 12 tables, provide:
- Table name (use `TableName.silver("<name>")` form)
- Grain (one row per …)
- Full column list: name, Spark type, nullable, source JSON path, business meaning
- Primary key columns
- Foreign keys
- Whether SCD2 columns apply (`_is_current`, `_valid_from`, `_valid_to`)
- Partition columns

Tables to spec:
1. `silver.matches` — one row per match (use MAX revision)
2. `silver.innings` — one row per innings (1-4 per match, super-over creates extra rows)
3. `silver.deliveries` — one row per ball bowled
4. `silver.wickets` — one row per wicket fallen
5. `silver.teams` — one row per team (SCD2 dim-like)
6. `silver.venues` — one row per venue (SCD2 dim-like)
7. `silver.competitions` — one row per competition (SCD2 dim-like)
8. `silver.match_players` — one row per (match_id, team, player_name)
9. `silver.match_officials` — one row per (match_id, role, person_name)
10. `silver.match_registry` — one row per (match_id, display_name, cricsheet_id) from info.registry.people
11. `silver.unmatched_persons_audit` — failed identity resolution rows
12. (any 12th table you recommend — e.g. `silver.match_powerplays`, justify)

### Section 3 — Edge cases per table

Be very specific. At minimum cover:
- `season` polymorphism: int 2007, string "2026", string "2011/12"
- `wickets[].player_out` is dismissal subject (not `batter`) — they differ on run-outs
- Super-overs: same match_id, extra innings rows with super_over=true
- Extras counting: wides/no-balls add extra deliveries; byes/legbyes/penalty don't add deliveries
- Missing `info.event` — friendlies and unofficial matches
- Missing `outcome.by` — ties and no-results
- Tie / no-result handling in `outcome.result`
- Replacement players (`replacements.role[]`, `replacements.match[]`)
- Substitute fielders (`wickets[].fielders[].substitute`)
- Forfeited / declared innings
- Match registry may map display_name → null cricsheet_id (cricsheet doesn't know who)
- Match dates list with multiple entries (Tests, multi-day matches)
- `team_type` field (international, club, etc.) — affects competition derivation
- Powerplay arrays (`powerplays[].type` = mandatory/batting/bowling)
- `balls_per_over` ≠ 6 in some formats (T10, The Hundred uses 5)

### Section 4 — Identity resolution algorithm

Pseudocode for resolving player/official names to `silver.persons.person_id`:

1. Join match_players + match_officials → match_registry on (match_id, display_name)
2. If match_registry has cricsheet_id: lookup person_id from silver.persons via cricsheet_id
3. If no cricsheet_id OR person not in register: fall back to silver.name_variations exact match
4. If still no match: write row to silver.unmatched_persons_audit with reason
5. NEVER drop rows from match_players or match_officials — populate person_id with NULL on failure

Reference `src/cip/transform/polars/silver/persons.py` for how silver.persons and
silver.name_variations are structured.

### Section 5 — Dependency order

List the 12 Silver tables in build order, with reasoning. Reads should land first
(matches, lookups), then dependent tables (innings, deliveries), then resolution.

### Section 6 — Test scenarios per table

For each of the 12 tables, list 3-6 test scenarios (happy path + each edge case
from section 3 that applies). Format as bullet list, one per test.

### Section 7 — Open questions

Anything ambiguous in the source data or contracts that you'd want to confirm
before implementation begins.

---

Be exhaustive. This spec drives ~2 weeks of implementation across 15 source files
and ~100 unit tests. Save me iteration time later.
```
