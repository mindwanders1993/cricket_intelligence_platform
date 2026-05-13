# Stage C — Phase 2 prompt (Claude Code)

Open a fresh Claude Code session at the project root, then paste:

---

```
Big Task 5 — Phase 2.

Read these files first:
- `docs/silver_match_spec/spec.md` (sections 1, 2 for matches/innings/deliveries/wickets, 3, 5, 6)
- `src/cip/transform/spark/silver/_shared.py` (uses MATCH_JSON_SCHEMA and read_bronze_matches)
- `src/cip/transform/spark/silver/teams.py` (the pattern to follow)
- `src/cip/transform/shared/writers.py` (SparkIcebergWriter.dynamic_overwrite)
- `src/cip/common/contracts/naming.py` (TableName, META)
- `CLAUDE.md` (project invariants)

Implement these 4 transforms + matching tests:

## File 1: `src/cip/transform/spark/silver/matches.py`

`MatchesTransform.run(snapshot_date, pipeline_run_id)`:
- Read Bronze with `read_bronze_matches` (already handles MAX revision dedup)
- Project header columns from `parsed.info`: match_type, gender, season,
  dates (use first element), team_type, balls_per_over, venue, city,
  toss.winner, toss.decision, toss.uncontested, outcome.winner, outcome.result,
  outcome.method, outcome.eliminator, outcome.by.{runs, wickets, innings},
  competition_name (from event.name), match_number, stage, sub_name,
  player_of_match (array → keep as string list or join with comma)
- Normalize `season`: if matches "^\d+$" cast int, else keep string. Add
  `season_year` as int (extract first 4 digits).
- Add silver_meta_columns
- Write to `cricket.silver.matches` with `partition_cols=["_snapshot_date"]`

## File 2: `src/cip/transform/spark/silver/innings.py`

`InningsTransform.run(...)`:
- Read Bronze, then `parsed.innings` (array) → explode with `posexplode_outer`
  to capture innings_order (0-based → +1 for innings_number)
- One row per (match_id, innings_number)
- Columns per spec: match_id, innings_number, team, super_over (bool), 
  declared (bool), forfeited (bool), target_overs, target_runs, 
  absent_hurt (array as string), powerplays (struct array as JSON string),
  penalty_runs_pre, penalty_runs_post
- Handle super_overs: same match_id may have innings_number 3, 4 with super_over=true
- Add silver_meta_columns

## File 3: `src/cip/transform/spark/silver/deliveries.py`

`DeliveriesTransform.run(...)`: the BIG one.

Double explode:
1. explode `parsed.innings` → innings array → innings_order
2. explode `innings.overs` → over array → over_index (0-based, NOT the `over` int field)
3. explode `overs.deliveries` → delivery array → delivery_index (0-based within over)

Compute:
- `over_num` = `overs.over` (the int field from JSON, NOT the array index)
- `ball_num` = delivery_index + 1 (1-based ball within over)
- `legal_ball_num` = cumulative count of legal balls within the over
  (legal = NOT wide AND NOT no-ball). Use a window function partitioned by
  (match_id, innings_number, over_num) ordered by delivery_index.

Columns:
- match_id, innings_number, over_num, ball_num, legal_ball_num
- batter, bowler, non_striker (Utf8)
- runs.batter, runs.extras, runs.total, runs.non_boundary
- extras.wides, extras.noballs, extras.byes, extras.legbyes, extras.penalty
- has_wicket (bool — wickets array non-empty and non-null)
- has_replacement (bool)
- super_over (inherit from innings)

CRITICAL edge cases:
- Wides and no-balls add to extras BUT also count as a delivery (extra ball needed)
- Byes/legbyes/penalty are extras BUT do NOT add an extra ball
- Some matches have `balls_per_over` ≠ 6 (T10=6, The Hundred=5 balls/"over" of 10)
- Default null for missing extras sub-fields (use coalesce → 0 where appropriate)

Add silver_meta_columns. Write with `partition_cols=["_snapshot_date"]`.

## File 4: `src/cip/transform/spark/silver/wickets.py`

`WicketsTransform.run(...)`:
- Same double explode as deliveries, PLUS `explode_outer(deliveries.wickets)`
  on the wickets array
- Filter rows where wicket is non-null (note: outer explode produces NULL rows
  when wickets is empty — filter those)
- One row per wicket

Columns:
- match_id, innings_number, over_num, ball_num
- player_out (Utf8) — IMPORTANT: this is the dismissal subject, NOT batter
- kind (bowled / caught / lbw / run out / stumped / hit wicket / retired hurt / 
  retired out / obstructing the field / hit the ball twice / timed out / 
  caught and bowled)
- bowler, batter, non_striker (carry forward for context)
- fielders (array → join with comma into Utf8 column, marking substitutes)
- substitute_fielder (bool — true if any fielder.substitute = true)

Add silver_meta_columns.

## Tests — one file per transform

For each of the 4 transforms, create
`tests/unit/transform/spark/silver/test_<name>.py`.

Test conventions:
- Mock all I/O (no real Spark session in unit tests — use `pyspark.sql.SparkSession.builder.master("local[1]")` in-process)
- Use `pytest.fixture` for a shared local SparkSession
- Build synthetic Row objects matching MATCH_JSON_SCHEMA for input
- Mock the writer via MagicMock — assert called with expected fqn and partition_cols
- Test scenarios per spec section 6, especially edge cases:
  - matches: polymorphic season int/string/slash, missing event, missing outcome.by
  - innings: super_over, forfeited, declared, target present/absent
  - deliveries: legal_ball_num with wides/no-balls, byes don't add a ball,
    balls_per_over != 6
  - wickets: run-out where player_out ≠ batter, substitute fielder, no wicket
    (empty array), null wickets field

## Constraints

- Follow CLAUDE.md invariants exactly
- TableName.silver(...) — never raw FQN
- META.* — never literal strings
- SparkIcebergWriter.dynamic_overwrite with partition_cols=["_snapshot_date"]
- Lazy import get_settings inside from_settings()
- Run lint and tests before claiming done:
  poetry run ruff check --fix src/cip/transform/spark/silver/
  poetry run black src/cip/transform/spark/silver/
  poetry run pytest tests/unit/transform/spark/silver/ -v
```
