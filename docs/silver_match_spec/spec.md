# Cricsheet Match Silver Pipeline Specification

This document specifies the PySpark Silver pipeline for transforming Cricsheet match JSON documents from Bronze to Silver Iceberg tables.

## Section 1 — PySpark StructType

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    ArrayType, BooleanType, DoubleType, MapType
)

MATCH_JSON_SCHEMA = StructType([
    StructField("meta", StructType([
        StructField("data_version", StringType(), True),
        StructField("created", StringType(), True),
        StructField("revision", IntegerType(), True)
    ]), True),
    StructField("info", StructType([
        StructField("balls_per_over", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("dates", ArrayType(StringType()), True),
        StructField("event", StructType([
            StructField("name", StringType(), True),
            StructField("match_number", StringType(), True),
            StructField("group", StringType(), True),
            StructField("stage", StringType(), True),
            StructField("sub_name", StringType(), True)
        ]), True),
        StructField("gender", StringType(), True),
        StructField("match_type", StringType(), True),
        StructField("match_type_number", IntegerType(), True),
        StructField("missing", ArrayType(StringType()), True),
        StructField("officials", StructType([
            StructField("match_referees", ArrayType(StringType()), True),
            StructField("reserve_umpires", ArrayType(StringType()), True),
            StructField("tv_umpires", ArrayType(StringType()), True),
            StructField("umpires", ArrayType(StringType()), True)
        ]), True),
        StructField("outcome", StructType([
            StructField("by", StructType([
                StructField("innings", IntegerType(), True),
                StructField("runs", IntegerType(), True),
                StructField("wickets", IntegerType(), True)
            ]), True),
            StructField("winner", StringType(), True),
            StructField("result", StringType(), True),
            StructField("method", StringType(), True),
            StructField("eliminator", StringType(), True),
            StructField("bowl_out", StringType(), True)
        ]), True),
        StructField("overs", IntegerType(), True),
        StructField("player_of_match", ArrayType(StringType()), True),
        StructField("players", MapType(StringType(), ArrayType(StringType())), True),
        StructField("registry", StructType([
            StructField("people", MapType(StringType(), StringType()), True)
        ]), True),
        StructField("season", StringType(), True),
        StructField("supersubs", MapType(StringType(), StringType()), True),
        StructField("teams", ArrayType(StringType()), True),
        StructField("team_type", StringType(), True),
        StructField("toss", StructType([
            StructField("decision", StringType(), True),
            StructField("winner", StringType(), True),
            StructField("uncontested", BooleanType(), True)
        ]), True),
        StructField("venue", StringType(), True)
    ]), True),
    StructField("innings", ArrayType(StructType([
        StructField("team", StringType(), True),
        StructField("overs", ArrayType(StructType([
            StructField("over", IntegerType(), True),
            StructField("deliveries", ArrayType(StructType([
                StructField("batter", StringType(), True),
                StructField("bowler", StringType(), True),
                StructField("non_striker", StringType(), True),
                StructField("runs", StructType([
                    StructField("batter", IntegerType(), True),
                    StructField("extras", IntegerType(), True),
                    StructField("total", IntegerType(), True),
                    StructField("non_boundary", IntegerType(), True)
                ]), True),
                StructField("extras", StructType([
                    StructField("wides", IntegerType(), True),
                    StructField("noballs", IntegerType(), True),
                    StructField("byes", IntegerType(), True),
                    StructField("legbyes", IntegerType(), True),
                    StructField("penalty", IntegerType(), True)
                ]), True),
                StructField("wickets", ArrayType(StructType([
                    StructField("player_out", StringType(), True),
                    StructField("kind", StringType(), True),
                    StructField("fielders", ArrayType(StructType([
                        StructField("name", StringType(), True),
                        StructField("substitute", BooleanType(), True)
                    ])), True)
                ])), True),
                StructField("replacements", StructType([
                    StructField("role", ArrayType(StringType()), True),
                    StructField("match", ArrayType(StringType()), True)
                ]), True)
            ])), True)
        ])), True),
        StructField("absent_hurt", ArrayType(StringType()), True),
        StructField("penalty_runs", StructType([
            StructField("pre", IntegerType(), True),
            StructField("post", IntegerType(), True)
        ]), True),
        StructField("declared", BooleanType(), True),
        StructField("forfeited", BooleanType(), True),
        StructField("target", StructType([
            StructField("overs", DoubleType(), True),
            StructField("runs", IntegerType(), True)
        ]), True),
        StructField("super_over", BooleanType(), True),
        StructField("powerplays", ArrayType(StructType([
            StructField("from", DoubleType(), True),
            StructField("to", DoubleType(), True),
            StructField("type", StringType(), True)
        ])), True),
        StructField("miscounted_overs", MapType(StringType(), IntegerType()), True)
    ])), True)
])
```

## Section 2 — Silver table schemas

All tables include mandatory metadata columns: `_snapshot_date` (Date), `_ingested_at` (Timestamp), `_pipeline_run_id` (String), `_dag_run_id` (String), `_source_file` (String), `_row_hash` (String), `_bronze_loaded_at` (Timestamp).

### 1. `silver.matches`
- **FQN**: `TableName.silver("matches")`
- **Grain**: One row per match (filtered to MAX revision per `match_id`).
- **PK**: `match_id`
- **Partition**: `_snapshot_date`, `match_type`

| Column | Type | Nullable | Source Path | Meaning |
|---|---|---|---|---|
| match_id | String | N | `_source_file` | Unique Cricsheet ID |
| season | String | N | `info.season` | Season (normalized) |
| match_type | String | N | `info.match_type` | Test, ODI, T20, etc. |
| gender | String | N | `info.gender` | male, female |
| match_date | Date | N | `info.dates[0]` | Primary date of match |
| team_a | String | N | `info.teams[0]` | First team |
| team_b | String | N | `info.teams[1]` | Second team |
| venue | String | N | `info.venue` | Stadium name |
| city | String | Y | `info.city` | City location |
| balls_per_over | Integer | Y | `info.balls_per_over` | Default 6 |
| limit_overs | Integer | Y | `info.overs` | Scheduled overs |
| event_name | String | Y | `info.event.name` | Competition/Tour name |
| event_number | String | Y | `info.event.match_number` | Match index in event |
| toss_winner | String | Y | `info.toss.winner` | Team that won toss |
| toss_decision | String | Y | `info.toss.decision` | bat, field |
| winner | String | Y | `info.outcome.winner` | Winning team |
| outcome_result | String | Y | `info.outcome.result` | tie, no result, draw |
| outcome_method | String | Y | `info.outcome.method` | D/L, etc. |
| win_by_runs | Integer | Y | `info.outcome.by.runs` | Margin |
| win_by_wickets | Integer | Y | `info.outcome.by.wickets` | Margin |
| win_by_innings | Integer | Y | `info.outcome.by.innings` | Margin |
| player_of_match | Array<String> | Y | `info.player_of_match` | MOTM awardees |

### 2. `silver.innings`
- **FQN**: `TableName.silver("innings")`
- **Grain**: One row per innings.
- **PK**: `match_id`, `innings_number`
- **FK**: `match_id`
- **Partition**: `_snapshot_date`

| Column | Type | Nullable | Source Path | Meaning |
|---|---|---|---|---|
| match_id | String | N | Root | Parent match |
| innings_number | Integer | N | `innings` index | 1-4 |
| team | String | N | `innings[].team` | Batting team |
| super_over | Boolean | N | `innings[].super_over` | Is super over? |
| declared | Boolean | Y | `innings[].declared` | Innings closed early |
| forfeited | Boolean | Y | `innings[].forfeited` | Innings skipped |
| target_runs | Integer | Y | `innings[].target.runs` | Runs to win |
| target_overs | Double | Y | `innings[].target.overs` | Overs to chase |

### 3. `silver.deliveries`
- **FQN**: `TableName.silver("deliveries")`
- **Grain**: One row per ball bowled (legal or extra).
- **PK**: `match_id`, `innings_number`, `over_number`, `delivery_number`
- **FK**: `match_id`, `innings_number`
- **Partition**: `_snapshot_date`

| Column | Type | Nullable | Source Path | Meaning |
|---|---|---|---|---|
| match_id | String | N | Root | Parent match |
| innings_number | Integer | N | Parent | 1-4 |
| over_number | Integer | N | `overs[].over` | 0-indexed over |
| delivery_number | Integer | N | `deliveries` index | 1-indexed ball in over |
| batter | String | N | `deliveries[].batter` | Striker |
| bowler | String | N | `deliveries[].bowler` | Bowler |
| non_striker | String | N | `deliveries[].non_striker` | Non-striker |
| runs_batter | Integer | N | `runs.batter` | Runs off bat |
| runs_extras | Integer | N | `runs.extras` | Extras on ball |
| runs_total | Integer | N | `runs.total` | Total runs |
| runs_non_boundary | Integer | Y | `runs.non_boundary` | All-run runs |
| extra_wides | Integer | Y | `extras.wides` | Wides count |
| extra_noballs | Integer | Y | `extras.noballs` | No-balls count |
| extra_byes | Integer | Y | `extras.byes` | Byes count |
| extra_legbyes | Integer | Y | `extras.legbyes` | Leg-byes count |
| extra_penalty | Integer | Y | `extras.penalty` | Penalty runs |
| is_wicket | Boolean | N | `wickets` present | Did a wicket fall? |

### 4. `silver.wickets`
- **FQN**: `TableName.silver("wickets")`
- **Grain**: One row per wicket fallen.
- **PK**: `match_id`, `innings_number`, `over_number`, `delivery_number`, `player_out`
- **FK**: `match_id`, `innings_number`, `over_number`, `delivery_number`
- **Partition**: `_snapshot_date`

| Column | Type | Nullable | Source Path | Meaning |
|---|---|---|---|---|
| match_id | String | N | Parent | Parent match |
| innings_number | Integer | N | Parent | 1-4 |
| over_number | Integer | N | Parent | 0-indexed over |
| delivery_number | Integer | N | Parent | 1-indexed ball |
| player_out | String | N | `wickets[].player_out` | Dismissed player |
| kind | String | N | `wickets[].kind` | bowled, caught, etc. |
| fielders | Array<String> | Y | `wickets[].fielders[].name` | Involved fielders |

### 5. `silver.teams`
- **FQN**: `TableName.silver("teams")`
- **Grain**: One row per unique team.
- **PK**: `team_name`
- **SCD2**: Applied (`_is_current`, `_valid_from`, `_valid_to`)

| Column | Type | Nullable | Source Path | Meaning |
|---|---|---|---|---|
| team_name | String | N | `info.teams` | Canonical team name |
| team_type | String | Y | `info.team_type` | international, club, etc. |

### 6. `silver.venues`
- **FQN**: `TableName.silver("venues")`
- **Grain**: One row per venue.
- **PK**: `venue_name`, `city`
- **SCD2**: Applied

| Column | Type | Nullable | Source Path | Meaning |
|---|---|---|---|---|
| venue_name | String | N | `info.venue` | Stadium name |
| city | String | Y | `info.city` | City location |

### 7. `silver.competitions`
- **FQN**: `TableName.silver("competitions")`
- **Grain**: One row per competition.
- **PK**: `competition_name`
- **SCD2**: Applied

| Column | Type | Nullable | Source Path | Meaning |
|---|---|---|---|---|
| competition_name | String | N | `info.event.name` | Canonical name |

### 8. `silver.match_players`
- **FQN**: `TableName.silver("match_players")`
- **Grain**: One row per (match_id, team, player_name).
- **PK**: `match_id`, `team`, `player_name`
- **FK**: `match_id`

| Column | Type | Nullable | Source Path | Meaning |
|---|---|---|---|---|
| match_id | String | N | Root | Parent match |
| team | String | N | `info.players` key | Team name |
| player_name | String | N | `info.players` val | Display name used in match |
| person_id | String | Y | Resolution | Resolved identity ID |

### 9. `silver.match_officials`
- **FQN**: `TableName.silver("match_officials")`
- **Grain**: One row per (match_id, role, official_name).
- **PK**: `match_id`, `role`, `official_name`
- **FK**: `match_id`

| Column | Type | Nullable | Source Path | Meaning |
|---|---|---|---|---|
| match_id | String | N | Root | Parent match |
| role | String | N | `info.officials` key | umpire, referee, etc. |
| official_name | String | N | `info.officials` val | Name string |
| person_id | String | Y | Resolution | Resolved identity ID |

### 10. `silver.match_registry`
- **FQN**: `TableName.silver("match_registry")`
- **Grain**: One row per (match_id, display_name, cricsheet_id).
- **PK**: `match_id`, `display_name`
- **FK**: `match_id`

| Column | Type | Nullable | Source Path | Meaning |
|---|---|---|---|---|
| match_id | String | N | Root | Parent match |
| display_name | String | N | `info.registry.people` key | Name in JSON |
| cricsheet_id | String | Y | `info.registry.people` val | Unique Cricsheet ID |

### 11. `silver.unmatched_persons_audit`
- **FQN**: `TableName.silver("unmatched_persons_audit")`
- **Grain**: One row per failed identity resolution.

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| match_id | String | N | Parent match |
| display_name | String | N | Name that failed resolution |
| role | String | N | player, umpire, referee, etc. |
| cricsheet_id | String | Y | Cricsheet ID (if present but missing in register) |
| reason | String | N | No registry mapping, No register match, etc. |

### 12. `silver.match_powerplays`
- **FQN**: `TableName.silver("match_powerplays")`
- **Grain**: One row per powerplay period.
- **PK**: `match_id`, `innings_number`, `from_over`, `type`
- **FK**: `match_id`, `innings_number`

| Column | Type | Nullable | Source Path | Meaning |
|---|---|---|---|---|
| match_id | String | N | Parent | Parent match |
| innings_number | Integer | N | Parent | 1-4 |
| from_over | Double | N | `powerplays[].from` | Start (e.g. 0.1) |
| to_over | Double | N | `powerplays[].to` | End (e.g. 6.0) |
| type | String | N | `powerplays[].type` | mandatory, batting, etc. |

## Section 3 — Edge cases per table

### General
- **`season` polymorphism**: Normalize to string. `2007` (int) → `"2007"`, `"2011/12"` → `"2011/12"`.
- **`match_type` consistency**: Map `IT20` → `T20` if necessary for aggregate analysis, but preserve original in Silver.

### `silver.deliveries` & `silver.wickets`
- **Wicket Subject**: `wickets[].player_out` is the primary key for dismissals. Usually matches `batter`, but differs on run-outs (non-striker or batter involved in mix-up).
- **Super Overs**: `super_over` is a boolean flag. Create new innings rows (e.g. 5, 6) or keep same innings numbering if schema supports (1, 2 with flag). Standard: separate rows in `silver.innings` with `super_over=True`.
- **Extras Counting**:
    - `wides` and `noballs` result in an extra ball (delivery) being recorded that doesn't count towards the over limit.
    - `byes`, `legbyes`, and `penalty` do NOT result in extra balls in the over count, but are recorded as extras.
- **Balls per over**: The Hundred uses 5 balls. T10 and others use 6. `balls_per_over` column in `silver.matches` is critical.

### `silver.matches`
- **Missing `info.event`**: For friendlies or unofficial matches, `event_name` will be NULL.
- **Missing `outcome.by`**: For Tie, No Result, or Draw, the `by` block is absent. Margins should be NULL.
- **Tie / No-Result**: Handled in `outcome_result`. `winner` will be NULL unless an eliminator/bowl-out decided a winner (record as `winner` but keep `outcome_result='tie'`).
- **Match Dates**: For multi-day matches (Tests), `info.dates` is a list. `match_date` in `silver.matches` should be the start date (`dates[0]`).

### `silver.match_players`
- **Replacement Players**: `replacements.role` and `replacements.match` should be monitored. If a player is replaced (concussion sub), they appear in `players` list for the team, but potentially not in all deliveries.
- **Substitute Fielders**: `wickets[].fielders[].substitute` flag must be checked. If True, the fielder might not be in the `info.players` list for the match.

### `silver.match_registry`
- **Incomplete Registry**: `info.registry.people` might map a name to `null` if Cricsheet doesn't have a unique ID for that player.

## Section 4 — Identity resolution algorithm

Pseudocode for resolving names to `silver.persons.person_id`:

```python
# 1. Prepare candidate list from match metadata
match_entities = union(
    select match_id, team, player_name as display_name from silver.match_players,
    select match_id, role, official_name as display_name from silver.match_officials
)

# 2. Join with match-specific registry
# registry: display_name -> cricsheet_id (optional)
candidates = left_join(match_entities, silver.match_registry, on=[match_id, display_name])

# 3. Attempt Resolution
# Path A: Cricsheet ID is available
resolved_via_id = left_join(
    candidates.filter(cricsheet_id is not null),
    silver.person_identifiers, 
    on=[source_identifier = cricsheet_id, source_system = 'cricsheet']
).select(person_id)

# Path B: No Cricsheet ID, fall back to name variations
resolved_via_name = left_join(
    candidates.filter(person_id is null),
    silver.name_variations,
    on=[name = display_name]
).select(identifier as person_id)

# 4. Audit Failures
unmatched = candidates.filter(person_id is null)
write unmatched to silver.unmatched_persons_audit

# 5. Populate Result
update silver.match_players/officials set person_id = resolved.person_id
```

## Section 5 — Dependency order

1. **`silver.matches`**: Foundation for all match-related data.
2. **`silver.teams`**, **`silver.venues`**, **`silver.competitions`**: Dimension-like tables that can be built in parallel.
3. **`silver.match_registry`**: Required for identity resolution.
4. **`silver.match_players`**, **`silver.match_officials`**: Depends on `matches`.
5. **`silver.innings`**: Depends on `matches`.
6. **`silver.deliveries`**: Depends on `innings`.
7. **`silver.wickets`**: Depends on `deliveries`.
8. **`silver.match_powerplays`**: Depends on `innings`.
9. **Identity Resolution**: Final step updating `person_id` in `match_players` and `match_officials` using `silver.persons` (from Register pipeline) and `match_registry`.

## Section 6 — Test scenarios per table

- **`silver.matches`**
    - Happy path: International T20 with all fields populated.
    - Edge: Multi-day Test match with 5-date list.
    - Edge: Abandoned match with `outcome.result = 'no result'`.
- **`silver.innings`**
    - Happy path: 2-innings T20.
    - Edge: Match with 2 Super Overs (Total 6 innings rows).
    - Edge: Test match with forfeiture in 3rd innings.
- **`silver.deliveries`**
    - Happy path: Normal legal delivery with runs.
    - Edge: Wide ball (does not increment over count in most formats).
    - Edge: No-ball with leg-byes (runs credited to extras, ball re-bowled).
- **`silver.wickets`**
    - Happy path: Bowled dismissal.
    - Edge: Run out where `player_out` is the non-striker.
    - Edge: Caught dismissal with `substitute=True` fielder.
- **`silver.teams` / `silver.venues` / `silver.competitions`**
    - SCD2 check: Same name, different `team_type` or metadata across snapshots.
    - Uniqueness: No duplicate rows for the same entity in the same snapshot.
- **`silver.match_players`**
    - Coverage: All 22 players in a standard match are present.
    - Resolution: Concussion substitute recorded in match metadata.
- **`silver.unmatched_persons_audit`**
    - Trigger: Player name exists in JSON but not in Cricsheet Register or Name Variations.

## Section 7 — Open questions

1. **SCD2 Grain for Teams/Venues**: Should we track changes by `match_id` (event-based) or `_snapshot_date` (ingest-based)? Current plan is `_snapshot_date`.
2. **Handling `IT20` vs `T20`**: Cricsheet uses `IT20` for Internationals. Should we unify them at Silver or Gold?
3. **Registry Nulls**: If `registry.people` maps `Name -> null`, should we still attempt name-based lookup in `silver.name_variations`? (Algorithm says yes).
4. **Partitioning Strategy**: For `deliveries`, partitioning by `_snapshot_date` is standard for ingestion, but `match_type` or `season` might be more efficient for Gold layer reads.
