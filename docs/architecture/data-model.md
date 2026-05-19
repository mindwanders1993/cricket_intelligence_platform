# Data Model — Cricket Intelligence Platform

End-to-end ER diagrams for the medallion lakehouse: **Bronze → Silver → Gold**.

All diagrams are Mermaid ERDs and render natively on GitHub. Edit the source in this file
to keep them in sync with `naming.py`, the Silver transforms, and the dbt models.

> **Source of truth**: table FQNs are defined in `src/cip/common/contracts/naming.py`
> (`TableName.BRONZE_TABLES`, `SILVER_TABLES`, `GOLD_TABLES`). dbt schemas live under
> `models/dbt/models/marts/{dimensions,facts,analytics}/_schema.yml`.

---

## 1. Layer overview

```mermaid
flowchart LR
    subgraph Landing["Landing (cricket-source-files)"]
        Z[all_json.zip / recently_added_2_json.zip]
        P[people.csv]
        N[names.csv]
    end

    subgraph Bronze["Bronze — Iceberg (all strings, source-faithful)"]
        BM[bronze.match_data]
        BP[bronze.people]
        BPI[bronze.people_identifiers]
        BNV[bronze.name_variations]
    end

    subgraph Silver["Silver — Iceberg (typed, exploded, deduped)"]
        SM[silver.matches]
        SI[silver.innings]
        SD[silver.deliveries]
        SW[silver.wickets]
        SMP[silver.match_players]
        SMO[silver.match_officials]
        SMPP[silver.match_powerplays]
        SP[silver.persons]
        SPI[silver.person_identifiers]
        SNV[silver.name_variations]
        ST[silver.teams]
        SV[silver.venues]
        SC[silver.competitions]
        SMR[silver.match_registry]
        SUPA[silver.unmatched_persons_audit]
    end

    subgraph Gold["Gold — Star schema (DuckDB tables + Iceberg)"]
        DM[dim_match]
        DP[dim_player]
        DT[dim_team]
        DV[dim_venue]
        DC[dim_competition]
        DD[dim_date]
        FD[fact_delivery]
        FI[fact_innings]
        FMR[fact_match_result]
        FPM[fact_player_match]
        FPOM[fact_player_of_match]
        PDN[player_display_names]
        MARTS[7 mart_* aggregates]
    end

    Z --> BM
    P --> BP
    P --> BPI
    N --> BNV

    BM --> SM
    BM --> SI
    BM --> SD
    BM --> SW
    BM --> SMP
    BM --> SMO
    BM --> SMPP
    BM --> ST
    BM --> SV
    BM --> SC
    BP --> SP
    BPI --> SPI
    BNV --> SNV
    SP --> SMR
    SMP --> SMR
    SMR --> SUPA

    SM --> DM
    SP --> DP
    ST --> DT
    SV --> DV
    SC --> DC
    SP --> PDN
    SM --> FMR
    SI --> FI
    SD --> FD
    SW --> FD
    SMP --> FD
    SMP --> FPM
    SM --> FPOM
    FD --> MARTS
    FPM --> MARTS
```

---

## 2. Bronze layer (raw, all-string)

> **Rule**: every column ingested as `String` (`infer_schema_length=0`). Type coercion is
> deferred to Silver. Match data is **append-only** with `(match_id, revision)` PK —
> corrections become new rows; Silver picks `MAX(revision)`.

```mermaid
erDiagram
    bronze_match_data {
        string match_id PK
        int    revision PK
        string raw_json_payload "Full Cricsheet match JSON"
        date   _snapshot_date
        string _source_file
        string _row_hash
        timestamp _ingested_at
    }

    bronze_people {
        string person_id PK "Cricsheet unique identifier"
        string name
        string unique_name
        date   _snapshot_date
        string _row_hash
    }

    bronze_people_identifiers {
        string person_id FK "→ bronze.people.person_id"
        string source_system "cricinfo | espn | cricbuzz | wikidata | twitter | …"
        string source_identifier
        date   _snapshot_date
    }

    bronze_name_variations {
        string person_id FK "→ bronze.people.person_id"
        string name_variation
        date   _snapshot_date
    }

    bronze_people ||--o{ bronze_people_identifiers : "has cross-site IDs"
    bronze_people ||--o{ bronze_name_variations    : "has known aliases"
```

**Notes**
- `bronze.match_data` is the only Bronze table holding match payloads. All match entities
  in Silver (`innings`, `deliveries`, `wickets`, …) are **exploded from this single JSON column** —
  no separate Bronze tables for each.
- `key_*` columns in `people.csv` are unpivoted long-form into `bronze.people_identifiers`,
  so new identifier sources (e.g. a future `key_bluesky`) flow through automatically.

---

## 3. Silver layer (typed, normalized)

Silver has two pipelines that produce 15 tables:

- **Match pipeline** (PySpark): explodes `bronze.match_data` into match-related entities
- **People & Names pipeline** (Polars): normalizes Cricsheet Register into person identity

```mermaid
erDiagram
    %% ---- Match entities ----
    silver_matches {
        string match_id PK
        string match_type "T20 | ODI | Test | IT20 | hundred | …"
        date   match_date
        string season
        string gender
        string team_a
        string team_b
        string venue
        string city
        string event_name
        string event_number "match number within event"
        int    balls_per_over
        int    limit_overs
        string toss_winner
        string toss_decision "bat | field"
        string winner
        string outcome_result
        string outcome_method "DLS | …"
        int    win_by_runs
        int    win_by_wickets
        boolean win_by_innings
        array  player_of_match
        date   _snapshot_date
    }

    silver_innings {
        string match_id PK,FK
        int    innings_number PK
        string team
        boolean super_over
        boolean declared
        boolean forfeited
        int    target_runs
        double target_overs
        date   _snapshot_date
    }

    silver_deliveries {
        string match_id PK,FK
        int    innings_number PK,FK
        int    over_number PK
        int    delivery_number PK
        string batter
        string non_striker
        string bowler
        int    runs_batter
        int    runs_extras
        int    runs_total
        int    runs_non_boundary
        int    extra_wides
        int    extra_noballs
        int    extra_byes
        int    extra_legbyes
        int    extra_penalty
        boolean is_wicket
        date   _snapshot_date
    }

    silver_wickets {
        string match_id PK,FK
        int    innings_number PK,FK
        int    over_number PK
        int    delivery_number PK
        string player_out PK
        string kind "caught | bowled | lbw | run out | …"
        array  fielders "array of fielder names (substitute flag dropped)"
        date   _snapshot_date
    }

    silver_match_players {
        string match_id PK,FK
        string player_name PK
        string person_id FK "→ silver.persons (sparse ~1%)"
        string team
        date   _snapshot_date
    }

    silver_match_officials {
        string match_id PK,FK
        string role PK "umpire | tv_umpire | match_referee | reserve_umpire"
        string official_name
        string person_id FK "→ silver.persons (sparse)"
        date   _snapshot_date
    }

    silver_match_powerplays {
        string match_id PK,FK
        int    innings_number PK,FK
        string type PK "mandatory | batting | bowling"
        float  from_over
        float  to_over
        date   _snapshot_date
    }

    %% ---- Lookup entities (observed-distinct) ----
    silver_teams {
        string team_name PK
        string team_type "international | domestic | …"
        date   _snapshot_date
    }
    silver_venues {
        string venue_name PK
        string city
        date   _snapshot_date
    }
    silver_competitions {
        string competition_name PK
        date   _snapshot_date
    }

    %% ---- Identity / People & Names ----
    silver_persons {
        string person_id PK
        string name
        string unique_name
        date   _snapshot_date
        boolean _is_current
        timestamp _valid_from
        timestamp _valid_to
    }
    silver_person_identifiers {
        string identifier PK,FK "→ silver.persons.person_id (col not renamed)"
        string source_system PK
        string source_identifier PK
        date   _snapshot_date
    }
    silver_name_variations {
        string identifier PK,FK "→ silver.persons.person_id (col not renamed)"
        string name PK
        date   _snapshot_date
    }

    %% ---- Audit / bridge ----
    silver_match_registry {
        string match_id PK,FK
        string display_name PK "player or official name from match JSON"
        string cricsheet_id "Cricsheet person ID from info.registry.people"
        date   _snapshot_date
    }
    silver_unmatched_persons_audit {
        string match_id PK,FK
        string display_name PK
        string role "player | umpire | tv_umpire | match_referee | …"
        string cricsheet_id "NULL when NO_REGISTRY_MAPPING"
        string reason "NO_REGISTRY_MAPPING | NO_REGISTER_MATCH"
        date   _snapshot_date
    }

    %% ---- Relationships ----
    silver_matches            ||--|{ silver_innings           : "has 1..N innings"
    silver_innings            ||--o{ silver_deliveries        : "contains balls"
    silver_innings            ||--o{ silver_wickets           : "contains wickets"
    silver_innings            ||--o{ silver_match_powerplays  : "has powerplays"
    silver_deliveries         ||--o| silver_wickets           : "may produce wicket"
    silver_matches            ||--o{ silver_match_players     : "has players"
    silver_matches            ||--o{ silver_match_officials   : "has officials"
    silver_matches            ||--o{ silver_match_registry    : "audited per player"

    silver_persons            ||--o{ silver_person_identifiers : "cross-site IDs"
    silver_persons            ||--o{ silver_name_variations    : "known aliases"
    silver_persons            ||--o{ silver_match_players      : "appears in matches"
    silver_persons            ||--o{ silver_match_officials    : "officiates"
    silver_match_players      ||--o| silver_match_registry     : "registry decision"
    silver_match_registry     ||--o| silver_unmatched_persons_audit : "rejects to audit"
```

**Notes**
- `silver.match_players.person_id` is **sparse** (~1–1.3%): Cricsheet match JSONs rarely embed
  registry IDs. Name-based joins in Gold close the gap at query time.
- `silver.wickets` is **not unique** on `(match_id, innings, over, delivery)` — multi-wicket
  deliveries (e.g. caught + run-out non-striker on the same ball) produce multiple rows.
  Gold `fact_delivery` dedups with `QUALIFY ROW_NUMBER()` before joining.
- Silver Iceberg accumulates `_snapshot_date` partitions across re-runs. Every Gold/dbt
  reader **must** filter `WHERE _snapshot_date = MAX(_snapshot_date)`.

---

## 4. Gold layer — star schema

Materialised as DuckDB tables under the `gold` schema (and Iceberg in the future).
Star schema: **6 dimensions + 5 facts + 7 marts + 1 bridge**.

### 4.1 Core star (dimensions + grain facts)

```mermaid
erDiagram
    dim_match {
        string match_id PK
        date   match_date
        string match_type "T20 | ODI | Test | IT20 | hundred"
        string season
        string gender
        string team_a
        string team_b
        string venue
        string city
        string event_name
        string toss_winner
        string toss_decision
        string winner
        string outcome_result
        string outcome_method
        int    win_by_runs
        int    win_by_wickets
        boolean win_by_innings
    }

    dim_player {
        string person_id PK "Cricsheet registry ID"
        string full_name
        string unique_name
        string cricinfo_id
        string espn_id
        string cricbuzz_id
        string wikidata_id
        string twitter_handle
        string instagram_handle
    }

    dim_team {
        string team_name PK
    }
    dim_venue {
        string venue_name PK
        string city
    }
    dim_competition {
        string competition_name PK
    }
    dim_date {
        int  date_id PK "YYYYMMDD"
        date date
        int  year
        int  month
        int  day
        string day_of_week
        boolean is_weekend
    }

    player_display_names {
        string cricsheet_name PK "Abbreviation: 'V Kohli'"
        string display_name      "Full: 'Virat Kohli' (falls back to cricsheet_name)"
        string unique_name       "unique_name from dim_player"
        string person_id FK      "→ dim_player (sparse)"
    }

    fact_delivery {
        string  match_id PK,FK
        int     innings_number PK
        int     over_number PK
        int     delivery_number PK
        string  batter FK "→ player_display_names.cricsheet_name"
        string  batter_person_id FK "→ dim_player (sparse)"
        string  bowler FK "→ player_display_names.cricsheet_name"
        string  bowler_person_id FK "→ dim_player (sparse)"
        string  non_striker
        int     runs_batter
        int     runs_extras
        int     runs_total
        int     runs_non_boundary
        int     extra_wides
        int     extra_noballs
        int     extra_byes
        int     extra_legbyes
        int     extra_penalty
        boolean is_wicket
        string  dismissal_kind
        string  player_out
        boolean is_bowler_wicket
        boolean is_legal_ball
        boolean is_dot_ball
        string  match_type
        string  season
        string  gender
    }

    fact_innings {
        string match_id PK,FK
        int    innings_number PK
        string team FK
        boolean super_over
        boolean declared
        boolean forfeited
        int    target_runs
        double target_overs
        int    total_runs
        int    legal_balls
        float  overs_faced
        int    wickets_fallen
        int    boundaries
        int    sixes
        float  run_rate
    }

    fact_match_result {
        string match_id PK,FK
        string winner FK "→ dim_team"
        string loser "computed: the other team"
        string outcome_result "win | tie | no result | draw"
        string outcome_method "DLS | …"
        int    win_by_runs
        int    win_by_wickets
        boolean win_by_innings
        boolean toss_winner_won "toss_winner = winner"
    }

    fact_player_match {
        string match_id PK,FK
        string person_id PK,FK "may be NULL (~98%)"
        string player_name
        string team FK
        int    runs_scored
        int    balls_faced
        int    fours
        int    sixes
        boolean was_dismissed
        float  batting_strike_rate
        int    balls_bowled
        float  overs_bowled
        int    runs_conceded
        int    wickets_taken
        float  economy_rate
    }

    fact_player_of_match {
        string match_id PK,FK
        string player_name PK "Cricsheet abbreviation"
    }

    %% Relationships
    dim_match    ||--|{ fact_delivery         : "ball-by-ball"
    dim_match    ||--|{ fact_innings          : "innings scoreboard"
    dim_match    ||--|| fact_match_result     : "one outcome per match"
    dim_match    ||--|{ fact_player_match     : "per-player summary"
    dim_match    ||--o{ fact_player_of_match  : "MOTM (1..N for ties)"

    dim_player   ||--o{ fact_delivery         : "as batter"
    dim_player   ||--o{ fact_delivery         : "as bowler"
    dim_player   ||--o{ fact_player_match     : "via person_id (sparse)"

    dim_team     ||--o{ fact_innings          : "batting team"
    dim_team     ||--o{ fact_match_result     : "winner"
    dim_team     ||--o{ fact_player_match     : "team played for"

    dim_venue    ||--o{ dim_match             : "match venue"
    dim_competition ||--o{ dim_match          : "event_name lookup"
    dim_date     ||--o{ dim_match             : "match_date → date_id"

    player_display_names ||--o{ fact_delivery         : "batter / bowler display"
    player_display_names ||--o{ fact_player_match     : "player_name display"
    player_display_names ||--o{ fact_player_of_match  : "MOTM display"
```

### 4.2 Aggregate marts

Pre-computed analytical marts for Metabase dashboards and the FastAPI serving layer.

```mermaid
erDiagram
    fact_delivery {
        string match_id PK
        int    innings_number PK
        int    over_number PK
        int    delivery_number PK
    }
    fact_player_match {
        string match_id PK
        string person_id PK
    }
    dim_player { string person_id PK }
    dim_team   { string team_name PK }
    dim_venue  { string venue_name PK }

    mart_player_batting {
        string person_id PK,FK
        string match_type PK
        string season PK
        int    runs
        int    balls_faced
        float  average
        float  strike_rate
        int    fifties
        int    hundreds
    }
    mart_player_bowling {
        string person_id PK,FK
        string match_type PK
        string season PK
        int    wickets
        float  economy
        float  average
        float  strike_rate
        int    four_wicket_hauls
        int    five_wicket_hauls
    }
    mart_team_performance {
        string team PK,FK
        string match_type PK
        string season PK
        int    matches_played
        int    matches_won
        int    matches_lost
        float  win_rate
    }
    mart_venue_dna {
        string venue PK,FK
        string match_type PK
        float  avg_first_innings_score
        float  chase_win_rate
        int    matches_hosted
    }
    mart_phase_scoring {
        string phase PK "powerplay | middle | death"
        string match_type PK
        float  avg_runs_per_over
        float  avg_balls_per_wicket
    }
    mart_toss_outcome {
        string toss_decision PK "bat | field"
        string match_type PK
        int    matches
        float  win_rate
    }
    mart_matchup_analysis {
        string batter_person_id PK,FK
        string bowler_person_id PK,FK
        int    balls_faced
        int    runs
        int    dismissals
        float  strike_rate
    }

    fact_delivery      ||--o{ mart_player_batting   : "aggregates"
    fact_delivery      ||--o{ mart_player_bowling   : "aggregates"
    fact_delivery      ||--o{ mart_phase_scoring    : "aggregates"
    fact_delivery      ||--o{ mart_matchup_analysis : "min 6 balls"
    fact_player_match  ||--o{ mart_player_batting   : "denormalised dims"
    fact_player_match  ||--o{ mart_player_bowling   : "denormalised dims"
    dim_player         ||--o{ mart_player_batting   : "person_id"
    dim_player         ||--o{ mart_player_bowling   : "person_id"
    dim_player         ||--o{ mart_matchup_analysis : "batter + bowler"
    dim_team           ||--o{ mart_team_performance : "team"
    dim_venue          ||--o{ mart_venue_dna        : "venue"
```

---

## 5. Grain reference

| Table | Grain (1 row per …) | Notes |
|---|---|---|
| `bronze.match_data` | `(match_id, revision)` | Append-only; corrections add new revisions |
| `bronze.people` | `(person_id, _snapshot_date)` | Re-ingested weekly |
| `silver.matches` | `match_id` | Latest revision only |
| `silver.innings` | `(match_id, innings_number)` | 1..N per match (Super Overs add rows) |
| `silver.deliveries` | `(match_id, innings, over, delivery)` | Unique on key |
| `silver.wickets` | `(match_id, innings, over, delivery, player_out)` | **Not** unique on delivery — multi-wicket balls exist |
| `silver.match_players` | `(match_id, player_name)` | Same name on both teams produces 2 rows |
| `dim_match` | `match_id` | Unique |
| `dim_player` | `person_id` | Unique (registry-deduped) |
| `fact_delivery` | `(match_id, innings, over, delivery)` | Wickets pre-deduped before LEFT JOIN |
| `fact_innings` | `(match_id, innings_number)` | |
| `fact_match_result` | `match_id` | Unique |
| `fact_player_match` | `(match_id, player_name)` | `person_id` may be NULL |
| `fact_player_of_match` | `(match_id, player_name)` | Tied matches → multiple MOTMs (EC-006) |

---

## 6. Maintaining this document

When adding a new table:

1. Add the FQN to `TableName.{BRONZE,SILVER,GOLD}_TABLES` in `src/cip/common/contracts/naming.py`.
2. Add the entity block + relationships to the matching layer's `erDiagram` above.
3. Add the grain to the table in §5.
4. Run `poetry run graphify update .` to refresh the knowledge graph.

When a relationship or grain changes, update both the diagram **and** the grain table —
they're checked together during code review.
