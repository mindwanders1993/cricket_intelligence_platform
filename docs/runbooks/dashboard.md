# Dashboard Runbook — Metabase MVP

> Operational guide for the Cricket Intelligence Platform's Metabase BI layer.
> Tool: Metabase v0.60.6 (OSS) · Driver: motherduckdb/metabase_duckdb_driver 1.5.2.0 · Backend: DuckDB read-only on `storage/duckdb/cricket.duckdb`.
>
> **Image note:** Metabase runs from a custom image built on `eclipse-temurin:21-jre-jammy` (Ubuntu / glibc), not the official `metabase/metabase` Alpine image. The DuckDB JNI native binary statically links jemalloc which segfaults at `malloc_init_hard` on Alpine's musl libc (even with gcompat). Both arm64 and amd64 native binaries fail. The custom Dockerfile lives at `infra/docker/metabase/Dockerfile`.

The MVP is **4 dashboards** built on the existing 17 Gold tables:
1. **Cricket Universe** (home) — counters + season leaders
2. **Player Spotlight** — career view for any player
3. **Match Centre** — replay any match with Manhattan + Worm charts
4. **Matchup Explorer** — batter vs bowler head-to-head

No new dbt models or marts are required — every query below reads existing `gold.*` tables.

---

## 1. Quick start

```bash
# Bring up Metabase (the rest of the stack should already be running)
make up

# Confirm health
curl -fsS http://localhost:3000/api/health   # expect: {"status":"ok"}

# Open in browser
open http://localhost:3000
```

First boot takes ~60 seconds while Metabase initialises its H2 metadata DB and loads the DuckDB driver from `/plugins/`.

---

## 2. First-time Metabase setup — already done

Admin user and DuckDB connection were provisioned via the `/api/setup` and `/api/database` REST endpoints during initial deployment, so the setup wizard is already past. Login at `http://localhost:3000`:

| Field | Value |
|---|---|
| Email | `admin@cricket-platform.local` |
| Password | `Cricket2026!` |

The DuckDB connection is named **Cricket Lakehouse** (database id `2`) and reads `/data/cricket.duckdb` in read-only mode. You should see four schemas in **Browse Data**: `bronze`, `silver`, `gold`, `staging`. The MVP uses `gold.*` (17 tables) and `staging.stg_silver_innings` (for the Manhattan/Worm charts).

**To re-provision from scratch** (e.g. after wiping `compose_metabase_data` volume), see §10 below.

---

## 3. Build the 4 dashboards

In Metabase, each chart is a **Question** (saved SQL). Questions are pinned onto **Dashboards**. The fastest workflow:

1. Click **+ New** → **SQL query** → **Cricket Lakehouse**.
2. Paste the SQL block from below.
3. Click **Visualization** → pick the chart type listed.
4. **Save** → name it as instructed → assign to the right dashboard.
5. Repeat for every block.

Once all questions exist, create the dashboards and drag the questions onto them.

---

### Dashboard 1 — **Cricket Universe** (home)

**Job:** prove the platform has serious data; orient the visitor in 5 seconds.

#### Q1.1 — Total matches *(Scalar / Number)*
```sql
SELECT COUNT(*) AS total_matches FROM gold.dim_match;
```
Expected: ~21,737.

#### Q1.2 — Total deliveries *(Scalar)*
```sql
SELECT COUNT(*) AS total_deliveries FROM gold.fact_delivery;
```
Expected: ~11.1 M.

#### Q1.3 — Total players *(Scalar)*
```sql
SELECT COUNT(*) AS total_players FROM gold.dim_player;
```
Expected: ~18,020.

#### Q1.4 — Seasons covered *(Scalar)*
```sql
SELECT COUNT(DISTINCT season) AS total_seasons FROM gold.dim_match;
```
Expected: 50.

#### Q1.5 — Matches per year *(Bar chart, X = year, Y = matches)*
```sql
SELECT
    EXTRACT(YEAR FROM match_date)::INTEGER AS year,
    COUNT(*)                                AS matches
FROM gold.dim_match
WHERE match_date IS NOT NULL
GROUP BY 1
ORDER BY 1;
```

#### Q1.6 — Format breakdown *(Pie / Donut)*
```sql
SELECT
    match_type    AS format,
    COUNT(*)      AS matches
FROM gold.dim_match
GROUP BY 1
ORDER BY 2 DESC;
```

#### Q1.7 — Top 10 venues by matches played *(Table or Row chart)*
```sql
SELECT
    venue,
    COUNT(*) AS matches
FROM gold.dim_match
WHERE venue IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
```

#### Q1.8 — Top run-scorer in the latest season *(Scalar with caption)*
```sql
WITH latest_season AS (
    SELECT MAX(season) AS season FROM gold.mart_player_batting
)
SELECT
    player_name,
    total_runs
FROM gold.mart_player_batting
WHERE season = (SELECT season FROM latest_season)
ORDER BY total_runs DESC
LIMIT 1;
```

#### Q1.9 — Top wicket-taker in the latest season *(Scalar with caption)*
```sql
WITH latest_season AS (
    SELECT MAX(season) AS season FROM gold.mart_player_bowling
)
SELECT
    player_name,
    total_wickets
FROM gold.mart_player_bowling
WHERE season = (SELECT season FROM latest_season)
ORDER BY total_wickets DESC
LIMIT 1;
```

#### Q1.10 — Highest individual score (all time) *(Scalar with caption)*
```sql
SELECT
    player_name,
    highest_score,
    match_type
FROM gold.mart_player_batting
ORDER BY highest_score DESC NULLS LAST
LIMIT 1;
```

**Dashboard layout suggestion:**
- Row 1: Q1.1, Q1.2, Q1.3, Q1.4 — four equal-width number cards.
- Row 2: Q1.5 (full width bar) | Q1.6 (donut, narrow).
- Row 3: Q1.7 table.
- Row 4: Q1.8 | Q1.9 | Q1.10 — three "season leader" cards.

---

### Dashboard 2 — **Player Spotlight**

**Job:** any visitor types a player name and gets their career story.

In Metabase, add a **Dashboard filter** of type *Text* called `Player` and connect it to the `{{player}}` parameter in each question below.

#### Q2.1 — Career headline (all formats) *(Table — one row)*
```sql
SELECT
    SUM(total_runs)                                                AS runs,
    SUM(innings)                                                   AS innings,
    SUM(total_balls)                                               AS balls,
    SUM(fifties)                                                   AS fifties,
    SUM(hundreds)                                                  AS hundreds,
    MAX(highest_score)                                             AS highest_score,
    ROUND(SUM(total_runs) * 1.0 / NULLIF(SUM(dismissals), 0), 2)   AS batting_avg,
    ROUND(SUM(total_runs) * 100.0 / NULLIF(SUM(total_balls), 0), 2) AS strike_rate
FROM gold.mart_player_batting
WHERE player_name = {{player}};
```

#### Q2.2 — Format split *(Bar chart, X = match_type, Y = total_runs)*
```sql
SELECT
    match_type,
    SUM(total_runs)                                                  AS runs,
    SUM(total_wickets_safe.wkts)                                     AS wickets
FROM gold.mart_player_batting AS b
LEFT JOIN (
    SELECT player_name, match_type, SUM(total_wickets) AS wkts
    FROM gold.mart_player_bowling
    GROUP BY 1, 2
) AS total_wickets_safe
    USING (player_name, match_type)
WHERE player_name = {{player}}
GROUP BY match_type
ORDER BY runs DESC;
```

#### Q2.3 — Season trend (runs by year) *(Line chart, X = season, Y = total_runs)*
```sql
SELECT
    season,
    SUM(total_runs)        AS runs,
    SUM(total_wickets_safe.wkts) AS wickets
FROM gold.mart_player_batting AS b
LEFT JOIN (
    SELECT player_name, season, SUM(total_wickets) AS wkts
    FROM gold.mart_player_bowling
    GROUP BY 1, 2
) AS total_wickets_safe
    USING (player_name, season)
WHERE player_name = {{player}}
GROUP BY season
ORDER BY season;
```

#### Q2.4 — Bowling card *(Table — one row, hide if no rows)*
```sql
SELECT
    SUM(total_wickets)                                                 AS wickets,
    SUM(innings_bowled)                                                AS innings,
    SUM(total_balls)                                                   AS balls_bowled,
    ROUND(SUM(total_runs) * 6.0 / NULLIF(SUM(total_balls), 0), 2)     AS economy,
    ROUND(SUM(total_runs) * 1.0 / NULLIF(SUM(total_wickets), 0), 2)   AS bowling_avg,
    MAX(best_innings_wickets)                                          AS best_innings
FROM gold.mart_player_bowling
WHERE player_name = {{player}};
```

#### Q2.5 — Last 10 matches *(Table)*
```sql
SELECT
    m.match_date,
    m.match_type,
    pm.venue,
    CASE WHEN pm.team = m.team_a THEN m.team_b ELSE m.team_a END  AS opponent,
    pm.runs_scored,
    pm.balls_faced,
    pm.wickets_taken,
    pm.overs_bowled
FROM gold.fact_player_match pm
JOIN gold.dim_match m USING (match_id)
WHERE pm.player_name = {{player}}
ORDER BY m.match_date DESC NULLS LAST
LIMIT 10;
```

**Dashboard layout suggestion:**
- Player filter at top (typeahead).
- Row 1: Q2.1 (wide).
- Row 2: Q2.2 (left) | Q2.4 (right).
- Row 3: Q2.3 (full-width line chart).
- Row 4: Q2.5 (table).

---

### Dashboard 3 — **Match Centre** *(the wow screen)*

**Job:** pick any match → see the story of it (Manhattan + Worm + top performers).

Add a Dashboard filter `Match ID` of type *Text* connected to `{{match_id}}` in every question.

#### Q3.1 — Match summary card *(Table — one row)*
```sql
SELECT
    match_date,
    match_type,
    venue,
    team_a || ' vs ' || team_b                                       AS fixture,
    winner,
    toss_winner || ' chose to ' || toss_decision                     AS toss,
    CASE
        WHEN win_by_runs    > 0 THEN winner || ' won by ' || win_by_runs    || ' runs'
        WHEN win_by_wickets > 0 THEN winner || ' won by ' || win_by_wickets || ' wickets'
        ELSE outcome_result
    END                                                              AS result
FROM gold.dim_match
WHERE match_id = {{match_id}};
```

#### Q3.2 — Manhattan chart (runs per over per team) *(Bar chart, X = over, Y = runs, series = team)*
```sql
WITH innings AS (
    SELECT match_id, innings_number, team
    FROM staging.stg_silver_innings
    WHERE match_id = {{match_id}}
)
SELECT
    i.team,
    d.over_number + 1                          AS over_no,
    SUM(d.runs_total)                          AS runs,
    SUM(CASE WHEN d.is_wicket THEN 1 ELSE 0 END) AS wickets
FROM gold.fact_delivery d
JOIN innings i
    ON i.match_id       = d.match_id
   AND i.innings_number = d.innings_number
WHERE d.match_id = {{match_id}}
GROUP BY 1, 2
ORDER BY 2, 1;
```

> **Note:** `staging.stg_silver_innings` is a dbt staging view. If Metabase doesn't show the `staging` schema, do **Admin → Databases → Cricket Lakehouse → Sync schema**.

#### Q3.3 — Worm chart (cumulative runs by ball) *(Line chart, X = ball_index, Y = cumulative_runs, series = team)*
```sql
WITH innings AS (
    SELECT match_id, innings_number, team
    FROM staging.stg_silver_innings
    WHERE match_id = {{match_id}}
),
ordered AS (
    SELECT
        i.team,
        d.innings_number,
        ROW_NUMBER() OVER (
            PARTITION BY d.innings_number
            ORDER BY d.over_number, d.delivery_number
        )                                       AS ball_index,
        d.runs_total
    FROM gold.fact_delivery d
    JOIN innings i
        ON i.match_id       = d.match_id
       AND i.innings_number = d.innings_number
    WHERE d.match_id = {{match_id}}
)
SELECT
    team,
    ball_index,
    SUM(runs_total) OVER (
        PARTITION BY innings_number
        ORDER BY ball_index
    )                                           AS cumulative_runs
FROM ordered
ORDER BY innings_number, ball_index;
```

#### Q3.4 — Top batters in this match *(Table, sorted by runs)*
```sql
SELECT
    player_name,
    team,
    runs_scored,
    balls_faced,
    fours,
    sixes,
    batting_strike_rate
FROM gold.fact_player_match
WHERE match_id = {{match_id}}
  AND balls_faced > 0
ORDER BY runs_scored DESC
LIMIT 5;
```

#### Q3.5 — Top bowlers in this match *(Table, sorted by wickets, then economy)*
```sql
SELECT
    player_name,
    team,
    overs_bowled,
    runs_conceded,
    wickets_taken,
    economy_rate
FROM gold.fact_player_match
WHERE match_id = {{match_id}}
  AND balls_bowled > 0
ORDER BY wickets_taken DESC, economy_rate ASC
LIMIT 5;
```

#### Q3.6 — Wicket timeline *(Table — over-by-over fall of wickets)*
```sql
SELECT
    innings_number          AS innings,
    over_number + 1         AS over_no,
    delivery_number         AS ball,
    player_out              AS dismissed,
    dismissal_kind          AS how,
    bowler
FROM gold.fact_delivery
WHERE match_id = {{match_id}}
  AND is_wicket
ORDER BY innings_number, over_number, delivery_number;
```

**Dashboard layout suggestion:**
- Match ID filter at top.
- Row 1: Q3.1 (full width).
- Row 2: Q3.2 Manhattan (full width).
- Row 3: Q3.3 Worm (full width).
- Row 4: Q3.4 (left) | Q3.5 (right).
- Row 5: Q3.6 (full width).

**Picking a match to test with:** the easiest way to grab a `match_id` is to open the *Cricket Universe* dashboard, drill into Q1.5 (matches per year), and copy any match_id from the underlying data. Or run this in Metabase's SQL editor:

```sql
SELECT match_id, match_date, team_a, team_b, venue
FROM gold.dim_match
ORDER BY match_date DESC NULLS LAST
LIMIT 20;
```

---

### Dashboard 4 — **Matchup Explorer**

**Job:** uniquely cricket — batter vs bowler head-to-head over their entire career.

Add two dashboard filters: `Batter` and `Bowler` (both *Text*).

#### Q4.1 — Matchup card *(Table — one row)*
```sql
SELECT
    SUM(balls_faced)                                                   AS balls,
    SUM(runs_scored)                                                   AS runs,
    SUM(dismissals)                                                    AS dismissals,
    ROUND(AVG(strike_rate), 2)                                         AS strike_rate,
    ROUND(AVG(dot_ball_pct), 2)                                        AS dot_pct,
    SUM(fours)                                                         AS fours,
    SUM(sixes)                                                         AS sixes
FROM gold.mart_matchup_analysis
WHERE batter_name = {{batter}}
  AND bowler_name = {{bowler}};
```

#### Q4.2 — Top 25 matchups in cricket history (by balls faced) *(Table)*
```sql
SELECT
    batter_name,
    bowler_name,
    match_type,
    SUM(balls_faced)        AS balls,
    SUM(runs_scored)        AS runs,
    SUM(dismissals)         AS dismissals,
    ROUND(AVG(strike_rate), 2) AS strike_rate
FROM gold.mart_matchup_analysis
GROUP BY batter_name, bowler_name, match_type
ORDER BY balls DESC
LIMIT 25;
```

#### Q4.3 — Batter's nemesis (bowlers who dismissed them most) *(Bar chart, X = bowler, Y = dismissals)*
```sql
SELECT
    bowler_name,
    SUM(dismissals) AS times_dismissed
FROM gold.mart_matchup_analysis
WHERE batter_name = {{batter}}
GROUP BY bowler_name
HAVING SUM(dismissals) > 0
ORDER BY times_dismissed DESC
LIMIT 10;
```

#### Q4.4 — Bowler's victims (batters they dismissed most) *(Bar chart, X = batter, Y = dismissals)*
```sql
SELECT
    batter_name,
    SUM(dismissals) AS times_dismissed
FROM gold.mart_matchup_analysis
WHERE bowler_name = {{bowler}}
GROUP BY batter_name
HAVING SUM(dismissals) > 0
ORDER BY times_dismissed DESC
LIMIT 10;
```

**Dashboard layout suggestion:**
- Two filters at top (Batter, Bowler).
- Row 1: Q4.1 (wide, "VS" card).
- Row 2: Q4.3 (left, batter's nemesis) | Q4.4 (right, bowler's victims).
- Row 3: Q4.2 (full width — top matchups leaderboard).

---

## 4. Pin the dashboards as the landing experience

1. **Settings → Admin Settings → Settings → General → Landing page** → set to `/dashboard/<id-of-Cricket-Universe>`.
2. In the Cricket Universe dashboard, add **dashboard links** (Markdown card) pointing to the other three dashboards. Example markdown:

```markdown
### Dive deeper

- 🏏 [Player Spotlight](/dashboard/N) — search any player
- 🎬 [Match Centre](/dashboard/N) — replay any match
- ⚔️ [Matchup Explorer](/dashboard/N) — head-to-head head-to-head
```

(Replace `N` with the dashboard IDs from the URL bar.)

---

## 5. Refresh workflow (when new data arrives)

DuckDB allows **multiple concurrent readers** but **only one writer**. Metabase holds a read connection; the Gold DAG (`dag_run_gold_dbt_models`) writes. They cannot overlap.

```bash
# 1. Stop Metabase (releases its read locks)
docker stop compose-metabase-1

# 2. Stop the host DuckDB CLI/UI if it's running
make duckdb-stop

# 3. Trigger the Gold refresh DAG via the Airflow UI or:
docker exec compose-airflow-scheduler-1 \
  airflow dags trigger dag_run_gold_dbt_models

# 4. Restart Metabase
docker start compose-metabase-1
```

Metabase auto-reloads schema metadata on next query. If a new dim/fact/mart isn't visible, do **Admin → Databases → Cricket Lakehouse → Sync schema**.

---

## 6. Data quality footer (recommended)

Add a Markdown card to every dashboard with this disclaimer (the `person_id` identity gap is real and not bug):

```markdown
> ℹ️ Player identity in Cricsheet matches is mostly carried by display name.
> Only ~1.27% of deliveries resolve to a registry `person_id`; the rest
> aggregate by `full_name`. Two players sharing a name will be merged in
> these counts.
```

---

## 7. Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Metabase startup hangs on first boot | Plugin JAR still extracting | Wait ~90 s; check `docker logs compose-metabase-1` for `Started Metabase` |
| Database type "DuckDB" not in dropdown | Driver JAR not loaded | `docker logs compose-metabase-1 \| grep -i duckdb` — should show `Registering driver`. If absent, `docker volume rm compose_metabase_plugins` and `make up` again |
| All queries return "IO Error … cannot open" | Another process holds a write lock | Run `make duckdb-stop`; ensure the Gold DAG isn't running; restart Metabase |
| Cumulative chart in Q3.3 wrong | DuckDB window-function precedence | Confirm `ORDER BY innings_number, ball_index` (not just `ball_index`) — the SQL above is correct |
| Filter `{{player}}` errors with "missing required parameter" | Need to mark the parameter as required in the question editor | In SQL question editor, click the variable settings cog → "Required" → "Default value" `V Kohli` (or any common name) |
| Schemas don't include new dbt models | Metabase caches schema | Admin → Databases → Cricket Lakehouse → **Sync schema** |

---

## 8. Files this runbook touches

- `infra/compose/compose.dev.yml` — `metabase-init` + `metabase` services
- `infra/docker/metabase/Dockerfile` — custom glibc-based Metabase image (Ubuntu Jammy + Temurin 21)
- `.env.example` — adds `METABASE_PORT=3000`
- `docs/runbooks/dashboard.md` — this file
- `storage/duckdb/cricket.duckdb` — read-only mount target
- Docker named volumes (created automatically): `compose_metabase_data`, `compose_metabase_plugins`

No changes to `models/dbt/**`, `src/cip/**`, or any DAG. All BI logic lives inside Metabase's H2 metadata DB (mounted at `/metabase-data`).

**Optional follow-up:** Metabase questions/dashboards can be serialised to YAML via `docker exec compose-metabase-1 java -jar /app/metabase.jar export /tmp/export`. Out of MVP scope.

---

## 10. Re-provision Metabase from scratch

If you wipe the `compose_metabase_data` volume (e.g. `docker volume rm compose_metabase_data`), you'll lose the admin user, the DuckDB connection, and any dashboards / saved questions. To re-bootstrap admin + the DuckDB connection via API in one shot (skip the browser wizard):

```bash
# 1. Recreate the metabase container so it boots into setup mode
docker compose -f infra/compose/compose.base.yml -f infra/compose/compose.dev.yml up -d --force-recreate metabase

# 2. Wait for healthy
until [ "$(curl -sf -o /dev/null -w '%{http_code}' http://localhost:3000/api/health)" = "200" ]; do
  echo "waiting..."; sleep 5
done

# 3. Grab the one-time setup token
TOKEN=$(curl -sS http://localhost:3000/api/session/properties | python3 -c "import json,sys; print(json.load(sys.stdin)['setup-token'])")

# 4. Provision admin user (no database — that has to be POSTed separately, the
#    setup endpoint silently drops it)
SESSION=$(curl -sS -X POST http://localhost:3000/api/setup \
  -H 'Content-Type: application/json' \
  -d "{
    \"token\": \"$TOKEN\",
    \"user\": {
      \"first_name\": \"Cricket\", \"last_name\": \"Admin\",
      \"email\": \"admin@cricket-platform.local\",
      \"password\": \"Cricket2026!\",
      \"site_name\": \"Cricket Intelligence Platform\"
    },
    \"prefs\": {\"site_name\": \"Cricket Intelligence Platform\", \"allow_tracking\": false}
  }" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")

# 5. Create the DuckDB connection
curl -sS -X POST http://localhost:3000/api/database \
  -H "Content-Type: application/json" -H "X-Metabase-Session: $SESSION" \
  -d '{"name":"Cricket Lakehouse","engine":"duckdb","details":{"database_file":"/data/cricket.duckdb","read_only":true}}'
```

Dashboards / saved questions still need to be rebuilt via the UI using the SQL in §3.

---

## 12. Open problems & planned improvements

Two known limitations were identified after the MVP was built (2026-05-17). Neither blocks the current dashboard — they are planned follow-up tasks.

---

### Problem 1 — Player name search uses Cricsheet abbreviations, not full names

**Root cause:** Cricsheet stores player names as abbreviated initials (`V Kohli`, `RG Sharma`). The `Player` dropdown is populated from `gold.fact_delivery.batter`, which inherits this naming. Searching `Virat Kohli` returns nothing; `V Kohli` works. Every user who types a full name gets empty cards.

**Solution options:**

| Option | Approach | Effort |
|--------|----------|--------|
| **A — Manual alias seed (recommended quick unblock)** | Create `scripts/data/player_aliases.csv` (~200 famous players). Load as DuckDB table `gold.player_display_names`. Update `Player Names` card SQL to query that table. | ~30 min |
| **B — Wikidata API script** | Use `bronze.people_identifiers.key_wikidata` IDs to fetch display names via the Wikidata API. Write to `silver.player_display_names`. Build a dbt Gold model on top. Covers all 10k+ players automatically. | ~2h |
| **C — Accept Cricsheet naming** | Add a dashboard description: "Use Cricsheet abbreviations (e.g. `V Kohli`, not `Virat Kohli`)". No code changes. | 5 min |

**Recommended execution order:**
1. Option A — seed top 200 aliases (unblocks demos immediately)
2. Option B — Wikidata script for full coverage
3. Update `Player Names` card SQL in `scripts/provision_metabase_dashboards.py` to `SELECT full_name FROM gold.player_display_names ORDER BY 1`
4. Re-run `python scripts/provision_metabase_dashboards.py` to refresh the dashboard

**Files to create/modify:**
- New: `scripts/data/player_aliases.csv`
- New: `scripts/load_player_aliases.py` — loads CSV → DuckDB `gold.player_display_names`
- Modified: `scripts/provision_metabase_dashboards.py` — `Player Names` card SQL

---

### Problem 2 — Filter dropdowns are not cascading (linked)

**Root cause:** Metabase does not support cascading/linked filters for native SQL questions. Selecting `India` in the Nationality filter does NOT narrow the Player dropdown to only Indian players. This is a Metabase architectural limit — linked filters only work for GUI (MBQL) questions, not native SQL.

**Impact:** All six filter dropdowns (Player, Nationality, Format, Season, Gender, Competition) are independent. The dashboard is still fully functional; filters just don't cross-filter each other.

**Solution options:**

| Option | Approach | Effort |
|--------|----------|--------|
| **A — Accept + document** | Add a note to the dashboard description explaining the limitation. No code changes. | 5 min |
| **B — Rebuild simpler cards as MBQL (GUI) questions** | Summary stat cards (career totals, win/loss) can be expressed as MBQL. MBQL questions support linked filters natively. Complex cards (season trend, match log) stay as native SQL. | ~3–4h |
| **C — Migrate to Apache Superset** | Superset supports cascading dashboard filters natively for SQL charts. Full migration off Metabase. | ~1 day |
| **D — Per-nationality player list cards (workaround)** | Separate `Player Names (India)`, `Player Names (Australia)` cards; wire Nationality to switch which list drives the Player dropdown. Fragile, doesn't scale. | ~2h |

**Recommended resolution:** Option A first (5 min, honest), then Option B for simpler cards in a future polish session.

**Files to modify (Option B):**
- `scripts/provision_metabase_dashboards.py` — replace native SQL card definitions for Career Stats and Match Summary with MBQL `dataset_query` (`type: "query"` instead of `type: "native"`)

---

### Suggested next-session execution order

1. `scripts/data/player_aliases.csv` — seed ~200 entries (quick win, unblocks search)
2. `scripts/load_player_aliases.py` — load into `gold.player_display_names` in DuckDB
3. Update `Player Names` card SQL in provisioning script
4. Re-run `python scripts/provision_metabase_dashboards.py`
5. Add dashboard description note about cascading filter limitation (Option A)
6. Evaluate Option B (MBQL cards) — schedule as a separate task

---

## 11. Verification checklist (when build is done)

- [ ] Metabase loads at `http://localhost:3000` in under 5 s on a warm cache.
- [ ] The `Cricket Lakehouse` database appears in Browse Data, with `gold` schema populated.
- [ ] Dashboard 1 (`Cricket Universe`) opens — all 4 counter cards render real numbers (~21.7K matches, ~11M deliveries, ~18K players, 50 seasons).
- [ ] Dashboard 2 (`Player Spotlight`) — type "V Kohli" into the player filter; career card shows non-zero runs + plausible average.
- [ ] Dashboard 3 (`Match Centre`) — paste a real `match_id` from Q1.5 drill-through; Manhattan chart shows 2 series (one per team).
- [ ] Dashboard 4 (`Matchup Explorer`) — top-matchups leaderboard (Q4.2) returns 25 rows, no nulls in `batter_name` / `bowler_name`.
- [ ] `cd models/dbt && poetry run dbt test` — all 40 tests still pass.
