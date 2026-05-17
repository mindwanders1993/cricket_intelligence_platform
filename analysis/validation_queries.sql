-- =================================================================
-- Cricket Intelligence Platform — Data Validation Suite
-- =================================================================
-- Paste each section into the DuckDB UI (http://localhost:4213).
-- Every query should return a small, easy-to-eyeball result.
-- "OK" or zero-row results indicate the check passed.
-- =================================================================


-- =================================================================
-- SECTION 1 — Row counts at a glance (33 tables across 3 layers)
-- =================================================================

SELECT 'bronze.match_data'         AS table_name, count(*) AS rows FROM bronze.match_data
UNION ALL SELECT 'bronze.people',              count(*) FROM bronze.people
UNION ALL SELECT 'bronze.people_identifiers',  count(*) FROM bronze.people_identifiers
UNION ALL SELECT 'bronze.name_variations',     count(*) FROM bronze.name_variations
UNION ALL SELECT '— silver —',                 NULL
UNION ALL SELECT 'silver.matches',             count(*) FROM silver.matches
UNION ALL SELECT 'silver.innings',             count(*) FROM silver.innings
UNION ALL SELECT 'silver.deliveries',          count(*) FROM silver.deliveries
UNION ALL SELECT 'silver.wickets',             count(*) FROM silver.wickets
UNION ALL SELECT 'silver.teams',               count(*) FROM silver.teams
UNION ALL SELECT 'silver.venues',              count(*) FROM silver.venues
UNION ALL SELECT 'silver.competitions',        count(*) FROM silver.competitions
UNION ALL SELECT 'silver.persons',             count(*) FROM silver.persons
UNION ALL SELECT 'silver.person_identifiers',  count(*) FROM silver.person_identifiers
UNION ALL SELECT 'silver.name_variations',     count(*) FROM silver.name_variations
UNION ALL SELECT 'silver.match_players',       count(*) FROM silver.match_players
UNION ALL SELECT 'silver.match_officials',     count(*) FROM silver.match_officials
UNION ALL SELECT 'silver.match_powerplays',    count(*) FROM silver.match_powerplays
UNION ALL SELECT 'silver.match_registry',      count(*) FROM silver.match_registry
UNION ALL SELECT 'silver.unmatched_persons_audit', count(*) FROM silver.unmatched_persons_audit
UNION ALL SELECT '— gold —',                   NULL
UNION ALL SELECT 'gold.dim_match',             count(*) FROM gold.dim_match
UNION ALL SELECT 'gold.dim_player',            count(*) FROM gold.dim_player
UNION ALL SELECT 'gold.dim_team',              count(*) FROM gold.dim_team
UNION ALL SELECT 'gold.dim_venue',             count(*) FROM gold.dim_venue
UNION ALL SELECT 'gold.dim_competition',       count(*) FROM gold.dim_competition
UNION ALL SELECT 'gold.dim_date',              count(*) FROM gold.dim_date
UNION ALL SELECT 'gold.fact_delivery',         count(*) FROM gold.fact_delivery
UNION ALL SELECT 'gold.fact_innings',          count(*) FROM gold.fact_innings
UNION ALL SELECT 'gold.fact_match_result',     count(*) FROM gold.fact_match_result
UNION ALL SELECT 'gold.fact_player_match',     count(*) FROM gold.fact_player_match
UNION ALL SELECT 'gold.fact_player_of_match',  count(*) FROM gold.fact_player_of_match
UNION ALL SELECT 'gold.mart_player_batting',   count(*) FROM gold.mart_player_batting
UNION ALL SELECT 'gold.mart_player_bowling',   count(*) FROM gold.mart_player_bowling
UNION ALL SELECT 'gold.mart_venue_dna',        count(*) FROM gold.mart_venue_dna
UNION ALL SELECT 'gold.mart_phase_scoring',    count(*) FROM gold.mart_phase_scoring
UNION ALL SELECT 'gold.mart_team_performance', count(*) FROM gold.mart_team_performance
UNION ALL SELECT 'gold.mart_toss_outcome',     count(*) FROM gold.mart_toss_outcome
UNION ALL SELECT 'gold.mart_matchup_analysis', count(*) FROM gold.mart_matchup_analysis;


-- =================================================================
-- SECTION 2 — Bronze layer integrity
-- =================================================================

-- 2.1 Sample one row from each Bronze table (eyeball schema + sources)
SELECT * FROM bronze.match_data         LIMIT 1;
SELECT * FROM bronze.people             LIMIT 1;
SELECT * FROM bronze.people_identifiers LIMIT 1;
SELECT * FROM bronze.name_variations    LIMIT 1;

-- 2.2 Bronze should be ALL strings (Bronze rule — type casting deferred to Silver)
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'bronze'
  AND data_type NOT IN ('VARCHAR', 'DATE', 'TIMESTAMP WITH TIME ZONE')
  AND column_name NOT LIKE '\_%' ESCAPE '\'
ORDER BY table_name, ordinal_position;
-- Expected: zero rows (system _meta columns excluded)

-- 2.3 Every Bronze table has exactly one _snapshot_date (post-dedup at materialisation)
SELECT 'bronze.match_data' AS tbl, count(DISTINCT _snapshot_date) AS snapshots FROM bronze.match_data
UNION ALL SELECT 'bronze.people',             count(DISTINCT _snapshot_date) FROM bronze.people
UNION ALL SELECT 'bronze.people_identifiers', count(DISTINCT _snapshot_date) FROM bronze.people_identifiers
UNION ALL SELECT 'bronze.name_variations',    count(DISTINCT _snapshot_date) FROM bronze.name_variations;
-- Expected: snapshots = 1 for every row

-- 2.4 Metadata columns populated on every row (note: bronze.match_data does
-- not carry _row_hash — match JSON is the source-of-truth identity here)
SELECT count(*)                AS total_rows,
       count(_snapshot_date)   AS with_snapshot_date,
       count(_pipeline_run_id) AS with_pipeline_run_id
FROM bronze.match_data;
-- Expected: all 3 numbers equal

SELECT count(*)                AS total_rows,
       count(_snapshot_date)   AS with_snapshot_date,
       count(_pipeline_run_id) AS with_pipeline_run_id,
       count(_row_hash)        AS with_row_hash
FROM bronze.people;
-- Expected: all 4 numbers equal


-- =================================================================
-- SECTION 3 — Silver layer integrity (typed, exploded, deduped)
-- =================================================================

-- 3.1 Silver grain uniqueness — primary keys should be unique
SELECT 'silver.matches.match_id'                      AS pk,
       count(*) - count(DISTINCT match_id)            AS duplicates
FROM silver.matches
UNION ALL
SELECT 'silver.innings (match_id, innings_number)',
       count(*) - count(DISTINCT (match_id, innings_number))
FROM silver.innings
UNION ALL
SELECT 'silver.deliveries (match_id, innings_number, over_number, delivery_number)',
       count(*) - count(DISTINCT (match_id, innings_number, over_number, delivery_number))
FROM silver.deliveries
UNION ALL
SELECT 'silver.persons.person_id',
       count(*) - count(DISTINCT person_id)
FROM silver.persons;
-- Expected: duplicates = 0 for every row

-- 3.2 No NULL primary keys
SELECT 'silver.matches'    AS tbl, sum(CASE WHEN match_id IS NULL THEN 1 ELSE 0 END) AS null_pk FROM silver.matches
UNION ALL SELECT 'silver.innings',    sum(CASE WHEN match_id IS NULL OR innings_number IS NULL THEN 1 ELSE 0 END) FROM silver.innings
UNION ALL SELECT 'silver.deliveries', sum(CASE WHEN match_id IS NULL OR innings_number IS NULL OR over_number IS NULL OR delivery_number IS NULL THEN 1 ELSE 0 END) FROM silver.deliveries
UNION ALL SELECT 'silver.persons',    sum(CASE WHEN person_id IS NULL THEN 1 ELSE 0 END) FROM silver.persons;
-- Expected: null_pk = 0 for every row

-- 3.3 Silver child tables fully reference silver.matches
SELECT 'innings orphans'    AS check_name, count(*) FROM silver.innings    i WHERE NOT EXISTS (SELECT 1 FROM silver.matches m WHERE m.match_id = i.match_id)
UNION ALL
SELECT 'deliveries orphans', count(*) FROM silver.deliveries d WHERE NOT EXISTS (SELECT 1 FROM silver.matches m WHERE m.match_id = d.match_id)
UNION ALL
SELECT 'wickets orphans',    count(*) FROM silver.wickets    w WHERE NOT EXISTS (SELECT 1 FROM silver.matches m WHERE m.match_id = w.match_id);
-- Expected: count = 0 for every row

-- 3.4 Every match has at least 1 innings (no abandoned matches in our corpus)
SELECT m.match_id
FROM silver.matches m
LEFT JOIN silver.innings i USING (match_id)
WHERE i.match_id IS NULL
LIMIT 10;
-- Expected: zero rows (or a small handful of legitimate abandonments)


-- =================================================================
-- SECTION 4 — Gold layer dimension integrity
-- =================================================================

-- 4.1 Dim primary keys all unique
SELECT 'dim_match.match_id'                AS pk, count(*) - count(DISTINCT match_id)        AS duplicates FROM gold.dim_match
UNION ALL SELECT 'dim_player.person_id',     count(*) - count(DISTINCT person_id)             FROM gold.dim_player
UNION ALL SELECT 'dim_team.team_name',       count(*) - count(DISTINCT team_name)             FROM gold.dim_team
UNION ALL SELECT 'dim_venue.venue_name',     count(*) - count(DISTINCT venue_name)            FROM gold.dim_venue
UNION ALL SELECT 'dim_competition.competition_name', count(*) - count(DISTINCT competition_name) FROM gold.dim_competition
UNION ALL SELECT 'dim_date.date_id',         count(*) - count(DISTINCT date_id)               FROM gold.dim_date;
-- Expected: duplicates = 0 for every row

-- 4.2 dim_date spans the right window (1970 to 2035)
SELECT min(date) AS min_date, max(date) AS max_date, count(*) AS days FROM gold.dim_date;
-- Expected: 1970-01-01 → 2035-12-31, ~24,107 days

-- 4.3 All match dates fall inside the dim_date spine
SELECT m.match_date, count(*) AS matches_without_date_row
FROM gold.dim_match m
LEFT JOIN gold.dim_date d ON m.match_date = d.date
WHERE d.date IS NULL
GROUP BY m.match_date
ORDER BY m.match_date;
-- Expected: zero rows


-- =================================================================
-- SECTION 5 — Gold fact ↔ dim referential integrity
-- =================================================================

-- 5.1 Every fact_delivery row points to a real dim_match
SELECT count(*) AS orphan_deliveries
FROM gold.fact_delivery f
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_match d WHERE d.match_id = f.match_id);
-- Expected: 0

-- 5.2 Every fact_innings row points to a real dim_match
SELECT count(*) AS orphan_innings
FROM gold.fact_innings f
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_match d WHERE d.match_id = f.match_id);
-- Expected: 0

-- 5.3 fact_match_result is one-to-one with dim_match
SELECT
  (SELECT count(*) FROM gold.dim_match)         AS dim_match_rows,
  (SELECT count(*) FROM gold.fact_match_result) AS fact_match_result_rows,
  (SELECT count(*) FROM gold.dim_match) - (SELECT count(*) FROM gold.fact_match_result) AS diff;
-- Expected: diff = 0

-- 5.4 fact_player_of_match — grain is (match_id, player_name); orphans to dim_match must be 0;
--     tied-MOTM matches surface as > 1 row per match_id.
SELECT
  count(*)                                                                AS total_rows,
  count(DISTINCT match_id)                                                AS distinct_matches,
  count(*) - count(DISTINCT (match_id, player_name))                      AS duplicate_grain_rows,
  (SELECT count(*) FROM gold.fact_player_of_match f
     WHERE NOT EXISTS (SELECT 1 FROM gold.dim_match d WHERE d.match_id = f.match_id)) AS orphan_rows,
  (SELECT count(*) FROM (
     SELECT match_id FROM gold.fact_player_of_match GROUP BY 1 HAVING count(*) > 1
   ) t)                                                                   AS tied_motm_matches
FROM gold.fact_player_of_match;
-- Expected: duplicate_grain_rows = 0, orphan_rows = 0, tied_motm_matches > 0 (tied finals etc.)


-- =================================================================
-- SECTION 6 — Cross-layer reconciliation (Bronze → Silver → Gold)
-- =================================================================

-- 6.1 Match count consistency: bronze == silver == gold
SELECT
  (SELECT count(*) FROM bronze.match_data) AS bronze_matches,
  (SELECT count(*) FROM silver.matches)    AS silver_matches,
  (SELECT count(*) FROM gold.dim_match)    AS gold_matches;
-- Expected: bronze = silver = gold (e.g., 21737 / 21737 / 21737)

-- 6.2 Person count consistency: bronze.people → silver.persons → gold.dim_player
SELECT
  (SELECT count(*) FROM bronze.people)    AS bronze_people,
  (SELECT count(*) FROM silver.persons)   AS silver_persons,
  (SELECT count(*) FROM gold.dim_player)  AS gold_dim_player;
-- Expected: all three equal

-- 6.3 Delivery count consistency: silver.deliveries → gold.fact_delivery
SELECT
  (SELECT count(*) FROM silver.deliveries)  AS silver_deliveries,
  (SELECT count(*) FROM gold.fact_delivery) AS gold_fact_delivery;
-- Expected: equal

-- 6.4 Innings count consistency: silver.innings → gold.fact_innings
SELECT
  (SELECT count(*) FROM silver.innings)     AS silver_innings,
  (SELECT count(*) FROM gold.fact_innings)  AS gold_fact_innings;
-- Expected: equal


-- =================================================================
-- SECTION 7 — Business-rule checks
-- =================================================================

-- 7.1 Every match has exactly 2 teams (team_a, team_b non-null)
SELECT count(*) AS matches_with_missing_team
FROM gold.dim_match
WHERE team_a IS NULL OR team_b IS NULL;
-- Expected: 0

-- 7.2 Winners must be one of the two teams in the match
SELECT count(*) AS impossible_winners
FROM gold.dim_match
WHERE winner IS NOT NULL AND winner NOT IN (team_a, team_b);
-- Expected: 0

-- 7.3 fact_innings.total_runs must equal sum(runs_total) from fact_delivery for that innings
WITH delivery_totals AS (
  SELECT match_id, innings_number, sum(runs_total) AS sum_runs
  FROM gold.fact_delivery
  GROUP BY match_id, innings_number
)
SELECT count(*) AS innings_with_run_mismatch
FROM gold.fact_innings i
JOIN delivery_totals d USING (match_id, innings_number)
WHERE i.total_runs != d.sum_runs;
-- Expected: 0 (or very small — float/declaration edge cases)

-- 7.4 Wicket count consistency: fact_delivery is grain "one row per ball",
-- so multi-wicket deliveries (e.g., bowled + retired-hurt on the same ball)
-- contribute only ONE row. Diff = total wickets MINUS distinct delivery-keys-with-wickets.
WITH multi AS (
  SELECT match_id, innings_number, over_number, delivery_number, count(*) AS wickets_on_ball
  FROM silver.wickets GROUP BY 1,2,3,4
)
SELECT
  (SELECT count(*) FROM silver.wickets)                       AS silver_wickets,
  (SELECT count(*) FROM gold.fact_delivery WHERE is_wicket)   AS fact_delivery_wickets,
  (SELECT sum(wickets_on_ball - 1) FROM multi)                AS expected_diff_from_multi_wickets;
-- Expected: silver - gold = expected_diff_from_multi_wickets

-- 7.5 Innings numbers are 1, 2 for limited-overs; 1-4 for Tests
SELECT match_type, min(innings_number) AS min_inn, max(innings_number) AS max_inn
FROM gold.fact_innings
GROUP BY match_type
ORDER BY match_type;
-- Expected: T20/T20I/ODI/IT20 → 1..2; Test → 1..4

-- 7.6 Run rate must be sensible (0-25 RPO)
SELECT match_type, min(run_rate) AS min_rr, max(run_rate) AS max_rr
FROM gold.fact_innings
WHERE legal_balls > 30  -- ignore tiny innings
GROUP BY match_type
ORDER BY match_type;
-- Expected: min ~ 1-3, max < 25


-- =================================================================
-- SECTION 8 — Mart row counts vs source facts (sanity)
-- =================================================================

-- 8.1 mart_player_batting players must exist in dim_player (when person_id known)
SELECT count(*) AS batting_players_missing_in_dim
FROM gold.mart_player_batting m
WHERE m.person_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM gold.dim_player p WHERE p.person_id = m.person_id);
-- Expected: 0

-- 8.2 mart_venue_dna venues all exist in dim_venue
SELECT count(*) AS venues_missing_in_dim
FROM gold.mart_venue_dna m
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_venue v WHERE v.venue_name = m.venue);
-- Expected: 0

-- 8.3 Phase scoring covers expected formats only (T20/IT20/ODI/ODM, Tests excluded)
SELECT match_type, count(*) AS phase_rows
FROM gold.mart_phase_scoring
GROUP BY match_type
ORDER BY match_type;
-- Expected: T20 / IT20 / ODI / ODM only; no Test


-- =================================================================
-- SECTION 9 — Date / freshness checks
-- =================================================================

-- 9.1 Latest match date by format (smoke test that recent data made it in)
SELECT match_type, max(match_date) AS latest_match
FROM gold.dim_match
GROUP BY match_type
ORDER BY latest_match DESC;

-- 9.2 Pipeline run metadata — when was each layer last refreshed?
SELECT 'bronze.match_data' AS tbl, max(_ingested_at) AS last_ingested FROM bronze.match_data
UNION ALL SELECT 'silver.matches',  max(_ingested_at) FROM silver.matches
UNION ALL SELECT 'silver.persons',  max(_ingested_at) FROM silver.persons;
