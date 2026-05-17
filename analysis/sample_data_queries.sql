-- analysis/sample_data_queries.sql
-- Sample 10 rows from every Bronze / Silver / Gold table.
-- Paste the whole file into the DuckDB UI, or run individual blocks.
--
-- Tables sourced from src/cip/common/contracts/naming.py
-- (BRONZE_TABLES, SILVER_TABLES, GOLD_TABLES) + dbt model directory.
--
-- 4 Bronze + 12 Silver + 17 Gold = 33 tables.
--
-- NOTE on Bronze raw_json:
--   bronze.match_data has a large `raw_json` column; the query below
--   substrings it to 200 chars so the result is browsable. Drop the
--   SUBSTRING wrapper if you want the full payload.

-- ===========================================================================
-- BRONZE (4 tables)
-- ===========================================================================

-- 1. bronze.match_data — one row per match JSON, raw text payload
SELECT
    match_id,
    revision,
    SUBSTRING(raw_json, 1, 200) AS raw_json_preview,
    _snapshot_date,
    _ingested_at,
    _source_file
FROM bronze.match_data
LIMIT 10;

-- 2. bronze.people — Cricsheet people register (one row per player)
SELECT * FROM bronze.people LIMIT 10;

-- 3. bronze.people_identifiers — unpivoted long-form key_* columns
SELECT * FROM bronze.people_identifiers LIMIT 10;

-- 4. bronze.name_variations — alternative spellings from names.csv
SELECT * FROM bronze.name_variations LIMIT 10;

-- ===========================================================================
-- SILVER (12 tables)
-- ===========================================================================

-- 5. silver.matches — one row per match
SELECT * FROM silver.matches LIMIT 10;

-- 6. silver.innings — one row per innings (typically 2 per match)
SELECT * FROM silver.innings LIMIT 10;

-- 7. silver.deliveries — ball-by-ball; the largest table in the lakehouse
SELECT * FROM silver.deliveries LIMIT 10;

-- 8. silver.wickets — one row per wicket fallen
SELECT * FROM silver.wickets LIMIT 10;

-- 9. silver.teams — distinct team names across all matches
SELECT * FROM silver.teams LIMIT 10;

-- 10. silver.venues — distinct venues
SELECT * FROM silver.venues LIMIT 10;

-- 11. silver.competitions — distinct competitions / event names
SELECT * FROM silver.competitions LIMIT 10;

-- 12. silver.persons — dedup'd people register (Silver Register pipeline)
SELECT * FROM silver.persons LIMIT 10;

-- 13. silver.person_identifiers — registry IDs per person, long-form
SELECT * FROM silver.person_identifiers LIMIT 10;

-- 14. silver.name_variations — Silver-side name variants
SELECT * FROM silver.name_variations LIMIT 10;

-- 15. silver.match_players — squad / playing XI per match
SELECT * FROM silver.match_players LIMIT 10;

-- 16. silver.match_officials — umpires, referees per match
SELECT * FROM silver.match_officials LIMIT 10;

-- ===========================================================================
-- GOLD — Dimensions (6 tables)
-- ===========================================================================

-- 17. gold.dim_match — one row per match
SELECT * FROM gold.dim_match LIMIT 10;

-- 18. gold.dim_player — one row per distinct player (sparse person_id)
SELECT * FROM gold.dim_player LIMIT 10;

-- 19. gold.dim_team — one row per team
SELECT * FROM gold.dim_team LIMIT 10;

-- 20. gold.dim_venue — one row per venue
SELECT * FROM gold.dim_venue LIMIT 10;

-- 21. gold.dim_competition — one row per competition / event
SELECT * FROM gold.dim_competition LIMIT 10;

-- 22. gold.dim_date — date dimension
SELECT * FROM gold.dim_date LIMIT 10;

-- ===========================================================================
-- GOLD — Facts (4 tables)
-- ===========================================================================

-- 23. gold.fact_delivery — one row per ball (largest fact)
SELECT * FROM gold.fact_delivery LIMIT 10;

-- 24. gold.fact_innings — one row per innings, with totals
SELECT * FROM gold.fact_innings LIMIT 10;

-- 25. gold.fact_match_result — one row per match outcome
SELECT * FROM gold.fact_match_result LIMIT 10;

-- 26. gold.fact_player_match — one row per (player, match)
-- NOTE: person_id ~1.27% populated; name-based joins close the gap.
SELECT * FROM gold.fact_player_match LIMIT 10;

-- ===========================================================================
-- GOLD — Analytics marts (7 tables)
-- ===========================================================================

-- 27. gold.mart_player_batting — career & season batting aggregates
SELECT * FROM gold.mart_player_batting LIMIT 10;

-- 28. gold.mart_player_bowling — career & season bowling aggregates
SELECT * FROM gold.mart_player_bowling LIMIT 10;

-- 29. gold.mart_team_performance — team-level win/loss aggregates
SELECT * FROM gold.mart_team_performance LIMIT 10;

-- 30. gold.mart_venue_dna — venue "personality": chasing %, avg score, etc.
SELECT * FROM gold.mart_venue_dna LIMIT 10;

-- 31. gold.mart_phase_scoring — powerplay / middle / death-overs splits
SELECT * FROM gold.mart_phase_scoring LIMIT 10;

-- 32. gold.mart_toss_outcome — toss decision vs match result correlations
SELECT * FROM gold.mart_toss_outcome LIMIT 10;

-- 33. gold.mart_matchup_analysis — batter vs bowler head-to-head records
SELECT * FROM gold.mart_matchup_analysis LIMIT 10;
