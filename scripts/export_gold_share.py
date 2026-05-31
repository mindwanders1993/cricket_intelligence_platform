"""
Export the Gold layer to a standalone DuckDB file + data dictionary.

Usage:
    poetry run python scripts/export_gold_share.py

Produces:
    storage/exports/cricket_gold.duckdb   — Gold-only database (read_only=True to connect)
    storage/exports/data_dictionary.md    — Table/column reference for RAG context
"""

from __future__ import annotations

import time
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DB = REPO_ROOT / "storage" / "duckdb" / "cricket.duckdb"
EXPORTS_DIR = REPO_ROOT / "storage" / "exports"
OUT_DB = EXPORTS_DIR / "cricket_gold.duckdb"
OUT_DICT = EXPORTS_DIR / "data_dictionary.md"

GOLD_TABLES = [
    "dim_competition",
    "dim_date",
    "dim_match",
    "dim_player",
    "dim_team",
    "dim_venue",
    "fact_delivery",
    "fact_innings",
    "fact_match_result",
    "fact_player_match",
    "fact_player_of_match",
    "mart_matchup_analysis",
    "mart_phase_scoring",
    "mart_player_batting",
    "mart_player_bowling",
    "mart_team_performance",
    "mart_toss_outcome",
    "mart_venue_dna",
    "player_aliases",
    "player_display_names",
]

# Table-level descriptions
TABLE_DESCRIPTIONS: dict[str, str] = {
    "dim_competition": "Distinct competition / event names (e.g. 'ICC Cricket World Cup', 'Indian Premier League').",
    "dim_date":        "Date spine from 1970-01-01 to 2035-12-31. Use date_id (YYYYMMDD int) to join from facts.",
    "dim_match":       "One row per match — full metadata: teams, venue, toss, outcome, format, season.",
    "dim_player":      "Canonical player register from Cricsheet. Contains cross-platform IDs (cricinfo, espn, cricbuzz, wikidata).",
    "dim_team":        "Distinct teams observed across all matches with team_type (international / club).",
    "dim_venue":       "Distinct venues with city. 880 unique grounds across all formats.",
    "fact_delivery":   "PRIMARY ANALYTICAL TABLE. One row per ball bowled — 11.2M rows. Grain: (match_id, innings_number, over_number, delivery_number).",
    "fact_innings":    "One row per innings per match — scoreboard summary: runs, wickets, run rate, boundaries.",
    "fact_match_result": "One row per match — result grain: winner, margin (runs/wickets/innings), DLS method, toss outcome.",
    "fact_player_match": "One row per player per match — batting and bowling match summary aggregated from fact_delivery.",
    "fact_player_of_match": "Bridge table — one row per (match_id, player_name) for Player of the Match awards. Tied matches have multiple rows.",
    "mart_matchup_analysis": "Batter vs bowler head-to-head stats (minimum 6 balls faced). Pre-aggregated for matchup queries.",
    "mart_phase_scoring": "Powerplay / middle / death over scoring rates per format and season.",
    "mart_player_batting": "Career batting aggregates per player × format × season: average, strike rate, 50s, 100s.",
    "mart_player_bowling": "Career bowling aggregates per player × format × season: economy, average, strike rate.",
    "mart_team_performance": "Win/loss record per team × format × season with toss win stats.",
    "mart_toss_outcome": "Toss decision and match outcome correlation by format and season.",
    "mart_venue_dna":  "Venue scoring environment: avg innings scores, run rate, boundary %, chasing win rate.",
    "player_aliases":  "Manual alias seed (61 rows). Use player_display_names for the full resolved mapping.",
    "player_display_names": "Maps every Cricsheet abbreviated name (e.g. 'V Kohli') to full display name. Join to fact_delivery.batter/bowler for readable names.",
}

# Column-level descriptions — only notable ones; others left as column name
COLUMN_DESCRIPTIONS: dict[str, dict[str, str]] = {
    "dim_match": {
        "match_id": "Cricsheet match ID — natural PK. Format: '<type>_<date>_<teams>' or UUID.",
        "match_type": "Format: T20, ODI, Test, IT20 (international T20), ODM, MDM, T20B, hundred.",
        "season": "Season string e.g. '2024', '2023/24'. Normalised from raw JSON (can be int or slash-year).",
        "outcome_result": "Outcome type: 'normal', 'tie', 'no result', 'draw'.",
        "outcome_method": "DLS method applied, if any.",
        "win_by_runs": "Margin if batting team won. 0 if won by wickets.",
        "win_by_wickets": "Margin if chasing team won. 0 if won by runs.",
        "win_by_innings": "1 if won by an innings (Test), else 0.",
    },
    "dim_player": {
        "person_id": "Cricsheet unique identifier — natural PK. Sparse in facts (~1.3% of deliveries have person_id).",
        "full_name": "Full registered name e.g. 'Virat Kohli'.",
        "unique_name": "Disambiguated name used in Cricsheet match files.",
        "cricinfo_id": "ESPNcricinfo numeric ID.",
    },
    "fact_delivery": {
        "match_id": "FK → dim_match.match_id",
        "innings_number": "1-based innings index.",
        "over_number": "0-based over number (0 = first over).",
        "delivery_number": "Ball number within the over (0-based).",
        "batter": "Abbreviated name (e.g. 'V Kohli'). Join player_display_names for full name.",
        "batter_person_id": "FK → dim_player.person_id. Sparse — only ~1.3% of rows populated.",
        "bowler": "Abbreviated name. Join player_display_names for full name.",
        "bowler_person_id": "FK → dim_player.person_id. Sparse.",
        "runs_batter": "Runs credited to the batter (excludes extras).",
        "runs_extras": "Total extras on this delivery.",
        "runs_total": "Total runs off this delivery (batter + extras).",
        "runs_non_boundary": "runs_batter when the shot is NOT a boundary hit.",
        "is_wicket": "True if any wicket fell on this delivery (incl. run-outs).",
        "dismissal_kind": "How the batter was dismissed: caught, bowled, lbw, run out, stumped, etc.",
        "player_out": "Name of the dismissed batter. Use this, not 'batter', for run-out identification.",
        "is_bowler_wicket": "True only for wickets credited to the bowler (caught, bowled, lbw, stumped, caught and bowled, hit wicket).",
        "is_legal_ball": "True if the delivery counts toward the over (not a wide or no-ball).",
        "is_dot_ball": "True if batter scored 0 runs off a legal ball.",
        "match_type": "Denormalised from dim_match for filter performance.",
        "season": "Denormalised from dim_match.",
    },
    "fact_innings": {
        "super_over": "True if this is a super over (tie-breaker).",
        "declared": "True if batting team declared (Test matches).",
        "target_runs": "DLS or normal target set for chasing team.",
        "run_rate": "Runs per over for this innings.",
    },
    "fact_player_match": {
        "person_id": "FK → dim_player.person_id. Sparse (~1.3%). Use player_name for joins.",
        "batting_strike_rate": "runs_scored / balls_faced * 100. NULL if no balls faced.",
        "economy_rate": "runs_conceded / overs_bowled. NULL if no balls bowled.",
    },
    "mart_player_batting": {
        "batting_average": "total_runs / dismissals. NULL if never dismissed.",
        "strike_rate": "total_runs / total_balls * 100.",
        "highest_score": "Highest individual innings score in this cohort.",
    },
    "mart_player_bowling": {
        "bowling_average": "total_runs / total_wickets. NULL if no wickets taken.",
        "bowling_strike_rate": "total_balls / total_wickets. NULL if no wickets taken.",
        "economy_rate": "total_runs / overs_bowled.",
    },
    "mart_venue_dna": {
        "chasing_win_pct": "% of matches at this venue won by the chasing team.",
        "boundary_pct": "% of legal deliveries resulting in a boundary (4 or 6).",
    },
    "player_display_names": {
        "cricsheet_name": "Abbreviated name as used in fact_delivery.batter / fact_delivery.bowler.",
        "display_name": "Full display name. Falls back to cricsheet_name if not in register.",
        "person_id": "FK → dim_player.person_id when the player exists in the register.",
    },
}

SAMPLE_QUERIES = """
## Starter queries

### 1. Top 10 ODI run-scorers since 2020
```sql
SELECT
    pdn.display_name,
    SUM(fd.runs_batter)  AS total_runs,
    COUNT(*)             AS balls_faced,
    ROUND(SUM(fd.runs_batter) * 100.0 / NULLIF(SUM(fd.is_legal_ball::int), 0), 1) AS strike_rate
FROM gold.fact_delivery fd
JOIN gold.player_display_names pdn ON fd.batter = pdn.cricsheet_name
WHERE fd.match_type = 'ODI'
  AND fd.season >= '2020'
  AND fd.is_legal_ball
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
```

### 2. Virat Kohli's batting record by format
```sql
SELECT match_type, season, innings, total_runs, batting_average, strike_rate
FROM gold.mart_player_batting
WHERE player_name = 'V Kohli'
ORDER BY match_type, season;
```

### 3. Best economy rates (T20Is, min 100 overs bowled)
```sql
SELECT player_name, overs_bowled, total_wickets, economy_rate, bowling_average
FROM gold.mart_player_bowling
WHERE match_type = 'IT20'
  AND overs_bowled >= 100
ORDER BY economy_rate
LIMIT 10;
```

### 4. Head-to-head: Kohli vs specific bowlers (min 12 balls)
```sql
SELECT
    batter_name, bowler_name,
    legal_balls AS balls, runs_scored, dismissals,
    strike_rate, dot_ball_pct
FROM gold.mart_matchup_analysis
WHERE batter_name = 'V Kohli'
  AND legal_balls >= 12
ORDER BY balls DESC
LIMIT 15;
```

### 5. Venue scoring environment — best chasing venues in T20Is
```sql
SELECT venue, matches, avg_first_innings_score, chasing_win_pct, boundary_pct
FROM gold.mart_venue_dna
WHERE match_type = 'IT20'
  AND matches >= 10
ORDER BY chasing_win_pct DESC
LIMIT 10;
```
"""


def copy_tables(src_path: Path, dst_path: Path) -> dict[str, int]:
    """Copy all Gold tables to a fresh DuckDB file. Returns {table: row_count}."""
    dst_path.unlink(missing_ok=True)
    con = duckdb.connect(str(dst_path))
    con.execute(f"ATTACH '{src_path}' AS source (READ_ONLY)")
    con.execute("CREATE SCHEMA gold")

    row_counts: dict[str, int] = {}
    for table in GOLD_TABLES:
        print(f"  copying gold.{table} ...", end=" ", flush=True)
        t0 = time.monotonic()
        con.execute(f"CREATE TABLE gold.{table} AS SELECT * FROM source.gold.{table}")
        n = con.execute(f"SELECT COUNT(*) FROM gold.{table}").fetchone()[0]
        elapsed = time.monotonic() - t0
        print(f"{n:,} rows  ({elapsed:.1f}s)")
        row_counts[table] = n

    con.close()
    return row_counts


def build_data_dictionary(dst_path: Path, row_counts: dict[str, int]) -> str:
    """Generate the data_dictionary.md content by reading the exported schema."""
    con = duckdb.connect(str(dst_path), read_only=True)
    cols = con.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'gold'
        ORDER BY table_name, ordinal_position
    """).fetchall()
    con.close()

    # Group columns by table
    schema: dict[str, list[tuple[str, str]]] = {}
    for tname, cname, dtype in cols:
        schema.setdefault(tname, []).append((cname, dtype))

    lines: list[str] = []

    lines.append("# Cricket Intelligence Platform — Gold Layer Data Dictionary\n")
    lines.append("> **21,835 matches · 11.2M deliveries · 18,040 players**  \n")
    lines.append("> Cricsheet data 1970–2026, all international formats + major T20 leagues.\n")

    lines.append("\n## Quick start\n")
    lines.append("```python")
    lines.append("import duckdb")
    lines.append("")
    lines.append('con = duckdb.connect("cricket_gold.duckdb", read_only=True)')
    lines.append("")
    lines.append("# List all tables")
    lines.append('con.execute("SHOW ALL TABLES").fetchdf()')
    lines.append("")
    lines.append("# Top 10 T20 run-scorers ever")
    lines.append('con.execute("""')
    lines.append("    SELECT batter, SUM(runs_batter) AS runs")
    lines.append("    FROM gold.fact_delivery")
    lines.append("    WHERE match_type = 'IT20'")
    lines.append("    GROUP BY 1 ORDER BY 2 DESC LIMIT 10")
    lines.append('""").fetchdf()')
    lines.append("```\n")

    lines.append("## Table inventory\n")
    lines.append("| Table | Rows | Description |")
    lines.append("|---|---:|---|")
    for table in GOLD_TABLES:
        n = row_counts.get(table, 0)
        desc = TABLE_DESCRIPTIONS.get(table, "")
        lines.append(f"| `gold.{table}` | {n:,} | {desc} |")

    lines.append("\n## Key relationships\n")
    lines.append("```")
    lines.append("fact_delivery.match_id          → dim_match.match_id")
    lines.append("fact_delivery.batter            → player_display_names.cricsheet_name  (for full name)")
    lines.append("fact_delivery.bowler            → player_display_names.cricsheet_name  (for full name)")
    lines.append("fact_delivery.batter_person_id  → dim_player.person_id  (sparse, ~1.3%)")
    lines.append("fact_delivery.bowler_person_id  → dim_player.person_id  (sparse, ~1.3%)")
    lines.append("fact_innings.match_id           → dim_match.match_id")
    lines.append("fact_match_result.match_id      → dim_match.match_id")
    lines.append("fact_player_match.match_id      → dim_match.match_id")
    lines.append("fact_player_match.person_id     → dim_player.person_id  (sparse)")
    lines.append("fact_player_of_match.match_id   → dim_match.match_id")
    lines.append("mart_player_batting.person_id   → dim_player.person_id")
    lines.append("mart_player_bowling.person_id   → dim_player.person_id")
    lines.append("mart_matchup_analysis.*_person_id → dim_player.person_id")
    lines.append("player_display_names.person_id  → dim_player.person_id  (when available)")
    lines.append("```\n")
    lines.append("> **Name join pattern**: Cricsheet uses abbreviated names (`V Kohli`, `RG Sharma`).  ")
    lines.append("> Always join `fact_delivery.batter → player_display_names.cricsheet_name`  ")
    lines.append("> to get `display_name` for readable output. Do **not** expect `dim_player.full_name`  ")
    lines.append("> to match `fact_delivery.batter` directly.\n")

    lines.append("## Column reference\n")
    for table in GOLD_TABLES:
        if table not in schema:
            continue
        n = row_counts.get(table, 0)
        lines.append(f"### `gold.{table}` ({n:,} rows)\n")
        desc = TABLE_DESCRIPTIONS.get(table, "")
        if desc:
            lines.append(f"{desc}\n")
        lines.append("| Column | Type | Description |")
        lines.append("|---|---|---|")
        col_descs = COLUMN_DESCRIPTIONS.get(table, {})
        for col, dtype in schema[table]:
            cdesc = col_descs.get(col, "")
            lines.append(f"| `{col}` | {dtype} | {cdesc} |")
        lines.append("")

    lines.append(SAMPLE_QUERIES)

    lines.append("## Important caveats for RAG / AI agent work\n")
    lines.append("- **Abbreviated player names**: `fact_delivery.batter/bowler` use Cricsheet short names (`V Kohli`).  ")
    lines.append("  Always resolve via `player_display_names` before presenting to users.")
    lines.append("- **Sparse person_id**: Only ~1.3% of deliveries have `batter_person_id` / `bowler_person_id` populated.  ")
    lines.append("  Use name-based joins as the primary path; person_id as an optional enrichment.")
    lines.append("- **Season format varies**: seasons appear as `'2024'`, `'2023/24'`, or (rarely) bare integers.  ")
    lines.append("  Use `LIKE '2024%'` or cast comparisons rather than exact equality.")
    lines.append("- **Multi-wicket deliveries**: A single delivery can have `is_wicket=True` and `is_bowler_wicket=False`  ")
    lines.append("  (run-out). Never `SUM(is_wicket)` for bowler wicket counts — use `SUM(is_bowler_wicket::int)` instead.")
    lines.append("- **fact_delivery grain**: `(match_id, innings_number, over_number, delivery_number)` is NOT unique  ")
    lines.append("  in the source (super-sub deliveries, data quirks). Use `delivery_uid` if you need a strict PK — or add  ")
    lines.append("  `ROW_NUMBER()` in your query.")
    lines.append("- **mart_player_batting / bowling**: pre-aggregated by `(person_id, match_type, season)`.  ")
    lines.append("  If you need career totals across all seasons, `SUM()` these marts rather than rebuilding from `fact_delivery`.")

    return "\n".join(lines)


def main() -> None:
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Source : {SOURCE_DB}  ({SOURCE_DB.stat().st_size / 1e6:.0f} MB)")
    print(f"Output : {OUT_DB}\n")

    print("Step 1/2 — copying Gold tables ...")
    row_counts = copy_tables(SOURCE_DB, OUT_DB)

    out_size = OUT_DB.stat().st_size / 1e6
    print(f"\nExported DB size: {out_size:.0f} MB  (was {SOURCE_DB.stat().st_size / 1e6:.0f} MB)\n")

    print("Step 2/2 — writing data dictionary ...")
    md = build_data_dictionary(OUT_DB, row_counts)
    OUT_DICT.write_text(md, encoding="utf-8")
    print(f"Written : {OUT_DICT}\n")

    print("Done. Share these two files:")
    print(f"  {OUT_DB}")
    print(f"  {OUT_DICT}")


if __name__ == "__main__":
    main()
