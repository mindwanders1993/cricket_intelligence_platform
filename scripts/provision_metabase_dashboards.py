"""Provision the Cricket Intelligence MVP dashboards in Metabase via REST API.

IMPORTANT — why we query fact_delivery directly instead of the *_player_*
marts: those marts filter to `*_person_id IS NOT NULL`, but Cricsheet only
resolves ~1.27% of deliveries to a registry person_id. So Virat Kohli's
career shows 0 runs in mart_player_batting. fact_delivery carries the raw
batter / bowler names for every row, which is what we need for an honest
MVP. CLAUDE.md notes the marts assume the gap is closed at query time via
name joins on dim_player — that closure has to happen here.

Idempotent: archives existing cards/dashboards with the same names before
recreating. Safe to re-run after SQL tweaks.

Usage:
    poetry run python scripts/provision_metabase_dashboards.py
"""

from __future__ import annotations

import os
import sys
import uuid
from dataclasses import dataclass, field

import requests

METABASE_URL = os.environ.get("METABASE_URL", "http://localhost:3000")
METABASE_USER = os.environ.get("METABASE_USER", "admin@cricket-platform.local")
METABASE_PASSWORD = os.environ.get("METABASE_PASSWORD", "Cricket2026!")
METABASE_DB_NAME = os.environ.get("METABASE_DB_NAME", "Cricket Lakehouse")

TIMEOUT = 120


def login() -> tuple[requests.Session, int]:
    s = requests.Session()
    r = s.post(
        f"{METABASE_URL}/api/session",
        json={"username": METABASE_USER, "password": METABASE_PASSWORD},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    s.headers["X-Metabase-Session"] = r.json()["id"]
    dbs = s.get(f"{METABASE_URL}/api/database", timeout=TIMEOUT).json()["data"]
    db = next((d for d in dbs if d["name"] == METABASE_DB_NAME), None)
    if db is None:
        sys.exit(f"Database {METABASE_DB_NAME!r} not found in Metabase")
    return s, db["id"]


def archive_existing(s: requests.Session, name: str, endpoint: str) -> None:
    items = s.get(f"{METABASE_URL}/api/{endpoint}", timeout=TIMEOUT).json()
    items = items if isinstance(items, list) else items.get("data", [])
    for item in items:
        if item.get("name") == name and not item.get("archived", False):
            s.put(
                f"{METABASE_URL}/api/{endpoint}/{item['id']}",
                json={"archived": True},
                timeout=TIMEOUT,
            )


@dataclass
class Card:
    name: str
    sql: str
    display: str = "table"
    viz_settings: dict = field(default_factory=dict)
    template_tags: dict = field(default_factory=dict)


def make_template_tag(name: str, display_name: str | None = None) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "name": name,
        "display-name": display_name or name.title(),
        "type": "text",
    }


def create_card(s: requests.Session, db_id: int, c: Card) -> int:
    archive_existing(s, c.name, "card")
    body = {
        "name": c.name,
        "dataset_query": {
            "database": db_id,
            "type": "native",
            "native": {"query": c.sql, "template-tags": c.template_tags},
        },
        "display": c.display,
        "visualization_settings": c.viz_settings,
    }
    r = s.post(f"{METABASE_URL}/api/card", json=body, timeout=TIMEOUT)
    if r.status_code >= 400:
        sys.exit(f"Failed to create card {c.name!r}: {r.status_code} {r.text[:400]}")
    return r.json()["id"]


def create_dashboard(s: requests.Session, name: str, description: str = "") -> int:
    archive_existing(s, name, "dashboard")
    r = s.post(
        f"{METABASE_URL}/api/dashboard",
        json={"name": name, "description": description},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.json()["id"]


def add_cards_to_dashboard(
    s: requests.Session, dashboard_id: int, layout: list[dict]
) -> None:
    dashcards = []
    for i, item in enumerate(layout):
        dashcards.append(
            {
                "id": -(i + 1),
                "card_id": item["card_id"],
                "row": item["row"],
                "col": item["col"],
                "size_x": item["size_x"],
                "size_y": item["size_y"],
                "parameter_mappings": item.get("parameter_mappings", []),
                "visualization_settings": item.get("visualization_settings", {}),
            }
        )
    r = s.put(
        f"{METABASE_URL}/api/dashboard/{dashboard_id}",
        json={"dashcards": dashcards},
        timeout=TIMEOUT,
    )
    if r.status_code >= 400:
        sys.exit(f"Failed to attach cards: {r.status_code} {r.text[:400]}")


# ============================================================================
# Dashboard 1: Cricket Universe (home)
# ============================================================================

UNIVERSE_CARDS: list[Card] = [
    Card(
        "Total Matches",
        "SELECT COUNT(*) AS total_matches FROM gold.dim_match",
        display="scalar",
    ),
    Card(
        "Total Deliveries",
        "SELECT COUNT(*) AS total_deliveries FROM gold.fact_delivery",
        display="scalar",
    ),
    Card(
        "Total Players",
        "SELECT COUNT(*) AS total_players FROM gold.dim_player",
        display="scalar",
    ),
    Card(
        "Seasons Covered",
        "SELECT COUNT(DISTINCT season) AS total_seasons FROM gold.dim_match",
        display="scalar",
    ),
    Card(
        "Matches per Year",
        """
        SELECT
            EXTRACT(YEAR FROM match_date)::INTEGER AS year,
            COUNT(*) AS matches
        FROM gold.dim_match
        WHERE match_date IS NOT NULL
        GROUP BY 1
        ORDER BY 1
        """.strip(),
        display="bar",
        viz_settings={
            "graph.dimensions": ["year"],
            "graph.metrics": ["matches"],
        },
    ),
    Card(
        "Format Breakdown",
        """
        SELECT match_type AS format, COUNT(*) AS matches
        FROM gold.dim_match
        GROUP BY 1
        ORDER BY 2 DESC
        """.strip(),
        display="pie",
        viz_settings={
            "pie.dimension": "format",
            "pie.metric": "matches",
        },
    ),
    Card(
        "Top 10 Venues",
        """
        SELECT venue, COUNT(*) AS matches
        FROM gold.dim_match
        WHERE venue IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10
        """.strip(),
        display="row",
        viz_settings={
            "graph.dimensions": ["venue"],
            "graph.metrics": ["matches"],
        },
    ),
    Card(
        "Top 10 Run-Scorers (All-Time)",
        """
        SELECT
            batter AS player,
            SUM(runs_batter) AS runs,
            SUM(CASE WHEN is_legal_ball THEN 1 ELSE 0 END) AS balls,
            COUNT(DISTINCT match_id) AS matches
        FROM gold.fact_delivery
        WHERE batter IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10
        """.strip(),
        display="table",
    ),
    Card(
        "Top 10 Wicket-Takers (All-Time)",
        """
        SELECT
            bowler AS player,
            SUM(CASE WHEN is_bowler_wicket THEN 1 ELSE 0 END) AS wickets,
            COUNT(DISTINCT match_id) AS matches
        FROM gold.fact_delivery
        WHERE bowler IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10
        """.strip(),
        display="table",
    ),
    Card(
        "Highest Individual Score Ever",
        """
        WITH innings_scores AS (
            SELECT
                batter,
                match_id,
                innings_number,
                match_type,
                SUM(runs_batter) AS innings_runs
            FROM gold.fact_delivery
            WHERE batter IS NOT NULL
            GROUP BY batter, match_id, innings_number, match_type
        )
        SELECT batter AS player, innings_runs AS score, match_type
        FROM innings_scores
        ORDER BY innings_runs DESC
        LIMIT 1
        """.strip(),
        display="table",
    ),
]


# ============================================================================
# Dashboard 4: Matchup Explorer
# ============================================================================

MATCHUP_CARDS: list[Card] = [
    Card(
        "Top 25 Matchups (by balls faced)",
        """
        SELECT
            batter,
            bowler,
            match_type,
            SUM(CASE WHEN is_legal_ball THEN 1 ELSE 0 END)        AS balls,
            SUM(runs_batter)                                       AS runs,
            SUM(CASE WHEN is_bowler_wicket THEN 1 ELSE 0 END)     AS dismissals,
            ROUND(SUM(runs_batter)*100.0
                  / NULLIF(SUM(CASE WHEN is_legal_ball THEN 1 ELSE 0 END),0), 2) AS strike_rate
        FROM gold.fact_delivery
        WHERE batter IS NOT NULL AND bowler IS NOT NULL
        GROUP BY batter, bowler, match_type
        HAVING SUM(CASE WHEN is_legal_ball THEN 1 ELSE 0 END) >= 30
        ORDER BY balls DESC
        LIMIT 25
        """.strip(),
        display="table",
    ),
]


# ============================================================================
# Dashboard 2: Player Spotlight — uses {{player}} parameter
# ============================================================================

def make_player_cards() -> list[Card]:
    pt = lambda: {"player": make_template_tag("player", "Player")}  # noqa: E731
    return [
        Card(
            "Player Career Headline",
            """
            WITH innings_scores AS (
                SELECT
                    match_id, innings_number, match_type,
                    SUM(runs_batter) AS innings_runs,
                    SUM(CASE WHEN is_legal_ball THEN 1 ELSE 0 END) AS balls_faced,
                    MAX(CASE WHEN is_wicket AND player_out = {{player}} THEN 1 ELSE 0 END) AS dismissed
                FROM gold.fact_delivery
                WHERE batter = {{player}}
                GROUP BY match_id, innings_number, match_type
            )
            SELECT
                SUM(innings_runs)                                            AS runs,
                COUNT(*)                                                     AS innings,
                SUM(balls_faced)                                             AS balls,
                SUM(CASE WHEN innings_runs >= 50  THEN 1 ELSE 0 END)        AS fifties,
                SUM(CASE WHEN innings_runs >= 100 THEN 1 ELSE 0 END)        AS hundreds,
                MAX(innings_runs)                                            AS highest_score,
                ROUND(SUM(innings_runs)*1.0/NULLIF(SUM(dismissed),0), 2)    AS batting_avg,
                ROUND(SUM(innings_runs)*100.0/NULLIF(SUM(balls_faced),0), 2) AS strike_rate
            FROM innings_scores
            """.strip(),
            display="table",
            template_tags=pt(),
        ),
        Card(
            "Player Format Split",
            """
            SELECT
                match_type,
                SUM(runs_batter) AS runs
            FROM gold.fact_delivery
            WHERE batter = {{player}}
            GROUP BY match_type
            ORDER BY runs DESC
            """.strip(),
            display="bar",
            template_tags=pt(),
            viz_settings={
                "graph.dimensions": ["match_type"],
                "graph.metrics": ["runs"],
            },
        ),
        Card(
            "Player Season Trend",
            """
            SELECT
                season,
                SUM(runs_batter) AS runs
            FROM gold.fact_delivery
            WHERE batter = {{player}}
            GROUP BY season
            ORDER BY season
            """.strip(),
            display="line",
            template_tags=pt(),
            viz_settings={
                "graph.dimensions": ["season"],
                "graph.metrics": ["runs"],
            },
        ),
        Card(
            "Player Bowling Card",
            """
            SELECT
                SUM(CASE WHEN is_bowler_wicket THEN 1 ELSE 0 END)             AS wickets,
                COUNT(DISTINCT (match_id || '-' || innings_number))           AS innings,
                SUM(CASE WHEN is_legal_ball THEN 1 ELSE 0 END)                AS balls_bowled,
                ROUND(SUM(runs_total)*6.0
                      / NULLIF(SUM(CASE WHEN is_legal_ball THEN 1 ELSE 0 END),0), 2) AS economy,
                ROUND(SUM(runs_total)*1.0
                      / NULLIF(SUM(CASE WHEN is_bowler_wicket THEN 1 ELSE 0 END),0), 2) AS bowling_avg
            FROM gold.fact_delivery
            WHERE bowler = {{player}}
            """.strip(),
            display="table",
            template_tags=pt(),
        ),
        Card(
            "Player Last 10 Innings",
            """
            WITH per_inn AS (
                SELECT
                    match_id, innings_number, match_type, season,
                    SUM(runs_batter) AS runs,
                    SUM(CASE WHEN is_legal_ball THEN 1 ELSE 0 END) AS balls
                FROM gold.fact_delivery
                WHERE batter = {{player}}
                GROUP BY match_id, innings_number, match_type, season
            )
            SELECT
                m.match_date,
                p.match_type,
                m.venue,
                m.team_a || ' vs ' || m.team_b AS fixture,
                p.runs,
                p.balls
            FROM per_inn p
            JOIN gold.dim_match m USING (match_id)
            ORDER BY m.match_date DESC NULLS LAST
            LIMIT 10
            """.strip(),
            display="table",
            template_tags=pt(),
        ),
    ]


# ============================================================================
# Provision
# ============================================================================

def main() -> None:
    s, db_id = login()
    print(f"Logged in. DuckDB database id = {db_id}")

    # --- Dashboard 1: Cricket Universe -------------------------------------
    print("\nBuilding Cricket Universe…")
    card_ids = [create_card(s, db_id, c) for c in UNIVERSE_CARDS]
    print(f"  created {len(card_ids)} cards")
    dash_id = create_dashboard(
        s,
        "Cricket Universe",
        "Hero counters + all-time leaderboards. Reads gold.dim_match and gold.fact_delivery directly (name-based aggregation; bypasses the person_id-only marts).",
    )
    add_cards_to_dashboard(
        s,
        dash_id,
        [
            # Row 1: 4 hero counters
            {"card_id": card_ids[0], "row": 0, "col": 0,  "size_x": 6, "size_y": 4},
            {"card_id": card_ids[1], "row": 0, "col": 6,  "size_x": 6, "size_y": 4},
            {"card_id": card_ids[2], "row": 0, "col": 12, "size_x": 6, "size_y": 4},
            {"card_id": card_ids[3], "row": 0, "col": 18, "size_x": 6, "size_y": 4},
            # Row 2: matches/year + format pie
            {"card_id": card_ids[4], "row": 4, "col": 0,  "size_x": 16, "size_y": 8},
            {"card_id": card_ids[5], "row": 4, "col": 16, "size_x": 8,  "size_y": 8},
            # Row 3: venues
            {"card_id": card_ids[6], "row": 12, "col": 0, "size_x": 24, "size_y": 8},
            # Row 4: leaderboards
            {"card_id": card_ids[7], "row": 20, "col": 0,  "size_x": 12, "size_y": 8},
            {"card_id": card_ids[8], "row": 20, "col": 12, "size_x": 12, "size_y": 8},
            # Row 5: highest score
            {"card_id": card_ids[9], "row": 28, "col": 0,  "size_x": 24, "size_y": 4},
        ],
    )
    print(f"  → http://localhost:3000/dashboard/{dash_id}")
    universe_dash_id = dash_id

    # --- Dashboard 4: Matchup Explorer -------------------------------------
    print("\nBuilding Matchup Explorer…")
    card_ids = [create_card(s, db_id, c) for c in MATCHUP_CARDS]
    dash_id = create_dashboard(
        s,
        "Matchup Explorer",
        "Batter-vs-bowler head-to-head — uniquely cricket. Reads gold.fact_delivery directly. Min 30 balls per pair.",
    )
    add_cards_to_dashboard(
        s,
        dash_id,
        [{"card_id": card_ids[0], "row": 0, "col": 0, "size_x": 24, "size_y": 14}],
    )
    print(f"  → http://localhost:3000/dashboard/{dash_id}")

    # --- Dashboard 2: Player Spotlight (with filter) -----------------------
    print("\nBuilding Player Spotlight…")

    # Player list card — powers the dropdown; lives in Questions collection.
    archive_existing(s, "Player Names", "card")
    player_list_card_id = create_card(
        s,
        db_id,
        Card(
            "Player Names",
            "SELECT DISTINCT batter AS player_name FROM gold.fact_delivery WHERE batter IS NOT NULL ORDER BY 1",
        ),
    )
    s.put(f"{METABASE_URL}/api/card/{player_list_card_id}", json={"collection_id": None}, timeout=TIMEOUT)

    cards = make_player_cards()
    card_ids = [create_card(s, db_id, c) for c in cards]

    dash_id = create_dashboard(
        s,
        "Player Spotlight",
        "Career view for any player. Filter by player name as it appears in Cricsheet (e.g. 'V Kohli', not 'Virat Kohli').",
    )

    dash_param_id = str(uuid.uuid4())[:8]
    s.put(
        f"{METABASE_URL}/api/dashboard/{dash_id}",
        json={
            "parameters": [
                {
                    "id": dash_param_id,
                    "name": "Player",
                    "slug": "player",
                    "type": "category",
                    "default": "V Kohli",
                    "values_source_type": "card",
                    "values_source_config": {
                        "card_id": player_list_card_id,
                        "value_field": ["field", "player_name", {"base-type": "type/Text"}],
                    },
                }
            ]
        },
        timeout=TIMEOUT,
    )

    def pmap(card_id: int) -> list[dict]:
        return [
            {
                "parameter_id": dash_param_id,
                "card_id": card_id,
                "target": ["variable", ["template-tag", "player"]],
            }
        ]

    add_cards_to_dashboard(
        s,
        dash_id,
        [
            # Career headline (wide)
            {"card_id": card_ids[0], "row": 0,  "col": 0,  "size_x": 24, "size_y": 4,
             "parameter_mappings": pmap(card_ids[0])},
            # Format split | Bowling card
            {"card_id": card_ids[1], "row": 4,  "col": 0,  "size_x": 12, "size_y": 8,
             "parameter_mappings": pmap(card_ids[1])},
            {"card_id": card_ids[3], "row": 4,  "col": 12, "size_x": 12, "size_y": 8,
             "parameter_mappings": pmap(card_ids[3])},
            # Season trend
            {"card_id": card_ids[2], "row": 12, "col": 0,  "size_x": 24, "size_y": 8,
             "parameter_mappings": pmap(card_ids[2])},
            # Last 10
            {"card_id": card_ids[4], "row": 20, "col": 0,  "size_x": 24, "size_y": 10,
             "parameter_mappings": pmap(card_ids[4])},
        ],
    )
    print(f"  → http://localhost:3000/dashboard/{dash_id}")

    print("\n✅ Done. Open the home dashboard:")
    print(f"   http://localhost:3000/dashboard/{universe_dash_id}")


if __name__ == "__main__":
    main()
