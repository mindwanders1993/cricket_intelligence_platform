"""Diagnose snapshot_date duplication in Silver DuckDB tables.

Run after refresh_duckdb.py has materialised silver.* tables. Reports the
_snapshot_date histogram and distinct-vs-total row counts for every
match-grained Silver table. Tables with >1 _snapshot_date and total > distinct
are the duplication culprits.

Usage:
    poetry run python scripts/diag_silver_snapshots.py
"""

from __future__ import annotations

import duckdb

DB_PATH = "storage/duckdb/cricket.duckdb"

MATCH_GRAINED_TABLES = [
    "matches",
    "innings",
    "deliveries",
    "wickets",
    "match_players",
    "match_officials",
    "match_powerplays",
    "match_registry",
    "unmatched_persons_audit",
]


def main() -> None:
    conn = duckdb.connect(DB_PATH, read_only=True)
    for t in MATCH_GRAINED_TABLES:
        try:
            snaps = conn.execute(
                f"SELECT _snapshot_date, COUNT(*) FROM silver.{t} GROUP BY 1 ORDER BY 1"
            ).fetchall()
            grain = conn.execute(
                f"SELECT COUNT(DISTINCT match_id), COUNT(*) FROM silver.{t}"
            ).fetchone()
            flag = "DUP" if grain[1] > grain[0] else "OK "
            print(f"[{flag}] silver.{t:28} grain={grain}  snapshots={snaps}")
        except Exception as exc:
            print(f"[ERR] silver.{t:28} {exc}")


if __name__ == "__main__":
    main()
