"""One-shot migration: delete stale _snapshot_date partitions from Silver.

Match-grained Silver tables written before PR 2 used `dynamic_overwrite`,
which appends a new _snapshot_date partition per run without writing
Iceberg v2 deletes. DuckDB's plain SELECT * in `create_silver_views`
sees both old and new partitions and produces duplicate match_ids in
downstream Gold dim/fact models.

This script issues PyIceberg row-level deletes (v2 delete files) for any
_snapshot_date older than the configured KEEP_LATEST_DATE, on every
match-grained Silver table. Safe to re-run.

Usage:
    ICEBERG_REST_URI=http://localhost:8181 \
    MINIO_S3_ENDPOINT=http://localhost:9000 \
    POSTGRES_HOST=localhost \
    poetry run python scripts/cleanup_silver_stale_snapshots.py
"""

from __future__ import annotations

import datetime as dt

from pyiceberg.expressions import LessThan

from cip.transform.shared.readers import PolarsIcebergReader

# Keep only rows with _snapshot_date >= this date. Tune as needed.
KEEP_FROM_DATE = "2026-05-18"

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
    reader = PolarsIcebergReader.from_settings()
    catalog = reader._catalog  # noqa: SLF001 — one-shot migration

    keep_from = dt.date.fromisoformat(KEEP_FROM_DATE)
    print(f"Cleaning Silver rows with _snapshot_date < {keep_from}")

    for t in MATCH_GRAINED_TABLES:
        fqn = f"silver.{t}"
        try:
            table = catalog.load_table(fqn)
        except Exception as exc:
            print(f"  {fqn:30} SKIP (load failed: {exc})")
            continue

        try:
            table.delete(LessThan("_snapshot_date", keep_from))
            print(f"  {fqn:30} OK")
        except Exception as exc:
            print(f"  {fqn:30} ERR {exc}")


if __name__ == "__main__":
    main()
