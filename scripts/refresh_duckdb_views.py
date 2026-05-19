"""Refresh Bronze/Silver/Control tables in DuckDB from Iceberg.

Wraps DuckDBRefresh.bootstrap() + create_*_views() for manual triggering
from the CLI. The Gold DAGs do this automatically; use this script when
running dbt locally outside Airflow.

Usage:
    ICEBERG_REST_URI=http://localhost:8181 \
    MINIO_S3_ENDPOINT=http://localhost:9000 \
    POSTGRES_HOST=localhost \
    poetry run python scripts/refresh_duckdb_views.py
"""

from __future__ import annotations

from cip.serving.duckdb.refresh import DuckDBRefresh


def main() -> None:
    r = DuckDBRefresh.from_settings()
    r.bootstrap()
    r.create_bronze_views()
    r.create_silver_views()
    r.create_control_views()
    print("DuckDB refreshed: bronze + silver + control")


if __name__ == "__main__":
    main()
