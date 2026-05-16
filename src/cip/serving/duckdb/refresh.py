from __future__ import annotations

import os
import subprocess
from pathlib import Path

from cip.common.logging import get_logger
from cip.common.settings import get_settings

logger = get_logger(__name__)

_BRONZE_TABLES = [
    "match_data",
    "people",
    "people_identifiers",
    "name_variations",
]

_SILVER_TABLES = [
    "matches",
    "innings",
    "deliveries",
    "wickets",
    "teams",
    "venues",
    "competitions",
    "persons",
    "person_identifiers",
    "name_variations",
    "match_players",
    "match_officials",
]

_INIT_SQL = Path(__file__).parent / "init.sql"


class DuckDBRefresh:
    """
    Refreshes the DuckDB serving database for the Gold layer.

    Steps:
      1. bootstrap()             — extensions + schemas
      2. create_silver_views()   — Iceberg-backed views over MinIO
      3. run_dbt("run" / "test") — materialise Gold models / run tests

    `run()` chains all three for one-shot manual invocation.
    """

    def __init__(self) -> None:
        self._cfg = get_settings()
        self._db_path = self._cfg.duckdb.db_path
        self._bucket = self._cfg.storage.bucket_lakehouse
        self._s3_endpoint = self._cfg.storage.endpoint.replace("http://", "").replace("https://", "")
        self._s3_key = self._cfg.storage.root_user
        self._s3_secret = self._cfg.storage.root_password.get_secret_value()
        self._s3_region = self._cfg.storage.region

    @classmethod
    def from_settings(cls) -> "DuckDBRefresh":
        return cls()

    @property
    def db_path(self) -> Path:
        return self._db_path

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, dbt_select: str | None = None, dbt_test: bool = True) -> None:
        self.bootstrap()
        self.create_bronze_views()
        self.create_silver_views()
        self.run_dbt("run", select=dbt_select)
        if dbt_test:
            self.run_dbt("test", select=dbt_select)

    def bootstrap(self) -> None:
        """Install extensions and create schemas. Settings are not applied here
        because they're session-scoped and the connection closes — every later
        connection re-applies them via `_configure_session`."""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._get_connection()
        try:
            conn.execute(_INIT_SQL.read_text())
            logger.info("DuckDB bootstrap complete", extra={"db_path": str(self._db_path)})
        finally:
            conn.close()

    def create_bronze_views(self) -> None:
        """Materialise Bronze Iceberg tables as native DuckDB tables.

        Tables (not views) so the DuckDB UI — which opens fresh connections
        that don't inherit session-scoped Iceberg/S3 settings — can query
        them without `SET unsafe_enable_version_guessing=true` etc.
        Snapshot is refreshed every time the Gold DAG runs.
        """
        conn = self._get_connection()
        try:
            self._configure_session(conn)
            for table in _BRONZE_TABLES:
                self._drop_if_view(conn, "bronze", table)
                path = f"s3://{self._bucket}/bronze/{table}"
                # Keep only the latest _snapshot_date partition (matches Silver semantics).
                sql = (
                    f"CREATE OR REPLACE TABLE bronze.{table} AS "
                    f"SELECT * FROM iceberg_scan('{path}') "
                    f"WHERE _snapshot_date = ("
                    f"  SELECT MAX(_snapshot_date) FROM iceberg_scan('{path}')"
                    f")"
                )
                conn.execute(sql)
                logger.debug("Bronze table materialised", extra={"table": table, "path": path})
            logger.info("Bronze tables materialised", extra={"count": len(_BRONZE_TABLES)})
        finally:
            conn.close()

    def create_silver_views(self) -> None:
        """Materialise Silver Iceberg tables as native DuckDB tables.

        See `create_bronze_views` for why we use tables instead of views.
        IMPORTANT: no trailing slash on S3 paths. DuckDB's iceberg extension
        appends '/metadata/…' to this path, and a trailing '/' here produces
        '…/matches//metadata/…' which MinIO rejects (XMinioInvalidObjectName).
        """
        conn = self._get_connection()
        try:
            self._configure_session(conn)
            for table in _SILVER_TABLES:
                self._drop_if_view(conn, "silver", table)
                path = f"s3://{self._bucket}/silver/{table}"
                # Silver Iceberg tables accumulate one partition per pipeline
                # run (_snapshot_date). Keep only the latest so downstream
                # (dbt + UI queries) sees a single, deduped picture.
                sql = (
                    f"CREATE OR REPLACE TABLE silver.{table} AS "
                    f"SELECT * FROM iceberg_scan('{path}') "
                    f"WHERE _snapshot_date = ("
                    f"  SELECT MAX(_snapshot_date) FROM iceberg_scan('{path}')"
                    f")"
                )
                conn.execute(sql)
                logger.debug("Silver table materialised", extra={"table": table, "path": path})
            logger.info("Silver tables materialised", extra={"count": len(_SILVER_TABLES)})
        finally:
            conn.close()

    def run_dbt(self, command: str, select: str | None = None) -> None:
        cfg = self._cfg
        cmd = [
            "dbt",
            command,
            "--project-dir", str(cfg.dbt.project_dir),
            "--profiles-dir", str(cfg.dbt.profiles_dir),
            "--target", cfg.dbt.target,
            "--threads", str(cfg.dbt.threads),
        ]
        if select:
            cmd += ["--select", select]

        env = {
            **os.environ,
            "DUCKDB_DB_PATH": str(self._db_path),
        }

        logger.info("Running dbt", extra={"command": command, "select": select or "all"})
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)

        # Surface dbt's own output into the Airflow log so failures are
        # diagnosable without docker-exec'ing into the worker.
        if result.stdout:
            logger.info("dbt stdout\n" + result.stdout)
        if result.stderr:
            logger.warning("dbt stderr\n" + result.stderr)

        if result.returncode != 0:
            raise RuntimeError(f"dbt {command} failed with exit code {result.returncode}")

        logger.info("dbt command complete", extra={"command": command})

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _get_connection(self):
        import duckdb

        conn = duckdb.connect(str(self._db_path))
        conn.execute(f"SET memory_limit='{self._cfg.duckdb.memory_limit}'")
        conn.execute(f"SET threads={self._cfg.duckdb.threads}")
        return conn

    def _drop_if_view(self, conn, schema: str, name: str) -> None:
        """Drop a legacy view if one exists at this name.

        Earlier versions of this module created Iceberg-backed views; we now
        materialise tables instead. `CREATE OR REPLACE TABLE` works fine when
        the existing object is already a table, but DuckDB refuses to replace
        a view with a table — so we drop the view here as a one-shot migration.
        """
        row = conn.execute(
            "SELECT table_type FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, name],
        ).fetchone()
        if row and row[0] == "VIEW":
            conn.execute(f"DROP VIEW {schema}.{name}")

    def _configure_session(self, conn) -> None:
        # S3 / MinIO
        conn.execute(f"SET s3_endpoint='{self._s3_endpoint}'")
        conn.execute(f"SET s3_access_key_id='{self._s3_key}'")
        conn.execute(f"SET s3_secret_access_key='{self._s3_secret}'")
        conn.execute(f"SET s3_region='{self._s3_region}'")
        conn.execute("SET s3_use_ssl=false")
        conn.execute("SET s3_url_style='path'")
        # Iceberg: PyIceberg does not write version-hint.text, so DuckDB must
        # glob metadata/*.json to find the latest snapshot. Safe here because
        # writes go through PyIceberg's atomic commit — no partial state.
        conn.execute("SET unsafe_enable_version_guessing=true")
