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

# Silver match-grained tables — written by SparkIcebergWriter.delete_and_insert
# under the audit-driven incremental Silver model. Iceberg v2 row-level deletes
# guarantee one row per natural grain in the live table; DuckDB's iceberg_scan
# honours those deletes (verified per docs/runbooks/duckdb-iceberg-deletes.md),
# so the refresh is a plain SELECT * — no MAX(_snapshot_date) filter needed.
_SILVER_MATCH_GRAINED_TABLES = [
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

# Silver dim-shaped tables — written by SparkIcebergWriter.dynamic_overwrite.
# They aggregate across the full Bronze corpus on every run and accumulate one
# partition per write. Keep the MAX(_snapshot_date) filter — gives the latest
# aggregate, ignores stale partitions.
_SILVER_DIM_SHAPED_TABLES = [
    "teams",
    "venues",
    "competitions",
]

# People & Names Silver tables — still written by the legacy register pipeline.
# Keep the existing MAX(_snapshot_date) filter until that pipeline migrates
# to the audit-driven model in a separate PR.
_SILVER_REGISTER_TABLES = [
    "persons",
    "person_identifiers",
    "name_variations",
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
        self.create_control_views()
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

        Two SQL shapes:
          - Match-grained tables (writes via delete_and_insert): plain
            SELECT *. Iceberg v2 row-level deletes keep one row per natural
            grain in the live view, and DuckDB honours those deletes.
          - Dim-shaped + register tables (writes via dynamic_overwrite):
            keep the MAX(_snapshot_date) filter. These re-aggregate the
            full corpus on every write and accumulate partitions; the
            filter gives the latest aggregate and ignores stale ones.
        """
        conn = self._get_connection()
        total = 0
        try:
            self._configure_session(conn)

            # Match-grained tables — plain SELECT *
            for table in _SILVER_MATCH_GRAINED_TABLES:
                self._drop_if_view(conn, "silver", table)
                path = f"s3://{self._bucket}/silver/{table}"
                sql = (
                    f"CREATE OR REPLACE TABLE silver.{table} AS "
                    f"SELECT * FROM iceberg_scan('{path}')"
                )
                conn.execute(sql)
                logger.debug("Silver match-grained table materialised", extra={"table": table, "path": path})
                total += 1

            # Dim-shaped + register tables — MAX(_snapshot_date) filter
            for table in _SILVER_DIM_SHAPED_TABLES + _SILVER_REGISTER_TABLES:
                self._drop_if_view(conn, "silver", table)
                path = f"s3://{self._bucket}/silver/{table}"
                sql = (
                    f"CREATE OR REPLACE TABLE silver.{table} AS "
                    f"SELECT * FROM iceberg_scan('{path}') "
                    f"WHERE _snapshot_date = ("
                    f"  SELECT MAX(_snapshot_date) FROM iceberg_scan('{path}')"
                    f")"
                )
                conn.execute(sql)
                logger.debug("Silver dim/register table materialised", extra={"table": table, "path": path})
                total += 1

            logger.info("Silver tables materialised", extra={"count": total})
        finally:
            conn.close()

    def create_control_views(self) -> None:
        """Materialise control.match_file_audit from PostgreSQL into DuckDB.

        Gold dbt models filter by audit state for incremental builds:
          WHERE match_id IN (
              SELECT match_id FROM control.match_file_audit
              WHERE gold_loaded_at IS NULL
          )

        This refresh produces a point-in-time snapshot of the audit log
        inside the DuckDB file so dbt can query it without spinning up a
        psycopg2 connection per model. Refresh frequency = once per Gold
        DAG invocation, which matches the cadence of dbt-incremental Gold
        runs.
        """
        conn = self._get_connection()
        try:
            self._configure_session(conn)
            pg_conn_str = self._postgres_libpq_dsn()
            sql = (
                "CREATE OR REPLACE TABLE control.match_file_audit AS "
                f"SELECT * FROM postgres_scan('{pg_conn_str}', 'control', 'match_file_audit')"
            )
            conn.execute(sql)
            row_count = conn.execute("SELECT COUNT(*) FROM control.match_file_audit").fetchone()[0]
            logger.info("control.match_file_audit refreshed", extra={"rows": row_count})
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

    def _postgres_libpq_dsn(self) -> str:
        """Return a libpq-compatible Postgres connection string for DuckDB's
        postgres_scan / postgres_attach functions.

        Settings store `postgresql+psycopg2://user:pass@host:port/dbname` —
        strip the `+psycopg2` dialect tag, DuckDB's postgres extension parses
        the rest natively.
        """
        return self._cfg.postgres.dsn.replace("postgresql+psycopg2://", "postgresql://")

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
