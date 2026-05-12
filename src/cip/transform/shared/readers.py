# platform/transform/shared/readers.py
#
# Iceberg-aware read abstractions for the Cricket Intelligence Platform.
#
# Two reader families:
#   1. PolarsIcebergReader  — reads Iceberg tables via PyIceberg → PyArrow → Polars
#                             Used in lightweight Bronze parse jobs
#   2. SparkIcebergReader   — reads Iceberg tables via PySpark + Iceberg runtime jar
#                             Used in Silver and Gold transform jobs
#   3. DuckDBIcebergReader  — reads Iceberg tables via DuckDB native extension
#                             Used in serving layer and dbt execution
#
# Usage:
#   from cip.transform.shared.readers import PolarsIcebergReader
#   reader = PolarsIcebergReader.from_settings()
#   df = reader.read_table("cricket.bronze.match_documents")

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cip.common.exceptions import TableNotFoundError
from cip.common.logging import get_logger
from cip.common.settings import get_settings

if TYPE_CHECKING:
    import duckdb
    import polars as pl
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession

logger = get_logger(__name__)


# ===========================================================================
# Shared catalog config builder
# ===========================================================================


def _build_pyiceberg_catalog_props() -> dict[str, str]:
    """
    Build PyIceberg REST catalog properties from platform settings.
    Used by both PolarsIcebergReader and any PyIceberg direct access.
    """
    cfg = get_settings()
    return {
        "uri": cfg.iceberg.rest_uri,
        "s3.endpoint": cfg.storage.endpoint,
        "s3.access-key-id": cfg.storage.root_user,
        "s3.secret-access-key": cfg.storage.root_password.get_secret_value(),
        "s3.path-style-access": "true",
        "s3.region": cfg.storage.region,
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
    }


def _build_spark_iceberg_conf() -> dict[str, str]:
    """
    Build Spark configuration dict for Iceberg REST catalog + MinIO.
    Injected into SparkSession.builder at job startup.
    """
    cfg = get_settings()
    catalog = cfg.iceberg.catalog_name
    spark_cfg = cfg.spark

    # JAR packages downloaded from Maven on first run (requires internet access).
    # Iceberg runtime + S3A connector for MinIO access.
    jars_packages = ",".join(
        [
            spark_cfg.iceberg_jar,
            f"org.apache.hadoop:hadoop-aws:{spark_cfg.hadoop_aws_version}",
            f"com.amazonaws:aws-java-sdk-bundle:{spark_cfg.aws_java_sdk_version}",
        ]
    )

    return {
        # JAR packages (Iceberg runtime + S3A / MinIO)
        "spark.jars.packages": jars_packages,
        # Iceberg REST catalog
        f"spark.sql.catalog.{catalog}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog}.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
        f"spark.sql.catalog.{catalog}.uri": cfg.iceberg.rest_uri,
        f"spark.sql.catalog.{catalog}.warehouse": cfg.iceberg.warehouse_path,
        f"spark.sql.catalog.{catalog}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"spark.sql.catalog.{catalog}.s3.endpoint": cfg.storage.endpoint,
        f"spark.sql.catalog.{catalog}.s3.path-style-access": "true",
        f"spark.sql.catalog.{catalog}.s3.access-key-id": cfg.storage.root_user,
        f"spark.sql.catalog.{catalog}.s3.secret-access-key": cfg.storage.root_password.get_secret_value(),
        # S3A / MinIO credentials for Hadoop filesystem
        "spark.hadoop.fs.s3a.endpoint": cfg.storage.endpoint,
        "spark.hadoop.fs.s3a.access.key": cfg.storage.root_user,
        "spark.hadoop.fs.s3a.secret.key": cfg.storage.root_password.get_secret_value(),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        # Iceberg extensions
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": catalog,
    }


# ===========================================================================
# 1. PolarsIcebergReader
# ===========================================================================


class PolarsIcebergReader:
    """
    Reads Iceberg tables into Polars DataFrames via PyIceberg → PyArrow.

    Best suited for:
        - Bronze parse jobs reading small-to-medium Iceberg tables
        - Validation jobs doing row-level checks
        - Incremental reads with snapshot_id filtering

    Not suited for:
        - Full Silver table scans (use SparkIcebergReader)
        - Joins across multiple large tables
    """

    def __init__(self, catalog_props: dict[str, str] | None = None) -> None:
        from pyiceberg.catalog import load_catalog

        props = catalog_props or _build_pyiceberg_catalog_props()
        catalog_name = get_settings().iceberg.catalog_name
        self._catalog = load_catalog(catalog_name, **props)
        logger.debug(
            "PolarsIcebergReader initialised",
            extra={"catalog": catalog_name, "uri": props.get("uri")},
        )

    @classmethod
    def from_settings(cls) -> "PolarsIcebergReader":
        return cls()

    def _resolve_table(self, fqn: str):
        """Load a PyIceberg Table object, raising TableNotFoundError if absent."""
        try:
            return self._catalog.load_table(fqn)
        except Exception as exc:
            parts = fqn.split(".")
            ns = parts[1] if len(parts) >= 3 else ""
            tbl = parts[2] if len(parts) >= 3 else fqn
            raise TableNotFoundError(ns, tbl) from exc

    def read_table(
        self,
        fqn: str,
        columns: list[str] | None = None,
        filters: list[Any] | None = None,
        row_filter: str | None = None,
        snapshot_id: int | None = None,
    ) -> "pl.DataFrame":
        """
        Read an Iceberg table into a Polars DataFrame.

        Args:
            fqn:         Fully qualified table name — use TableName.bronze(...)
            columns:     Column projection — None reads all columns
            filters:     PyIceberg expression filters for partition pruning
            row_filter:  SQL-style row filter string (PyIceberg Expression)
            snapshot_id: Read a specific Iceberg snapshot (time-travel)

        Returns:
            polars.DataFrame

        Example:
            df = reader.read_table(
                TableName.bronze("match_documents"),
                columns=["match_id", "_snapshot_date", "raw_json"],
                row_filter="match_type = 'T20'",
            )
        """
        import polars as pl

        table = self._resolve_table(fqn)
        scan_kwargs: dict = {}
        if row_filter is not None:
            scan_kwargs["row_filter"] = row_filter
        if snapshot_id is not None:
            scan_kwargs["snapshot_id"] = snapshot_id
        if columns:
            scan_kwargs["selected_fields"] = tuple(columns)
        scan = table.scan(**scan_kwargs)

        logger.info(
            "Reading Iceberg table (Polars)",
            extra={
                "table": fqn,
                "columns": columns,
                "snapshot_id": snapshot_id,
                "row_filter": row_filter or "",
            },
        )

        arrow_table = scan.to_arrow()
        df = pl.from_arrow(arrow_table)

        logger.info(
            "Iceberg read complete (Polars)",
            extra={"table": fqn, "rows": len(df), "cols": len(df.columns)},
        )
        return df

    def read_incremental(
        self,
        fqn: str,
        watermark_col: str = "_snapshot_date",
        since: str | None = None,
        columns: list[str] | None = None,
    ) -> "pl.DataFrame":
        """
        Read rows added since a watermark date.
        Used by Silver jobs to pick up only new Bronze rows.

        Args:
            fqn:           Table FQN
            watermark_col: Date column to filter on (default: _snapshot_date)
            since:         ISO date string — only rows WHERE watermark_col >= since
            columns:       Column projection

        Example:
            df = reader.read_incremental(
                TableName.bronze("match_documents"),
                since="2024-11-01",
            )
        """
        row_filter = f"{watermark_col} >= '{since}'" if since else None
        return self.read_table(fqn, columns=columns, row_filter=row_filter)

    def table_exists(self, fqn: str) -> bool:
        """Return True if the table exists in the catalog."""
        try:
            self._catalog.load_table(fqn)
            return True
        except Exception:
            return False

    def list_snapshots(self, fqn: str) -> list[dict[str, Any]]:
        """Return snapshot history for a table — useful for time-travel debugging."""
        table = self._resolve_table(fqn)
        return [
            {
                "snapshot_id": s.snapshot_id,
                "timestamp_ms": s.timestamp_ms,
                "operation": s.summary.get("operation", ""),
                "added_records": s.summary.get("added-records", 0),
                "total_records": s.summary.get("total-records", 0),
            }
            for s in table.history()
        ]

    def latest_snapshot_id(self, fqn: str) -> int | None:
        """Return the latest snapshot ID for a table."""
        table = self._resolve_table(fqn)
        snap = table.current_snapshot()
        return snap.snapshot_id if snap else None


# ===========================================================================
# 2. SparkIcebergReader
# ===========================================================================


class SparkIcebergReader:
    """
    Reads Iceberg tables into PySpark DataFrames via the Iceberg Spark runtime.

    Best suited for:
        - Full Silver and Gold table scans
        - Complex joins across multiple Iceberg tables
        - Large-scale historical backfills

    Requires a SparkSession — call get_or_create_spark() before using.
    """

    def __init__(self, spark: "SparkSession") -> None:
        self._spark = spark
        logger.debug("SparkIcebergReader initialised")

    @classmethod
    def from_spark(cls, spark: "SparkSession") -> "SparkIcebergReader":
        return cls(spark)

    def read_table(
        self,
        fqn: str,
        columns: list[str] | None = None,
        filter_expr: str | None = None,
        snapshot_id: int | None = None,
        as_of_timestamp: str | None = None,
    ) -> "SparkDataFrame":
        """
        Read an Iceberg table into a Spark DataFrame.

        Args:
            fqn:              Fully qualified table name
            columns:          Column projection
            filter_expr:      SQL WHERE clause string
            snapshot_id:      Read a specific snapshot (time-travel)
            as_of_timestamp:  Read table as of a timestamp (time-travel)
                              Format: "2024-11-01 00:00:00"

        Example:
            df = reader.read_table(
                TableName.silver("deliveries"),
                filter_expr="match_type = 'T20' AND season = '2024'",
                columns=["match_id", "batter", "runs_batter", "is_wicket"],
            )
        """
        logger.info(
            "Reading Iceberg table (Spark)",
            extra={
                "table": fqn,
                "columns": columns,
                "filter_expr": filter_expr or "",
                "snapshot_id": snapshot_id,
                "as_of_timestamp": as_of_timestamp or "",
            },
        )

        reader = self._spark.read.format("iceberg")

        if snapshot_id:
            reader = reader.option("snapshot-id", snapshot_id)
        if as_of_timestamp:
            reader = reader.option("as-of-timestamp", as_of_timestamp)

        df: SparkDataFrame = reader.load(fqn)

        if columns:
            df = df.select(*columns)
        if filter_expr:
            df = df.filter(filter_expr)

        logger.info(
            "Iceberg read complete (Spark)",
            extra={"table": fqn},
        )
        return df

    def read_incremental(
        self,
        fqn: str,
        start_snapshot_id: int,
        end_snapshot_id: int | None = None,
    ) -> "SparkDataFrame":
        """
        Read only the rows appended between two Iceberg snapshots.
        Uses Iceberg incremental read — more efficient than watermark filtering
        for large append-only Bronze tables.

        Args:
            fqn:               Table FQN
            start_snapshot_id: Exclusive start snapshot
            end_snapshot_id:   Inclusive end snapshot (None = latest)
        """
        reader = self._spark.read.format("iceberg").option("start-snapshot-id", start_snapshot_id)
        if end_snapshot_id:
            reader = reader.option("end-snapshot-id", end_snapshot_id)

        logger.info(
            "Iceberg incremental read (Spark)",
            extra={
                "table": fqn,
                "start_snapshot_id": start_snapshot_id,
                "end_snapshot_id": end_snapshot_id,
            },
        )
        return reader.load(fqn)

    def read_sql(self, query: str) -> "SparkDataFrame":
        """
        Execute a Spark SQL query over Iceberg tables.
        Useful for complex multi-table joins in Silver jobs.

        Example:
            df = reader.read_sql('''
                SELECT d.*, p.identifier as player_key
                FROM cricket.silver.deliveries d
                JOIN cricket.silver.persons p
                  ON d.batter = p.cricsheet_name
                WHERE d.match_type = 'T20'
            ''')
        """
        logger.info("Executing Spark SQL", extra={"query_preview": query[:120]})
        return self._spark.sql(query)

    def table_schema(self, fqn: str) -> list[dict[str, str]]:
        """Return the schema of an Iceberg table as a list of {name, type} dicts."""
        df = self._spark.read.format("iceberg").load(fqn).limit(0)
        return [{"name": f.name, "type": str(f.dataType)} for f in df.schema.fields]


# ===========================================================================
# 3. DuckDBIcebergReader
# ===========================================================================


class DuckDBIcebergReader:
    """
    Reads Iceberg tables via DuckDB's native Iceberg extension.

    Best suited for:
        - Gold layer serving and dbt execution target
        - Fast analytical queries in FastAPI endpoints
        - Notebook exploration

    Requires DuckDB with httpfs + iceberg extensions installed.
    Run infra/bootstrap/init-duckdb.sql once to set these up.
    """

    def __init__(self, conn: "duckdb.DuckDBPyConnection | None" = None) -> None:
        import duckdb

        cfg = get_settings()
        if conn:
            self._conn = conn
        else:
            self._conn = duckdb.connect(str(cfg.duckdb.db_path))
            self._conn.execute(f"SET memory_limit='{cfg.duckdb.memory_limit}'")
            self._conn.execute(f"SET threads={cfg.duckdb.threads}")

        self._configure_s3()
        logger.debug("DuckDBIcebergReader initialised")

    def _configure_s3(self) -> None:
        """Configure DuckDB S3 credentials for MinIO access."""
        cfg = get_settings().storage
        self._conn.execute(f"SET s3_endpoint='{cfg.endpoint.replace('http://', '')}'")
        self._conn.execute(f"SET s3_access_key_id='{cfg.root_user}'")
        self._conn.execute(f"SET s3_secret_access_key='{cfg.root_password.get_secret_value()}'")
        self._conn.execute("SET s3_use_ssl=false")
        self._conn.execute("SET s3_url_style='path'")

    @classmethod
    def from_settings(cls) -> "DuckDBIcebergReader":
        return cls()

    def read_table(
        self,
        iceberg_path: str,
        columns: list[str] | None = None,
        where: str | None = None,
        limit: int | None = None,
    ) -> "pl.DataFrame":
        """
        Read an Iceberg table from MinIO into a Polars DataFrame via DuckDB.

        Args:
            iceberg_path: S3 path to the Iceberg table root
                          e.g. "s3://iceberg-warehouse/gold/fact_delivery/"
            columns:      Column projection (None = all)
            where:        SQL WHERE clause
            limit:        Row limit

        Example:
            df = reader.read_table(
                "s3://iceberg-warehouse/gold/fact_delivery/",
                columns=["match_id", "batter", "runs_batter"],
                where="season = '2024' AND match_type = 'T20'",
                limit=10000,
            )
        """
        import polars as pl

        col_expr = ", ".join(columns) if columns else "*"
        query = f"SELECT {col_expr} FROM iceberg_scan('{iceberg_path}')"
        if where:
            query += f" WHERE {where}"
        if limit:
            query += f" LIMIT {limit}"

        logger.info(
            "DuckDB Iceberg scan",
            extra={"path": iceberg_path, "query_preview": query[:120]},
        )
        result = self._conn.execute(query).arrow()
        return pl.from_arrow(result)

    def execute(self, sql: str) -> "pl.DataFrame":
        """
        Execute arbitrary SQL over registered DuckDB views/tables.
        Used by FastAPI endpoints and AI assistant SQL guardrails.
        """
        import polars as pl

        logger.debug("DuckDB execute", extra={"query_preview": sql[:120]})
        return pl.from_arrow(self._conn.execute(sql).arrow())

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "DuckDBIcebergReader":
        return self

    def __exit__(self, *_) -> None:
        self.close()
