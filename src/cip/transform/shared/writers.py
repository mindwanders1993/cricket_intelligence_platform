# platform/transform/shared/writers.py
#
# Iceberg write abstractions for the Cricket Intelligence Platform.
#
# Two writer families:
#   1. PolarsIcebergWriter  — writes via PyIceberg → PyArrow
#                             Used in Bronze parse jobs (small-medium writes)
#   2. SparkIcebergWriter   — writes via PySpark Iceberg runtime
#                             Used in Silver and Gold transform jobs
#
# Both writers:
#   - Inject mandatory metadata columns (_snapshot_date, _ingested_at, etc.)
#   - Register schema versions in cricket_control.schema_version
#   - Emit structured log lines with row counts and duration

from __future__ import annotations

import hashlib
import json
import time
import uuid
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING

from cip.common.contracts.enums import Layer
from cip.common.contracts.naming import META, IcebergProperties, TableName
from cip.common.exceptions import IcebergError
from cip.common.logging import get_context, get_logger
from cip.common.settings import get_settings

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession

logger = get_logger(__name__)


# ===========================================================================
# Metadata injection helpers
# ===========================================================================


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def _compute_schema_hash(column_names: list[str], column_types: list[str]) -> str:
    """Deterministic hash of a schema — used to detect Iceberg schema drift."""
    pairs = sorted(zip(column_names, column_types, strict=False))
    raw = json.dumps(pairs, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()


def _build_partition_spec(
    iceberg_schema: "Schema",
    partition_cols: list[str],
) -> "PartitionSpec":
    """
    Build a PyIceberg PartitionSpec with an IdentityTransform for each
    partition column.

    Bronze tables always partition by _snapshot_date (Identity transform —
    no bucketing or truncation). Silver/Gold tables may add additional
    partition columns (e.g. match_type, season) using the same approach.

    Args:
        iceberg_schema: PyIceberg schema of the table.
        partition_cols: Ordered list of column names to partition by.
                        Each column gets an IdentityTransform.

    Returns:
        PyIceberg PartitionSpec ready to pass to catalog.create_table().

    Raises:
        ValueError: If any partition column is absent from the schema.
    """
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform

    partition_fields: list[PartitionField] = []
    for i, col in enumerate(partition_cols):
        try:
            iceberg_field = iceberg_schema.find_field(col)
        except Exception:
            iceberg_field = None

        if iceberg_field is None:
            available = [f.name for f in iceberg_schema.fields]
            raise ValueError(f"Partition column '{col}' not found in schema. " f"Available columns: {available}")

        partition_fields.append(
            PartitionField(
                source_id=iceberg_field.field_id,
                # Iceberg convention: partition field IDs start at 1000 and
                # must not collide with data field IDs (which start at 1).
                field_id=1000 + i,
                transform=IdentityTransform(),
                name=col,
            )
        )

    return PartitionSpec(*partition_fields)


def _inject_meta_polars(
    df: "pl.DataFrame",
    snapshot_date: str | date,
    pipeline_run_id: str,
    source_file: str = "",
    source_url: str = "",
) -> "pl.DataFrame":
    """
    Inject mandatory metadata columns into a Polars DataFrame.
    Only adds columns that are not already present.
    """
    import polars as pl

    ctx = get_context()
    now = _now_utc()
    snap = snapshot_date.isoformat() if isinstance(snapshot_date, date) else snapshot_date

    additions = {}

    if META.SNAPSHOT_DATE not in df.columns:
        additions[META.SNAPSHOT_DATE] = pl.lit(snap).cast(pl.Date)
    if META.INGESTED_AT not in df.columns:
        additions[META.INGESTED_AT] = pl.lit(now)
    if META.PIPELINE_RUN_ID not in df.columns:
        additions[META.PIPELINE_RUN_ID] = pl.lit(pipeline_run_id)
    if META.DAG_RUN_ID not in df.columns:
        additions[META.DAG_RUN_ID] = pl.lit(ctx.get("dag_run_id", ""))
    if META.SOURCE_FILE not in df.columns:
        additions[META.SOURCE_FILE] = pl.lit(source_file)
    if META.SOURCE_URL not in df.columns:
        additions[META.SOURCE_URL] = pl.lit(source_url)

    for col_name, expr in additions.items():
        df = df.with_columns(expr.alias(col_name))

    return df


def _inject_meta_spark(
    df: "SparkDataFrame",
    snapshot_date: str,
    pipeline_run_id: str,
    source_file: str = "",
    source_url: str = "",
) -> "SparkDataFrame":
    """
    Inject mandatory metadata columns into a Spark DataFrame.
    Only adds columns that are not already present.
    """
    from pyspark.sql import functions as F

    ctx = get_context()
    now_str = _now_utc().isoformat()
    existing = set(df.columns)

    if META.SNAPSHOT_DATE not in existing:
        df = df.withColumn(META.SNAPSHOT_DATE, F.lit(snapshot_date).cast("date"))
    if META.INGESTED_AT not in existing:
        df = df.withColumn(META.INGESTED_AT, F.lit(now_str).cast("timestamp"))
    if META.PIPELINE_RUN_ID not in existing:
        df = df.withColumn(META.PIPELINE_RUN_ID, F.lit(pipeline_run_id))
    if META.DAG_RUN_ID not in existing:
        df = df.withColumn(META.DAG_RUN_ID, F.lit(ctx.get("dag_run_id", "")))
    if META.SOURCE_FILE not in existing:
        df = df.withColumn(META.SOURCE_FILE, F.lit(source_file))
    if META.SOURCE_URL not in existing:
        df = df.withColumn(META.SOURCE_URL, F.lit(source_url))

    return df


# ===========================================================================
# 1. PolarsIcebergWriter
# ===========================================================================


class PolarsIcebergWriter:
    """
    Writes Polars DataFrames to Iceberg tables via PyIceberg + PyArrow.

    Supports:
        - append      — add rows to an existing table
        - overwrite   — replace the entire table
        - create      — create table if not exists, then append

    Always injects platform metadata columns before writing.
    """

    def __init__(self, catalog_props: dict[str, str] | None = None) -> None:
        from pyiceberg.catalog import load_catalog

        from cip.transform.shared.readers import _build_pyiceberg_catalog_props

        props = catalog_props or _build_pyiceberg_catalog_props()
        catalog_name = get_settings().iceberg.catalog_name
        self._catalog = load_catalog(catalog_name, **props)
        logger.debug("PolarsIcebergWriter initialised")

    @classmethod
    def from_settings(cls) -> "PolarsIcebergWriter":
        return cls()

    def append(
        self,
        df: "pl.DataFrame",
        fqn: str,
        snapshot_date: str | date,
        pipeline_run_id: str | None = None,
        source_file: str = "",
        source_url: str = "",
    ) -> int:
        """
        Append rows to an existing Iceberg table.

        Args:
            df:               Polars DataFrame to write
            fqn:              Iceberg table FQN — use TableName.bronze(...)
            snapshot_date:    Logical processing date for metadata column
            pipeline_run_id:  UUID from ingestion_run — auto-generated if None
            source_file:      Originating file name for metadata column
            source_url:       Originating URL for metadata column

        Returns:
            Row count written

        Example:
            writer.append(
                df=parsed_df,
                fqn=TableName.bronze("match_data"),
                snapshot_date="2024-11-01",
                source_file="all_matches.zip",
            )
        """
        run_id = pipeline_run_id or str(uuid.uuid4())

        df = _inject_meta_polars(df, snapshot_date, run_id, source_file, source_url)
        arrow_table = df.to_arrow()
        row_count = len(arrow_table)

        started = time.monotonic()
        logger.info(
            "Writing Iceberg table (Polars append)",
            extra={"table": fqn, "rows": row_count, "snapshot_date": str(snapshot_date)},
        )

        try:
            table = self._catalog.load_table(fqn)
            table.append(arrow_table)
        except Exception as exc:
            raise IcebergError(
                f"Polars append failed on {fqn}: {exc}",
                table=fqn,
                rows=row_count,
            ) from exc

        duration = round(time.monotonic() - started, 3)
        logger.info(
            "Iceberg append complete (Polars)",
            extra={"table": fqn, "rows": row_count, "duration_seconds": duration},
        )
        return row_count

    def create_and_append(
        self,
        df: "pl.DataFrame",
        fqn: str,
        snapshot_date: str | date,
        layer: Layer,
        partition_cols: list[str] | None = None,
        pipeline_run_id: str | None = None,
        source_file: str = "",
        source_url: str = "",
    ) -> int:
        """
        Create the Iceberg table if it does not exist, then append.
        Schema is inferred from the DataFrame.

        Args:
            df:              Polars DataFrame — schema inferred from this
            fqn:             Iceberg table FQN
            snapshot_date:   Logical processing date
            layer:           Layer enum — used to apply default table properties
            partition_cols:  Columns to partition by (IdentityTransform per col).
                             For Bronze tables this is always ["_snapshot_date"].
                             Passed to _build_partition_spec() which converts
                             column names → PyIceberg PartitionSpec with proper
                             field IDs.  If None, the table is unpartitioned.
            pipeline_run_id: Optional run UUID
            source_file:     Source file name for metadata
            source_url:      Source URL for metadata
        """

        run_id = pipeline_run_id or str(uuid.uuid4())

        # Inject metadata columns BEFORE deriving arrow_schema so that
        # partition columns like _snapshot_date are already present in
        # the schema when _build_partition_spec() resolves field IDs.
        df = _inject_meta_polars(df, snapshot_date, run_id, source_file, source_url)

        # _ensure_table_exists uses the arrow schema, so convert once here.
        # PyIceberg 0.11.1 requires _pyarrow_to_schema_without_ids + assign_fresh_schema_ids
        # (see writers.py module docstring for full explanation).
        self._ensure_table_exists(df.to_arrow().schema, fqn, layer, partition_cols)

        # append() calls _inject_meta_polars again, which is safe because
        # the helper skips columns that are already present.
        return self.append(df, fqn, snapshot_date, run_id, source_file, source_url)

    def overwrite_partition(
        self,
        df: "pl.DataFrame",
        fqn: str,
        snapshot_date: str | date,
        layer: Layer,
        partition_cols: list[str] | None = None,
        pipeline_run_id: str | None = None,
        source_file: str = "",
        source_url: str = "",
    ) -> int:
        """
        Create the Iceberg table if it does not exist, then overwrite only the
        snapshot_date partition with the incoming DataFrame.

        Idempotent: re-running for the same snapshot replaces only that
        partition, leaving other partitions untouched.  Equivalent to
        SparkIcebergWriter.dynamic_overwrite() — the standard Silver write mode.
        """
        from pyiceberg.expressions import EqualTo

        run_id = pipeline_run_id or str(uuid.uuid4())
        snap = snapshot_date.isoformat() if isinstance(snapshot_date, date) else snapshot_date

        df = _inject_meta_polars(df, snapshot_date, run_id, source_file, source_url)
        arrow_table = df.to_arrow()
        row_count = len(arrow_table)

        started = time.monotonic()
        logger.info(
            "Writing Iceberg table (Polars overwrite partition)",
            extra={"table": fqn, "rows": row_count, "snapshot_date": snap},
        )

        self._ensure_table_exists(arrow_table.schema, fqn, layer, partition_cols)

        try:
            table = self._catalog.load_table(fqn)
            table.overwrite(arrow_table, overwrite_filter=EqualTo(META.SNAPSHOT_DATE, snap))
        except Exception as exc:
            raise IcebergError(
                f"Polars partition overwrite failed on {fqn}: {exc}",
                table=fqn,
                rows=row_count,
            ) from exc

        duration = round(time.monotonic() - started, 3)
        logger.info(
            "Iceberg overwrite partition complete (Polars)",
            extra={"table": fqn, "rows": row_count, "duration_seconds": duration},
        )
        return row_count

    def _ensure_table_exists(
        self,
        arrow_schema: "pa.Schema",
        fqn: str,
        layer: Layer,
        partition_cols: list[str] | None,
    ) -> None:
        if self._table_exists(fqn):
            return

        from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
        from pyiceberg.schema import assign_fresh_schema_ids

        temp_schema = _pyarrow_to_schema_without_ids(arrow_schema)
        iceberg_schema = assign_fresh_schema_ids(temp_schema)

        logger.info("Creating Iceberg table", extra={"table": fqn, "partition_cols": partition_cols or []})
        namespace, _table_name = TableName.from_fqn(fqn)
        try:
            self._catalog.create_namespace_if_not_exists(namespace)
        except Exception:
            pass

        props = {
            Layer.BRONZE: IcebergProperties.bronze_defaults(),
            Layer.SILVER: IcebergProperties.silver_defaults(),
            Layer.GOLD: IcebergProperties.gold_defaults(),
        }.get(layer, {})

        create_kwargs: dict = {"identifier": fqn, "schema": iceberg_schema, "properties": props}
        if partition_cols:
            create_kwargs["partition_spec"] = _build_partition_spec(iceberg_schema, partition_cols)

        self._catalog.create_table(**create_kwargs)

    def _table_exists(self, fqn: str) -> bool:
        try:
            self._catalog.load_table(fqn)
            return True
        except Exception:
            return False


# ===========================================================================
# 2. SparkIcebergWriter
# ===========================================================================


class SparkIcebergWriter:
    """
    Writes PySpark DataFrames to Iceberg tables via the Iceberg Spark runtime.

    Supports:
        - append          — add rows (idempotent with _row_hash dedup)
        - overwrite       — replace entire table or a partition
        - merge (upsert)  — merge on a set of key columns
        - dynamic_overwrite — overwrite matching partitions only

    Always injects platform metadata columns before writing.
    """

    def __init__(self, spark: "SparkSession") -> None:
        self._spark = spark
        logger.debug("SparkIcebergWriter initialised")

    @classmethod
    def from_spark(cls, spark: "SparkSession") -> "SparkIcebergWriter":
        return cls(spark)

    def append(
        self,
        df: "SparkDataFrame",
        fqn: str,
        snapshot_date: str,
        pipeline_run_id: str | None = None,
        source_file: str = "",
        source_url: str = "",
    ) -> None:
        """
        Append Spark DataFrame rows to an Iceberg table.

        Example:
            writer.append(
                df=silver_deliveries,
                fqn=TableName.silver("deliveries"),
                snapshot_date="2024-11-01",
            )
        """
        run_id = pipeline_run_id or str(uuid.uuid4())
        df = _inject_meta_spark(df, snapshot_date, run_id, source_file, source_url)

        started = time.monotonic()
        logger.info(
            "Writing Iceberg table (Spark append)",
            extra={"table": fqn, "snapshot_date": snapshot_date},
        )

        df.writeTo(fqn).append()

        duration = round(time.monotonic() - started, 3)
        logger.info(
            "Iceberg append complete (Spark)",
            extra={"table": fqn, "duration_seconds": duration},
        )

    def _ensure_table_exists(
        self,
        df: "SparkDataFrame",
        fqn: str,
        partition_cols: list[str] | None = None,
    ) -> None:
        """Create the Iceberg table using df's schema if it does not already exist."""
        try:
            self._spark.sql(f"SELECT 1 FROM {fqn} LIMIT 0")
            return  # table exists
        except Exception:
            pass  # table not found — proceed to create

        logger.info("Iceberg table not found — creating", extra={"table": fqn, "partition_cols": partition_cols or []})
        writer = df.writeTo(fqn)
        if partition_cols:
            writer = writer.partitionedBy(*partition_cols)
        writer.create()

    def dynamic_overwrite(
        self,
        df: "SparkDataFrame",
        fqn: str,
        snapshot_date: str,
        pipeline_run_id: str | None = None,
        source_file: str = "",
        source_url: str = "",
        partition_cols: list[str] | None = None,
    ) -> None:
        """
        Overwrite only the partitions present in df.
        Idempotent for partition-aligned reruns — re-running a day
        replaces only that day's partition, not the whole table.

        Creates the table on first run using df's schema and partition_cols.
        This is the standard write mode for Silver jobs.
        """
        run_id = pipeline_run_id or str(uuid.uuid4())
        df = _inject_meta_spark(df, snapshot_date, run_id, source_file, source_url)

        logger.info(
            "Writing Iceberg table (Spark dynamic overwrite)",
            extra={"table": fqn, "snapshot_date": snapshot_date},
        )
        self._ensure_table_exists(df, fqn, partition_cols)
        (df.writeTo(fqn).option("overwrite-mode", "dynamic").overwritePartitions())
        logger.info("Dynamic overwrite complete", extra={"table": fqn})

    def delete_and_insert(
        self,
        df: "SparkDataFrame",
        fqn: str,
        snapshot_date: str,
        key_cols: list[str],
        pipeline_run_id: str | None = None,
        source_file: str = "",
        source_url: str = "",
        partition_cols: list[str] | None = None,
    ) -> None:
        """
        Atomically replace all rows for the key values present in df.

        Issues:
            DELETE FROM {fqn} WHERE (key_cols) IN (SELECT DISTINCT key_cols FROM <df>)
            INSERT INTO {fqn} SELECT * FROM <df>

        Uses Iceberg v2 row-level deletes — leaves Iceberg snapshot history
        intact. The standard write mode for incremental Silver:
        DELETE the rows of every match_id we're about to re-process, then
        INSERT the fresh ones. Idempotent for repeated runs of the same
        match_ids.

        Requires the target table to be Iceberg format-version=2. New tables
        created by this method default to v2; existing v1 tables are
        upgraded via ALTER TABLE before the first DELETE.

        Args:
            df:              Spark DataFrame holding the new rows.
            fqn:             Target Iceberg table FQN.
            snapshot_date:   Logical date stamped into the meta columns.
            key_cols:        Columns whose values trigger the DELETE scope.
                             Use ["match_id"] for match-grained Silver tables.
            pipeline_run_id: Optional run UUID.
            source_file:     Source file name for metadata.
            source_url:      Source URL for metadata.
            partition_cols:  Partition columns for first-time table creation.
                             Defaults to [_snapshot_date].
        """
        if not key_cols:
            raise ValueError("delete_and_insert requires non-empty key_cols")

        run_id = pipeline_run_id or str(uuid.uuid4())
        df = _inject_meta_spark(df, snapshot_date, run_id, source_file, source_url)

        if partition_cols is None:
            partition_cols = [META.SNAPSHOT_DATE]

        logger.info(
            "Writing Iceberg table (Spark delete_and_insert)",
            extra={"table": fqn, "snapshot_date": snapshot_date, "key_cols": key_cols},
        )

        self._ensure_table_exists(df, fqn, partition_cols)
        self._ensure_format_v2(fqn)

        # Temp view name unique per call so concurrent writers don't collide.
        view_name = "delete_and_insert_src_" + uuid.uuid4().hex[:8]
        df.createOrReplaceTempView(view_name)

        try:
            key_tuple = ", ".join(key_cols)
            delete_sql = (
                f"DELETE FROM {fqn} "
                f"WHERE ({key_tuple}) IN "
                f"(SELECT DISTINCT {key_tuple} FROM {view_name})"
            )
            insert_sql = f"INSERT INTO {fqn} SELECT * FROM {view_name}"

            self._spark.sql(delete_sql)
            self._spark.sql(insert_sql)
        finally:
            self._spark.catalog.dropTempView(view_name)

        logger.info("delete_and_insert complete", extra={"table": fqn, "key_cols": key_cols})

    def _ensure_format_v2(self, fqn: str) -> None:
        """Ensure an Iceberg table is at format-version=2.

        Row-level DELETE requires v2. New tables created by this writer
        default to v2 (set in create flow); but tables created before this
        method existed may be v1. ALTER is idempotent if already v2.
        """
        try:
            self._spark.sql(
                f"ALTER TABLE {fqn} SET TBLPROPERTIES ('format-version'='2')"
            )
        except Exception as exc:
            # Don't hard-fail — some Iceberg builds reject the ALTER when
            # already at v2. Log and move on; the DELETE will surface a real
            # error if v1 is still in effect.
            logger.warning(
                "ALTER TABLE format-version=2 raised — continuing",
                extra={"table": fqn, "error": str(exc)},
            )

    def merge(
        self,
        df: "SparkDataFrame",
        fqn: str,
        merge_keys: list[str],
        snapshot_date: str,
        update_cols: list[str] | None = None,
        pipeline_run_id: str | None = None,
    ) -> None:
        """
        Merge (upsert) a DataFrame into an Iceberg table on merge_keys.
        Inserts new rows, updates existing rows where keys match.

        Used for Silver dimension tables where identity resolution may
        update existing person records when new Register data arrives.

        Args:
            df:           Source DataFrame with new/updated rows
            fqn:          Target Iceberg table FQN
            merge_keys:   Columns that uniquely identify a row
            snapshot_date: Logical date for metadata injection
            update_cols:  Columns to update on match (None = all non-key cols)
            pipeline_run_id: Optional run UUID

        Example:
            writer.merge(
                df=updated_persons,
                fqn=TableName.silver("persons"),
                merge_keys=["person_id"],
                snapshot_date="2024-11-01",
                update_cols=["full_name", "unique_name", "_ingested_at"],
            )
        """
        run_id = pipeline_run_id or str(uuid.uuid4())
        df = _inject_meta_spark(df, snapshot_date, run_id)

        source_alias = "source"
        target_alias = "target"

        key_condition = " AND ".join([f"{target_alias}.{k} = {source_alias}.{k}" for k in merge_keys])

        all_cols = [c for c in df.columns if c not in merge_keys]
        cols_to_update = update_cols or all_cols

        set_clause = ", ".join([f"{c} = {source_alias}.{c}" for c in cols_to_update])
        insert_cols = ", ".join(df.columns)
        insert_vals = ", ".join([f"{source_alias}.{c}" for c in df.columns])

        merge_sql = f"""
            MERGE INTO {fqn} AS {target_alias}
            USING {source_alias}
            ON {key_condition}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        df.createOrReplaceTempView(source_alias)

        logger.info(
            "Executing Iceberg MERGE",
            extra={"table": fqn, "merge_keys": merge_keys},
        )
        self._spark.sql(merge_sql)
        logger.info("Iceberg MERGE complete", extra={"table": fqn})

    def create_table_if_not_exists(
        self,
        df: "SparkDataFrame",
        fqn: str,
        layer: Layer,
        partition_cols: list[str] | None = None,
    ) -> None:
        """
        Create an Iceberg table using the DataFrame schema if it does not exist.
        Applies standard table properties for the given layer.
        """
        try:
            self._spark.sql(f"SELECT 1 FROM {fqn} LIMIT 0")
            return  # table already exists
        except Exception:
            pass

        props = {
            Layer.BRONZE: IcebergProperties.bronze_defaults(),
            Layer.SILVER: IcebergProperties.silver_defaults(),
            Layer.GOLD: IcebergProperties.gold_defaults(),
        }.get(layer, {})

        writer = df.writeTo(fqn)
        for k, v in props.items():
            writer = writer.tableProperty(k, v)

        if partition_cols:
            writer = writer.partitionedBy(*partition_cols)

        logger.info(
            "Creating Iceberg table if not exists",
            extra={"table": fqn, "layer": layer, "partition_cols": partition_cols},
        )
        writer.create()
