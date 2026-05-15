# src/cip/transform/spark/silver/innings.py
#
# Silver transform: bronze.match_data → silver.innings
#
# Grain: one row per innings.  PK: (match_id, innings_number).
# Innings number is 1-indexed by position in parsed.innings.
# Partition: _snapshot_date.

from __future__ import annotations

from typing import TYPE_CHECKING

from cip.common.contracts.naming import TableName
from cip.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cip.transform.shared.writers import SparkIcebergWriter

logger = get_logger(__name__)

_SILVER_INNINGS = TableName.silver("innings")


class InningsSilverTransform:
    """
    Builds silver.innings.

    Edge cases handled:
        - Super Overs surface as their own innings rows with super_over=True.
        - Test forfeitures preserve declared/forfeited flags.
        - Run-chase target preserves both target_runs and target_overs.
    """

    def __init__(self, spark: "SparkSession", writer: "SparkIcebergWriter") -> None:
        self._spark = spark
        self._writer = writer

    def run(self, bronze_df: "DataFrame", snapshot_date: str, pipeline_run_id: str) -> int:
        from pyspark.sql import functions as F

        with_innings = bronze_df.select(
            F.col("match_id"),
            F.col("parsed.innings").alias("innings_array"),
            F.col("_bronze_loaded_at"),
            F.col("_source_file"),
            F.col("_source_url"),
        )

        # posexplode emits (pos, col) — pos is 0-indexed, we want 1-indexed.
        exploded = with_innings.select(
            "match_id",
            F.posexplode("innings_array").alias("innings_idx", "innings"),
            "_bronze_loaded_at",
            "_source_file",
            "_source_url",
        )

        df = exploded.select(
            F.col("match_id"),
            (F.col("innings_idx") + F.lit(1)).alias("innings_number"),
            F.col("innings").getField("team").alias("team"),
            F.coalesce(F.col("innings").getField("super_over"), F.lit(False)).alias("super_over"),
            F.col("innings").getField("declared").alias("declared"),
            F.col("innings").getField("forfeited").alias("forfeited"),
            F.col("innings").getField("target").getField("runs").alias("target_runs"),
            F.col("innings").getField("target").getField("overs").alias("target_overs"),
            F.col("_bronze_loaded_at"),
            F.col("_source_file"),
            F.col("_source_url"),
        )

        row_count = df.count()
        self._writer.dynamic_overwrite(
            df=df,
            fqn=_SILVER_INNINGS,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            source_file="all_json.zip",
            partition_cols=["_snapshot_date"],
        )
        logger.info("silver.innings written", extra={"rows": row_count, "snapshot_date": snapshot_date})
        return row_count
