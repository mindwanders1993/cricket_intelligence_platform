# src/cip/transform/spark/silver/powerplays.py
#
# Silver transform: bronze.match_data → silver.match_powerplays
#
# Grain: one row per powerplay window per innings.
# PK: (match_id, innings_number, from_over, type).
# Partition: _snapshot_date.

from __future__ import annotations

from typing import TYPE_CHECKING

from cip.common.contracts.naming import TableName
from cip.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cip.transform.shared.writers import SparkIcebergWriter

logger = get_logger(__name__)

_SILVER_POWERPLAYS = TableName.silver("match_powerplays")


class MatchPowerplaysSilverTransform:
    """
    Builds silver.match_powerplays from parsed.innings[].powerplays[].

    Cricsheet emits one row per declared powerplay window per innings,
    typed as { from: double, to: double, type: string }. Matches without
    powerplay declarations (e.g. Test cricket) contribute zero rows.
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
        innings_exploded = with_innings.select(
            "match_id",
            F.posexplode("innings_array").alias("innings_idx", "innings"),
            "_bronze_loaded_at",
            "_source_file",
            "_source_url",
        )

        # Drop innings without any powerplay declarations before the second
        # explode — avoids surfacing innings as null-only rows.
        with_powerplays = innings_exploded.select(
            "match_id",
            (F.col("innings_idx") + F.lit(1)).alias("innings_number"),
            F.col("innings").getField("powerplays").alias("powerplays_array"),
            "_bronze_loaded_at",
            "_source_file",
            "_source_url",
        ).filter(F.col("powerplays_array").isNotNull() & (F.size("powerplays_array") > 0))

        pp_exploded = with_powerplays.select(
            "match_id",
            "innings_number",
            F.explode("powerplays_array").alias("powerplay"),
            "_bronze_loaded_at",
            "_source_file",
            "_source_url",
        )

        df = pp_exploded.select(
            F.col("match_id"),
            F.col("innings_number"),
            F.col("powerplay").getField("from").alias("from_over"),
            F.col("powerplay").getField("to").alias("to_over"),
            F.col("powerplay").getField("type").alias("type"),
            F.col("_bronze_loaded_at"),
            F.col("_source_file"),
            F.col("_source_url"),
        )

        row_count = df.count()
        self._writer.dynamic_overwrite(
            df=df,
            fqn=_SILVER_POWERPLAYS,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            source_file="all_json.zip",
            partition_cols=["_snapshot_date"],
        )
        logger.info(
            "silver.match_powerplays written",
            extra={"rows": row_count, "snapshot_date": snapshot_date},
        )
        return row_count
