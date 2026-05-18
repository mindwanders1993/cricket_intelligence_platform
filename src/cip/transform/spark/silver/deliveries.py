# src/cip/transform/spark/silver/deliveries.py
#
# Silver transform: bronze.match_data → silver.deliveries
#
# Grain: one row per ball bowled (legal or extra).
# PK: (match_id, innings_number, over_number, delivery_number).
# Partition: _snapshot_date.

from __future__ import annotations

from typing import TYPE_CHECKING

from cip.common.contracts.naming import TableName
from cip.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cip.transform.shared.writers import SparkIcebergWriter

logger = get_logger(__name__)

_SILVER_DELIVERIES = TableName.silver("deliveries")


class DeliveriesSilverTransform:
    """
    Builds silver.deliveries by exploding parsed.innings[].overs[].deliveries[].

    Edge cases handled:
        - Wides and no-balls produce delivery rows (they're balls, even if
          they don't count toward the over).  Caller must inspect
          extra_wides / extra_noballs to compute legal-ball counts.
        - delivery_number is 1-indexed within each over (position-based).
        - is_wicket = True when wickets[] is non-empty.
    """

    def __init__(self, spark: "SparkSession", writer: "SparkIcebergWriter") -> None:
        self._spark = spark
        self._writer = writer

    def run(self, bronze_df: "DataFrame", snapshot_date: str, pipeline_run_id: str) -> int:
        from pyspark.sql import functions as F

        # Layer 1: explode innings
        l1 = bronze_df.select(
            "match_id",
            F.posexplode("parsed.innings").alias("innings_idx", "innings"),
            "_bronze_loaded_at",
            "_source_file",
            "_source_url",
        ).select(
            "match_id",
            (F.col("innings_idx") + F.lit(1)).alias("innings_number"),
            F.col("innings").getField("overs").alias("overs_array"),
            "_bronze_loaded_at",
            "_source_file",
            "_source_url",
        )

        # Layer 2: explode overs
        l2 = l1.select(
            "match_id",
            "innings_number",
            F.explode("overs_array").alias("over_struct"),
            "_bronze_loaded_at",
            "_source_file",
            "_source_url",
        ).select(
            "match_id",
            "innings_number",
            F.col("over_struct").getField("over").alias("over_number"),
            F.col("over_struct").getField("deliveries").alias("deliveries_array"),
            "_bronze_loaded_at",
            "_source_file",
            "_source_url",
        )

        # Layer 3: explode deliveries with position → delivery_number
        l3 = l2.select(
            "match_id",
            "innings_number",
            "over_number",
            F.posexplode("deliveries_array").alias("delivery_idx", "delivery"),
            "_bronze_loaded_at",
            "_source_file",
            "_source_url",
        )

        runs = F.col("delivery").getField("runs")
        extras = F.col("delivery").getField("extras")
        wickets = F.col("delivery").getField("wickets")

        df = l3.select(
            F.col("match_id"),
            F.col("innings_number"),
            F.col("over_number"),
            (F.col("delivery_idx") + F.lit(1)).alias("delivery_number"),
            F.col("delivery").getField("batter").alias("batter"),
            F.col("delivery").getField("bowler").alias("bowler"),
            F.col("delivery").getField("non_striker").alias("non_striker"),
            F.coalesce(runs.getField("batter"), F.lit(0)).alias("runs_batter"),
            F.coalesce(runs.getField("extras"), F.lit(0)).alias("runs_extras"),
            F.coalesce(runs.getField("total"), F.lit(0)).alias("runs_total"),
            runs.getField("non_boundary").alias("runs_non_boundary"),
            extras.getField("wides").alias("extra_wides"),
            extras.getField("noballs").alias("extra_noballs"),
            extras.getField("byes").alias("extra_byes"),
            extras.getField("legbyes").alias("extra_legbyes"),
            extras.getField("penalty").alias("extra_penalty"),
            (F.size(F.coalesce(wickets, F.array())) > 0).alias("is_wicket"),
            F.col("_bronze_loaded_at"),
            F.col("_source_file"),
            F.col("_source_url"),
        )

        row_count = df.count()
        self._writer.delete_and_insert(
            df=df,
            fqn=_SILVER_DELIVERIES,
            snapshot_date=snapshot_date,
            key_cols=["match_id"],
            pipeline_run_id=pipeline_run_id,
            source_file="all_json.zip",
            partition_cols=["_snapshot_date"],
        )
        logger.info("silver.deliveries written", extra={"rows": row_count, "snapshot_date": snapshot_date})
        return row_count
