# src/cip/transform/spark/silver/wickets.py
#
# Silver transform: bronze.match_data → silver.wickets
#
# Grain: one row per wicket fallen.
# PK: (match_id, innings_number, over_number, delivery_number, player_out).
# Partition: _snapshot_date.
#
# IMPORTANT: `player_out` is the authoritative dismissal subject —
# NOT `batter`.  On run-outs the player_out may be the non-striker.

from __future__ import annotations

from typing import TYPE_CHECKING

from cip.common.contracts.naming import TableName
from cip.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cip.transform.shared.writers import SparkIcebergWriter

logger = get_logger(__name__)

_SILVER_WICKETS = TableName.silver("wickets")


class WicketsSilverTransform:
    """
    Builds silver.wickets by exploding parsed.innings[].overs[].deliveries[].wickets[].

    Edge cases handled:
        - Run-outs where player_out != batter (we trust player_out).
        - Substitute fielders preserved via fielders[].substitute (we
          flatten fielders to an array of names; the boolean flag is
          dropped in this first pass — fielders[] grain is conserved).
        - A single delivery may produce two wickets (run-out + caught).
    """

    def __init__(self, spark: "SparkSession", writer: "SparkIcebergWriter") -> None:
        self._spark = spark
        self._writer = writer

    def run(self, bronze_df: "DataFrame", snapshot_date: str, pipeline_run_id: str) -> int:
        from pyspark.sql import functions as F

        # Layer 1: innings
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

        # Layer 2: overs
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

        # Layer 3: deliveries (only keep those with wickets[])
        l3 = (
            l2.select(
                "match_id",
                "innings_number",
                "over_number",
                F.posexplode("deliveries_array").alias("delivery_idx", "delivery"),
                "_bronze_loaded_at",
                "_source_file",
                "_source_url",
            )
            .withColumn("delivery_number", F.col("delivery_idx") + F.lit(1))
            .withColumn("wickets_array", F.col("delivery").getField("wickets"))
            .filter(F.size(F.coalesce(F.col("wickets_array"), F.array())) > 0)
        )

        # Layer 4: wickets — explode the wickets array
        l4 = l3.select(
            "match_id",
            "innings_number",
            "over_number",
            "delivery_number",
            F.explode("wickets_array").alias("wicket"),
            "_bronze_loaded_at",
            "_source_file",
            "_source_url",
        )

        df = l4.select(
            F.col("match_id"),
            F.col("innings_number"),
            F.col("over_number"),
            F.col("delivery_number"),
            F.col("wicket").getField("player_out").alias("player_out"),
            F.col("wicket").getField("kind").alias("kind"),
            # Flatten fielders[] structs → array<string> of fielder names.
            F.transform(
                F.coalesce(F.col("wicket").getField("fielders"), F.array()),
                lambda fielder: fielder.getField("name"),
            ).alias("fielders"),
            F.col("_bronze_loaded_at"),
            F.col("_source_file"),
            F.col("_source_url"),
        )

        row_count = df.count()
        self._writer.dynamic_overwrite(
            df=df,
            fqn=_SILVER_WICKETS,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            source_file="all_json.zip",
            partition_cols=["_snapshot_date"],
        )
        logger.info("silver.wickets written", extra={"rows": row_count, "snapshot_date": snapshot_date})
        return row_count
