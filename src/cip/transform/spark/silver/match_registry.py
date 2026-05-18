# src/cip/transform/spark/silver/match_registry.py
#
# Silver transform: bronze.match_data → silver.match_registry
#
# Grain: one row per (match_id, display_name).
# PK: (match_id, display_name).
# Partition: _snapshot_date.
#
# Source: info.registry.people — a MapType<String, String> in the match JSON
# where keys are display names (players + officials) and values are
# Cricsheet IDs. This table is the authoritative per-match cricsheet_id
# lookup that Path A identity resolution joins against.

from __future__ import annotations

from typing import TYPE_CHECKING

from cip.common.contracts.naming import TableName
from cip.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cip.transform.shared.writers import SparkIcebergWriter

logger = get_logger(__name__)

_SILVER_MATCH_REGISTRY = TableName.silver("match_registry")


class MatchRegistrySilverTransform:
    """
    Builds silver.match_registry by exploding info.registry.people per match.

    Many matches carry a sparse registry (only some players have cricsheet_ids).
    Matches whose registry map is null or empty contribute zero rows.
    """

    def __init__(self, spark: "SparkSession", writer: "SparkIcebergWriter") -> None:
        self._spark = spark
        self._writer = writer

    def run(self, bronze_df: "DataFrame", snapshot_date: str, pipeline_run_id: str) -> int:
        from pyspark.sql import functions as F

        with_registry = bronze_df.select(
            F.col("match_id"),
            F.col("parsed.info.registry.people").alias("registry_map"),
            F.col("_bronze_loaded_at"),
            F.col("_source_file"),
            F.col("_source_url"),
        ).filter(F.col("registry_map").isNotNull() & (F.size(F.map_keys("registry_map")) > 0))

        exploded = with_registry.select(
            "match_id",
            F.explode(F.map_entries("registry_map")).alias("entry"),
            "_bronze_loaded_at",
            "_source_file",
            "_source_url",
        )

        df = exploded.select(
            F.col("match_id"),
            F.col("entry").getField("key").alias("display_name"),
            F.col("entry").getField("value").alias("cricsheet_id"),
            F.col("_bronze_loaded_at"),
            F.col("_source_file"),
            F.col("_source_url"),
        ).dropDuplicates(["match_id", "display_name"])

        row_count = df.count()
        self._writer.delete_and_insert(
            df=df,
            fqn=_SILVER_MATCH_REGISTRY,
            snapshot_date=snapshot_date,
            key_cols=["match_id"],
            pipeline_run_id=pipeline_run_id,
            source_file="all_json.zip",
            partition_cols=["_snapshot_date"],
        )
        logger.info(
            "silver.match_registry written",
            extra={"rows": row_count, "snapshot_date": snapshot_date},
        )
        return row_count
