# src/cip/transform/spark/silver/competitions.py
#
# Silver transform: bronze.match_data → silver.competitions
#
# Grain: distinct competition_name (from info.event.name).
# PK: competition_name.
# Partition: _snapshot_date.
#
# Friendlies / unofficial matches have NULL info.event and produce no rows.

from __future__ import annotations

from typing import TYPE_CHECKING

from cip.common.contracts.naming import TableName
from cip.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cip.transform.shared.writers import SparkIcebergWriter

logger = get_logger(__name__)

_SILVER_COMPETITIONS = TableName.silver("competitions")


class CompetitionsSilverTransform:
    """Builds silver.competitions as distinct event names."""

    def __init__(self, spark: "SparkSession", writer: "SparkIcebergWriter") -> None:
        self._spark = spark
        self._writer = writer

    def run(self, bronze_df: "DataFrame", snapshot_date: str, pipeline_run_id: str) -> int:
        from pyspark.sql import functions as F

        df = (
            bronze_df.select(
                F.col("parsed.info.event.name").alias("competition_name"),
                F.col("_bronze_loaded_at"),
            )
            .filter(F.col("competition_name").isNotNull())
            .groupBy("competition_name")
            .agg(F.max("_bronze_loaded_at").alias("_bronze_loaded_at"))
        )

        row_count = df.count()
        self._writer.dynamic_overwrite(
            df=df,
            fqn=_SILVER_COMPETITIONS,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            source_file="all_json.zip",
            partition_cols=["_snapshot_date"],
        )
        logger.info("silver.competitions written", extra={"rows": row_count, "snapshot_date": snapshot_date})
        return row_count
