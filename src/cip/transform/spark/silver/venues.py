# src/cip/transform/spark/silver/venues.py
#
# Silver transform: bronze.match_data → silver.venues
#
# Grain: distinct (venue_name, city).  PK: (venue_name, city).
# Partition: _snapshot_date.

from __future__ import annotations

from typing import TYPE_CHECKING

from cip.common.contracts.naming import TableName
from cip.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cip.transform.shared.writers import SparkIcebergWriter

logger = get_logger(__name__)

_SILVER_VENUES = TableName.silver("venues")


class VenuesSilverTransform:
    """
    Builds silver.venues as distinct (venue_name, city).

    Cricsheet venue spellings drift slightly over time — canonicalisation
    is deferred to the Gold dim_venue model.
    """

    def __init__(self, spark: "SparkSession", writer: "SparkIcebergWriter") -> None:
        self._spark = spark
        self._writer = writer

    def run(self, bronze_df: "DataFrame", snapshot_date: str, pipeline_run_id: str) -> int:
        from pyspark.sql import functions as F

        df = (
            bronze_df.select(
                F.col("parsed.info.venue").alias("venue_name"),
                F.col("parsed.info.city").alias("city"),
                F.col("_bronze_loaded_at"),
            )
            .filter(F.col("venue_name").isNotNull())
            .groupBy("venue_name", "city")
            .agg(F.max("_bronze_loaded_at").alias("_bronze_loaded_at"))
        )

        row_count = df.count()
        self._writer.dynamic_overwrite(
            df=df,
            fqn=_SILVER_VENUES,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            source_file="all_json.zip",
            partition_cols=["_snapshot_date"],
        )
        logger.info("silver.venues written", extra={"rows": row_count, "snapshot_date": snapshot_date})
        return row_count
