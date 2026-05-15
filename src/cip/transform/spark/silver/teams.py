# src/cip/transform/spark/silver/teams.py
#
# Silver transform: bronze.match_data → silver.teams
#
# Grain: distinct team_name.  PK: team_name.
# Partition: _snapshot_date.

from __future__ import annotations

from typing import TYPE_CHECKING

from cip.common.contracts.naming import TableName
from cip.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cip.transform.shared.writers import SparkIcebergWriter

logger = get_logger(__name__)

_SILVER_TEAMS = TableName.silver("teams")


class TeamsSilverTransform:
    """
    Builds silver.teams as the distinct set of team names seen across all
    matches in the snapshot.  team_type is carried where available.
    """

    def __init__(self, spark: "SparkSession", writer: "SparkIcebergWriter") -> None:
        self._spark = spark
        self._writer = writer

    def run(self, bronze_df: "DataFrame", snapshot_date: str, pipeline_run_id: str) -> int:
        from pyspark.sql import functions as F

        df = (
            bronze_df.select(
                F.explode("parsed.info.teams").alias("team_name"),
                F.col("parsed.info.team_type").alias("team_type"),
                F.col("_bronze_loaded_at"),
            )
            .filter(F.col("team_name").isNotNull())
            .groupBy("team_name")
            .agg(
                F.max("team_type").alias("team_type"),
                F.max("_bronze_loaded_at").alias("_bronze_loaded_at"),
            )
        )

        row_count = df.count()
        self._writer.dynamic_overwrite(
            df=df,
            fqn=_SILVER_TEAMS,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            source_file="all_json.zip",
            partition_cols=["_snapshot_date"],
        )
        logger.info("silver.teams written", extra={"rows": row_count, "snapshot_date": snapshot_date})
        return row_count
