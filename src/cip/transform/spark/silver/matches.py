# src/cip/transform/spark/silver/matches.py
#
# Silver transform: bronze.match_data → silver.matches
#
# Grain: one row per match (MAX revision).
# Partition: _snapshot_date, match_type.

from __future__ import annotations

from typing import TYPE_CHECKING

from cip.common.contracts.naming import TableName
from cip.common.logging import get_logger
from cip.transform.spark.silver.normalize import season_to_string

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cip.transform.shared.writers import SparkIcebergWriter

logger = get_logger(__name__)

_SILVER_MATCHES = TableName.silver("matches")


class MatchesSilverTransform:
    """
    Builds silver.matches by projecting typed top-level fields from each
    Cricsheet match document.

    Edge cases handled:
        - Missing info.event → event_name/event_number = NULL
        - Missing outcome.by → margin columns = NULL (tie / no-result / draw)
        - Multi-day Tests → match_date = first date in info.dates
        - Tie or no-result → outcome_result preserved; winner may be NULL
    """

    def __init__(self, spark: "SparkSession", writer: "SparkIcebergWriter") -> None:
        self._spark = spark
        self._writer = writer

    def run(self, bronze_df: "DataFrame", snapshot_date: str, pipeline_run_id: str) -> int:
        from pyspark.sql import functions as F

        info = F.col("parsed.info")
        outcome = info.getField("outcome")
        by = outcome.getField("by")
        event = info.getField("event")
        toss = info.getField("toss")
        teams = info.getField("teams")
        dates = info.getField("dates")

        df = bronze_df.select(
            F.col("match_id"),
            season_to_string(F.coalesce(info.getField("season"), F.col("bronze_season"))).alias("season"),
            F.coalesce(info.getField("match_type"), F.col("bronze_match_type")).alias("match_type"),
            F.coalesce(info.getField("gender"), F.col("bronze_gender")).alias("gender"),
            F.coalesce(dates.getItem(0), F.col("bronze_match_date")).cast("date").alias("match_date"),
            F.coalesce(teams.getItem(0), F.col("bronze_team_a")).alias("team_a"),
            F.coalesce(teams.getItem(1), F.col("bronze_team_b")).alias("team_b"),
            F.coalesce(info.getField("venue"), F.col("bronze_venue")).alias("venue"),
            F.coalesce(info.getField("city"), F.col("bronze_city")).alias("city"),
            info.getField("balls_per_over").alias("balls_per_over"),
            info.getField("overs").alias("limit_overs"),
            event.getField("name").alias("event_name"),
            event.getField("match_number").alias("event_number"),
            toss.getField("winner").alias("toss_winner"),
            toss.getField("decision").alias("toss_decision"),
            outcome.getField("winner").alias("winner"),
            outcome.getField("result").alias("outcome_result"),
            outcome.getField("method").alias("outcome_method"),
            by.getField("runs").alias("win_by_runs"),
            by.getField("wickets").alias("win_by_wickets"),
            by.getField("innings").alias("win_by_innings"),
            info.getField("player_of_match").alias("player_of_match"),
            F.col("_bronze_loaded_at"),
            F.col("_source_file"),
            F.col("_source_url"),
        )

        row_count = df.count()
        self._writer.delete_and_insert(
            df=df,
            fqn=_SILVER_MATCHES,
            snapshot_date=snapshot_date,
            key_cols=["match_id"],
            pipeline_run_id=pipeline_run_id,
            source_file="all_json.zip",
            partition_cols=["_snapshot_date", "match_type"],
        )
        logger.info("silver.matches written", extra={"rows": row_count, "snapshot_date": snapshot_date})
        return row_count
