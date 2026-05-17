# src/cip/transform/spark/silver/pipeline.py
#
# Orchestrator for the Match Silver build.
#
# Runs every entity transform in dependency order against a single
# cached Bronze DataFrame.  Used by the build_silver_match_data job.
#
# Dependency order:
#   1. silver.matches
#   2. silver.teams, silver.venues, silver.competitions  (parallel-safe)
#   3. silver.innings
#   4. silver.match_powerplays
#   5. silver.deliveries
#   6. silver.wickets
#   7. silver.match_registry      (needed by players + officials Path A)
#   8. silver.match_players       (also needs silver.person_identifiers + name_variations)
#   9. silver.match_officials     (also needs silver.name_variations)
#  10. silver.unmatched_persons_audit (consumes unmatched dfs from 8 + 9)

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from cip.common.logging import get_logger
from cip.transform.spark.silver.bronze_reader import read_bronze_matches
from cip.transform.spark.silver.competitions import CompetitionsSilverTransform
from cip.transform.spark.silver.deliveries import DeliveriesSilverTransform
from cip.transform.spark.silver.innings import InningsSilverTransform
from cip.transform.spark.silver.match_registry import MatchRegistrySilverTransform
from cip.transform.spark.silver.matches import MatchesSilverTransform
from cip.transform.spark.silver.officials import MatchOfficialsSilverTransform
from cip.transform.spark.silver.players import MatchPlayersSilverTransform
from cip.transform.spark.silver.powerplays import MatchPowerplaysSilverTransform
from cip.transform.spark.silver.teams import TeamsSilverTransform
from cip.transform.spark.silver.unmatched_audit import UnmatchedPersonsAuditSilverTransform
from cip.transform.spark.silver.venues import VenuesSilverTransform
from cip.transform.spark.silver.wickets import WicketsSilverTransform

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = get_logger(__name__)


@dataclass
class MatchSilverResult:
    matches_rows: int = 0
    innings_rows: int = 0
    deliveries_rows: int = 0
    wickets_rows: int = 0
    teams_rows: int = 0
    venues_rows: int = 0
    competitions_rows: int = 0
    match_players_rows: int = 0
    match_officials_rows: int = 0
    match_powerplays_rows: int = 0
    match_registry_rows: int = 0
    unmatched_persons_audit_rows: int = 0
    tables_run: list[str] = field(default_factory=list)

    @property
    def total_rows(self) -> int:
        return (
            self.matches_rows
            + self.innings_rows
            + self.deliveries_rows
            + self.wickets_rows
            + self.teams_rows
            + self.venues_rows
            + self.competitions_rows
            + self.match_players_rows
            + self.match_officials_rows
            + self.match_powerplays_rows
            + self.match_registry_rows
            + self.unmatched_persons_audit_rows
        )


class MatchSilverPipeline:
    """
    Coordinates the Match Silver build.  All transforms share a single
    cached Bronze DataFrame for cost efficiency.
    """

    def __init__(self, spark: "SparkSession") -> None:
        from cip.transform.shared.writers import SparkIcebergWriter

        self._spark = spark
        self._writer = SparkIcebergWriter.from_spark(spark)

    @classmethod
    def from_spark(cls, spark: "SparkSession") -> "MatchSilverPipeline":
        return cls(spark)

    def run_all(self, snapshot_date: str, pipeline_run_id: str) -> MatchSilverResult:
        """
        Run every Match Silver transform in dependency order.

        Args:
            snapshot_date:   ISO date for the Silver write partition.
            pipeline_run_id: Airflow run_id or manual UUID.

        Returns:
            MatchSilverResult with row counts per target table.
        """
        logger.info(
            "MatchSilverPipeline.run_all starting",
            extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id},
        )

        bronze_df = read_bronze_matches(self._spark, snapshot_date)

        result = MatchSilverResult()
        try:
            result.matches_rows = MatchesSilverTransform(self._spark, self._writer).run(
                bronze_df, snapshot_date, pipeline_run_id
            )
            result.tables_run.append("matches")

            result.teams_rows = TeamsSilverTransform(self._spark, self._writer).run(
                bronze_df, snapshot_date, pipeline_run_id
            )
            result.tables_run.append("teams")

            result.venues_rows = VenuesSilverTransform(self._spark, self._writer).run(
                bronze_df, snapshot_date, pipeline_run_id
            )
            result.tables_run.append("venues")

            result.competitions_rows = CompetitionsSilverTransform(self._spark, self._writer).run(
                bronze_df, snapshot_date, pipeline_run_id
            )
            result.tables_run.append("competitions")

            result.innings_rows = InningsSilverTransform(self._spark, self._writer).run(
                bronze_df, snapshot_date, pipeline_run_id
            )
            result.tables_run.append("innings")

            result.match_powerplays_rows = MatchPowerplaysSilverTransform(self._spark, self._writer).run(
                bronze_df, snapshot_date, pipeline_run_id
            )
            result.tables_run.append("match_powerplays")

            result.deliveries_rows = DeliveriesSilverTransform(self._spark, self._writer).run(
                bronze_df, snapshot_date, pipeline_run_id
            )
            result.tables_run.append("deliveries")

            result.wickets_rows = WicketsSilverTransform(self._spark, self._writer).run(
                bronze_df, snapshot_date, pipeline_run_id
            )
            result.tables_run.append("wickets")

            # match_registry must land before players/officials so Path A can join it.
            result.match_registry_rows = MatchRegistrySilverTransform(self._spark, self._writer).run(
                bronze_df, snapshot_date, pipeline_run_id
            )
            result.tables_run.append("match_registry")

            players_result = MatchPlayersSilverTransform(self._spark, self._writer).run(
                bronze_df, snapshot_date, pipeline_run_id
            )
            result.match_players_rows = players_result.row_count
            result.tables_run.append("match_players")

            officials_result = MatchOfficialsSilverTransform(self._spark, self._writer).run(
                bronze_df, snapshot_date, pipeline_run_id
            )
            result.match_officials_rows = officials_result.row_count
            result.tables_run.append("match_officials")

            unmatched_dfs = [r.unmatched_df for r in (players_result, officials_result) if r.unmatched_df is not None]
            result.unmatched_persons_audit_rows = UnmatchedPersonsAuditSilverTransform(self._spark, self._writer).run(
                unmatched_dfs, snapshot_date, pipeline_run_id
            )
            result.tables_run.append("unmatched_persons_audit")
        finally:
            bronze_df.unpersist()

        logger.info(
            "MatchSilverPipeline.run_all complete",
            extra={
                "snapshot_date": snapshot_date,
                "tables_run": result.tables_run,
                "total_rows": result.total_rows,
            },
        )
        return result
