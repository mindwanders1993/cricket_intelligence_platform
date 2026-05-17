# src/cip/transform/spark/silver/players.py
#
# Silver transform: bronze.match_data → silver.match_players
#
# Grain: one row per (match_id, team, player_name).
# Partition: _snapshot_date.
#
# Identity resolution:
#   Path A — Cricsheet registry hit (via silver.match_registry):
#     Join silver.match_registry on (match_id, display_name = player_name) → cricsheet_id.
#     Join silver.person_identifiers where source_system='cricsheet' AND
#     source_identifier = cricsheet_id → person_id.
#   Path B — Name fallback:
#     If Path A returns NULL, join silver.name_variations on name = player_name
#     → identifier (which is the person_id).
#   Path C — Unresolved:
#     person_id remains NULL. The row is emitted to silver.unmatched_persons_audit
#     via the ResolutionResult.unmatched_df side channel.

from __future__ import annotations

from typing import TYPE_CHECKING

from cip.common.contracts.naming import TableName
from cip.common.logging import get_logger
from cip.transform.spark.silver.unmatched_audit import (
    REASON_NO_REGISTER_MATCH,
    REASON_NO_REGISTRY_MAPPING,
    ResolutionResult,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cip.transform.shared.writers import SparkIcebergWriter

logger = get_logger(__name__)

_SILVER_MATCH_PLAYERS = TableName.silver("match_players")
_SILVER_MATCH_REGISTRY = TableName.silver("match_registry")
_SILVER_PERSON_IDENTIFIERS = TableName.silver("person_identifiers")
_SILVER_NAME_VARIATIONS = TableName.silver("name_variations")

_CRICSHEET_SOURCE_SYSTEM = "cricsheet"
_ROLE_PLAYER = "player"


class MatchPlayersSilverTransform:
    """
    Builds silver.match_players with two-path identity resolution.

    The Register Silver tables (silver.persons / person_identifiers /
    name_variations) AND silver.match_registry MUST already exist —
    this transform joins all four.
    """

    def __init__(self, spark: "SparkSession", writer: "SparkIcebergWriter") -> None:
        self._spark = spark
        self._writer = writer

    def run(self, bronze_df: "DataFrame", snapshot_date: str, pipeline_run_id: str) -> ResolutionResult:
        from pyspark.sql import functions as F

        # ------------------------------------------------------------------
        # 1. Pull (match_id, team, player_name) rows from info.players.
        # ------------------------------------------------------------------
        team_lists = bronze_df.select(
            "match_id",
            F.explode(F.map_entries("parsed.info.players")).alias("team_kv"),
            F.col("_bronze_loaded_at"),
            F.col("_source_file"),
            F.col("_source_url"),
        ).select(
            "match_id",
            F.col("team_kv").getField("key").alias("team"),
            F.col("team_kv").getField("value").alias("player_list"),
            "_bronze_loaded_at",
            "_source_file",
            "_source_url",
        )

        players = team_lists.select(
            "match_id",
            "team",
            F.explode("player_list").alias("player_name"),
            "_bronze_loaded_at",
            "_source_file",
            "_source_url",
        )

        # ------------------------------------------------------------------
        # 2. Path A step 1: join silver.match_registry on (match_id, name).
        # ------------------------------------------------------------------
        registry = (
            self._spark.read.format("iceberg")
            .load(_SILVER_MATCH_REGISTRY)
            .filter(F.col("_snapshot_date") <= F.lit(snapshot_date))
            .select(
                F.col("match_id"),
                F.col("display_name").alias("player_name"),
                F.col("cricsheet_id"),
            )
            .dropDuplicates(["match_id", "player_name"])
        )

        with_cricsheet = players.join(registry, on=["match_id", "player_name"], how="left")

        # ------------------------------------------------------------------
        # 3. Path A step 2: cricsheet_id → silver.person_identifiers.
        # ------------------------------------------------------------------
        path_a = self._resolve_via_cricsheet_id(with_cricsheet, snapshot_date)

        # ------------------------------------------------------------------
        # 4. Path B: rows still unresolved → silver.name_variations.
        # ------------------------------------------------------------------
        resolved = self._resolve_via_name_variations(path_a, snapshot_date)

        # ------------------------------------------------------------------
        # 5. Final projection. Drop duplicate (match_id, team, player_name).
        # ------------------------------------------------------------------
        final = resolved.select(
            "match_id",
            "team",
            "player_name",
            "cricsheet_id",
            "person_id_via_id",
            "person_id",
            "_bronze_loaded_at",
            "_source_file",
            "_source_url",
        ).dropDuplicates(["match_id", "team", "player_name"])

        # Audit DF — every row where person_id is NULL, with the reason it failed.
        unmatched_df = final.filter(F.col("person_id").isNull()).select(
            F.col("match_id"),
            F.lit(_ROLE_PLAYER).alias("role"),
            F.col("player_name").alias("display_name"),
            F.col("cricsheet_id"),
            F.when(F.col("cricsheet_id").isNull(), F.lit(REASON_NO_REGISTRY_MAPPING))
            .otherwise(F.lit(REASON_NO_REGISTER_MATCH))
            .alias("reason"),
            F.col("_bronze_loaded_at"),
            F.col("_source_file"),
            F.col("_source_url"),
        )

        df = final.select(
            "match_id",
            "team",
            "player_name",
            "person_id",
            "_bronze_loaded_at",
            "_source_file",
            "_source_url",
        )

        row_count = df.count()
        unresolved = unmatched_df.count()
        self._writer.dynamic_overwrite(
            df=df,
            fqn=_SILVER_MATCH_PLAYERS,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            source_file="all_json.zip",
            partition_cols=["_snapshot_date"],
        )
        logger.info(
            "silver.match_players written",
            extra={"rows": row_count, "unresolved": unresolved, "snapshot_date": snapshot_date},
        )
        return ResolutionResult(row_count=row_count, unmatched_df=unmatched_df)

    # ----------------------------------------------------------------------
    # Internal helpers
    # ----------------------------------------------------------------------

    def _resolve_via_cricsheet_id(self, players_df: "DataFrame", snapshot_date: str) -> "DataFrame":
        """Left-join silver.person_identifiers to add person_id where cricsheet_id exists."""
        from pyspark.sql import functions as F

        # silver.person_identifiers keeps the original `identifier` column —
        # only silver.persons renames it to person_id. See spark/silver/persons.py.
        identifiers = (
            self._spark.read.format("iceberg")
            .load(_SILVER_PERSON_IDENTIFIERS)
            .filter(F.col("_snapshot_date") <= F.lit(snapshot_date))
            .filter(F.col("source_system") == F.lit(_CRICSHEET_SOURCE_SYSTEM))
            .select(
                F.col("source_identifier").alias("cricsheet_id"),
                F.col("identifier").alias("person_id_via_id"),
            )
            .dropDuplicates(["cricsheet_id"])
        )

        return players_df.join(identifiers, on="cricsheet_id", how="left")

    def _resolve_via_name_variations(self, players_df: "DataFrame", snapshot_date: str) -> "DataFrame":
        """For rows still NULL, attempt name → identifier match in silver.name_variations."""
        from pyspark.sql import functions as F

        # silver.name_variations columns: identifier, name + metadata.
        name_vars = (
            self._spark.read.format("iceberg")
            .load(_SILVER_NAME_VARIATIONS)
            .filter(F.col("_snapshot_date") <= F.lit(snapshot_date))
            .select(
                F.col("name").alias("player_name"),
                F.col("identifier").alias("person_id_via_name"),
            )
            .dropDuplicates(["player_name"])
        )

        joined = players_df.join(name_vars, on="player_name", how="left")
        return joined.withColumn(
            "person_id",
            F.coalesce(F.col("person_id_via_id"), F.col("person_id_via_name")),
        ).drop("person_id_via_name")
