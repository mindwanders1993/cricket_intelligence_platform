# src/cip/transform/spark/silver/officials.py
#
# Silver transform: bronze.match_data → silver.match_officials
#
# Grain: one row per (match_id, role, official_name).
# Partition: _snapshot_date.
#
# info.officials is a struct with four optional array fields:
#   - umpires, tv_umpires, reserve_umpires, match_referees
# Each becomes a separate set of rows tagged with the corresponding role.
#
# Identity resolution mirrors MatchPlayersSilverTransform:
#   Path A — silver.match_registry → cricsheet_id → silver.person_identifiers.
#   Path B — silver.name_variations.
#   Path C — unresolved; emitted to silver.unmatched_persons_audit.
#
# Cricsheet rarely populates registry entries for officials, so Path B
# carries most of the resolution load in practice.

from __future__ import annotations

from typing import TYPE_CHECKING

from cip.common.contracts.enums import OfficialRole
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

_SILVER_MATCH_OFFICIALS = TableName.silver("match_officials")
_SILVER_MATCH_REGISTRY = TableName.silver("match_registry")
_SILVER_PERSON_IDENTIFIERS = TableName.silver("person_identifiers")
_SILVER_NAME_VARIATIONS = TableName.silver("name_variations")

_CRICSHEET_SOURCE_SYSTEM = "cricsheet"


class MatchOfficialsSilverTransform:
    """
    Builds silver.match_officials. Each role's array is unioned with a
    constant role tag, then identity-resolved via match_registry (Path A)
    and name_variations (Path B).
    """

    _ROLES = (
        OfficialRole.UMPIRE,
        OfficialRole.TV_UMPIRE,
        OfficialRole.RESERVE_UMPIRE,
        OfficialRole.MATCH_REFEREE,
    )

    def __init__(self, spark: "SparkSession", writer: "SparkIcebergWriter") -> None:
        self._spark = spark
        self._writer = writer

    def run(self, bronze_df: "DataFrame", snapshot_date: str, pipeline_run_id: str) -> ResolutionResult:
        from pyspark.sql import functions as F

        officials = F.col("parsed.info.officials")

        per_role_dfs: list = []
        for role in self._ROLES:
            role_col = officials.getField(str(role))
            per_role_dfs.append(
                bronze_df.select(
                    F.col("match_id"),
                    F.lit(str(role)).alias("role"),
                    F.explode(F.coalesce(role_col, F.array())).alias("official_name"),
                    F.col("_bronze_loaded_at"),
                    F.col("_source_file"),
                    F.col("_source_url"),
                )
            )

        unioned = per_role_dfs[0]
        for extra in per_role_dfs[1:]:
            unioned = unioned.unionByName(extra)

        # Path A: silver.match_registry on (match_id, display_name=official_name).
        registry = (
            self._spark.read.format("iceberg")
            .load(_SILVER_MATCH_REGISTRY)
            .filter(F.col("_snapshot_date") <= F.lit(snapshot_date))
            .select(
                F.col("match_id"),
                F.col("display_name").alias("official_name"),
                F.col("cricsheet_id"),
            )
            .dropDuplicates(["match_id", "official_name"])
        )
        with_cricsheet = unioned.join(registry, on=["match_id", "official_name"], how="left")

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
        path_a = with_cricsheet.join(identifiers, on="cricsheet_id", how="left")

        # Path B: silver.name_variations on name → identifier.
        name_vars = (
            self._spark.read.format("iceberg")
            .load(_SILVER_NAME_VARIATIONS)
            .filter(F.col("_snapshot_date") <= F.lit(snapshot_date))
            .select(
                F.col("name").alias("official_name"),
                F.col("identifier").alias("person_id_via_name"),
            )
            .dropDuplicates(["official_name"])
        )
        resolved = path_a.join(name_vars, on="official_name", how="left").withColumn(
            "person_id",
            F.coalesce(F.col("person_id_via_id"), F.col("person_id_via_name")),
        )

        final = resolved.select(
            "match_id",
            "role",
            "official_name",
            "cricsheet_id",
            "person_id_via_id",
            "person_id",
            "_bronze_loaded_at",
            "_source_file",
            "_source_url",
        ).dropDuplicates(["match_id", "role", "official_name"])

        unmatched_df = final.filter(F.col("person_id").isNull()).select(
            F.col("match_id"),
            F.col("role"),
            F.col("official_name").alias("display_name"),
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
            "role",
            "official_name",
            "person_id",
            "_bronze_loaded_at",
            "_source_file",
            "_source_url",
        )

        row_count = df.count()
        unresolved = unmatched_df.count()
        self._writer.dynamic_overwrite(
            df=df,
            fqn=_SILVER_MATCH_OFFICIALS,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            source_file="all_json.zip",
            partition_cols=["_snapshot_date"],
        )
        logger.info(
            "silver.match_officials written",
            extra={"rows": row_count, "unresolved": unresolved, "snapshot_date": snapshot_date},
        )
        return ResolutionResult(row_count=row_count, unmatched_df=unmatched_df)
