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

from __future__ import annotations

from typing import TYPE_CHECKING

from cip.common.contracts.enums import OfficialRole
from cip.common.contracts.naming import TableName
from cip.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cip.transform.shared.writers import SparkIcebergWriter

logger = get_logger(__name__)

_SILVER_MATCH_OFFICIALS = TableName.silver("match_officials")
_SILVER_NAME_VARIATIONS = TableName.silver("name_variations")


class MatchOfficialsSilverTransform:
    """
    Builds silver.match_officials.  Each role's array is unioned with a
    constant role tag, then identity-resolved via silver.name_variations.

    Cricsheet rarely provides cricsheet_ids for officials, so we go
    straight to name-based resolution.
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

    def run(self, bronze_df: "DataFrame", snapshot_date: str, pipeline_run_id: str) -> int:
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

        # Identity resolution via name_variations (officials rarely have cricsheet_ids).
        name_vars = (
            self._spark.read.format("iceberg")
            .load(_SILVER_NAME_VARIATIONS)
            .filter(F.col("_snapshot_date") <= F.lit(snapshot_date))
            .select(
                F.col("name").alias("official_name"),
                F.col("identifier").alias("person_id"),
            )
            .dropDuplicates(["official_name"])
        )

        df = (
            unioned.join(name_vars, on="official_name", how="left")
            .select(
                "match_id",
                "role",
                "official_name",
                "person_id",
                "_bronze_loaded_at",
                "_source_file",
                "_source_url",
            )
            .dropDuplicates(["match_id", "role", "official_name"])
        )

        row_count = df.count()
        self._writer.dynamic_overwrite(
            df=df,
            fqn=_SILVER_MATCH_OFFICIALS,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            source_file="all_json.zip",
            partition_cols=["_snapshot_date"],
        )
        logger.info("silver.match_officials written", extra={"rows": row_count, "snapshot_date": snapshot_date})
        return row_count
