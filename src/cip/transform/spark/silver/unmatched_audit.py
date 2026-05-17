# src/cip/transform/spark/silver/unmatched_audit.py
#
# Silver transform: collected unmatched rows → silver.unmatched_persons_audit
#
# Grain: one row per failed identity-resolution event.
# Columns: match_id, role, display_name, cricsheet_id, reason + system metadata.
#
# Unlike other Silver transforms, this one does NOT read from Bronze — it
# consumes DataFrames already produced by MatchPlayersSilverTransform and
# MatchOfficialsSilverTransform during the same pipeline run. The two
# upstream transforms emit a `ResolutionResult` (row_count + unmatched_df);
# this transform unions the unmatched DataFrames and writes the audit.
#
# Reasons emitted:
#   - NO_REGISTRY_MAPPING: cricsheet_id was NULL — the per-match registry
#     had no entry for this display_name (Path A blocked at step 1).
#   - NO_REGISTER_MATCH:   cricsheet_id was set but silver.person_identifiers
#     had no matching row (Path A blocked at step 2). Path B also failed.

from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

from cip.common.contracts.naming import TableName
from cip.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from cip.transform.shared.writers import SparkIcebergWriter

logger = get_logger(__name__)

_SILVER_UNMATCHED_AUDIT = TableName.silver("unmatched_persons_audit")

# Reason taxonomy — keep stable; downstream DQ check MAT-SLV-005 reads these.
REASON_NO_REGISTRY_MAPPING = "NO_REGISTRY_MAPPING"
REASON_NO_REGISTER_MATCH = "NO_REGISTER_MATCH"


class ResolutionResult(NamedTuple):
    """Return type for MatchPlayersSilverTransform.run() and MatchOfficialsSilverTransform.run().

    The unmatched_df is a DataFrame of person_id-NULL rows with columns:
    match_id, role, display_name, cricsheet_id, reason + bronze metadata.
    None means the transform produced zero unmatched rows (or short-circuited).
    """

    row_count: int
    unmatched_df: "DataFrame | None"


class UnmatchedPersonsAuditSilverTransform:
    """
    Unions the unmatched DataFrames from MatchPlayers + MatchOfficials and
    writes silver.unmatched_persons_audit.
    """

    def __init__(self, spark: "SparkSession", writer: "SparkIcebergWriter") -> None:
        self._spark = spark
        self._writer = writer

    def run(
        self,
        unmatched_dfs: list["DataFrame"],
        snapshot_date: str,
        pipeline_run_id: str,
    ) -> int:
        if not unmatched_dfs:
            logger.info(
                "silver.unmatched_persons_audit — no unmatched DataFrames provided; skipping write",
                extra={"snapshot_date": snapshot_date},
            )
            return 0

        unioned = unmatched_dfs[0]
        for extra in unmatched_dfs[1:]:
            unioned = unioned.unionByName(extra)

        df = unioned.dropDuplicates(["match_id", "role", "display_name"])

        row_count = df.count()
        self._writer.dynamic_overwrite(
            df=df,
            fqn=_SILVER_UNMATCHED_AUDIT,
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            source_file="all_json.zip",
            partition_cols=["_snapshot_date"],
        )
        logger.info(
            "silver.unmatched_persons_audit written",
            extra={"rows": row_count, "snapshot_date": snapshot_date},
        )
        return row_count
