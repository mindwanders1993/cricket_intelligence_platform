# src/cip/transform/spark/silver/bronze_reader.py
#
# Shared Bronze reader for Match Silver transforms.
#
# Responsibilities:
#   1. Read bronze.match_data up to and including snapshot_date.
#   2. Deduplicate by MAX(revision) per match_id — corrections handled
#      automatically because Bronze appends new (match_id, revision) rows.
#   3. Parse `raw_json` with MATCH_JSON_SCHEMA into a nested struct column.
#   4. Cache and return.  Every downstream transform consumes the same
#      cached DataFrame to avoid re-parsing on each pass.

from __future__ import annotations

from typing import TYPE_CHECKING

from cip.common.contracts.naming import META, TableName
from cip.common.logging import get_logger
from cip.transform.spark.silver.schema import MATCH_JSON_SCHEMA

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = get_logger(__name__)

_BRONZE_FQN = TableName.bronze("match_data")


def read_bronze_matches(
    spark: "SparkSession",
    snapshot_date: str,
    match_ids: list[str] | None = None,
) -> "DataFrame":
    """
    Read Bronze match documents, dedup by MAX(revision) per match_id, parse JSON.

    Args:
        spark:          Active SparkSession (Iceberg-aware).
        snapshot_date:  ISO date.  Only Bronze rows with
                        `_snapshot_date <= snapshot_date` are considered.
        match_ids:      Optional incremental scope. When provided, the Bronze
                        read is filtered to these match_ids BEFORE the dedup
                        window — saves both scan cost and shuffle cost
                        proportional to the skipped match count. When None,
                        every match_id in scope of the snapshot is read
                        (full rebuild semantics).

    Returns:
        Cached DataFrame with columns:
            match_id, revision, match_type, gender, season, match_date,
            team_a, team_b, venue, city,
            parsed (StructType matching MATCH_JSON_SCHEMA),
            _bronze_loaded_at (renamed from Bronze _ingested_at),
            _source_file, _source_url
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    bronze = spark.read.format("iceberg").load(_BRONZE_FQN).filter(F.col(META.SNAPSHOT_DATE) <= F.lit(snapshot_date))

    if match_ids is not None:
        # Push the match_id filter BEFORE the dedup window so the shuffle
        # operates on the scoped set, not all 21k matches.
        bronze = bronze.filter(F.col("match_id").isin(list(match_ids)))

    # Dedup: pick row with MAX(revision) per match_id.  Revision is stored
    # as string in Bronze; cast to int for proper ordering.
    rev_window = Window.partitionBy("match_id").orderBy(F.col("revision").cast("int").desc())
    deduped = bronze.withColumn("__rn", F.row_number().over(rev_window)).filter(F.col("__rn") == 1).drop("__rn")

    # Parse the raw_json blob into a struct column we can navigate via dot.
    parsed = deduped.withColumn("parsed", F.from_json(F.col("raw_json"), MATCH_JSON_SCHEMA))

    # Project the columns Silver transforms actually need.  Drop raw_json
    # to keep the cached DF lean (raw_json is megabytes per match).
    #
    # Use MEMORY_AND_DISK persistence rather than .cache() (= MEMORY_ONLY)
    # because the parsed nested struct can balloon to multiple GB on a full
    # backfill — MEMORY_AND_DISK spills to local disk instead of OOMing.
    from pyspark import StorageLevel

    projected = parsed.select(
        F.col("match_id"),
        F.col("revision"),
        F.col("match_type").alias("bronze_match_type"),
        F.col("gender").alias("bronze_gender"),
        F.col("season").alias("bronze_season"),
        F.col("match_date").alias("bronze_match_date"),
        F.col("team_a").alias("bronze_team_a"),
        F.col("team_b").alias("bronze_team_b"),
        F.col("venue").alias("bronze_venue"),
        F.col("city").alias("bronze_city"),
        F.col("parsed"),
        F.col(META.INGESTED_AT).alias(META.BRONZE_LOADED_AT),
        F.col(META.SOURCE_FILE),
        F.col(META.SOURCE_URL),
    ).persist(StorageLevel.MEMORY_AND_DISK)

    row_count = projected.count()  # materialise the persisted DF
    logger.info(
        "Bronze match documents loaded for Silver",
        extra={"snapshot_date": snapshot_date, "matches_after_dedup": row_count},
    )
    return projected
