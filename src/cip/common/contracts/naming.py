# platform/common/contracts/naming.py
#
# Naming convention enforcer for the Cricket Intelligence Platform.
#
# All table names, bucket paths, DAG IDs, and metadata column names
# are generated here. No f-string path building outside this module.
#
# Usage:
#   from cip.common.contracts.naming import TableName, PathBuilder, MetaColumns
#
#   table = TableName.bronze("match_data")
#   # → "bronze.match_data"
#
#   path = PathBuilder.source_match_data_zip("all_json.zip", "2024-11-01")
#   # → "s3://cricket-source-files/match_data/zip/snapshot_date=2024-11-01/all_json.zip"

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import ClassVar

from cip.common.contracts.enums import Layer

# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

_SAFE_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")


def _validate_name(name: str, context: str = "name") -> str:
    """Ensure a name is lowercase snake_case. Raises ValueError otherwise."""
    if not _SAFE_NAME_RE.match(name):
        raise ValueError(f"Invalid {context} '{name}': must be lowercase snake_case matching ^[a-z][a-z0-9_]*$")
    return name


def _validate_date(d: str | date) -> str:
    """Return an ISO date string, validating format."""
    if isinstance(d, date):
        return d.isoformat()
    try:
        date.fromisoformat(d)
    except ValueError as exc:
        raise ValueError(f"Invalid date '{d}': must be ISO format YYYY-MM-DD") from exc
    return d


# ===========================================================================
# TABLE 1: TableName — Iceberg fully-qualified table names
# ===========================================================================


class TableName:
    """
    Generates Iceberg table FQNs in the form: catalog.namespace.table

    Convention:
        catalog   = "cricket"  (always)
        namespace = layer name (bronze | silver | gold)
        table     = snake_case entity name

    Examples:
        TableName.bronze("match_data")    → "bronze.match_data"
        TableName.silver("deliveries")    → "silver.deliveries"
        TableName.gold("fact_delivery")   → "gold.fact_delivery"

    The Spark catalog (`spark.sql.catalog.cricket = SparkCatalog` with
    `defaultCatalog=cricket`) resolves these two-segment identifiers via
    its default catalog, so on-disk paths become `s3://cricket-lakehouse/
    {layer}/{table}/`.  The `CATALOG` constant is retained for callers
    that still need to construct the fully-qualified three-segment form
    (e.g. cross-catalog SQL).
    """

    CATALOG: ClassVar[str] = "cricket"

    # Known table names per layer — used for validation when strict=True
    BRONZE_TABLES: ClassVar[frozenset[str]] = frozenset(
        {
            "match_data",
            "people",
            "people_identifiers",
            "name_variations",
        }
    )

    SILVER_TABLES: ClassVar[frozenset[str]] = frozenset(
        {
            "matches",
            "innings",
            "deliveries",
            "wickets",
            "teams",
            "venues",
            "competitions",
            "persons",
            "person_identifiers",
            "name_variations",
            "match_players",
            "match_officials",
            "match_powerplays",
            "match_registry",
            "unmatched_persons_audit",
        }
    )

    GOLD_TABLES: ClassVar[frozenset[str]] = frozenset(
        {
            "dim_player",
            "dim_match",
            "dim_team",
            "dim_venue",
            "dim_competition",
            "dim_date",
            "fact_delivery",
            "fact_innings",
            "fact_match_result",
            "fact_player_match",
            "mart_player_batting",
            "mart_player_bowling",
            "mart_team_performance",
            "mart_venue_dna",
            "mart_phase_scoring",
            "mart_toss_outcome",
            "mart_matchup_analysis",
        }
    )

    @classmethod
    def _fqn(cls, layer: Layer, table: str, strict: bool = True) -> str:
        _validate_name(table, context="table name")
        if strict:
            known = {
                Layer.BRONZE: cls.BRONZE_TABLES,
                Layer.SILVER: cls.SILVER_TABLES,
                Layer.GOLD: cls.GOLD_TABLES,
            }.get(layer, frozenset())
            if known and table not in known:
                raise ValueError(f"Unknown {layer} table '{table}'. Add it to TableName.{layer.upper()}_TABLES first.")
        # Two-segment FQN — {layer}.{table}. The Spark catalog is named
        # `cricket` and has `defaultCatalog=cricket`, so SQL identifiers
        # like `bronze.match_data` resolve correctly without a catalog
        # prefix. PyIceberg sees namespace=(layer,) directly, which gives
        # the canonical Iceberg disk layout: warehouse/{layer}/{table}/.
        return f"{layer}.{table}"

    @classmethod
    def bronze(cls, table: str, strict: bool = True) -> str:
        return cls._fqn(Layer.BRONZE, table, strict)

    @classmethod
    def silver(cls, table: str, strict: bool = True) -> str:
        return cls._fqn(Layer.SILVER, table, strict)

    @classmethod
    def gold(cls, table: str, strict: bool = True) -> str:
        return cls._fqn(Layer.GOLD, table, strict)

    @classmethod
    def from_fqn(cls, fqn: str) -> tuple[str, str]:
        """
        Parse an FQN back into (namespace, table).
        Raises ValueError if the FQN does not match expected format.
        """
        parts = fqn.split(".")
        if len(parts) != 2:
            raise ValueError(f"Invalid FQN '{fqn}': expected namespace.table")
        return parts[0], parts[1]


# ===========================================================================
# TABLE 2: PathBuilder — MinIO / S3 path construction
# ===========================================================================


class PathBuilder:
    """
    Builds S3 paths for every dataset and layer in the platform.

    Three buckets total:
        cricket-source-files  → raw downloads (match_data ZIP/JSON, people_and_names CSV)
        cricket-lakehouse     → all Iceberg tables (bronze/silver/gold namespaces)
        cricket-ml-models     → MLflow run artifacts

    Source-file paths follow:
        s3://cricket-source-files/{dataset}/{format}/snapshot_date={date}/{filename}

    Iceberg paths follow:
        s3://cricket-lakehouse/{layer}/{table}/

    Examples:
        PathBuilder.source_match_data_zip("all_json.zip", "2024-11-01")
        → "s3://cricket-source-files/match_data/zip/snapshot_date=2024-11-01/all_json.zip"

        PathBuilder.source_match_data_json("12345.json", "2024-11-01")
        → "s3://cricket-source-files/match_data/json/snapshot_date=2024-11-01/12345.json"

        PathBuilder.source_people_and_names_csv("people.csv", "2024-11-01")
        → "s3://cricket-source-files/people_and_names/csv/snapshot_date=2024-11-01/people.csv"

        PathBuilder.lakehouse_table(Layer.SILVER, "deliveries")
        → "s3://cricket-lakehouse/silver/deliveries/"
    """

    # Bucket names — must match create-buckets.sh and StorageSettings
    SOURCE_FILES_BUCKET: ClassVar[str] = "cricket-source-files"
    LAKEHOUSE_BUCKET: ClassVar[str] = "cricket-lakehouse"
    ML_MODELS_BUCKET: ClassVar[str] = "cricket-ml-models"

    @staticmethod
    def _partition(snapshot_date: str | date) -> str:
        return f"snapshot_date={_validate_date(snapshot_date)}"

    # --- Source-file paths ---

    @classmethod
    def source_match_data_zip(cls, file_name: str, snapshot_date: str | date) -> str:
        return f"s3://{cls.SOURCE_FILES_BUCKET}/match_data/zip/{cls._partition(snapshot_date)}/{file_name}"

    @classmethod
    def source_match_data_json(cls, file_name: str, snapshot_date: str | date) -> str:
        return f"s3://{cls.SOURCE_FILES_BUCKET}/match_data/json/{cls._partition(snapshot_date)}/{file_name}"

    @classmethod
    def source_people_and_names_csv(cls, file_name: str, snapshot_date: str | date) -> str:
        return f"s3://{cls.SOURCE_FILES_BUCKET}/people_and_names/csv/{cls._partition(snapshot_date)}/{file_name}"

    # --- Lakehouse paths (Iceberg tables, used by REST catalog) ---

    @classmethod
    def lakehouse_table(cls, layer: Layer, table: str) -> str:
        """Root path for an Iceberg table in the lakehouse bucket."""
        _validate_name(table, "table")
        return f"s3://{cls.LAKEHOUSE_BUCKET}/{layer}/{table}/"

    @classmethod
    def lakehouse_namespace(cls, layer: Layer) -> str:
        return f"s3://{cls.LAKEHOUSE_BUCKET}/{layer}/"

    # --- ML model artifact paths ---

    @classmethod
    def ml_model_run(cls, experiment_name: str, run_id: str) -> str:
        return f"s3://{cls.ML_MODELS_BUCKET}/{experiment_name}/{run_id}/"


# ===========================================================================
# TABLE 3: MetaColumns — standard metadata column names
# ===========================================================================


@dataclass(frozen=True)
class MetaColumns:
    """
    Canonical metadata column names injected into every Iceberg table.

    These are the mandatory audit columns defined in the HLD warehouse contract.
    All Bronze, Silver, and Gold tables carry these.

    Reference: HLD section 8 — "define metadata columns"
    """

    # Ingestion provenance
    SNAPSHOT_DATE: str = "_snapshot_date"  # DATE — logical processing date
    INGESTED_AT: str = "_ingested_at"  # TIMESTAMPTZ — wall-clock ingest time
    PIPELINE_RUN_ID: str = "_pipeline_run_id"  # UUID — ingestion_run.run_id FK
    DAG_RUN_ID: str = "_dag_run_id"  # TEXT — Airflow dag_run_id
    SOURCE_FILE: str = "_source_file"  # TEXT — originating filename (e.g. 12345.json)
    SOURCE_URL: str = "_source_url"  # TEXT — originating download URL

    # Row identity
    ROW_HASH: str = "_row_hash"  # TEXT — SHA-256 of business key columns
    IS_CURRENT: str = "_is_current"  # BOOLEAN — SCD2 flag (Silver/Gold only)
    VALID_FROM: str = "_valid_from"  # TIMESTAMPTZ — SCD2 start (Silver/Gold)
    VALID_TO: str = "_valid_to"  # TIMESTAMPTZ — SCD2 end (Silver/Gold, NULL=current)

    # Layer tracking
    BRONZE_LOADED_AT: str = "_bronze_loaded_at"  # TIMESTAMPTZ — when row entered Bronze
    SILVER_LOADED_AT: str = "_silver_loaded_at"  # TIMESTAMPTZ — when row entered Silver

    @classmethod
    def ingestion_columns(cls) -> list[str]:
        """Columns added at ingestion time (landing → bronze)."""
        return [
            cls.SNAPSHOT_DATE,
            cls.INGESTED_AT,
            cls.PIPELINE_RUN_ID,
            cls.DAG_RUN_ID,
            cls.SOURCE_FILE,
            cls.SOURCE_URL,
            cls.ROW_HASH,
        ]

    @classmethod
    def scd2_columns(cls) -> list[str]:
        """SCD2 columns added at Silver layer."""
        return [
            cls.IS_CURRENT,
            cls.VALID_FROM,
            cls.VALID_TO,
        ]

    @classmethod
    def all_columns(cls) -> list[str]:
        """All metadata columns — used in schema validation."""
        return [
            cls.SNAPSHOT_DATE,
            cls.INGESTED_AT,
            cls.PIPELINE_RUN_ID,
            cls.DAG_RUN_ID,
            cls.SOURCE_FILE,
            cls.SOURCE_URL,
            cls.ROW_HASH,
            cls.IS_CURRENT,
            cls.VALID_FROM,
            cls.VALID_TO,
            cls.BRONZE_LOADED_AT,
            cls.SILVER_LOADED_AT,
        ]


# Module-level singleton — import directly
META = MetaColumns()


# ===========================================================================
# TABLE 4: DagNames — canonical DAG IDs matching orchestration/airflow/dags/
# ===========================================================================


class DagNames:
    """
    Canonical Airflow DAG IDs.
    Must match the dag_id defined in each DAG file exactly.
    Used in pipeline_watermark seeds and cross-DAG trigger logic.
    """

    INGEST_MATCH_DATA: str = "dag_ingest_match_data"
    INGEST_PEOPLE_AND_NAMES: str = "dag_ingest_people_and_names"
    BUILD_SILVER_MATCH_DATA: str = "dag_build_silver_match_data"
    BUILD_SILVER_PEOPLE_AND_NAMES: str = "dag_build_silver_people_and_names"
    PARSE_BRONZE_MATCH_DATA: str = "dag_parse_bronze_match_data"
    RUN_GOLD_DBT: str = "dag_run_gold_dbt_models"
    RUN_QUALITY: str = "dag_run_quality_checks"
    REFRESH_SERVING: str = "dag_refresh_serving_layer"
    TRAIN_ML: str = "dag_train_ml_model"
    REFRESH_AI: str = "dag_refresh_ai_metadata"

    @classmethod
    def all(cls) -> list[str]:
        return [
            cls.INGEST_MATCH_DATA,
            cls.INGEST_PEOPLE_AND_NAMES,
            cls.BUILD_SILVER_MATCH_DATA,
            cls.BUILD_SILVER_PEOPLE_AND_NAMES,
            cls.PARSE_BRONZE_MATCH_DATA,
            cls.RUN_GOLD_DBT,
            cls.RUN_QUALITY,
            cls.REFRESH_SERVING,
            cls.TRAIN_ML,
            cls.REFRESH_AI,
        ]


# ===========================================================================
# TABLE 5: IcebergProperties — standard Iceberg table property keys
# ===========================================================================


class IcebergProperties:
    """
    Standard Iceberg table property keys used when creating/altering tables.
    Keeps property strings consistent across Spark, PyIceberg, and REST API calls.
    """

    # Compaction
    WRITE_TARGET_FILE_SIZE: str = "write.target-file-size-bytes"
    WRITE_FORMAT_DEFAULT: str = "write.format.default"  # "parquet"

    # Partitioning hints for readers
    READ_SPLIT_TARGET_SIZE: str = "read.split.target-size-bytes"

    # History retention
    HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS: str = "history.expire.max-snapshot-age-ms"
    HISTORY_EXPIRE_MIN_SNAPSHOTS_TO_KEEP: str = "history.expire.min-snapshots-to-keep"

    # Platform custom tags
    PLATFORM_LAYER: str = "platform.layer"  # bronze | silver | gold
    PLATFORM_OWNER: str = "platform.owner"  # pipeline name
    PLATFORM_CREATED_BY: str = "platform.created-by"  # "cricket-platform/v0.1.0"

    @classmethod
    def bronze_defaults(cls) -> dict[str, str]:
        return {
            cls.WRITE_FORMAT_DEFAULT: "parquet",
            cls.WRITE_TARGET_FILE_SIZE: str(128 * 1024 * 1024),  # 128 MB
            cls.HISTORY_EXPIRE_MIN_SNAPSHOTS_TO_KEEP: "3",
            cls.PLATFORM_LAYER: Layer.BRONZE,
        }

    @classmethod
    def silver_defaults(cls) -> dict[str, str]:
        return {
            cls.WRITE_FORMAT_DEFAULT: "parquet",
            cls.WRITE_TARGET_FILE_SIZE: str(256 * 1024 * 1024),  # 256 MB
            cls.HISTORY_EXPIRE_MIN_SNAPSHOTS_TO_KEEP: "5",
            cls.PLATFORM_LAYER: Layer.SILVER,
        }

    @classmethod
    def gold_defaults(cls) -> dict[str, str]:
        return {
            cls.WRITE_FORMAT_DEFAULT: "parquet",
            cls.WRITE_TARGET_FILE_SIZE: str(256 * 1024 * 1024),
            cls.HISTORY_EXPIRE_MIN_SNAPSHOTS_TO_KEEP: "5",
            cls.PLATFORM_LAYER: Layer.GOLD,
        }
