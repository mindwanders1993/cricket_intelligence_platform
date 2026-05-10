# platform/common/contracts/naming.py
#
# Naming convention enforcer for the Cricket Intelligence Platform.
#
# All table names, bucket paths, DAG IDs, and metadata column names
# are generated here. No f-string path building outside this module.
#
# Usage:
#   from platform.common.contracts.naming import TableName, PathBuilder, MetaColumns
#
#   table = TableName.bronze("match_documents")
#   # → "cricket.bronze.match_documents"
#
#   path = PathBuilder.landing_raw_zip("all_matches_json.zip", "2024-11-01")
#   # → "s3://cricket-landing/raw_zips/snapshot_date=2024-11-01/all_matches_json.zip"

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from platform.common.contracts.enums import Layer
from typing import ClassVar

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
        TableName.bronze("match_documents")   → "cricket.bronze.match_documents"
        TableName.silver("deliveries")         → "cricket.silver.deliveries"
        TableName.gold("fact_delivery")        → "cricket.gold.fact_delivery"
    """

    CATALOG: ClassVar[str] = "cricket"

    # Known table names per layer — used for validation when strict=True
    BRONZE_TABLES: ClassVar[frozenset[str]] = frozenset(
        {
            "match_documents",
            "register_people",
            "register_identifiers",
            "register_name_variations",
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
            "match_players",
            "match_officials",
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
        return f"{cls.CATALOG}.{layer}.{table}"

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
    def from_fqn(cls, fqn: str) -> tuple[str, str, str]:
        """
        Parse a FQN back into (catalog, namespace, table).
        Raises ValueError if the FQN does not match expected format.
        """
        parts = fqn.split(".")
        if len(parts) != 3:
            raise ValueError(f"Invalid FQN '{fqn}': expected catalog.namespace.table")
        return parts[0], parts[1], parts[2]


# ===========================================================================
# TABLE 2: PathBuilder — MinIO / S3 path construction
# ===========================================================================


class PathBuilder:
    """
    Builds S3 paths for every layer and prefix in the platform.

    Convention:
        s3://{bucket}/{prefix}/snapshot_date={date}/{filename}

    All paths include a Hive-style partition prefix for date-based
    organisation in the landing and bronze layers.

    Examples:
        PathBuilder.landing_raw_zip("all_matches.zip", "2024-11-01")
        → "s3://cricket-landing/raw_zips/snapshot_date=2024-11-01/all_matches.zip"

        PathBuilder.landing_extracted_json("12345.json", "2024-11-01")
        → "s3://cricket-landing/extracted_json/snapshot_date=2024-11-01/12345.json"

        PathBuilder.iceberg_table(Layer.SILVER, "deliveries")
        → "s3://iceberg-warehouse/silver/deliveries/"
    """

    # Bucket names — must match create-buckets.sh and StorageSettings
    _BUCKETS: ClassVar[dict[Layer, str]] = {
        Layer.LANDING: "cricket-landing",
        Layer.BRONZE: "cricket-bronze",
        Layer.SILVER: "cricket-silver",
        Layer.GOLD: "cricket-gold",
    }

    @staticmethod
    def _partition(snapshot_date: str | date) -> str:
        return f"snapshot_date={_validate_date(snapshot_date)}"

    # --- Landing paths ---

    @classmethod
    def landing_raw_zip(cls, file_name: str, snapshot_date: str | date) -> str:
        return f"s3://cricket-landing/raw_zips/{cls._partition(snapshot_date)}/{file_name}"

    @classmethod
    def landing_extracted_json(cls, file_name: str, snapshot_date: str | date) -> str:
        return f"s3://cricket-landing/extracted_json/{cls._partition(snapshot_date)}/{file_name}"

    @classmethod
    def landing_register_csv(cls, file_name: str, snapshot_date: str | date) -> str:
        return f"s3://cricket-landing/register_csv/{cls._partition(snapshot_date)}/{file_name}"

    # --- Iceberg warehouse paths (used by REST catalog) ---

    @classmethod
    def iceberg_table(cls, layer: Layer, table: str) -> str:
        """Root path for an Iceberg table in the warehouse bucket."""
        _validate_name(table, "table")
        return f"s3://iceberg-warehouse/{layer}/{table}/"

    @classmethod
    def iceberg_namespace(cls, layer: Layer) -> str:
        return f"s3://iceberg-warehouse/{layer}/"

    # --- Bronze layer paths (for direct-write jobs before Iceberg) ---

    @classmethod
    def bronze_raw(cls, table: str, snapshot_date: str | date) -> str:
        _validate_name(table, "table")
        return f"s3://cricket-bronze/{table}/{cls._partition(snapshot_date)}/"

    # --- MLflow artifact paths ---

    @classmethod
    def mlflow_run(cls, experiment_name: str, run_id: str) -> str:
        return f"s3://mlflow-artifacts/{experiment_name}/{run_id}/"


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

    INGEST_ARCHIVES: str = "dag_ingest_cricsheet_archives"
    INGEST_REGISTER: str = "dag_ingest_cricsheet_register"
    PARSE_BRONZE: str = "dag_parse_bronze_match_documents"
    BUILD_SILVER: str = "dag_build_silver_entities"
    RUN_GOLD_DBT: str = "dag_run_gold_dbt_models"
    RUN_QUALITY: str = "dag_run_quality_checks"
    REFRESH_SERVING: str = "dag_refresh_serving_layer"
    TRAIN_ML: str = "dag_train_ml_model"
    REFRESH_AI: str = "dag_refresh_ai_metadata"

    @classmethod
    def all(cls) -> list[str]:
        return [
            cls.INGEST_ARCHIVES,
            cls.INGEST_REGISTER,
            cls.PARSE_BRONZE,
            cls.BUILD_SILVER,
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
