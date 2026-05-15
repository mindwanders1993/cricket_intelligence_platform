# platform/common/settings.py
#
# Central configuration for the Cricket Intelligence Platform.
# Resolution order (highest wins):
#   1. Real environment variables (CI, Docker, shell exports)
#   2. .env file at repo root
#   3. conf/base/*.yaml files (loaded explicitly)
#   4. Pydantic field defaults
#
# Usage:
#   from cip.common.settings import get_settings
#   cfg = get_settings()
#   cfg.minio.endpoint

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Literal

import yaml
from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# ---------------------------------------------------------------------------
# Repo root resolution — works regardless of where Python is invoked from
# ---------------------------------------------------------------------------
_REPO_ROOT = (
    Path(os.environ.get("CIP_REPO_ROOT", ""))
    if os.environ.get("CIP_REPO_ROOT")
    else Path(__file__).resolve().parents[3]
)

_CONF_BASE = _REPO_ROOT / "conf" / "base"
_ENV_FILE = _REPO_ROOT / ".env"


# ---------------------------------------------------------------------------
# YAML loader helper — merges conf/base/<name>.yaml into a flat dict
# ---------------------------------------------------------------------------
def _load_yaml(name: str) -> dict:
    path = _CONF_BASE / f"{name}.yaml"
    if path.exists():
        with path.open() as f:
            return yaml.safe_load(f) or {}
    return {}


# ===========================================================================
# Sub-settings — one class per conf/base/*.yaml file
# ===========================================================================


class StorageSettings(BaseSettings):
    """MinIO / S3-compatible object storage. Maps to conf/base/storage.yaml"""

    model_config = SettingsConfigDict(
        env_prefix="MINIO_",
        env_file=str(_ENV_FILE),
        extra="ignore",
    )

    endpoint: str = Field(
        default="http://minio:9000",
        validation_alias="MINIO_S3_ENDPOINT",
        description="MinIO API endpoint",
    )
    console_url: str = Field(default="http://localhost:9001")
    root_user: str = Field(default="cricket_admin")
    root_password: SecretStr = Field(default=SecretStr("cricket_secret"))
    region: str = Field(default="us-east-1")
    path_style: bool = Field(default=True, description="Use path-style addressing (required for MinIO)")
    use_ssl: bool = Field(default=False)

    # Bucket names — align with create-buckets.sh
    # Three buckets only:
    #   cricket-source-files  → raw downloads (ZIPs, CSVs, JSONs) from cricsheet.org
    #   cricket-lakehouse     → all Iceberg tables (bronze/silver/gold namespaces)
    #   cricket-ml-models     → MLflow artifacts + trained models
    bucket_source_files: str = Field(default="cricket-source-files")
    bucket_lakehouse: str = Field(default="cricket-lakehouse")
    bucket_ml_models: str = Field(default="cricket-ml-models")

    # Source-file prefixes (folders inside cricket-source-files)
    #   match_data/zip/        → all_json.zip and future incremental zips
    #   match_data/json/       → extracted match JSON files
    #   people_and_names/csv/  → people.csv + names.csv
    prefix_match_data_zip: str = Field(default="match_data/zip")
    prefix_match_data_json: str = Field(default="match_data/json")
    prefix_people_and_names_csv: str = Field(default="people_and_names/csv")

    @property
    def source_match_data_zip(self) -> str:
        return f"s3://{self.bucket_source_files}/{self.prefix_match_data_zip}"

    @property
    def source_match_data_json(self) -> str:
        return f"s3://{self.bucket_source_files}/{self.prefix_match_data_json}"

    @property
    def source_people_and_names_csv(self) -> str:
        return f"s3://{self.bucket_source_files}/{self.prefix_people_and_names_csv}"

    @property
    def lakehouse_uri(self) -> str:
        return f"s3://{self.bucket_lakehouse}/"


class IcebergSettings(BaseSettings):
    """Apache Iceberg REST catalog. Maps to conf/base/storage.yaml [iceberg] section."""

    model_config = SettingsConfigDict(
        env_prefix="ICEBERG_",
        env_file=str(_ENV_FILE),
        extra="ignore",
    )

    rest_uri: str = Field(default="http://iceberg-rest:8181")
    lakehouse_bucket: str = Field(default="cricket-lakehouse")
    catalog_name: str = Field(default="cricket")
    namespace_bronze: str = Field(default="bronze")
    namespace_silver: str = Field(default="silver")
    namespace_gold: str = Field(default="gold")

    @property
    def lakehouse_uri(self) -> str:
        return f"s3://{self.lakehouse_bucket}/"


class PostgresSettings(BaseSettings):
    """PostgreSQL control DB. Maps to conf/base/storage.yaml [postgres] section."""

    model_config = SettingsConfigDict(
        env_prefix="POSTGRES_",
        env_file=str(_ENV_FILE),
        extra="ignore",
    )

    host: str = Field(default="postgres")
    port: int = Field(default=5432)
    user: str = Field(default="cricket_user")
    password: SecretStr = Field(default=SecretStr("cricket_pass"))
    db: str = Field(default="cricket_platform")
    pool_size: int = Field(default=5)
    max_overflow: int = Field(default=10)
    connect_timeout: int = Field(default=10)

    @property
    def dsn(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password.get_secret_value()}@{self.host}:{self.port}/{self.db}"

    @property
    def async_dsn(self) -> str:
        return f"postgresql+asyncpg://{self.user}:{self.password.get_secret_value()}@{self.host}:{self.port}/{self.db}"


class AirflowSettings(BaseSettings):
    """Airflow orchestration config. Maps to conf/base/airflow.yaml"""

    model_config = SettingsConfigDict(
        env_prefix="AIRFLOW_",
        env_file=str(_ENV_FILE),
        extra="ignore",
    )

    webserver_url: str = Field(default="http://localhost:8080")
    admin_user: str = Field(default="admin")
    admin_password: SecretStr = Field(default=SecretStr("admin"))
    fernet_key: SecretStr = Field(default=SecretStr(""))
    secret_key: SecretStr = Field(default=SecretStr(""))
    dag_run_timeout_minutes: int = Field(default=120)
    default_pool_slots: int = Field(default=4)


class SparkSettings(BaseSettings):
    """PySpark job config. Maps to conf/base/spark.yaml"""

    model_config = SettingsConfigDict(
        env_prefix="SPARK_",
        env_file=str(_ENV_FILE),
        extra="ignore",
    )

    master: str = Field(default="local[*]")
    app_name_prefix: str = Field(default="cricket-platform")
    # Driver memory bumped to 4g — parsing the full Bronze match_documents
    # snapshot (~22k rows × ~50KB raw_json each) needs more than the pyspark
    # default 1g and the previous 2g. Override via SPARK_DRIVER_MEMORY env if
    # you need more for backfills (e.g. "8g").
    driver_memory: str = Field(default="4g")
    executor_memory: str = Field(default="4g")
    executor_cores: int = Field(default=2)
    # Cap each Parquet scan task at 32 MB compressed so a single task never
    # decompresses more than ~150 MB of raw_json into the JVM heap.
    max_partition_bytes: int = Field(default=33_554_432)  # 32 MB
    # Default 200 is excessive for our 21k-row Match Silver dedup; 50 keeps
    # shuffle coordination overhead low while staying well above driver cores.
    shuffle_partitions: int = Field(default=50)
    iceberg_version: str = Field(default="1.5.0")
    hadoop_aws_version: str = Field(default="3.3.4")
    aws_java_sdk_version: str = Field(default="1.12.262")

    @property
    def iceberg_jar(self) -> str:
        return f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{self.iceberg_version}"


class PolarsSettings(BaseSettings):
    """Polars ingestion config. Maps to conf/base/polars.yaml"""

    model_config = SettingsConfigDict(
        env_prefix="POLARS_",
        env_file=str(_ENV_FILE),
        extra="ignore",
    )

    streaming_chunk_size: int = Field(default=10_000)
    max_threads: int = Field(default=4)
    infer_schema_length: int = Field(default=1000)


class DuckDBSettings(BaseSettings):
    """DuckDB serving config. Maps to conf/base/duckdb.yaml"""

    model_config = SettingsConfigDict(
        env_prefix="DUCKDB_",
        env_file=str(_ENV_FILE),
        extra="ignore",
    )

    db_path: Path = Field(default=_REPO_ROOT / "storage" / "duckdb" / "cricket.duckdb")
    memory_limit: str = Field(default="2GB")
    threads: int = Field(default=4)
    read_only: bool = Field(default=False)


class DbtSettings(BaseSettings):
    """dbt Core config. Maps to conf/base/dbt.yaml"""

    model_config = SettingsConfigDict(
        env_prefix="DBT_",
        env_file=str(_ENV_FILE),
        extra="ignore",
    )

    project_dir: Path = Field(default=_REPO_ROOT / "models" / "dbt")
    profiles_dir: Path = Field(default=_REPO_ROOT / "models" / "dbt" / "profiles")
    target: str = Field(default="dev")
    threads: int = Field(default=4)


class MLflowSettings(BaseSettings):
    """MLflow tracking config. Maps to conf/base/mlflow.yaml"""

    model_config = SettingsConfigDict(
        env_prefix="MLFLOW_",
        env_file=str(_ENV_FILE),
        extra="ignore",
    )

    tracking_uri: str = Field(default="http://mlflow:5001")
    experiment_prefix: str = Field(default="cricket-platform")
    artifact_root: str = Field(default="s3://cricket-ml-models/")
    registry_uri: str = Field(default="")

    @model_validator(mode="after")
    def set_registry_uri_default(self) -> "MLflowSettings":
        if not self.registry_uri:
            self.registry_uri = self.tracking_uri
        return self


class AISettings(BaseSettings):
    """AI assistant config. Maps to conf/base/ai.yaml"""

    model_config = SettingsConfigDict(
        env_prefix="AI_",
        env_file=str(_ENV_FILE),
        extra="ignore",
    )

    ollama_base_url: str = Field(default="http://localhost:11434")
    ollama_model: str = Field(default="llama3.1:8b")
    ollama_timeout_seconds: int = Field(default=120)
    embedding_model: str = Field(default="nomic-embed-text")
    max_sql_result_rows: int = Field(default=500)
    prompt_registry_path: Path = Field(default=_REPO_ROOT / "src" / "cip" / "serving" / "ai" / "prompt_registry")


class PathSettings(BaseSettings):
    """Repo-relative path constants. Maps to conf/base/paths.yaml"""

    model_config = SettingsConfigDict(
        env_prefix="PATH_",
        env_file=str(_ENV_FILE),
        extra="ignore",
    )

    repo_root: Path = Field(default=_REPO_ROOT)
    conf_base: Path = Field(default=_CONF_BASE)
    platform_dir: Path = Field(default=_REPO_ROOT / "src" / "cip")
    storage_dir: Path = Field(default=_REPO_ROOT / "storage")
    duckdb_dir: Path = Field(default=_REPO_ROOT / "storage" / "duckdb")
    artifacts_dir: Path = Field(default=_REPO_ROOT / "storage" / "artifacts")
    external_dir: Path = Field(default=_REPO_ROOT / "storage" / "external")
    notebooks_dir: Path = Field(default=_REPO_ROOT / "notebooks")


# ===========================================================================
# Root settings — composes all sub-settings into one object
# ===========================================================================


class PlatformSettings(BaseSettings):
    """
    Root settings object for the Cricket Intelligence Platform.
    Compose all sub-settings here and expose via get_settings().

    Environment-level:  'dev' | 'prod'
    Resolved from:      ENV_NAME env var, defaults to 'dev'
    """

    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    env_name: Literal["dev", "prod"] = Field(default="dev", alias="ENV_NAME")
    platform_name: str = Field(default="Cricket Intelligence Platform")
    platform_version: str = Field(default="0.1.0")
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(default="INFO", alias="LOG_LEVEL")

    # Sub-settings — each loads its own env prefix
    storage: StorageSettings = Field(default_factory=StorageSettings)
    iceberg: IcebergSettings = Field(default_factory=IcebergSettings)
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    airflow: AirflowSettings = Field(default_factory=AirflowSettings)
    spark: SparkSettings = Field(default_factory=SparkSettings)
    polars: PolarsSettings = Field(default_factory=PolarsSettings)
    duckdb: DuckDBSettings = Field(default_factory=DuckDBSettings)
    dbt: DbtSettings = Field(default_factory=DbtSettings)
    mlflow: MLflowSettings = Field(default_factory=MLflowSettings)
    ai: AISettings = Field(default_factory=AISettings)
    paths: PathSettings = Field(default_factory=PathSettings)

    @field_validator("env_name", mode="before")
    @classmethod
    def normalise_env(cls, v: str) -> str:
        return str(v).lower().strip()

    def is_dev(self) -> bool:
        return self.env_name == "dev"

    def is_prod(self) -> bool:
        return self.env_name == "prod"

    @model_validator(mode="after")
    def _overlay_yaml_defaults(self) -> "PlatformSettings":
        """
        Overlay conf/base/*.yaml values as defaults only — env vars always win.
        This keeps YAML as the human-readable default layer without blocking
        environment-level overrides.
        """
        yaml_map = {
            "storage": _load_yaml("storage"),
            "airflow": _load_yaml("airflow"),
            "spark": _load_yaml("spark"),
            "polars": _load_yaml("polars"),
            "duckdb": _load_yaml("duckdb"),
            "dbt": _load_yaml("dbt"),
            "mlflow": _load_yaml("mlflow"),
            "ai": _load_yaml("ai"),
            "paths": _load_yaml("paths"),
        }
        # YAML values are informational defaults; env vars already took priority
        # during sub-settings construction. No override needed here — this
        # validator is a hook for future conf/env/prod overlay logic.
        _ = yaml_map  # reserved for conf/prod overlay in Phase 2
        return self


# ===========================================================================
# Module-level singleton — cached for the process lifetime
# ===========================================================================


@lru_cache(maxsize=1)
def get_settings() -> PlatformSettings:
    """
    Return the cached PlatformSettings singleton.

    All platform modules call this function — never instantiate PlatformSettings
    directly in application code. The lru_cache ensures .env is parsed once.

    Example:
        from cip.common.settings import get_settings
        cfg = get_settings()
        print(cfg.storage.endpoint)         # http://localhost:9000
        print(cfg.postgres.dsn)             # postgresql+psycopg2://...
        print(cfg.storage.source_match_data_zip) # s3://cricket-source-files/match_data/zip
    """
    return PlatformSettings()


def invalidate_settings_cache() -> None:
    """
    Clear the settings cache. Used in tests to reload settings between cases.

    Example:
        from cip.common.settings import invalidate_settings_cache
        invalidate_settings_cache()
    """
    get_settings.cache_clear()
