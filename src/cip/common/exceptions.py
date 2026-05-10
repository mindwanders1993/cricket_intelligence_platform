# platform/common/exceptions.py
#
# Platform-wide exception hierarchy for the Cricket Intelligence Platform.
#
# Design rules:
#   1. All platform exceptions inherit from CricketPlatformError.
#   2. Every exception carries structured context (layer, table, pipeline, etc.)
#      so error logs are machine-parseable without string parsing.
#   3. Exceptions are grouped by domain: Ingestion, Storage, Transform,
#      Quality, Serving, Config.
#
# Usage:
#   from cip.common.exceptions import IngestionError, ChecksumMismatchError
#   raise ChecksumMismatchError(
#       file_name="all_matches.zip",
#       expected="abc123",
#       got="def456",
#   )

from __future__ import annotations

from typing import Any

# ===========================================================================
# Base
# ===========================================================================


class CricketPlatformError(Exception):
    """
    Root exception for all Cricket Intelligence Platform errors.
    Carries a structured context dict that gets serialised into log lines.
    """

    def __init__(self, message: str, **context: Any) -> None:
        super().__init__(message)
        self.message = message
        self.context = context  # arbitrary structured fields

    def __repr__(self) -> str:
        ctx_str = ", ".join(f"{k}={v!r}" for k, v in self.context.items())
        return f"{self.__class__.__name__}({self.message!r}, {ctx_str})"

    def to_dict(self) -> dict[str, Any]:
        return {"error_type": self.__class__.__name__, "message": self.message, **self.context}


# ===========================================================================
# Configuration errors
# ===========================================================================


class ConfigurationError(CricketPlatformError):
    """Raised when required configuration is missing or invalid."""


class MissingSettingError(ConfigurationError):
    """Raised when a required settings key is absent."""

    def __init__(self, setting_name: str, source: str = ".env") -> None:
        super().__init__(
            f"Required setting '{setting_name}' not found in {source}",
            setting_name=setting_name,
            source=source,
        )


# ===========================================================================
# Storage / IO errors
# ===========================================================================


class StorageError(CricketPlatformError):
    """Base for MinIO / S3 / filesystem errors."""


class BucketNotFoundError(StorageError):
    """Raised when a MinIO bucket does not exist."""

    def __init__(self, bucket: str) -> None:
        super().__init__(
            f"Bucket '{bucket}' does not exist. Run: make bootstrap",
            bucket=bucket,
        )


class ObjectNotFoundError(StorageError):
    """Raised when an expected S3 object is missing."""

    def __init__(self, path: str) -> None:
        super().__init__(f"Object not found: {path}", path=path)


class ObjectUploadError(StorageError):
    """Raised when an S3 put operation fails."""

    def __init__(self, path: str, reason: str = "") -> None:
        super().__init__(f"Failed to upload object: {path}", path=path, reason=reason)


# ===========================================================================
# Ingestion errors
# ===========================================================================


class IngestionError(CricketPlatformError):
    """Base for ingestion pipeline errors."""


class DownloadError(IngestionError):
    """Raised when a source URL download fails."""

    def __init__(self, url: str, status_code: int | None = None, reason: str = "") -> None:
        super().__init__(
            f"Download failed for {url}",
            url=url,
            status_code=status_code,
            reason=reason,
        )


class ChecksumMismatchError(IngestionError):
    """Raised when a downloaded file's checksum does not match the expected value."""

    def __init__(self, file_name: str, expected: str, got: str) -> None:
        super().__init__(
            f"Checksum mismatch for {file_name}: expected={expected}, got={got}",
            file_name=file_name,
            expected=expected,
            got=got,
        )


class ExtractionError(IngestionError):
    """Raised when archive extraction fails."""

    def __init__(self, archive_path: str, reason: str = "") -> None:
        super().__init__(
            f"Failed to extract archive: {archive_path}",
            archive_path=archive_path,
            reason=reason,
        )


class ManifestError(IngestionError):
    """Raised when the ingestion manifest is missing, corrupt, or invalid."""

    def __init__(self, manifest_path: str, reason: str = "") -> None:
        super().__init__(
            f"Manifest error at {manifest_path}: {reason}",
            manifest_path=manifest_path,
            reason=reason,
        )


class DuplicateIngestionError(IngestionError):
    """Raised when a file has already been ingested and dedup is enforced."""

    def __init__(self, file_name: str, existing_run_id: str) -> None:
        super().__init__(
            f"File '{file_name}' already ingested in run {existing_run_id}",
            file_name=file_name,
            existing_run_id=existing_run_id,
        )


# ===========================================================================
# Transform / parse errors
# ===========================================================================


class TransformError(CricketPlatformError):
    """Base for Polars / PySpark transformation errors."""


class ParseError(TransformError):
    """Raised when a source file cannot be parsed."""

    def __init__(self, file_path: str, layer: str, reason: str = "") -> None:
        super().__init__(
            f"Failed to parse {file_path} at layer={layer}: {reason}",
            file_path=file_path,
            layer=layer,
            reason=reason,
        )


class SchemaEvolutionError(TransformError):
    """Raised when an Iceberg schema change is incompatible with the write path."""

    def __init__(self, table: str, change_type: str, detail: str = "") -> None:
        super().__init__(
            f"Incompatible schema change on {table}: {change_type}",
            table=table,
            change_type=change_type,
            detail=detail,
        )


class PartitionError(TransformError):
    """Raised when partition values are invalid or missing."""

    def __init__(self, table: str, partition_key: str, value: Any) -> None:
        super().__init__(
            f"Invalid partition value on {table}: {partition_key}={value!r}",
            table=table,
            partition_key=partition_key,
            value=value,
        )


# ===========================================================================
# Data quality errors
# ===========================================================================


class DataQualityError(CricketPlatformError):
    """Base for data quality check failures."""


class QualityCheckFailedError(DataQualityError):
    """Raised when a DQ check exceeds its failure threshold."""

    def __init__(
        self,
        check_name: str,
        table: str,
        layer: str,
        failure_rate: float,
        threshold_pct: float,
    ) -> None:
        super().__init__(
            f"DQ check '{check_name}' failed on {layer}.{table}: "
            f"failure_rate={failure_rate:.2f}% > threshold={threshold_pct:.2f}%",
            check_name=check_name,
            table=table,
            layer=layer,
            failure_rate=failure_rate,
            threshold_pct=threshold_pct,
        )


class ContractViolationError(DataQualityError):
    """Raised when data does not meet a layer promotion contract."""

    def __init__(self, source_layer: str, target_layer: str, table: str, reason: str) -> None:
        super().__init__(
            f"Contract violation: cannot promote {source_layer}.{table} → {target_layer}. {reason}",
            source_layer=source_layer,
            target_layer=target_layer,
            table=table,
            reason=reason,
        )


# ===========================================================================
# Iceberg / catalog errors
# ===========================================================================


class IcebergError(CricketPlatformError):
    """Base for Apache Iceberg catalog and table errors."""


class TableNotFoundError(IcebergError):
    """Raised when an Iceberg table does not exist in the catalog."""

    def __init__(self, namespace: str, table_name: str) -> None:
        super().__init__(
            f"Iceberg table not found: {namespace}.{table_name}",
            namespace=namespace,
            table_name=table_name,
        )


class SnapshotError(IcebergError):
    """Raised when an Iceberg snapshot operation fails."""

    def __init__(self, table: str, operation: str, reason: str = "") -> None:
        super().__init__(
            f"Snapshot {operation} failed on {table}: {reason}",
            table=table,
            operation=operation,
            reason=reason,
        )


# ===========================================================================
# Serving / API errors
# ===========================================================================


class ServingError(CricketPlatformError):
    """Base for DuckDB / FastAPI serving errors."""


class QueryError(ServingError):
    """Raised when a DuckDB analytical query fails."""

    def __init__(self, query_name: str, reason: str = "") -> None:
        super().__init__(
            f"Query '{query_name}' failed: {reason}",
            query_name=query_name,
            reason=reason,
        )


class ViewRefreshError(ServingError):
    """Raised when a DuckDB view fails to refresh from Iceberg."""

    def __init__(self, view_name: str, reason: str = "") -> None:
        super().__init__(
            f"View refresh failed for '{view_name}': {reason}",
            view_name=view_name,
            reason=reason,
        )
