# platform/ingestion/io/minio.py
#
# MinIO / S3-compatible I/O client for the Cricket Intelligence Platform.
#
# All ingestion and transform jobs use this module — never call boto3 directly.
#
# Responsibilities:
#   - Upload / download / delete objects
#   - List objects with prefix filtering
#   - Check object and bucket existence
#   - Generate presigned URLs for debugging
#   - Retry transient S3 errors with exponential back-off
#   - Emit structured log lines for every operation
#
# Usage:
#   from platform.ingestion.io.minio import MinIOClient
#   client = MinIOClient.from_settings()
#   client.upload_file(local_path, "cricket-landing", "raw_zips/file.zip")

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from pathlib import Path
from platform.common.exceptions import (
    BucketNotFoundError,
    ObjectNotFoundError,
    ObjectUploadError,
    StorageError,
)
from platform.common.logging import get_logger
from platform.common.settings import get_settings
from typing import Iterator

import boto3
import botocore.exceptions
from botocore.config import Config

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Transfer configuration defaults
# ---------------------------------------------------------------------------

_DEFAULT_MULTIPART_THRESHOLD = 64 * 1024 * 1024  # 64 MB — use multipart above this
_DEFAULT_MULTIPART_CHUNK_SIZE = 16 * 1024 * 1024  # 16 MB chunks
_DEFAULT_MAX_CONCURRENCY = 4
_DEFAULT_MAX_RETRIES = 5
_DEFAULT_RETRY_BASE_DELAY = 0.5  # seconds
_DEFAULT_RETRY_MAX_DELAY = 30.0  # seconds


# ===========================================================================
# Data classes
# ===========================================================================


@dataclass(frozen=True)
class S3ObjectMeta:
    """Metadata returned for a listed or head-checked S3 object."""

    bucket: str
    key: str
    size_bytes: int
    etag: str
    last_modified: str  # ISO string

    @property
    def path(self) -> str:
        return f"s3://{self.bucket}/{self.key}"

    @property
    def file_name(self) -> str:
        return Path(self.key).name


@dataclass
class UploadResult:
    """Result of a successful upload operation."""

    bucket: str
    key: str
    size_bytes: int
    checksum_sha256: str
    duration_seconds: float
    multipart_used: bool = False

    @property
    def s3_path(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


@dataclass
class DownloadResult:
    """Result of a successful download operation."""

    local_path: Path
    bucket: str
    key: str
    size_bytes: int
    checksum_sha256: str
    duration_seconds: float

    @property
    def s3_path(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


# ===========================================================================
# Retry decorator
# ===========================================================================


def _with_retry(
    max_retries: int = _DEFAULT_MAX_RETRIES,
    base_delay: float = _DEFAULT_RETRY_BASE_DELAY,
    max_delay: float = _DEFAULT_RETRY_MAX_DELAY,
    retryable_codes: tuple[str, ...] = (
        "RequestTimeout",
        "RequestTimeTooSkewed",
        "SlowDown",
        "ServiceUnavailable",
        "InternalError",
    ),
):
    """
    Decorator: retry an S3 operation with exponential back-off on transient errors.
    Non-retryable errors (NoSuchBucket, NoSuchKey, etc.) propagate immediately.
    """

    def decorator(fn):
        def wrapper(*args, **kwargs):
            delay = base_delay
            for attempt in range(1, max_retries + 1):
                try:
                    return fn(*args, **kwargs)
                except botocore.exceptions.ClientError as exc:
                    code = exc.response["Error"]["Code"]
                    if code not in retryable_codes or attempt == max_retries:
                        raise
                    logger.warning(
                        "S3 transient error — retrying",
                        extra={
                            "error_code": code,
                            "attempt": attempt,
                            "retry_after_seconds": delay,
                            "fn": fn.__name__,
                        },
                    )
                    time.sleep(delay)
                    delay = min(delay * 2, max_delay)
                except botocore.exceptions.EndpointConnectionError:
                    if attempt == max_retries:
                        raise
                    logger.warning(
                        "S3 endpoint unreachable — retrying",
                        extra={"attempt": attempt, "retry_after_seconds": delay},
                    )
                    time.sleep(delay)
                    delay = min(delay * 2, max_delay)

        return wrapper

    return decorator


# ===========================================================================
# MinIOClient
# ===========================================================================


class MinIOClient:
    """
    S3-compatible object storage client for the Cricket Intelligence Platform.

    All methods emit structured log lines and raise platform exceptions
    (never raw botocore errors) so callers get consistent error handling.

    Instantiation:
        # From settings (recommended):
        client = MinIOClient.from_settings()

        # Explicit (for tests / overrides):
        client = MinIOClient(
            endpoint="http://localhost:9000",
            access_key="cricket_admin",
            secret_key="cricket_secret",
        )
    """

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
        use_ssl: bool = False,
        max_retries: int = _DEFAULT_MAX_RETRIES,
    ) -> None:
        self._endpoint = endpoint
        self._region = region
        self._max_retries = max_retries

        boto_config = Config(
            region_name=region,
            retries={"max_attempts": 1, "mode": "standard"},  # our own retry wrapper handles this
            s3={"addressing_style": "path"},
        )

        self._s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=boto_config,
            use_ssl=use_ssl,
        )

        self._resource = boto3.resource(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=boto_config,
            use_ssl=use_ssl,
        )

        logger.debug(
            "MinIOClient initialised",
            extra={"endpoint": endpoint, "region": region},
        )

    @classmethod
    def from_settings(cls) -> "MinIOClient":
        """
        Create a MinIOClient from platform settings.
        This is the standard factory used by all pipeline modules.
        """
        cfg = get_settings().storage
        return cls(
            endpoint=cfg.endpoint,
            access_key=cfg.root_user,
            secret_key=cfg.root_password.get_secret_value(),
            region=cfg.region,
            use_ssl=cfg.use_ssl,
        )

    # -----------------------------------------------------------------------
    # Bucket operations
    # -----------------------------------------------------------------------

    def bucket_exists(self, bucket: str) -> bool:
        """Return True if the bucket exists and is accessible."""
        try:
            self._s3.head_bucket(Bucket=bucket)
            return True
        except botocore.exceptions.ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("404", "NoSuchBucket"):
                return False
            raise StorageError(
                f"Unexpected error checking bucket '{bucket}': {code}",
                bucket=bucket,
                error_code=code,
            ) from exc

    def assert_bucket_exists(self, bucket: str) -> None:
        """Raise BucketNotFoundError if the bucket does not exist."""
        if not self.bucket_exists(bucket):
            raise BucketNotFoundError(bucket)

    def list_buckets(self) -> list[str]:
        """Return names of all accessible buckets."""
        response = self._s3.list_buckets()
        return [b["Name"] for b in response.get("Buckets", [])]

    # -----------------------------------------------------------------------
    # Object existence / metadata
    # -----------------------------------------------------------------------

    def object_exists(self, bucket: str, key: str) -> bool:
        """Return True if the object exists."""
        try:
            self._s3.head_object(Bucket=bucket, Key=key)
            return True
        except botocore.exceptions.ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            raise

    def head_object(self, bucket: str, key: str) -> S3ObjectMeta:
        """Return metadata for a single object. Raises ObjectNotFoundError if missing."""
        try:
            resp = self._s3.head_object(Bucket=bucket, Key=key)
            return S3ObjectMeta(
                bucket=bucket,
                key=key,
                size_bytes=resp["ContentLength"],
                etag=resp["ETag"].strip('"'),
                last_modified=resp["LastModified"].isoformat(),
            )
        except botocore.exceptions.ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
                raise ObjectNotFoundError(f"s3://{bucket}/{key}") from exc
            raise

    # -----------------------------------------------------------------------
    # List operations
    # -----------------------------------------------------------------------

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        suffix_filter: str | None = None,
        max_keys: int | None = None,
    ) -> list[S3ObjectMeta]:
        """
        List objects under a prefix. Returns full S3ObjectMeta per object.

        Args:
            bucket:        S3 bucket name
            prefix:        Key prefix to filter on
            suffix_filter: Optional suffix filter (e.g. ".json", ".zip")
            max_keys:      Cap result count (None = all)
        """
        results: list[S3ObjectMeta] = []
        paginator = self._s3.get_paginator("list_objects_v2")

        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if suffix_filter and not key.endswith(suffix_filter):
                    continue
                results.append(
                    S3ObjectMeta(
                        bucket=bucket,
                        key=key,
                        size_bytes=obj["Size"],
                        etag=obj.get("ETag", "").strip('"'),
                        last_modified=obj["LastModified"].isoformat(),
                    )
                )
                if max_keys and len(results) >= max_keys:
                    return results

        logger.debug(
            "Listed objects",
            extra={
                "bucket": bucket,
                "prefix": prefix,
                "object_count": len(results),
                "suffix_filter": suffix_filter or "",
            },
        )
        return results

    def iter_objects(
        self,
        bucket: str,
        prefix: str = "",
        suffix_filter: str | None = None,
    ) -> Iterator[S3ObjectMeta]:
        """
        Memory-efficient generator version of list_objects.
        Use for large prefixes with thousands of objects.
        """
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if suffix_filter and not key.endswith(suffix_filter):
                    continue
                yield S3ObjectMeta(
                    bucket=bucket,
                    key=key,
                    size_bytes=obj["Size"],
                    etag=obj.get("ETag", "").strip('"'),
                    last_modified=obj["LastModified"].isoformat(),
                )

    def count_objects(self, bucket: str, prefix: str = "") -> int:
        """Return the count of objects under a prefix without loading full metadata."""
        count = 0
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            count += page.get("KeyCount", 0)
        return count

    # -----------------------------------------------------------------------
    # Upload operations
    # -----------------------------------------------------------------------

    @_with_retry()
    def upload_file(
        self,
        local_path: Path | str,
        bucket: str,
        key: str,
        extra_metadata: dict[str, str] | None = None,
    ) -> UploadResult:
        """
        Upload a local file to S3. Uses multipart for files > 64 MB.
        Computes SHA-256 before upload for checksum logging.

        Args:
            local_path:     Local file path
            bucket:         Target bucket
            key:            Target S3 key
            extra_metadata: Optional S3 object metadata dict

        Returns:
            UploadResult with path, size, checksum, and duration
        """
        local_path = Path(local_path)
        if not local_path.exists():
            raise StorageError(
                f"Local file not found: {local_path}",
                local_path=str(local_path),
            )

        self.assert_bucket_exists(bucket)

        file_size = local_path.stat().st_size
        checksum = self._sha256_file(local_path)
        multipart = file_size >= _DEFAULT_MULTIPART_THRESHOLD

        extra_args: dict = {}
        if extra_metadata:
            extra_args["Metadata"] = extra_metadata

        started = time.monotonic()

        logger.info(
            "Uploading file",
            extra={
                "local_path": str(local_path),
                "s3_path": f"s3://{bucket}/{key}",
                "size_bytes": file_size,
                "multipart": multipart,
                "checksum_sha256": checksum,
            },
        )

        try:
            if multipart:
                from boto3.s3.transfer import TransferConfig

                config = TransferConfig(
                    multipart_threshold=_DEFAULT_MULTIPART_THRESHOLD,
                    multipart_chunksize=_DEFAULT_MULTIPART_CHUNK_SIZE,
                    max_concurrency=_DEFAULT_MAX_CONCURRENCY,
                )
                self._s3.upload_file(
                    str(local_path),
                    bucket,
                    key,
                    ExtraArgs=extra_args or None,
                    Config=config,
                )
            else:
                self._s3.upload_file(
                    str(local_path),
                    bucket,
                    key,
                    ExtraArgs=extra_args or None,
                )
        except botocore.exceptions.ClientError as exc:
            raise ObjectUploadError(
                f"s3://{bucket}/{key}",
                reason=str(exc),
            ) from exc

        duration = time.monotonic() - started
        result = UploadResult(
            bucket=bucket,
            key=key,
            size_bytes=file_size,
            checksum_sha256=checksum,
            duration_seconds=round(duration, 3),
            multipart_used=multipart,
        )

        logger.info(
            "Upload complete",
            extra={
                "s3_path": result.s3_path,
                "size_bytes": file_size,
                "duration_seconds": result.duration_seconds,
                "multipart": multipart,
            },
        )
        return result

    @_with_retry()
    def upload_bytes(
        self,
        data: bytes,
        bucket: str,
        key: str,
        content_type: str = "application/octet-stream",
        extra_metadata: dict[str, str] | None = None,
    ) -> UploadResult:
        """
        Upload raw bytes to S3. Used for small in-memory objects
        (manifests, checksums, metadata JSON blobs).
        """
        self.assert_bucket_exists(bucket)

        checksum = hashlib.sha256(data).hexdigest()
        started = time.monotonic()

        extra_args: dict = {"ContentType": content_type}
        if extra_metadata:
            extra_args["Metadata"] = extra_metadata

        try:
            self._s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=data,
                **extra_args,
            )
        except botocore.exceptions.ClientError as exc:
            raise ObjectUploadError(f"s3://{bucket}/{key}", reason=str(exc)) from exc

        duration = time.monotonic() - started
        return UploadResult(
            bucket=bucket,
            key=key,
            size_bytes=len(data),
            checksum_sha256=checksum,
            duration_seconds=round(duration, 3),
        )

    # -----------------------------------------------------------------------
    # Download operations
    # -----------------------------------------------------------------------

    @_with_retry()
    def download_file(
        self,
        bucket: str,
        key: str,
        local_path: Path | str,
        mkdir: bool = True,
    ) -> DownloadResult:
        """
        Download an S3 object to a local file.
        Creates parent directories if mkdir=True.

        Returns:
            DownloadResult with local path, size, checksum, and duration
        """
        local_path = Path(local_path)
        if mkdir:
            local_path.parent.mkdir(parents=True, exist_ok=True)

        if not self.object_exists(bucket, key):
            raise ObjectNotFoundError(f"s3://{bucket}/{key}")

        meta = self.head_object(bucket, key)
        started = time.monotonic()

        logger.info(
            "Downloading file",
            extra={
                "s3_path": f"s3://{bucket}/{key}",
                "local_path": str(local_path),
                "size_bytes": meta.size_bytes,
            },
        )

        try:
            self._s3.download_file(bucket, key, str(local_path))
        except botocore.exceptions.ClientError as exc:
            raise StorageError(
                f"Download failed: s3://{bucket}/{key}",
                bucket=bucket,
                key=key,
                reason=str(exc),
            ) from exc

        duration = time.monotonic() - started
        checksum = self._sha256_file(local_path)

        result = DownloadResult(
            local_path=local_path,
            bucket=bucket,
            key=key,
            size_bytes=local_path.stat().st_size,
            checksum_sha256=checksum,
            duration_seconds=round(duration, 3),
        )

        logger.info(
            "Download complete",
            extra={
                "s3_path": result.s3_path,
                "local_path": str(local_path),
                "size_bytes": result.size_bytes,
                "duration_seconds": result.duration_seconds,
                "checksum_sha256": checksum,
            },
        )
        return result

    @_with_retry()
    def read_bytes(self, bucket: str, key: str) -> bytes:
        """
        Read an S3 object directly into memory.
        Use only for small objects (manifests, metadata JSON) — not for large files.
        """
        try:
            response = self._s3.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()
        except botocore.exceptions.ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
                raise ObjectNotFoundError(f"s3://{bucket}/{key}") from exc
            raise

    # -----------------------------------------------------------------------
    # Delete operations
    # -----------------------------------------------------------------------

    def delete_object(self, bucket: str, key: str) -> None:
        """Delete a single object. No-op if the object does not exist."""
        try:
            self._s3.delete_object(Bucket=bucket, Key=key)
            logger.debug("Deleted object", extra={"s3_path": f"s3://{bucket}/{key}"})
        except botocore.exceptions.ClientError as exc:
            raise StorageError(
                f"Delete failed: s3://{bucket}/{key}",
                bucket=bucket,
                key=key,
                reason=str(exc),
            ) from exc

    def delete_prefix(self, bucket: str, prefix: str) -> int:
        """
        Delete all objects under a prefix.
        Returns the count of deleted objects.
        Uses S3 batch delete (up to 1000 per request).
        """
        keys = [obj.key for obj in self.iter_objects(bucket, prefix)]
        if not keys:
            return 0

        deleted = 0
        batch_size = 1000
        for i in range(0, len(keys), batch_size):
            batch = keys[i : i + batch_size]
            self._s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": k} for k in batch]},
            )
            deleted += len(batch)

        logger.info(
            "Deleted prefix",
            extra={"bucket": bucket, "prefix": prefix, "deleted_count": deleted},
        )
        return deleted

    # -----------------------------------------------------------------------
    # Presigned URLs (for debugging and local dev handoffs)
    # -----------------------------------------------------------------------

    def presign_get(
        self,
        bucket: str,
        key: str,
        expires_in: int = 3600,
    ) -> str:
        """Generate a presigned GET URL for an object (default 1 hour)."""
        return self._s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires_in,
        )

    # -----------------------------------------------------------------------
    # Platform-aware convenience methods
    # -----------------------------------------------------------------------

    def upload_to_landing(
        self,
        local_path: Path | str,
        prefix: str,
        snapshot_date: str,
        extra_metadata: dict[str, str] | None = None,
    ) -> UploadResult:
        """
        Upload a file to the cricket-landing bucket under the correct
        Hive-partitioned prefix. Resolves bucket from platform settings.

        Args:
            local_path:     Local file path
            prefix:         Landing prefix — "raw_zips" | "extracted_json" | "register_csv"
            snapshot_date:  ISO date string for Hive partition key
            extra_metadata: Optional S3 object metadata

        Example:
            client.upload_to_landing(
                Path("/tmp/all_matches.zip"),
                prefix="raw_zips",
                snapshot_date="2024-11-01",
            )
            # → s3://cricket-landing/raw_zips/snapshot_date=2024-11-01/all_matches.zip
        """
        local_path = Path(local_path)
        cfg = get_settings().storage
        bucket = cfg.bucket_landing
        key = f"{prefix}/snapshot_date={snapshot_date}/{local_path.name}"

        return self.upload_file(
            local_path=local_path,
            bucket=bucket,
            key=key,
            extra_metadata=extra_metadata,
        )

    def list_landing_json_files(self, snapshot_date: str) -> list[S3ObjectMeta]:
        """
        List all extracted JSON files in the landing zone for a given snapshot date.
        Used by the Bronze parse DAG to discover files for processing.
        """
        cfg = get_settings().storage
        prefix = f"{cfg.prefix_extracted_json}/snapshot_date={snapshot_date}/"
        return self.list_objects(
            bucket=cfg.bucket_landing,
            prefix=prefix,
            suffix_filter=".json",
        )

    def list_unprocessed_json_files(self, snapshot_date: str) -> list[S3ObjectMeta]:
        """
        List JSON files in landing that have not yet been promoted to Bronze.
        Cross-references with file_inventory.is_processed — currently returns
        all files; the DAG task filters against the control DB.
        """
        return self.list_landing_json_files(snapshot_date)

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    @staticmethod
    def _sha256_file(path: Path, chunk_size: int = 8192) -> str:
        """Compute SHA-256 checksum of a local file in streaming chunks."""
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(chunk_size):
                h.update(chunk)
        return h.hexdigest()

    def health_check(self) -> bool:
        """
        Verify MinIO is reachable and the platform buckets exist.
        Used by `make bootstrap` and Airflow startup sensors.
        Returns True if healthy, raises StorageError otherwise.
        """
        cfg = get_settings().storage
        required_buckets = [
            cfg.bucket_landing,
            cfg.bucket_bronze,
            cfg.bucket_silver,
            cfg.bucket_gold,
            cfg.bucket_iceberg,
        ]
        missing = [b for b in required_buckets if not self.bucket_exists(b)]
        if missing:
            raise StorageError(
                f"MinIO health check failed — missing buckets: {missing}. Run: make bootstrap",
                missing_buckets=missing,
            )
        logger.info(
            "MinIO health check passed",
            extra={"endpoint": self._endpoint, "buckets_verified": len(required_buckets)},
        )
        return True

    def __repr__(self) -> str:
        return f"MinIOClient(endpoint={self._endpoint!r})"
