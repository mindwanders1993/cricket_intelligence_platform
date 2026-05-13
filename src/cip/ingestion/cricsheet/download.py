# src/cip/ingestion/cricsheet/download.py
#
# Downloads the Cricsheet all-matches ZIP archive to the MinIO landing zone
# and records the attempt in control.archive_download_log.
#
# Idempotency:
#   Skips if a SUCCESS row already exists for (archive_file, snapshot_date)
#   unless force=True.
#
# Usage:
#   downloader = ArchiveDownloader.from_settings()
#   record = downloader.download(snapshot_date="2026-05-01", pipeline_run_id="run-xyz")

from __future__ import annotations

import tempfile
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from cip.common.logging import get_logger
from cip.ingestion.cricsheet.checksum import sha256_file
from cip.ingestion.io.minio import MinIOClient

logger = get_logger(__name__)

_ARCHIVE_URL = "https://cricsheet.org/downloads/all_json.zip"
_ARCHIVE_FILE = "all_json.zip"
_MIN_EXPECTED_BYTES = 10 * 1024 * 1024  # 10 MB
_DAG_ID = "dag_ingest_cricsheet_archives"


@dataclass(frozen=True)
class DownloadRecord:
    id: int
    archive_file: str
    source_url: str
    snapshot_date: str
    landing_path: str
    file_size_bytes: int
    checksum_sha256: str
    status: str


class ArchiveDownloader:
    """
    Downloads the Cricsheet all-matches ZIP and lands it in MinIO.

    Writes one row to control.archive_download_log per attempt.
    The row ID is returned so downstream tasks can reference it as
    archive_download_id in control.bronze_match_ingestion_log.
    """

    def __init__(self, minio: MinIOClient, pg_dsn: str) -> None:
        self._minio = minio
        self._pg_dsn = pg_dsn

    @classmethod
    def from_settings(cls) -> "ArchiveDownloader":
        from cip.common.settings import get_settings

        cfg = get_settings()
        pg_dsn = cfg.postgres.dsn.replace("postgresql+psycopg2://", "postgresql://")
        return cls(minio=MinIOClient.from_settings(), pg_dsn=pg_dsn)

    def download(
        self,
        snapshot_date: str,
        pipeline_run_id: str,
        force: bool = False,
    ) -> DownloadRecord:
        """
        Download the archive ZIP to MinIO landing.

        Args:
            snapshot_date:   ISO date — used as Hive partition key.
            pipeline_run_id: Airflow run_id or manual UUID.
            force:           Bypass idempotency guard and re-download.

        Returns:
            DownloadRecord with the log row id and landing path.
        """
        if not force:
            existing = self._check_idempotency(snapshot_date)
            if existing is not None:
                logger.info(
                    "Archive already downloaded — skipping (use force=True to re-run)",
                    extra={
                        "snapshot_date": snapshot_date,
                        "landing_path": existing.landing_path,
                        "log_id": existing.id,
                    },
                )
                return existing

        log_id = self._insert_log_row(pipeline_run_id, snapshot_date)
        started = time.monotonic()

        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                local_path = Path(tmp_dir) / _ARCHIVE_FILE

                logger.info(
                    "Downloading archive",
                    extra={"url": _ARCHIVE_URL, "snapshot_date": snapshot_date},
                )
                urllib.request.urlretrieve(_ARCHIVE_URL, local_path)  # noqa: S310

                file_size = local_path.stat().st_size
                if file_size < _MIN_EXPECTED_BYTES:
                    raise ValueError(
                        f"Archive too small: {file_size} bytes < {_MIN_EXPECTED_BYTES} minimum. "
                        f"Possible truncated download or upstream issue."
                    )

                checksum = sha256_file(local_path)

                logger.info(
                    "Archive downloaded — uploading to MinIO",
                    extra={
                        "file_size_bytes": file_size,
                        "checksum_sha256": checksum,
                        "snapshot_date": snapshot_date,
                    },
                )

                upload_result = self._minio.upload_to_landing(
                    local_path=local_path,
                    prefix="raw_zips",
                    snapshot_date=snapshot_date,
                )

            duration = round(time.monotonic() - started, 3)
            record = self._update_log_success(
                log_id=log_id,
                file_size_bytes=file_size,
                checksum_sha256=checksum,
                landing_path=upload_result.s3_path,
                snapshot_date=snapshot_date,
                pipeline_run_id=pipeline_run_id,
            )

            logger.info(
                "Archive download complete",
                extra={
                    "snapshot_date": snapshot_date,
                    "landing_path": upload_result.s3_path,
                    "duration_seconds": duration,
                },
            )
            return record

        except Exception as exc:
            self._update_log_failure(log_id, str(exc))
            raise

    # -------------------------------------------------------------------------
    # Control DB helpers
    # -------------------------------------------------------------------------

    def _check_idempotency(self, snapshot_date: str) -> DownloadRecord | None:
        import psycopg2

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, archive_file, source_url, snapshot_date::text,
                           landing_path, file_size_bytes, checksum_sha256, status::text
                    FROM control.archive_download_log
                    WHERE archive_file = %s
                      AND snapshot_date = %s
                      AND status = 'SUCCESS'
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (_ARCHIVE_FILE, snapshot_date),
                )
                row = cur.fetchone()

        if row is None:
            return None
        return DownloadRecord(
            id=row[0],
            archive_file=row[1],
            source_url=row[2],
            snapshot_date=row[3],
            landing_path=row[4] or "",
            file_size_bytes=row[5] or 0,
            checksum_sha256=row[6] or "",
            status=row[7],
        )

    def _insert_log_row(self, pipeline_run_id: str, snapshot_date: str) -> int:
        import psycopg2

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO control.archive_download_log (
                        pipeline_run_id, dag_id, archive_file,
                        source_url, snapshot_date, status
                    ) VALUES (%s, %s, %s, %s, %s, 'RUNNING')
                    RETURNING id
                    """,
                    (pipeline_run_id, _DAG_ID, _ARCHIVE_FILE, _ARCHIVE_URL, snapshot_date),
                )
                log_id = cur.fetchone()[0]
            conn.commit()
        return log_id

    def _update_log_success(
        self,
        log_id: int,
        file_size_bytes: int,
        checksum_sha256: str,
        landing_path: str,
        snapshot_date: str,
        pipeline_run_id: str,
    ) -> DownloadRecord:
        import psycopg2

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE control.archive_download_log
                    SET status = 'SUCCESS',
                        completed_at = NOW(),
                        file_size_bytes = %s,
                        checksum_sha256 = %s,
                        landing_path = %s
                    WHERE id = %s
                    """,
                    (file_size_bytes, checksum_sha256, landing_path, log_id),
                )
            conn.commit()

        return DownloadRecord(
            id=log_id,
            archive_file=_ARCHIVE_FILE,
            source_url=_ARCHIVE_URL,
            snapshot_date=snapshot_date,
            landing_path=landing_path,
            file_size_bytes=file_size_bytes,
            checksum_sha256=checksum_sha256,
            status="SUCCESS",
        )

    def _update_log_failure(self, log_id: int, error_message: str) -> None:
        import psycopg2

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE control.archive_download_log
                    SET status = 'FAILED',
                        completed_at = NOW(),
                        error_message = %s
                    WHERE id = %s
                    """,
                    (error_message[:2000], log_id),
                )
            conn.commit()

        logger.error(
            "Archive download failed",
            extra={"log_id": log_id, "error": error_message},
        )
