# src/cip/ingestion/match_data/download.py
#
# Downloads the Cricsheet all-matches ZIP archive to the MinIO landing zone
# and records the attempt in control.archive_download_log.
#
# Idempotency:
#   Skips if a SUCCESS row already exists for (archive_file, snapshot_date)
#   unless force=True.
#
# Usage:
#   downloader = MatchDataDownloader.from_settings()
#   record = downloader.download(snapshot_date="2026-05-01", pipeline_run_id="run-xyz")

from __future__ import annotations

import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from cip.common.logging import get_logger
from cip.ingestion.match_data.checksum import sha256_file
from cip.ingestion.io.minio import MinIOClient

logger = get_logger(__name__)

_ARCHIVE_URL = "https://cricsheet.org/downloads/all_json.zip"
_ARCHIVE_FILE = "all_json.zip"
_MIN_EXPECTED_BYTES = 10 * 1024 * 1024  # 10 MB
_DAG_ID = "dag_ingest_match_data"

# Public defaults — used by both `from_settings()` (full backfill) and by
# the incremental pipeline (which overrides via constructor / factory).
DEFAULT_ARCHIVE_URL = _ARCHIVE_URL
DEFAULT_ARCHIVE_FILE = _ARCHIVE_FILE
DEFAULT_MIN_EXPECTED_BYTES = _MIN_EXPECTED_BYTES


def _stream_download(
    url: str,
    dest_path: Path,
    *,
    retries: int = 5,
    chunk_size: int = 1024 * 1024,
) -> None:
    """Stream-download url to dest_path with resume and internal retry.

    Uses HTTP Range requests so a partial file is continued rather than
    restarted.  If the server returns 200 instead of 206 it does not support
    Range — the file is restarted from byte 0.  Retries up to `retries` times
    with exponential backoff before propagating the exception.
    """
    import requests  # noqa: PLC0415

    for attempt in range(1, retries + 2):  # attempts 1..retries+1
        try:
            downloaded = dest_path.stat().st_size if dest_path.exists() else 0
            headers = {"Range": f"bytes={downloaded}-"} if downloaded else {}
            mode = "ab" if downloaded else "wb"

            with requests.get(url, headers=headers, stream=True, timeout=120) as resp:
                if resp.status_code == 200 and downloaded:
                    # Server ignored the Range header — restart from scratch.
                    downloaded = 0
                    mode = "wb"
                elif resp.status_code not in (200, 206):
                    resp.raise_for_status()

                with open(dest_path, mode) as fh:
                    for chunk in resp.iter_content(chunk_size=chunk_size):
                        if chunk:
                            fh.write(chunk)
            return  # success

        except Exception as exc:
            if attempt > retries:
                raise
            wait = 2**attempt
            logger.warning(
                "Download attempt failed — retrying",
                extra={"attempt": attempt, "wait_seconds": wait, "error": str(exc)},
            )
            time.sleep(wait)


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


class MatchDataDownloader:
    """
    Downloads the Cricsheet all-matches ZIP and lands it in MinIO.

    Writes one row to control.archive_download_log per attempt.
    The row ID is returned so downstream tasks can reference it as
    archive_download_id in control.bronze_match_ingestion_log.
    """

    def __init__(
        self,
        minio: MinIOClient,
        pg_dsn: str,
        archive_file: str = DEFAULT_ARCHIVE_FILE,
        archive_url: str = DEFAULT_ARCHIVE_URL,
        min_expected_bytes: int = DEFAULT_MIN_EXPECTED_BYTES,
        dag_id: str = _DAG_ID,
    ) -> None:
        self._minio = minio
        self._pg_dsn = pg_dsn
        self._archive_file = archive_file
        self._archive_url = archive_url
        self._min_expected_bytes = min_expected_bytes
        self._dag_id = dag_id

    @classmethod
    def from_settings(
        cls,
        archive_file: str = DEFAULT_ARCHIVE_FILE,
        archive_url: str = DEFAULT_ARCHIVE_URL,
        min_expected_bytes: int = DEFAULT_MIN_EXPECTED_BYTES,
        dag_id: str = _DAG_ID,
    ) -> "MatchDataDownloader":
        from cip.common.settings import get_settings

        cfg = get_settings()
        pg_dsn = cfg.postgres.dsn.replace("postgresql+psycopg2://", "postgresql://")
        return cls(
            minio=MinIOClient.from_settings(),
            pg_dsn=pg_dsn,
            archive_file=archive_file,
            archive_url=archive_url,
            min_expected_bytes=min_expected_bytes,
            dag_id=dag_id,
        )

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
                local_path = Path(tmp_dir) / self._archive_file

                logger.info(
                    "Downloading archive",
                    extra={"url": self._archive_url, "snapshot_date": snapshot_date},
                )
                _stream_download(self._archive_url, local_path)

                file_size = local_path.stat().st_size
                if file_size < self._min_expected_bytes:
                    raise ValueError(
                        f"Archive too small: {file_size} bytes < {self._min_expected_bytes} minimum. "
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

                upload_result = self._minio.upload_to_source_files(
                    local_path=local_path,
                    prefix="match_data/zip",
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
                    (self._archive_file, snapshot_date),
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
                    (pipeline_run_id, self._dag_id, self._archive_file, self._archive_url, snapshot_date),
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
            archive_file=self._archive_file,
            source_url=self._archive_url,
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
