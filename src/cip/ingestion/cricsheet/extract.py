# src/cip/ingestion/cricsheet/extract.py
#
# Extracts JSON files from the Cricsheet ZIP archive in MinIO landing and
# re-uploads each JSON file to the extracted_json prefix.
#
# Idempotency:
#   Skips if control.archive_download_log shows extracted_path IS NOT NULL
#   and status = 'SUCCESS' for (archive_file, snapshot_date), unless force=True.
#
# Concurrency:
#   Uses ThreadPoolExecutor with 20 workers to parallelise MinIO uploads
#   across the ~21,000 JSON files in all_json.zip.

from __future__ import annotations

import tempfile
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from cip.common.logging import get_logger
from cip.ingestion.cricsheet.checksum import sha256_bytes
from cip.ingestion.cricsheet.manifest import (
    ExtractionManifest,
    ManifestEntry,
    write_manifest,
)
from cip.ingestion.io.minio import MinIOClient

logger = get_logger(__name__)

_ARCHIVE_FILE = "all_json.zip"
_DAG_ID = "dag_ingest_cricsheet_archives"
_MAX_WORKERS = 20


@dataclass(frozen=True)
class ExtractionResult:
    snapshot_date: str
    archive_file: str
    file_count: int
    extracted_prefix: str
    manifest: ExtractionManifest


class ArchiveExtractor:
    """
    Downloads the ZIP from MinIO landing, extracts all .json files,
    and uploads each to the extracted_json prefix.

    After upload, writes a _manifest.json for downstream verification
    and updates control.archive_download_log with the extraction path
    and file count.
    """

    def __init__(self, minio: MinIOClient, pg_dsn: str) -> None:
        self._minio = minio
        self._pg_dsn = pg_dsn

    @classmethod
    def from_settings(cls) -> "ArchiveExtractor":
        from cip.common.settings import get_settings

        cfg = get_settings()
        pg_dsn = cfg.postgres.dsn.replace("postgresql+psycopg2://", "postgresql://")
        return cls(minio=MinIOClient.from_settings(), pg_dsn=pg_dsn)

    def extract(
        self,
        snapshot_date: str,
        pipeline_run_id: str,
        force: bool = False,
    ) -> ExtractionResult:
        """
        Extract JSON files from the landing ZIP to extracted_json prefix.

        Args:
            snapshot_date:   ISO date — Hive partition key.
            pipeline_run_id: Airflow run_id or manual UUID.
            force:           Bypass idempotency guard and re-extract.

        Returns:
            ExtractionResult with file count and manifest.
        """
        if not force:
            existing = self._check_idempotency(snapshot_date)
            if existing is not None:
                logger.info(
                    "Archive already extracted — skipping (use force=True to re-run)",
                    extra={"snapshot_date": snapshot_date, "extracted_path": existing},
                )
                manifest = self._read_existing_manifest(snapshot_date)
                return ExtractionResult(
                    snapshot_date=snapshot_date,
                    archive_file=_ARCHIVE_FILE,
                    file_count=manifest.file_count,
                    extracted_prefix=existing,
                    manifest=manifest,
                )

        log_id = self._get_log_id(snapshot_date)
        cfg_storage = self._get_storage_cfg()
        landing_bucket = cfg_storage.bucket_landing
        zip_key = f"raw_zips/snapshot_date={snapshot_date}/{_ARCHIVE_FILE}"
        extracted_prefix = f"s3://{landing_bucket}/extracted_json/snapshot_date={snapshot_date}/"

        started = time.monotonic()
        logger.info(
            "Extracting archive",
            extra={
                "zip_key": zip_key,
                "snapshot_date": snapshot_date,
                "extracted_prefix": extracted_prefix,
            },
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            local_zip = Path(tmp_dir) / _ARCHIVE_FILE
            self._minio.download_file(landing_bucket, zip_key, local_zip)

            entries, failed = self._extract_and_upload(
                local_zip=local_zip,
                snapshot_date=snapshot_date,
                landing_bucket=landing_bucket,
            )

        if failed:
            logger.warning(
                "Some files failed during extraction",
                extra={"failed_count": len(failed), "snapshot_date": snapshot_date},
            )

        manifest = ExtractionManifest(
            snapshot_date=snapshot_date,
            archive_file=_ARCHIVE_FILE,
            file_count=len(entries),
            entries=entries,
        )
        write_manifest(self._minio, manifest)

        if log_id is not None:
            self._update_log_extracted(log_id, extracted_prefix, len(entries))

        duration = round(time.monotonic() - started, 3)
        logger.info(
            "Extraction complete",
            extra={
                "snapshot_date": snapshot_date,
                "file_count": len(entries),
                "failed": len(failed),
                "duration_seconds": duration,
            },
        )

        return ExtractionResult(
            snapshot_date=snapshot_date,
            archive_file=_ARCHIVE_FILE,
            file_count=len(entries),
            extracted_prefix=extracted_prefix,
            manifest=manifest,
        )

    # -------------------------------------------------------------------------
    # Extraction helpers
    # -------------------------------------------------------------------------

    def _extract_and_upload(
        self,
        local_zip: Path,
        snapshot_date: str,
        landing_bucket: str,
    ) -> tuple[list[ManifestEntry], list[str]]:
        """
        Open the ZIP and upload each .json file via ThreadPoolExecutor.
        Returns (successful_entries, failed_file_names).
        """
        with zipfile.ZipFile(local_zip, "r") as zf:
            json_names = [n for n in zf.namelist() if n.endswith(".json")]
            # Read all content before closing the ZIP (not safe to share across threads)
            file_contents: dict[str, bytes] = {name: zf.read(name) for name in json_names}

        entries: list[ManifestEntry] = []
        failed: list[str] = []

        def _upload_one(file_name: str, content: bytes) -> ManifestEntry | None:
            key = f"extracted_json/snapshot_date={snapshot_date}/{Path(file_name).name}"
            try:
                self._minio.upload_bytes(
                    data=content,
                    bucket=landing_bucket,
                    key=key,
                    content_type="application/json",
                )
                return ManifestEntry(
                    file_name=Path(file_name).name,
                    size_bytes=len(content),
                    checksum_sha256=sha256_bytes(content),
                )
            except Exception as exc:
                logger.error(
                    "Failed to upload extracted file",
                    extra={"file_name": file_name, "error": str(exc)},
                )
                return None

        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
            futures = {executor.submit(_upload_one, name, content): name for name, content in file_contents.items()}
            for future in as_completed(futures):
                name = futures[future]
                result = future.result()
                if result is not None:
                    entries.append(result)
                else:
                    failed.append(name)

        return entries, failed

    # -------------------------------------------------------------------------
    # Control DB helpers
    # -------------------------------------------------------------------------

    def _get_storage_cfg(self):
        from cip.common.settings import get_settings

        return get_settings().storage

    def _check_idempotency(self, snapshot_date: str) -> str | None:
        """Return extracted_path if already extracted successfully, else None."""
        import psycopg2

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT extracted_path
                    FROM control.archive_download_log
                    WHERE archive_file = %s
                      AND snapshot_date = %s
                      AND status = 'SUCCESS'
                      AND extracted_path IS NOT NULL
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (_ARCHIVE_FILE, snapshot_date),
                )
                row = cur.fetchone()
        return row[0] if row else None

    def _get_log_id(self, snapshot_date: str) -> int | None:
        """Return the latest archive_download_log id for this snapshot."""
        import psycopg2

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id FROM control.archive_download_log
                    WHERE archive_file = %s AND snapshot_date = %s
                    ORDER BY id DESC LIMIT 1
                    """,
                    (_ARCHIVE_FILE, snapshot_date),
                )
                row = cur.fetchone()
        return row[0] if row else None

    def _update_log_extracted(self, log_id: int, extracted_path: str, file_count: int) -> None:
        import psycopg2

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE control.archive_download_log
                    SET extracted_path = %s,
                        file_count = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (extracted_path, file_count, log_id),
                )
            conn.commit()

    def _read_existing_manifest(self, snapshot_date: str) -> ExtractionManifest:
        from cip.ingestion.cricsheet.manifest import read_manifest

        return read_manifest(self._minio, snapshot_date)
