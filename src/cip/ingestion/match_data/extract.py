# src/cip/ingestion/match_data/extract.py
#
# Extracts JSON files from the Cricsheet ZIP archive in MinIO source-files
# bucket and re-uploads each JSON file to the match_data/json prefix.
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
from datetime import datetime, timezone
from pathlib import Path

from cip.common.logging import get_logger
from cip.ingestion.audit.match_file_audit import AuditRow, MatchFileAudit
from cip.ingestion.io.minio import MinIOClient
from cip.ingestion.match_data.checksum import sha256_bytes
from cip.ingestion.match_data.manifest import (
    ExtractionManifest,
    ManifestEntry,
    write_manifest,
)

logger = get_logger(__name__)

_ARCHIVE_FILE = "all_json.zip"
_DAG_ID = "dag_ingest_match_data"
_MAX_WORKERS = 20

DEFAULT_ARCHIVE_FILE = _ARCHIVE_FILE
DEFAULT_LOADED_BY_PIPELINE = "full"


@dataclass(frozen=True)
class ExtractionResult:
    snapshot_date: str
    archive_file: str
    file_count: int
    extracted_prefix: str
    manifest: ExtractionManifest


class MatchDataExtractor:
    """
    Downloads the ZIP from the source-files bucket, extracts all .json files,
    and uploads each to the match_data/json prefix.

    After upload, writes a _manifest.json for downstream verification
    and updates control.archive_download_log with the extraction path
    and file count.
    """

    def __init__(
        self,
        minio: MinIOClient,
        pg_dsn: str,
        archive_file: str = DEFAULT_ARCHIVE_FILE,
        loaded_by_pipeline: str = DEFAULT_LOADED_BY_PIPELINE,
        audit: MatchFileAudit | None = None,
    ) -> None:
        self._minio = minio
        self._pg_dsn = pg_dsn
        self._archive_file = archive_file
        # Stem of the archive (e.g. "all_json", "recently_added_2_json") used to
        # partition the MinIO JSON prefix so the full-backfill and incremental
        # pipelines never share files even when they share a snapshot_date.
        self._archive_stem = archive_file.removesuffix(".zip")
        self._loaded_by_pipeline = loaded_by_pipeline
        # Audit log handle — stamps landing_loaded_at + landing_path per file
        # right after the JSONs land in MinIO. Bronze loader later updates the
        # same rows with bronze_loaded_at and archive_path.
        self._audit = audit or MatchFileAudit(pg_dsn)

    @classmethod
    def from_settings(
        cls,
        archive_file: str = DEFAULT_ARCHIVE_FILE,
        loaded_by_pipeline: str = DEFAULT_LOADED_BY_PIPELINE,
    ) -> "MatchDataExtractor":
        from cip.common.settings import get_settings

        cfg = get_settings()
        pg_dsn = cfg.postgres.dsn.replace("postgresql+psycopg2://", "postgresql://")
        return cls(
            minio=MinIOClient.from_settings(),
            pg_dsn=pg_dsn,
            archive_file=archive_file,
            loaded_by_pipeline=loaded_by_pipeline,
        )

    def extract(
        self,
        snapshot_date: str,
        pipeline_run_id: str,
        force: bool = False,
    ) -> ExtractionResult:
        """
        Extract JSON files from the source-files ZIP to the match_data/json prefix.

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
                    archive_file=self._archive_file,
                    file_count=manifest.file_count,
                    extracted_prefix=existing,
                    manifest=manifest,
                )

        log_id = self._get_log_id(snapshot_date)
        cfg_storage = self._get_storage_cfg()
        source_files_bucket = cfg_storage.bucket_source_files
        zip_key = f"match_data/zip/snapshot_date={snapshot_date}/{self._archive_file}"
        extracted_prefix = (
            f"s3://{source_files_bucket}/match_data/json/"
            f"snapshot_date={snapshot_date}/archive={self._archive_stem}/"
        )

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
            local_zip = Path(tmp_dir) / self._archive_file
            self._minio.download_file(source_files_bucket, zip_key, local_zip)

            entries, failed = self._extract_and_upload(
                local_zip=local_zip,
                snapshot_date=snapshot_date,
                source_files_bucket=source_files_bucket,
            )

        if failed:
            logger.warning(
                "Some files failed during extraction",
                extra={"failed_count": len(failed), "snapshot_date": snapshot_date},
            )

        manifest = ExtractionManifest(
            snapshot_date=snapshot_date,
            archive_file=self._archive_file,
            file_count=len(entries),
            entries=entries,
        )
        write_manifest(self._minio, manifest)

        if log_id is not None:
            self._update_log_extracted(log_id, extracted_prefix, len(entries))

        # Stamp landing_loaded_at + landing_path in the file-level audit log.
        # ON CONFLICT DO NOTHING — re-runs and overlap with prior extracts
        # both collapse to a single row per (file_name, content_hash).
        self._insert_audit_landing_rows(
            entries=entries,
            extracted_prefix=extracted_prefix,
            archive_download_id=log_id,
            pipeline_run_id=pipeline_run_id,
        )

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
            archive_file=self._archive_file,
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
        source_files_bucket: str,
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
            key = (
                f"match_data/json/snapshot_date={snapshot_date}/"
                f"archive={self._archive_stem}/{Path(file_name).name}"
            )
            try:
                self._minio.upload_bytes(
                    data=content,
                    bucket=source_files_bucket,
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
                    (self._archive_file, snapshot_date),
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
                    (self._archive_file, snapshot_date),
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
        from cip.ingestion.match_data.manifest import read_manifest

        return read_manifest(self._minio, snapshot_date, self._archive_file)

    # -------------------------------------------------------------------------
    # Audit log integration
    # -------------------------------------------------------------------------

    def _insert_audit_landing_rows(
        self,
        entries: list[ManifestEntry],
        extracted_prefix: str,
        archive_download_id: int | None,
        pipeline_run_id: str,
    ) -> None:
        if not entries:
            return

        now = datetime.now(timezone.utc)
        rows: list[AuditRow] = []
        for entry in entries:
            match_id = entry.file_name.removesuffix(".json")
            landing_path = f"{extracted_prefix}{entry.file_name}"
            rows.append(
                AuditRow(
                    file_name=entry.file_name,
                    content_hash=entry.checksum_sha256,
                    match_id=match_id,
                    archive_file=self._archive_file,
                    archive_download_id=archive_download_id,
                    landing_path=landing_path,
                    loaded_by_pipeline=self._loaded_by_pipeline,
                    pipeline_run_id=pipeline_run_id,
                    landing_loaded_at=now,
                )
            )

        inserted = self._audit.insert_landing(rows)
        logger.info(
            "Audit landing rows inserted",
            extra={"requested": len(rows), "inserted": inserted},
        )
