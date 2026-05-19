# platform/ingestion/register/download.py
#
# HTTP downloader for the Cricsheet Register files (people.csv, names.csv).
#
# Responsibilities:
#   - Download people.csv and names.csv from cricsheet.org
#   - Compute SHA-256 checksum of each file
#   - Detect schema drift vs the previous snapshot
#   - Upload raw files to s3://cricket-source-files/people_and_names/csv/snapshot_date=.../
#   - Write / update rows in control.register_ingestion_log
#   - Write schema fingerprint to control.register_schema_versions
#
# Called by:
#   platform/ingestion/jobs/ingest_people_and_names.py  (job entry point)
#   orchestration/airflow/dags/dag_ingest_people_and_names.py (via PythonOperator)
#
# Usage:
#   from cip.ingestion.people_and_names.download import PeopleAndNamesDownloader
#   downloader = PeopleAndNamesDownloader.from_settings()
#   results = downloader.run(snapshot_date="2026-05-10", pipeline_run_id="manual-001")

from __future__ import annotations

import hashlib
import tempfile
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import psycopg2
import psycopg2.extras

from cip.common.logging import get_logger
from cip.common.settings import get_settings
from cip.ingestion.io.minio import MinIOClient

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Cricsheet Register source URLs
# ---------------------------------------------------------------------------
REGISTER_SOURCES: dict[str, str] = {
    "people.csv": "https://cricsheet.org/register/people.csv",
    "names.csv": "https://cricsheet.org/register/names.csv",
}

# ---------------------------------------------------------------------------
# DQ thresholds — flag anomaly if row count changes by more than this %
# ---------------------------------------------------------------------------
_ROW_COUNT_ANOMALY_PCT = 5.0  # 5% change triggers anomaly flag
_DOWNLOAD_TIMEOUT_SECONDS = 120
_MIN_EXPECTED_ROWS = {"people.csv": 5000, "names.csv": 5000}


# ===========================================================================
# Result dataclasses
# ===========================================================================


@dataclass
class FileDownloadResult:
    """Result of downloading a single Register file."""

    source_file: str
    source_url: str
    snapshot_date: str
    pipeline_run_id: str

    # File stats
    local_path: Path | None = None
    file_size_bytes: int = 0
    row_count: int = 0
    checksum_sha256: str = ""

    # Landing path
    landing_path: str = ""

    # Schema fingerprint
    column_names: list[str] = field(default_factory=list)
    schema_hash: str = ""
    is_schema_changed: bool = False
    new_columns: list[str] = field(default_factory=list)
    removed_columns: list[str] = field(default_factory=list)

    # Control DB row id
    ingestion_log_id: int | None = None

    # Status
    status: str = "RUNNING"  # RUNNING | SUCCESS | SKIPPED | FAILED
    error_message: str | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None


@dataclass
class PeopleAndNamesDownloadSummary:
    """Aggregated result of a full register download run."""

    pipeline_run_id: str
    snapshot_date: str
    results: list[FileDownloadResult] = field(default_factory=list)

    @property
    def all_succeeded(self) -> bool:
        return all(r.status in ("SUCCESS", "SKIPPED") for r in self.results)

    @property
    def any_schema_changed(self) -> bool:
        return any(r.is_schema_changed for r in self.results)


# ===========================================================================
# PeopleAndNamesDownloader
# ===========================================================================


class PeopleAndNamesDownloader:
    """
    Downloads Cricsheet Register CSV files, validates them, uploads to landing,
    and writes full audit trail to the control schema.

    Instantiation:
        downloader = PeopleAndNamesDownloader.from_settings()

    Usage:
        summary = downloader.run(snapshot_date="2026-05-10")
    """

    def __init__(
        self,
        minio_client: MinIOClient,
        postgres_dsn: str,
        download_timeout: int = _DOWNLOAD_TIMEOUT_SECONDS,
    ) -> None:
        self._minio = minio_client
        self._postgres_dsn = postgres_dsn
        self._timeout = download_timeout

    @classmethod
    def from_settings(cls) -> "PeopleAndNamesDownloader":
        cfg = get_settings()
        return cls(
            minio_client=MinIOClient.from_settings(),
            postgres_dsn=cfg.postgres.dsn,
            download_timeout=_DOWNLOAD_TIMEOUT_SECONDS,
        )

    # -----------------------------------------------------------------------
    # Public entry point
    # -----------------------------------------------------------------------

    def run(
        self,
        snapshot_date: str | None = None,
        pipeline_run_id: str | None = None,
        force: bool = False,
    ) -> PeopleAndNamesDownloadSummary:
        """
        Execute the full register download pipeline for all source files.

        Args:
            snapshot_date:    ISO date string (YYYY-MM-DD). Defaults to today.
            pipeline_run_id:  Airflow run_id or manual UUID. Auto-generated if None.
            force:            If True, re-download even if already logged as SUCCESS.

        Returns:
            PeopleAndNamesDownloadSummary with per-file results and overall status.
        """
        snapshot_date = snapshot_date or date.today().isoformat()
        pipeline_run_id = pipeline_run_id or f"manual-{uuid.uuid4().hex[:8]}"

        logger.info(
            "Starting Register download run",
            extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id},
        )

        summary = PeopleAndNamesDownloadSummary(
            pipeline_run_id=pipeline_run_id,
            snapshot_date=snapshot_date,
        )

        with tempfile.TemporaryDirectory(prefix="register_dl_") as tmp_dir:
            tmp_path = Path(tmp_dir)

            for source_file, source_url in REGISTER_SOURCES.items():
                result = FileDownloadResult(
                    source_file=source_file,
                    source_url=source_url,
                    snapshot_date=snapshot_date,
                    pipeline_run_id=pipeline_run_id,
                )
                try:
                    self._process_file(
                        result=result,
                        tmp_path=tmp_path,
                        force=force,
                    )
                except Exception as exc:
                    result.status = "FAILED"
                    result.error_message = str(exc)
                    result.completed_at = datetime.now(timezone.utc)
                    logger.error(
                        "Register file download failed",
                        extra={
                            "source_file": source_file,
                            "error": str(exc),
                            "pipeline_run_id": pipeline_run_id,
                        },
                    )
                    self._upsert_ingestion_log(result)
                    raise  # re-raise so Airflow marks the task FAILED

                summary.results.append(result)

        logger.info(
            "Register download run complete",
            extra={
                "pipeline_run_id": pipeline_run_id,
                "snapshot_date": snapshot_date,
                "all_succeeded": summary.all_succeeded,
                "any_schema_changed": summary.any_schema_changed,
            },
        )
        return summary

    # -----------------------------------------------------------------------
    # Per-file orchestration
    # -----------------------------------------------------------------------

    def _process_file(
        self,
        result: FileDownloadResult,
        tmp_path: Path,
        force: bool,
    ) -> None:
        """Full pipeline for a single Register CSV file."""

        # Step 1 — Idempotency: skip if already successfully loaded
        if not force and self._already_loaded(result.source_file, result.snapshot_date):
            logger.info(
                "Skipping — already loaded",
                extra={
                    "source_file": result.source_file,
                    "snapshot_date": result.snapshot_date,
                },
            )
            result.status = "SKIPPED"
            result.completed_at = datetime.now(timezone.utc)
            return

        # Step 2 — Insert RUNNING row in control DB
        result.ingestion_log_id = self._insert_ingestion_log(result)

        # Step 3 — Download file
        local_path = tmp_path / result.source_file
        self._download_http(result.source_url, local_path)
        result.local_path = local_path
        result.file_size_bytes = local_path.stat().st_size
        result.checksum_sha256 = self._sha256_file(local_path)

        # Step 4 — Parse header + row count
        result.column_names, result.row_count = self._parse_csv_meta(local_path)
        result.schema_hash = self._hash_columns(result.column_names)

        logger.info(
            "File downloaded",
            extra={
                "source_file": result.source_file,
                "size_bytes": result.file_size_bytes,
                "row_count": result.row_count,
                "checksum": result.checksum_sha256,
                "schema_hash": result.schema_hash,
            },
        )

        # Step 5 — Minimum row count guard
        min_rows = _MIN_EXPECTED_ROWS.get(result.source_file, 1)
        if result.row_count < min_rows:
            raise ValueError(
                f"{result.source_file} has only {result.row_count} rows "
                f"(expected >= {min_rows}). Possible truncated download."
            )

        # Step 6 — Schema drift detection
        self._detect_schema_drift(result)

        # Step 7 — Upload to MinIO source-files bucket
        upload = self._minio.upload_to_source_files(
            local_path=local_path,
            prefix="people_and_names/csv",
            snapshot_date=result.snapshot_date,
            extra_metadata={
                "source_file": result.source_file,
                "pipeline_run_id": result.pipeline_run_id,
                "checksum_sha256": result.checksum_sha256,
                "row_count": str(result.row_count),
            },
        )
        result.landing_path = upload.s3_path

        # Step 8 — Write schema version
        self._upsert_schema_version(result)

        # Step 9 — Write change log delta
        self._write_change_log(result)

        # Step 10 — Mark SUCCESS in control DB
        result.status = "SUCCESS"
        result.completed_at = datetime.now(timezone.utc)
        self._upsert_ingestion_log(result)

        logger.info(
            "Register file landing complete",
            extra={
                "source_file": result.source_file,
                "landing_path": result.landing_path,
                "row_count": result.row_count,
                "schema_changed": result.is_schema_changed,
            },
        )

    # -----------------------------------------------------------------------
    # HTTP Download
    # -----------------------------------------------------------------------

    def _download_http(self, url: str, dest: Path) -> None:
        """Stream-download a URL to a local file with timeout."""
        logger.info("Downloading", extra={"url": url, "dest": str(dest)})
        with httpx.stream("GET", url, timeout=self._timeout, follow_redirects=True) as resp:
            resp.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=65536):
                    f.write(chunk)

    # -----------------------------------------------------------------------
    # CSV parsing helpers
    # -----------------------------------------------------------------------

    @staticmethod
    def _parse_csv_meta(path: Path) -> tuple[list[str], int]:
        """
        Read only the header + count rows without loading into memory.
        Returns (column_names, data_row_count).
        """
        with open(path, encoding="utf-8") as f:
            header_line = f.readline().strip()
            columns = [c.strip() for c in header_line.split(",")]
            row_count = sum(1 for _ in f)
        return columns, row_count

    @staticmethod
    def _hash_columns(columns: list[str]) -> str:
        """SHA-256 of sorted column names — stable fingerprint for drift detection."""
        fingerprint = ",".join(sorted(columns))
        return hashlib.sha256(fingerprint.encode()).hexdigest()

    # -----------------------------------------------------------------------
    # Checksum
    # -----------------------------------------------------------------------

    @staticmethod
    def _sha256_file(path: Path, chunk_size: int = 65536) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(chunk_size):
                h.update(chunk)
        return h.hexdigest()

    # -----------------------------------------------------------------------
    # Control DB — PostgreSQL operations
    # -----------------------------------------------------------------------

    def _get_conn(self):
        """Return a raw psycopg2 connection. Caller must close."""
        dsn = self._postgres_dsn.replace("postgresql+psycopg2://", "postgresql://")
        return psycopg2.connect(dsn)

    def _already_loaded(self, source_file: str, snapshot_date: str) -> bool:
        """Return True if this (source_file, snapshot_date) was already SUCCESS in landing."""
        sql = """
            SELECT 1 FROM control.register_ingestion_log
            WHERE source_file = %s
              AND snapshot_date = %s
              AND status = 'SUCCESS'
            LIMIT 1
        """
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (source_file, snapshot_date))
                return cur.fetchone() is not None

    def _insert_ingestion_log(self, result: FileDownloadResult) -> int:
        """Insert a RUNNING row and return the generated id."""
        sql = """
            INSERT INTO control.register_ingestion_log (
                pipeline_run_id, dag_id, source_file, source_url,
                snapshot_date, status, started_at
            ) VALUES (%s, %s, %s, %s, %s, 'RUNNING', %s)
            RETURNING id
        """
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        result.pipeline_run_id,
                        "ingest_people_and_names_bronze",
                        result.source_file,
                        result.source_url,
                        result.snapshot_date,
                        result.started_at,
                    ),
                )
                row_id = cur.fetchone()[0]
            conn.commit()
        return row_id

    def _upsert_ingestion_log(self, result: FileDownloadResult) -> None:
        """Update the existing RUNNING row with final status and metadata."""
        sql = """
            UPDATE control.register_ingestion_log SET
                file_size_bytes  = %s,
                row_count        = %s,
                checksum_sha256  = %s,
                landing_path     = %s,
                status           = %s::control.pipeline_status,
                completed_at     = %s,
                error_message    = %s,
                updated_at       = NOW()
            WHERE id = %s
        """
        if result.ingestion_log_id is None:
            return
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        result.file_size_bytes or None,
                        result.row_count or None,
                        result.checksum_sha256 or None,
                        result.landing_path or None,
                        result.status,
                        result.completed_at,
                        result.error_message,
                        result.ingestion_log_id,
                    ),
                )
            conn.commit()

    def _get_previous_schema(self, source_file: str) -> dict[str, Any] | None:
        """Fetch the most recent schema version for this file."""
        sql = """
            SELECT id, column_names, schema_hash
            FROM control.register_schema_versions
            WHERE source_file = %s
            ORDER BY snapshot_date DESC
            LIMIT 1
        """
        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (source_file,))
                row = cur.fetchone()
                return dict(row) if row else None

    def _detect_schema_drift(self, result: FileDownloadResult) -> None:
        """Compare current column set to previous snapshot. Sets result fields in-place."""
        prev = self._get_previous_schema(result.source_file)
        if prev is None:
            logger.info(
                "First-time schema registration",
                extra={"source_file": result.source_file},
            )
            return

        prev_cols = set(prev["column_names"])
        curr_cols = set(result.column_names)

        result.new_columns = sorted(curr_cols - prev_cols)
        result.removed_columns = sorted(prev_cols - curr_cols)
        result.is_schema_changed = bool(result.new_columns or result.removed_columns)

        if result.is_schema_changed:
            logger.warning(
                "Schema drift detected",
                extra={
                    "source_file": result.source_file,
                    "new_columns": result.new_columns,
                    "removed_columns": result.removed_columns,
                    "snapshot_date": result.snapshot_date,
                },
            )

    def _upsert_schema_version(self, result: FileDownloadResult) -> None:
        """Insert schema fingerprint for this snapshot (idempotent on conflict)."""
        prev = self._get_previous_schema(result.source_file)
        key_cols = [c for c in result.column_names if c.startswith("key_")]

        sql = """
            INSERT INTO control.register_schema_versions (
                pipeline_run_id, source_file, snapshot_date,
                column_names, column_count, key_columns, schema_hash,
                is_schema_changed, new_columns, removed_columns,
                previous_schema_id, detected_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (source_file, snapshot_date) DO UPDATE SET
                schema_hash       = EXCLUDED.schema_hash,
                is_schema_changed = EXCLUDED.is_schema_changed,
                new_columns       = EXCLUDED.new_columns,
                removed_columns   = EXCLUDED.removed_columns
        """
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        result.pipeline_run_id,
                        result.source_file,
                        result.snapshot_date,
                        result.column_names,
                        len(result.column_names),
                        key_cols,
                        result.schema_hash,
                        result.is_schema_changed,
                        result.new_columns,
                        result.removed_columns,
                        prev["id"] if prev else None,
                    ),
                )
            conn.commit()

    def _write_change_log(self, result: FileDownloadResult) -> None:
        """Compute delta vs previous snapshot and write to register_change_log."""
        sql_prev = """
            SELECT row_count FROM control.register_ingestion_log
            WHERE source_file = %s
              AND status = 'SUCCESS'
            ORDER BY snapshot_date DESC
            LIMIT 1
        """
        sql_insert = """
            INSERT INTO control.register_change_log (
                pipeline_run_id, source_file, snapshot_date,
                current_row_count, previous_row_count,
                delta_rows, delta_pct,
                is_anomaly, anomaly_reason, recorded_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (source_file, snapshot_date) DO NOTHING
        """
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_prev, (result.source_file,))
                row = cur.fetchone()
                prev_count = row[0] if row else None

                delta = None
                delta_pct = None
                is_anomaly = False
                anomaly_reason = None

                if prev_count is not None and prev_count > 0:
                    delta = result.row_count - prev_count
                    delta_pct = round((delta / prev_count) * 100, 4)
                    if abs(delta_pct) > _ROW_COUNT_ANOMALY_PCT:
                        is_anomaly = True
                        anomaly_reason = f"Row count changed by {delta_pct:.2f}% ({prev_count} → {result.row_count})"
                        logger.warning(
                            "Row count anomaly detected",
                            extra={
                                "source_file": result.source_file,
                                "delta_pct": delta_pct,
                                "prev": prev_count,
                                "current": result.row_count,
                            },
                        )

                cur.execute(
                    sql_insert,
                    (
                        result.pipeline_run_id,
                        result.source_file,
                        result.snapshot_date,
                        result.row_count,
                        prev_count,
                        delta,
                        delta_pct,
                        is_anomaly,
                        anomaly_reason,
                    ),
                )
            conn.commit()
