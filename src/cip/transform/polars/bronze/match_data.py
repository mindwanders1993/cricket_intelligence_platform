# src/cip/transform/polars/bronze/match_data.py
#
# Bronze loader for Cricsheet match JSON documents.
#
# Reads all extracted JSON files for a snapshot from the MinIO landing zone,
# parses the minimal header fields, attaches a revision number, and appends
# to bronze.match_data via PolarsIcebergWriter.
#
# Schema (all columns are strings — Bronze rule):
#   match_id, revision, match_type, gender, season, match_date,
#   team_a, team_b, venue, city, raw_json
#   + standard META columns (_snapshot_date, _ingested_at, etc.)
#
# Revision logic:
#   revision = MAX(existing revision for match_id across all snapshots) + 1
#   For new matches (first-ever load): revision = 1
#
# Idempotency:
#   Checks control.bronze_match_ingestion_log for status=SUCCESS on
#   (archive_file, snapshot_date) before loading. Use force=True to skip.
#   force=True also deletes the _snapshot_date partition before rewriting.
#
# Usage:
#   loader = MatchBronzeLoader.from_settings()
#   result = loader.load(snapshot_date="2026-05-01", pipeline_run_id="run-xyz")

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import polars as pl

from cip.common.contracts.enums import Layer
from cip.common.contracts.naming import META, TableName
from cip.common.logging import get_logger
from cip.ingestion.io.minio import MinIOClient
from cip.transform.shared.writers import PolarsIcebergWriter

logger = get_logger(__name__)

_TABLE = TableName.bronze("match_data")
_PARTITION_COL = META.SNAPSHOT_DATE
_ARCHIVE_FILE = "all_json.zip"
_SOURCE_URL = "https://cricsheet.org/downloads/all_json.zip"
_DAG_ID = "dag_ingest_match_data"
_MAX_WORKERS = 20
# Number of JSON files processed (read + parse + write) per Iceberg append batch.
# Keeps peak memory bounded — the full 21k-file run would otherwise materialise
# every match's raw_json (~1 GB+ of strings) in memory at once and OOM small
# Airflow workers.
_BATCH_SIZE = 2000


@dataclass
class MatchLoadResult:
    snapshot_date: str
    pipeline_run_id: str
    files_attempted: int
    files_succeeded: int
    files_failed: int
    rows_written: int
    duration_seconds: float
    archive_download_id: int | None = None


class MatchBronzeLoader:
    """
    Reads extracted JSON files from MinIO and writes to bronze.match_data.

    Two modes:
        load()     — append, with idempotency guard (default)
        overwrite_snapshot() — delete partition first, then load
    """

    def __init__(
        self,
        minio: MinIOClient,
        writer: PolarsIcebergWriter,
        pg_dsn: str,
    ) -> None:
        self._minio = minio
        self._writer = writer
        self._pg_dsn = pg_dsn

    @classmethod
    def from_settings(cls) -> "MatchBronzeLoader":
        from cip.common.settings import get_settings

        cfg = get_settings()
        pg_dsn = cfg.postgres.dsn.replace("postgresql+psycopg2://", "postgresql://")
        return cls(
            minio=MinIOClient.from_settings(),
            writer=PolarsIcebergWriter.from_settings(),
            pg_dsn=pg_dsn,
        )

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def load(
        self,
        snapshot_date: str,
        pipeline_run_id: str,
        archive_download_id: int | None = None,
        force: bool = False,
    ) -> MatchLoadResult:
        """
        Load extracted JSON files into bronze.match_data.

        Args:
            snapshot_date:        ISO date — partition key.
            pipeline_run_id:      Airflow run_id or manual UUID.
            archive_download_id:  FK to control.archive_download_log row.
            force:                Bypass idempotency guard; delete partition first.

        Returns:
            MatchLoadResult with row counts and timing.
        """
        if not force:
            existing = self._check_idempotency(snapshot_date)
            if existing is not None:
                logger.info(
                    "Bronze match documents already loaded — skipping",
                    extra={"snapshot_date": snapshot_date, "log_id": existing},
                )
                return MatchLoadResult(
                    snapshot_date=snapshot_date,
                    pipeline_run_id=pipeline_run_id,
                    files_attempted=0,
                    files_succeeded=0,
                    files_failed=0,
                    rows_written=0,
                    duration_seconds=0.0,
                    archive_download_id=archive_download_id,
                )

        if force:
            self._delete_partition(snapshot_date)

        log_id = self._insert_log_row(pipeline_run_id, snapshot_date, archive_download_id)
        started = time.monotonic()

        try:
            result = self._run_load(
                snapshot_date=snapshot_date,
                pipeline_run_id=pipeline_run_id,
                archive_download_id=archive_download_id,
                log_id=log_id,
                started=started,
            )
            self._update_log_success(log_id, result)
            return result

        except Exception as exc:
            self._update_log_failure(log_id, str(exc))
            raise

    def overwrite_snapshot(
        self,
        snapshot_date: str,
        pipeline_run_id: str,
        archive_download_id: int | None = None,
    ) -> MatchLoadResult:
        """Idempotent re-run: delete partition then load."""
        return self.load(
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            archive_download_id=archive_download_id,
            force=True,
        )

    # -------------------------------------------------------------------------
    # Core load logic
    # -------------------------------------------------------------------------

    def _run_load(
        self,
        snapshot_date: str,
        pipeline_run_id: str,
        archive_download_id: int | None,
        log_id: int,
        started: float,
    ) -> MatchLoadResult:
        json_files = self._list_json_files(snapshot_date)
        files_attempted = len(json_files)

        logger.info(
            "Reading JSON files from landing",
            extra={"file_count": files_attempted, "snapshot_date": snapshot_date},
        )

        existing_revisions = self._fetch_existing_revisions()

        total_succeeded = 0
        total_failed_keys: list[str] = []
        total_rows_written = 0

        for batch_start in range(0, files_attempted, _BATCH_SIZE):
            batch = json_files[batch_start : batch_start + _BATCH_SIZE]
            batch_num = batch_start // _BATCH_SIZE + 1
            total_batches = (files_attempted + _BATCH_SIZE - 1) // _BATCH_SIZE

            logger.info(
                "Processing batch",
                extra={
                    "batch": f"{batch_num}/{total_batches}",
                    "batch_size": len(batch),
                    "files_processed_so_far": batch_start,
                },
            )

            records, failed_files = self._read_json_files(batch)
            total_succeeded += len(records)
            total_failed_keys.extend(failed_files)

            if records:
                rows = self._attach_revisions(records, existing_revisions)
                df = pl.DataFrame(rows, schema=_bronze_schema())
                rows_written = self._writer.create_and_append(
                    df=df,
                    fqn=_TABLE,
                    snapshot_date=snapshot_date,
                    layer=Layer.BRONZE,
                    partition_cols=[_PARTITION_COL],
                    pipeline_run_id=pipeline_run_id,
                    source_file=_ARCHIVE_FILE,
                    source_url=_SOURCE_URL,
                )
                total_rows_written += rows_written
                # Drop large refs so GC can reclaim before the next batch loads.
                del records, rows, df

        if total_succeeded == 0:
            logger.warning(
                "No rows parsed — Iceberg write skipped",
                extra={"snapshot_date": snapshot_date},
            )

        duration = round(time.monotonic() - started, 3)

        logger.info(
            "Bronze match documents loaded",
            extra={
                "snapshot_date": snapshot_date,
                "files_attempted": files_attempted,
                "files_succeeded": total_succeeded,
                "files_failed": len(total_failed_keys),
                "rows_written": total_rows_written,
                "duration_seconds": duration,
            },
        )

        return MatchLoadResult(
            snapshot_date=snapshot_date,
            pipeline_run_id=pipeline_run_id,
            files_attempted=files_attempted,
            files_succeeded=total_succeeded,
            files_failed=len(total_failed_keys),
            rows_written=total_rows_written,
            duration_seconds=duration,
            archive_download_id=archive_download_id,
        )

    def _list_json_files(self, snapshot_date: str) -> list:
        """Return S3ObjectMeta list for all match JSON files in the match_data/json prefix.

        Sidecar files starting with `_` (e.g. `_manifest.json`) are excluded —
        they're internal metadata, not match documents.
        """
        from cip.common.settings import get_settings

        cfg = get_settings().storage
        prefix = f"match_data/json/snapshot_date={snapshot_date}/"
        objs = self._minio.list_objects(
            bucket=cfg.bucket_source_files,
            prefix=prefix,
            suffix_filter=".json",
        )
        return [o for o in objs if not o.file_name.startswith("_")]

    def _read_json_files(self, json_files: list) -> tuple[list[dict], list[str]]:
        """Read and parse JSON files from MinIO with thread pool. Returns (records, failed_keys)."""
        from cip.common.settings import get_settings

        source_files_bucket = get_settings().storage.bucket_source_files
        records: list[dict] = []
        failed: list[str] = []

        def _read_one(obj_meta) -> dict | None:
            try:
                content = self._minio.read_bytes(source_files_bucket, obj_meta.key)
                return _parse_json_file(obj_meta.file_name, content)
            except Exception as exc:
                logger.error(
                    "Failed to read JSON file",
                    extra={"key": obj_meta.key, "error": str(exc)},
                )
                return None

        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
            futures = {executor.submit(_read_one, obj): obj.key for obj in json_files}
            for future in as_completed(futures):
                key = futures[future]
                result = future.result()
                if result is not None:
                    records.append(result)
                else:
                    failed.append(key)

        return records, failed

    def _fetch_existing_revisions(self) -> dict[str, int]:
        """Query Bronze for max revision per match_id across all snapshots."""
        try:
            table = self._writer._catalog.load_table(_TABLE)
            arrow_tbl = table.scan(selected_fields=("match_id", "revision")).to_arrow()
        except Exception:
            return {}

        if len(arrow_tbl) == 0:
            return {}

        df = pl.from_arrow(arrow_tbl)
        return dict(
            df.with_columns(pl.col("revision").cast(pl.Int64))
            .group_by("match_id")
            .agg(pl.col("revision").max().alias("revision"))
            .iter_rows()
        )

    def _attach_revisions(
        self,
        records: list[dict],
        existing: dict[str, int],
    ) -> list[dict]:
        """Mutate each record to add revision = MAX(existing) + 1."""
        for rec in records:
            mid = rec["match_id"]
            rec["revision"] = str(existing.get(mid, 0) + 1)
        return records

    def _delete_partition(self, snapshot_date: str) -> None:
        from pyiceberg.expressions import EqualTo

        try:
            table = self._writer._catalog.load_table(_TABLE)
            table.delete(EqualTo(_PARTITION_COL, snapshot_date))
            logger.info("Deleted partition", extra={"table": _TABLE, "snapshot_date": snapshot_date})
        except Exception as exc:
            logger.warning(
                "Partition delete skipped (table may not exist yet)",
                extra={"table": _TABLE, "error": str(exc)},
            )

    # -------------------------------------------------------------------------
    # Control DB helpers
    # -------------------------------------------------------------------------

    def _check_idempotency(self, snapshot_date: str) -> int | None:
        """Return log row id if already loaded successfully, else None."""
        import psycopg2

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id FROM control.bronze_match_ingestion_log
                    WHERE archive_file = %s AND snapshot_date = %s AND status = 'SUCCESS'
                    ORDER BY id DESC LIMIT 1
                    """,
                    (_ARCHIVE_FILE, snapshot_date),
                )
                row = cur.fetchone()
        return row[0] if row else None

    def _insert_log_row(self, pipeline_run_id: str, snapshot_date: str, archive_download_id: int | None) -> int:
        import psycopg2

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO control.bronze_match_ingestion_log (
                        pipeline_run_id, dag_id, archive_download_id,
                        archive_file, snapshot_date, status
                    ) VALUES (%s, %s, %s, %s, %s, 'RUNNING')
                    RETURNING id
                    """,
                    (
                        pipeline_run_id,
                        _DAG_ID,
                        archive_download_id,
                        _ARCHIVE_FILE,
                        snapshot_date,
                    ),
                )
                log_id = cur.fetchone()[0]
            conn.commit()
        return log_id

    def _update_log_success(self, log_id: int, result: MatchLoadResult) -> None:
        import psycopg2

        duration = result.duration_seconds
        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE control.bronze_match_ingestion_log
                    SET status = 'SUCCESS',
                        completed_at = NOW(),
                        duration_seconds = %s,
                        files_attempted = %s,
                        files_succeeded = %s,
                        files_failed = %s,
                        rows_written = %s
                    WHERE id = %s
                    """,
                    (
                        duration,
                        result.files_attempted,
                        result.files_succeeded,
                        result.files_failed,
                        result.rows_written,
                        log_id,
                    ),
                )
            conn.commit()

    def _update_log_failure(self, log_id: int, error_message: str) -> None:
        import psycopg2

        with psycopg2.connect(self._pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE control.bronze_match_ingestion_log
                    SET status = 'FAILED',
                        completed_at = NOW(),
                        error_message = %s
                    WHERE id = %s
                    """,
                    (error_message[:2000], log_id),
                )
            conn.commit()

        logger.error("Bronze match load failed", extra={"log_id": log_id})


# ===========================================================================
# Helpers
# ===========================================================================


def _bronze_schema() -> dict[str, type]:
    """Polars schema for the Bronze match_documents DataFrame (all strings)."""
    return {
        "match_id": pl.Utf8,
        "revision": pl.Utf8,
        "match_type": pl.Utf8,
        "gender": pl.Utf8,
        "season": pl.Utf8,
        "match_date": pl.Utf8,
        "team_a": pl.Utf8,
        "team_b": pl.Utf8,
        "venue": pl.Utf8,
        "city": pl.Utf8,
        "raw_json": pl.Utf8,
    }


def _parse_json_file(file_name: str, content: bytes) -> dict | None:
    """
    Parse a Cricsheet match JSON into a Bronze row dict.
    Returns None if parsing fails — the file will be counted as failed.
    """
    try:
        data = json.loads(content)
        info = data.get("info", {})
        teams = info.get("teams", [])
        dates = info.get("dates", [])

        return {
            "match_id": file_name.removesuffix(".json"),
            "revision": "1",  # placeholder — overwritten by _attach_revisions()
            "match_type": str(info.get("match_type", "") or ""),
            "gender": str(info.get("gender", "") or ""),
            "season": str(info.get("season", "") or ""),
            "match_date": str(dates[0]) if dates else "",
            "team_a": str(teams[0]) if len(teams) > 0 else "",
            "team_b": str(teams[1]) if len(teams) > 1 else "",
            "venue": str(info.get("venue", "") or ""),
            "city": str(info.get("city", "") or ""),
            "raw_json": content.decode("utf-8", errors="replace"),
        }
    except Exception as exc:
        logger.error(
            "Failed to parse JSON file",
            extra={"file_name": file_name, "error": str(exc)},
        )
        return None
