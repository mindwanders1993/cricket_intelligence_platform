# src/cip/ingestion/jobs/ingest_match_data.py
#
# Airflow-callable entry points for the Cricsheet archive ingestion pipeline.
# Each Airflow task calls exactly one function from this module.
#
# Pipeline stages:
#   task_download_archive  → download all_json.zip to cricket-source-files/match_data/zip/
#   task_extract_archive   → extract JSONs from ZIP to match_data/json/ prefix
#   task_load_bronze       → read JSONs, parse, write to bronze.match_data
#   task_run_dq            → run MAT-BRZ DQ checks, persist to control.dq_results
#
# Design:
#   - Thin wrappers — all business logic lives in domain classes.
#   - XCom payloads are plain dicts of JSON-serialisable primitives.
#   - Jinja string coercion handled here for bool params.
#
# Called by:
#   orchestration/airflow/dags/dag_ingest_match_data.py
#
# Manual invocation (dev):
#   poetry run python -m cip.ingestion.jobs.ingest_match_data --task all
#   poetry run python -m cip.ingestion.jobs.ingest_match_data --snapshot-date 2026-05-01 --task download

from __future__ import annotations

import argparse
import logging
import uuid
from datetime import date

logger = logging.getLogger(__name__)


# ===========================================================================
# Helpers
# ===========================================================================


def _coerce_bool(value: bool | str) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1", "yes")


def _today() -> str:
    return date.today().isoformat()


# ===========================================================================
# Task 1 — Download archive
# ===========================================================================


def task_download_archive(
    snapshot_date: str,
    pipeline_run_id: str,
    force: bool = False,
    **context,
) -> dict:
    """
    Airflow PythonOperator callable — Stage 1.

    Downloads all_json.zip from cricsheet.org to MinIO landing zone and
    records the attempt in control.archive_download_log.

    XCom payload:
        snapshot_date:       ISO date of this run
        pipeline_run_id:     Airflow run_id
        archive_download_id: FK into control.archive_download_log
        landing_path:        s3://cricket-source-files/match_data/zip/snapshot_date=.../all_json.zip
        file_size_bytes:     Archive size in bytes
        checksum_sha256:     SHA-256 of the downloaded file
        skipped:             True if idempotency guard fired
    """
    from cip.ingestion.match_data.download import MatchDataDownloader

    force = _coerce_bool(force)

    logger.info(
        "task_download_archive starting",
        extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id, "force": force},
    )

    downloader = MatchDataDownloader.from_settings()
    record = downloader.download(
        snapshot_date=snapshot_date,
        pipeline_run_id=pipeline_run_id,
        force=force,
    )

    payload = {
        "snapshot_date": snapshot_date,
        "pipeline_run_id": pipeline_run_id,
        "archive_download_id": record.id,
        "landing_path": record.landing_path,
        "file_size_bytes": record.file_size_bytes,
        "checksum_sha256": record.checksum_sha256,
        "skipped": record.status == "SUCCESS" and not force,
    }

    logger.info("task_download_archive complete", extra=payload)
    return payload


# ===========================================================================
# Task 2 — Extract archive
# ===========================================================================


def task_extract_archive(
    snapshot_date: str,
    pipeline_run_id: str,
    force: bool = False,
    **context,
) -> dict:
    """
    Airflow PythonOperator callable — Stage 2.

    Extracts all .json files from the source-files ZIP and uploads them to
    the match_data/json prefix. Writes _manifest.json alongside.

    XCom payload:
        snapshot_date:    ISO date
        pipeline_run_id:  Airflow run_id
        file_count:       Number of JSON files extracted
        extracted_prefix: s3:// prefix where JSONs were written
        skipped:          True if idempotency guard fired
    """
    from cip.ingestion.match_data.extract import MatchDataExtractor

    force = _coerce_bool(force)

    logger.info(
        "task_extract_archive starting",
        extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id, "force": force},
    )

    extractor = MatchDataExtractor.from_settings()
    result = extractor.extract(
        snapshot_date=snapshot_date,
        pipeline_run_id=pipeline_run_id,
        force=force,
    )

    payload = {
        "snapshot_date": snapshot_date,
        "pipeline_run_id": pipeline_run_id,
        "file_count": result.file_count,
        "extracted_prefix": result.extracted_prefix,
        "skipped": False,
    }

    logger.info("task_extract_archive complete", extra=payload)
    return payload


# ===========================================================================
# Task 3 — Load Bronze
# ===========================================================================


def task_load_bronze(
    snapshot_date: str,
    pipeline_run_id: str,
    force: bool = False,
    **context,
) -> dict:
    """
    Airflow PythonOperator callable — Stage 3.

    Reads extracted JSON files from MinIO, parses header fields, attaches
    revision numbers, and appends to bronze.match_data.

    XCom payload:
        snapshot_date:    ISO date
        pipeline_run_id:  Airflow run_id
        rows_written:     Total rows written to Iceberg
        files_attempted:  JSON files read
        files_succeeded:  JSON files successfully parsed
        files_failed:     JSON files that raised parse errors
        skipped:          True if idempotency guard fired
    """
    from cip.transform.polars.bronze.match_data import MatchBronzeLoader

    force = _coerce_bool(force)

    # Pull archive_download_id from XCom if available
    archive_download_id: int | None = None
    ti = context.get("ti")
    if ti is not None:
        dl_payload = ti.xcom_pull(task_ids="download_archive") or {}
        archive_download_id = dl_payload.get("archive_download_id")

    logger.info(
        "task_load_bronze starting",
        extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id, "force": force},
    )

    loader = MatchBronzeLoader.from_settings()
    result = loader.load(
        snapshot_date=snapshot_date,
        pipeline_run_id=pipeline_run_id,
        archive_download_id=archive_download_id,
        force=force,
    )

    payload = {
        "snapshot_date": snapshot_date,
        "pipeline_run_id": pipeline_run_id,
        "rows_written": result.rows_written,
        "files_attempted": result.files_attempted,
        "files_succeeded": result.files_succeeded,
        "files_failed": result.files_failed,
        "skipped": result.files_attempted == 0 and not force,
    }

    logger.info("task_load_bronze complete", extra=payload)
    return payload


# ===========================================================================
# Task 4 — Run DQ checks
# ===========================================================================


def task_run_dq(
    snapshot_date: str,
    pipeline_run_id: str,
    **context,
) -> dict:
    """
    Airflow PythonOperator callable — Stage 4.

    Runs MAT-BRZ-001..004 DQ checks against Bronze Iceberg table,
    control.bronze_match_ingestion_log, and the extraction manifest.
    Results are persisted to control.dq_results.

    Raises DQBlockingFailureError if any BLOCK severity check fails.

    XCom payload:
        snapshot_date:     ISO date
        pipeline_run_id:   Airflow run_id
        total_checks:      Number of checks run
        passed:            Number of PASSED checks
        failed:            Number of FAILED + WARNING checks
        blocking_failures: Number of BLOCK severity failures
    """
    from cip.quality.checks.match_bronze_dq import MatchBronzeDQChecker

    logger.info(
        "task_run_dq starting",
        extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id},
    )

    checker = MatchBronzeDQChecker.from_settings()
    summary = checker.run_all(
        snapshot_date=snapshot_date,
        pipeline_run_id=pipeline_run_id,
    )

    payload = {
        "snapshot_date": snapshot_date,
        "pipeline_run_id": pipeline_run_id,
        "total_checks": len(summary.checks),
        "passed": summary.passed_count,
        "failed": summary.failed_count,
        "blocking_failures": len(summary.blocking_failures),
    }

    logger.info("task_run_dq complete", extra=payload)
    return payload


# ===========================================================================
# CLI entrypoint (dev / manual runs, no Airflow needed)
# ===========================================================================


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run Cricsheet archive ingestion pipeline tasks manually.")
    p.add_argument(
        "--snapshot-date",
        default=_today(),
        help="ISO date (YYYY-MM-DD). Defaults to today.",
    )
    p.add_argument(
        "--task",
        choices=["download", "extract", "bronze", "dq", "all"],
        default="all",
        help="Pipeline stage to run.",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Bypass idempotency guard and re-run the task.",
    )
    return p.parse_args()


def main() -> None:
    import sys
    import logging as stdlib_logging

    stdlib_logging.basicConfig(
        level=stdlib_logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )

    args = _parse_args()
    snapshot_date = args.snapshot_date
    run_id = str(uuid.uuid4())

    ctx: dict = {}

    if args.task in ("download", "all"):
        task_download_archive(
            snapshot_date=snapshot_date,
            pipeline_run_id=run_id,
            force=args.force,
            **ctx,
        )

    if args.task in ("extract", "all"):
        task_extract_archive(
            snapshot_date=snapshot_date,
            pipeline_run_id=run_id,
            force=args.force,
            **ctx,
        )

    if args.task in ("bronze", "all"):
        task_load_bronze(
            snapshot_date=snapshot_date,
            pipeline_run_id=run_id,
            force=args.force,
            **ctx,
        )

    if args.task in ("dq", "all"):
        task_run_dq(
            snapshot_date=snapshot_date,
            pipeline_run_id=run_id,
            **ctx,
        )


if __name__ == "__main__":
    main()
