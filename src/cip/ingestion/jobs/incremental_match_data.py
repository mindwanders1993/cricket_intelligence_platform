# src/cip/ingestion/jobs/incremental_match_data.py
#
# Airflow-callable entry points for the DAILY INCREMENTAL match-data pipeline.
#
# Source archive: https://cricsheet.org/downloads/recently_added_2_json.zip
#   — ~200 KB, ~30 matches added in the last 2 days. Daily schedule
#     at 02:00 UTC.
#
# Pipeline stages (same shape as the full-load pipeline; identical underlying
# helpers; only the archive constants differ):
#   task_download_archive  → recently_added_2_json.zip → MinIO landing
#   task_extract_archive   → JSONs → match_data/json/snapshot_date=…/archive=recently_added_2_json/
#   task_load_bronze       → bronze.match_data; audit-skip drops the 2-day overlap
#   task_run_dq            → MAT-BRZ-001..004 (per-run audit coherence)
#   task_build_silver      → MatchSilverPipeline.run_all(match_ids = audit pending)
#
# Audit-skip is what makes daily runs phantom-revision-free: every file in
# this archive that already lives in Bronze (via prior full-load or prior
# incremental) is dropped before Bronze write.

from __future__ import annotations

import argparse
import logging
import uuid
from datetime import date

logger = logging.getLogger(__name__)


# ===========================================================================
# Pipeline identity — daily incremental
# ===========================================================================

ARCHIVE_URL = "https://cricsheet.org/downloads/recently_added_2_json.zip"
ARCHIVE_FILE = "recently_added_2_json.zip"
DAG_ID = "ingest_two_day_match_data_bronze"
LOADED_BY_PIPELINE = "incremental"

# Real archive is ~200 KB; treat anything under 50 KB as truncated/empty.
MIN_EXPECTED_BYTES = 50 * 1024


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
    from cip.ingestion.match_data.download import MatchDataDownloader

    force = _coerce_bool(force)
    logger.info(
        "task_download_archive (incremental) starting",
        extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id, "force": force},
    )

    downloader = MatchDataDownloader.from_settings(
        archive_file=ARCHIVE_FILE,
        archive_url=ARCHIVE_URL,
        min_expected_bytes=MIN_EXPECTED_BYTES,
        dag_id=DAG_ID,
    )
    record = downloader.download(
        snapshot_date=snapshot_date,
        pipeline_run_id=pipeline_run_id,
        force=force,
    )

    return {
        "snapshot_date": snapshot_date,
        "pipeline_run_id": pipeline_run_id,
        "archive_download_id": record.id,
        "landing_path": record.landing_path,
        "file_size_bytes": record.file_size_bytes,
        "checksum_sha256": record.checksum_sha256,
        "skipped": record.status == "SUCCESS" and not force,
    }


# ===========================================================================
# Task 2 — Extract archive
# ===========================================================================


def task_extract_archive(
    snapshot_date: str,
    pipeline_run_id: str,
    force: bool = False,
    **context,
) -> dict:
    from cip.ingestion.match_data.extract import MatchDataExtractor

    force = _coerce_bool(force)
    logger.info(
        "task_extract_archive (incremental) starting",
        extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id, "force": force},
    )

    extractor = MatchDataExtractor.from_settings(
        archive_file=ARCHIVE_FILE,
        loaded_by_pipeline=LOADED_BY_PIPELINE,
    )
    result = extractor.extract(
        snapshot_date=snapshot_date,
        pipeline_run_id=pipeline_run_id,
        force=force,
    )

    return {
        "snapshot_date": snapshot_date,
        "pipeline_run_id": pipeline_run_id,
        "file_count": result.file_count,
        "extracted_prefix": result.extracted_prefix,
        "skipped": False,
    }


# ===========================================================================
# Task 3 — Load Bronze
# ===========================================================================


def task_load_bronze(
    snapshot_date: str,
    pipeline_run_id: str,
    force: bool = False,
    **context,
) -> dict:
    from cip.transform.polars.bronze.match_data import MatchBronzeLoader

    force = _coerce_bool(force)

    archive_download_id: int | None = None
    ti = context.get("ti")
    if ti is not None:
        dl_payload = ti.xcom_pull(task_ids="download_archive") or {}
        archive_download_id = dl_payload.get("archive_download_id")

    logger.info(
        "task_load_bronze (incremental) starting",
        extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id, "force": force},
    )

    loader = MatchBronzeLoader.from_settings(
        archive_file=ARCHIVE_FILE,
        archive_url=ARCHIVE_URL,
        dag_id=DAG_ID,
    )
    result = loader.load(
        snapshot_date=snapshot_date,
        pipeline_run_id=pipeline_run_id,
        archive_download_id=archive_download_id,
        force=force,
    )

    return {
        "snapshot_date": snapshot_date,
        "pipeline_run_id": pipeline_run_id,
        "rows_written": result.rows_written,
        "files_attempted": result.files_attempted,
        "files_succeeded": result.files_succeeded,
        "files_failed": result.files_failed,
        "files_skipped_by_audit": result.files_skipped_by_audit,
        "skipped": result.files_attempted == 0 and not force,
    }


# ===========================================================================
# Task 4 — Bronze DQ
# ===========================================================================


def task_run_dq(
    snapshot_date: str,
    pipeline_run_id: str,
    **context,
) -> dict:
    from cip.quality.checks.match_bronze_dq import MatchBronzeDQChecker

    logger.info(
        "task_run_dq (incremental) starting",
        extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id},
    )

    checker = MatchBronzeDQChecker.from_settings(archive_file=ARCHIVE_FILE)
    summary = checker.run_all(
        snapshot_date=snapshot_date,
        pipeline_run_id=pipeline_run_id,
        dag_id=DAG_ID,
    )

    return {
        "snapshot_date": snapshot_date,
        "pipeline_run_id": pipeline_run_id,
        "total_checks": len(summary.checks),
        "passed": summary.passed_count,
        "failed": summary.failed_count,
        "blocking_failures": len(summary.blocking_failures),
    }


# ===========================================================================
# Task 5 — Build Silver (incremental, audit-driven scope)
# ===========================================================================


def task_build_silver(
    snapshot_date: str,
    pipeline_run_id: str,
    force: bool = False,
    **context,
) -> dict:
    from cip.ingestion.jobs.build_silver_match_data import task_build_silver as _build

    return _build(
        snapshot_date=snapshot_date,
        pipeline_run_id=pipeline_run_id,
        force=force,
        **context,
    )


# ===========================================================================
# CLI entrypoint (dev / manual runs, no Airflow needed)
# ===========================================================================


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run the daily incremental Cricsheet pipeline tasks manually.")
    p.add_argument("--snapshot-date", default=_today())
    p.add_argument(
        "--task",
        choices=["download", "extract", "bronze", "dq", "silver", "all"],
        default="all",
    )
    p.add_argument("--force", action="store_true")
    return p.parse_args()


def main() -> None:
    import logging as stdlib_logging
    import sys

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
        task_download_archive(snapshot_date=snapshot_date, pipeline_run_id=run_id, force=args.force, **ctx)
    if args.task in ("extract", "all"):
        task_extract_archive(snapshot_date=snapshot_date, pipeline_run_id=run_id, force=args.force, **ctx)
    if args.task in ("bronze", "all"):
        task_load_bronze(snapshot_date=snapshot_date, pipeline_run_id=run_id, force=args.force, **ctx)
    if args.task in ("dq", "all"):
        task_run_dq(snapshot_date=snapshot_date, pipeline_run_id=run_id, **ctx)
    if args.task in ("silver", "all"):
        task_build_silver(snapshot_date=snapshot_date, pipeline_run_id=run_id, force=args.force, **ctx)


if __name__ == "__main__":
    main()
