# platform/ingestion/jobs/ingest_cricsheet_register.py
#
# Airflow-callable entry point for the Register ingestion job.
# Each task in the DAG calls one function from this module.
#
# Design: thin wrappers — all logic lives in RegisterDownloader.
# Airflow XCom pushes snapshot_date and pipeline_run_id to downstream tasks.

from __future__ import annotations

import logging

from cip.ingestion.register.download import RegisterDownloader, RegisterDownloadSummary

logger = logging.getLogger(__name__)


def task_download_and_land(
    snapshot_date: str,
    pipeline_run_id: str,
    force: bool = False,
    **context,
) -> dict:
    """
    Airflow PythonOperator callable.
    Downloads people.csv + names.csv, uploads to landing, logs to control DB.

    Returns a dict pushed to XCom for downstream tasks.
    """
    # Coerce Jinja string → bool  ("False" → False,  "True" → True)
    if isinstance(force, str):
        force = force.strip().lower() in ("true", "1", "yes")

    downloader = RegisterDownloader.from_settings()
    summary: RegisterDownloadSummary = downloader.run(
        snapshot_date=snapshot_date,
        pipeline_run_id=pipeline_run_id,
        force=force,
    )

    if not summary.all_succeeded:
        failed = [r.source_file for r in summary.results if r.status == "FAILED"]
        raise RuntimeError(f"Register download failed for files: {failed}")

    xcom_payload = {
        "snapshot_date": snapshot_date,
        "pipeline_run_id": pipeline_run_id,
        "all_succeeded": summary.all_succeeded,
        "any_schema_changed": summary.any_schema_changed,
        "files": [
            {
                "source_file": r.source_file,
                "status": r.status,
                "landing_path": r.landing_path,
                "row_count": r.row_count,
                "checksum_sha256": r.checksum_sha256,
                "is_schema_changed": r.is_schema_changed,
                "new_columns": r.new_columns,
            }
            for r in summary.results
        ],
    }

    if summary.any_schema_changed:
        logger.warning(
            "Schema drift detected in register files — review before Bronze load. "
            "Check control.register_schema_versions for details."
        )

    logger.info("Register landing complete", extra={"xcom": xcom_payload})
    return xcom_payload
