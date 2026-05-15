# src/cip/ingestion/jobs/ingest_people_and_names.py
#
# Airflow-callable entry points for the people.csv + names.csv ingestion
# pipeline (Landing → Bronze).  Each Airflow task calls exactly one function
# from this module.
#
# Pipeline stages (left → right):
#
#   task_download_and_land   (Cricsheet CSVs → MinIO landing)
#         │
#         ▼
#   task_load_bronze         (Landing CSVs → bronze.people / people_identifiers / name_variations)
#
# Silver promotion + DQ live in `build_silver_people_and_names.py` and
# `dag_build_silver_people_and_names.py` — they run as a separate stage
# after this DAG completes.
#
# Design principles:
#   - Thin wrappers — all business logic lives in domain classes.
#   - Each task is independently re-runnable (idempotent) via force/overwrite flags.
#   - XCom payloads are plain dicts of primitives (JSON-serialisable).
#   - Jinja string coercion is handled here for bool params (Airflow quirk).
#
# Called by:
#   orchestration/airflow/dags/dag_ingest_people_and_names.py
#
# Manual invocation (dev):
#   poetry run python -m cip.ingestion.jobs.ingest_people_and_names --task all
#   poetry run python -m cip.ingestion.jobs.ingest_people_and_names \
#       --snapshot-date 2026-05-11 --task download
#   poetry run python -m cip.ingestion.jobs.ingest_people_and_names \
#       --snapshot-date 2026-05-11 --task bronze --force

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
    """Coerce Airflow Jinja-rendered string booleans to Python bool."""
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1", "yes")


# ===========================================================================
# Task 1 — Download + Land
# ===========================================================================


def task_download_and_land(
    snapshot_date: str,
    pipeline_run_id: str,
    force: bool = False,
    **context,
) -> dict:
    """
    Airflow PythonOperator callable — Stage 1.

    Downloads people.csv + names.csv from cricsheet.org, validates checksums
    and row counts, uploads to MinIO landing zone, writes audit rows to
    control.register_ingestion_log and control.register_schema_versions.

    Args:
        snapshot_date:   ISO date string (YYYY-MM-DD).
        pipeline_run_id: Airflow run_id passed via op_kwargs Jinja template.
        force:           Re-download even if this snapshot already exists.
        **context:       Airflow task context (unused; accepted for compatibility).

    Returns:
        XCom dict consumed by task_load_bronze.

    Raises:
        RuntimeError: if any file fails to download or land.
    """
    from cip.ingestion.people_and_names.download import PeopleAndNamesDownloader, PeopleAndNamesDownloadSummary

    force = _coerce_bool(force)

    logger.info(
        "task_download_and_land started",
        extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id, "force": force},
    )

    downloader = PeopleAndNamesDownloader.from_settings()
    summary: PeopleAndNamesDownloadSummary = downloader.run(
        snapshot_date=snapshot_date,
        pipeline_run_id=pipeline_run_id,
        force=force,
    )

    if not summary.all_succeeded:
        failed = [r.source_file for r in summary.results if r.status == "FAILED"]
        raise RuntimeError(
            f"Register download failed for files: {failed}. " f"Check control.register_ingestion_log for details."
        )

    if summary.any_schema_changed:
        changed = [r.source_file for r in summary.results if r.is_schema_changed]
        logger.warning(
            "Schema drift detected in register files — review before Bronze load.",
            extra={
                "changed_files": changed,
                "snapshot_date": snapshot_date,
            },
        )

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
                "removed_columns": r.removed_columns,
            }
            for r in summary.results
        ],
    }

    logger.info("task_download_and_land complete", extra={"xcom": xcom_payload})
    return xcom_payload


# ===========================================================================
# Task 2 — Load Bronze
# ===========================================================================


def task_load_bronze(
    snapshot_date: str,
    pipeline_run_id: str,
    force: bool = False,
    **context,
) -> dict:
    """
    Airflow PythonOperator callable — Stage 2.

    Reads people.csv + names.csv from MinIO landing zone via PeopleAndNamesNormalizer
    (all-string Polars), parses into three Bronze-shaped frames via PeopleAndNamesParser,
    and writes to Iceberg Bronze tables via PeopleAndNamesLoader:
      - bronze.people          (persons frame)
      - bronze.people_identifiers     (key_* columns unpivoted to long form)
      - bronze.name_variations (names frame)

    Args:
        snapshot_date:   ISO date string (YYYY-MM-DD).
        pipeline_run_id: Airflow run_id passed via op_kwargs.
        force:           If True, delete the _snapshot_date partition before writing
                         (overwrite_snapshot). If False, append only (load).
        **context:       Airflow task context (unused).

    Returns:
        XCom dict with row counts per Bronze table.

    Raises:
        FileNotFoundError: if landing CSVs are absent for this snapshot_date.
        IcebergError:      propagated from PolarsIcebergWriter on Iceberg failures.
    """
    import psycopg2

    from cip.common.settings import get_settings
    from cip.ingestion.people_and_names.normalize import PeopleAndNamesNormalizer
    from cip.ingestion.people_and_names.parse import PeopleAndNamesParser
    from cip.transform.polars.bronze.people_and_names_loader import LoadResult, PeopleAndNamesLoader

    force = _coerce_bool(force)

    logger.info(
        "task_load_bronze started",
        extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id, "force": force},
    )

    normalized = PeopleAndNamesNormalizer.from_settings().run(
        snapshot_date=snapshot_date,
        pipeline_run_id=pipeline_run_id,
    )

    parsed = PeopleAndNamesParser.parse(normalized)

    loader = PeopleAndNamesLoader.from_settings()
    result: LoadResult = loader.overwrite_snapshot(parsed)

    # Mark bronze_loaded in the control table so the audit trail is complete.
    cfg = get_settings()
    pg_dsn = cfg.postgres.dsn.replace("postgresql+psycopg2://", "postgresql://")
    with psycopg2.connect(pg_dsn) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                UPDATE control.register_ingestion_log
                   SET bronze_loaded    = TRUE,
                       bronze_loaded_at = NOW(),
                       updated_at       = NOW()
                 WHERE snapshot_date    = %s
                   AND pipeline_run_id  = %s
                   AND status           = 'SUCCESS'
                """,
                (snapshot_date, pipeline_run_id),
            )
        pg_conn.commit()
    logger.info("control.register_ingestion_log bronze_loaded updated", extra={"snapshot_date": snapshot_date})

    xcom_payload = {
        "snapshot_date": snapshot_date,
        "pipeline_run_id": pipeline_run_id,
        "all_succeeded": True,
        "total_rows_written": result.total_rows,
        "persons_rows": result.persons_rows,
        "identifiers_rows": result.identifiers_rows,
        "name_variations_rows": result.name_variations_rows,
        "tables": result.tables,
        "duration_seconds": result.duration_seconds,
    }

    logger.info("task_load_bronze complete", extra={"xcom": xcom_payload})
    return xcom_payload


# ===========================================================================
# CLI entry point (dev / manual runs)
# ===========================================================================


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Cricket Intelligence Platform — People & Names ingestion runner (Landing → Bronze)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run the full pipeline for today
  python -m cip.ingestion.jobs.ingest_people_and_names --task all

  # Run only the download step for a specific date
  python -m cip.ingestion.jobs.ingest_people_and_names \\
      --snapshot-date 2026-05-11 --task download

  # Force re-run Bronze load for a past snapshot
  python -m cip.ingestion.jobs.ingest_people_and_names \\
      --snapshot-date 2026-05-10 --task bronze --force
        """,
    )
    parser.add_argument(
        "--snapshot-date",
        default=date.today().isoformat(),
        help="ISO date (YYYY-MM-DD) for the snapshot. Defaults to today.",
    )
    parser.add_argument(
        "--pipeline-run-id",
        default=None,
        help="Pipeline run ID. Auto-generated if not provided.",
    )
    parser.add_argument(
        "--task",
        choices=["download", "bronze", "all"],
        default="all",
        help=(
            "Which task to run: "
            "'download' = task_download_and_land, "
            "'bronze' = task_load_bronze, "
            "'all' = download → bronze."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Force re-run even if this snapshot was already processed.",
    )
    return parser


def main() -> None:
    """
    CLI entry point for manual invocation and local development.

    Usage:
        poetry run python -m cip.ingestion.jobs.ingest_people_and_names --help
    """
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    parser = _build_arg_parser()
    args = parser.parse_args()

    run_id = args.pipeline_run_id or f"cli-{uuid.uuid4().hex[:8]}"
    snap = args.snapshot_date

    logger.info(
        "CIP People & Names ingestion starting",
        extra={
            "snapshot_date": snap,
            "pipeline_run_id": run_id,
            "task": args.task,
            "force": args.force,
        },
    )

    if args.task in ("download", "all"):
        result = task_download_and_land(
            snapshot_date=snap,
            pipeline_run_id=run_id,
            force=args.force,
        )
        logger.info("download result", extra=result)

    if args.task in ("bronze", "all"):
        result = task_load_bronze(
            snapshot_date=snap,
            pipeline_run_id=run_id,
            force=args.force,
        )
        logger.info("bronze result", extra=result)

    logger.info("CIP People & Names ingestion finished.")
    sys.exit(0)


if __name__ == "__main__":
    main()
