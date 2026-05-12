# src/cip/ingestion/jobs/ingest_cricsheet_register.py
#
# Airflow-callable entry points for the Cricsheet Register ingestion pipeline.
# Each Airflow task calls exactly one function from this module.
#
# Pipeline stages (left → right):
#
#   task_download_and_land
#         │  XCom: snapshot_date, pipeline_run_id
#         ▼
#   task_load_bronze
#         │  XCom: snapshot_date, pipeline_run_id, rows written
#         ▼
#   (future) task_run_dbt_silver
#
# Design principles:
#   - Thin wrappers — all business logic lives in domain classes.
#   - Each task is independently re-runnable (idempotent) via force/overwrite flags.
#   - XCom payloads are plain dicts of primitives (JSON-serialisable).
#   - Jinja string coercion is handled here for bool params (Airflow quirk).
#   - No DataFrames in XCom — staged Parquet in MinIO for large payloads.
#
# Called by:
#   orchestration/airflow/dags/dag_ingest_cricsheet_register.py
#
# Manual invocation (dev):
#   poetry run python -m cip.ingestion.jobs.ingest_cricsheet_register #       --snapshot-date 2026-05-11 --task download
#   poetry run python -m cip.ingestion.jobs.ingest_cricsheet_register #       --snapshot-date 2026-05-11 --task bronze

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


def _strip_s3_prefix(s3_path: str, bucket: str = "cricket-landing") -> str:
    """
    Remove s3://<bucket>/ prefix to obtain the raw MinIO object key.

    Example:
        s3://cricket-landing/register_staging/snapshot_date=2026-05-11/abc123
        → register_staging/snapshot_date=2026-05-11/abc123
    """
    prefix = f"s3://{bucket}/"
    return s3_path.removeprefix(prefix)


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
    from cip.ingestion.register.download import RegisterDownloader, RegisterDownloadSummary

    force = _coerce_bool(force)

    logger.info(
        "task_download_and_land started",
        extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id, "force": force},
    )

    downloader = RegisterDownloader.from_settings()
    summary: RegisterDownloadSummary = downloader.run(
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

    Reads people.csv + names.csv from MinIO landing zone via RegisterNormalizer
    (all-string Polars), parses into three Bronze-shaped frames via RegisterParser,
    and writes to Iceberg Bronze tables via RegisterLoader:
      - cricket.bronze.register_people          (persons frame)
      - cricket.bronze.register_identifiers     (key_* columns unpivoted to long form)
      - cricket.bronze.register_name_variations (names frame)

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
    from cip.ingestion.register.normalize import RegisterNormalizer
    from cip.ingestion.register.parse import RegisterParser
    from cip.transform.polars.bronze.register_loader import LoadResult, RegisterLoader

    force = _coerce_bool(force)

    logger.info(
        "task_load_bronze started",
        extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id, "force": force},
    )

    normalized = RegisterNormalizer.from_settings().run(
        snapshot_date=snapshot_date,
        pipeline_run_id=pipeline_run_id,
    )

    parsed = RegisterParser.parse(normalized)

    loader = RegisterLoader.from_settings()
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
# Task 3 — Load Silver (PySpark Register transform)
# ===========================================================================


def task_load_silver(
    snapshot_date: str,
    pipeline_run_id: str,
    **context,
) -> dict:
    """
    Airflow PythonOperator callable — Stage 3.

    Reads three Bronze Register Iceberg tables for the given snapshot_date
    and writes promoted Silver tables via PySpark:
      - cricket.silver.persons           (from bronze.register_people)
      - cricket.silver.person_identifiers (from bronze.register_identifiers)
      - cricket.silver.name_variations   (from bronze.register_name_variations)

    Args:
        snapshot_date:   ISO date string (YYYY-MM-DD) — must match Bronze partition.
        pipeline_run_id: Airflow run_id passed via op_kwargs.
        **context:       Airflow task context (unused).

    Returns:
        XCom dict with row counts per Silver table.
    """
    from cip.transform.polars.silver.persons import PolarsRegisterSilverTransform

    logger.info(
        "task_load_silver started",
        extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id},
    )

    transform = PolarsRegisterSilverTransform.from_settings()
    result = transform.run_all(snapshot_date=snapshot_date, pipeline_run_id=pipeline_run_id)

    xcom_payload = {
        "snapshot_date": snapshot_date,
        "pipeline_run_id": pipeline_run_id,
        "all_succeeded": True,
        "total_rows_written": result.total_rows,
        "persons_rows": result.persons_rows,
        "person_identifiers_rows": result.person_identifiers_rows,
        "name_variations_rows": result.name_variations_rows,
    }

    logger.info("task_load_silver complete", extra={"xcom": xcom_payload})
    return xcom_payload


# ===========================================================================
# Task 4 — Run DQ checks (Silver layer)
# ===========================================================================


def task_run_dq(
    snapshot_date: str,
    pipeline_run_id: str,
    **context,
) -> dict:
    """
    Airflow PythonOperator callable — Stage 4.

    Runs all seven Register DQ checks against Silver and Bronze Iceberg tables
    for the given snapshot_date and persists results to control.dq_results.

    Checks:
        REG-SLV-001  silver.persons — person_id not null                    BLOCK
        REG-SLV-002  silver.persons — person_id unique                       BLOCK
        REG-SLV-003  silver.person_identifiers — key columns not null        BLOCK
        REG-SLV-004  silver.person_identifiers — unique grain                WARN
        REG-SLV-005  bronze.register_people row count vs people.csv          BLOCK
        REG-SLV-006  bronze.register_name_variations row count vs names.csv  BLOCK
        REG-SLV-007  orphan check — name_variations.identifier in persons    WARN

    Args:
        snapshot_date:   ISO date string (YYYY-MM-DD).
        pipeline_run_id: Airflow run_id passed via op_kwargs.
        **context:       Airflow task context (unused).

    Returns:
        XCom dict with check counts and status.

    Raises:
        DQBlockingFailureError: if any BLOCK-severity check fails.
    """
    from cip.quality.checks.register_dq import RegisterDQChecker

    logger.info(
        "task_run_dq started",
        extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id},
    )

    checker = RegisterDQChecker.from_settings()
    summary = checker.run_all(
        snapshot_date=snapshot_date,
        pipeline_run_id=pipeline_run_id,
    )

    xcom_payload = {
        "snapshot_date": snapshot_date,
        "pipeline_run_id": pipeline_run_id,
        "total_checks": len(summary.checks),
        "passed": summary.passed_count,
        "failed_or_warned": summary.failed_count,
        "blocking_failures": len(summary.blocking_failures),
        "check_ids": [r.check_id for r in summary.checks],
        "statuses": {r.check_id: r.status for r in summary.checks},
    }

    logger.info("task_run_dq complete", extra={"xcom": xcom_payload})
    return xcom_payload


# ===========================================================================
# Task 5 — (Future) Run dbt Silver models
# ===========================================================================


def task_run_dbt_silver(
    snapshot_date: str,
    pipeline_run_id: str,
    select: str = "tag:register",
    **context,
) -> dict:
    """
    Airflow PythonOperator callable — Stage 3 (stub, ready for implementation).

    Triggers dbt Core run for the register Silver models via subprocess.
    Designed to run after task_load_bronze completes.

    Args:
        snapshot_date:   ISO date string passed as dbt var.
        pipeline_run_id: Airflow run_id passed as dbt var.
        select:          dbt --select expression. Default: 'tag:register'.
        **context:       Airflow task context (unused).

    Returns:
        XCom dict with dbt run metadata.

    Implementation note:
        Uncomment and adapt once dbt Silver models for register are authored.
        Uses subprocess to invoke dbt CLI — keeps dbt isolated from Airflow
        Python environment.
    """
    import subprocess

    logger.info(
        "task_run_dbt_silver started",
        extra={
            "snapshot_date": snapshot_date,
            "pipeline_run_id": pipeline_run_id,
            "select": select,
        },
    )

    cmd = [
        "dbt",
        "run",
        "--project-dir",
        "/opt/airflow/dbt/cip",
        "--profiles-dir",
        "/opt/airflow/dbt/cip",
        "--select",
        select,
        "--vars",
        f'{{"snapshot_date": "{snapshot_date}", "pipeline_run_id": "{pipeline_run_id}"}}',
    ]

    logger.info("Running dbt command", extra={"cmd": " ".join(cmd)})

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=600,
    )

    if result.returncode != 0:
        logger.error("dbt run failed", extra={"stderr": result.stderr, "stdout": result.stdout})
        raise RuntimeError(
            f"dbt Silver run failed (exit code {result.returncode}). " f"Stderr: {result.stderr[-2000:]}"
        )

    logger.info("dbt run succeeded", extra={"stdout": result.stdout[-1000:]})

    return {
        "snapshot_date": snapshot_date,
        "pipeline_run_id": pipeline_run_id,
        "select": select,
        "returncode": result.returncode,
    }


# ===========================================================================
# CLI entry point (dev / manual runs)
# ===========================================================================


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Cricket Intelligence Platform — Register ingestion job runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run the full pipeline for today
  python -m cip.ingestion.jobs.ingest_cricsheet_register --task all

  # Run only the download step for a specific date
  python -m cip.ingestion.jobs.ingest_cricsheet_register \
      --snapshot-date 2026-05-11 --task download

  # Force re-run Bronze load for a past snapshot
  python -m cip.ingestion.jobs.ingest_cricsheet_register \
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
        choices=["download", "bronze", "silver", "dq", "dbt", "all"],
        default="all",
        help=(
            "Which task to run: "
            "'download' = task_download_and_land, "
            "'bronze' = task_load_bronze, "
            "'silver' = task_load_silver, "
            "'dq' = task_run_dq (Silver DQ checks), "
            "'dbt' = task_run_dbt_silver, "
            "'all' = all tasks in sequence."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Force re-run even if this snapshot was already processed.",
    )
    parser.add_argument(
        "--dbt-select",
        default="tag:register",
        help="dbt --select expression for task_run_dbt_silver. Default: 'tag:register'.",
    )
    return parser


def main() -> None:
    """
    CLI entry point for manual invocation and local development.

    Usage:
        poetry run python -m cip.ingestion.jobs.ingest_cricsheet_register --help
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
        "CIP Register ingestion job starting",
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

    if args.task in ("silver", "all"):
        result = task_load_silver(
            snapshot_date=snap,
            pipeline_run_id=run_id,
        )
        logger.info("silver result", extra=result)

    if args.task in ("dq", "all"):
        result = task_run_dq(
            snapshot_date=snap,
            pipeline_run_id=run_id,
        )
        logger.info("dq result", extra=result)

    if args.task in ("dbt",):
        result = task_run_dbt_silver(
            snapshot_date=snap,
            pipeline_run_id=run_id,
            select=args.dbt_select,
        )
        logger.info("dbt result", extra=result)

    logger.info("CIP Register ingestion job finished.")
    sys.exit(0)


if __name__ == "__main__":
    main()
