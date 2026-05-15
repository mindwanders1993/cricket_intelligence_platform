# src/cip/ingestion/jobs/build_silver_people_and_names.py
#
# Airflow-callable entry points for the People & Names Silver build.
# Each Airflow task calls exactly one function from this module.
#
# Pipeline stages:
#   task_load_silver       — Bronze register_* → Silver persons / person_identifiers / name_variations
#   task_run_dq            — DQ checks on Silver + Bronze counts, persists to control.dq_results
#   task_run_dbt_silver    — (future) dbt Silver models for the register tag
#
# Design principles:
#   - Thin wrappers — all business logic lives in domain classes.
#   - Each task is independently re-runnable (idempotent).
#   - XCom payloads are plain dicts of JSON-serialisable primitives.
#
# Called by:
#   orchestration/airflow/dags/dag_build_silver_people_and_names.py
#
# Manual invocation (dev):
#   poetry run python -m cip.ingestion.jobs.build_silver_people_and_names --task all
#   poetry run python -m cip.ingestion.jobs.build_silver_people_and_names \
#       --snapshot-date 2026-05-11 --task silver

from __future__ import annotations

import argparse
import logging
import uuid
from datetime import date

logger = logging.getLogger(__name__)


# ===========================================================================
# Task 1 — Load Silver (Polars Register transform)
# ===========================================================================


def task_load_silver(
    snapshot_date: str,
    pipeline_run_id: str,
    **context,
) -> dict:
    """
    Read three Bronze Register tables for snapshot_date and write the
    promoted Silver tables via PolarsPeopleAndNamesSilverTransform:
      - silver.persons              (from bronze.people)
      - silver.person_identifiers   (from bronze.people_identifiers)
      - silver.name_variations      (from bronze.name_variations)
    """
    from cip.transform.polars.silver.persons import PolarsPeopleAndNamesSilverTransform

    logger.info(
        "task_load_silver started",
        extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id},
    )

    transform = PolarsPeopleAndNamesSilverTransform.from_settings()
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
# Task 2 — Run DQ checks (Silver + Bronze counts)
# ===========================================================================


def task_run_dq(
    snapshot_date: str,
    pipeline_run_id: str,
    **context,
) -> dict:
    """
    Run all seven Register DQ checks against Silver and Bronze Iceberg tables
    for snapshot_date and persist results to control.dq_results.

    Checks:
        REG-SLV-001  silver.persons — person_id not null                    BLOCK
        REG-SLV-002  silver.persons — person_id unique                       BLOCK
        REG-SLV-003  silver.person_identifiers — key columns not null        BLOCK
        REG-SLV-004  silver.person_identifiers — unique grain                WARN
        REG-SLV-005  bronze.people row count vs people.csv          BLOCK
        REG-SLV-006  bronze.name_variations row count vs names.csv  BLOCK
        REG-SLV-007  orphan check — name_variations.identifier in persons    WARN

    Raises:
        DQBlockingFailureError: if any BLOCK-severity check fails.
    """
    from cip.quality.checks.people_and_names_dq import PeopleAndNamesDQChecker

    logger.info(
        "task_run_dq started",
        extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id},
    )

    checker = PeopleAndNamesDQChecker.from_settings()
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
# Task 3 — (Future) Run dbt Silver models
# ===========================================================================


def task_run_dbt_silver(
    snapshot_date: str,
    pipeline_run_id: str,
    select: str = "tag:register",
    **context,
) -> dict:
    """
    Trigger dbt Core run for the register Silver models via subprocess.
    Designed to run after task_load_silver completes.

    Implementation note:
        Stub today — keeps the wiring in place for when register Silver
        moves from PolarsPeopleAndNamesSilverTransform to dbt models.
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
        raise RuntimeError(f"dbt Silver run failed (exit code {result.returncode}). Stderr: {result.stderr[-2000:]}")

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
        description="Cricket Intelligence Platform — People & Names Silver build runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all Silver+DQ steps for today
  python -m cip.ingestion.jobs.build_silver_people_and_names --task all

  # Only build Silver for a specific snapshot
  python -m cip.ingestion.jobs.build_silver_people_and_names \\
      --snapshot-date 2026-05-11 --task silver

  # Only run DQ checks
  python -m cip.ingestion.jobs.build_silver_people_and_names \\
      --snapshot-date 2026-05-11 --task dq
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
        choices=["silver", "dq", "dbt", "all"],
        default="all",
        help=(
            "Which task to run: "
            "'silver' = task_load_silver, "
            "'dq' = task_run_dq (Silver DQ checks), "
            "'dbt' = task_run_dbt_silver, "
            "'all' = silver → dq."
        ),
    )
    parser.add_argument(
        "--dbt-select",
        default="tag:register",
        help="dbt --select expression for task_run_dbt_silver. Default: 'tag:register'.",
    )
    return parser


def main() -> None:
    """
    CLI entry point.

    Usage:
        poetry run python -m cip.ingestion.jobs.build_silver_people_and_names --help
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
        "CIP People & Names Silver build starting",
        extra={"snapshot_date": snap, "pipeline_run_id": run_id, "task": args.task},
    )

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

    logger.info("CIP People & Names Silver build finished.")
    sys.exit(0)


if __name__ == "__main__":
    main()
