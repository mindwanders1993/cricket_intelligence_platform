# src/cip/ingestion/jobs/build_silver_match_data.py
#
# Airflow-callable entry points for Big Task 5 — Match Silver build.
# Each Airflow task calls exactly one function from this module.
#
# Pipeline stages:
#   task_check_bronze_ready  → confirm bronze.match_data has data up to snapshot_date
#   task_build_silver        → run MatchSilverPipeline.run_all()
#   task_run_dq              → run MatchDataSilverDQChecker against the just-written tables
#
# XCom payloads are plain dicts of JSON-serialisable primitives.
#
# Manual invocation (dev):
#   poetry run python -m cip.ingestion.jobs.build_silver_match_data --task all
#   poetry run python -m cip.ingestion.jobs.build_silver_match_data \
#       --snapshot-date 2026-05-01 --task silver
#   poetry run python -m cip.ingestion.jobs.build_silver_match_data \
#       --snapshot-date 2026-05-01 --task dq

from __future__ import annotations

import argparse
import logging
import uuid
from datetime import date

from cip.common.contracts.naming import TableName

logger = logging.getLogger(__name__)

_BRONZE_FQN = TableName.bronze("match_data")


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
# Task 1 — Bronze readiness check
# ===========================================================================


def task_check_bronze_ready(
    snapshot_date: str,
    pipeline_run_id: str,
    **context,
) -> dict:
    """
    Pre-flight check: bronze.match_data must have at least one row with
    `_snapshot_date <= snapshot_date`.  Fails fast otherwise.
    """
    from cip.transform.spark.session import get_or_create_spark

    logger.info(
        "task_check_bronze_ready starting",
        extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id},
    )

    spark = get_or_create_spark(app_name_suffix="silver-match-check")
    try:
        from pyspark.sql import functions as F

        try:
            bronze = spark.read.format("iceberg").load(_BRONZE_FQN)
        except Exception as exc:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(exc) or "table or view" in str(exc).lower():
                raise RuntimeError(
                    f"{_BRONZE_FQN} does not exist — run dag_ingest_match_data first "
                    "to populate Bronze before building Silver."
                ) from None
            raise

        ready_count = bronze.filter(F.col("_snapshot_date") <= F.lit(snapshot_date)).limit(1).count()
    finally:
        spark.stop()

    if ready_count == 0:
        raise RuntimeError(
            f"bronze.match_data has no rows with _snapshot_date <= {snapshot_date} — "
            "run dag_ingest_match_data first."
        )

    return {
        "snapshot_date": snapshot_date,
        "pipeline_run_id": pipeline_run_id,
        "bronze_ready": True,
    }


# ===========================================================================
# Task 2 — Build Silver
# ===========================================================================


def task_build_silver(
    snapshot_date: str,
    pipeline_run_id: str,
    force: bool = False,
    **context,
) -> dict:
    """
    Run MatchSilverPipeline.run_all() for the given snapshot.

    `force` is reserved for parity with the rest of the platform — Silver
    writes use dynamic_overwrite, which is already idempotent per partition.
    """
    from cip.transform.spark.session import get_or_create_spark
    from cip.transform.spark.silver.pipeline import MatchSilverPipeline

    force = _coerce_bool(force)

    logger.info(
        "task_build_silver starting",
        extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id, "force": force},
    )

    spark = get_or_create_spark(app_name_suffix="silver-match-build")
    try:
        pipeline = MatchSilverPipeline.from_spark(spark)
        result = pipeline.run_all(snapshot_date=snapshot_date, pipeline_run_id=pipeline_run_id)
    finally:
        spark.stop()

    return {
        "snapshot_date": snapshot_date,
        "pipeline_run_id": pipeline_run_id,
        "tables_run": result.tables_run,
        "row_counts": {
            "matches": result.matches_rows,
            "innings": result.innings_rows,
            "deliveries": result.deliveries_rows,
            "wickets": result.wickets_rows,
            "teams": result.teams_rows,
            "venues": result.venues_rows,
            "competitions": result.competitions_rows,
            "match_players": result.match_players_rows,
            "match_officials": result.match_officials_rows,
            "match_powerplays": result.match_powerplays_rows,
            "match_registry": result.match_registry_rows,
            "unmatched_persons_audit": result.unmatched_persons_audit_rows,
        },
        "total_rows": result.total_rows,
    }


# ===========================================================================
# Task 3 — Run Silver DQ checks
# ===========================================================================


def task_run_dq(
    snapshot_date: str,
    pipeline_run_id: str,
    **context,
) -> dict:
    """
    Airflow PythonOperator callable — Stage 3.

    Runs MAT-SLV-001..012 DQ checks against the Silver match tables for
    this snapshot. Persists results to control.dq_results. Raises
    DQBlockingFailureError if any BLOCK severity check fails.
    """
    from cip.quality.checks.match_silver_dq import MatchDataSilverDQChecker

    logger.info(
        "task_run_dq starting",
        extra={"snapshot_date": snapshot_date, "pipeline_run_id": pipeline_run_id},
    )

    checker = MatchDataSilverDQChecker.from_settings()
    summary = checker.run_all(
        snapshot_date=snapshot_date,
        pipeline_run_id=pipeline_run_id,
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
# CLI entry point
# ===========================================================================


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run Match Silver build pipeline tasks manually.")
    p.add_argument(
        "--snapshot-date",
        default=_today(),
        help="ISO date for the Silver write partition (default: today).",
    )
    p.add_argument(
        "--pipeline-run-id",
        default=None,
        help="Override the auto-generated pipeline_run_id (UUID).",
    )
    p.add_argument(
        "--task",
        choices=("check", "silver", "dq", "all"),
        default="all",
        help="Which task to run.",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Force re-run of Silver build (no-op — dynamic overwrite is always idempotent).",
    )
    return p.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    args = _parse_args()

    run_id = args.pipeline_run_id or str(uuid.uuid4())

    if args.task in ("check", "all"):
        task_check_bronze_ready(snapshot_date=args.snapshot_date, pipeline_run_id=run_id)

    if args.task in ("silver", "all"):
        payload = task_build_silver(
            snapshot_date=args.snapshot_date,
            pipeline_run_id=run_id,
            force=args.force,
        )
        logger.info("Silver build complete", extra={"payload": payload})

    if args.task in ("dq", "all"):
        dq_payload = task_run_dq(snapshot_date=args.snapshot_date, pipeline_run_id=run_id)
        logger.info("Silver DQ complete", extra={"payload": dq_payload})


if __name__ == "__main__":
    main()
