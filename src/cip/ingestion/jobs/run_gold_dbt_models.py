from __future__ import annotations

from datetime import datetime, timezone

from cip.common.logging import get_logger
from cip.ingestion.audit.match_file_audit import MatchFileAudit
from cip.serving.duckdb.refresh import DuckDBRefresh

logger = get_logger(__name__)


def task_refresh_duckdb_views(**context) -> dict:
    """Airflow-callable: bootstrap DuckDB and create Silver Iceberg views."""
    pipeline_run_id: str = context.get("run_id", "manual")
    logger.info("task_refresh_duckdb_views started", extra={"pipeline_run_id": pipeline_run_id})

    refresh = DuckDBRefresh.from_settings()
    refresh.bootstrap()
    refresh.create_bronze_views()
    refresh.create_silver_views()
    refresh.create_control_views()

    logger.info("task_refresh_duckdb_views complete")
    return {"status": "ok", "pipeline_run_id": pipeline_run_id}


def task_run_dbt_incremental(snapshot_date: str, pipeline_run_id: str, **context) -> dict:
    logger.info("task_run_dbt_incremental started", extra={"pipeline_run_id": pipeline_run_id})
    refresh = DuckDBRefresh.from_settings()
    refresh.run_dbt(command="seed")
    refresh.run_dbt(command="run", full_refresh=False)
    logger.info("task_run_dbt_incremental complete")
    return {"status": "ok", "pipeline_run_id": pipeline_run_id}


def task_run_dbt_full_refresh(snapshot_date: str, pipeline_run_id: str, **context) -> dict:
    logger.info("task_run_dbt_full_refresh started", extra={"pipeline_run_id": pipeline_run_id})
    refresh = DuckDBRefresh.from_settings()
    refresh.run_dbt(command="seed")
    refresh.run_dbt(command="run", full_refresh=True)
    logger.info("task_run_dbt_full_refresh complete")
    return {"status": "ok", "pipeline_run_id": pipeline_run_id}


def task_run_dbt_test(snapshot_date: str, pipeline_run_id: str, **context) -> dict:
    logger.info("task_run_dbt_test started", extra={"pipeline_run_id": pipeline_run_id})
    refresh = DuckDBRefresh.from_settings()
    refresh.run_dbt(command="test")
    logger.info("task_run_dbt_test complete")
    return {"status": "ok", "pipeline_run_id": pipeline_run_id}


def task_mark_gold_loaded_pending(snapshot_date: str, pipeline_run_id: str, **context) -> dict:
    audit = MatchFileAudit.from_settings()
    match_ids = audit.pending_gold_match_ids()
    if match_ids:
        audit.mark_gold_loaded_pending(match_ids, ts=datetime.now(timezone.utc))
    return {"stamped": len(match_ids)}


def task_mark_gold_loaded_all_silver(snapshot_date: str, pipeline_run_id: str, **context) -> dict:
    audit = MatchFileAudit.from_settings()
    stamped = audit.mark_gold_loaded_all_silver(ts=datetime.now(timezone.utc))
    return {"stamped": stamped}


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--task", required=True)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    args = parser.parse_args()

    task_map = {
        "refresh_duckdb_views": task_refresh_duckdb_views,
        "run_dbt_incremental": task_run_dbt_incremental,
        "run_dbt_full_refresh": task_run_dbt_full_refresh,
        "mark_gold_loaded_pending": task_mark_gold_loaded_pending,
        "mark_gold_loaded_all_silver": task_mark_gold_loaded_all_silver,
    }

    task_fn = task_map.get(args.task)
    if not task_fn:
        raise ValueError(f"Unknown task: {args.task}")

    result = task_fn(
        snapshot_date=args.snapshot_date,
        pipeline_run_id=args.pipeline_run_id,
    )
    print(result)


if __name__ == "__main__":
    main()
