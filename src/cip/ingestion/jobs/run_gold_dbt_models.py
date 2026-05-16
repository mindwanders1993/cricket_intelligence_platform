from __future__ import annotations

from cip.common.logging import get_logger
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

    logger.info("task_refresh_duckdb_views complete")
    return {"status": "ok", "pipeline_run_id": pipeline_run_id}


def task_run_dbt(dbt_select: str | None = None, dbt_test: bool = True, **context) -> dict:
    """Airflow-callable: run dbt models and optional tests against DuckDB."""
    pipeline_run_id: str = context.get("run_id", "manual")

    # Airflow Jinja renders Python None as the literal string "None", and
    # empty conf renders as "". Normalise both to a real None.
    if not dbt_select or dbt_select.strip() in ("", "None"):
        dbt_select = None

    logger.info(
        "task_run_dbt started",
        extra={"pipeline_run_id": pipeline_run_id, "select": dbt_select or "all"},
    )
    refresh = DuckDBRefresh.from_settings()
    refresh.run_dbt("run", select=dbt_select)
    if dbt_test:
        refresh.run_dbt("test", select=dbt_select)

    logger.info("task_run_dbt complete")
    return {"status": "ok", "pipeline_run_id": pipeline_run_id}
