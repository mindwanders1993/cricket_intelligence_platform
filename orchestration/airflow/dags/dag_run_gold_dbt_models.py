# orchestration/airflow/dags/dag_run_gold_dbt_models.py
#
# DAG: dag_run_gold_dbt_models
#
# Purpose:
#   Phase 4 — Gold layer.  Refreshes DuckDB Silver views from Iceberg,
#   then runs dbt to materialise all Gold dims, facts, and marts.
#
# Task graph:
#
#   refresh_duckdb_views
#       └─► run_dbt
#             └─► done
#
# Idempotency:
#   DuckDB views are CREATE OR REPLACE — safe to re-run.
#   dbt table models are fully replaced on each run.
#
# Manual trigger:
#   airflow dags trigger dag_run_gold_dbt_models
#   airflow dags trigger dag_run_gold_dbt_models \
#     --conf '{"dbt_select": "marts.facts.fact_delivery"}'

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from cip.ingestion.jobs.run_gold_dbt_models import task_refresh_duckdb_views, task_run_dbt

logger = logging.getLogger(__name__)

_DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_run_gold_dbt_models",
    description="Gold layer — refresh DuckDB Silver views and run dbt dims/facts/marts",
    start_date=datetime(2026, 5, 1),
    schedule="0 4 3 * *",  # 3rd of each month at 04:00 UTC — after silver builds
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["gold", "dbt", "duckdb"],
    max_active_runs=1,
) as dag:

    refresh_views = PythonOperator(
        task_id="refresh_duckdb_views",
        python_callable=task_refresh_duckdb_views,
    )

    run_dbt = PythonOperator(
        task_id="run_dbt",
        python_callable=task_run_dbt,
        # Airflow Jinja renders Python None as the literal string "None", which
        # would be passed to dbt as `--select None`. Use empty string instead —
        # task_run_dbt normalises empty → None before calling dbt.
        op_kwargs={
            "dbt_select": "{{ dag_run.conf.get('dbt_select', '') }}",
            "dbt_test": True,
        },
    )

    done = EmptyOperator(task_id="done")

    refresh_views >> run_dbt >> done
