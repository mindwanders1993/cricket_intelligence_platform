# orchestration/airflow/dags/dag_build_silver_match_data.py
#
# DAG: dag_build_silver_match_data
#
# Purpose:
#   Big Task 5 — Match Silver build.  Promotes bronze.match_data
#   to the nine Silver match entity tables via PySpark + Iceberg.
#
# Schedule: Monthly on the 2nd at 02:30 UTC — runs the day after
#           dag_ingest_match_data finishes landing Bronze.
#
# Task graph:
#
#   check_bronze_ready
#       └─► build_silver
#             └─► run_dq
#                   └─► done
#
# Idempotency:
#   Silver writes use SparkIcebergWriter.dynamic_overwrite — re-running
#   the same snapshot_date replaces only that partition.
#
# Manual trigger examples:
#   airflow dags trigger dag_build_silver_match_data \
#     --conf '{"snapshot_date": "2026-05-01"}'
#
#   airflow dags trigger dag_build_silver_match_data \
#     --conf '{"snapshot_date": "2026-05-01", "force": true}'

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from cip.ingestion.jobs.build_silver_match_data import (
    task_build_silver,
    task_check_bronze_ready,
    task_run_dq,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DAG-level defaults
# ---------------------------------------------------------------------------

_DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Jinja-templated kwargs passed to every PythonOperator.
_OP_KWARGS = {
    "snapshot_date": "{{ dag_run.conf.get('snapshot_date', ds) }}",
    "pipeline_run_id": "{{ run_id }}",
    "force": "{{ dag_run.conf.get('force', false) }}",
}


with DAG(
    dag_id="dag_build_silver_match_data",
    description="Match Silver build — explode Bronze match documents into typed Silver entities",
    start_date=datetime(2026, 5, 1),
    schedule="30 2 2 * *",  # 2nd of each month at 02:30 UTC
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["silver", "match", "spark"],
    max_active_runs=1,
) as dag:

    check_bronze_ready = PythonOperator(
        task_id="check_bronze_ready",
        python_callable=task_check_bronze_ready,
        op_kwargs={k: v for k, v in _OP_KWARGS.items() if k != "force"},
    )

    build_silver = PythonOperator(
        task_id="build_silver",
        python_callable=task_build_silver,
        op_kwargs=_OP_KWARGS,
    )

    run_dq = PythonOperator(
        task_id="run_dq",
        python_callable=task_run_dq,
        op_kwargs={k: v for k, v in _OP_KWARGS.items() if k != "force"},
    )

    done = EmptyOperator(task_id="done")

    check_bronze_ready >> build_silver >> run_dq >> done
