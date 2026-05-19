#
# DAG: ingest_two_day_match_data_gold
#
# Purpose:
#   Manual incremental Gold DAG. Refreshes DuckDB views from Iceberg,
#   runs dbt (incremental) to update only new/changed match_ids,
#   runs dbt test, then marks pending match_ids as loaded.
#
# Schedule: None — Metabase holds DuckDB read lock; operator triggers manually.
#
# Task graph:
#
#   refresh_duckdb_views
#       └─► dbt_run_incremental
#             └─► dbt_test
#                   └─► mark_gold_loaded_pending
#                         └─► done
#
# Manual trigger:
#   airflow dags trigger ingest_two_day_match_data_gold
#   airflow dags trigger ingest_two_day_match_data_gold \
#     --conf '{"snapshot_date": "2026-05-17"}'

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from cip.ingestion.jobs.run_gold_dbt_models import (
    task_mark_gold_loaded_pending,
    task_refresh_duckdb_views,
    task_run_dbt_incremental,
    task_run_dbt_test,
)

logger = logging.getLogger(__name__)


_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
    "email_on_failure": False,
    "email_on_retry": False,
}

_SNAPSHOT_DATE = "{{ dag_run.conf.get('snapshot_date', macros.datetime.utcnow().strftime('%Y-%m-%d')) }}"
_PIPELINE_RUN_ID = "{{ run_id }}"


with DAG(
    dag_id="ingest_two_day_match_data_gold",
    description=(
        "Manual incremental Gold DAG: refresh DuckDB views → dbt run → dbt test "
        "→ mark_gold_loaded_pending."
    ),
    start_date=datetime(2026, 5, 17),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["gold", "dbt", "incremental", "manual"],
    doc_md="""
## ingest_two_day_match_data_gold

Manual incremental Gold DAG. Normal run after a Silver build. dbt's
`is_incremental()` filter scopes match-grained dim/fact models to
`control.match_file_audit WHERE gold_loaded_at IS NULL`. `mark_gold_loaded_pending`
stamps only those match_ids.

### Manual trigger

```bash
airflow dags trigger ingest_two_day_match_data_gold
airflow dags trigger ingest_two_day_match_data_gold \\
  --conf '{"snapshot_date": "2026-05-17"}'
```
""",
) as dag:
    refresh_duckdb_views = PythonOperator(
        task_id="refresh_duckdb_views",
        python_callable=task_refresh_duckdb_views,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
        },
        execution_timeout=timedelta(minutes=10),
    )

    dbt_run_incremental = PythonOperator(
        task_id="dbt_run_incremental",
        python_callable=task_run_dbt_incremental,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
        },
        execution_timeout=timedelta(hours=1),
    )

    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=task_run_dbt_test,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
        },
        execution_timeout=timedelta(minutes=30),
    )

    mark_gold_loaded_pending = PythonOperator(
        task_id="mark_gold_loaded_pending",
        python_callable=task_mark_gold_loaded_pending,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
        },
        execution_timeout=timedelta(minutes=5),
    )

    done = EmptyOperator(task_id="done", trigger_rule="all_done")

    refresh_duckdb_views >> dbt_run_incremental >> dbt_test >> mark_gold_loaded_pending >> done
