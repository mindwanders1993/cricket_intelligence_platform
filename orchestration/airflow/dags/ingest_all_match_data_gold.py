#
# DAG: ingest_all_match_data_gold
#
# Purpose:
#   Manual full-refresh Gold DAG. Refreshes DuckDB views from Iceberg,
#   runs dbt with --full-refresh to rebuild every Gold model from scratch,
#   runs dbt test, then marks all Silver match_ids as loaded.
#
# Schedule: None — operator triggers manually after stopping Metabase.
#
# Task graph:
#
#   refresh_duckdb_views
#       └─► dbt_run_full_refresh
#             └─► dbt_test
#                   └─► mark_gold_loaded_all_silver
#                         └─► done
#
# Manual trigger:
#   airflow dags trigger ingest_all_match_data_gold
#   airflow dags trigger ingest_all_match_data_gold \
#     --conf '{"snapshot_date": "2026-05-01"}'

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from cip.ingestion.jobs.run_gold_dbt_models import (
    task_mark_gold_loaded_all_silver,
    task_refresh_duckdb_views,
    task_run_dbt_full_refresh,
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
    dag_id="ingest_all_match_data_gold",
    description=(
        "Manual full-refresh Gold DAG: refresh DuckDB views → dbt run --full-refresh "
        "→ dbt test → mark_gold_loaded_all_silver."
    ),
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["gold", "dbt", "full-refresh", "manual"],
    doc_md="""
## ingest_all_match_data_gold

Manual full-refresh Gold DAG. Rebuilds every Gold model from scratch — use after
schema changes or data drift. Metabase DuckDB read lock must be released first
(`docker stop compose-metabase-1` or `make duckdb-stop`).

### Manual trigger

```bash
airflow dags trigger ingest_all_match_data_gold
airflow dags trigger ingest_all_match_data_gold \\
  --conf '{"snapshot_date": "2026-05-01"}'
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

    dbt_run_full_refresh = PythonOperator(
        task_id="dbt_run_full_refresh",
        python_callable=task_run_dbt_full_refresh,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
        },
        execution_timeout=timedelta(hours=2),
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

    mark_gold_loaded_all_silver = PythonOperator(
        task_id="mark_gold_loaded_all_silver",
        python_callable=task_mark_gold_loaded_all_silver,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
        },
        execution_timeout=timedelta(minutes=5),
    )

    done = EmptyOperator(task_id="done", trigger_rule="all_done")

    refresh_duckdb_views >> dbt_run_full_refresh >> dbt_test >> mark_gold_loaded_all_silver >> done
