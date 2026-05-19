# orchestration/airflow/dags/ingest_two_day_match_data_silver.py
#
# DAG: ingest_two_day_match_data_silver
#
# Purpose:
#   Incremental Silver build for daily match data. Reads Bronze match_data
#   Iceberg table, runs incremental delete_and_insert for pending match_ids,
#   and writes to Silver Iceberg tables.
#
#   Triggered automatically by ingest_two_day_match_data_bronze, but can also
#   be run standalone to re-process Silver without re-downloading Bronze.
#
# Schedule: None — triggered by ingest_two_day_match_data_bronze or manually.
#
# Task graph:
#
#   check_infra
#       └─► build_silver
#             └─► done
#
# Manual trigger:
#   airflow dags trigger ingest_two_day_match_data_silver
#   airflow dags trigger ingest_two_day_match_data_silver \
#     --conf '{"snapshot_date": "2026-05-17", "force": true}'

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from cip.common.contracts.naming import DagNames
from cip.ingestion.jobs.incremental_match_data import task_build_silver

logger = logging.getLogger(__name__)


_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
    "email_on_retry": False,
}

_SNAPSHOT_DATE = "{{ dag_run.conf.get('snapshot_date', macros.datetime.utcnow().strftime('%Y-%m-%d')) }}"
_PIPELINE_RUN_ID = "{{ run_id }}"
_FORCE = "{{ dag_run.conf.get('force', False) }}"


def _check_infra(**context) -> None:
    """Verify MinIO and PostgreSQL control schema are reachable."""
    import psycopg2

    from cip.common.settings import get_settings
    from cip.ingestion.io.minio import MinIOClient

    MinIOClient.from_settings().health_check()

    cfg = get_settings().postgres
    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password.get_secret_value(),
        dbname=cfg.db,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM control.match_file_audit LIMIT 0")
    finally:
        conn.close()

    logger.info("Infrastructure check passed", extra={"dag_run_id": context.get("run_id")})


with DAG(
    dag_id="ingest_two_day_match_data_silver",
    description=(
        "Incremental Silver build: Bronze match_data → Silver Iceberg tables "
        "(delete_and_insert for pending match_ids)."
    ),
    start_date=datetime(2026, 5, 17),
    schedule=None,  # triggered by ingest_two_day_match_data_bronze or manual
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["silver", "cricsheet", "incremental"],
    doc_md="""
## ingest_two_day_match_data_silver

Incremental Silver build for daily match data. Reads `bronze.match_data`,
runs `delete_and_insert` for pending match_ids only, and writes typed
Silver Iceberg tables.

Triggered automatically by `ingest_two_day_match_data_bronze` after DQ passes.
Can also be run standalone to re-process Silver without re-downloading Bronze.

### Task graph

```
check_infra
└─► build_silver  (incremental delete_and_insert by match_id)
      └─► done
```

### Observability

- Files actually written:
  `SELECT rows_written FROM control.bronze_match_ingestion_log
   WHERE dag_id = 'ingest_two_day_match_data_silver' ORDER BY id DESC LIMIT 10;`
- Silver pending match_ids:
  `SELECT COUNT(*) FROM control.match_file_audit WHERE silver_loaded_at IS NULL;`

### Manual trigger

```bash
airflow dags trigger ingest_two_day_match_data_silver
airflow dags trigger ingest_two_day_match_data_silver \\
  --conf '{"snapshot_date": "2026-05-17", "force": true}'
```
""",
) as dag:
    check_infra = PythonOperator(
        task_id="check_infra",
        python_callable=_check_infra,
        execution_timeout=timedelta(minutes=2),
    )

    build_silver = PythonOperator(
        task_id="build_silver",
        python_callable=task_build_silver,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
            "force": _FORCE,
        },
        execution_timeout=timedelta(minutes=30),
    )

    # NOTE: Gold writes to DuckDB — ensure Metabase is stopped before triggering silver
    # so that this auto-trigger doesn't hit the DuckDB read lock.
    # Run: docker stop compose-metabase-1  before triggering this DAG.
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold",
        trigger_dag_id=DagNames.INGEST_TWO_DAY_MATCH_DATA_GOLD,
        wait_for_completion=False,
        reset_dag_run=False,
        conf={"snapshot_date": _SNAPSHOT_DATE, "pipeline_run_id": _PIPELINE_RUN_ID},
        execution_timeout=timedelta(minutes=2),
    )

    done = EmptyOperator(task_id="done", trigger_rule="all_done")

    check_infra >> build_silver >> trigger_gold >> done
