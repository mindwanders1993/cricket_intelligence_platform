# orchestration/airflow/dags/ingest_two_day_match_data_bronze.py
#
# DAG: ingest_two_day_match_data_bronze
#
# Purpose:
#   Daily incremental Cricsheet pipeline: Bronze layer only.
#   Downloads recently_added_2_json.zip (~200 KB, ~30 matches),
#   extracts JSONs, audit-skips byte-identical re-arrivals before
#   Bronze, runs DQ, then auto-triggers ingest_two_day_match_data_silver.
#
# Silver runs in a separate DAG (ingest_two_day_match_data_silver) and
# can also be triggered independently.
#
# Gold (ingest_two_day_match_data_gold) is NOT auto-triggered —
# Metabase DuckDB lock; run Gold during a maintenance window.
#
# Schedule: Daily at 02:00 UTC.
#
# Task graph:
#
#   check_infra
#       └─► download_archive
#             └─► extract_archive
#                   └─► load_bronze
#                         └─► run_dq
#                               └─► trigger_silver
#                                     └─► done
#
# Audit-skip is what makes the daily run phantom-revision-free: the
# 2-day overlap in recently_added_2_json.zip means yesterday's files
# arrive again today; their (file_name, content_hash) is already
# bronze_loaded_at IS NOT NULL, so they're dropped before any Bronze
# write happens.
#
# Manual trigger:
#   airflow dags trigger ingest_two_day_match_data_bronze
#   airflow dags trigger ingest_two_day_match_data_bronze \
#     --conf '{"snapshot_date": "2026-05-17", "force": true}'

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from cip.common.contracts.naming import DagNames
from cip.ingestion.jobs.incremental_match_data import (
    task_download_archive,
    task_extract_archive,
    task_load_bronze,
    task_run_dq,
)

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
            cur.execute("SELECT 1 FROM control.archive_download_log LIMIT 0")
            cur.execute("SELECT 1 FROM control.match_file_audit LIMIT 0")
    finally:
        conn.close()

    logger.info("Infrastructure check passed", extra={"dag_run_id": context.get("run_id")})


with DAG(
    dag_id="ingest_two_day_match_data_bronze",
    description=(
        "Daily incremental Cricsheet pipeline (Bronze): recently_added_2_json.zip → "
        "Bronze (audit-skip on 2-day overlap) → DQ → trigger ingest_two_day_match_data_silver."
    ),
    start_date=datetime(2026, 5, 17),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["bronze", "cricsheet", "ingestion", "incremental"],
    doc_md="""
## ingest_two_day_match_data_bronze

Daily incremental pipeline — Bronze layer. Downloads
[`recently_added_2_json.zip`](https://cricsheet.org/downloads/recently_added_2_json.zip)
and writes new matches to `bronze.match_data` with audit-skip.

Automatically fires `ingest_two_day_match_data_silver` after DQ passes.
Silver can also be run standalone without re-running this DAG.

### Audit-skip in action

The Cricsheet archive carries 2 days of overlap by design. With audit-skip:

1. `extract_archive` uploads all ~30 JSONs and stamps `landing_loaded_at`.
2. `load_bronze` drops files whose `(file_name, content_hash)` is already
   `bronze_loaded_at IS NOT NULL`, writing only genuinely new files.

A 30-match incremental on a no-op day costs 0 Bronze rows.

### Manual trigger

```bash
airflow dags trigger ingest_two_day_match_data_bronze
airflow dags trigger ingest_two_day_match_data_bronze \\
  --conf '{"snapshot_date": "2026-05-17", "force": true}'
```
""",
) as dag:
    check_infra = PythonOperator(
        task_id="check_infra",
        python_callable=_check_infra,
        execution_timeout=timedelta(minutes=2),
    )

    download_archive = PythonOperator(
        task_id="download_archive",
        python_callable=task_download_archive,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
            "force": _FORCE,
        },
        execution_timeout=timedelta(minutes=5),
    )

    extract_archive = PythonOperator(
        task_id="extract_archive",
        python_callable=task_extract_archive,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
            "force": _FORCE,
        },
        execution_timeout=timedelta(minutes=5),
    )

    load_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=task_load_bronze,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
            "force": _FORCE,
        },
        execution_timeout=timedelta(minutes=15),
    )

    run_dq = PythonOperator(
        task_id="run_dq",
        python_callable=task_run_dq,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
        },
        execution_timeout=timedelta(minutes=10),
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id=DagNames.INGEST_TWO_DAY_MATCH_DATA_SILVER,
        wait_for_completion=False,  # bronze completes independently; watch silver in its own run
        reset_dag_run=False,
        conf={"snapshot_date": _SNAPSHOT_DATE, "pipeline_run_id": _PIPELINE_RUN_ID},
        execution_timeout=timedelta(minutes=2),
    )

    done = EmptyOperator(task_id="done", trigger_rule="all_done")

    check_infra >> download_archive >> extract_archive >> load_bronze >> run_dq >> trigger_silver >> done
