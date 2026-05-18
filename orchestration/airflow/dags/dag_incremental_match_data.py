# orchestration/airflow/dags/dag_incremental_match_data.py
#
# DAG: dag_incremental_match_data
#
# Purpose:
#   Daily incremental Cricsheet pipeline. Downloads recently_added_2_json.zip
#   (~200 KB, ~30 matches added in the last 2 days), extracts JSONs, audit-
#   skips byte-identical re-arrivals before Bronze, runs DQ, and rebuilds
#   incremental Silver for the new match_ids only.
#
# Schedule: Daily at 02:00 UTC.
#
# Task graph: identical to dag_full_load_match_data, different archive.
#
#   check_infra
#       └─► download_archive
#             └─► extract_archive
#                   └─► load_bronze
#                         └─► run_dq
#                               └─► build_silver
#                                     └─► done
#
# Audit-skip is what makes the daily run phantom-revision-free: the
# 2-day overlap in recently_added_2_json.zip means yesterday's files
# arrive again today; their (file_name, content_hash) is already
# bronze_loaded_at IS NOT NULL, so they're dropped before any Bronze
# write happens. Silver picks up only genuinely new or content-changed
# matches.
#
# Gold (dag_full_load_gold / dag_incremental_gold) is NOT auto-triggered —
# Metabase DuckDB lock; run Gold during a maintenance window.

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from cip.ingestion.jobs.incremental_match_data import (
    task_build_silver,
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
    dag_id="dag_incremental_match_data",
    description=(
        "Daily incremental Cricsheet pipeline: recently_added_2_json.zip → "
        "Bronze (audit-skip on 2-day overlap) → DQ → Silver (delete_and_insert)."
    ),
    start_date=datetime(2026, 5, 17),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["ingestion", "incremental", "bronze", "silver", "cricsheet"],
    doc_md="""
## dag_incremental_match_data

Daily incremental pipeline. Downloads
[`recently_added_2_json.zip`](https://cricsheet.org/downloads/recently_added_2_json.zip)
— the last-2-days archive — and writes new matches to `bronze.match_data`,
then runs incremental Silver scoped to just those match_ids.

### Audit-skip in action

The Cricsheet archive carries 2 days of overlap by design. Without audit-skip
the daily DAG would produce phantom revisions in Bronze (~50% of files
re-loaded every day, each as a new revision row). With audit-skip:

1. `extract_archive` uploads all ~30 JSONs and stamps `landing_loaded_at`
   in `control.match_file_audit`.
2. `load_bronze` reads the audit log, drops every file whose `(file_name,
   content_hash)` is already `bronze_loaded_at IS NOT NULL`, then writes
   only the genuinely new or content-changed files.
3. `build_silver` reads `pending_silver_match_ids()` and processes only
   those matches via `delete_and_insert(key_cols=["match_id"])`.

A 30-match incremental on a no-op day costs 0 Bronze rows + 0 Silver rows.

### Manual trigger

```bash
airflow dags trigger dag_incremental_match_data
airflow dags trigger dag_incremental_match_data \\
  --conf '{"snapshot_date": "2026-05-17", "force": true}'
```

### Observability

- Files actually written:
  `SELECT rows_written FROM control.bronze_match_ingestion_log
   WHERE dag_id = 'dag_incremental_match_data' ORDER BY id DESC LIMIT 10;`
- Files audit-skipped:
  `SELECT COUNT(*) FROM control.match_file_audit
   WHERE pipeline_run_id != :run AND bronze_loaded_at IS NOT NULL;`
- DQ results:
  `SELECT * FROM control.dq_results WHERE dag_id = 'dag_incremental_match_data'
   ORDER BY checked_at DESC LIMIT 20;`
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

    done = EmptyOperator(task_id="done", trigger_rule="all_done")

    check_infra >> download_archive >> extract_archive >> load_bronze >> run_dq >> build_silver >> done
