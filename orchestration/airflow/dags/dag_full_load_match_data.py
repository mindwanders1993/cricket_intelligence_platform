# orchestration/airflow/dags/dag_full_load_match_data.py
#
# DAG: dag_full_load_match_data
#
# Purpose:
#   Manual full-load Cricsheet pipeline. Downloads all_json.zip (~1 GB,
#   ~21k matches), extracts JSONs, loads to Bronze, runs DQ, and rebuilds
#   the incremental Silver layer for every match_id whose Bronze content
#   has new revisions (or all of them, on a fresh deploy).
#
# Schedule: None — operator triggers manually via Airflow UI / CLI.
#
# Task graph:
#
#   check_infra
#       └─► download_archive
#             └─► extract_archive
#                   └─► load_bronze
#                         └─► run_dq
#                               └─► build_silver
#                                     └─► done
#
# Gold (dag_full_load_gold / dag_incremental_gold) is NOT auto-triggered —
# Metabase holds a DuckDB read lock. Run Gold manually after stopping the
# Metabase container.
#
# Manual trigger:
#   airflow dags trigger dag_full_load_match_data
#   airflow dags trigger dag_full_load_match_data \
#     --conf '{"snapshot_date": "2026-05-01", "force": true}'

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from cip.ingestion.jobs.full_load_match_data import (
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
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
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
    dag_id="dag_full_load_match_data",
    description=(
        "Manual full-load Cricsheet pipeline: download all_json.zip → "
        "Bronze (audit-skip) → DQ → Silver (incremental delete_and_insert)."
    ),
    start_date=datetime(2026, 5, 1),
    schedule=None,  # manual trigger only
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["ingestion", "full-load", "bronze", "silver", "cricsheet", "manual"],
    doc_md="""
## dag_full_load_match_data

Manual full-load Cricsheet pipeline. Operator-triggered only.

### Task graph

```
check_infra
└─► download_archive
      └─► extract_archive
            └─► load_bronze        (audit-skip on (filename, content_hash))
                  └─► run_dq       (MAT-BRZ-001..004 incl. per-run audit coherence)
                        └─► build_silver  (incremental delete_and_insert by match_id)
                              └─► done
```

### Audit-log lifecycle

- `extract_archive` stamps `landing_loaded_at` on every JSON it uploads.
- `load_bronze` reads the audit log, drops files whose `(file_name, content_hash)`
  is already `bronze_loaded_at IS NOT NULL`, then writes the surviving rows
  and stamps `bronze_loaded_at + revision`. Each successful file is also
  copied to `match_data/archive/processed_date={today}/` and `archive_path +
  archived_at` are stamped.
- `build_silver` queries `pending_silver_match_ids()` and scopes the Silver
  pipeline to just those matches. Match-grained Silver tables use
  `delete_and_insert(key_cols=["match_id"])`; dim-shaped tables (teams,
  venues, competitions) always read full Bronze.

Gold is NOT triggered — see CLAUDE.md for the Metabase DuckDB-lock dance.

### Manual trigger

```bash
airflow dags trigger dag_full_load_match_data
airflow dags trigger dag_full_load_match_data \\
  --conf '{"snapshot_date": "2026-05-01", "force": true}'
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
        execution_timeout=timedelta(minutes=30),
    )

    extract_archive = PythonOperator(
        task_id="extract_archive",
        python_callable=task_extract_archive,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
            "force": _FORCE,
        },
        execution_timeout=timedelta(hours=1),
    )

    load_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=task_load_bronze,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
            "force": _FORCE,
        },
        execution_timeout=timedelta(hours=2),
    )

    run_dq = PythonOperator(
        task_id="run_dq",
        python_callable=task_run_dq,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
        },
        execution_timeout=timedelta(minutes=30),
    )

    build_silver = PythonOperator(
        task_id="build_silver",
        python_callable=task_build_silver,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
            "force": _FORCE,
        },
        execution_timeout=timedelta(hours=2),
    )

    done = EmptyOperator(task_id="done", trigger_rule="all_done")

    check_infra >> download_archive >> extract_archive >> load_bronze >> run_dq >> build_silver >> done
