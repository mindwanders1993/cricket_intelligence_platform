# orchestration/airflow/dags/dag_ingest_cricsheet_register.py
#
# DAG: dag_ingest_cricsheet_register
#
# Purpose:
#   Download Cricsheet Register files (people.csv, names.csv) from cricsheet.org,
#   upload to MinIO landing zone, detect schema drift, and log full audit trail
#   to the control schema.
#
# Schedule:  Weekly on Sunday at 06:00 IST (00:30 UTC)
#            Cricsheet updates their register periodically — weekly is sufficient.
#
# Flow:
#   check_infra  →  download_and_land  →  notify_schema_drift (conditional)
#
# Dependencies:
#   - MinIO reachable + cricket-landing bucket exists
#   - PostgreSQL control schema bootstrapped (init-metastore.sql)

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from cip.ingestion.jobs.ingest_cricsheet_register import task_download_and_land

# ---------------------------------------------------------------------------
# DAG-level defaults
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Infrastructure check — fails fast before downloading anything
# ---------------------------------------------------------------------------
def _check_infra(**context) -> None:
    """Verify MinIO is reachable and control DB is accessible before downloading."""
    import psycopg2

    from cip.common.settings import get_settings
    from cip.ingestion.io.minio import MinIOClient

    # MinIO health check
    client = MinIOClient.from_settings()
    client.health_check()

    # PostgreSQL connectivity
    cfg = get_settings().postgres
    conn = psycopg2.connect(cfg.dsn)
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM control.register_ingestion_log LIMIT 0")
    conn.close()

    import logging

    logging.getLogger(__name__).info("Infrastructure check passed")


# ---------------------------------------------------------------------------
# Conditional schema drift notification
# ---------------------------------------------------------------------------
def _has_schema_drift(**context) -> bool:
    """ShortCircuitOperator: proceeds only if schema drift was detected."""
    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="download_and_land")
    return bool(payload and payload.get("any_schema_changed"))


def _log_schema_drift_alert(**context) -> None:
    """Log a structured alert for schema drift. Wire to Slack/PagerDuty in prod."""
    import logging

    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="download_and_land")
    logger = logging.getLogger(__name__)
    for file_info in payload.get("files", []):
        if file_info.get("is_schema_changed"):
            logger.warning(
                "SCHEMA DRIFT ALERT",
                extra={
                    "source_file": file_info["source_file"],
                    "new_columns": file_info.get("new_columns", []),
                    "snapshot_date": payload["snapshot_date"],
                    "action_required": (
                        "Review control.register_schema_versions and update "
                        "Bronze loader column mapping if new key_* columns were added."
                    ),
                },
            )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dag_ingest_cricsheet_register",
    description="Download Cricsheet Register CSVs → MinIO landing → control schema audit",
    start_date=datetime(2026, 5, 11, 0, 30),  # 06:00 IST = 00:30 UTC
    schedule="30 0 * * 0",  # Weekly, Sunday 00:30 UTC
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["ingestion", "register", "landing", "cricsheet"],
    doc_md="""
## dag_ingest_cricsheet_register

Downloads `people.csv` and `names.csv` from [cricsheet.org](https://cricsheet.org/register/)
and lands them in MinIO under `s3://cricket-landing/register_csv/snapshot_date={date}/`.

### What it does
1. **check_infra** — verifies MinIO + PostgreSQL are reachable
2. **download_and_land** — HTTP download → SHA256 checksum → MinIO upload → control DB log
3. **schema_drift_check** — conditional branch: alerts if new columns detected

### Idempotency
Re-running this DAG for the same `snapshot_date` is safe — the downloader skips
files already marked `SUCCESS` in `control.register_ingestion_log`.
Use `force=True` via Airflow Variables to override.

### Manual trigger
```bash
airflow dags trigger dag_ingest_cricsheet_register \\
  --conf '{"snapshot_date": "2026-05-10", "force": false}'
```
    """,
) as dag:
    # Task 1 — Infrastructure readiness gate
    check_infra = PythonOperator(
        task_id="check_infra",
        python_callable=_check_infra,
        doc_md="Verifies MinIO health and PostgreSQL control schema connectivity.",
    )

    # Task 2 — Core download + land task
    download_and_land = PythonOperator(
        task_id="download_and_land",
        python_callable=task_download_and_land,
        op_kwargs={
            # snapshot_date defaults to today via Airflow macro
            "snapshot_date": "{{ dag_run.conf.get('snapshot_date', ds) }}",
            "pipeline_run_id": "{{ run_id }}",
            "force": "{{ dag_run.conf.get('force', False) }}",
        },
        doc_md="Downloads people.csv + names.csv, uploads to landing, writes control DB audit.",
    )

    # Task 3 — Schema drift short-circuit
    schema_drift_check = ShortCircuitOperator(
        task_id="schema_drift_check",
        python_callable=_has_schema_drift,
        doc_md="Short-circuits if no schema drift was detected. Proceeds to alert if drift found.",
    )

    # Task 4 — Drift alert (only runs if schema changed)
    schema_drift_alert = PythonOperator(
        task_id="schema_drift_alert",
        python_callable=_log_schema_drift_alert,
        doc_md="Logs structured warning. Wire to Slack/PagerDuty webhook for production.",
    )

    # Task 5 — Done marker
    done = EmptyOperator(
        task_id="done",
        trigger_rule="none_failed_min_one_success",
    )

    # ---------------------------------------------------------------------------
    # Dependencies
    # ---------------------------------------------------------------------------
    check_infra >> download_and_land >> schema_drift_check >> schema_drift_alert >> done
    download_and_land >> done
