# orchestration/airflow/dags/dag_ingest_cricsheet_register.py
#
# DAG: dag_ingest_cricsheet_register
#
# Purpose:
#   Cricsheet Register pipeline: Download + Land → Bronze Iceberg load.
#   people.csv + names.csv → cricket.bronze.register_people
#                          → cricket.bronze.register_identifiers
#                          → cricket.bronze.register_name_variations
#
# Schedule: Weekly on Sunday at 06:00 IST (00:30 UTC)
#
# Task graph:
#
#   check_infra
#       └─► download_and_land
#             ├─► schema_drift_check ──(drift only)──► schema_drift_alert
#             └─► load_bronze
#                   └─► done
#
# Idempotency:
#   - download_and_land: skips if status=SUCCESS already in control DB.
#   - load_bronze: append-only by default; pass {"force": true} to delete the
#     _snapshot_date partition before re-writing (overwrite_snapshot).
#
# Manual trigger examples:
#   # Normal weekly run with explicit date
#   airflow dags trigger dag_ingest_cricsheet_register \
#     --conf '{"snapshot_date": "2026-05-11"}'
#
#   # Force re-run for a past snapshot
#   airflow dags trigger dag_ingest_cricsheet_register \
#     --conf '{"snapshot_date": "2026-05-04", "force": true}'

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from cip.ingestion.jobs.ingest_cricsheet_register import task_download_and_land, task_load_bronze, task_load_silver

logger = logging.getLogger(__name__)

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
# Jinja template tokens — resolved at task execution time from dag_run.conf
# ---------------------------------------------------------------------------
#
# _SNAPSHOT_DATE:  User can override via conf["snapshot_date"]; otherwise
#                  Airflow's logical date (ds = YYYY-MM-DD) is used.
# _PIPELINE_RUN_ID: Airflow's run_id is always unique per DAG run — used
#                   as the pipeline_run_id throughout the control schema.
# _FORCE:           Defaults to False; set conf["force"] = true to bypass
#                   idempotency guards in both tasks.
#
_SNAPSHOT_DATE = "{{ dag_run.conf.get('snapshot_date', ds) }}"
_PIPELINE_RUN_ID = "{{ run_id }}"
_FORCE = "{{ dag_run.conf.get('force', False) }}"


# ---------------------------------------------------------------------------
# Infrastructure health check
# ---------------------------------------------------------------------------


def _check_infra(**context) -> None:
    """
    Gate task: verify MinIO and PostgreSQL control schema are reachable
    before spending network budget downloading Register files.

    Raises immediately if either dependency is unavailable so the DAG
    fails fast instead of wasting a retry on infrastructure issues.
    """
    import psycopg2
    from cip.common.settings import get_settings
    from cip.ingestion.io.minio import MinIOClient

    # MinIO health check — raises on connection failure
    MinIOClient.from_settings().health_check()

    # PostgreSQL control schema reachability — lightweight table probe
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
            # Validates that control schema and table exist without reading rows
            cur.execute("SELECT 1 FROM control.register_ingestion_log LIMIT 0")
    finally:
        conn.close()

    logger.info(
        "Infrastructure check passed — MinIO and PostgreSQL reachable",
        extra={"dag_run_id": context.get("run_id")},
    )


# ---------------------------------------------------------------------------
# Schema drift branch
# ---------------------------------------------------------------------------


def _has_schema_drift(**context) -> bool:
    """
    ShortCircuitOperator callable.

    Reads the XCom payload from download_and_land and returns True only if
    any source file had schema drift detected. When False, Airflow
    short-circuits and skips schema_drift_alert (trigger_rule=all_done
    on load_bronze ensures Bronze load still runs regardless).

    Returns:
        bool: True if drift detected (alert fires), False to skip alert.
    """
    ti = context["ti"]
    payload: dict = ti.xcom_pull(task_ids="download_and_land") or {}
    drift = bool(payload.get("any_schema_changed", False))

    logger.info(
        "Schema drift check",
        extra={
            "any_schema_changed": drift,
            "snapshot_date": payload.get("snapshot_date"),
        },
    )
    return drift


def _log_schema_drift_alert(**context) -> None:
    """
    Emit a structured WARNING for each drifted file.

    In production, replace the logger.warning call with a Slack webhook
    or PagerDuty event. The structured fields are ready for log-based
    alerting (Grafana Loki, Datadog, CloudWatch Insights).

    Action required when this fires:
      1. Inspect control.register_schema_versions for new/removed columns.
      2. If new key_* columns: no code change needed (auto-detected by loader).
      3. If core columns changed: update Silver dbt models accordingly.
    """
    ti = context["ti"]
    payload: dict = ti.xcom_pull(task_ids="download_and_land") or {}

    for f in payload.get("files", []):
        if not f.get("is_schema_changed"):
            continue
        logger.warning(
            "SCHEMA DRIFT ALERT — Cricsheet Register column change detected",
            extra={
                "source_file": f["source_file"],
                "new_columns": f.get("new_columns", []),
                "removed_columns": f.get("removed_columns", []),
                "snapshot_date": payload.get("snapshot_date"),
                "pipeline_run_id": payload.get("pipeline_run_id"),
                "control_table": "control.register_schema_versions",
                "action_required": (
                    "Review control.register_schema_versions. "
                    "New key_* columns flow automatically into Bronze. "
                    "Structural column removals require Silver dbt model review."
                ),
            },
        )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="dag_ingest_cricsheet_register",
    description=(
        "Cricsheet Register pipeline: Download (people.csv + names.csv) " "→ MinIO landing → Bronze Iceberg tables"
    ),
    start_date=datetime(2026, 5, 1),
    schedule="30 0 * * 0",  # Sunday 00:30 UTC ≡ 06:00 IST
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["ingestion", "register", "landing", "bronze", "cricsheet"],
    doc_md="""
## dag_ingest_cricsheet_register

Downloads `people.csv` + `names.csv` from [cricsheet.org](https://cricsheet.org/register/)
and writes them to three Bronze Iceberg tables.

### Task graph

```
check_infra
└─► download_and_land
      ├─► schema_drift_check ──(drift only)──► schema_drift_alert
      └─► load_bronze
            └─► done
```

`schema_drift_alert` and `load_bronze` are parallel branches off `download_and_land`.
`load_bronze` uses `trigger_rule=all_done` so it always runs regardless of the drift branch.

### Bronze target tables

| Iceberg table | Source | Key columns |
|---|---|---|
| `cricket.bronze.register_people` | `people.csv` | `identifier` |
| `cricket.bronze.register_identifiers` | `people.csv` key_* | `identifier, key_source, key_value` |
| `cricket.bronze.register_name_variations` | `names.csv` | `identifier, name` |

### Idempotency

- `download_and_land`: skips if `status=SUCCESS` already recorded in
  `control.register_ingestion_log` for `(source_file, snapshot_date)`.
- `load_bronze`: append-only by default (`force=false`). Pass `{"force": true}`
  to delete the `_snapshot_date` partition before re-writing
  (`RegisterLoader.overwrite_snapshot`).

### Manual trigger

```bash
# Normal run
airflow dags trigger dag_ingest_cricsheet_register \
  --conf '{"snapshot_date": "2026-05-11"}'

# Force re-run for a historical snapshot
airflow dags trigger dag_ingest_cricsheet_register \
  --conf '{"snapshot_date": "2026-05-04", "force": true}'
```

### Observability

- Control DB audit: `SELECT * FROM control.register_ingestion_log ORDER BY id DESC LIMIT 10;`
- Schema drift history: `SELECT * FROM control.register_schema_versions ORDER BY snapshot_date DESC;`
""",
) as dag:

    # ── Task 1: Infrastructure gate ─────────────────────────────────────────
    check_infra = PythonOperator(
        task_id="check_infra",
        python_callable=_check_infra,
        execution_timeout=timedelta(minutes=2),
        doc_md=(
            "Verifies MinIO health and PostgreSQL control schema connectivity. "
            "Fails fast so retries are not wasted on infrastructure outages."
        ),
    )

    # ── Task 2: Download + Land ──────────────────────────────────────────────
    download_and_land = PythonOperator(
        task_id="download_and_land",
        python_callable=task_download_and_land,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
            "force": _FORCE,
        },
        execution_timeout=timedelta(minutes=15),
        doc_md=(
            "HTTP download of people.csv + names.csv → SHA-256 checksum → "
            "MinIO landing zone → control.register_ingestion_log + "
            "control.register_schema_versions."
        ),
    )

    # ── Task 3a: Schema drift gate (parallel branch) ─────────────────────────
    schema_drift_check = ShortCircuitOperator(
        task_id="schema_drift_check",
        python_callable=_has_schema_drift,
        # ignore_downstream_trigger_rules=False so only the alert is skipped,
        # not load_bronze (load_bronze has its own trigger_rule=all_done)
        ignore_downstream_trigger_rules=False,
        doc_md=(
            "Short-circuits (skips schema_drift_alert) if no column changes detected. "
            "Does NOT block load_bronze — Bronze load runs regardless via trigger_rule=all_done."
        ),
    )

    schema_drift_alert = PythonOperator(
        task_id="schema_drift_alert",
        python_callable=_log_schema_drift_alert,
        doc_md=(
            "Emits structured WARNING log per drifted file. " "Wire to Slack/PagerDuty webhook for production alerting."
        ),
    )

    # ── Task 3b: Load Bronze ─────────────────────────────────────────────────
    load_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=task_load_bronze,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
            "force": _FORCE,
        },
        execution_timeout=timedelta(minutes=20),
        # all_done: load_bronze runs even if schema_drift_check short-circuits
        # or schema_drift_alert fails. Bronze load must not be blocked by the
        # alert branch, which is informational only.
        trigger_rule="all_done",
        doc_md=(
            "Reads landing CSVs from MinIO (all-string Polars), computes _row_hash, "
            "injects platform metadata, appends to Iceberg Bronze tables:\n"
            "- cricket.bronze.register_people\n"
            "- cricket.bronze.register_identifiers (key_* exploded)\n"
            "- cricket.bronze.register_name_variations"
        ),
    )

    # ── Task 4: Load Silver ──────────────────────────────────────────────────
    load_silver = PythonOperator(
        task_id="load_silver",
        python_callable=task_load_silver,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
        },
        execution_timeout=timedelta(minutes=30),
        doc_md=(
            "PySpark job: promotes Bronze Register tables to Silver Iceberg tables:\n"
            "- cricket.silver.persons (identifier→person_id, deduped)\n"
            "- cricket.silver.person_identifiers (key_source→source_system)\n"
            "- cricket.silver.name_variations (deduped on identifier+name)"
        ),
    )

    # ── Task 5: Done marker ──────────────────────────────────────────────────
    done = EmptyOperator(
        task_id="done",
        trigger_rule="all_done",
        doc_md="Terminal marker. Used for SLA monitoring and downstream DAG sensors.",
    )

    # ── Dependency graph ─────────────────────────────────────────────────────
    #
    #   check_infra
    #       └─► download_and_land
    #             ├─► schema_drift_check ──► schema_drift_alert
    #             └─► load_bronze
    #                   └─► load_silver
    #                         └─► done
    #
    # Note: load_bronze trigger_rule=all_done ensures Bronze runs whether or
    # not the schema drift branch fires. schema_drift_alert is informational.
    #
    check_infra >> download_and_land
    download_and_land >> schema_drift_check >> schema_drift_alert
    download_and_land >> load_bronze >> load_silver >> done
