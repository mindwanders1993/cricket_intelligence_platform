# orchestration/airflow/dags/dag_ingest_match_data.py
#
# DAG: dag_ingest_match_data
#
# Purpose:
#   Cricsheet match archive pipeline: Download all_json.zip → Extract JSONs
#   → Bronze Iceberg load → DQ checks.
#
# Schedule: Monthly on the 1st at 01:00 UTC
#
# Task graph:
#
#   check_infra
#       └─► download_archive
#             └─► extract_archive
#                   └─► load_bronze
#                         └─► run_dq
#                               └─► done
#
# Idempotency:
#   - download_archive: skips if status=SUCCESS already in archive_download_log.
#   - extract_archive:  skips if extracted_path is set in archive_download_log.
#   - load_bronze:      skips if status=SUCCESS in bronze_match_ingestion_log.
#   - All tasks: pass {"force": true} in dag_run.conf to bypass.
#
# Manual trigger examples:
#   airflow dags trigger dag_ingest_match_data \
#     --conf '{"snapshot_date": "2026-05-01"}'
#
#   airflow dags trigger dag_ingest_match_data \
#     --conf '{"snapshot_date": "2026-04-01", "force": true}'

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from cip.ingestion.jobs.ingest_match_data import (
    task_download_archive,
    task_extract_archive,
    task_load_bronze,
    task_run_dq,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG-level defaults
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Jinja template tokens
# ---------------------------------------------------------------------------
#
# _SNAPSHOT_DATE:  User can override via conf["snapshot_date"]; otherwise
#                  the wall-clock UTC date the task runs on is used.
#                  Intentionally NOT `ds` (Airflow logical date) — see register DAG.
# _PIPELINE_RUN_ID: Airflow run_id is unique per DAG run.
# _FORCE:           Defaults to False; set conf["force"] = true to bypass
#                   idempotency guards in all tasks.

_SNAPSHOT_DATE = "{{ dag_run.conf.get('snapshot_date', macros.datetime.utcnow().strftime('%Y-%m-%d')) }}"
_PIPELINE_RUN_ID = "{{ run_id }}"
_FORCE = "{{ dag_run.conf.get('force', False) }}"


# ---------------------------------------------------------------------------
# Infrastructure health check
# ---------------------------------------------------------------------------


def _check_infra(**context) -> None:
    """
    Gate task: verify MinIO and PostgreSQL control schema are reachable.
    Fails fast so retries are not wasted on infrastructure outages.
    """
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
    finally:
        conn.close()

    logger.info(
        "Infrastructure check passed — MinIO and PostgreSQL reachable",
        extra={"dag_run_id": context.get("run_id")},
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="dag_ingest_match_data",
    description=(
        "Cricsheet match archive pipeline: Download all_json.zip " "→ Extract JSONs → Bronze Iceberg → DQ checks"
    ),
    start_date=datetime(2026, 5, 1),
    schedule="0 1 1 * *",  # 1st of each month at 01:00 UTC
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["ingestion", "archive", "bronze", "dq", "cricsheet"],
    doc_md="""
## dag_ingest_match_data

Downloads `all_json.zip` from [cricsheet.org](https://cricsheet.org/downloads/all_json.zip)
and writes all match JSON documents to `bronze.match_data`.

### Task graph

```
check_infra
└─► download_archive
      └─► extract_archive
            └─► load_bronze
                  └─► run_dq
                        └─► done
```

### Bronze target table

| Iceberg table | Source | Key columns |
|---|---|---|
| `bronze.match_data` | `all_json.zip` JSON files | `match_id, revision` |

### Idempotency

- `download_archive`: skips if `status=SUCCESS` in `control.archive_download_log`
  for `(archive_file, snapshot_date)`.
- `extract_archive`:  skips if `extracted_path IS NOT NULL` in same table.
- `load_bronze`:      skips if `status=SUCCESS` in `control.bronze_match_ingestion_log`.
- All stages: pass `{"force": true}` in `dag_run.conf` to bypass.

### Revision logic

Each monthly run appends rows with `revision = MAX(existing revision) + 1` per
`match_id`. Silver reads only `MAX(revision)` per match_id. First-ever load → revision=1.

### Manual trigger

```bash
# Normal monthly run
airflow dags trigger dag_ingest_match_data \\
  --conf '{"snapshot_date": "2026-05-01"}'

# Force re-run for a historical snapshot
airflow dags trigger dag_ingest_match_data \\
  --conf '{"snapshot_date": "2026-04-01", "force": true}'
```

### Observability

- Download log: `SELECT * FROM control.archive_download_log ORDER BY id DESC LIMIT 10;`
- Bronze load log: `SELECT * FROM control.bronze_match_ingestion_log ORDER BY id DESC LIMIT 10;`
- DQ results:
  `SELECT * FROM control.dq_results WHERE dag_id = 'dag_ingest_match_data' ORDER BY checked_at DESC;`
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

    # ── Task 2: Download archive ─────────────────────────────────────────────
    download_archive = PythonOperator(
        task_id="download_archive",
        python_callable=task_download_archive,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
            "force": _FORCE,
        },
        execution_timeout=timedelta(minutes=30),
        doc_md=(
            "Downloads all_json.zip from cricsheet.org → SHA-256 checksum → "
            "MinIO cricket-source-files bucket (match_data/zip/snapshot_date=.../all_json.zip) → "
            "control.archive_download_log."
        ),
    )

    # ── Task 3: Extract archive ──────────────────────────────────────────────
    extract_archive = PythonOperator(
        task_id="extract_archive",
        python_callable=task_extract_archive,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
            "force": _FORCE,
        },
        execution_timeout=timedelta(hours=1),
        doc_md=(
            "Downloads ZIP from MinIO, extracts ~21k JSON files, "
            "uploads each to match_data/json/snapshot_date=.../ with 20-worker pool, "
            "writes _manifest.json for downstream verification."
        ),
    )

    # ── Task 4: Load Bronze ──────────────────────────────────────────────────
    load_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=task_load_bronze,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
            "force": _FORCE,
        },
        execution_timeout=timedelta(hours=2),
        doc_md=(
            "Reads JSON files from MinIO match_data/json prefix (20-worker pool), "
            "parses match header fields (match_type, gender, season, teams, venue), "
            "attaches revision numbers (MAX existing + 1 per match_id), "
            "writes all-string rows to bronze.match_data."
        ),
    )

    # ── Task 5: Run DQ checks ────────────────────────────────────────────────
    run_dq = PythonOperator(
        task_id="run_dq",
        python_callable=task_run_dq,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
        },
        execution_timeout=timedelta(minutes=30),
        doc_md=(
            "Runs 4 DQ checks against Bronze match_documents:\n"
            "- MAT-BRZ-001  files_failed == 0 in ingestion log (BLOCK)\n"
            "- MAT-BRZ-002  (match_id, revision) unique per snapshot (BLOCK)\n"
            "- MAT-BRZ-003  Bronze row count == manifest file_count (BLOCK)\n"
            "- MAT-BRZ-004  metadata coverage null rate <= 1% (WARN)\n"
            "Results persisted to control.dq_results. BLOCK failures fail this task."
        ),
    )

    # ── Task 6: Done marker ──────────────────────────────────────────────────
    done = EmptyOperator(
        task_id="done",
        trigger_rule="all_done",
        doc_md="Terminal marker. Used for SLA monitoring and downstream DAG sensors.",
    )

    # ── Dependency graph ─────────────────────────────────────────────────────
    #
    #   check_infra
    #       └─► download_archive
    #             └─► extract_archive
    #                   └─► load_bronze
    #                         └─► run_dq
    #                               └─► done
    #
    check_infra >> download_archive >> extract_archive >> load_bronze >> run_dq >> done
