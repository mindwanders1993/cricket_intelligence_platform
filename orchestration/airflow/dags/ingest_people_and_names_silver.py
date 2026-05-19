# orchestration/airflow/dags/ingest_people_and_names_silver.py
#
# DAG: ingest_people_and_names_silver
#
# Purpose:
#   Promote Bronze Register tables to Silver Iceberg tables and run DQ checks.
#   Runs after ingest_people_and_names_bronze completes.
#
#   bronze.people          → silver.persons
#   bronze.people_identifiers     → silver.person_identifiers
#   bronze.name_variations → silver.name_variations
#
# Schedule: Weekly on Sunday at 07:00 IST (01:30 UTC) — 1h after bronze DAG
#
# Task graph:
#
#   load_silver
#       └─► run_dq
#             └─► done
#
# Idempotency:
#   PolarsPeopleAndNamesSilverTransform uses overwrite_partition — re-runs for the
#   same snapshot_date replace only that partition, leaving others intact.
#
# Manual trigger examples:
#   airflow dags trigger ingest_people_and_names_silver \
#     --conf '{"snapshot_date": "2026-05-11"}'

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from cip.ingestion.jobs.build_silver_people_and_names import (
    task_load_silver,
    task_run_dq,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG-level defaults
# ---------------------------------------------------------------------------

_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ---------------------------------------------------------------------------
# Jinja template tokens
# ---------------------------------------------------------------------------

_SNAPSHOT_DATE = "{{ dag_run.conf.get('snapshot_date', macros.datetime.utcnow().strftime('%Y-%m-%d')) }}"
_PIPELINE_RUN_ID = "{{ run_id }}"

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="ingest_people_and_names_silver",
    description=(
        "Register Silver build: Bronze Register tables → Silver persons / "
        "person_identifiers / name_variations + DQ checks"
    ),
    start_date=datetime(2026, 5, 1),
    schedule="30 1 * * 0",  # Sunday 01:30 UTC ≡ 07:00 IST (1h after bronze DAG)
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["silver", "cricsheet", "register", "dq"],
    doc_md="""
## ingest_people_and_names_silver

Promotes three Bronze Register tables to Silver Iceberg tables and runs
Register DQ checks. Designed to run after `ingest_people_and_names_bronze` completes.

### Task graph

```
load_silver
└─► run_dq
      └─► done
```

### Silver target tables

| Iceberg table | Source Bronze table | Key transform |
|---|---|---|
| `silver.persons` | `register_people` | identifier → person_id, deduped |
| `silver.person_identifiers` | `register_identifiers` | key_source → source_system |
| `silver.name_variations` | `register_name_variations` | deduped on identifier + name |

### DQ checks (run_dq)

| Check ID | Table | Type | Severity |
|---|---|---|---|
| REG-SLV-001 | silver.persons | person_id not null | BLOCK |
| REG-SLV-002 | silver.persons | person_id unique | BLOCK |
| REG-SLV-003 | silver.person_identifiers | key columns not null | BLOCK |
| REG-SLV-004 | silver.person_identifiers | unique grain | WARN |
| REG-SLV-005 | bronze.people | row count vs people.csv | BLOCK |
| REG-SLV-006 | bronze.name_variations | row count vs names.csv | BLOCK |
| REG-SLV-007 | silver.name_variations | orphan identifiers | WARN |

Results persisted to `control.dq_results`. BLOCK failures fail the task.

### Idempotency

`load_silver` uses `overwrite_partition` — re-running for the same `snapshot_date`
replaces only that partition. Safe to re-trigger without data duplication.

### Manual trigger

```bash
airflow dags trigger ingest_people_and_names_silver \\
  --conf '{"snapshot_date": "2026-05-11"}'
```

### Observability

- DQ results: `SELECT * FROM control.dq_results WHERE pipeline_run_id = '<run_id>';`
""",
) as dag:
    # ── Task 1: Load Silver ──────────────────────────────────────────────────
    load_silver = PythonOperator(
        task_id="load_silver",
        python_callable=task_load_silver,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
        },
        execution_timeout=timedelta(minutes=30),
        doc_md=(
            "Polars transform: Bronze Register tables → Silver Iceberg tables:\n"
            "- silver.persons (identifier→person_id, deduped)\n"
            "- silver.person_identifiers (key_source→source_system)\n"
            "- silver.name_variations (deduped on identifier+name)"
        ),
    )

    # ── Task 2: Run DQ checks ────────────────────────────────────────────────
    run_dq = PythonOperator(
        task_id="run_dq",
        python_callable=task_run_dq,
        op_kwargs={
            "snapshot_date": _SNAPSHOT_DATE,
            "pipeline_run_id": _PIPELINE_RUN_ID,
        },
        execution_timeout=timedelta(minutes=15),
        doc_md=(
            "Runs 7 DQ checks against Silver + Bronze Iceberg tables for this snapshot. "
            "Results persisted to control.dq_results. BLOCK failures fail this task."
        ),
    )

    # ── Task 3: Done marker ──────────────────────────────────────────────────
    done = EmptyOperator(
        task_id="done",
        trigger_rule="all_done",
        doc_md="Terminal marker. Used for SLA monitoring and downstream DAG sensors.",
    )

    # ── Dependency graph ─────────────────────────────────────────────────────
    load_silver >> run_dq >> done
