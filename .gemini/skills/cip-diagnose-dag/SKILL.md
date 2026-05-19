---
name: cip-diagnose-dag
description: "Diagnose a failed Cricket Intelligence Platform Airflow DAG run: fetch task logs, inspect control-schema audit row, look for landing artifacts, and propose a root cause + retry strategy. Use when the user says 'why did the DAG fail', 'check error logs <run_id>', or names a DAG that errored."
trigger: /cip-diagnose-dag
---

# /cip-diagnose-dag

Pulls together everything needed to understand a single failed Airflow run without bouncing between the UI, container logs, and PostgreSQL: task logs from the scheduler container, the audit row from `control.*_ingestion_log`, landing-bucket presence in MinIO, and a hypothesis with a suggested next step.

## Usage

```
/cip-diagnose-dag                              # ask which DAG run
/cip-diagnose-dag <run_id>                     # diagnose a specific Airflow run id
/cip-diagnose-dag <dag_name>                   # diagnose the most recent failed run of this DAG
/cip-diagnose-dag <dag_name> --last-success    # show the last successful run for comparison
```

DAG names (from `orchestration/airflow/dags/`):
- `ingest_people_and_names_bronze`
- `ingest_people_and_names_silver`
- `ingest_all_match_data_bronze`
- `ingest_all_match_data_silver`
- `ingest_all_match_data_gold`
- `ingest_two_day_match_data_bronze`
- `ingest_two_day_match_data_silver`
- `ingest_two_day_match_data_gold`
- `dag_parse_bronze_match_data` (placeholder — not yet implemented)

## What You Must Do When Invoked

If invoked with `--help` or `-h`, print Usage and stop.

### Step 1 — Identify the run

`airflow dags list-runs` **requires** `-d DAG_ID` — you cannot look up a run by run_id alone. Resolve `(DAG_NAME, RUN_ID)` first:

- **DAG_NAME + RUN_ID both given** → use both directly. Skip to Step 2.
- **DAG_NAME only** → find the most recent failed run:
  ```bash
  docker exec compose-airflow-scheduler-1 \
      airflow dags list-runs -d <DAG_NAME> --state failed -o json 2>/dev/null \
      | python3 -c "import json,sys; runs=json.load(sys.stdin); print(runs[0]['run_id']) if runs else print('NONE')"
  ```
  If `NONE`, tell the user there are no failed runs for that DAG.
- **RUN_ID only** → ask via `ask_user tool` which DAG it belongs to (8 active DAGs listed above). No CLI lookup is possible without the DAG name.
- **Neither** → `ask_user tool` for the DAG name.

### Step 2 — Fetch task logs

First, find which task(s) failed:

```bash
docker exec compose-airflow-scheduler-1 \
    airflow tasks states-for-dag-run <DAG_NAME> <RUN_ID> -o json
```

There is **no** `airflow tasks logs` subcommand. Logs live on disk inside the scheduler container at `/opt/airflow/logs/dag_id=<DAG>/run_id=<RUN_ID>/task_id=<TASK>/attempt=<N>.log`. The `run_id` can contain colons (e.g. `manual__2026-05-16T08:36:18.272955+00:00`) — quote everything.

For each failed task, fetch the latest attempt log:

```bash
docker exec compose-airflow-scheduler-1 bash -c \
    'find "/opt/airflow/logs/dag_id=<DAG_NAME>/run_id=<RUN_ID>/task_id=<TASK_ID>" -name "attempt=*.log" 2>/dev/null | sort | tail -1 | xargs -r cat' \
    | tail -200
```

Don't dump the full log — extract: the **traceback** (if any), the **last 30 lines**, and any `ERROR`/`CRITICAL` log lines.

### Step 3 — Check the control-schema audit

Pipelines write audit rows to `control.*_ingestion_log`. Pick the right table:

| DAG | Control table |
|---|---|
| `ingest_people_and_names_bronze`, `ingest_people_and_names_silver` | `control.register_ingestion_log` |
| `ingest_all_match_data_bronze`, `ingest_two_day_match_data_bronze` | `control.archive_download_log` |
| `ingest_all_match_data_silver`, `ingest_two_day_match_data_silver` | `control.match_file_audit` |
| `ingest_all_match_data_gold`, `ingest_two_day_match_data_gold` | (no dedicated table — relies on dbt artifacts + `control.match_file_audit.gold_loaded_at`) |

```bash
docker exec compose-postgres-1 \
    psql -U postgres -d cricket_platform -c \
    "SELECT pipeline_run_id, task, snapshot_date, status, error_message, started_at, finished_at
     FROM control.<TABLE>
     WHERE pipeline_run_id = '<RUN_ID>' OR dag_run_id = '<RUN_ID>'
     ORDER BY started_at DESC LIMIT 10;"
```

Surface: the `status` (RUNNING / SUCCESS / FAILED / SKIPPED), `error_message`, and how far through the task list the pipeline got before failing.

### Step 4 — Check landing artifacts (Bronze ingest DAGs only)

For `dag_ingest_*` failures, check whether the source file made it to MinIO landing:

```bash
docker exec compose-minio-1 \
    mc ls --recursive local/cricket-source-files/landing/ | head -20
```

For Bronze build failures, check the Bronze Iceberg path was even written. Use `/cip-inspect-table bronze.<entity>` for a quick row count.

### Step 5 — Hypothesise and recommend

Pattern-match the error against known failure modes (memory: project_*):

| Pattern in logs | Likely cause | Suggested action |
|---|---|---|
| `ModuleNotFoundError: pydantic_settings` (or pyspark, etc.) | Airflow image stale | `make build-airflow` then `make up` |
| `Connection refused: iceberg-rest:8181` | REST catalog container down or unhealthy | `docker ps`, restart with `make down && make up` |
| `IO Error: Could not set lock on file ... cricket.duckdb` | DuckDB UI running concurrently | `make duckdb-stop`, re-trigger |
| `psycopg2.OperationalError ... role "postgres"` | Password mismatch with `.env` | recreate postgres container after `.env` change |
| `No such file or directory: people.csv` | Cricsheet URL changed / blocked | Check `cip.ingestion.people_and_names.download` |
| `S3Error: Access Denied` on MinIO | bucket policy or creds stale | `make bootstrap` to recreate buckets + policies |
| `RuntimeError: Java gateway process exited` (Spark only) | JAR download or memory | Check `SPARK_DRIVER_MEMORY`, internet for Maven |

Report findings in this shape:

```
=== DAG: <name>  Run: <run_id>  State: failed  Failed task: <task_id>

Root cause hypothesis:
  <one or two sentences>

Evidence:
  - Traceback excerpt: <key line>
  - Control row: status=<X>, error_message=<Y>
  - Landing: <present|missing>

Suggested next step:
  <one concrete command or action>

Comparable last success (if --last-success): <run_id at YYYY-MM-DD HH:MM:SS>
```

## Pre-flight checks

- `docker ps --filter name=compose-airflow-scheduler-1 -q` must be non-empty.
- `docker ps --filter name=compose-postgres-1 -q` must be non-empty.

## Honesty rules

- Do not invent root causes. If logs don't match a known pattern, say so and surface the raw traceback for the user to read.
- Don't trigger retries automatically. Suggest the command; let the user decide.
- If multiple tasks failed, diagnose the **earliest** one — downstream failures are usually noise.
