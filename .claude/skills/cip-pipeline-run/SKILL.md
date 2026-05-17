---
name: cip-pipeline-run
description: "Run any Cricket Intelligence Platform pipeline locally (ingest_people_and_names, ingest_match_data, build_silver_people_and_names, build_silver_match_data) with the correct env-var soup pre-applied. Use when the user wants to manually trigger a Bronze ingest or Silver build without going through Airflow — e.g. 'rerun yesterday's match ingest', 'rebuild Silver people for 2026-05-10'."
trigger: /cip-pipeline-run
---

# /cip-pipeline-run

Thin wrapper that runs one of the four CIP ingestion/transform jobs from the host shell with the right env-var overrides for the dockerised dev stack.

## Usage

```
/cip-pipeline-run                                                       # ask which pipeline + task
/cip-pipeline-run <pipeline>                                            # run with --task all + today's date
/cip-pipeline-run <pipeline> --task <task>                              # specific task
/cip-pipeline-run <pipeline> --snapshot-date YYYY-MM-DD --task <task>   # full control
/cip-pipeline-run <pipeline> ... --force                                # bypass control-schema idempotency guards
```

`<pipeline>` is one of:

| Pipeline | Tasks | Engine |
|---|---|---|
| `people-bronze` (or `ingest_people_and_names`) | `download`, `bronze`, `all` | Polars |
| `match-bronze` (or `ingest_match_data`) | `download`, `extract`, `bronze`, `dq`, `all` | Polars |
| `people-silver` (or `build_silver_people_and_names`) | `silver`, `dq`, `dbt`, `all` | Polars |
| `match-silver` (or `build_silver_match_data`) | `check`, `silver`, `all` | **PySpark** |

## What You Must Do When Invoked

If invoked with `--help` or `-h` (no other args), print the Usage block above and stop.

### Step 1 — Resolve the request

Parse the user's input into `{pipeline, task, snapshot_date, force}`. Defaults: `task=all`, `snapshot_date=today (YYYY-MM-DD)`, `force=false`.

If the user wrote natural language ("rerun yesterday's match Silver"), map it:
- "people" / "register" / "names" → `people-*`
- "match" / "deliveries" / "cricsheet" → `match-*`
- "ingest" / "bronze" / "land" → `*-bronze`
- "silver" / "build silver" / "transform" → `*-silver`
- "yesterday" → today's date minus 1
- "rerun" / "force" / "again" → `--force`

If pipeline is ambiguous, **ask** with `AskUserQuestion` — do not guess between people and match. If task is omitted, default to `all`.

### Step 2 — Map to module path

| Pipeline alias | Python module |
|---|---|
| `people-bronze` | `cip.ingestion.jobs.ingest_people_and_names` |
| `match-bronze` | `cip.ingestion.jobs.ingest_match_data` |
| `people-silver` | `cip.ingestion.jobs.build_silver_people_and_names` |
| `match-silver` | `cip.ingestion.jobs.build_silver_match_data` |

### Step 3 — Pick the env-var profile

**Polars profile** (default — all pipelines except `match-silver`):
```
ICEBERG_REST_URI=http://localhost:8181
MINIO_S3_ENDPOINT=http://localhost:9000
POSTGRES_HOST=localhost
```

**Spark profile** (`match-silver` only — adds to the Polars profile):
```
SPARK_DRIVER_MEMORY=8g
SPARK_MASTER=local[2]
```

The localhost overrides exist because the .env values point at Docker-internal hostnames (`iceberg-rest`, `minio`, `postgres`) which don't resolve from the host shell.

### Step 4 — Build and run the command

Construct a single Bash command:

```bash
<ENV_BLOCK> poetry run python -m <MODULE> \
    --snapshot-date <DATE> \
    --task <TASK> \
    [--force]
```

Tell the user the resolved invocation before running it (one short line — pipeline + date + task + force flag), then execute via Bash. Do not run in background — pipelines emit log lines users want to see live.

### Step 5 — Report

On success: print the last 10 lines of output and confirm the task that ran.
On failure: print the full traceback and surface the exit code. Do NOT retry automatically — the user may want to inspect the failure first.

## Pre-flight checks

Before invoking, verify (cheap, fast):
- `docker ps --filter name=compose-postgres-1 --filter status=running -q` is non-empty (services are up). If not, tell the user to run `make up` first.
- For Spark pipelines, also check `compose-iceberg-rest-1` is up.

Do **not** check `pyproject.toml` or `.env` existence — assume the project is set up.

## Examples

```
User: /cip-pipeline-run match-bronze --snapshot-date 2026-05-10 --task all --force
You:  Running ingest_match_data for snapshot_date=2026-05-10, task=all, force=True (Polars profile)
      [runs the command]

User: rerun yesterday's people silver
You:  Running build_silver_people_and_names for snapshot_date=2026-05-15, task=all (Polars profile)
      [runs the command]

User: /cip-pipeline-run match-silver
You:  Running build_silver_match_data for snapshot_date=2026-05-16, task=all (Spark profile, 8g driver, local[2])
      [runs the command]
```

## Honesty rules

- If the user requests a task that doesn't exist for the chosen pipeline (e.g. `extract` on `people-bronze`), tell them the valid tasks and ask.
- If the user passes a snapshot-date in the future, warn but proceed if confirmed.
- Never silently substitute pipelines. If "ingest" is given without people/match, ask.
