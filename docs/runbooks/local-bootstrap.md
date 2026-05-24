# Local Bootstrap — Cricket Intelligence Platform

> From-zero setup on a fresh machine. Pair with `docs/runbooks/full-rebuild.md` (wipe + reboot of an existing checkout).

---

## Prerequisites

| Tool | Version | Install hint |
|---|---|---|
| Docker Desktop | recent (with Compose v2) | docker.com / `brew install --cask docker` |
| Python | 3.11+ | `pyenv install 3.11`; `pyenv local 3.11` |
| Poetry | 1.8+ | `curl -sSL https://install.python-poetry.org \| python3 -` |
| Node.js | v18 or v20 LTS | `nvm install 20` |
| Git | any modern | already installed on macOS |

**Disk:** ~30GB free (Docker images + Cricsheet archive + DuckDB file).
**RAM:** 16GB minimum, 32GB recommended (Spark + Iceberg JVM overhead).

---

## Phase 1 — Clone and configure

```bash
git clone <repo-url>
cd cricket_intelligence_platform
cp .env.example .env
```

Open `.env` and confirm the defaults are fine for local dev:

| Var | Default | Note |
|---|---|---|
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | dev creds | Change for any networked machine |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | dev creds | Same — local-only |
| `AIRFLOW_ADMIN_PASSWORD` | dev pwd | Used only on first boot; rotate via CLI afterwards |
| `MINIO_S3_ENDPOINT` | `http://minio:9000` | Containers use service name; host overrides to `localhost:9000` when running scripts directly |
| `POSTGRES_HOST` | `postgres` | Same — host scripts override to `localhost` |
| `ICEBERG_REST_URI` | `http://iceberg-rest:8181` | Same — host scripts override to `localhost:8181` |

(`.env.example` is the canonical reference for every variable.)

---

## Phase 2 — Build the custom Airflow image

The Airflow container needs PySpark + JDK17 + the Iceberg JAR cache + Pydantic v2. The build is heavy (~5–10 min) but happens once.

```bash
make build-airflow
```

Re-run if the Dockerfile or `pyproject.toml` changes (memory: `project_custom_airflow_image`).

---

## Phase 3 — Bring the stack up

```bash
make up           # MinIO + Postgres + Iceberg REST + Airflow + MLflow + Metabase + pgAdmin
make bootstrap    # MinIO buckets + control schema DDL
```

Wait until all healthchecks pass (~60s). `docker ps` should show every service in `healthy` status (Iceberg REST may show `started` — its healthcheck uses bash `/dev/tcp` because the container has no curl/wget).

---

## Phase 4 — Verify each UI

| URL | Service | Login |
|---|---|---|
| http://localhost:8080 | Airflow | `admin` / `$AIRFLOW_ADMIN_PASSWORD` |
| http://localhost:9001 | MinIO Console | `$MINIO_ROOT_USER` / `$MINIO_ROOT_PASSWORD` |
| http://localhost:5050 | pgAdmin | `admin@cricket-platform.local` / `admin123` |
| http://localhost:3000 | Metabase | `admin@cricket-platform.local` / `Cricket2026!` |
| http://localhost:5001 | MLflow | — |
| http://localhost:8181 | Iceberg REST | API-only — `curl /v1/config` returns JSON |

If Airflow login fails after an `.env` change to `AIRFLOW_ADMIN_PASSWORD`:

```bash
docker exec compose-airflow-scheduler-1 \
  airflow users reset-password --username admin \
  --password "$(grep AIRFLOW_ADMIN_PASSWORD .env | cut -d= -f2)"
```

(The admin user is created only on first boot. `AIRFLOW_ADMIN_PASSWORD` is not re-read on subsequent restarts. Memory: `project_airflow_admin_password`.)

---

## Phase 5 — Install Python deps (host side)

For manual job runs + tests + linting:

```bash
poetry install
poetry run pre-commit install
```

This populates `.venv/` and installs the pre-commit hooks.

---

## Phase 6 — Validate DAGs

```bash
make dag-validate
```

Expected: 0 import errors, 8 DAGs listed (see `docs/architecture/as-built.md` for the canonical list). If anything imports red, the most common cause is a missing dep in the custom Airflow image — rebuild with `make build-airflow`.

---

## Phase 7 — First end-to-end pipeline run

Pick the **incremental** path (~30 matches, ~5 min) for a smoke test before committing to a full backfill.

### 7a. Trigger from Airflow UI

1. Open http://localhost:8080 → log in.
2. Find `ingest_two_day_match_data_bronze` → toggle "Unpause".
3. Click ▶ **Trigger DAG w/ config**.
4. Paste:
   ```json
   {"snapshot_date": "<today's-date>"}
   ```
5. Click Trigger.

The Grid view should turn green within ~5 minutes. The auto-trigger chain fires `ingest_two_day_match_data_silver` and `ingest_two_day_match_data_gold` automatically.

**Important:** Before the Gold DAG fires (or before manually triggering it), stop Metabase so the DuckDB write lock is available:

```bash
docker stop compose-metabase-1
```

Restart Metabase once the Gold DAG completes:

```bash
docker start compose-metabase-1
```

(See `docs/runbooks/dashboard.md` for the full lock coordination protocol.)

### 7b. Trigger from the host (alternative)

Same outcome, no UI:

```bash
docker exec compose-airflow-scheduler-1 \
  airflow dags trigger ingest_two_day_match_data_bronze \
  --conf '{"snapshot_date": "<today>"}'
```

---

## Phase 8 — Verify the data landed

### MinIO Console
Browse `cricket-source-files/match_data/json/snapshot_date=<today>/archive=recently_added_2_json/` — expect ~30 `.json` files.

### pgAdmin
Navigate **Servers → Cricket Platform → cricket_platform → control → Tables**. Confirm rows in:
- `control.archive_download_log`
- `control.match_file_audit`
- `control.bronze_match_ingestion_log`
- `control.dq_results`

### DuckDB UI
```bash
make duckdb-ui   # opens http://localhost:4213
```
Browse `bronze.match_data`, `silver.deliveries`, `gold.fact_delivery`. Or paste:
```sql
SELECT 'bronze.match_data'   AS table_name, COUNT(*) FROM bronze.match_data
UNION ALL SELECT 'silver.deliveries', COUNT(*) FROM silver.deliveries
UNION ALL SELECT 'gold.fact_delivery', COUNT(*) FROM gold.fact_delivery;
```

Close the UI with `make duckdb-stop` before triggering any more Gold DAGs.

### Metabase
http://localhost:3000 → "Cricket Universe" dashboard. Counters should reflect the ingested rows.

---

## Phase 9 — Install pre-commit + run tests

```bash
poetry run pytest               # ~60s, all unit tests
poetry run pre-commit run --all-files    # ruff + black + isort
```

If any of these are red, **don't proceed**. Fix and re-run.

---

## Phase 10 — Optional: install the dashboard

For working on the Observable Framework player portfolio:

```bash
cd dashboard
make dashboard-install         # npm install
make dashboard-dev             # starts dev server on http://localhost:3030
```

The dashboard reads DuckDB **read-only** so it coexists with Metabase and pipelines (except active Gold DAGs — see lock coordination above).

---

## Phase 11 — Optional: bring up revamp-v2 stacks (when implemented)

| Sprint | Command | Adds |
|---|---|---|
| 0 (Observability) | `make obs-up` | Marquez + Grafana + Prometheus + Tempo + OTEL Collector |
| 1 (FastAPI + Lightdash) | `make api-up && make lightdash-up` | FastAPI on :8000, Lightdash on :8082 |
| 2 (AI assistant) | `make ai-up` | Ollama + Qdrant + Chainlit |

These are only available once the corresponding sprint has shipped. Check `docs/planning.md` for sprint status.

---

## Common first-time problems

| Symptom | Cause | Fix |
|---|---|---|
| `make up` hangs on Iceberg REST | Container starts but healthcheck flaky | Wait 90s; check `docker logs compose-iceberg-rest-1` |
| Airflow login 401 | `AIRFLOW_ADMIN_PASSWORD` changed in `.env` after first boot | Reset via the CLI snippet in Phase 4 |
| `make bootstrap` fails: bucket exists | Re-running — bootstrap is idempotent but logs noisy | Safe to ignore |
| MinIO Console asks for new login | First-time setup | Use `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` from `.env` |
| Spark job fails with "Maven download" | Iceberg JARs not in custom image cache | Rebuild with `make build-airflow` |
| pytest fails with `ModuleNotFoundError: cip` | Poetry env not active | `poetry shell` or `poetry run pytest` |
| `make dashboard-dev` fails to bind :3030 | Port in use | `lsof -i :3030` then kill, or change port in `dashboard/package.json` |
| Metabase logs show "DuckDB file is locked" | Gold DAG or DuckDB UI holds the lock | `make duckdb-stop && docker restart compose-metabase-1` |

---

## What to read next

- `docs/runbooks/full-rebuild.md` — the wipe + reboot scenario
- `docs/runbooks/backfill-cricsheet.md` — the full historical backfill
- `docs/runbooks/dashboard.md` — Metabase + DuckDB UI lock coordination
- `docs/planning_dev.md` — daily command reference + conventions
- `CLAUDE.md` — working agreement (read before opening a PR)
