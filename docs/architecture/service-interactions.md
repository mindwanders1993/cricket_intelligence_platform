# Service Interactions — Cricket Intelligence Platform

> **Companion to** `docs/architecture/hld-hla.md` (target architecture) and `docs/architecture/data-flow.md` (per-record flow).
> Documents the **service-to-service** boundaries: ports, protocols, auth, locks, container names, and the contract each integration depends on. Updated to reflect revamp v2 target state.

---

## 1. Service catalogue (target state)

Containers are named `compose-<service>-<n>` because the Compose project folder is `infra/compose`. The matrix below lists every service that will exist after Sprint 0–2 lands.

| Container | Image | Host port | Internal DNS | Purpose | Status |
|---|---|---|---|---|---|
| `compose-minio-1` | `minio/minio` | 9000 (API), 9001 (console) | `minio` | S3-compatible object storage | ✅ live |
| `compose-postgres-1` | `postgres:15` | 5432 | `postgres` | Iceberg metastore + `control.*` + Airflow metadata + MLflow (SQLite swap) | ✅ live |
| `compose-iceberg-rest-1` | `tabulario/iceberg-rest` | 8181 | `iceberg-rest` | Iceberg REST catalog (no UI) | ✅ live |
| `compose-airflow-init-1` | custom | — | — | One-shot DB init + admin user creation | ✅ live |
| `compose-airflow-webserver-1` | custom (PySpark + JDK17 baked in) | 8080 | `airflow-webserver` | Airflow UI | ✅ live |
| `compose-airflow-scheduler-1` | custom | — | `airflow-scheduler` | DAG scheduling + execution | ✅ live |
| `compose-mlflow-1` | `ghcr.io/mlflow/mlflow` | 5001 | `mlflow` | ML experiment tracking (SQLite backend) | ✅ live |
| `compose-pgadmin-1` | `dpage/pgadmin4` | 5050 | `pgadmin` | PostgreSQL UI (auto-connects via libpq passfile) | ✅ live |
| `compose-metabase-1` | custom (Temurin 21 + DuckDB driver) | 3000 | `metabase` | BI dashboards (DuckDB read-only) | ✅ live |
| `compose-otel-collector-1` | `otel/opentelemetry-collector` | 4317 (OTLP gRPC), 4318 (HTTP) | `otel-collector` | Fan-out telemetry to Prometheus/Tempo | ⬜ Sprint 0 |
| `compose-prometheus-1` | `prom/prometheus` | 9090 | `prometheus` | Metrics store | ⬜ Sprint 0 |
| `compose-grafana-1` | `grafana/grafana` | 3001 | `grafana` | Metrics + traces UI | ⬜ Sprint 0 |
| `compose-tempo-1` | `grafana/tempo` | 3200 | `tempo` | Distributed trace store | ⬜ Sprint 0 |
| `compose-marquez-1` | `marquezproject/marquez` | 5000 (UI), 5002 (API) | `marquez` | OpenLineage events store + UI | ⬜ Sprint 0 |
| `compose-lightdash-1` | `lightdash/lightdash` | 8080 *(remap to 8082)* | `lightdash` | Semantic-layer BI | ⬜ Sprint 1 |
| `compose-fastapi-1` *(or local process)* | custom | 8000 | `fastapi` | API gateway (`/health`, `/query`, `/chat`, …) | ⬜ Sprint 1 |
| `compose-ollama-1` | `ollama/ollama` | 11434 | `ollama` | Local LLM server | ⬜ Sprint 2 |
| `compose-qdrant-1` | `qdrant/qdrant` | 6333 (HTTP), 6334 (gRPC) | `qdrant` | Vector store for dbt-docs RAG | ⬜ Sprint 2 |
| `compose-chainlit-1` *(or local process)* | custom | 8100 | `chainlit` | AI chat UI | ⬜ Sprint 2 |

**Local-only consumers (no container):**

| App | Port | Notes |
|---|---|---|
| DuckDB UI (`make duckdb-ui`) | 4213 | Browser UI for the DuckDB file; single writer (holds `_ui` lock) |
| Observable Framework dev (`make dashboard-dev`) | 3030 | Node.js dev server reading DuckDB read-only |
| Observable Framework build (`make dashboard-build`) | — | Outputs static site to `dashboard/dist/` |

---

## 2. Network topology

Everything runs on the default Compose network `compose_default` so internal DNS works. Inside containers, services address each other by service name (`postgres`, `minio`, `iceberg-rest`, etc.). From the host, services are reached at `localhost:<port>`.

Critical implication: **the `.env` file uses host-style endpoints by default** (`localhost`, `iceberg-rest`, etc. resolved via `${VAR:-default}` overrides in Compose). Manual job runs from the host must export host-style overrides:

```bash
ICEBERG_REST_URI=http://localhost:8181 \
MINIO_S3_ENDPOINT=http://localhost:9000 \
POSTGRES_HOST=localhost \
poetry run python -m cip.ingestion.jobs.full_load_match_data --task all
```

See `docs/runbooks/full-rebuild.md` and `docs/runbooks/local-bootstrap.md` for the precise overrides.

---

## 3. Service-to-service edges

### 3.1 Airflow scheduler / worker edges

```
airflow-scheduler   ─→ postgres:5432          (Airflow metadata DB + control schema RW)
                    ─→ iceberg-rest:8181      (catalog ops via PyIceberg or Spark)
                    ─→ minio:9000             (S3 API for landing + lakehouse)
                    ─→ otel-collector:4317    (spans + metrics)
                    ─→ marquez:5002           (OpenLineage events via openlineage-airflow listener)
                    ─→ mlflow:5000            (ML tracking)
                    ─→ duckdb (file mount)    (Gold task writes via dbt; lock-bound)
                    ─→ bigquery               (Sprint 3, sync DAGs only)
```

### 3.2 Iceberg REST catalog edges

```
iceberg-rest:8181  ─→ postgres:5432       (metastore — namespace, table, snapshot metadata)
                   ─→ minio:9000          (S3 API for table metadata + data file references)
```

Iceberg REST is the **only** service that talks to both PG and MinIO for catalog/data correlation. Polars/Spark/PyIceberg writers talk to Iceberg REST for catalog ops and directly to MinIO for data Parquet writes.

### 3.3 dbt edges

```
dbt (CLI / Airflow task)
  target=dev    ─→ duckdb file               (Gold materialisation in DuckDB)
  target=bq_dev ─→ bigquery                  (Gold materialisation in BigQuery, Sprint 3)
                ─→ dbt-ol → marquez:5002     (lineage events per model)
                ─→ otel-collector            (optional, dbt-otel package)
```

### 3.4 BI consumer edges (DuckDB readers)

```
metabase        ─→ duckdb file (READ-ONLY)
lightdash       ─→ duckdb file (READ-ONLY) + reads dbt manifest from filesystem
dashboard build ─→ duckdb file (READ-ONLY)  (via Python loaders)
fastapi         ─→ duckdb file (READ-ONLY)  (connection pool)
duckdb-ui       ─→ duckdb file (READ-WRITE) (holds the _ui internal catalog lock)
gold dag task   ─→ duckdb file (READ-WRITE) (exclusive — needs all readers stopped)
```

**Lock contract:** DuckDB is *single-writer + multiple-reader*. A read connection co-exists with the UI's `_ui` write lock, but the Gold DAG's `refresh_duckdb_views` task needs exclusive write — all readers must be stopped first.

See §5 below for the exact stop-restart protocol.

### 3.5 FastAPI gateway edges (Sprint 1)

```
fastapi:8000
   ├─→ duckdb file (READ-ONLY pool)
   ├─→ dbt artifacts dir (manifest.json + catalog.json, for /explain and MetricFlow)
   ├─→ qdrant:6333 (search_metrics tool, Sprint 2)
   ├─→ ollama:11434 (LangGraph agent backend, Sprint 2)
   ├─→ otel-collector:4317 (spans + metrics)
   └─→ postgres:5432 (read-only on control.* for /catalog/freshness etc.)

Consumers:
   chainlit → fastapi /chat (SSE)
   dashboard → fastapi /query (optional; primary path is DuckDB direct)
   curl / test scripts → all endpoints
```

### 3.6 Agent edges (Sprint 2)

```
LangGraph agent (inside fastapi process)
   ├─ tool: query_metric    → fastapi /query (in-process call)
   ├─ tool: search_metrics  → qdrant:6333
   ├─ tool: lookup_player   → duckdb file
   ├─ tool: explain_table   → dbt manifest.json
   ├─ tool: generate_chart_spec → in-process
   └─ LLM provider:
        if AISettings.llm_provider == "ollama" → ollama:11434
        if AISettings.llm_provider == "bedrock" → AWS Bedrock (boto3)
```

### 3.7 Observability fan-out (Sprint 0)

```
                                     ┌─→ prometheus:9090  (metrics)
otel-collector:4317 ─→ pipelines  ───┼─→ tempo:3200       (traces)
                                     └─→ (future) loki   (logs)

grafana:3001  ─→ prometheus (datasource)
              ─→ tempo (datasource)
              ─→ marquez (datasource via JSON API plugin)

marquez:5002 (lineage events ingest) ◀── writers / dbt-ol / openlineage-airflow
marquez:5000 (UI)
```

### 3.8 Cloud edges (Sprint 3, plan-only)

```
Local (today)                       AWS target (Terraform plan)
─────────────                       ─────────────────────────────
minio:9000                ────────  s3.amazonaws.com (S3 API)
iceberg-rest:8181         ────────  AWS Glue (catalog API)
postgres:5432             ────────  RDS (managed Postgres)
airflow-scheduler         ────────  MWAA
spark in Airflow          ────────  EMR Serverless
duckdb (file)             ────────  Athena workgroup (same Iceberg metadata)
ollama:11434              ────────  AWS Bedrock (Claude / Llama / Mistral)
qdrant:6333               ────────  AWS OpenSearch (k-NN plugin)
marquez                   ────────  DataHub OSS or Atlan (consumes same OL events)
prometheus + tempo        ────────  AWS Managed Prometheus / Datadog
metabase + lightdash + fastapi ──   ECS Fargate or EKS deployments
```

**Endpoint swap is the only delta.** No business logic touched.

---

## 4. Authentication / authorization

| Boundary | Mechanism | Today | Cloud target |
|---|---|---|---|
| MinIO access | Access key + secret in `PlatformSettings.storage` | env-injected on every client | IAM role via IRSA on EKS / instance profile on EC2 |
| PostgreSQL | `POSTGRES_USER` + `POSTGRES_PASSWORD` from `.env` | env-injected; pgAdmin uses libpq passfile | RDS IAM auth |
| Iceberg REST | No auth in local dev | open | Glue catalog: IAM |
| Airflow UI | `AIRFLOW_ADMIN_PASSWORD` from `.env` (first-boot only) | `airflow users reset-password` to rotate | SSO via OIDC |
| Metabase | `admin@cricket-platform.local` / `Cricket2026!` | hardcoded for dev | SSO via OIDC |
| Lightdash | first-boot wizard | dev only | SSO via OIDC |
| FastAPI gateway | none in v1 (localhost only) | open | API key + OPA / OAuth on cloud |
| Ollama | none (localhost) | — | Bedrock IAM in cloud |
| Marquez / Grafana / Prometheus | none in v1 (localhost only) | — | Behind ALB + IAM auth on cloud |

**v1 scope:** all services run on the developer's laptop and bind only to localhost; auth is intentionally simple. Production AWS deployment adds the boundary controls noted above.

---

## 5. Lock semantics (the critical operational gotcha)

DuckDB's single-writer rule is the source of most operational pain. The protocol below is the rule of the platform.

### 5.1 DuckDB lock-holders

| Process | Read or Write | Holds lock for |
|---|---|---|
| Gold DAG task `refresh_duckdb_views` | WRITE (exclusive) | Duration of materialise() + dbt run + tests |
| DuckDB UI (`make duckdb-ui`) | WRITE | Always (for `_ui` internal catalog) |
| Metabase | READ | While the container is running |
| Lightdash | READ | While the container is running |
| Observable Framework dev server | READ | While `make dashboard-dev` is running |
| FastAPI gateway | READ (pool) | While the API is running |
| `make duckdb-ui` running concurrently with Gold DAG | **CONFLICT** | — |
| Metabase running concurrently with Gold DAG | **CONFLICT** | — |

### 5.2 Operational rule

**Before triggering any `*_gold` DAG, stop all readers:**

```bash
make duckdb-stop                     # closes the DuckDB UI lock
docker stop compose-metabase-1        # stops Metabase reader
docker stop compose-lightdash-1       # stops Lightdash reader (when added)
# stop dashboard dev server with Ctrl-C
# FastAPI uses a connection pool; uvicorn restart releases locks
```

The convenience target `make refresh-gold` does this automatically for the common case (Metabase + DuckDB UI) and restarts Metabase after the DAG finishes.

### 5.3 Why DuckDB UI must be stopped before Gold DAG

The UI keeps a write lock on the file for its own `_ui` internal catalog (metadata, query history). Even though it would only be reading user tables, the lock is held at the file level. `make duckdb-stop` releases it.

---

## 6. Endpoint-port reference (host machine)

| URL | Service | Login |
|---|---|---|
| http://localhost:8080 | Airflow | admin / `$AIRFLOW_ADMIN_PASSWORD` |
| http://localhost:9001 | MinIO console | `$MINIO_ROOT_USER` / `$MINIO_ROOT_PASSWORD` |
| http://localhost:5050 | pgAdmin | admin@cricket-platform.local / admin123 |
| http://localhost:5001 | MLflow | — |
| http://localhost:3000 | Metabase | admin@cricket-platform.local / Cricket2026! |
| http://localhost:4213 | DuckDB UI | — |
| http://localhost:8181 | Iceberg REST (API only) | — |
| http://localhost:3030 | Observable Framework dev | — |
| **http://localhost:5000** | **Marquez (Sprint 0)** | — |
| **http://localhost:3001** | **Grafana (Sprint 0)** | admin/admin |
| **http://localhost:9090** | **Prometheus (Sprint 0)** | — |
| **http://localhost:8082** | **Lightdash (Sprint 1)** | first-boot wizard |
| **http://localhost:8000** | **FastAPI (Sprint 1)** | — |
| **http://localhost:8100** | **Chainlit (Sprint 2)** | — |

(Conflicts to watch: Metabase owns :3000 → Grafana remapped to 3001; Lightdash defaults to 8080 → remap to 8082 to avoid Airflow.)

---

## 7. Inter-service contracts

### 7.1 Iceberg REST ↔ writers

- Writers obtain catalogs via `from cip.transform.shared.readers import get_catalog`
- Catalog name **must** be `iceberg` (matches the REST instance label, not `cricket`)
- All FQNs are 2-segment: `<layer>.<entity>` (catalog name **not** in the FQN string)
- Writers handle the PyIceberg 0.11.x schema-conversion quirk (`_pyarrow_to_schema_without_ids` + `assign_fresh_schema_ids`)

### 7.2 Airflow ↔ Postgres `control.*` schema

- Every DAG task that touches data reads/writes a row in the appropriate audit table
- Task callables receive `pipeline_run_id` and `snapshot_date` via Jinja templates (`{{ run_id }}`, `{{ ds }}`)
- XCom payloads are plain JSON-serialisable dicts only — no DataFrames, no datetime objects

### 7.3 dbt ↔ DuckDB

- dbt expects the DuckDB file at `storage/duckdb/cricket.duckdb` (configured in `profiles.yml`)
- `DuckDBRefresh.materialise()` must run **before** `dbt run` so `bronze.*` and `silver.*` tables exist
- Custom test `fact_player_of_match_unique_grain` enforces the bridge table grain
- dbt sources are filtered to `MAX(_snapshot_date)` via the `DuckDBRefresh` materialisation logic

### 7.4 FastAPI ↔ MetricFlow (Sprint 1+)

- FastAPI loads `models/dbt/target/manifest.json` + `catalog.json` at startup (refreshed nightly by `dag_refresh_ai_metadata`)
- `POST /query` with `{metric, dimensions, ...}` calls `MetricFlowClient.query(...)` which generates SQL and executes against DuckDB
- `POST /query` with `{sql}` runs the SQL through `sql_guardrails` (AST walk) before executing

### 7.5 Agent ↔ FastAPI (Sprint 2)

- LangGraph agent calls FastAPI endpoints (`/query`, `/explain`, `/catalog/*`) **in-process** when colocated; via HTTP when separate
- No tool may construct raw SQL — all data access goes through `query_metric` (MetricFlow) or `lookup_player` (parameterized SQL with no string interpolation)
- `sql_guardrails` is the last-mile defense if a tool tries to bypass

### 7.6 OpenLineage event shape (cross-emitter)

All emitters (writers, Airflow, dbt) emit events conforming to OpenLineage's `RunEvent` schema:

```json
{
  "eventType": "START" | "COMPLETE" | "FAIL",
  "eventTime": "ISO-8601",
  "run": {"runId": "<pipeline_run_id>"},
  "job": {"namespace": "cricket", "name": "<layer>.<table>.write"},
  "inputs": [{"namespace": "s3://...", "name": "..."}],
  "outputs": [{"namespace": "iceberg", "name": "bronze.match_data",
               "facets": {"schema": {...}, "_snapshot_date": "2026-05-10"}}]
}
```

Marquez stitches events sharing `run.runId` into a single graph.

---

## 8. Health checks & startup ordering

Compose declares `depends_on` with `condition: service_healthy` for the critical path:

```
minio  →  iceberg-rest  →  airflow-init  →  airflow-scheduler + airflow-webserver
postgres →  iceberg-rest, airflow-init, mlflow, metabase, pgadmin
```

Sprint 0+:
```
postgres → otel-collector  (writes to no DB, but starts after)
prometheus → grafana
tempo → grafana
marquez → marquez-api → marquez-web
```

Healthcheck specifics:
- Iceberg REST container has **no curl/wget** — healthcheck uses bash `/dev/tcp` (saved memory: `project_iceberg_rest_healthcheck`)
- PostgreSQL: `pg_isready -U $POSTGRES_USER`
- MinIO: `mc ready local`

If anything starts misbehaving, the first move is `make dag-validate` to confirm Airflow imports cleanly.

---

## 9. Custom Airflow image dependencies

The custom image (`infra/docker/airflow/Dockerfile`) bakes:

- `pyspark` (matches `SparkSettings.spark_version`)
- JDK 17 (Temurin)
- `pydantic_settings` (Airflow Docker images ship Pydantic v1; we need v2)
- Iceberg JAR cache (warmed via a dummy SparkSession at build time so the first DAG run doesn't hit Maven Central)
- `openlineage-airflow` *(Sprint 0)*
- `opentelemetry-api`, `opentelemetry-instrumentation-airflow` *(Sprint 0)*
- `soda-core-duckdb` *(Sprint 0)*
- `dbt-bigquery` *(Sprint 3)*
- `google-cloud-bigquery` *(Sprint 3)*

Rebuild with `make build-airflow` after any change to `pyproject.toml` or the Dockerfile.

---

## 10. References

- `docs/architecture/hld-hla.md` — target-state architecture
- `docs/architecture/data-flow.md` — per-record flow
- `docs/architecture/as-built.md` — current snapshot
- `docs/runbooks/local-bootstrap.md` — from-zero bring-up
- `docs/runbooks/full-rebuild.md` — wipe + re-bootstrap
- `docs/runbooks/dashboard.md` — Metabase + DuckDB UI coordination
- ADR 0007 — FastAPI gateway design *(Sprint 1)*
- ADR 0005 — Observability stack *(Sprint 0)*
