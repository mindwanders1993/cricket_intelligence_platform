# Cricket Intelligence Platform — High-Level Design / High-Level Architecture (Target State)

> **Document type:** HLD / HLA — target architecture after revamp v2 lands.
> **Companion docs:** `docs/architecture/as-built.md` (current state), `docs/architecture/data-flow.md` (data movement), `docs/architecture/service-interactions.md` (service-to-service), `docs/architecture/repo-structure.md` (codebase layout), `docs/planning.md` (execution plan).
> **Audience:** engineering review, architecture discussions, portfolio walkthrough, interview prep.

---

## 1. Executive overview

The Cricket Intelligence Platform is an open-source, cloud-agnostic **lakehouse + analytics engineering + AI-grounded serving stack** that ingests, processes, governs, and serves cricket data from Cricsheet. The target state (revamp v2) extends the existing Source → Bronze → Silver → Gold lakehouse with:

- **Observability spine** — OpenLineage + OpenTelemetry instrumented across writers, DAGs, and dbt.
- **Semantic layer** — dbt MetricFlow models + metrics + exposures as the single source of truth for both BI tools and the AI assistant.
- **Two BI surfaces** — Metabase (SQL-card, ad-hoc) and Lightdash (semantic-driven, ops + FinOps) sharing the same metric definitions.
- **Observable Framework dashboard** — custom D3-driven player portfolio dashboard.
- **FastAPI gateway** — OpenAPI-spec'd surface for catalog introspection, semantic-layer queries, SQL-guardrailed ad-hoc queries, and streaming chat.
- **Agentic AI assistant** — LangGraph agent (Ollama local / Bedrock cloud) with semantic-layer-grounded tools and a Chainlit chat UI.
- **FinOps cost mart** — `mart_pipeline_cost_daily` modeling per-task executor-seconds and rows/bytes as $ cost.
- **BigQuery secondary target** — same dbt models build on both DuckDB and BigQuery free tier, proving OLAP portability.
- **Scale story** — 100M+ row synthetic delivery table with documented partition pruning + clustering wins.
- **Terraform IaC** — module that provisions equivalent AWS workloads (S3 + Glue + EMR Serverless + MWAA + Athena).

The platform is intentionally designed to look like a modern product-company data platform. Every component speaks an open protocol with a managed enterprise cousin (see `docs/adr/0004-open-standards-first.md`).

---

## 2. Design principles (target state)

| # | Principle | Why it matters | Where it shows up |
|---|---|---|---|
| 1 | **Decoupled storage and compute** | Engines can change without re-platforming data | Iceberg + S3 API allows Polars, Spark, DuckDB, BigQuery, Athena to all read the same tables |
| 2 | **Open table format first** | Multi-engine, snapshot isolation, schema/partition evolution | Apache Iceberg with REST catalog |
| 3 | **Right tool per workload** | Ingestion ≠ explosion ≠ serving | Polars (small/medium files), PySpark (heavy joins), DuckDB (local OLAP), BigQuery (cloud OLAP) |
| 4 | **Contract-driven movement** | Bad data blocked at each boundary | DQ gates: landing → bronze → silver → gold; Soda Core layered on top |
| 5 | **Analytics as a product** | Gold is the interface, not an output | dbt semantic layer is the contract; exposures declare consumers |
| 6 | **AI is grounded, not generative** | Hallucination control via curated context | Agent reads only MetricFlow + dbt manifest; SQL guardrails ban writes |
| 7 | **Open standards first** | No vendor lock-in; cloud migration is config | OpenLineage, OpenTelemetry, ANSI SQL, dbt manifest, Kafka API, OpenAPI |
| 8 | **Develop local, deploy cloud** | Single mental model from laptop to AWS | Endpoint env vars are the only delta |
| 9 | **Cost is a first-class signal** | FinOps thinking baked into the platform itself | `control.pipeline_cost_event` + `mart_pipeline_cost_daily` |
| 10 | **Modular monolith over microservices** | Solo developer, shared types | `src/cip/*` packages, no service boundaries inside the platform |

---

## 3. Logical layers

The target platform has **eleven logical layers** (existing 9 + observability + consumer-apps split):

1. **Source layer** — Cricsheet archives (`all_json.zip`, `recently_added_2_json.zip`) + Register files (`people.csv`, `names.csv`)
2. **Control & orchestration** — Apache Airflow + PostgreSQL `control` schema (audit, DQ, schema versions, **cost events**, **AI metadata refresh state**)
3. **Object storage** — MinIO locally / AWS S3 in cloud
4. **Open table layer** — Apache Iceberg via REST catalog (local) / AWS Glue (cloud)
5. **Compute & transformation** — Polars (ingestion), PySpark (heavy Silver), dbt (Gold)
6. **Analytics engineering** — dbt-core + MetricFlow semantic layer + Soda Core DQ
7. **Serving — analytical engines** — DuckDB (local) + BigQuery (cloud); identical dbt models
8. **Serving — API gateway** — FastAPI (OpenAPI, OTEL instrumented, SQL guardrails)
9. **Serving — consumer apps** — Metabase, Lightdash, Observable Framework, Chainlit (AI chat), MLflow UI
10. **Intelligence layer** — LangGraph agent + Ollama / Bedrock + Qdrant (vector store for dbt docs)
11. **Observability** — OpenLineage → Marquez; OpenTelemetry → Prometheus + Grafana + Tempo; FinOps mart → Lightdash + Metabase

---

## 4. Top-level architecture diagram

```
                                CONSUMERS
┌──────────────────┐  ┌────────────────────┐  ┌──────────────────────────────┐
│ Observable       │  │ AI Studio          │  │ Lightdash (semantic-driven)  │
│ player portfolio │  │ Chainlit chat UI   │  │ Metabase  (SQL-card)         │
└────────┬─────────┘  └─────────┬──────────┘  └──────────────┬───────────────┘
         │ DuckDB direct        │ HTTP                       │ DuckDB direct
         │ (read-only)          ▼                            │
         │           ┌──────────────────────────┐            │
         │           │ FastAPI gateway          │            │
         │           │  /health /metrics /query │            │
         │           │  /explain /catalog /chat │            │
         │           │  + SQL guardrails + OTEL │            │
         │           └────┬──────────────────┬──┘            │
         │                │                  │               │
         │     LangGraph agent          MetricFlow client    │
         │     (Ollama/Bedrock,         (semantic→SQL)       │
         │     6 tools, Qdrant RAG)                          │
         │                │                  │               │
         └────────────────┴─────────┬────────┴───────────────┘
                                    ▼
            ┌─────────────────────────────────────────────────┐
            │ Serving layer — DuckDB + BigQuery (twin OLAP)   │
            │ Gold star schema + MetricFlow + exposures       │
            └────────────────────────▲────────────────────────┘
                                     │ dbt build (DuckDB)
                                     │ dbt build (BigQuery, via sync)
                                     │
            ┌────────────────────────┴────────────────────────┐
            │ Gold (Iceberg + dbt)                            │
            │  SCD2 dims, incremental facts, marts,           │
            │  FinOps cost mart, MetricFlow semantic layer    │
            └────────────────────────▲────────────────────────┘
                                     │
            ┌────────────────────────┴────────────────────────┐
            │ Silver (Iceberg / MinIO)                        │
            │  Polars + Spark writers, instrumented with      │
            │  OpenLineage events + OTEL spans +              │
            │  cost_emission → control.pipeline_cost_event    │
            └────────────────────────▲────────────────────────┘
                                     │
            ┌────────────────────────┴────────────────────────┐
            │ Bronze (Iceberg / MinIO)                        │
            │  (match_id, revision) PK; audit-driven dedup    │
            │  via control.match_file_audit                   │
            └────────────────────────▲────────────────────────┘
                                     │
            ┌────────────────────────┴────────────────────────┐
            │ Landing (MinIO, S3-compatible)                  │
            │  raw downloads — ZIPs, CSVs, extracted JSONs    │
            └────────────────────────▲────────────────────────┘
                                     │
                              ┌──────┴───────┐
                              │ Cricsheet.org│
                              └──────────────┘

                       ORCHESTRATION & OBSERVABILITY (cross-cutting)
┌────────────────────────────────────────────────────────────────────────────┐
│ Airflow DAGs (8 existing + 3 new: sync_bq, dq_soda, refresh_ai_metadata)   │
│ + OpenLineage listener → Marquez                                           │
│ + OTEL collector → Prometheus + Grafana + Tempo                            │
│ + Soda Core nightly + per-DAG checks → control.dq_results                  │
│ + PostgreSQL control schema (audit, DQ, schema versions, cost events)      │
└────────────────────────────────────────────────────────────────────────────┘
```

See `docs/architecture/data-flow.md` for per-event flow diagrams and `docs/architecture/service-interactions.md` for endpoint-level interactions.

---

## 5. Component inventory (target state)

### Stateful infrastructure (always-on)

| Service | OSS local | Enterprise cousin | Notes |
|---|---|---|---|
| Object storage | MinIO | AWS S3 | S3 API; named volume locally |
| Metastore + control DB | PostgreSQL 15 | AWS RDS | Iceberg metastore + `control.*` + Airflow metadata + MLflow (SQLite swap) |
| Iceberg catalog | Iceberg REST (`tabulario/iceberg-rest`) | AWS Glue / Nessie | Same protocol on both |
| Lineage store | Marquez | DataHub / Atlan | Consumes OpenLineage events |
| Metrics store | Prometheus | Datadog / Managed Prometheus | Consumes OTEL metrics |
| Trace store | Tempo | Datadog / Honeycomb | Consumes OTEL traces |
| BI (SQL-card) | Metabase | Looker / Power BI | Reads DuckDB read-only |
| BI (semantic) | Lightdash | Looker | Reads dbt MetricFlow definitions |
| Vector store | Qdrant | pgvector / Pinecone / OpenSearch | dbt-docs RAG |

### Ephemeral workloads (pod-per-task or process)

| Workload | Engine | Where it runs locally | Where it runs in cloud |
|---|---|---|---|
| Bronze writers | Polars + PyIceberg | Airflow worker process | EMR Serverless / EKS pod |
| Silver writers (match) | PySpark + Iceberg | Spark in Airflow container | EMR Serverless |
| Silver writers (register) | Polars + PyIceberg | Airflow worker | Same |
| dbt build | dbt-core (DuckDB & BQ) | Airflow worker / make | MWAA / dbt Cloud |
| BigQuery sync | Python + google-cloud-bigquery | Airflow worker | Same |
| Soda Core scan | soda-core (DuckDB) | Airflow worker / make | Same |
| FastAPI gateway | uvicorn | local process | EKS / ECS Fargate |
| LangGraph agent | Python + Ollama | local process | EKS (calls Bedrock) |
| Chainlit chat UI | Chainlit | local process | EKS / ECS Fargate |
| Observable build | Node.js | local / CI | Static hosting (S3 + CloudFront) |

### Data assets (logical)

- **Bronze (Iceberg):** `bronze.people`, `bronze.people_identifiers`, `bronze.name_variations`, `bronze.match_data`
- **Silver (Iceberg):** `silver.persons`, `silver.person_identifiers`, `silver.name_variations`, `silver.matches`, `silver.innings`, `silver.deliveries`, `silver.wickets`, `silver.match_players`, `silver.match_officials`, `silver.teams`, `silver.venues`, `silver.competitions`
- **Gold (DuckDB + BigQuery via dbt):**
  - Dims: `dim_match`, `dim_player`, `dim_player_scd2` ← *new*, `dim_team`, `dim_venue`, `dim_competition`, `dim_official`, `dim_date`
  - Facts: `fact_delivery` *(incremental)*, `fact_innings`, `fact_match_result`, `fact_player_match` *(incremental)*, `fact_player_of_match`
  - Marts: `mart_player_batting_career/_season`, `mart_player_bowling_career/_season`, `mart_team_performance`, `mart_venue_dna`, `mart_phase_scoring`, `mart_toss_outcome`, `mart_matchup_analysis`, **`mart_pipeline_cost_daily`** *(new)*, **`mart_top_expensive_tasks`** *(new)*, **`mart_data_freshness`** *(new)*
- **Synthetic scale (Sprint 4):** `silver.deliveries_synth` — 100M+ rows for perf testing
- **Control schema (Postgres):** `register_ingestion_log`, `register_schema_versions`, `register_change_log`, `archive_download_log`, `bronze_match_ingestion_log`, `match_file_audit`, `dq_results`, **`pipeline_cost_event`** *(new)*, **`ai_metadata_refresh_log`** *(new)* + views
- **MLflow (SQLite dev):** experiment + run + artifact registry

---

## 6. Data plane

### 6.1 Ingestion patterns

| Pattern | Bronze | Used by |
|---|---|---|
| **All-string Polars** for CSVs | `infer_schema_length=0` + meta columns | Register pipeline |
| **Per-file Polars + revision tracking** for JSONs | `(match_id, revision)` PK; new revisions append; `control.match_file_audit` drives dedup | Match pipeline |

### 6.2 Transformation patterns

| Pattern | Engine | Used by |
|---|---|---|
| **`overwrite_partition`** on `_snapshot_date` | Polars + PyIceberg | Register Silver |
| **`dynamic_overwrite`** with per-partition replace | PySpark + Iceberg | Match Silver |
| **dbt incremental** (`unique_key`, `on_schema_change`) | dbt-core | Sprint 0+ — `fact_delivery`, `fact_player_match` |
| **dbt snapshot → SCD2 view** | dbt snapshot | Sprint 0+ — `dim_player_scd2` |
| **MetricFlow semantic resolution** | dbt-metricflow | Sprint 0+ — `batting_average`, `strike_rate`, etc. |

### 6.3 Idempotency

Every layer uses an audit row in the control schema to short-circuit re-runs unless `force=True`:
- Register: `control.register_ingestion_log` keyed on `(source_file, snapshot_date)`
- Match Bronze: `control.match_file_audit` keyed on `(match_id, file_sha256)`
- Match Silver: reads `MAX(revision) per match_id` so corrections naturally supersede earlier loads
- Gold incremental dbt: `is_incremental()` filters to `control.match_file_audit WHERE gold_loaded_at IS NULL`

### 6.4 Lineage emission

Each writer call emits an OpenLineage `RunEvent` (START + COMPLETE) with:
- `job.namespace = "cricket"`, `job.name = "<layer>.<table>.write"`
- `inputs[]` from source S3 paths or upstream Iceberg tables
- `outputs[]` = target Iceberg FQN
- Facets: `schema`, `dataQuality.assertions`, `pipeline_run_id`, `_snapshot_date`, `_row_hash` aggregate

dbt and Airflow listeners emit their own events; Marquez stitches them.

### 6.5 Cost emission

At the end of each instrumented call, `cost_emission.record(...)` writes a row to `control.pipeline_cost_event` capturing: `pipeline_run_id, dag_id, task_id, target_table, rows_written, bytes_written, executor_seconds, wall_time_seconds`. The `mart_pipeline_cost_daily` aggregates these against configurable `$/executor-second` and `$/TB-written` constants in `dbt_project.yml` `vars:`.

---

## 7. Serving plane

### 7.1 DuckDB local serving

- File at `storage/duckdb/cricket.duckdb` (bind-mounted into Airflow + Metabase + dashboard containers)
- Bronze + Silver schemas materialised as **native tables** (not views) by `DuckDBRefresh.materialise()` — filtered to `MAX(_snapshot_date)`
- Gold built by dbt run/test
- Single writer; multiple readers (Metabase + dashboard + DuckDB UI + FastAPI). Lock coordination: stop other readers before triggering Gold DAGs.

### 7.2 BigQuery secondary target

- `scripts/sync_silver_to_bq.py` syncs Silver Iceberg → BQ `cricket_silver.*` daily
- dbt `bq_dev` target builds the same Gold models on BQ free tier
- Validation harness re-runs key queries on both targets; row counts must match

### 7.3 FastAPI gateway

Single OpenAPI-spec'd surface:

| Endpoint | Purpose |
|---|---|
| `GET /health` | Liveness |
| `GET /metrics` | Prometheus scrape (OTEL) |
| `POST /query` | Either `{metric, dimensions, filters, time_range}` (MetricFlow) or `{sql}` (guardrailed) |
| `GET /explain/{model}` | dbt lineage + column docs for a model |
| `GET /catalog/metrics` | MetricFlow metric catalog |
| `GET /catalog/tables` | dbt manifest table catalog |
| `POST /chat` | SSE-streamed agent responses (Sprint 2) |

All endpoints are OTEL instrumented; SQL guardrails inspect AST before execution.

### 7.4 Consumer apps

| App | Reads from | Purpose |
|---|---|---|
| Metabase | DuckDB (read-only) | SQL-card-driven ad-hoc analyst dashboards |
| Lightdash | dbt MetricFlow + DuckDB | Semantic-layer-driven ops + FinOps dashboards |
| Observable Framework | DuckDB (read-only, build-time CSV cache) | Player portfolio (Kohli showcase) + embedded AI chat |
| Chainlit | FastAPI `/chat` (SSE) | AI assistant playground |
| MLflow UI | MLflow SQLite | Experiment / run / model registry browse |

---

## 8. Intelligence plane

### 8.1 Agent architecture

LangGraph state machine with the following tools:

| Tool | Backed by | Purpose |
|---|---|---|
| `search_metrics` | Qdrant + nomic-embed-text | Semantic search over MetricFlow metric catalog |
| `get_metric_definition` | MetricFlow Python SDK | Returns SQL + dimensions for a metric |
| `query_metric` | FastAPI `/query` | Executes a metric query against DuckDB |
| `lookup_player` | DuckDB `dim_player` + `gold.player_display_names` | Fuzzy player name resolution |
| `explain_table` | dbt manifest + catalog | Returns column descriptions + lineage |
| `generate_chart_spec` | Python | Returns Vega-Lite spec for a result set |

### 8.2 Grounding

The agent has no raw SQL ability — it can only call tools. The `query_metric` tool resolves through MetricFlow, which validates dimensions and filters against the semantic model. The `sql` mode of `/query` (used only for explicit ad-hoc queries from authenticated users, not chat) passes through AST-walked guardrails.

### 8.3 LLM selection

`AISettings.llm_provider` switches between Ollama (default, local) and Bedrock (cloud). Same prompts; same tools. Eval harness (`apps/ai-studio/evaluation/`) runs golden set against both for regression detection.

---

## 9. Observability plane

### 9.1 Lineage

Writers + Airflow + dbt all emit OpenLineage events. Marquez stitches them into a single DAG view. Future: swap Marquez → DataHub / Atlan with no code change.

### 9.2 Metrics

OTEL Collector receives spans + metrics from writers, FastAPI, agent. Routes:
- Metrics → Prometheus
- Traces → Tempo
- (Future) Logs → Loki

Grafana dashboards: `pipeline_health.json` (per-DAG SLOs), `finops.json` (cost panels mirroring Lightdash).

### 9.3 Data quality

- **dbt-tests** (40+) on Gold models — uniqueness, not_null, accepted_values, relationships, custom grain test (`fact_player_of_match_unique_grain`).
- **Soda Core** declarative checks on Silver and Gold critical tables — schema drift, partition completeness, row count thresholds, PK nullness.
- Both write to `control.dq_results` for cross-tool reporting.

### 9.4 Cost

`mart_pipeline_cost_daily` (and its panels in Metabase + Lightdash + Grafana) shows: cost per DAG run, cost per TB written, top 10 most expensive tasks, cost trend over time. The miniature Cloud Cost Management analog.

---

## 10. Deployment topology

### 10.1 Local (Docker Compose)

```
infra/compose/
├── compose.base.yml          MinIO, Postgres, Iceberg REST, Airflow, MLflow, Metabase, pgAdmin
├── compose.dev.yml           dev overrides (volume binds, ports)
├── compose.observability.yml otel-collector, prometheus, grafana, tempo, marquez   (Sprint 0)
├── compose.lightdash.yml     lightdash                                              (Sprint 1)
└── compose.ai.yml            ollama, qdrant, chainlit                               (Sprint 2)
```

Each profile is independently startable: `make obs-up`, `make lightdash-up`, `make ai-up`.

### 10.2 Cloud target (AWS via Terraform)

```
infra/terraform/
├── bigquery/                 # Sprint 3: apply-ready (BQ free tier)
└── aws/                      # Sprint 3: plan-only
    ├── s3/                    cricket-source-files, cricket-lakehouse, cricket-ml-models
    ├── glue/                  catalog for Iceberg tables
    ├── emr-serverless/        Silver Spark workloads
    ├── mwaa/                  Airflow
    ├── athena/                ad-hoc + serving
    ├── ecs/                   FastAPI + Metabase + Lightdash
    ├── opensearch/            replaces Qdrant for vector store
    └── bedrock/               IAM for agent calls
```

Endpoint config (env vars) is the only delta vs. local. No business logic changes.

---

## 11. Non-functional requirements

| Requirement | Design intent |
|---|---|
| Reproducibility | One-command bootstrap (`make up && make bootstrap`); deterministic ingestion; runbooks under `docs/runbooks/` |
| Portability | Open storage + table format + lineage + telemetry; Terraform module mirrors local |
| Maintainability | Modular monolith with clear boundaries; ADRs; runbooks; CI checks; project skills |
| Trust | DQ at every boundary; lineage stitched across emitters; idempotency at every layer |
| Performance | Engine specialization; partition pruning; BQ clustering documented |
| Observability | OpenLineage + OTEL + Soda; cost as a first-class metric |
| Explainability | Every choice has an ADR; semantic layer is the single source of truth |
| Cost awareness | FinOps mart on the platform itself; cloud cost ceiling alerts when AWS lands |

---

## 12. Risks and mitigations (target state)

| Risk | Impact | Mitigation |
|---|---|---|
| Agent hallucination | High | Tools-first design; no raw SQL via chat; SQL guardrails on the `sql` mode; eval harness ≥80% required to merge |
| BigQuery free-tier limits | Medium | Sandbox mode; cap sync volume; document the limit |
| Ollama local performance | Medium | Bedrock fallback via config; eval harness measures latency; agent timeout |
| DuckDB single-writer lock | Medium | `make duckdb-stop` + stop Metabase before Gold DAG; runbook documented |
| Marquez vs DataHub future swap | Low | Both consume OpenLineage; swap is config |
| Open-standards drift | Medium | Every new dep reviewed against ADR 0004; rejected if not standards-binding |
| Two BI tools = two maintenance surfaces | Medium | Lightdash is config-as-code (dashboards as YAML); Metabase is provisioned via script; both rebuildable from source |
| 6–8 weeks aggressive | High | Each sprint independently shippable; JD-only variant (Sprints 0+1) covers 80% of Harness JD bullets in 3 weeks |

---

## 13. Comparison: as-built vs target

| Capability | As-built (today) | Target (after revamp v2) |
|---|---|---|
| Lakehouse | Bronze + Silver + Gold (Iceberg) | Same |
| Orchestration | 8 Airflow DAGs | 11 DAGs (+sync_bq, +dq_soda, +refresh_ai_metadata) |
| OLAP | DuckDB only | DuckDB + BigQuery (twin target) |
| BI | Metabase | Metabase + Lightdash + Observable |
| Semantic layer | None | dbt MetricFlow |
| Lineage | None | OpenLineage → Marquez |
| Telemetry | Container logs only | OTEL → Prometheus + Grafana + Tempo |
| DQ | dbt-tests (40) + custom checks (31) | + Soda Core declarative on top |
| Cost telemetry | None | `mart_pipeline_cost_daily` + dashboards |
| API gateway | Empty stub | FastAPI with /query, /chat, guardrails |
| AI assistant | Empty stub | LangGraph agent + 6 tools + Chainlit UI |
| Scale story | 21k matches | + 100M-row synthetic, perf write-up |
| Cloud parity | Local only | Terraform BigQuery (apply) + AWS (plan) |
| ADRs | 3 empty files | 10 complete |

---

## 14. Glossary (terms that show up across all docs)

| Term | Meaning |
|---|---|
| **Bronze** | Source-faithful, all-string Iceberg tables |
| **Silver** | Typed, exploded, deduplicated Iceberg tables (one row per business entity) |
| **Gold** | dbt star schema in DuckDB + BigQuery; the analytical contract |
| **MetricFlow** | dbt's semantic layer — declarative metrics resolved to SQL at query time |
| **OpenLineage** | Open spec for emitting lineage events from data tools |
| **OpenTelemetry** | Open spec for emitting metrics, traces, and logs |
| **SCD2** | Slowly Changing Dimension Type 2 — keeps history with `valid_from`/`valid_to` |
| **MAR (match_file_audit revision)** | Audit-driven dedup mechanism — Bronze appends new revisions when same match_id arrives with new file hash |
| **Semantic-layer-grounded AI** | Agent can only query through MetricFlow, not raw SQL — eliminates a class of hallucination |
| **FinOps mart** | Pipeline cost telemetry modeled dimensionally — the platform's analog of Cloud Cost Management |

---

## 15. References

- ADR 0001 — modular monolith
- ADR 0002 — Apache Iceberg
- ADR 0003 — Apache Airflow
- ADR 0004 — open standards first (founding principle for v2)
- ADR 0005 — BigQuery secondary target *(Sprint 3)*
- ADR 0006 — MetricFlow as semantic layer *(Sprint 0/1)*
- ADR 0007 — FastAPI gateway design *(Sprint 1)*
- ADR 0008 — SQL guardrails *(Sprint 1/2)*
- ADR 0009 — LangGraph agent design *(Sprint 2)*
- ADR 0010 — scale strategy *(Sprint 4)*

- `docs/planning.md` — canonical execution plan
- `docs/architecture/as-built.md` — current snapshot
- `docs/architecture/data-flow.md` — per-event flow diagrams
- `docs/architecture/service-interactions.md` — endpoint-level interactions
- `docs/architecture/repo-structure.md` — codebase layout
- `docs/architecture/data-model.md` — ERDs
- `docs/architecture/source-warehouse-contracts.md` — landing/Bronze contracts
