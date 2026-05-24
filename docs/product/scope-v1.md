# Scope — v1 (Cricket Intelligence Platform)

> **Pair with** `docs/product/vision.md` (why), `docs/product/roadmap.md` (when), `docs/planning.md` (how).
> v1 = the state of the platform at the end of revamp v2 (Sprints 0–4 of `docs/planning.md`).

---

## 1. In scope for v1

### 1.1 Data layer

- **Sources** — Cricsheet `all_json.zip` (~21,600 matches), daily `recently_added_2_json.zip`, `people.csv`, `names.csv`.
- **Landing** — MinIO `cricket-source-files` bucket; archive-scoped MinIO prefixes (`archive=all_json/` vs `archive=recently_added_2_json/`).
- **Bronze (Iceberg)** — all-string ingestion via Polars; `(match_id, revision)` PK for match data; `control.match_file_audit` drives append-only dedup.
- **Silver (Iceberg)** — Polars (Register) + PySpark (Match); 12 tables incl. `silver.deliveries` ball-grain fact precursor.
- **Gold (dbt + DuckDB + BigQuery)** — 6 dims (incl. SCD2 `dim_player_scd2`), 5 facts (incl. incremental `fact_delivery`, `fact_player_match`), 10 marts (existing 7 + 3 FinOps: `mart_pipeline_cost_daily`, `mart_top_expensive_tasks`, `mart_data_freshness`).
- **Synthetic scale dataset** — `silver.deliveries_synth` (~100M rows) for partition-pruning + clustering demos.
- **Control schema (Postgres)** — `register_ingestion_log`, `register_schema_versions`, `register_change_log`, `archive_download_log`, `bronze_match_ingestion_log`, `match_file_audit`, `dq_results`, **`pipeline_cost_event`** (new), **`ai_metadata_refresh_log`** (new), plus views.

### 1.2 Orchestration

- 8 existing Airflow DAGs + 3 new (`sync_silver_to_bigquery`, `dq_soda`, `dag_refresh_ai_metadata`).
- `TriggerDagRunOperator` auto-trigger chains Bronze → Silver → Gold for both monthly full and daily incremental match pipelines.
- Custom Airflow image with PySpark + JDK17 + Iceberg JAR cache + Pydantic v2 + new Sprint deps (openlineage-airflow, opentelemetry, soda-core-duckdb, dbt-bigquery).

### 1.3 Analytics engineering

- dbt-core with **MetricFlow semantic layer** (≥5 metrics: `batting_average`, `strike_rate`, `economy_rate`, `boundary_pct`, `run_rate`).
- **dbt SCD2** snapshot on `dim_player` + view over current rows.
- **dbt incremental** on `fact_delivery` and `fact_player_match` with `unique_key` + `on_schema_change='append_new_columns'`.
- **dbt exposures** declaring every consumer (Metabase dashboards, Lightdash dashboards, Observable dashboard, AI assistant, MLflow).
- **Source freshness SLAs** on all Silver sources.
- **50+ dbt tests** (existing 40 + Soda Core baseline + dbt-expectations on critical columns).
- **Twin targets**: `dev` (DuckDB) and `bq_dev` (BigQuery free tier); identical models, target-aware materialization (BQ partitioning + clustering vs DuckDB no-op).

### 1.4 Data quality

- dbt-tests (50+) on Gold models.
- **Soda Core declarative DQ** on Silver and Gold critical tables (row count thresholds, PK nullness, partition completeness, schema drift, freshness).
- Custom DQ checks in `src/cip/quality/` (existing 31 checks across landing/Bronze/Silver) preserved.
- All DQ results land in `control.dq_results` for cross-tool reporting.

### 1.5 Observability

- **OpenLineage** events from writers + Airflow listener + dbt-ol → Marquez UI.
- **OpenTelemetry** spans + metrics from writers + FastAPI + agent → OTEL Collector → Prometheus + Tempo + Grafana.
- **`pipeline_cost_event`** rows emitted per task; aggregated into `mart_pipeline_cost_daily`.
- **Grafana dashboards** — `pipeline_health.json` (DAG SLOs), `finops.json` (cost panels).

### 1.6 Serving + BI

- **DuckDB** native tables for Bronze/Silver + Gold star schema; single-writer / multi-reader lock semantics documented.
- **BigQuery** parallel Gold materialization with row-count parity validation.
- **Metabase** (SQL-card-driven, executive + analyst dashboards).
- **Lightdash** (semantic-layer-driven, pipeline health + FinOps + DQ dashboards as config-as-code).
- **Observable Framework** — Kohli player portfolio dashboard (M3–M22, embedded AI chat at M22).

### 1.7 Gateway + AI

- **FastAPI gateway** at `src/cip/serving/api/` with:
  - `GET /health`, `GET /metrics`
  - `POST /query` — MetricFlow-backed (`{metric, dimensions, filters}`) **or** SQL-guardrailed
  - `GET /explain/{model}` — dbt lineage + column docs
  - `GET /catalog/metrics`, `GET /catalog/tables`
  - `POST /chat` — SSE-streamed agent responses
- **SQL guardrails** — AST walker bans DROP/DELETE/UPDATE/ATTACH/CREATE; allows reads against the semantic-layer-exposed schemas only.
- **LangGraph agent** with 6 tools:
  - `search_metrics` (Qdrant semantic search)
  - `get_metric_definition`
  - `query_metric` (calls `/query`)
  - `lookup_player` (fuzzy on `dim_player` + `gold.player_display_names`)
  - `explain_table`
  - `generate_chart_spec`
- **LLM provider** switchable via `AISettings`: Ollama (default, local Llama 3.1) / AWS Bedrock (cloud).
- **Chainlit chat UI** in `apps/ai-studio/playground/` calling FastAPI `/chat`.
- **Golden-set eval** — ~30 questions, ≥80% pass rate.
- **`dag_refresh_ai_metadata`** — nightly DAG rebuilding embeddings + metric catalog cache.

### 1.8 Cloud-readiness

- **Terraform module for BigQuery** — apply-ready against BigQuery free tier.
- **Terraform module for AWS** — plan-only against S3 + Glue + EMR Serverless + MWAA + Athena + ECS (BI). No apply in v1 (no funded account).
- **Open-standards-binding** — every component on the JD-relevant migration path: S3 API, Iceberg REST, OpenLineage, OpenTelemetry, ANSI SQL, dbt manifest, OpenAPI.

### 1.9 Documentation + portfolio polish

- **10 ADRs** (0001–0010) covering modular monolith, Iceberg, Airflow, open-standards-first, BigQuery secondary target, MetricFlow, FastAPI gateway, SQL guardrails, LangGraph agent, scale strategy.
- **Architecture docs** — HLD/HLA, data flow, service interactions, repo structure, ERDs, source contracts.
- **Runbooks** — local bootstrap, full rebuild, backfill, recover failed DAG, dashboard coordination, BigQuery setup, AI metadata refresh.
- **Demo assets** — `docs/portfolio/demo_script.md` + `docs/portfolio/demo.gif` (or `.mp4`).
- **Perf write-up** — `docs/perf/scale_test.md` with EXPLAIN ANALYZE deltas on DuckDB + BigQuery.

---

## 2. Out of scope for v1

Each item below is **deliberately deferred**. Most have a documented re-open trigger.

### 2.1 Streaming / real-time

- Apache Kafka / Redpanda / Confluent integration.
- Spark Structured Streaming / Flink jobs.
- Sub-second freshness SLAs.
- Re-open if a streaming-first JD targets the candidate.

### 2.2 ML

- Model training (win probability, batter projection, captain pick).
- Feature store (Feast).
- Online model serving.
- A/B testing infrastructure.
- MLflow tracking is live but no models ship in v1.

### 2.3 Production AWS

- `terraform apply` against AWS.
- EKS / EMR / RDS / S3 actually provisioned.
- Billing alerts.
- CloudWatch wired up.
- v1 deliverable is **plan-only**; apply happens when a funded account exists.

### 2.4 Multi-tenancy / SaaS

- Authn / authz (Cognito / Auth0).
- Per-user data isolation.
- API rate limiting.
- Pricing tiers.
- v1 is single-developer, localhost-bound.

### 2.5 Extended data domains

- External enrichment feeds (CricViz, Opta).
- Live scorecards.
- Fantasy cricket integration.
- Audio commentary transcripts.
- Cricsheet alone is sufficient for the portfolio narrative.

### 2.6 Other deferred

- **Java / Go services** — Python-only for v1.
- **Kubernetes (k3d / EKS)** — Compose only; k3d optional in a hypothetical Sprint 5+.
- **Vault / Secrets Manager** — `.env` is sufficient for local dev.
- **SSO** — Metabase / Lightdash / Airflow use default local credentials.
- **Kubeflow** — MLflow covers MLOps needs.
- **GraphQL** — REST/OpenAPI only.
- **Real DataHub / Atlan / Datadog** — Marquez + Prometheus/Grafana/Tempo + Soda Core are the OSS equivalents; cloud cousins documented but not provisioned.

---

## 3. Out-of-scope decision rationale

The OOS list is the **anti-portfolio** list — things that look good but don't return on the time invested for Senior DE applications **right now**. The two heuristics:

1. **Does it map to a JD bullet I'm targeting today?** If no → defer.
2. **Does it depend on the v1 platform being shipped first?** If yes → defer until v1 is done.

Streaming, ML, k8s, polyglot all fail heuristic 1 against the current target (Harness CCM + adjacent batch-shape data-platform roles). They'll be re-opened when a JD makes them load-bearing.

---

## 4. Boundary conditions

What v1 explicitly handles vs. punts:

| Scenario | v1 behaviour | Reason |
|---|---|---|
| Cricsheet publishes a new `key_*` column in `people.csv` | Schema drift detected via `register_schema_versions`; informational alert; Bronze load continues | Unpivoted long-form Bronze means new columns flow through without code change |
| A match JSON arrives with a new wicket kind | Bronze load succeeds (all-string); Silver `accepted_values` test fails; DQ result flagged | Type/enum drift is a Silver problem, not Bronze |
| BigQuery free tier hits slot/quota | `sync_silver_to_bigquery` DAG fails; DuckDB target unaffected | Documented in ADR 0005; operator decides defer/shrink/upgrade |
| Ollama is slow / unavailable | Agent falls back via `AISettings.llm_provider="bedrock"` (config flag) | Latency targets in golden eval |
| User asks the chat to drop a table | SQL guardrails reject at AST walk; logged + telemetered | Hard requirement: the agent has no destructive-SQL path |
| DuckDB write lock contended | `make refresh-gold` stop-trigger-restart sequence handles common cases; runbook covers edge cases | Single-writer rule documented in `docs/runbooks/dashboard.md` and `service-interactions.md` |
| Metabase + Lightdash show different numbers for the same metric | Bug — semantic layer must be the single source of truth | dbt MetricFlow models feed both; investigation root-causes Metabase SQL drift vs MetricFlow definition |
| AI assistant golden eval drops below 80% | Sprint 2 verification gate fails; sprint can't be marked done | Hard threshold in `docs/planning.md` |

---

## 5. References

- Why this scope: `docs/product/vision.md`
- When each piece lands: `docs/product/roadmap.md`
- How each piece is built: `docs/planning.md`
- Open-standards principle: `docs/adr/0004-open-standards-first.md`
