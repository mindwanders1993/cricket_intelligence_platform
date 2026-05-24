# Cricket Intelligence Platform — Development Plan

> Canonical roadmap. Personal scratch version lives at `~/.claude/plans/hi-soft-prism.md`.
> Last updated: 2026-05-24.

The platform is built in **vertical slices** end-to-end (Source → Bronze → Silver → Gold). Phase 1–4 are complete and shipped on `main`. The current revamp (v2) executes the deferred Phase 5/6/7 plus a set of extensions that turn the platform into a senior-DE portfolio asset with open-standards-first architecture.

---

## Status legend

- ✅ Done — built, tested, lint-clean, merged to `main`
- 🔄 In progress — partially built
- ⬜ Not started

---

## Part A — Foundation phases (✅ shipped)

These are the eight "Big Tasks" that brought the platform from zero to a working Source-to-Gold lakehouse with Metabase BI. They are kept here as a one-line summary; the detailed historical task lists live in git history (see `docs/architecture/as-built.md` for the current snapshot).

| # | Big Task | Status | Output |
|---|---|---|---|
| 1 | Project + environment foundation | ✅ | Runnable platform skeleton, control schema, shared `src/cip/` modules, Makefile, 16 settings tests |
| 2 | Source understanding + contracts | ✅ | `docs/architecture/source-contracts.md`, `source-warehouse-contracts.md`, 14 known edge cases documented |
| 3 | Register pipeline (people + names) | ✅ | Landing → Bronze → Silver (Polars + PyIceberg); schema drift detection; weekly DAGs |
| 4 | Match ingestion + Bronze | ✅ | `all_json.zip` (~21k matches) + daily `recently_added_2_json.zip`; audit-driven dedup via `control.match_file_audit`; `(match_id, revision)` PK |
| 5 | Match Silver explosion | ✅ | PySpark + Iceberg: matches / innings / deliveries / wickets / match_players / match_officials / teams / venues / competitions |
| 6 | Silver DQ + reconciliation | ✅ | Structural + cricket-specific checks; `control.dq_results`; 31 active checks across landing/bronze/silver |
| 7 | Gold warehouse foundation | ✅ | dbt project (`models/dbt/`), DuckDB target; staging, dims, facts |
| 8 | Gold marts + warehouse validation | ✅ | 6 dims + 5 facts + 7 marts; 40 dbt tests; `analysis/validation_queries.sql` (9 sections, ~30 queries); Metabase BI provisioned |

**Snapshot of `main` (2026-05-24):** Iceberg lakehouse end-to-end, 8 Airflow DAGs with auto-trigger chains, dbt star schema in DuckDB, Metabase dashboards, Observable Framework dashboard scaffolded at M1–M2. See `docs/architecture/as-built.md` for module-level detail.

---

## Part B — Revamp v2 (current scope, ✅ planned / ⬜ in flight)

> Goal: execute Phases 5/6/7 of the original roadmap and add open-standards extensions so the platform demonstrates senior-DE depth. Targets Senior Data Engineer roles (primary: Harness Cloud Cost Management).
>
> **North star:** open standards first → OSS implementations → enterprise-swappable. Endpoint config — not code — is the local-to-cloud delta. See `docs/adr/0004-open-standards-first.md`.
>
> **Out of scope (deferred):** real-time streaming, Java/Go service, ML model training, production AWS deployment with billing, Kubeflow.

Default execution order is linear (Sprint 0 → 1 → 2 → 3 → 4). Each sprint is independently shippable.

### Sprint 0 — Observability retrofit + dbt depth foundation (~2 weeks) ⬜

> Unlocks every later sprint. Highest dependency density.

#### Observability spine ⬜
- [ ] `ObservabilitySettings` sub-settings in `src/cip/common/settings.py` (OTEL endpoint, OpenLineage URL, service name, flags)
- [ ] `src/cip/observability/{__init__.py, lineage.py, telemetry.py, cost_emission.py}`
- [ ] `infra/compose/compose.observability.yml` — services: `otel-collector`, `prometheus`, `grafana`, `tempo`, `marquez`
- [ ] `observability/grafana/{datasources,dashboards}/` — datasource configs + `pipeline_health.json`
- [ ] `observability/prometheus/prometheus.yml`
- [ ] `control.pipeline_cost_event` DDL added to `infra/bootstrap/init-metastore.sql`
- [ ] Instrument `src/cip/transform/shared/writers.py` — wrap `PolarsIcebergWriter.create_and_append/overwrite_partition` and `SparkIcebergWriter.dynamic_overwrite` with OTEL spans + emit OpenLineage `RunEvent` + call `cost_emission.record(...)`
- [ ] Install `openlineage-airflow` listener via env vars across all 8 DAGs
- [ ] `Makefile` — `obs-up`, `obs-down`
- [ ] Unit tests in `tests/unit/observability/`

#### dbt depth ⬜
- [ ] `models/dbt/snapshots/dim_player_snapshot.sql` — SCD2 snapshot keyed on `person_id`
- [ ] `models/dbt/models/marts/dimensions/dim_player_scd2.sql` — SCD2 surface
- [ ] `dim_player.sql` → view over `dim_player_scd2 WHERE dbt_valid_to IS NULL`
- [ ] Convert `fact_delivery` + `fact_player_match` to `materialized='incremental'`
- [ ] `models/dbt/models/semantic_models/{players, matches, deliveries}.yml` — MetricFlow semantic models
- [ ] `models/dbt/models/metrics/{batting_average, strike_rate, economy_rate, boundary_pct, run_rate}.yml` — 5 declarative metrics
- [ ] `models/dbt/models/exposures.yml` — declare exposures for Metabase, Observable dashboard, future Lightdash + AI assistant
- [ ] `models/dbt/models/sources.yml` — add `loaded_at_field` + freshness SLAs
- [ ] `models/dbt/macros/test_grain_uniqueness.sql` — reusable grain test

#### Data quality (Soda Core baseline) ⬜
- [ ] `quality/soda/configuration.yml` — DuckDB datasource
- [ ] `quality/soda/checks/{silver_deliveries, silver_matches, gold_fact_delivery}.yml`
- [ ] `make soda-scan` target
- [ ] Hook into pre-PR validation pipeline

#### Architecture decisions ⬜
- [ ] Fill `docs/adr/0001-use-modular-monolith.md`
- [ ] Fill `docs/adr/0002-use-apache-iceberg.md`
- [ ] Fill `docs/adr/0003-use-airflow-for-orchestration.md`
- [ ] Write `docs/adr/0004-open-standards-first.md` (founding principle for v2)

#### Sprint 0 verification gate
- Full stack up via `make up && make obs-up`
- Trigger `ingest_all_match_data_bronze` → Marquez shows lineage; Grafana shows metrics; `control.pipeline_cost_event` populated
- `dbt snapshot && dbt build` clean; `dim_player_scd2` populated
- Re-running `dbt build` with no source change is a no-op for incremental facts
- `mf list metrics` returns ≥5 metrics
- `soda scan` green
- `poetry run pytest tests/unit/observability/ tests/unit/transform/` clean
- `make dag-validate` clean

---

### Sprint 1 — FastAPI gateway + FinOps mart + Lightdash platform dashboard (~1.5 weeks) ⬜

> Brings the API layer online. First Harness-CCM-shaped artefact ships.

#### FastAPI gateway ⬜
- [ ] `src/cip/serving/api/main.py` — FastAPI app, OTEL instrumented, OpenAPI metadata
- [ ] `src/cip/serving/api/dependencies.py` — DI for settings, DuckDB pool, MetricFlow client
- [ ] `src/cip/serving/api/routers/{health, metrics, query, explain, catalog, chat}.py`
- [ ] `src/cip/serving/api/services/{metricflow_client, duckdb_pool, sql_guardrails}.py`
- [ ] AST-walked `sql_guardrails`: blocklist (DROP/DELETE/UPDATE/ATTACH/CREATE), enforce semantic-layer-only sources for chat-issued queries
- [ ] `make api-up`, `api-down`

#### FinOps cost mart ⬜
- [ ] `models/dbt/models/staging/stg_control__pipeline_cost.sql`
- [ ] `models/dbt/models/marts/aggregates/mart_pipeline_cost_daily.sql`
- [ ] `models/dbt/models/marts/aggregates/mart_top_expensive_tasks.sql`
- [ ] `models/dbt/models/marts/aggregates/mart_data_freshness.sql`
- [ ] `models/dbt/dbt_project.yml` — `vars:` for `$/executor-second`, `$/TB-written`, `$/row-written` constants
- [ ] Extend `scripts/provision_metabase_dashboards.py` with "FinOps — Pipeline Cost" dashboard

#### Lightdash platform dashboard ⬜
- [ ] `infra/compose/compose.lightdash.yml`
- [ ] `infra/lightdash/lightdash.yml` — points at dbt project
- [ ] `infra/lightdash/dashboards/{pipeline_health, finops, data_quality}.yml` — config-as-code dashboards
- [ ] `make lightdash-up`

#### Architecture decisions ⬜
- [ ] Fill `docs/adr/0006-metricflow-as-semantic-layer.md`
- [ ] Fill `docs/adr/0007-fastapi-gateway-design.md`
- [ ] Fill `docs/adr/0008-sql-guardrails.md`

#### Sprint 1 verification gate
- `curl localhost:8000/health` returns 200
- `curl localhost:8000/catalog/metrics` returns ≥5 metrics
- `POST /query` with `{"metric": "batting_average", "group_by": ["dim_player__full_name"]}` returns rows
- `POST /query` with raw `DROP TABLE` returns 403 (sql_guardrails reject)
- `mart_pipeline_cost_daily` populated after a clean ingest cycle
- Lightdash + Metabase show the same metrics (semantic-layer portability proven)
- Tests in `tests/unit/serving/` + `tests/integration/serving/` clean

---

### Sprint 2 — Agentic AI assistant with tools (~2 weeks) ⬜

> Phase 5 finish. Single biggest interview differentiator.

#### Agent + tools ⬜
- [ ] `src/cip/serving/ai/chains/{agent.py, llm.py}` — LangGraph agent + Ollama (default) / Bedrock (config flag) client factories
- [ ] Tools in `src/cip/serving/ai/tools/`:
  - [ ] `search_metrics.py` — semantic search over MetricFlow catalog (nomic-embed-text via Ollama)
  - [ ] `get_metric_definition.py`
  - [ ] `query_metric.py` — calls FastAPI `/query`
  - [ ] `lookup_player.py` — fuzzy match against `dim_player` + `gold.player_display_names`
  - [ ] `explain_table.py` — returns dbt docs + column descriptions
  - [ ] `generate_chart_spec.py` — returns Vega-Lite spec
- [ ] `src/cip/serving/ai/prompt_registry/` — system + tool + few-shot prompts
- [ ] `src/cip/serving/ai/retrieval/{embed_dbt_docs.py, vector_store.py}` — Qdrant (or FAISS fallback)
- [ ] `src/cip/serving/ai/jobs/refresh_ai_metadata.py`
- [ ] `orchestration/airflow/dags/dag_refresh_ai_metadata.py` (placeholder in `DagNames` becomes real)

#### Chat UI + API ⬜
- [ ] `apps/ai-studio/playground/chainlit_app.py` — Chainlit chat UI calling FastAPI `/chat`
- [ ] `src/cip/serving/api/routers/chat.py` — SSE streaming endpoint
- [ ] `infra/compose/compose.ai.yml` — `ollama`, `qdrant`, `chainlit`

#### Eval ⬜
- [ ] `apps/ai-studio/evaluation/eval_questions.yml` — ~30 golden questions with expected metrics/SQL
- [ ] `apps/ai-studio/evaluation/run_eval.py` — runs golden set, reports accuracy + latency
- [ ] `make ai-eval`

#### Architecture decisions ⬜
- [ ] Fill `docs/adr/0009-langgraph-agent-design.md` (tools-first vs RAG-first)

#### Sprint 2 verification gate
- Chainlit UI at `localhost:8100` answers *"Virat Kohli batting average in ODIs since 2020"* end-to-end
- `make ai-eval` ≥80% accuracy
- Attempt to issue destructive SQL via chat is refused at `sql_guardrails`
- Agent calls visible in Tempo (one OTEL trace per agent turn)
- `dag_refresh_ai_metadata` runs nightly; rebuilds embeddings + metric catalog cache

---

### Sprint 3 — Cloud-ready: BigQuery target + Terraform plan (~1 week) ⬜

> Proves OLAP portability + IaC discipline. JD-direct hit on BigQuery, GCP, Terraform.

#### BigQuery secondary target ⬜
- [ ] `scripts/sync_silver_to_bq.py` — uses `PolarsIcebergReader` → `google-cloud-bigquery` load jobs; idempotent on `(_snapshot_date, _row_hash)`
- [ ] `orchestration/airflow/dags/sync_silver_to_bigquery.py` — daily; downstream of Silver DAGs
- [ ] `models/dbt/profiles.yml` — `bq_dev` target
- [ ] `models/dbt/dbt_project.yml` — target-aware materialization (BQ `partition_by` + `cluster_by`; DuckDB no-op)
- [ ] `models/dbt/models/sources.yml` — Jinja-switched source schemas via `{{ target.name }}`
- [ ] `pyproject.toml` — `dbt-bigquery`, `google-cloud-bigquery`
- [ ] `make bq-sync`, `make bq-build`, `make tf-plan-bq`

#### Terraform ⬜
- [ ] `infra/terraform/bigquery/{main, variables, outputs}.tf` — BQ dataset + service account + IAM
- [ ] `infra/terraform/aws/{main, variables, outputs}.tf` — S3 + Glue + EMR Serverless + MWAA + Athena (plan-only)
- [ ] `docs/runbooks/bigquery_setup.md`

#### Architecture decisions ⬜
- [ ] Fill `docs/adr/0005-bigquery-as-secondary-target.md`

#### Sprint 3 verification gate
- `make tf-plan-bq` → clean plan; provisioning the dataset works
- `make bq-sync` populates `cricket_silver.*` from local Iceberg
- `cd models/dbt && dbt build --target bq_dev` runs all marts + tests on BigQuery
- Row counts match DuckDB target for every Gold table
- `make tf-plan-aws` clean (no apply)
- FastAPI `/query` returns identical numbers from DuckDB and BigQuery via env-var swap

---

### Sprint 4 — Scale generator + dashboard finish + portfolio polish (~1.5 weeks) ⬜

> Public-facing finish. Story complete.

#### Scale + perf ⬜
- [ ] `scripts/synth/generate_synthetic_deliveries.py` — N=100M realistic delivery rows
- [ ] `docs/perf/scale_test.md` — DuckDB vs BigQuery: query plans, scanned bytes, wall-time before/after partition pruning + clustering
- [ ] `tests/integration/perf/test_partition_pruning.py`

#### Player portfolio dashboard (Observable Framework, M3–M22) ⬜
- [ ] M3 — design system tokens (`dashboard/src/styles/`)
- [ ] M4–M10 — Python data loaders for batting career, season heatmap, dismissal types, opponent matchups, venue performance
- [ ] M11–M20 — D3 + Observable Plot components (timeline river, records wall, KPI strip, dismissal lab, opponent chord)
- [ ] M21 — page assembly (`dashboard/src/index.md`)
- [ ] M22 — embedded Chainlit chat widget (`dashboard/src/ai-chat.md`)

#### Portfolio polish ⬜
- [ ] `docs/portfolio/demo_script.md` — interview demo walkthrough
- [ ] `docs/portfolio/demo.gif` (or `.mp4`) — recorded walkthrough
- [ ] `README.md` — top-level diagram + OSS↔Enterprise mapping + "what this demonstrates" section
- [ ] Updated `docs/architecture/as-built.md` reflecting Sprint 0–4 components

#### Architecture decisions ⬜
- [ ] Write `docs/adr/0010-scale-strategy.md`

#### Sprint 4 verification gate
- `make synth-100m` builds `silver.deliveries_synth` (~100M rows)
- Representative query runs <30s on DuckDB, <10s on BigQuery (with documented EXPLAIN ANALYZE deltas)
- `cd dashboard && npm run build` succeeds; M21 pages render
- Embedded AI chat works in dashboard
- Demo video covers: data flow → lineage → cost → AI → BigQuery parity
- All ADRs 0001–0010 complete

---

## Cross-sprint verification (end of revamp v2)

After Sprint 0–4, the following must all be true:

1. `make up && make obs-up && make api-up && make ai-up && make lightdash-up` brings the full stack online
2. Triggering `ingest_all_match_data_bronze` shows: Marquez lineage Bronze→Silver→Gold (incl. BigQuery sync); Grafana metrics; `control.pipeline_cost_event` populated; Lightdash dashboards reflect the run
3. `dbt build` clean on both `dev` (DuckDB) and `bq_dev` (BigQuery); 50+ tests pass
4. FastAPI `/query` returns identical results from both targets
5. AI assistant `make ai-eval` ≥80%
6. Observable dashboard renders M21 + embedded AI chat
7. Lightdash dashboards mirror the same metrics through MetricFlow
8. `make synth-100m` + `docs/perf/scale_test.md` document partition-pruning wins
9. ADRs 0001–0010 each have Decision/Context/Consequences/Alternatives/Status
10. `make dag-validate` clean; `pytest` clean; `pre-commit run --all-files` clean

---

## Sequencing variants

- **Linear** (default): 0 → 1 → 2 → 3 → 4
- **API-first** (cloud parity before agent): 0 → 1 → 3 → 2 → 4
- **Dashboard-led** (recruiter visibility urgent): 0 → 4 (player dashboard + scale) → 1 → 2 → 3
- **JD-only** (Harness-only push, ~3 weeks): 0 → 1 — observability + dbt depth + semantic layer + FastAPI + FinOps mart + Lightdash; skip AI, BQ, scale, dashboard for now

---

## Reference

- **Target-state architecture:** `docs/architecture/hld-hla.md`
- **Current snapshot:** `docs/architecture/as-built.md`
- **Data flow:** `docs/architecture/data-flow.md`
- **Service interactions:** `docs/architecture/service-interactions.md`
- **Open standards principle:** `docs/adr/0004-open-standards-first.md`
- **Developer cheat-sheet:** `docs/planning_dev.md`
- **Job module reference:** `docs/jobs.md`
- **Runbooks:** `docs/runbooks/`
- **Personal scratch plan:** `~/.claude/plans/hi-soft-prism.md`
