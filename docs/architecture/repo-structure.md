# Repository Structure — Cricket Intelligence Platform

> Top-level navigation guide for the codebase. Pair with `docs/architecture/hld-hla.md` (target architecture) and `docs/architecture/as-built.md` (current state).
> Items marked **(planned)** ship during revamp v2 — see `docs/planning.md` for the sprint that adds them.

---

## 1. Top-level layout

```
cricket-intelligence-platform/
├── README.md                       Master HLD/HLA + roadmap pointer
├── CLAUDE.md / GEMINI.md           AI-assistant working agreements
├── Makefile                        All operational commands (up, down, bootstrap, refresh-gold, …)
├── pyproject.toml + poetry.lock    Python deps (single source of truth)
├── docker-compose.yml              Top-level shim that includes infra/compose/*
├── .env / .env.example             Environment variables (host vs container — see README §21)
├── .pre-commit-config.yaml         ruff + black + isort + pytest hooks
├── .github/workflows/              CI: ci.yml, dbt-ci.yml, docs.yml
│
├── src/cip/                        Python platform (modular monolith) — see §2
├── orchestration/airflow/          DAG definitions + plugins — see §3
├── models/dbt/                     dbt project (sources, staging, marts, tests, snapshots, semantic) — see §4
├── infra/                          Docker Compose, bootstrap SQL, custom images, Helm/k3d, Terraform — see §5
├── apps/                           Consumer apps (ai-studio playground, fastapi symlink) — see §6
├── dashboard/                      Observable Framework player portfolio — see §7
├── observability/                  Grafana + Prometheus configs + dashboards (planned, Sprint 0) — see §8
├── docs/                           All long-form documentation — see §9
├── quality/                        Soda Core checks (planned, Sprint 0) — see §10
├── analysis/                       Hand-curated validation SQL — see §11
├── scripts/                        One-off operational scripts — see §12
├── tests/                          unit / integration / contract / data_quality / fixtures — see §13
├── validation/                     Validation harness runner — see §14
├── notebooks/                      Exploration, validation, ML notebooks — see §15
├── conf/                           Layered YAML config (base / dev / prod) — see §16
├── local/                          Gitignored scratch (planning docs, sample matches, scratch) — see §17
├── storage/                        Bind-mounted DuckDB file (gitignored)
├── support_docs/                   Source-data sample/spec files
└── graphify-out/                   Knowledge graph (auto-generated via `graphify update`)
```

---

## 2. `src/cip/` — the platform monolith

Modular monolith with shared types/utilities accessible across packages. Decided in ADR 0001.

```
src/cip/
├── common/
│   ├── settings.py              PlatformSettings + get_settings() singleton
│   ├── logging.py               structlog wrapper (get_logger, bind_context)
│   ├── exceptions.py            IcebergError + platform exception hierarchy
│   ├── checksum.py              SHA-256 helpers
│   └── contracts/
│       ├── enums.py             StrEnum: Layer, MatchType, WicketKind, ExtraType, …
│       └── naming.py            TableName, PathBuilder, META, DagNames, IcebergProperties
│
├── ingestion/
│   ├── io/
│   │   └── minio.py             MinIOClient (from_settings, health_check, …)
│   ├── people_and_names/        Register pipeline (download + parse + Bronze writers)
│   ├── match_data/              Match pipeline (download + extract + checksum)
│   └── jobs/                    Airflow-callable wrappers + CLI entrypoints
│       ├── ingest_people_and_names.py
│       ├── build_silver_people_and_names.py
│       ├── full_load_match_data.py
│       ├── incremental_match_data.py
│       ├── build_silver_match_data.py
│       └── run_gold_dbt_models.py
│
├── transform/
│   ├── polars/
│   │   ├── bronze/              match_data + people_and_names Bronze writers (Polars + PyIceberg)
│   │   └── silver/persons.py    PolarsPeopleAndNamesSilverTransform
│   ├── spark/
│   │   └── silver/              PySpark match-data Silver pipeline (matches/innings/deliveries/…)
│   └── shared/
│       ├── writers.py           PolarsIcebergWriter (Bronze + Silver-Polars) + SparkIcebergWriter
│       ├── readers.py           PolarsIcebergReader + Spark-Iceberg session bootstrap
│       └── partitioning.py      PartitionStrategy registry
│
├── serving/
│   ├── api/                     FastAPI gateway (planned, Sprint 1)
│   │   ├── main.py              app + OTEL middleware
│   │   ├── dependencies.py      DI (settings, DuckDB pool, MetricFlow client)
│   │   ├── routers/             health, metrics, query, explain, catalog, chat
│   │   └── services/            metricflow_client, duckdb_pool, sql_guardrails
│   ├── duckdb/
│   │   └── refresh.py           DuckDBRefresh — Iceberg → DuckDB tables (Bronze + Silver + control schemas)
│   └── ai/                      LangGraph agent + tools + prompt registry (planned, Sprint 2)
│       ├── chains/              agent.py (LangGraph) + llm.py (Ollama/Bedrock factories)
│       ├── tools/               search_metrics, get_metric_definition, query_metric,
│       │                         lookup_player, explain_table, generate_chart_spec
│       ├── prompt_registry/     system + tool + few-shot markdown prompts
│       ├── retrieval/           embed_dbt_docs.py + vector_store.py (Qdrant wrapper)
│       └── jobs/                refresh_ai_metadata.py
│
├── observability/               OpenLineage + OTEL + cost emission (planned, Sprint 0)
│   ├── lineage.py               OpenLineage facet builder
│   ├── telemetry.py             OTEL tracer/meter helpers
│   └── cost_emission.py         control.pipeline_cost_event writer
│
├── quality/                     DQ checkers per pipeline — writes to control.dq_results
│   └── checks/
│
└── ml/                          Feature engineering + training + MLflow (deferred)
```

### Key rules

- Always call `get_settings()`, never instantiate `PlatformSettings` directly.
- Always build paths via `TableName`/`PathBuilder` — no raw f-strings (see `docs/planning_dev.md`).
- Always use `META.*` constants for metadata column names.
- Writers/readers go through `transform/shared/` — don't write to Iceberg from anywhere else.
- AI tools never construct raw SQL — they go through MetricFlow or parameterized helpers.

---

## 3. `orchestration/airflow/`

```
orchestration/airflow/
├── dags/
│   ├── ingest_people_and_names_bronze.py
│   ├── ingest_people_and_names_silver.py
│   ├── ingest_all_match_data_bronze.py
│   ├── ingest_all_match_data_silver.py
│   ├── ingest_all_match_data_gold.py
│   ├── ingest_two_day_match_data_bronze.py
│   ├── ingest_two_day_match_data_silver.py
│   ├── ingest_two_day_match_data_gold.py
│   ├── dag_parse_bronze_match_data.py     placeholder (DagNames reserves the id)
│   ├── dag_run_quality_checks.py          placeholder
│   ├── dag_refresh_serving_layer.py       placeholder
│   ├── dag_refresh_ai_metadata.py         placeholder → real in Sprint 2
│   ├── dag_train_ml_model.py              placeholder
│   ├── sync_silver_to_bigquery.py         planned, Sprint 3
│   └── dq_soda.py                          planned, Sprint 0
├── plugins/                                (empty — OpenLineage listener installed via env vars)
└── include/                                shared task utilities
```

DAG files are thin wrappers around callables in `src/cip/ingestion/jobs/`. Task callables receive `snapshot_date`, `pipeline_run_id`, `force` via Jinja `op_kwargs`.

---

## 4. `models/dbt/`

```
models/dbt/
├── dbt_project.yml
├── profiles.yml                          dev (DuckDB) + bq_dev (BigQuery) (Sprint 3)
├── packages.yml                          dbt-utils, dbt-expectations, dbt-labs/metricflow
├── seeds/
│   └── player_aliases.csv                Source for gold.player_display_names
├── snapshots/
│   └── dim_player_snapshot.sql           (Sprint 0) SCD2 snapshot keyed on person_id
├── models/
│   ├── sources.yml                       Silver Iceberg tables + control.* sources
│   ├── exposures.yml                     (Sprint 0) Metabase, Lightdash, dashboard, AI
│   ├── staging/                          stg_silver_* views; stg_control__pipeline_cost (Sprint 1)
│   ├── marts/
│   │   ├── dimensions/                   dim_match, dim_player, dim_player_scd2 (Sprint 0),
│   │   │                                  dim_team, dim_venue, dim_competition, dim_official, dim_date
│   │   ├── facts/                        fact_delivery (incremental, Sprint 0), fact_innings,
│   │   │                                  fact_match_result, fact_player_match (incremental, Sprint 0),
│   │   │                                  fact_player_of_match
│   │   └── aggregates/                   mart_player_batting_*, mart_player_bowling_*,
│   │                                      mart_team_performance, mart_venue_dna, mart_phase_scoring,
│   │                                      mart_toss_outcome, mart_matchup_analysis,
│   │                                      mart_pipeline_cost_daily (Sprint 1),
│   │                                      mart_top_expensive_tasks (Sprint 1),
│   │                                      mart_data_freshness (Sprint 1)
│   ├── semantic_models/                  (Sprint 0) players.yml, matches.yml, deliveries.yml
│   └── metrics/                          (Sprint 0) batting_average.yml, strike_rate.yml,
│                                          economy_rate.yml, boundary_pct.yml, run_rate.yml
├── macros/
│   ├── test_grain_uniqueness.sql         (Sprint 0) reusable grain test
│   └── (existing macros)
└── tests/                                custom tests (e.g. fact_player_of_match_unique_grain)
```

`dbt build` runs models + tests; `dbt snapshot` runs SCD2 snapshots; `mf list metrics` lists MetricFlow metrics.

---

## 5. `infra/`

```
infra/
├── compose/
│   ├── compose.base.yml                  MinIO + Postgres + Iceberg REST + Airflow + MLflow + pgAdmin + Metabase
│   ├── compose.dev.yml                   dev overrides (bind mounts, host ports)
│   ├── compose.observability.yml         (Sprint 0) otel-collector + prometheus + grafana + tempo + marquez
│   ├── compose.lightdash.yml             (Sprint 1) lightdash
│   └── compose.ai.yml                    (Sprint 2) ollama + qdrant + chainlit
├── bootstrap/
│   ├── create-buckets.sh                 MinIO bucket creation
│   ├── init-metastore.sql                control schema DDL — appended in Sprint 0 with control.pipeline_cost_event
│   └── setup-pgadmin-passfile.sh
├── docker/
│   ├── airflow/Dockerfile                Custom Airflow image (PySpark + JDK17 + Iceberg JAR cache + ...)
│   └── metabase/Dockerfile               Temurin-21 + DuckDB driver
├── iceberg/                              catalog config, table properties
├── pgadmin/
│   ├── servers.json                      pgAdmin auto-connect config
│   └── setup-pgpass.sh                   generates /pgpassfile inside container
├── lightdash/                            (Sprint 1) lightdash.yml + dashboards/*.yml
├── k8s/                                  (Sprint 5+ optional) Helm charts for k3d / EKS
└── terraform/
    ├── bigquery/                         (Sprint 3) BQ dataset + service account + IAM (apply-ready)
    └── aws/                              (Sprint 3) S3 + Glue + EMR Serverless + MWAA + Athena (plan-only)
```

---

## 6. `apps/`

```
apps/
├── ai-studio/                            (Sprint 2)
│   ├── playground/
│   │   └── chainlit_app.py               Chainlit chat UI calling FastAPI /chat
│   └── evaluation/
│       ├── eval_questions.yml            ~30 golden questions with expected metrics/SQL
│       └── run_eval.py                   golden-set runner → accuracy + latency report
├── fastapi/                              symlink → ../src/cip/serving/api
└── superset/                              DEPRECATED — empty stubs; Metabase + Lightdash chosen instead
```

ADR 0007 documents why FastAPI ships as a `src/cip/serving/api/` package (importable as a library) but is exposed at `apps/fastapi/` as a symlinked deployment unit.

---

## 7. `dashboard/`

Observable Framework site — player portfolio (Virat Kohli showcase).

```
dashboard/
├── README.md                             Player dashboard docs (separate from platform/ops Lightdash)
├── docs/AI_DEVELOPMENT_GUIDE.md          M1–M27 milestone plan
├── observablehq.config.js                Site config + Python interpreter wiring
├── package.json                          npm scripts (dev binds to :3030)
├── .env.example                          DUCKDB_PATH (gitignored)
└── src/
    ├── index.md                          Landing page (player dropdown)
    ├── components/                       D3 + Plot chart components (Sprint 4)
    ├── data/*.csv.py                     Python data loaders (build-time)
    ├── styles/                           CSS design tokens (Sprint 4)
    └── ai-chat.md                        Embedded Chainlit widget (Sprint 4 M22)
```

Reads DuckDB **direct** (read-only). The platform/ops Lightdash dashboards are a separate surface — see `infra/lightdash/`.

---

## 8. `observability/`

```
observability/
├── prometheus/                           (Sprint 0) prometheus.yml + scrape configs
├── grafana/
│   ├── datasources/                      (Sprint 0) prometheus.yml + tempo.yml + marquez.yml
│   └── dashboards/                       (Sprint 0) pipeline_health.json + finops.json (Sprint 1)
└── alerts/                               (Sprint 0+) alert rules — DAG failures, freshness SLOs, cost overrun
```

Configs are mounted into the `grafana`/`prometheus` containers via Compose volumes (defined in `compose.observability.yml`).

---

## 9. `docs/`

```
docs/
├── planning.md                           Canonical execution plan (sprints + checklists)
├── planning_dev.md                       Developer cheat-sheet (daily commands + conventions)
├── jobs.md                               Job module reference (per-module task / DAG mapping)
├── architecture/
│   ├── hld-hla.md                        Target-state HLD/HLA (this revamp)
│   ├── as-built.md                       Current snapshot on main
│   ├── data-flow.md                      End-to-end data-flow diagrams + per-record trace
│   ├── service-interactions.md           Service-to-service edges, ports, locks, auth
│   ├── repo-structure.md                 (this doc) top-level navigation guide
│   ├── data-model.md                     ERDs (Bronze + Silver + Gold)
│   ├── source-contracts.md               Cricsheet source contracts (formats, schemas, edge cases)
│   └── source-warehouse-contracts.md     Source → warehouse contracts (naming, partitioning, idempotency)
├── adr/
│   ├── README.md                         ADR index + how-to (Sprint 0)
│   ├── adr-template.md                   MADR-lite template (Sprint 0)
│   ├── 0001-use-modular-monolith.md      (Sprint 0)
│   ├── 0002-use-apache-iceberg.md        (Sprint 0)
│   ├── 0003-use-airflow-for-orchestration.md (Sprint 0)
│   ├── 0004-open-standards-first.md      (Sprint 0 — founding principle for v2)
│   ├── 0005-bigquery-as-secondary-target.md  (Sprint 3)
│   ├── 0006-metricflow-as-semantic-layer.md  (Sprint 0/1)
│   ├── 0007-fastapi-gateway-design.md    (Sprint 1)
│   ├── 0008-sql-guardrails.md            (Sprint 1/2)
│   ├── 0009-langgraph-agent-design.md    (Sprint 2)
│   └── 0010-scale-strategy.md            (Sprint 4)
├── product/
│   ├── vision.md                         Portfolio framing, target persona, value prop
│   ├── scope-v1.md                       In/out of scope
│   └── roadmap.md                        Links to planning.md sprints with date estimates
├── runbooks/
│   ├── local-bootstrap.md                From-zero setup
│   ├── full-rebuild.md                   Wipe + reboot
│   ├── backfill-cricsheet.md             Full historical backfill
│   ├── recover-failed-dag.md             DAG recovery procedures
│   ├── dashboard.md                      Metabase + DuckDB UI coordination
│   ├── duckdb-iceberg-deletes.md         DuckDB ↔ Iceberg delete semantics
│   ├── refresh-ai-metadata.md            AI metadata refresh (Sprint 2)
│   ├── bigquery_setup.md                 BigQuery target setup (Sprint 3)
│   ├── claude-dev-guide.md               AI-assistant dev workflow
│   └── gemini-dev-guide.md               same, for Gemini
├── silver_match_spec/                    Historical Big Task 5 spec + RUNBOOK (archival)
├── portfolio/                            (Sprint 4) demo_script.md + demo.gif
├── perf/                                 (Sprint 4) scale_test.md
└── images/                               diagrams (e.g. platform_arch_clean.png)
```

---

## 10. `quality/`

```
quality/
├── soda/                                 (Sprint 0)
│   ├── configuration.yml                 DuckDB datasource
│   └── checks/
│       ├── silver_deliveries.yml         row count, PK nulls, partition completeness
│       ├── silver_matches.yml
│       └── gold_fact_delivery.yml
└── (existing per-pipeline DQ stays in src/cip/quality/)
```

Two DQ surfaces: code-side checks in `src/cip/quality/` (already on `main`), declarative Soda Core (added Sprint 0).

---

## 11. `analysis/`

```
analysis/
├── validation_queries.sql                9 sections, ~30 queries; end-to-end correctness gate
└── charts/                               ad-hoc charts (e.g. exported from DuckDB UI)
```

Pasted into the DuckDB UI for milestone validation. Section 7.4 documents a small known wicket diff (multi-wicket deliveries).

---

## 12. `scripts/`

```
scripts/
├── bootstrap_match_file_audit.py         One-shot backfill of control.match_file_audit
├── cleanup_silver_stale_snapshots.py     Garbage-collect Iceberg snapshots beyond retention
├── diag_silver_snapshots.py              Diagnostic: list current Silver snapshots per table
├── provision_metabase_dashboards.py      Idempotent Metabase dashboard provisioner
├── refresh_duckdb_views.py               Standalone DuckDBRefresh runner (mirrors the Gold task)
├── sync_iceberg_to_bq.py                 (Sprint 3) Silver Iceberg → BigQuery
└── synth/
    └── generate_synthetic_deliveries.py  (Sprint 4) 100M-row synthetic delivery generator
```

All scripts are CLI-driven (`poetry run python scripts/...`). Frequently used ones get a `make` target.

---

## 13. `tests/`

```
tests/
├── unit/                                 Mocked I/O; runs without containers
│   ├── test_settings.py
│   ├── ingestion/...
│   ├── transform/...
│   ├── quality/...
│   ├── observability/  (Sprint 0)
│   ├── serving/        (Sprint 1+)
│   └── serving/ai/     (Sprint 2)
├── integration/                          Hits real MinIO + Iceberg REST + Postgres
│   ├── transform/
│   ├── serving/        (Sprint 1+)
│   └── perf/           (Sprint 4) — partition pruning assertions
├── contract/                             Schema / API contract tests (planned)
├── data_quality/                         DQ-specific tests (planned)
└── fixtures/                             Sample JSONs + CSVs + DataFrames
```

`pytest tests/unit/` is fast (<10s) and runs in CI; integration tests require `make up`.

---

## 14. `validation/`

```
validation/
├── run.sh                                Pre-PR / pre-push / milestone validation harness
├── lib/                                  Shared shell helpers
├── modes/                                pre-push / pre-pr / milestone scripts
├── modules/                              Component-specific validation steps
└── runs/                                 (gitignored) outputs from past runs
```

Triggered via the `/cip-validate` skill or directly with `bash validation/run.sh <mode>`.

---

## 15. `notebooks/`

```
notebooks/
├── exploration/                          Ad-hoc data exploration
├── validation/                           Manual validation drafts (before promoting to validation/)
└── ml/                                   Feature engineering / model dev (deferred)
```

Not part of the production code path; throwaway analyses live here.

---

## 16. `conf/`

```
conf/
├── base/
│   ├── platform.yaml                     Default settings (env-prefix `BASE_`)
│   ├── spark.yaml                        Spark / Iceberg JAR versions
│   └── duckdb.yaml                       DuckDB paths and file lock policies
├── dev/                                  Dev overrides (gitignored except `*.example.yaml`)
└── prod/                                 Prod overrides (gitignored)
```

Resolution order: env vars > `.env` > `conf/{env_name}/*.yaml` > `conf/base/*.yaml` > Pydantic defaults.

---

## 17. `local/`

```
local/
├── airflow-logs/                         (gitignored)
├── cricsheet_downloads/                  (gitignored) — manual one-off downloads
├── sample_matches/                       Sample JSONs for tests
├── planning_docs/                        AI planning session outputs (PDFs) — archival
├── support_docs/                         Sample CSVs, manifests
└── scratch/                              Experiment scratch
```

Everything under `local/` is for personal/experimental work; not promoted to `main` without intent.

---

## 18. Where things go (decision flowchart)

- **New Iceberg table** → add to `TableName.{BRONZE,SILVER,GOLD}_TABLES` in `src/cip/common/contracts/naming.py`, then create the writer/transform; never reference the FQN as a raw string.
- **New DAG** → reserve the id in `DagNames`, create file under `orchestration/airflow/dags/`, write the callable in `src/cip/ingestion/jobs/`.
- **New control table** → add DDL to `infra/bootstrap/init-metastore.sql`, then `make bootstrap` to apply.
- **New dbt model** → choose layer (staging / marts/{dimensions,facts,aggregates} / semantic / metrics), register in `sources.yml` if it consumes a new source, declare exposures consuming it in `exposures.yml`.
- **New Soda check** → drop a YAML under `quality/soda/checks/`, register in `quality/soda/configuration.yml`.
- **New ADR** → copy `docs/adr/adr-template.md` → `0XXX-<slug>.md`, fill it in, add to `docs/adr/README.md` index.
- **New runbook** → drop in `docs/runbooks/` (style: see `docs/runbooks/full-rebuild.md` for the format we use).
- **New AI tool** → add a module under `src/cip/serving/ai/tools/`, register in `agent.py`, write a system prompt under `prompt_registry/tool_<name>.md`, add at least one golden eval question under `apps/ai-studio/evaluation/eval_questions.yml`.
- **New script** → drop in `scripts/` if used more than once; if used 3+ times, add a `make` target.

---

## 19. Things that look like they belong elsewhere

- **`docker-compose.yml`** at the repo root is intentionally empty (it's a shim that includes `infra/compose/*`).
- **`requirements.txt`** at the repo root exists for non-Poetry environments (CI in some workflows). Poetry is the source of truth.
- **`apps/fastapi`** is a symlink to `src/cip/serving/api` — kept so `apps/` is the unified deployable-unit directory.
- **`gemini-graphify-out/`** parallels `graphify-out/` for Gemini's variant of the knowledge graph.
- **`spark-warehouse/`** is Spark's local default warehouse directory — gitignored.
- **`hs_err_pid*.log` + `replay_pid*.log`** are JVM crash artefacts from past Spark crashes — safe to delete (gitignored).
