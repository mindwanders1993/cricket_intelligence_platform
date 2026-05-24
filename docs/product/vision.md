# Product Vision — Cricket Intelligence Platform

## What this is

The Cricket Intelligence Platform is a **portfolio-grade open-source data platform** that turns public Cricsheet match data into a trusted analytical product. It is deliberately built to look and behave like a modern product-company data platform: medallion lakehouse on Apache Iceberg, dimensional modeling via dbt, semantic-layer-driven BI, observability, data quality, governed AI assistant, and cloud-portable infrastructure.

It is **not** a hobby analytics project. Every architectural choice is interview-defensible, cloud-migration-ready, and grounded in open standards so any single OSS component has a managed enterprise cousin a single config-change away.

---

## Why this exists (the goal behind the goal)

The underlying purpose is **portfolio building for Senior Data Engineer roles** — Harness CCM specifically, and the broader market for data-platform / FinOps / analytics-engineering / agentic-AI-on-data positions. The cricket dataset is the canvas; the demonstrable skills are the product:

- Iceberg lakehouse at non-trivial scale
- dbt with SCD2, incremental, semantic layer, exposures
- Multi-target dbt (DuckDB local + BigQuery cloud)
- OpenLineage + OpenTelemetry instrumentation
- FastAPI gateway with OpenAPI + SQL guardrails
- Agentic LLM assistant grounded in a semantic layer
- FinOps cost telemetry as a first-class metric on the platform itself
- Open-standards-first architecture with a Terraform AWS path

Each of these maps directly to lines on the Harness JD and adjacent Senior DE roles.

---

## Who it's for

| Persona | What they care about | Where they engage |
|---|---|---|
| **Senior DE hiring panel** | Architecture rigour, defensible choices, cloud portability, real DQ + observability | README, ADRs, demo video, live interview walkthrough |
| **Recruiter / engineering manager** | "Can this person ship a portfolio system end-to-end?" | Top-level README, player portfolio dashboard, demo video |
| **Tech lead reviewing the code** | Tests, contracts, idempotency, clean Python | `src/cip/`, `tests/`, ADRs |
| **The author (Biswajit)** | Re-learn unfamiliar tools (Iceberg, MetricFlow, OTEL, LangGraph) by building, not reading | Daily work in the repo |
| **Future contributors / agents** | A docs-first project they can pick up cold | `CLAUDE.md`, `GEMINI.md`, `docs/runbooks/`, project skills |

---

## Value proposition

> *"I built an Iceberg lakehouse + semantic layer + agentic AI + FinOps cost mart end-to-end, on open standards, with a Terraform path to AWS."*

That single sentence unlocks every conversation:
- "An Iceberg lakehouse" → 2 phases of deep ingestion + transform work
- "+ semantic layer" → dbt MetricFlow + dual BI surfaces proving portability
- "+ agentic AI" → LangGraph + SQL guardrails + grounded-not-generative design
- "+ FinOps cost mart" → directly explains Harness's Cloud Cost Management product
- "On open standards" → no vendor lock-in, cloud-migration is config
- "With a Terraform path to AWS" → IaC + cloud-readiness without burning cloud spend

---

## Non-goals

What this product is **not** trying to be:

- **Not a real-time scoring platform.** Cricsheet publishes post-match JSONs; building a fake live feed is a synthetic demo that interviewers correctly discount. Re-open streaming only if a streaming-shaped JD warrants it.
- **Not a production multi-tenant SaaS.** No auth, no tenancy, no rate-limits in v1.
- **Not a sports prediction product.** ML training and serving are explicitly deferred. The platform supports ML (MLflow is live), but no model ships in v1.
- **Not a Cricsheet-derivative analytics company.** The data domain is the canvas, not the differentiator.
- **Not over-architected.** Modular monolith over microservices. DuckDB over Snowflake. Compose over Kubernetes (until k3d adds breadth).

---

## Success criteria

V1 is successful when:

1. The full stack runs locally with `make up && make obs-up && make api-up && make ai-up && make lightdash-up`.
2. A clean `ingest_all_match_data_bronze` triggers Bronze → Silver → Gold → Lightdash + Metabase + Observable dashboard updates, with full lineage in Marquez, metrics in Grafana, cost events in `mart_pipeline_cost_daily`.
3. `dbt build` runs identically against DuckDB **and** BigQuery, with row-count parity proven on the validation harness.
4. The AI assistant answers a multi-step natural-language question end-to-end through MetricFlow with SQL guardrails enforced, evaluated against a 30-question golden set at ≥80% accuracy.
5. The Observable dashboard renders the Kohli player portfolio with the embedded AI chat working end-to-end.
6. The portfolio README + demo video walk a recruiter through "from raw Cricsheet JSON to AI-answered question" in under 5 minutes.
7. Every architectural choice has an ADR (10 in total) with Decision / Context / Consequences / Alternatives.
8. A Terraform `plan` against AWS S3 + Glue + EMR Serverless + MWAA + Athena emits no errors — proving the cloud-migration narrative isn't hand-waving.

---

## What I'm explicitly betting on

- **Open standards beat best-of-breed for a portfolio piece.** A platform that can swap Marquez → DataHub by changing one URL is more interview-defensible than a Datadog-only proprietary stack.
- **Depth > breadth in one project.** One polished system covering 8–10 senior-DE skills is more memorable than two half-finished ones.
- **The FinOps mart is the Harness-specific killer demo.** Most candidates have "FinOps awareness"; few have built a working pipeline-cost dashboard on their own platform.
- **Grounded AI > naive RAG on structured data.** Text-to-SQL through a semantic layer with guardrails is the right pattern, and it's defensible in a Harness-AI-product interview.
- **The dashboard is the public face.** A working Observable Framework page with a real player portfolio is what someone clicks first on the GitHub repo.

---

## Out of scope (revisit triggers)

| Item | Re-open if … |
|---|---|
| Real streaming (Kafka/Redpanda) | A streaming-first JD targets this candidate |
| Java / Go service | A polyglot JD requires it |
| ML model training + serving | A sports-tech or ML-platform JD opens |
| Production AWS deployment (apply) | An AWS account is funded; until then, Terraform plan is sufficient |
| Multi-tenant SaaS | Project pivots to a real product |
| Kubeflow | MLOps depth becomes a target gap |

---

## References

- Execution plan: `docs/planning.md`
- Target architecture: `docs/architecture/hld-hla.md`
- Founding principle: `docs/adr/0004-open-standards-first.md`
- Scope: `docs/product/scope-v1.md`
- Roadmap: `docs/product/roadmap.md`
