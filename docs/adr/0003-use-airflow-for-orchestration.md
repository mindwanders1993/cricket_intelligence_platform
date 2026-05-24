# 0003 — Use Apache Airflow for orchestration

- **Status:** Accepted
- **Date of decision:** 2026-02 (Phase 1)
- **Deciders:** Biswajit Brahmma
- **Tags:** orchestration, scheduling, sprint-0-docs

---

## Context

The platform runs batch-shaped workloads (Bronze, Silver, Gold) on weekly, daily, and ad-hoc schedules. Dependencies between layers (Bronze must finish before Silver; Silver before Gold) must be enforced. Operators (the author) need a UI to inspect run history, drill into failures, and trigger re-runs with config overrides.

Choices in 2026:
- **Apache Airflow** — the de-facto standard.
- **Dagster** — software-defined-asset framework with strong typing.
- **Prefect** — Python-native, dynamic DAGs.
- **Temporal** — durable execution platform (overkill for batch but popular).
- **Plain cron** — the floor.

---

## Decision drivers

- **Maturity + reliability** — must not lose schedules; battle-tested at scale.
- **AWS migration path** — must have a managed cloud equivalent (MWAA).
- **UI + observability built in** — Grid view, task logs, retry visibility, config-driven re-runs.
- **Auto-trigger chains** — Bronze → Silver → Gold must compose; `TriggerDagRunOperator` (Airflow) or equivalent must exist.
- **Python-native DAGs** — task callables are Python; no JVM/YAML DSL.
- **Community size** — when something breaks, Stack Overflow / GitHub has been there.
- **Interview signal** — Airflow on the resume is broadly recognized.

---

## Considered options

### Option A — Apache Airflow

The default. v2.x ships LocalExecutor / KubernetesExecutor / CeleryExecutor. DAGs are Python files. UI at port 8080. Managed cloud: AWS MWAA, GCP Composer, Astronomer.

- **Pros:** maturity (Airbnb 2014 → ASF 2016); massive ecosystem (~thousand providers); managed AWS path (MWAA); excellent UI; `TriggerDagRunOperator` composes DAGs; KubernetesExecutor is the production migration target; community knowledge dwarfs alternatives.
- **Cons:** scheduler can be heavy (custom Airflow image required, ~1GB); metadata DB needed (PostgreSQL); DAG-import errors are easy to introduce; XCom serialization is finicky (we already limit to JSON primitives via memory `project_big_task3_status`).

### Option B — Dagster

Modern Python-first orchestrator built around "software-defined assets". Strong type system, observability built in, decent UI.

- **Pros:** clean asset model that aligns with medallion; native lineage emission; type system catches errors at definition; modern dev UX; OpenLineage support; dbt integration is excellent.
- **Cons:** smaller community than Airflow; managed cloud (Dagster Cloud) is the only viable production path — no AWS-managed equivalent; rewriting the existing 8 DAGs would be Sprint-sized work; less interview brand-recognition (changing).

### Option C — Prefect

Python-native, dynamic flows. Cloud-first.

- **Pros:** Python-idiomatic; dynamic task generation; modern UX.
- **Cons:** Prefect Cloud is the canonical deploy; self-hosted is second-class; community smaller than Airflow + Dagster; weak AWS-managed story.

### Option D — Temporal

Durable execution / workflow engine. Strong at long-running stateful workflows.

- **Pros:** durable; great for state machines; strong typing.
- **Cons:** overkill for batch ETL; not the conventional shape for data pipelines; learning curve; SDK-first vs DAG-as-Python-file.

### Option E — Plain cron + bash

Just shell scripts triggered by cron.

- **Pros:** zero overhead.
- **Cons:** no DAG visualization; no retry semantics; no XCom-equivalent; no UI; no auto-trigger chains; not interview-defensible for senior DE.

---

## Decision

We will use **Option A — Apache Airflow** with LocalExecutor in dev (Compose) and KubernetesExecutor on cloud (MWAA / EKS). DAGs live in `orchestration/airflow/dags/` and wrap callables in `src/cip/ingestion/jobs/`. Auto-trigger chains use `TriggerDagRunOperator` with `wait_for_completion=False`.

---

## Consequences

### Positive

- The orchestration story is the boring, correct one — no surprise battles with the scheduler.
- MWAA is a straight lift on AWS — same DAGs, same callables, just a managed Airflow tier.
- The Grid view + log drill-down is exactly what's needed for solo operation.
- `TriggerDagRunOperator` chains Bronze → Silver → Gold cleanly; each DAG owns its own success state independently.
- Existing project memory documents the gotchas (XCom JSON primitives only, KubernetesExecutor maps to EMR-on-EKS later).
- OpenLineage integration via `openlineage-airflow` is one-line wiring (Sprint 0).

### Negative / trade-offs

- The custom Airflow image is a build-once-after-Dockerfile-changes burden — managed via `make build-airflow`.
- Scheduler tuning matters at scale; for v1 single-developer scale this is invisible.
- Migrating to Dagster later would be a meaningful lift if the asset-first model becomes more attractive.
- XCom serialization limits us to JSON primitives; not a real constraint in practice.

### Neutral

- DAG files are kept thin (just import the callable, define ops). Logic lives in `src/cip/ingestion/jobs/`. Same callable is invokable via `python -m cip.ingestion.jobs.<module>` for non-Airflow local runs.

---

## Migration path / future swap

If we move off Airflow:
- **To Dagster** — re-implement DAGs as asset graphs; reuse all Python callables verbatim (Dagster's `@op` wraps existing Python). Cost: Sprint-sized.
- **To Temporal** — much heavier rewrite; only worth it if state-machine semantics dominate (they don't, for batch ETL).
- **To Prefect** — similar shape to Airflow; not enough advantage to justify the swap.

Trigger to revisit: scheduler instability at scale (not a v1 concern) OR asset-graph-first orchestration becomes industry default (watch the next 2–3 years).

---

## References

- Code: `orchestration/airflow/dags/*.py`, `src/cip/ingestion/jobs/*.py`
- Custom image: `infra/docker/airflow/Dockerfile`
- Related: [[0001-use-modular-monolith]] (DAGs wrap callables in the monolith); [[0004-open-standards-first]] (Airflow's Python DAG file format is itself an open de-facto standard, and MWAA / Composer / Astronomer all consume it); [[0005-observability-stack]] (openlineage-airflow + opentelemetry-instrumentation-airflow)
- External: [Airflow docs](https://airflow.apache.org), [MWAA docs](https://docs.aws.amazon.com/mwaa/), Dagster comparison: airflow vs dagster blog series
