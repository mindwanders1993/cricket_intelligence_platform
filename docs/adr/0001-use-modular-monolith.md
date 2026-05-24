# 0001 — Use a modular monolith for the Python platform

- **Status:** Accepted
- **Date of decision:** 2026-02 (Phase 1)
- **Deciders:** Biswajit Brahmma
- **Tags:** architecture, packaging, sprint-0-docs

---

## Context

The platform spans ingestion (Polars + PyIceberg), heavy transformation (PySpark), modeling (dbt), serving (DuckDB + future FastAPI + AI assistant), data quality, observability, and ML. Each concern is a distinct workload with distinct dependencies — but they all share types (settings, enums, naming contracts), conventions (metadata columns, idempotency patterns, audit-row writes), and a single operator (the author, working part-time).

The repo has to be one of:
1. A **modular monolith**: one Python package (`src/cip/`) with internal sub-packages by concern, shipped as a single installable.
2. **Multiple microservice repos**: separate Python projects per concern (ingestion, transform, serving, AI), each with its own deploy unit.
3. **Notebook-first**: ad-hoc scripts and notebooks with no platform layer.

---

## Decision drivers

- **Solo developer / part-time bandwidth** — one person can't maintain N service boundaries.
- **Shared types must stay in sync** — `META.*`, `TableName`, `PlatformSettings` are referenced from ingestion, transform, quality, and serving; drift would silently break the contract graph.
- **Workloads are batch-shaped** — no service-to-service hot paths where process isolation buys anything (no per-request latency budgets, no per-tenant scaling).
- **AWS migration path** — the cloud target is EMR Serverless + MWAA + ECS, not 10 microservices on EKS. A single deployable maps naturally.
- **Testability** — unit tests must run without Docker. Microservice boundaries would force integration tests for what is logically a function call.
- **Refactor cost** — splitting a monolith later is cheap; merging microservices back is expensive. Optionality is one-directional.

---

## Considered options

### Option A — Modular monolith (`src/cip/<sub-package>`)

One Python package with clear internal boundaries: `common/`, `ingestion/`, `transform/`, `serving/`, `quality/`, `observability/`, `ml/`. Each sub-package owns its own modules but freely imports shared types from `common/`. Single `pyproject.toml`, single Poetry env, single Docker image (for Airflow). Internal interfaces are normal Python imports; no RPC, no message bus.

- **Pros:** trivial type sharing; refactors stay within the package; one test suite; one dependency manifest; aligns with AWS deployment shape; existing community pattern (Netflix Genie, Lyft Flyte, many dbt projects).
- **Cons:** the entire process gets the union of all dependencies (Spark + Polars + dbt + LangChain + FastAPI = ~1GB image); a single bug in one module can crash all others if not isolated by task boundaries; `src/cip/` will grow large.

### Option B — Microservices (per-concern repos)

`cip-ingestion`, `cip-transform`, `cip-serving`, `cip-ai`, `cip-quality`, `cip-platform` (shared). Each repo has its own deploy unit. Inter-service contracts are HTTP/gRPC or a message bus.

- **Pros:** clear blast radius per service; smaller deploy units; independent dependency upgrades; clean cloud-native shape.
- **Cons:** shared-types repo creates a dependency cycle if any service needs to evolve a contract; sync between repos becomes the new bottleneck; CI matrix multiplies; the author has to maintain N CI / lint / release pipelines; integration tests must be cross-repo.

### Option C — Notebook-first / scripts

Just `.py` scripts and Jupyter notebooks, no package structure. Run via `python -m` or notebook cells.

- **Pros:** zero packaging overhead; fast to iterate.
- **Cons:** no shared types; no enforceable contracts; no unit tests; no IDE navigation; can't be productionized; not interview-defensible for a senior DE portfolio.

---

## Decision

We will use **Option A — modular monolith** under `src/cip/`. Sub-packages by concern; shared types in `common/`; one Poetry env, one Docker image (the custom Airflow image bakes the same code).

---

## Consequences

### Positive

- One repo, one dependency graph, one test suite.
- `META`, `TableName`, `PathBuilder`, `PlatformSettings` are imports, not RPC payloads.
- Refactor velocity stays high — module renames and signature changes are local.
- The "platform layer + jobs layer" pattern (`src/cip/...` + `src/cip/ingestion/jobs/`) maps directly onto Airflow's expected shape: thin DAG files wrapping `python -m` callables.
- The build artifact is one Docker image; deploys are atomic.

### Negative / trade-offs

- The image is large (~1GB) because Spark + Polars + PyIceberg + dbt + LangChain all need to be installed. Mitigated by the custom Airflow image baking JARs and the v1 deploy target being managed services rather than per-pod images.
- A bad dependency upgrade affects every job. Mitigated by Poetry lock file + CI gate on `pre-commit run --all-files`.
- Module boundaries are convention-only — there's no compile-time enforcement preventing `serving/` from reaching into `ingestion/internals/`. Mitigated by `CLAUDE.md` and code review.

### Neutral

- Future split is possible if a specific module (e.g., the AI gateway) develops independent scaling needs — extract its sub-package into its own service. Until then, internal imports are simpler than RPC.

---

## Migration path / future swap

If a specific concern becomes load-bearing for production traffic (e.g., the FastAPI gateway needs to scale separately on EKS while the Airflow batch tier stays on MWAA), the relevant sub-package can be lifted into its own repo + Docker image with no semantic change — sub-packages already have clear interfaces. The shared `common/` package would become a published library consumed by both.

Trigger to revisit: more than one production deploy target with different scaling envelopes.

---

## References

- Code path: `src/cip/`
- Related: [[0003-use-airflow-for-orchestration]] (Airflow DAGs wrap the modular monolith); [[0004-open-standards-first]] (modular monolith doesn't violate open-standards — it's a packaging choice, not a protocol choice)
- External: Lyft Flyte, Netflix Metaflow — both modular monoliths at much larger scale
