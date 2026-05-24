# 0004 — Open standards first (OSS now, enterprise-swappable later)

- **Status:** Accepted
- **Date of decision:** 2026-05-24 (Sprint 0, revamp v2 founding principle)
- **Deciders:** Biswajit Brahmma
- **Tags:** architecture, principles, sprint-0, revamp-v2

---

## Context

The platform is built locally on open-source tools (MinIO, Iceberg REST, Spark, Polars, dbt-core, DuckDB, Metabase, …) and the intent is to deploy it to AWS later (S3, Glue, EMR, MWAA, Athena, …). The revamp v2 plan adds many new components — observability, semantic layer, agentic AI, secondary OLAP target, second BI surface — and each adds a choice between:

1. Pick an OSS tool that speaks an **open protocol**, with a managed enterprise cousin that consumes the same protocol.
2. Pick an OSS tool that's vendor-controlled or doesn't speak an open protocol (locks you in to that one tool).
3. Pick the managed enterprise tool directly (defeats the local-dev story).

Without an explicit principle, decisions drift toward whatever was hot last quarter, and the "AWS migration story" becomes hand-waving in interviews.

---

## Decision drivers

- **Cloud migration story must be config-only**, not a rewrite. Endpoint env var changes; no code changes.
- **No vendor lock-in** during the OSS phase. Every component must be replaceable behind its open protocol.
- **Interview narrative coherence.** A platform whose components were chosen by their migration story is a much stronger pitch than a platform that happens to be open source.
- **Don't make decisions twice.** If the principle is explicit, every new component decision becomes a yes/no against the criterion, not a fresh debate.
- **Resume defensibility.** Every component should map to one line on a JD ("AWS Glue compatible", "OpenLineage", "OpenTelemetry", "JDBC/ODBC").

---

## Considered options

### Option A — Open standards first (this ADR)

Every external dependency must speak an **open protocol** with a managed enterprise cousin and a binding standard. Endpoint config is the only delta between local and cloud.

Concretely:

| Concern | Constraint |
|---|---|
| Storage | S3 API (covers MinIO, S3, GCS via gcsfuse, MinIO Gateway, Azure Blob via SAS) |
| Table format | Iceberg spec or Delta spec (Iceberg chosen — see ADR 0002) |
| Catalog | Iceberg REST protocol (works for `iceberg-rest`, Glue, Unity, Nessie) |
| Compute | Spark API or Polars (portable runtime) |
| OLAP | ANSI SQL + JDBC/ODBC (covers DuckDB, BigQuery, Snowflake, Athena, ClickHouse) |
| Transform | dbt manifest format (works for dbt Cloud, SQLMesh adapters, vendor BI tools) |
| Orchestration | Python DAG-as-code (works for Airflow, MWAA, Astronomer, Dagster, Prefect adapters) |
| Lineage | OpenLineage spec (works for Marquez, DataHub, Atlan, Unity Catalog) |
| Telemetry | OpenTelemetry spec (works for Prometheus, Grafana, Datadog, New Relic, Honeycomb) |
| BI | dbt semantic layer / MetricFlow (works for Lightdash, Looker, Tableau, Power BI) |
| DQ | Soda Checks Language (works for Soda Core OSS, Soda Cloud, Monte Carlo) |
| API | OpenAPI (works everywhere) |
| LLM | OpenAI-compatible API (works for Ollama, vLLM, Bedrock via gateway, OpenAI, Anthropic, Gemini) |
| IaC | Terraform HCL (works for AWS, GCP, Azure) |

A tool is **rejected** if it doesn't speak an open standard from the matching row of the table, even if it's otherwise the best choice.

### Option B — Pragmatic / case-by-case

Each component decision is made on its merits, with no overarching principle. Choose Datadog if it's cheaper than running Grafana yourself; choose Snowflake-only features if they're more powerful; choose a proprietary lineage tool if it's nicer.

- **Pros:** lets you use the best-in-class option for each concern.
- **Cons:** the "AWS migration is config" narrative falls apart; vendor sprawl; harder to articulate the architecture as one story; each new decision is an unbounded debate.

### Option C — Cloud-managed first

Skip the OSS local-dev story entirely. Use AWS S3 + Glue + Athena from day one. Local dev is "connect to a personal AWS account".

- **Pros:** less infra to run locally; cloud-native shape from day one.
- **Cons:** requires a funded AWS account and ongoing spend; defeats the "develop local, deploy cloud" principle; loses the open-source portfolio narrative; AWS only.

---

## Decision

We will adopt **Option A — Open standards first** as the founding principle for revamp v2 and all subsequent decisions.

Every new dependency added to the platform must pass the **standards check**:

1. What open protocol does this tool speak?
2. What's the managed cloud equivalent that consumes the same protocol?
3. Is the endpoint swap the **only** code-level change needed to migrate?

If any answer is unclear or "none", we either pick a different tool or write an ADR explicitly waiving this principle for that one decision (and we re-evaluate when the trigger event occurs).

---

## Consequences

### Positive

- The architecture has a single coherent story: "I chose tools by their migration story."
- Every component swap (Marquez → DataHub, Prometheus → Datadog, Lightdash → Looker, MinIO → S3, etc.) is a documented endpoint change with no business-logic touch.
- Interview answers are crisp: when asked "why X?", the answer is always "X speaks <protocol> which lets us migrate to <enterprise tool> later".
- New-component decisions are fast — the standards check is a 30-second evaluation.
- The OSS↔Enterprise mapping table in `docs/architecture/hld-hla.md` doubles as the resume narrative.

### Negative / trade-offs

- Sometimes the best-in-class tool isn't standards-compliant and gets rejected. Example: some niche vector stores have great features but no open protocol — we pick Qdrant (HTTP/gRPC, OpenAI-compatible embeddings) instead.
- Performance occasionally loses to portability. Example: a vendor-specific BigQuery feature that has no ANSI SQL equivalent gets avoided in dbt models even if it'd be faster.
- The principle adds friction to "let me just try this cool new tool" decisions. Friction is the point.

### Neutral

- The principle applies to **architectural** components, not implementation details. Choosing `polars` vs `pandas` is not a principle decision — both are open-source Python libraries. Choosing `MetricFlow` vs `cube.dev` is — semantic layer protocol matters for which BI tools can consume it.

---

## Standards check — worked examples

### Adding observability (Sprint 0)

| Question | Answer |
|---|---|
| What open protocol? | OpenLineage (lineage), OpenTelemetry (metrics/traces) |
| Managed cousin? | DataHub / Atlan (lineage); Datadog / New Relic / Honeycomb (telemetry) |
| Endpoint swap = only change? | ✅ — change `OPENLINEAGE_URL` from `http://marquez:5002` to a managed endpoint |

→ **Pass.** Use Marquez + OTEL Collector + Prometheus + Grafana + Tempo.

### Adding a semantic layer (Sprint 0)

| Question | Answer |
|---|---|
| What open protocol? | dbt semantic layer / MetricFlow definitions |
| Managed cousin? | dbt Cloud Semantic Layer, Looker LookML (via translation), cube.dev |
| Endpoint swap = only change? | ✅ — same `.yml` files consumed |

→ **Pass.** Use dbt MetricFlow.

### Adding a vector store (Sprint 2)

| Question | Answer |
|---|---|
| What open protocol? | None canonical, but Qdrant HTTP/gRPC API is open + replicable |
| Managed cousin? | pgvector (Postgres extension), AWS OpenSearch (k-NN plugin), Pinecone (proprietary but adapter-friendly) |
| Endpoint swap = only change? | ⚠️ — embedding model + vector format consistent; HTTP client swap needed |

→ **Pass with documented swap path.** Use Qdrant. AWS migration uses OpenSearch with a `VectorStoreClient` interface that abstracts both.

### Hypothetical: Adding a proprietary BI tool

| Question | Answer |
|---|---|
| What open protocol? | None — proprietary connectors only |
| Managed cousin? | Same vendor (Looker/Tableau/PowerBI) — not OSS local-dev cousin |
| Endpoint swap = only change? | ❌ — would have to rewrite dashboards |

→ **Reject.** Use Metabase + Lightdash instead.

---

## Migration path / future swap

This principle is **load-bearing for the whole platform**. Reversing it would mean accepting vendor lock-in component-by-component. The trigger to revisit is unlikely — it would be something like "OSS lineage tooling has stagnated to the point where the standard-binding case no longer holds". This is not in sight as of 2026.

If a single ADR needs to waive the principle for a specific component (e.g., a hypothetical Sprint 5+ where we pick a proprietary vector store for genuine technical reasons), the waiver lives in that ADR with explicit reasoning.

---

## References

- The OSS ↔ enterprise mapping table: `docs/architecture/hld-hla.md` §5
- README §23 — the same mapping for outside readers
- Related: every revamp-v2 component ADR (0005–0010) cites this one
- External: [Software Architecture Principles](https://docs.aws.amazon.com/wellarchitected/latest/framework/architecture.html), [CNCF graduated projects](https://www.cncf.io/projects/) (the rough universe of open-protocol tooling)
