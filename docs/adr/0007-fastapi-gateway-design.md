# 0007 — FastAPI as the gateway for runtime consumers

- **Status:** Proposed (filled during Sprint 1)
- **Date of decision:** TBD (Sprint 1 of revamp v2)
- **Tags:** api, serving, sprint-1

---

## Context (placeholder)

Consumers that need runtime data access — the AI assistant, ad-hoc tools, future external integrations — need a single entry point that wraps:
- DuckDB connection pool (read-only)
- MetricFlow client (semantic-layer resolution)
- SQL guardrails (AST walker)
- Streaming chat (SSE)

The dashboard (Observable) and BI tools (Metabase, Lightdash) read DuckDB direct — they don't need the gateway. The gateway is specifically for **runtime / programmatic / agentic** access.

To be filled during Sprint 1 with:
- FastAPI vs Flask vs Litestar vs Django REST Framework
- Why FastAPI (OpenAPI built-in, async, Pydantic native)
- Endpoint shape: `/health`, `/metrics`, `/query`, `/explain`, `/catalog/*`, `/chat`
- Connection-pool semantics
- OTEL middleware

See `docs/planning.md` → Sprint 1.

---

## Decision drivers (placeholder)

- OpenAPI must be auto-generated (open-standards-first)
- Type-safe request/response (Pydantic)
- Async-native (SSE streaming for /chat)
- Mature Python ecosystem

---

## References

- `docs/planning.md` → Sprint 1
- Related: [[0001-use-modular-monolith]], [[0006-metricflow-as-semantic-layer]], [[0008-sql-guardrails]]
