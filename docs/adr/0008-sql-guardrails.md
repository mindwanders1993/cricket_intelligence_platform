# 0008 — SQL guardrails for chat-issued queries

- **Status:** Proposed (filled during Sprint 1 / 2)
- **Date of decision:** TBD (Sprint 1 + 2 of revamp v2)
- **Tags:** security, api, ai, sprint-1, sprint-2

---

## Context (placeholder)

The FastAPI `/query` endpoint supports a `{sql}` payload for ad-hoc reads. The AI agent also calls `/query` via tools. We must guarantee that:
1. No destructive SQL ever executes (DROP, DELETE, UPDATE, ATTACH, CREATE outside `cricket.tmp_*`).
2. Chat-issued queries reach only the semantic-layer-exposed schemas.
3. SQL injection via `lookup_player` / other tools is impossible.

This ADR documents the AST-walking guardrail strategy.

To be filled during Sprint 1+2 with:
- Walker library choice (sqlglot, sqloxide, custom)
- Blocklist vs allowlist (we choose **AST allowlist** for chat; blocklist for authenticated ad-hoc reads)
- How the guardrail composes with MetricFlow (MetricFlow-generated SQL is trusted; raw SQL is gated)
- Audit logging (every reject is logged + telemetered)

See `docs/planning.md` → Sprint 1 (gateway) + Sprint 2 (agent).

---

## Decision drivers (placeholder)

- Agent must have no destructive-SQL path (hard requirement)
- Auditability — every rejected query logged + telemetered
- Performance — AST walk must be sub-millisecond
- Open-standards: sqlglot is permissive AST library used widely

---

## References

- `docs/planning.md` → Sprint 1 (sql_guardrails.py)
- Related: [[0007-fastapi-gateway-design]], [[0009-langgraph-agent-design]]
