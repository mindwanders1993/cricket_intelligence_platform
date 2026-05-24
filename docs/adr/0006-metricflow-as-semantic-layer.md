# 0006 — Use dbt MetricFlow as the semantic layer

- **Status:** Proposed (filled during Sprint 0 / 1)
- **Date of decision:** TBD (Sprints 0–1 of revamp v2)
- **Tags:** modeling, semantic-layer, bi, sprint-0, sprint-1

---

## Context (placeholder)

The platform has multiple consumers — Metabase, Lightdash, FastAPI / AI assistant, Observable Framework — and a metric like `batting_average` must mean the same thing in every consumer. Without a semantic layer, each BI tool re-implements the SQL and definitions drift.

This ADR documents the choice of **dbt MetricFlow** as the single source of truth for metric definitions.

To be filled during Sprint 0/1 with:
- MetricFlow vs cube.dev vs LookML (translation) vs custom JSON specs
- Why dbt-native wins for a dbt-heavy project
- How Lightdash + FastAPI + agent all consume the same definitions
- Drift detection (Metabase SQL should match MetricFlow output)

See `docs/planning.md` → Sprint 0 (semantic models + metrics) and Sprint 1 (FastAPI MetricFlow client).

---

## Decision drivers (placeholder)

- Same metric definitions across Metabase, Lightdash, FastAPI, AI agent
- dbt project already exists → MetricFlow is in-house dialect
- ADR 0004 open-standards — MetricFlow is a dbt-foundation spec; multiple BI tools consume it

---

## References

- `docs/planning.md` → Sprint 0 (models/dbt/models/semantic_models/, models/dbt/models/metrics/)
- Related: [[0007-fastapi-gateway-design]], [[0009-langgraph-agent-design]]
