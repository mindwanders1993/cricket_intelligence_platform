# Architecture Decision Records (ADRs)

Every significant architectural choice in the Cricket Intelligence Platform is captured as an ADR using a [MADR-lite](https://adr.github.io/madr/) style. ADRs document the **reasoning** behind a choice so future contributors (and the author) understand *why* something is the way it is, not just *what* it is.

---

## When to write an ADR

Write an ADR when:

- A choice affects more than one module / package.
- A choice has multiple reasonable alternatives.
- A reviewer is likely to ask "why did you choose X over Y?".
- A choice is hard to reverse (e.g., changing the table format, migrating a primary OLAP engine).
- The choice has a known migration path or future swap candidate.

Skip the ADR for:

- Pure refactors with no external behaviour change.
- Library version bumps unless they change semantics.
- Style / formatting decisions (those go in `CLAUDE.md` / `pyproject.toml`).
- Things already documented in a runbook with no architectural component.

---

## How to write an ADR

1. Copy `docs/adr/adr-template.md` → `docs/adr/0XXX-<kebab-slug>.md`.
2. Pick the next free number; don't reuse retired numbers.
3. Fill in every section. Empty sections defeat the point.
4. Add a one-line entry to the index below.
5. Status starts as **Proposed**; flips to **Accepted** once merged.
6. If superseded, set status to **Superseded by 00YY** and link to the new ADR.

---

## ADR index

| # | Title | Status | Sprint |
|---|---|---|---|
| 0001 | [Use modular monolith](0001-use-modular-monolith.md) | Accepted | Phase 1 / Sprint 0 (docs) |
| 0002 | [Use Apache Iceberg](0002-use-apache-iceberg.md) | Accepted | Phase 1 / Sprint 0 (docs) |
| 0003 | [Use Airflow for orchestration](0003-use-airflow-for-orchestration.md) | Accepted | Phase 1 / Sprint 0 (docs) |
| 0004 | [Open standards first](0004-open-standards-first.md) | Accepted | Sprint 0 (revamp v2 founding principle) |
| 0005 | [BigQuery as secondary OLAP target](0005-bigquery-as-secondary-target.md) | Proposed | Sprint 3 |
| 0006 | [MetricFlow as semantic layer](0006-metricflow-as-semantic-layer.md) | Proposed | Sprint 0 / 1 |
| 0007 | [FastAPI as gateway](0007-fastapi-gateway-design.md) | Proposed | Sprint 1 |
| 0008 | [SQL guardrails for chat-issued queries](0008-sql-guardrails.md) | Proposed | Sprint 1 / 2 |
| 0009 | [LangGraph agent design (tools-first)](0009-langgraph-agent-design.md) | Proposed | Sprint 2 |
| 0010 | [Scale strategy (100M-row synthetic + dual-engine perf)](0010-scale-strategy.md) | Proposed | Sprint 4 |

---

## Conventions

- **Filenames:** `0XXX-<kebab-slug>.md` (zero-padded 4-digit number).
- **One decision per ADR.** If you find yourself writing about two choices, split it.
- **Link both ways.** When ADR B builds on ADR A, both should reference each other.
- **No retroactive rewrites.** When an ADR is superseded, the original stays — add a "Superseded by NNNN" line at the top.
- **No deletions.** ADRs are immutable history.
- **Date-of-decision** is recorded once. Don't update it on edits.

---

## Reference

- [MADR (Markdown Any Decision Records)](https://adr.github.io/madr/)
- [Michael Nygard's original ADR proposal](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
- Template: [`adr-template.md`](adr-template.md)
