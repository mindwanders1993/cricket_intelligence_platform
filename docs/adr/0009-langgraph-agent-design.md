# 0009 — LangGraph agent design (tools-first, not RAG-first)

- **Status:** Proposed (filled during Sprint 2)
- **Date of decision:** TBD (Sprint 2 of revamp v2)
- **Tags:** ai, agent, sprint-2

---

## Context (placeholder)

The AI assistant answers natural-language questions about cricket data. Two designs:

- **Tools-first** — the LLM picks from a small set of tools (`search_metrics`, `query_metric`, `lookup_player`, …) and orchestrates them. Data comes from MetricFlow + DuckDB.
- **RAG-first** — embed everything (table schemas, sample rows, docs) and retrieve relevant chunks before the LLM answers.

For structured analytical data, **tools-first is strictly better**: structured queries return ground-truth numbers; RAG over structured data returns approximate text matches.

To be filled during Sprint 2 with:
- LangGraph state machine design
- 6-tool inventory (`search_metrics`, `get_metric_definition`, `query_metric`, `lookup_player`, `explain_table`, `generate_chart_spec`)
- Light RAG (dbt-docs embeddings only — for "what does this table mean" questions)
- Ollama default + Bedrock fallback (via `AISettings.llm_provider`)
- Eval harness — 30 golden questions, ≥80% pass rate

See `docs/planning.md` → Sprint 2.

---

## Decision drivers (placeholder)

- Ground-truth answers required (no hallucinated numbers)
- Must be defensible in interviews ("why didn't you just use RAG?")
- LangGraph state machine over LangChain agents (more deterministic)
- Local Ollama + cloud Bedrock without code change

---

## References

- `docs/planning.md` → Sprint 2
- Related: [[0006-metricflow-as-semantic-layer]], [[0007-fastapi-gateway-design]], [[0008-sql-guardrails]]
