# Roadmap — Cricket Intelligence Platform

> **Pair with** `docs/product/vision.md` (why), `docs/product/scope-v1.md` (what), `docs/planning.md` (canonical execution detail).
> This doc is the **dates-and-themes** view. The canonical task lists live in `docs/planning.md`.

---

## 1. Status overview

| Phase | Theme | Status | Date landed |
|---|---|---|---|
| Phase 1 | Environment + foundation | ✅ Done | 2026-Q1 |
| Phase 2 | Register pipeline (first vertical slice) | ✅ Done | 2026-Q1 |
| Phase 3 | Match ingestion + Silver explosion | ✅ Done | 2026-Q2 |
| Phase 4 | Gold layer + BI | ✅ Done | 2026-Q2 (May) |
| Phase 5+6+7 (executed together as revamp v2) | AI + Cloud + Portfolio hardening | 🔄 In flight | Target: 2026-Q3 |

### Phase 4 → Phase 5+6+7 transition

Phase 4 landed in mid-May 2026 with the Gold layer + dbt + DuckDB + Metabase stack live. The deferred Phases 5–7 from the original `README.md` §20 are now executed together as **revamp v2** because:

1. They share most of the supporting infrastructure (observability spine, semantic layer, FastAPI gateway).
2. The combined story (observability + cost mart + AI agent + cloud parity) is the resume narrative — separated, each phase is a thinner bullet.
3. The dashboard work (Observable Framework, M1–M27) is in parallel and depends on the same Gold layer Phase 4 produced.

---

## 2. Revamp v2 timeline (target)

Total: ~6–8 weeks of focused work. Each sprint independently shippable. Personal scratch plan: `~/.claude/plans/hi-soft-prism.md`.

```
Sprint 0  ─ Sprint 1  ─ Sprint 2  ─ Sprint 3  ─ Sprint 4
 ~2 wks      ~1.5 wks   ~2 wks      ~1 wk       ~1.5 wks
   │           │          │           │           │
   ▼           ▼          ▼           ▼           ▼
Obs + dbt   FastAPI    Agentic    BigQuery    Scale +
depth       + FinOps   AI         + TF        dashboard
            + Lightdash                       + polish
```

### Sprint 0 — Observability + dbt depth foundation
- **Target window:** 2026-05 → 2026-06
- **Goal:** OpenLineage + OTEL instrumented across writers + Airflow + dbt. dbt SCD2, incremental, MetricFlow semantic models. Soda Core baseline. ADRs 0001–0004.
- **Verification gate:** Marquez shows full lineage; Grafana dashboards render; `mf list metrics` ≥5; `soda scan` green; tests pass.
- **Unlocks:** every later sprint.

### Sprint 1 — FastAPI gateway + FinOps mart + Lightdash
- **Target window:** 2026-06
- **Goal:** API gateway with semantic queries + catalog introspection + SQL guardrails. Pipeline cost mart end-to-end. Lightdash platform dashboards.
- **Verification gate:** `/query` returns correct metric rows; `/query` with destructive SQL rejected; FinOps panels populated; same metrics in Metabase + Lightdash.
- **Unlocks:** Sprint 2 (agent calls FastAPI tools).

### Sprint 2 — Agentic AI assistant
- **Target window:** 2026-06 → 2026-07
- **Goal:** LangGraph agent with 6 tools, Chainlit chat UI, golden-set eval ≥80%.
- **Verification gate:** Chat answers multi-step Kohli question end-to-end; destructive SQL refused; OTEL traces visible in Tempo; nightly metadata refresh DAG runs.
- **Unlocks:** Sprint 4 (dashboard embeds chat).

### Sprint 3 — Cloud-ready (BigQuery + Terraform)
- **Target window:** 2026-07
- **Goal:** Same dbt models build on BigQuery free tier; Terraform module for BQ (apply) + AWS (plan-only).
- **Verification gate:** `dbt build --target bq_dev` clean; row-count parity DuckDB vs BQ on all Gold tables; `terraform plan -no-color` for AWS module clean.
- **Unlocks:** cloud-migration story for interviews.

### Sprint 4 — Scale + dashboard + portfolio polish
- **Target window:** 2026-07 → 2026-08
- **Goal:** 100M-row synthetic dataset + perf write-up. Observable dashboard M3–M22 (player portfolio + embedded chat). Demo video + README rewrite.
- **Verification gate:** Synth dataset built; documented partition-pruning + clustering wins; dashboard renders M21; embedded chat works; ADRs 0005–0010 complete.
- **Unlocks:** application kickoff.

---

## 3. Decision points along the way

These are the points where the roadmap can branch based on feedback / job-search signals.

| After … | If … | Then … |
|---|---|---|
| Sprint 0 ships | Harness-only push needed before Sprint 1 lands | Defer Sprint 2 + 3, prioritize Sprint 1 to get the FinOps demo + Lightdash for the Harness pitch |
| Sprint 1 ships | Interviews are scheduled, no time for AI | Polish Sprint 1 demo; defer Sprint 2 |
| Sprint 2 ships | Eval drops below 80% | Iterate prompts + few-shots; reduce tool set if needed; document the regression in ADR 0009 |
| Sprint 3 ships | BigQuery free tier blocks the sync | Cap sync to a single table for the demo; document in ADR 0005 |
| Sprint 4 ships | A streaming-shaped JD appears | Re-open the deferred streaming work with a dedicated Sprint 5 (Redpanda + Spark Structured Streaming) |
| Any sprint | Bandwidth shrinks (full-time interviewing) | Drop to the JD-only path (Sprint 0 + 1) — still hits 80% of Harness JD bullets |

---

## 4. After revamp v2 — possible Sprint 5+ (no firm timeline)

These are documented for completeness, not committed.

| Theme | Trigger to re-open |
|---|---|
| Streaming branch (Redpanda + Spark Structured Streaming) | Streaming-first JD targets this candidate |
| Kubernetes via k3d + Helm | A K8s-shaped JD requires it |
| Real AWS deployment (`terraform apply`) | Funded AWS account |
| ML model training (win probability, batter projection) | Sports-tech or ML-platform JD opens |
| Polyglot — one Go ingestion microservice | Polyglot JD requires it |
| Feature store (Feast) | After ML model lands |
| Real DataHub deployment | Lineage-heavy enterprise JD |

---

## 5. Cadence + meta

- **Weekly cadence:** End-of-week verification gate per the active sprint. Drift > 1 week → re-plan.
- **Documentation cadence:** Every sprint ships with its ADR(s) and any runbook updates as part of the verification gate.
- **`docs/architecture/as-built.md`** gets a row added per sprint as it ships (✅ marker once verified).
- **Resume update cadence:** After each sprint lands, update the resume bullet list with the new capability.

---

## 6. References

- Canonical task lists: `docs/planning.md`
- Target architecture: `docs/architecture/hld-hla.md`
- What's in / what's out: `docs/product/scope-v1.md`
- Why this exists: `docs/product/vision.md`
- Personal scratch plan: `~/.claude/plans/hi-soft-prism.md`
