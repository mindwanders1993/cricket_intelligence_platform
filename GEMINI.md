# GEMINI.md

This file provides foundational mandates and project context for Gemini CLI when working in this repository.

## Core Mandates & Context

- **Architecture:** Medallion architecture (Landing → Bronze → Silver → Gold).
- **Storage:** MinIO (local) / S3 (AWS) using Apache Iceberg table format.
- **Compute:** Polars (Ingestion/Bronze), PySpark (Silver), dbt + DuckDB (Gold/Warehouse).
- **Orchestration:** Airflow with KubernetesExecutor.
- **Instructions:** Refer to `CLAUDE.md` for detailed developer commands, module layouts, and naming conventions.
- **High-Level Design:** Refer to `README.md` for the full HLD/HLA documentation.
- **Planning:** Refer to `planning.md` for the step-by-step development roadmap.

## Standards & Conventions

- **Settings:** Always use `cip.common.settings.get_settings()`. Never instantiate `PlatformSettings` directly.
- **Naming:** Use builders in `cip.common.contracts.naming` (e.g., `TableName`, `PathBuilder`).
- **Data Quality:** Adhere to the 31-check DQ framework (Landing, Bronze, Silver).
- **Idempotency:** All jobs must be idempotent, guarding via `control` schema logs in PostgreSQL.
- **Type Safety:** All Bronze columns are ingested as strings (`infer_schema_length=0`).
- **Gold Layer:** Star schema materialized in DuckDB for serving and Iceberg for persistence. Filter by `MAX(_snapshot_date)` when consuming.

## Working agreement (Karpathy skills)

Behavioural guardrails for agent-assisted edits in this repo. Adapted from [forrestchang/andrej-karpathy-skills](https://github.com/forrestchang/andrej-karpathy-skills). These bias toward caution over speed — use judgement on trivial tasks.

### 1. Think before coding
*Don't assume. Don't hide confusion. Surface tradeoffs.*

- State assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them — don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

*Applied here:* Data-pipeline invariants (grain, partition keys, snapshot semantics) are easy to assume wrong. Before writing a JOIN against a Silver table, verify the right-side key is actually unique at that grain. Multi-wicket deliveries broke `fact_delivery` exactly this way — a 30-second "is this key unique?" check would have caught it.

### 2. Simplicity first
*Minimum code that solves the problem. Nothing speculative.*

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.
- Sanity check: *"Would a senior engineer say this is overcomplicated?"*

*Applied here:* Writers are intentionally thin (`PolarsIcebergWriter`, `SparkIcebergWriter`). Don't wrap PyIceberg/Spark calls in defensive try/except for exceptions that can't fire. Don't introduce a Pydantic model for a one-shot DAG payload — XCom takes plain dicts. Don't add a `force` flag to functions that already get one from upstream.

### 3. Surgical changes
*Touch only what you must. Clean up only your own mess.*

- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it — don't delete it.
- Remove imports/variables/functions YOUR changes made unused; don't remove pre-existing dead code unless asked.
- The test: every changed line should trace directly to the user's request.

*Applied here:* This is a contract graph (Bronze → Silver → Gold → DuckDB → dbt → validation). An "improvement" to `naming.py`, `META`, or a writer signature can silently break every downstream consumer. Keep changes local to the task; raise concerns about adjacent code in chat rather than editing it.

### 4. Goal-driven execution
*Define success criteria. Loop until verified.*

Transform vague tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, write a brief plan with a verify-check per step.

*Applied here:* This repo already has strong verify-loops — use them as success criteria up front, not as afterthoughts:
- Gold/dbt change → success = `poetry run dbt test` (40 tests) passes + relevant section of `analysis/validation_queries.sql` returns expected counts.
- DAG change → success = `make dag-validate` clean + the DAG runs green end-to-end.
- Bronze/Silver writer change → success = `poetry run pytest tests/unit/transform/` + a real snapshot write to MinIO that reads back correctly.

**These guidelines are working if:** fewer unnecessary lines in diffs, fewer rewrites due to overcomplication, and clarifying questions come *before* implementation rather than after a broken pipeline run.

## Gemini Graphify

This project uses `graphifyy` to maintain a knowledge graph of the codebase at `graphify-out/` with god nodes, community structure, and cross-file relationships.

Rules for Context Optimization:
- ALWAYS read `graphify-out/GRAPH_REPORT.md` before reading any source files, running grep/glob searches, or answering codebase questions. The graph is your primary map of the codebase and saves significant context tokens.
- IF `graphify-out/wiki/index.md` EXISTS, navigate it instead of reading raw files.
- For cross-module "how does X relate to Y" questions, prefer to review the graph relationships over massive greps — these traverse the graph's EXTRACTED + INFERRED edges instead of scanning files.
- After modifying code, run `poetry run graphify update .` to keep the graph current (AST-only, no API cost).

## Context & Memory Optimization Mandates

To maintain efficiency in the Gemini CLI:
1. **Targeted Reads:** When using `read_file`, ALWAYS use `start_line` and `end_line` if you only need a specific class or function. Avoid dumping 1000-line files into context.
2. **Conservative Searches:** When using `grep_search`, limit `total_max_matches` and use tight `include_pattern` scopes to prevent context flooding.
3. **Private Memory Offloading:** Keep this `GEMINI.md` file lean. Store specific workflows, bug notes, or temporary mental models in `.gemini/tmp/cricket-intelligence-platform/memory/MEMORY.md` instead of looping them in the chat.
4. **Sub-Agent Delegation:** Use `generalist` or `codebase_investigator` sub-agents for wide exploratory tasks to compress history.

## Useful Commands (from CLAUDE.md)

- `make up` / `make down`: Manage infrastructure services.
- `make bootstrap`: Initialize MinIO and PostgreSQL.
- `make duckdb-ui` / `make duckdb-stop`: Manage the DuckDB serving UI.
- `poetry run pytest`: Run the test suite.
- `poetry run graphify update .`: Update the knowledge graph.

## Memory Management

- **Project Instructions (`GEMINI.md`):** This file. Team-shared architecture and workflows.
- **Private Project Memory:** Store local-only notes, machine-specific setups, or private workflows in the `.gemini/tmp/.../memory/` folder. Do not commit these to the repo.
- **Global Personal Memory:** Store cross-project preferences in `~/.gemini/GEMINI.md`.
