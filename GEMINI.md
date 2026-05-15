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
- `poetry run pytest`: Run the test suite.
- `poetry run graphify update .`: Update the knowledge graph.

## Memory Management

- **Project Instructions (`GEMINI.md`):** This file. Team-shared architecture and workflows.
- **Private Project Memory:** Store local-only notes, machine-specific setups, or private workflows in the `.gemini/tmp/.../memory/` folder. Do not commit these to the repo.
- **Global Personal Memory:** Store cross-project preferences in `~/.gemini/GEMINI.md`.
