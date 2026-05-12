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

This project uses `gemini graphify` to maintain a knowledge graph of the codebase at `gemini-graphify-out/` with god nodes, community structure, and cross-file relationships.

Rules:
- ALWAYS read `gemini-graphify-out/GRAPH_REPORT.md` before reading any source files, running grep/glob searches, or answering codebase questions. The graph is your primary map of the codebase.
- IF `gemini-graphify-out/wiki/index.md` EXISTS, navigate it instead of reading raw files.
- For cross-module "how does X relate to Y" questions, prefer `gemini graphify query "<question>"`, `gemini graphify path "<A>" "<B>"`, or `gemini graphify explain "<concept>"` over grep — these traverse the graph's EXTRACTED + INFERRED edges instead of scanning files.
- After modifying code, run `gemini graphify update .` to keep the graph current (AST-only, no API cost).

## Useful Commands (from CLAUDE.md)

- `make up` / `make down`: Manage infrastructure services.
- `make bootstrap`: Initialize MinIO and PostgreSQL.
- `poetry run pytest`: Run the test suite.
- `gemini graphify update .`: Update the knowledge graph.

## Memory Management

- **Project Instructions (`GEMINI.md`):** This file. Team-shared architecture and workflows.
- **Private Project Memory:** Store local-only notes, machine-specific setups, or private workflows in the `.gemini/tmp/.../memory/` folder. Do not commit these to the repo.
- **Global Personal Memory:** Store cross-project preferences in `~/.gemini/GEMINI.md`.
