---
name: cip-senior-engineer
description: Senior engineering mandates and architectural guide for the Cricket Intelligence Platform (CIP). Use when developing features, fixing bugs, or analyzing the Medallion architecture (Polars, Spark, dbt, Iceberg, DuckDB).
---

# CIP Senior Engineer Skill

This skill enforces senior-level engineering standards and architectural consistency for the Cricket Intelligence Platform.

## Core Engineering Mandates (Karpathy Skills)

When performing any task in this codebase, adhere to these mandates:

1. **Think Before Coding**:
   - Explicitly state assumptions and implementation plan before writing code.
   - If a request is ambiguous, ASK for clarification immediately.
   - Map the task to the correct Medallion layer and engine (Polars for Bronze/Register, Spark for Silver Match).

2. **Simplicity First (YAGNI)**:
   - Implement the absolute minimum code required.
   - Avoid speculative abstractions, new classes, or "just-in-case" flexibility.
   - Use existing platform utilities (`naming.py`, `settings.py`, `writers.py`).

3. **Surgical Changes**:
   - Touch only the lines required for the task.
   - Match existing style (120 line length, ruff/black formatting) perfectly.
   - Do NOT refactor adjacent code or fix unrelated lint errors unless explicitly requested.

4. **Goal-Driven Execution**:
   - Define success criteria and verification steps before starting.
   - Prioritize writing reproduction tests for bugs.
   - Verify data correctness in Iceberg/DuckDB after pipeline changes.

## Architectural Constraints

### Medallion Layers
- **Bronze**: All-string ingestion (`infer_schema_length=0`), source-faithful.
- **Silver**: Typed, deduplicated, exploded. Use `MAX(_snapshot_date)` for reads.
- **Gold**: Star schema in DuckDB (serving) and Iceberg (persistence).

### Engines & Tools
- **Polars**: Primary engine for Ingestion, Bronze, and Silver Register transforms.
- **PySpark**: Used for Silver Match data transforms (heavy lifting).
- **dbt + DuckDB**: Power the Gold layer and analytics.
- **Iceberg**: Table format for all persistent lakehouse layers.

### Important Workflows
- **DuckDB UI**: Always run `make duckdb-stop` before running Gold dbt models to release file locks.
- **Naming**: Always use `cip.common.contracts.naming` builders. Never hardcode table names.
- **Settings**: Use `get_settings()` from `cip.common.settings`.

## Success Criteria Checklist
- [ ] Assumptions stated and plan approved.
- [ ] Code is minimalist and surgical.
- [ ] Tests/queries verify behavioral correctness.
- [ ] `poetry run graphify update .` executed if structural changes were made.
