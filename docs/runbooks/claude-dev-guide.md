# Claude Code Interaction Guide

How to work with Claude Code across every phase of development in this repo.

---

## Overview

This repo is configured with several layers that shape how Claude behaves:

| Layer | File | Purpose |
|---|---|---|
| Default mode | `~/.claude/settings.json` | Plan mode on by default — Claude plans before executing |
| Permissions | `.claude/settings.local.json` | Pre-approved CI/test commands (no prompt for these) |
| Hooks | `.claude/settings.json` | Auto-nudge to check knowledge graph; auto-update graph after edits |
| Project memory | `~/.claude/projects/.../memory/` | 13 files of accumulated project gotchas, loaded every session |
| Project skills | `.claude/skills/` | 6 domain workflows (scaffold, inspect, validate, diagnose, etc.) |
| Architecture doc | `CLAUDE.md` | Always in context — architecture, commands, working agreement |

**Philosophy:** caution over speed. Data-pipeline bugs are expensive to undo (wrong partition overwrite = silent data loss). The toolchain is biased toward verify-before-run, minimum-code, and surfacing tradeoffs early.

---

## Modes & Defaults

### Plan mode (default)

Claude will not execute anything without your approval. The flow is:

1. You describe a task
2. Claude explores the codebase (read-only)
3. Claude writes a plan to `.claude/plans/`
4. You see the plan and approve or redirect
5. Claude executes

This is the right default for transform logic, schema changes, DAG wiring, and anything touching the medallion layers. The cost of a 30-second review is far lower than a bad partition overwrite.

**Toggle:** type `/plan` in the prompt to switch between plan and auto-execute mode for the session. Use auto-execute only for low-stakes tasks (formatting fixes, comment edits, single-file refactors).

### Permissions allowlist

`.claude/settings.local.json` pre-approves a specific set of commands so you don't get prompted for routine CI operations:

- `poetry run *` — tests, linting, pipeline jobs, dbt
- `docker compose *`, `docker exec *` — container management
- `make dag-validate`, `make up`, `make down` — infra
- `git add *`, `git commit *` — version control
- Spark env-var prefixed commands (`ICEBERG_REST_URI=... python3 *`)
- `bash validation/run.sh pre-push` and individual validation modules

Anything outside this list will prompt. That prompt is the safety gate — don't bypass it without understanding what the command does.

---

## Auto-behaviors (Hooks)

Two hooks run automatically without any action from you:

### 1. Graphify nudge (PreToolUse)

Whenever Claude is about to run `grep`, `rg`, `find`, `fd`, `ack`, or `ag`, it gets a message:

> "graphify: Knowledge graph exists. Read graphify-out/GRAPH_REPORT.md for god nodes and community structure before searching raw files."

This nudges Claude (and you) to check the knowledge graph before brute-forcing a file search. The graph has pre-extracted cross-file relationships, community clusters, and god nodes that raw grep can't surface.

### 2. Graph auto-update (PostToolUse)

After every `.py` file write or edit, Claude automatically runs:

```bash
poetry run graphify update .
```

This keeps the knowledge graph current with your code. You never need to manually refresh it after code changes.

---

## Memory

Project memory lives at `~/.claude/projects/.../memory/` and is loaded into every session. It contains 13 files covering:

- Container naming convention (`compose-<service>-1` prefix)
- Airflow admin password lifecycle (only set at first boot)
- Spark + Iceberg + MinIO config quirks (JAR bundles, s3.region, catalog label)
- DuckDB UI lock workflow (`make duckdb-stop` before Gold DAG)
- Custom Airflow image requirement (`make build-airflow` after Dockerfile changes)
- MLflow SQLite backend in dev
- `ruff --fix` vs `isort` for import sorting (they differ — always use ruff)
- Pipeline status, FQN format, module rename mapping

**To add to memory:** say "remember that..." and Claude will save a new memory file. Memory persists across sessions.

**To recall memory:** just ask ("what was the issue with the Airflow image?") — it's already loaded.

---

## Phase 1 — Research

**Goal:** Understand before touching.

### Use the knowledge graph first

The repo has a pre-built graph at `graphify-out/`. Always start here for codebase questions:

```
/graphify query "how does silver.deliveries get built?"
/graphify path "SparkIcebergWriter" "fact_delivery"
/graphify explain "DuckDBRefresh"
/graphify query "what tables does build_silver_match_data write?"
```

Graph traversal finds cross-file relationships that grep misses — it has EXTRACTED edges (AST-level imports, calls) and INFERRED edges (semantic relationships). For "how does X relate to Y" questions, this is faster and more complete than searching files.

**When to skip graphify:** single targeted lookups where you already know the file. Use `grep` or `Read` directly for "find where `META.SNAPSHOT_DATE` is used in this one file."

### Use `/cip-inspect-table` before writing any JOIN

Before writing a transform or join against any Silver/Gold table, check its actual shape:

```
/cip-inspect-table silver.deliveries
/cip-inspect-table silver.wickets
/cip-inspect-table bronze.match_data
/cip-inspect-table gold.fact_delivery
```

Returns: row count, `_snapshot_date` histogram, full schema, sample rows. Critical for verifying key uniqueness before using a table as the right side of a join — the multi-wicket delivery bug in `fact_delivery` would have been caught in 30 seconds with this check.

### Natural-language codebase questions

Since `CLAUDE.md` is always in context, Claude can answer architecture questions without searching:

- "What's the grain of `silver.wickets`?"
- "Why are Bronze/Silver materialised as DuckDB tables instead of views?"
- "Which Silver tables use Polars vs PySpark?"
- "What columns does every Bronze table carry?"
- "How does the idempotency guard work?"
- "What's the correct way to build a new table FQN?"

Ask before exploring — the answer is usually already in context.

---

## Phase 2 — Design

**Goal:** Decide what to build and define success criteria before writing code.

### Plan mode workflow

For any non-trivial task:

1. Describe the goal, not the implementation: "I need to add a `powerplays` Silver table from the match JSON."
2. Claude explores relevant code (read-only), then writes a plan.
3. Review the plan. Push back on anything that feels over-engineered.
4. Approve → Claude executes.

The plan file lives at `.claude/plans/`. You can re-read it mid-execution if you lose context.

### The 4 working agreement guardrails

These are in `CLAUDE.md` and apply to every design conversation:

**1. Think before coding**
Claude must state assumptions explicitly. If Claude says "I'll assume `match_id` is unique at this grain" — verify it. Multi-wicket deliveries broke `fact_delivery` because a uniqueness assumption wasn't checked. Push back with: "have you verified that key is unique?"

**2. Simplicity first**
No abstractions for single-use code. No error handling for impossible scenarios. No `force` flags on functions that already receive one from upstream. If the plan is over 200 lines for a task that should be 50, say "is there a simpler approach?"

**3. Surgical changes**
Claude should touch only files the task requires. If the plan includes "I'll also improve adjacent code" or "while I'm here, I'll refactor X" — that's scope creep. Say "don't touch that, keep it minimal."

**4. Goal-driven execution**
Define success criteria before Claude writes a single line. For each task type:

| Task | Success criteria |
|---|---|
| New Silver table | `poetry run pytest tests/unit/transform/` passes + real snapshot reads back from MinIO |
| Gold/dbt change | `poetry run dbt test` (40 tests) passes + relevant section of `validation_queries.sql` returns expected counts |
| DAG change | `make dag-validate` clean + DAG runs green end-to-end |
| Bronze writer change | Unit tests pass + real write to MinIO reads back correctly |

State these upfront. Don't let Claude define them retroactively.

### Scaffolding a new Silver entity

Don't design from scratch. Use:

```
/cip-add-silver-table powerplays
```

This creates stubs across 6 files: `naming.py` registration, transform module, unit test, DQ check, DAG task wire-up, job wrapper. Patterns are already correct. You fill in the transform logic — everything else is done.

---

## Phase 3 — Building

**Goal:** Write minimal, correct code.

### Surgical changes in practice

Claude is instructed to:
- Touch only what the task requires
- Match existing style (don't "improve" adjacent code)
- Remove only imports/variables its own changes made unused
- Mention unrelated dead code in chat — not delete it

Review each diff. Every changed line should trace directly to your request. If lines are changing that you didn't ask about, ask Claude why.

### Running pipelines with `/cip-pipeline-run`

Describe what you want in natural language:

```
/cip-pipeline-run
"rebuild Silver match data for 2026-05-01"
"rerun yesterday's people ingest"
"run Bronze match data for 2026-04-15"
```

The skill maps your description to the correct job + task + env vars. It pre-applies the Spark env-var soup (`ICEBERG_REST_URI`, `MINIO_S3_ENDPOINT`, `POSTGRES_HOST`, `SPARK_DRIVER_MEMORY`, `SPARK_MASTER`) so you don't have to remember the full command.

### Pre-approved commands (no prompt)

These run without a permission prompt:

```bash
poetry run pytest ...             # any test invocation
poetry run ruff check --fix .     # lint + auto-fix
poetry run dbt run / dbt test     # dbt operations
docker compose up / down / logs   # container management
make dag-validate                 # DAG import check
git add <file> / git commit ...   # version control
```

Commands outside this list will prompt. Answer the prompt deliberately — it's there for a reason.

### Graph stays current automatically

After every `.py` edit, the PostToolUse hook runs `poetry run graphify update .`. You don't need to do anything. The graph in `graphify-out/` reflects the current state of the code within seconds of each change.

---

## Phase 4 — Testing

**Goal:** Verify correctness before sharing.

### `/cip-validate` — auto-pick validation tier

```
/cip-validate
```

Based on your git state, it selects:

| Tier | When | Cost | What runs |
|---|---|---|---|
| `pre-push` | Local changes, not yet pushed | Free | Unit tests + lint + import checks |
| `pre-pr` | Branch ready for PR | Free | Integration checks + Silver/Gold cross-layer |
| `milestone` | Major feature complete | ~$1-2 | Full end-to-end harness (all 9 validation sections) |

For milestone mode, Claude confirms cost before running. Don't skip this confirmation.

### Gold layer changes → `/cip-gold-refresh`

After any Silver schema or Gold dbt change:

```
/cip-gold-refresh
```

This handles the full sequence:
1. Detects if DuckDB UI is holding the file lock → stops it
2. Rebuilds Bronze + Silver as DuckDB tables (not views — they fail in the UI without session config)
3. Runs `dbt run` (all models)
4. Runs `dbt test` (40 tests)
5. Hands UI restart back to you

**Success criteria:** all 40 dbt tests pass.

### Debugging failed DAG runs → `/cip-diagnose-dag`

```
/cip-diagnose-dag <run_id>
```

Pulls together:
- Task state + logs from the scheduler filesystem
- `control.*_ingestion_log` audit row for that run
- Landing artifacts present on MinIO
- Pattern-matches against known failure modes

Known failure modes it checks:
- DuckDB file lock (forgot `make duckdb-stop`)
- Spark JAR not yet downloaded (first run, no internet)
- Bronze not landed yet (Silver task ran before ingest)
- Stale Airflow image (missing pyspark or pydantic_settings)
- Connection refused to Iceberg REST or MinIO

### Unit tests

```bash
poetry run pytest                                           # all tests
poetry run pytest tests/unit/transform/spark/silver/       # Silver transforms
poetry run pytest tests/unit/quality/                      # DQ checks
poetry run pytest -k "test_match_registry"                 # single test by name
```

Test stubs for new Silver tables are created automatically by `/cip-add-silver-table` — fill in the assert bodies.

### End-to-end validation queries

After a Gold refresh, open `make duckdb-ui` and run sections of `analysis/validation_queries.sql`:

| Section | What it checks |
|---|---|
| 1 | Row counts across all 33 tables |
| 2 | Bronze integrity |
| 3 | Silver grain uniqueness |
| 4 | Gold dim PKs |
| 5 | Fact ↔ dim referential integrity |
| 6 | Cross-layer reconciliation (deliveries/wickets/matches) |
| 7 | Business rules (section 7.4: multi-wicket delivery diff — expect small non-zero) |
| 8 | Mart sanity |
| 9 | Freshness |

Section 7.4 will always show a small wicket diff (10 deliveries with 2 wickets, 1 with 10 wickets in current snapshot). This is expected — not a bug.

---

## Skills Quick Reference

| Skill | Trigger | Phase | What it does |
|---|---|---|---|
| `/graphify` | `/graphify query/path/explain "..."` | Research | Query the knowledge graph for cross-file relationships |
| `/cip-inspect-table` | `/cip-inspect-table <layer.table>` | Research | Row count, snapshot histogram, schema, sample rows |
| `/cip-add-silver-table` | `/cip-add-silver-table <name>` | Design/Build | Scaffold new Bronze→Silver entity across 6 files |
| `/cip-pipeline-run` | `/cip-pipeline-run` then describe | Build | Run any ingest/Silver job with correct env vars |
| `/cip-gold-refresh` | `/cip-gold-refresh` | Test | Stop DuckDB UI lock → rebuild tables → dbt run + test |
| `/cip-validate` | `/cip-validate` | Test | Auto-pick pre-push/pre-pr/milestone validation tier |
| `/cip-diagnose-dag` | `/cip-diagnose-dag <run_id>` | Test/Debug | Root cause analysis for failed Airflow DAG runs |

All skills accept `--help` for details.

---

## Common Workflows

### Add a new Silver table end-to-end

1. **Research:** `/cip-inspect-table bronze.match_data` — understand the source shape
2. **Research:** `/graphify query "how does build_silver_match_data process innings?"` — find analogous transform
3. **Scaffold:** `/cip-add-silver-table powerplays` — creates 6 file stubs
4. **Build:** Fill in the transform logic in the generated stub (follow the pattern from `officials.py` or `players.py`)
5. **Test:** `poetry run pytest tests/unit/transform/spark/silver/test_powerplays.py`
6. **Run:** `/cip-pipeline-run rebuild Silver match data for 2026-05-01`
7. **Verify:** `/cip-inspect-table silver.powerplays` — check row count and schema
8. **Validate:** `/cip-validate`

### Debug a failed Airflow DAG

1. `/cip-diagnose-dag <run_id>` — get root cause hypothesis
2. If it's a DuckDB lock: `make duckdb-stop`, then re-trigger the DAG
3. If it's a stale image: `make build-airflow && make up`
4. If it's a Bronze-not-landed issue: `/cip-pipeline-run ingest_match_data for <date>` first
5. After fix: verify in Airflow UI that the task goes green

### Refresh Gold after Silver changes

1. `make duckdb-stop` — release the file lock if the UI is open
2. `/cip-gold-refresh` — handles the rest automatically
3. Open `make duckdb-ui`
4. Run sections 3–7 of `analysis/validation_queries.sql` to verify

### Inspect a table before writing a transform

```
/cip-inspect-table silver.match_registry
```

Check: is the key you plan to join on actually unique? What's the `_snapshot_date` distribution? Are there nulls in the column you care about? Answer these before writing the JOIN.

### Run pre-PR checks before opening a PR

```
/cip-validate
```

It detects you're on a feature branch with unpushed commits and runs the `pre-pr` tier automatically. Fix any failures before pushing.

---

## Anti-patterns to Avoid

**Asking Claude to implement without stating assumptions.**
Always ask "what are you assuming about this table's grain?" before approving a plan that joins Silver tables. Multi-wicket deliveries have bitten this codebase before.

**Skipping `/cip-inspect-table` before writing JOINs.**
You cannot assume key uniqueness in Silver tables without checking. Run the inspect skill — it takes 10 seconds and has saved hours of debugging.

**Forgetting `make duckdb-stop` before the Gold DAG.**
The DuckDB UI holds a write lock. The DAG's `refresh_duckdb_views` task will fail with a file-lock error. Always stop the UI before triggering `dag_run_gold_dbt_models`.

**Using `isort` instead of `ruff --fix` for import sorting.**
`ruff` reports `I001` import-order errors. `isort` and `ruff --fix` produce different results — always use `ruff check --fix .` to resolve import errors reported by ruff. Running bare `isort` will not satisfy ruff's checks.

**Asking Claude to "also improve" adjacent code.**
Scope creep in a contract graph (Bronze → Silver → Gold → DuckDB → dbt → validation) is dangerous. An "improvement" to `naming.py` or a writer signature can silently break every downstream consumer. Keep changes local to the task.

**Running milestone validation without confirming cost.**
Milestone mode runs the full validation harness and costs ~$1-2 in LLM API calls. Claude will ask for confirmation — answer deliberately.

**Treating plan mode as friction.**
The 30-second approval step has caught bad partition overwrites, wrong table grains, and over-engineered designs. Use it. Toggle to auto-execute only for truly low-risk tasks.
