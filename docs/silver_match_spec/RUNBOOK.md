# Big Task 5 — Match Silver Pipeline Runbook

End-to-end execution plan for building the 10-table Silver Match pipeline using
a multi-tool workflow: Gemini CLI for design/review, Aider+Ollama for boilerplate,
Claude Code for complex logic.

---

## Stages at a glance

| # | Stage | Tool | Time | Prompt file |
|---|---|---|---|---|
| A | Spec design | Gemini CLI | 15 min | `prompts/01-gemini-spec.md` |
| B | Phase 0 + 1: shared schema + lookups | Aider + Ollama | 30 min | `prompts/02-aider-phase1.md` |
| C | Phase 2: matches / innings / deliveries / wickets | Claude Code | 2-3 hrs | `prompts/03-claude-phase2.md` |
| D | Phase 3 + 4: participants + identity resolution | Claude Code | 1-2 hrs | `prompts/04-claude-phase3-4.md` |
| E | Phase 5 + 6: DAG wiring + DQ checks | Claude Code | 1-2 hrs | `prompts/05-claude-phase5-6.md` |
| F | Final consistency review | Gemini CLI | 30 min | `prompts/06-gemini-review.md` |

**Total estimated time: ~6-9 hours focused work.**

---

## Pre-flight (once, before Stage A)

```bash
# 1. Confirm tools installed
ollama list | grep qwen2.5-coder:32b   # must show the model
aider --version                         # 0.86.x
gemini --version                        # 0.41.x
which claude                            # /opt/homebrew/bin/claude

# 2. Confirm services are clean
docker ps --filter "name=compose-" --format "{{.Names}}"   # cricket platform up
cd ~/Desktop/Projects/cricket_intelligence_platform
poetry run pytest --tb=no -q | tail -1                     # all tests green
```

If anything is missing — fix that first.

---

## Stage A — Spec design (Gemini CLI)

**Goal:** Produce `docs/silver_match_spec/spec.md` — the authoritative spec for
all 12 Silver tables, used by every later stage.

```bash
cd ~/Desktop/Projects/cricket_intelligence_platform
gemini
```

Inside the Gemini session, paste the prompt from `prompts/01-gemini-spec.md`.

When Gemini finishes, copy its output and save it:
```bash
# Either redirect from Gemini's output, or paste into:
$EDITOR docs/silver_match_spec/spec.md
```

**Verification:**
- File exists at `docs/silver_match_spec/spec.md`
- Has sections: StructType, 12 table schemas, edge cases, identity algorithm,
  dependency order, test scenarios.
- Sanity-read it. If a section is thin, ask Gemini to expand that section.

---

## Stage B — Phase 0 + 1: shared schema + lookup tables (Aider + Ollama)

**Goal:** Generate 4 files of boilerplate that don't need complex reasoning:
- `src/cip/transform/spark/silver/_shared.py`
- `src/cip/transform/spark/silver/teams.py`
- `src/cip/transform/spark/silver/venues.py`
- `src/cip/transform/spark/silver/competitions.py`

```bash
# Stop Docker — frees ~5GB RAM so qwen2.5-coder:32b runs smoothly
cd ~/Desktop/Projects/cricket_intelligence_platform
make down

# Check available RAM (should now be ~16-18GB free)
vm_stat | awk '/free/ {free=$3} /inactive/ {inactive=$3} END {printf "Free: %.1fGB\n", (free+inactive)*16384/1073741824}'

# Open Aider with only the files we need in context
aider \
  docs/silver_match_spec/spec.md \
  src/cip/transform/polars/silver/persons.py \
  src/cip/transform/shared/writers.py \
  src/cip/common/contracts/naming.py \
  src/cip/transform/spark/silver/_shared.py \
  src/cip/transform/spark/silver/teams.py \
  src/cip/transform/spark/silver/venues.py \
  src/cip/transform/spark/silver/competitions.py
```

Inside Aider, paste the prompt from `prompts/02-aider-phase1.md`. Approve each
diff with `/yes`. Exit with `/exit`.

**Verification:**
```bash
# Lint
poetry run ruff check --fix src/cip/transform/spark/silver/
poetry run black src/cip/transform/spark/silver/

# Syntax check
poetry run python -c "from cip.transform.spark.silver import _shared, teams, venues, competitions"

# Bring Docker back up
make up
```

If the local-model output is poor, fall back to Claude Code with the same prompt.

---

## Stage C — Phase 2: matches / innings / deliveries / wickets (Claude Code)

**Goal:** The hardest 4 files in the project. Double explode, edge cases,
revision dedup.

```bash
# In a NEW Claude Code session (do NOT continue from this one — fresh context)
cd ~/Desktop/Projects/cricket_intelligence_platform
claude
```

Paste the prompt from `prompts/03-claude-phase2.md`.

**Verification at the end:**
```bash
poetry run pytest tests/unit/transform/spark/silver/ -v
# Should have tests for: matches, innings, deliveries, wickets
# All passing.
```

---

## Stage D — Phase 3 + 4: participants + identity resolution (Claude Code)

**Goal:** Build match_players, match_officials, match_registry, identity
resolution, and unmatched_persons_audit.

```bash
# Continue the Claude session from Stage C if context is still fresh,
# OR start a new one with the prompt
```

Paste the prompt from `prompts/04-claude-phase3-4.md`.

**Verification:**
```bash
poetry run pytest tests/unit/transform/spark/silver/ -v
# Adds tests for match_players, match_officials, match_registry, identity_resolution
```

---

## Stage E — Phase 5 + 6: DAG wiring + DQ checks (Claude Code)

**Goal:** Wire it all into Airflow, add the 6 cricket-specific DQ checks.

```bash
# New Claude session OR continue
```

Paste the prompt from `prompts/05-claude-phase5-6.md`.

**Verification:**
```bash
# DAG validates
make dag-validate

# DQ tests pass
poetry run pytest tests/unit/quality/test_match_silver_dq.py -v

# Full suite still green
poetry run pytest --tb=no -q | tail -1
```

---

## Stage F — Final consistency review (Gemini CLI)

**Goal:** Catch inconsistencies across the 15+ new files before declaring done.

```bash
cd ~/Desktop/Projects/cricket_intelligence_platform
gemini
```

Paste the prompt from `prompts/06-gemini-review.md`.

Take Gemini's checklist back to Claude Code:
```bash
claude
> "Here is Gemini's review of Task 5: [paste]. Work through each FIX item."
```

---

## Stage F+ — End-to-end smoke test

Once the full DAG validates and all unit tests pass:

```bash
# Run the actual pipeline end-to-end on a small snapshot
poetry run python -m cip.ingestion.jobs.build_match_silver \
  --snapshot-date 2026-05-01 --task all

# Verify rows landed in Silver
poetry run python -c "
from cip.transform.shared.readers import PolarsIcebergReader
r = PolarsIcebergReader.from_settings()
for tbl in ['matches', 'innings', 'deliveries', 'wickets', 'match_players']:
    df = r.read_table(f'cricket.silver.{tbl}')
    print(f'{tbl}: {df.height} rows')
"

# DQ summary
psql ${POSTGRES_DSN:-postgresql://cricket_user:cricket_pass@localhost:5432/cricket_platform} \
  -c "SELECT check_id, status FROM control.dq_results WHERE dag_id = 'dag_build_silver_entities' ORDER BY checked_at DESC LIMIT 20;"
```

---

## When things go wrong

- **Aider produces unusable output**: switch the same prompt to Claude Code.
- **Claude Code session getting too long**: save a status note to `docs/silver_match_spec/PROGRESS.md`, start fresh.
- **Tests fail unexpectedly**: don't paper over. Read the error, fix root cause.
- **Spec turns out to be wrong mid-implementation**: update `spec.md`, note the
  change in `PROGRESS.md`, then continue.

---

## Token budget vs all-Claude

| Stage | All-Claude est. | Multi-tool est. | Tool used |
|---|---|---|---|
| Stage A — Spec | 50K | 0 | Gemini (free) |
| Stage B — Phase 0+1 | 80K | 0 | Aider+Ollama (free) |
| Stage C — Phase 2 | 150K | 150K | Claude |
| Stage D — Phase 3+4 | 100K | 100K | Claude |
| Stage E — Phase 5+6 | 80K | 80K | Claude |
| Stage F — Review | 60K | 0 | Gemini (free) |
| **Total** | **~520K** | **~330K** | **~37% savings** |
