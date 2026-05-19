---
name: cip-validate
description: "Run the Cricket Intelligence Platform validation harness with the right mode for the current git state. Use when the user says 'validate', 'check before push', 'run pre-PR checks', or 'milestone validation'. Picks pre-push / pre-pr / milestone based on what's changed."
trigger: /cip-validate
---

# /cip-validate

Smart front-end for `validation/run.sh`. Picks the cheapest mode that actually exercises what changed, so the user doesn't have to remember the cost tier.

## Modes (from validation/README.md)

| Mode | Includes | Cost | Wall time | Requires Docker |
|---|---|---|---|---|
| `pre-push` | modules 01–04 (lint, tests, file audit, env vars) | $0 | ~30s | no |
| `pre-pr` | adds 05–07, 09, 11–14, 16 (docker bring-up, pipeline run, side-effect checks, idempotency) | $0 | ~10min | yes |
| `milestone` | adds 08, 13b, 14b, 15, 17, 18 (T2 Sonnet + T3 Opus AI synthesis) | ~$1–2 | ~20min | yes |

## Usage

```
/cip-validate                 # auto-pick mode based on git state
/cip-validate pre-push        # explicit mode
/cip-validate pre-pr
/cip-validate milestone
/cip-validate milestone --dry # SKIP_AI=1 milestone — runs everything but skips token-spend modules
```

## What You Must Do When Invoked

If invoked with `--help` or `-h`, print the Modes table + Usage block and stop.

### Step 1 — Decide the mode

If the user passed an explicit mode (`pre-push` / `pre-pr` / `milestone`), use it. Otherwise auto-select from git state:

```bash
# Gather signals
BRANCH=$(git branch --show-current)
UNCOMMITTED=$(git status --porcelain | wc -l | tr -d ' ')
AHEAD=$(git rev-list --count origin/main..HEAD 2>/dev/null || echo 0)
HAS_OPEN_PR=$(gh pr view --json state -q .state 2>/dev/null || echo "none")
```

Decision tree:
- `BRANCH == main` AND `UNCOMMITTED == 0` AND `AHEAD == 0` → nothing changed. Tell user "Working tree clean and main is in sync — nothing to validate." Stop.
- `UNCOMMITTED > 0` AND `AHEAD == 0` → **pre-push** (fast feedback on local edits).
- `AHEAD > 0` AND `HAS_OPEN_PR != "OPEN"` → **pre-pr** (about to open a PR).
- `HAS_OPEN_PR == "OPEN"` → **pre-pr** (CI substitute before pushing more commits).
- User explicitly says "milestone" / "Big Task done" / "wrap up" → **milestone** (warn about cost first).

Tell the user the chosen mode and one-sentence rationale, e.g.:
> "Auto-selected `pre-pr` (8 commits ahead of main, no open PR). Costs $0, takes ~10min, requires `make up`."

### Step 2 — Pre-flight

- `pre-pr` / `milestone` need Docker. Run `docker ps --filter name=compose-postgres-1 --filter status=running -q`. If empty, tell the user `make up` is required first; do NOT auto-start.
- `milestone` will spend ~$1–2 in tokens. If `--dry` was passed, set `SKIP_AI=1` for the run. Otherwise confirm cost with `ask_user tool` if the mode was auto-selected (not user-explicit).

### Step 3 — Run

```bash
[SKIP_AI=1] bash validation/run.sh <MODE>
```

Run in foreground. The harness prints per-module status as it goes — users want to see this live.

### Step 4 — Report

After completion, the harness writes results to `validation/runs/<timestamp>/` with a `latest` symlink. Print:
- Overall exit code (0 = all PASS, 1 = some FAIL/SKIP).
- The output of `bash validation/lib/print_summary.sh validation/runs/latest` (already invoked by run.sh — relay if needed).
- For milestone mode, also point at `validation/runs/latest/SUMMARY.md` (the Opus synthesis) and `TRIAGE.md` (if any failures).

If any modules FAIL, surface the failing module names. Suggest re-running just those modules during iteration:
```bash
bash validation/modules/<NN>_<name>.sh validation/runs/latest/<NN>_<name>
```

## Honesty rules

- Never run `milestone` without confirming cost when auto-selected. Explicit `/cip-validate milestone` from the user is implicit consent.
- Never auto-start Docker. Tell the user; let them decide.
- Do not retry on failure. Failures are the signal — surface them.
- If `validation/run.sh` exits non-zero but no modules FAIL, the harness halted on a foundation module (01/05/06). Look at `validation/runs/latest/<halted_module>/stderr.log`.
