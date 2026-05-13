# Validation Harness

A tiered validation system for the Cricket Intelligence Platform.
Modules are classified by the AI cost they incur, so commits stay cheap
and only milestone gates pay for synthesis.

## Tiers

| Tier | Tools                                                | Cost per run |
|------|------------------------------------------------------|--------------|
| T1   | bash, psql, mc, pytest, docker exec, airflow CLI     | $0           |
| T2   | Claude Sonnet 4.6                                    | cents        |
| T3   | Claude Opus 4.7                                      | ~$1          |

The AI model used can be overridden via env vars:
- `VALIDATION_SMALL_MODEL` (default `claude-sonnet-4-6`)
- `VALIDATION_BIG_MODEL` (default `claude-opus-4-7`)

## Modes

| Mode      | Includes                  | Token cost | When to run                       |
|-----------|---------------------------|------------|-----------------------------------|
| pre-push  | 01–04 (T1, no docker)     | $0         | every commit                      |
| pre-pr    | 01–07, 09, 11–14, 16 (T1) | $0         | before opening a PR (docker up)   |
| milestone | everything incl. T2 + T3  | ~$1–2      | end of each Big Task              |

## Usage

```bash
bash validation/run.sh pre-push     # fast gate, no docker
bash validation/run.sh pre-pr       # full T1, requires `make up`
bash validation/run.sh milestone    # full chain incl. AI synthesis
SKIP_AI=1 bash validation/run.sh milestone   # dry-run, no token spend
```

## Modules

```
T1 — shell only (free)
  01_file_audit            inventory paths exist
  02_env_vars              .env.example declares required keys
  03_lint                  ruff + black + isort
  04_unit_tests            poetry run pytest tests/unit/
  05_compose_up            make up + healthchecks (foundation)
  06_bootstrap             make bootstrap + asserts buckets/control tables
  07_dag_validate          make dag-validate
  09_run_pipeline          make run-register ARGS="--task all"
  11_pg_control            assert SUCCESS row in control.register_ingestion_log
  12_minio_landing         assert people.csv + names.csv landed
  13_bronze_inspect        assert 3 Bronze tables non-empty
  14_silver_inspect        assert 3 Silver tables non-empty
  15_airflow_e2e           trigger DAG (synthetic 9999-01-01) + assert states
  16_rerun_idempotency     re-run + assert no row drift

T2 — Sonnet / Flash (cents)
  08_contract_diff         contract doc vs implementation drift audit
  13b_bronze_semantic      anomaly review of Bronze samples
  14b_silver_semantic      anomaly review of Silver samples
  17_failure_triage        root-cause + fix for any prior FAILs (only invokes AI if failures exist)

T3 — Opus / Pro (~$1)
  18_final_report          synthesizes everything into <run_dir>/SUMMARY.md
```

## Output

Each run writes to `validation/runs/<timestamp>/`. A symlink
`validation/runs/latest` always points to the most recent run.

Per-module artifacts:

| File          | Purpose                                       |
|---------------|-----------------------------------------------|
| `result.json` | machine-readable status, duration, exit code  |
| `evidence.txt`| human-readable bullet checklist               |
| `stdout.log`  | captured stdout                               |
| `stderr.log`  | captured stderr                               |
| `ai_response.md` (T2/T3 modules) | AI output                  |
| `context.md`  (T2/T3 modules) | grounding context fed to AI    |

Run-level artifacts (milestone mode):

| File           | Source                  | Purpose                       |
|----------------|-------------------------|-------------------------------|
| `TRIAGE.md`    | `17_failure_triage`     | root-cause for any failures   |
| `SUMMARY.md`   | `18_final_report`       | the milestone report (Opus)   |

## Status semantics

| Status | Meaning                                | Halts run? |
|--------|----------------------------------------|------------|
| PASS   | All checks passed                      | no         |
| FAIL   | At least one check failed              | only if `01_*`, `05_*`, `06_*` |
| SKIP   | Preconditions not met (no docker, no CLI, SKIP_AI=1) | no |

## Token-cost gating

To dry-run milestone mode without spending tokens, set `SKIP_AI=1`:

```bash
SKIP_AI=1 bash validation/run.sh milestone
```

Every AI module returns SKIP. Useful when iterating on the harness or
debugging the T1 layers without burning tokens on noise.

The `17_failure_triage` module is also self-gating: if no prior module
FAILed, it short-circuits to PASS without calling the AI. So a clean
milestone run only pays for `08`, `13b`, `14b`, and `18`.

## Cross-module state

Modules can share data via `validation/runs/<id>/_state/<key>` using
`state_set` / `state_get` from `lib/state.sh`. Current state keys:

| Key             | Set by              | Read by                        |
|-----------------|---------------------|--------------------------------|
| snapshot_date   | `09_run_pipeline`   | `11`, `12`, `16`               |
| pipeline_run_id | `09_run_pipeline`   | (available, currently unused)  |
| rows.<table>    | `13`, `14`          | `16_rerun_idempotency`         |
| inspect_tables.log | `13_bronze_inspect` | `14_silver_inspect`, `13b`, `14b` |

## Adding a module

1. Create `validation/modules/NN_name.sh`.
2. Take one argument: the output directory.
3. Source `lib/log.sh` (always), and `lib/env.sh` / `lib/state.sh` / `lib/ai.sh` as needed.
4. Call `log_init "$1"`, do the work, end with `log_finish PASS|FAIL|SKIP [exit_code]`.
5. For AI calls, build context in `${OUT_DIR}/context.md`, then
   `ai_small "<prompt>" < "${CONTEXT_FILE}" > "${OUT_DIR}/ai_response.md"`.
   Always end your prompt with a `VERDICT:` line so the module can parse it.
6. Add the module name (without `.sh`) to the appropriate mode file.

Foundation modules whose failure should halt the run are named with a
`01_`, `05_`, or `06_` prefix — the orchestrator checks the prefix.

## Prompt heredocs — read this if you edit T2/T3 modules

macOS bash 3.2 has a known bug parsing heredocs nested inside `$(...)`
when the body contains apostrophes. AI modules therefore use the
`read -r -d ''` pattern instead:

```bash
PROMPT=
IFS='' read -r -d '' PROMPT <<'PROMPT_END' || true
Your prompt with 'apostrophes' is fine here.
PROMPT_END
```

Keep this pattern when adding new AI modules. Do not switch back to
`PROMPT=$(cat <<'EOF' ... EOF)` even though it looks cleaner.
true
Your prompt with 'apostrophes' is fine here.
PROMPT_END
```

Keep this pattern when adding new AI modules. Do not switch back to
`PROMPT=$(cat <<'EOF' ... EOF)` even though it looks cleaner.
