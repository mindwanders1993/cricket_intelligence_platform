#!/usr/bin/env bash
# 18_final_report — T3 model synthesizes all evidence into SUMMARY.md.
#
# Tier: T3 (T3 Model)
# Mode: milestone (the most expensive module — runs once per milestone)
#
# Reads every prior module's result.json and evidence.txt, produces a single
# markdown milestone report at <run_dir>/SUMMARY.md.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "${SCRIPT_DIR}/../lib/log.sh"
# shellcheck source=../lib/ai.sh
source "${SCRIPT_DIR}/../lib/ai.sh"

log_init "${1:?usage: 18_final_report.sh <out_dir>}"
cd "${REPO_ROOT}"

RUN_DIR="$(dirname "${OUT_DIR}")"

if ! ai_available && [[ "${SKIP_AI:-0}" != "1" ]]; then
    log_evidence "AI CLI not on PATH"
    log_finish SKIP 0
fi

# Build the evidence bundle: all result.json + each evidence.txt + the
# triage report if present.
CONTEXT_FILE="${OUT_DIR}/context.md"
{
    echo "## VALIDATION RUN EVIDENCE"
    echo ""
    echo "Run dir: ${RUN_DIR}"
    echo "Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo ""
    echo "---"
    echo ""

    for module_dir in "${RUN_DIR}"/*/; do
        name="$(basename "${module_dir}")"
        # Skip ourselves to avoid recursion in context.
        [[ "${name}" == "${MODULE_NAME}" ]] && continue
        # Skip the state directory.
        [[ "${name}" == "_state" ]] && continue

        result="${module_dir}result.json"
        [[ -f "${result}" ]] || continue

        echo "### ${name}"
        echo ""
        echo '```json'
        cat "${result}"
        echo '```'
        echo ""

        if [[ -f "${module_dir}evidence.txt" ]]; then
            echo "Evidence:"
            echo '```'
            head -120 "${module_dir}evidence.txt"
            echo '```'
            echo ""
        fi

        echo "---"
        echo ""
    done

    if [[ -f "${RUN_DIR}/TRIAGE.md" ]]; then
        echo "## TRIAGE REPORT (from 17_failure_triage)"
        echo ""
        cat "${RUN_DIR}/TRIAGE.md"
        echo ""
    fi
} > "${CONTEXT_FILE}"

context_bytes=$(wc -c < "${CONTEXT_FILE}")
log_evidence "Context bundle: ${context_bytes} bytes"

PROMPT=
IFS='' read -r -d '' PROMPT <<'PROMPT_END' || true
You are writing the final validation report for a Big Task milestone of the Cricket Intelligence Platform.

The evidence below comes from a structured validation harness — each section is one module with its result.json and evidence.txt. A triage report may be present at the bottom.

Produce a markdown report with these sections, in this order:

# Validation Report

## 1. Scope validated
One paragraph: what was checked end-to-end.

## 2. Results table
| Module | Status | Duration | Notes |
One row per module. Notes column: a 1-line summary of what passed/failed.

## 3. Defects and remediation
For each FAIL: 1-2 lines on root cause and what was/should be done. Reuse the triage report if present.

## 4. Remaining gaps
Things this validation did not exercise. Be specific (e.g. "match ingestion not yet implemented, no validation possible").

## 5. Final verdict
One of:
- **PASS** — every module passed, no concerns.
- **PARTIAL PASS** — failures exist but are non-blocking for the milestone.
- **FAIL** — milestone is not ready to ship; list blocking issues.

Lead the verdict with a single bold line, then a 2-3 sentence justification.

Keep the whole report under 1500 words. Prefer tables and bullets over prose.
PROMPT_END

log_evidence ""
log_evidence "Invoking T3 model for final report synthesis..."

ai_exit=0
ai_big "${PROMPT}" < "${CONTEXT_FILE}" > "${OUT_DIR}/ai_response.md" 2> "${OUT_DIR}/ai_stderr.log" || ai_exit=$?

case "${ai_exit}" in
    0) ;;
    99) log_evidence "SKIP_AI=1 set — skipping"; log_finish SKIP 0 ;;
    127) log_evidence "AI CLI missing"; log_finish SKIP 0 ;;
    *) log_evidence "AI call failed (exit ${ai_exit})"
       tail -20 "${OUT_DIR}/ai_stderr.log" >> "${EVIDENCE_FILE}" 2>/dev/null
       log_finish FAIL "${ai_exit}" ;;
esac

# Publish to the run-dir root so users always know where to look.
cp "${OUT_DIR}/ai_response.md" "${RUN_DIR}/SUMMARY.md"
log_evidence ""
log_evidence "Milestone report: ${RUN_DIR}/SUMMARY.md"

# 18 always passes if T3 model returned — the verdict inside SUMMARY.md is
# what the human reads. The harness's own verdict for this module
# reflects only whether the AI call succeeded.
log_finish PASS 0
