#!/usr/bin/env bash
# 17_failure_triage — T2 model diagnoses prior FAILed modules.
#
# Tier: T2 (T2 Model) — but ONLY invokes AI if there are failures.
# Mode: milestone
#
# Reads every <run_dir>/<module>/result.json and groups by status. If no
# failures exist, exits PASS without burning tokens. If failures exist,
# bundles their evidence and asks T2 model for root-cause + fix suggestions.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "${SCRIPT_DIR}/../lib/log.sh"
# shellcheck source=../lib/ai.sh
source "${SCRIPT_DIR}/../lib/ai.sh"

log_init "${1:?usage: 17_failure_triage.sh <out_dir>}"
cd "${REPO_ROOT}"

RUN_DIR="$(dirname "${OUT_DIR}")"

# Collect failing modules.
failures=()
for result in "${RUN_DIR}"/*/result.json; do
    [[ -f "${result}" ]] || continue
    status=$(sed -n 's/.*"status": *"\([^"]*\)".*/\1/p' "${result}")
    if [[ "${status}" == "FAIL" ]]; then
        module_dir="$(dirname "${result}")"
        # Exclude ourselves if somehow already run.
        [[ "$(basename "${module_dir}")" == "${MODULE_NAME}" ]] && continue
        failures+=("${module_dir}")
    fi
done

log_evidence "Failures detected: ${#failures[@]}"

if [[ ${#failures[@]} -eq 0 ]]; then
    log_evidence "Nothing to triage — skipping AI call (saved tokens)."
    log_finish PASS 0
fi

# Build context: per-failure evidence + tail of logs.
CONTEXT_FILE="${OUT_DIR}/context.md"
{
    echo "## VALIDATION FAILURES TO TRIAGE"
    echo ""
    for dir in "${failures[@]}"; do
        name="$(basename "${dir}")"
        echo "---"
        echo ""
        echo "### Module: ${name}"
        echo ""
        if [[ -f "${dir}/evidence.txt" ]]; then
            echo "**Evidence:**"
            echo '```'
            cat "${dir}/evidence.txt"
            echo '```'
            echo ""
        fi
        if [[ -s "${dir}/stderr.log" ]]; then
            echo "**Last 50 lines of stderr:**"
            echo '```'
            tail -50 "${dir}/stderr.log"
            echo '```'
            echo ""
        fi
        if [[ -s "${dir}/stdout.log" ]]; then
            echo "**Last 30 lines of stdout:**"
            echo '```'
            tail -30 "${dir}/stdout.log"
            echo '```'
            echo ""
        fi
    done
} > "${CONTEXT_FILE}"

PROMPT=
IFS='' read -r -d '' PROMPT <<'PROMPT_END' || true
You are triaging validation failures in the Cricket Intelligence Platform (a Polars + Iceberg + Airflow lakehouse).

For each failing module below, produce one block:

### <module name>
- **Root cause:** one sentence.
- **Fix:** specific file path + change (or specific command to run).
- **Category:** real bug | misconfiguration | infrastructure | flaky test | unknown.
- **Priority:** P0 (blocks everything) | P1 (blocks milestone) | P2 (cleanup).

After all blocks, add a single line:
VERDICT: TRIAGED
PROMPT_END

log_evidence ""
log_evidence "Invoking T2 model to triage ${#failures[@]} failure(s)..."

ai_exit=0
ai_small "${PROMPT}" < "${CONTEXT_FILE}" > "${OUT_DIR}/ai_response.md" 2> "${OUT_DIR}/ai_stderr.log" || ai_exit=$?

case "${ai_exit}" in
    0) ;;
    99|127) log_evidence "AI unavailable (exit ${ai_exit})"; log_finish SKIP 0 ;;
    *) log_evidence "AI call failed (exit ${ai_exit})"; log_finish FAIL "${ai_exit}" ;;
esac

# Surface the report at the run-dir root for easy access.
cp "${OUT_DIR}/ai_response.md" "${RUN_DIR}/TRIAGE.md"
log_evidence ""
log_evidence "Triage report: ${RUN_DIR}/TRIAGE.md"

verdict="$(ai_verdict "${OUT_DIR}/ai_response.md")"
log_evidence "Verdict: ${verdict:-<missing>}"

# Triage running successfully is itself the success signal —
# the prior failures already counted in their own modules.
if [[ "${verdict}" == "TRIAGED" ]]; then
    log_finish PASS 0
else
    log_evidence "Missing VERDICT line — triage may be incomplete"
    log_finish FAIL 1
fi
