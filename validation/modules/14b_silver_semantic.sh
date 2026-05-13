#!/usr/bin/env bash
# 14b_silver_semantic — AI reads Silver sample rows and flags anomalies.
#
# Tier: T2 (T2 Model)
# Mode: milestone
#
# Depends on: 13_bronze_inspect / 14_silver_inspect (reuses _state/inspect_tables.log)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "${SCRIPT_DIR}/../lib/log.sh"
# shellcheck source=../lib/state.sh
source "${SCRIPT_DIR}/../lib/state.sh"
# shellcheck source=../lib/ai.sh
source "${SCRIPT_DIR}/../lib/ai.sh"

log_init "${1:?usage: 14b_silver_semantic.sh <out_dir>}"
cd "${REPO_ROOT}"

INSPECT_LOG="$(state_dir)/inspect_tables.log"
if [[ ! -f "${INSPECT_LOG}" ]]; then
    log_evidence "No cached inspect output (run 13_bronze_inspect first)"
    log_finish SKIP 0
fi

if ! ai_available && [[ "${SKIP_AI:-0}" != "1" ]]; then
    log_evidence "AI CLI not on PATH and SKIP_AI not set"
    log_finish SKIP 0
fi

# Extract Silver sections only.
CONTEXT_FILE="${OUT_DIR}/context.md"
{
    echo "## SILVER TABLE SAMPLES (from check_tables.py)"
    echo ""
    echo "Silver contract reminders:"
    echo "- Silver is typed (typecast from Bronze strings). Flag any string-looking columns that should be typed."
    echo "- Persons: person_id is renamed from Bronze 'identifier'."
    echo "- Person identifiers: source_system / source_identifier (unpivoted from Bronze key_source / key_value)."
    echo "- Name variations: dedup on (identifier, name)."
    echo "- Metadata: _snapshot_date, _ingested_at, _pipeline_run_id, _bronze_loaded_at, _row_hash."
    echo ""
    echo '```'
    awk '/=== cricket\.silver/{found=1} found {print}' "${INSPECT_LOG}"
    echo '```'
} > "${CONTEXT_FILE}"

PROMPT=
IFS='' read -r -d '' PROMPT <<'PROMPT_END' || true
You are reviewing Silver Iceberg table samples from the Cricket Intelligence Platform's Register pipeline.

Tasks:
1. Confirm typing: Silver should be typed (numbers as numbers, dates as dates), not raw strings.
2. Confirm renames are applied: 'identifier' → 'person_id' on silver.persons; key_source/key_value → source_system/source_identifier on silver.person_identifiers.
3. Confirm metadata columns exist (_snapshot_date, _ingested_at, _pipeline_run_id, _bronze_loaded_at, _row_hash).
4. Flag duplicates, nulls in identity fields, or unexpected values.
5. Skip cosmetic comments.

End your response with exactly one line, all caps:
VERDICT: PASS
VERDICT: WARN
VERDICT: FAIL
PROMPT_END

log_evidence "Invoking T2 model for Silver semantic review..."

ai_exit=0
ai_small "${PROMPT}" < "${CONTEXT_FILE}" > "${OUT_DIR}/ai_response.md" 2> "${OUT_DIR}/ai_stderr.log" || ai_exit=$?

case "${ai_exit}" in
    0) ;;
    99|127) log_evidence "AI unavailable (exit ${ai_exit})"; log_finish SKIP 0 ;;
    *) log_evidence "AI call failed (exit ${ai_exit})"; log_finish FAIL "${ai_exit}" ;;
esac

verdict="$(ai_verdict "${OUT_DIR}/ai_response.md")"
log_evidence "Verdict: ${verdict:-<missing>}"

case "${verdict}" in
    PASS) log_finish PASS 0 ;;
    WARN) log_finish PASS 0 ;;
    FAIL) log_finish FAIL 1 ;;
    *)    log_evidence "Missing VERDICT line — treating as FAIL"
          log_finish FAIL 2 ;;
esac
