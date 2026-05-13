#!/usr/bin/env bash
# 13b_bronze_semantic — AI reads Bronze sample rows and flags anomalies.
#
# Tier: T2 (T2 Model)
# Mode: milestone
#
# Depends on: 13_bronze_inspect (reuses _state/inspect_tables.log)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "${SCRIPT_DIR}/../lib/log.sh"
# shellcheck source=../lib/state.sh
source "${SCRIPT_DIR}/../lib/state.sh"
# shellcheck source=../lib/ai.sh
source "${SCRIPT_DIR}/../lib/ai.sh"

log_init "${1:?usage: 13b_bronze_semantic.sh <out_dir>}"
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

# Extract only Bronze sections from the inspect log.
CONTEXT_FILE="${OUT_DIR}/context.md"
{
    echo "## BRONZE TABLE SAMPLES (from check_tables.py)"
    echo ""
    echo "Bronze contract reminders:"
    echo "- All columns must be string-typed (no pre-casting at Bronze)."
    echo "- Metadata columns required: _snapshot_date, _ingested_at, _pipeline_run_id, _source_file, _source_url, _row_hash."
    echo "- (match_id, revision) is the Bronze primary key for matches; (identifier) for register."
    echo ""
    echo '```'
    # Print the inspect log up to (but excluding) the first silver section.
    awk '/=== cricket.silver/{exit} {print}' "${INSPECT_LOG}"
    echo '```'
} > "${CONTEXT_FILE}"

PROMPT=
IFS='' read -r -d '' PROMPT <<'PROMPT_END' || true
You are reviewing Bronze Iceberg table samples from the Cricket Intelligence Platform.

Tasks:
1. For each table, check that the metadata columns (_snapshot_date, _ingested_at, _pipeline_run_id, _source_file, _source_url, _row_hash) are present and look populated.
2. Bronze ingests all CSV/JSON columns as strings. Flag any column that appears to have been pre-cast (e.g., integers or dates not quoted).
3. Look for obvious data-quality red flags: unexpected nulls, encoding artifacts, malformed values.
4. Skip cosmetic comments.

Your final output line MUST be one of these three, with no trailing text:
VERDICT: PASS
VERDICT: WARN
VERDICT: FAIL

Do not omit this line. It is machine-parsed.
PROMPT_END

log_evidence "Invoking T2 model for Bronze semantic review..."

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
    WARN) log_finish PASS 0 ;;  # WARN does not block the run
    FAIL) log_finish FAIL 1 ;;
    *)    log_evidence "Missing VERDICT line — treating as FAIL"
          log_finish FAIL 2 ;;
esac
