#!/usr/bin/env bash
# 14_silver_inspect — assert Silver Register tables have data.
#
# Tier: T1 (shell only)
# Mode: pre-pr, milestone
#
# Depends on: 13_bronze_inspect (reuses its inspect-tables output if present)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "${SCRIPT_DIR}/../lib/log.sh"
# shellcheck source=../lib/state.sh
source "${SCRIPT_DIR}/../lib/state.sh"

log_init "${1:?usage: 14_silver_inspect.sh <out_dir>}"
cd "${REPO_ROOT}"

SILVER_TABLES=(
    "cricket.silver.persons"
    "cricket.silver.person_identifiers"
    "cricket.silver.name_variations"
)

# Re-use 13's output if present; otherwise re-run inspect.
INSPECT_LOG="$(state_dir)/inspect_tables.log"
if [[ -f "${INSPECT_LOG}" ]]; then
    log_evidence "Reusing inspect output from state: ${INSPECT_LOG}"
    cp "${INSPECT_LOG}" "${OUT_DIR}/stdout.log"
else
    log_evidence "No cached inspect output — running make inspect-tables"
    if ! command -v poetry >/dev/null 2>&1; then
        log_evidence "poetry not on PATH"
        log_finish SKIP 0
    fi
    exit_code=0
    make inspect-tables > "${OUT_DIR}/stdout.log" 2> "${OUT_DIR}/stderr.log" || exit_code=$?
    if [[ ${exit_code} -ne 0 ]]; then
        log_evidence "make inspect-tables failed (exit ${exit_code})"
        log_finish FAIL "${exit_code}"
    fi
fi

log_evidence ""
empty=0
for tbl in "${SILVER_TABLES[@]}"; do
    row_line="$(grep -E "=== ${tbl} \([0-9]+ rows\) ===" "${OUT_DIR}/stdout.log" | head -1 || true)"
    if [[ -z "${row_line}" ]]; then
        log_evidence "  ✗ ${tbl} (table not found in output)"
        empty=$((empty + 1))
        continue
    fi
    count="$(echo "${row_line}" | grep -oE '[0-9]+ rows' | grep -oE '[0-9]+')"
    state_set "rows.${tbl}" "${count}"
    if [[ "${count}" -gt 0 ]]; then
        log_evidence "  ✓ ${tbl}: ${count} rows"
    else
        log_evidence "  ✗ ${tbl}: 0 rows"
        empty=$((empty + 1))
    fi
done

log_evidence ""
log_evidence "Silver tables checked: ${#SILVER_TABLES[@]}, empty: ${empty}"

if [[ ${empty} -eq 0 ]]; then
    log_finish PASS 0
else
    log_finish FAIL 1
fi
