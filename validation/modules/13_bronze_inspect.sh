#!/usr/bin/env bash
# 13_bronze_inspect — run `make inspect-tables` and verify Bronze tables have data.
#
# Tier: T1 (shell only)
# Mode: pre-pr, milestone
#
# Depends on: 09_run_pipeline
#
# Persists the full inspect-tables output to shared state so module 14
# (silver) and module 16 (idempotency baseline) can reuse it.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "${SCRIPT_DIR}/../lib/log.sh"
# shellcheck source=../lib/state.sh
source "${SCRIPT_DIR}/../lib/state.sh"

log_init "${1:?usage: 13_bronze_inspect.sh <out_dir>}"
cd "${REPO_ROOT}"

if ! command -v poetry >/dev/null 2>&1; then
    log_evidence "poetry not on PATH"
    log_finish SKIP 0
fi

BRONZE_TABLES=(
    "cricket.bronze.register_people"
    "cricket.bronze.register_identifiers"
    "cricket.bronze.register_name_variations"
)

log_evidence "Command: make inspect-tables"
exit_code=0
make inspect-tables > "${OUT_DIR}/stdout.log" 2> "${OUT_DIR}/stderr.log" || exit_code=$?

# Persist the output for downstream modules.
cp "${OUT_DIR}/stdout.log" "$(state_dir)/inspect_tables.log" 2>/dev/null || true

if [[ ${exit_code} -ne 0 ]]; then
    log_evidence "make inspect-tables failed (exit ${exit_code})"
    log_evidence "Tail of stderr:"
    tail -20 "${OUT_DIR}/stderr.log" >> "${EVIDENCE_FILE}"
    log_finish FAIL "${exit_code}"
fi

log_evidence ""
empty=0
for tbl in "${BRONZE_TABLES[@]}"; do
    # Output line format: === <fqn> (<N> rows) ===
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
log_evidence "Bronze tables checked: ${#BRONZE_TABLES[@]}, empty: ${empty}"

if [[ ${empty} -eq 0 ]]; then
    log_finish PASS 0
else
    log_finish FAIL 1
fi
