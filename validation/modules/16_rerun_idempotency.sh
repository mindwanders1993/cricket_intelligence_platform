#!/usr/bin/env bash
# 16_rerun_idempotency — re-execute the pipeline and verify no data drift.
#
# Tier: T1 (shell only)
# Mode: pre-pr, milestone
#
# Depends on: 09_run_pipeline, 13_bronze_inspect, 14_silver_inspect
#
# Idempotency contract per CLAUDE.md:
#   - control.register_ingestion_log guards via (source_file, snapshot_date)
#   - A re-run without --force should SKIP (no new SUCCESS rows, no row growth)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "${SCRIPT_DIR}/../lib/log.sh"
# shellcheck source=../lib/env.sh
source "${SCRIPT_DIR}/../lib/env.sh"
# shellcheck source=../lib/state.sh
source "${SCRIPT_DIR}/../lib/state.sh"

log_init "${1:?usage: 16_rerun_idempotency.sh <out_dir>}"
cd "${REPO_ROOT}"

load_env

if ! command -v poetry >/dev/null 2>&1; then
    log_evidence "poetry not on PATH"
    log_finish SKIP 0
fi

SNAPSHOT_DATE="$(state_get snapshot_date "$(date +%Y-%m-%d)")"
log_evidence "Snapshot under test: ${SNAPSHOT_DATE}"
log_evidence ""

# Baseline row counts (captured by modules 13/14).
ALL_TABLES=(
    "cricket.bronze.register_people"
    "cricket.bronze.register_identifiers"
    "cricket.bronze.register_name_variations"
    "cricket.silver.persons"
    "cricket.silver.person_identifiers"
    "cricket.silver.name_variations"
)

log_evidence "Baseline row counts (from state):"
for tbl in "${ALL_TABLES[@]}"; do
    n="$(state_get "rows.${tbl}" "")"
    # Snapshot baseline under a separate key so rerun doesn't clobber it
    state_set "baseline_rows.${tbl}" "${n}"
    log_evidence "  ${tbl}: ${n:-<unknown>}"
done

# Re-run the pipeline.
log_evidence ""
log_evidence "Re-running pipeline (no --force): make run-register ARGS=\"--snapshot-date ${SNAPSHOT_DATE} --task all\""

exit_code=0
make run-register ARGS="--snapshot-date ${SNAPSHOT_DATE} --task all" \
    > "${OUT_DIR}/rerun_stdout.log" 2> "${OUT_DIR}/rerun_stderr.log" || exit_code=$?

if [[ ${exit_code} -ne 0 ]]; then
    log_evidence "Rerun failed (exit ${exit_code})"
    tail -20 "${OUT_DIR}/rerun_stderr.log" >> "${EVIDENCE_FILE}"
    log_finish FAIL "${exit_code}"
fi

# Count SKIPPED / idempotency-guard messages in the rerun log.
skip_markers="$(grep -cE "already.*processed|idempotency|SKIP" "${OUT_DIR}/rerun_stdout.log" || true)"
log_evidence ""
log_evidence "Idempotency markers in rerun log: ${skip_markers}"

# Re-inspect tables.
make inspect-tables > "${OUT_DIR}/after_inspect.log" 2>&1 || true

drift=0
log_evidence ""
log_evidence "Row counts after rerun:"
for tbl in "${ALL_TABLES[@]}"; do
    before="$(state_get "baseline_rows.${tbl}" "")"
    row_line="$(grep -E "=== ${tbl} \([0-9]+ rows\) ===" "${OUT_DIR}/after_inspect.log" | head -1 || true)"
    after="$(echo "${row_line}" | grep -oE '[0-9]+ rows' | grep -oE '[0-9]+' || true)"
    if [[ -z "${before}" || -z "${after}" ]]; then
        log_evidence "  ? ${tbl}: before=${before:-?} after=${after:-?} (insufficient data)"
        continue
    fi
    if [[ "${before}" == "${after}" ]]; then
        log_evidence "  ✓ ${tbl}: ${before} → ${after} (stable)"
    else
        log_evidence "  ✗ ${tbl}: ${before} → ${after} (DRIFT)"
        drift=$((drift + 1))
    fi
done

# Verify no new SUCCESS row appeared in control log for this snapshot.
if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "compose-postgres-1"; then
    new_success="$(docker exec compose-postgres-1 psql \
        -U "${POSTGRES_USER:-postgres}" \
        -d "${POSTGRES_DB:-cricket_platform}" \
        -tA -c "SELECT COUNT(*) FROM control.register_ingestion_log
                WHERE snapshot_date='${SNAPSHOT_DATE}' AND status='SUCCESS';" \
        2>/dev/null | tr -d '[:space:]')"
    log_evidence ""
    log_evidence "control.register_ingestion_log SUCCESS rows for ${SNAPSHOT_DATE}: ${new_success}"
fi

if [[ ${drift} -eq 0 ]]; then
    log_finish PASS 0
else
    log_finish FAIL 1
fi
