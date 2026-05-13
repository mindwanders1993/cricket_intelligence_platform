#!/usr/bin/env bash
# 09_run_pipeline — execute the Register pipeline end-to-end via `make run-register`.
#
# Tier: T1 (shell only)
# Mode: pre-pr, milestone
#
# Captures the snapshot_date and pipeline_run_id used by the run and writes
# them to shared state for downstream modules (11, 13, 14, 16).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "${SCRIPT_DIR}/../lib/log.sh"
# shellcheck source=../lib/state.sh
source "${SCRIPT_DIR}/../lib/state.sh"

log_init "${1:?usage: 09_run_pipeline.sh <out_dir>}"
cd "${REPO_ROOT}"

if ! command -v poetry >/dev/null 2>&1; then
    log_evidence "poetry not on PATH"
    log_finish SKIP 0
fi

# Today's date in ISO format — also stored to state for downstream queries.
SNAPSHOT_DATE="$(date +%Y-%m-%d)"
state_set "snapshot_date" "${SNAPSHOT_DATE}"

log_evidence "Snapshot date: ${SNAPSHOT_DATE}"
log_evidence "Command: make run-register ARGS=\"--snapshot-date ${SNAPSHOT_DATE} --task all\""
log_evidence ""

exit_code=0
make run-register ARGS="--snapshot-date ${SNAPSHOT_DATE} --task all" \
    > "${OUT_DIR}/stdout.log" 2> "${OUT_DIR}/stderr.log" || exit_code=$?

# Best-effort extraction of pipeline_run_id from the log lines.
# The job emits structured logs with pipeline_run_id in the extra dict.
run_id="$(grep -oE "pipeline_run_id['\":= ]+[a-zA-Z0-9_.-]+" "${OUT_DIR}/stdout.log" \
    | head -1 | grep -oE "[a-zA-Z0-9_.-]+$" || true)"
if [[ -n "${run_id}" ]]; then
    state_set "pipeline_run_id" "${run_id}"
    log_evidence "Captured pipeline_run_id: ${run_id}"
fi

# Surface task completion markers from the log.
log_evidence ""
log_evidence "Task markers seen in log:"
for task in task_download_and_land task_load_bronze task_load_silver; do
    if grep -q "${task} complete" "${OUT_DIR}/stdout.log"; then
        log_evidence "  ✓ ${task}"
    else
        log_evidence "  ✗ ${task} (no 'complete' marker)"
    fi
done

if [[ ${exit_code} -eq 0 ]]; then
    log_finish PASS 0
else
    log_evidence ""
    log_evidence "Pipeline failed. Tail of stderr:"
    tail -30 "${OUT_DIR}/stderr.log" >> "${EVIDENCE_FILE}"
    log_finish FAIL "${exit_code}"
fi
