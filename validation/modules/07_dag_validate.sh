#!/usr/bin/env bash
# 07_dag_validate — verify Airflow can parse the DAG file.
#
# Tier: T1 (shell only)
# Mode: pre-pr, milestone
#
# Wraps `make dag-validate`. SKIPs cleanly if the Airflow scheduler
# container isn't running — does not fail the run.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "${SCRIPT_DIR}/../lib/log.sh"

log_init "${1:?usage: 07_dag_validate.sh <out_dir>}"
cd "${REPO_ROOT}"

SCHEDULER_CONTAINER="compose-airflow-scheduler-1"
EXPECTED_DAG="dag_ingest_cricsheet_register"

if ! command -v docker >/dev/null 2>&1; then
    log_evidence "docker not on PATH"
    log_finish SKIP 0
fi

if ! docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "${SCHEDULER_CONTAINER}"; then
    log_evidence "Container '${SCHEDULER_CONTAINER}' is not running."
    log_evidence "Run 'make up' before this module."
    log_finish SKIP 0
fi

exit_code=0
make dag-validate > "${OUT_DIR}/stdout.log" 2> "${OUT_DIR}/stderr.log" || exit_code=$?

# Assert no import errors.
# "airflow dags list-import-errors" prints "No data found" when there are none;
# actual errors show a table with a non-empty filepath column.
# Run the check directly (bypassing make echo) to avoid matching on the command itself.
import_err_output="$(docker exec "${SCHEDULER_CONTAINER}" \
    airflow dags list-import-errors 2>/dev/null || true)"
if echo "${import_err_output}" | grep -qvE "^No data found$|^$|INFO|WARNING"; then
    log_evidence "Import errors: PRESENT"
    echo "${import_err_output}" >> "${OUT_DIR}/stdout.log"
    exit_code=1
else
    log_evidence "Import errors: none"
fi

# Assert expected DAG is registered.
if grep -q "${EXPECTED_DAG}" "${OUT_DIR}/stdout.log"; then
    log_evidence "DAG '${EXPECTED_DAG}': FOUND"
else
    log_evidence "DAG '${EXPECTED_DAG}': MISSING"
    exit_code=1
fi

if [[ ${exit_code} -eq 0 ]]; then
    log_finish PASS 0
else
    log_finish FAIL "${exit_code}"
fi
