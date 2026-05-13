#!/usr/bin/env bash
# 04_unit_tests — run the unit test suite.
#
# Tier: T1 (shell only)
# Mode: pre-push, pre-pr, milestone

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "${SCRIPT_DIR}/../lib/log.sh"

log_init "${1:?usage: 04_unit_tests.sh <out_dir>}"
cd "${REPO_ROOT}"

if ! command -v poetry >/dev/null 2>&1; then
    log_evidence "poetry not on PATH — cannot run pytest"
    log_finish SKIP 0
fi

exit_code=0
poetry run pytest tests/unit/ -q --no-header \
    > "${OUT_DIR}/stdout.log" 2> "${OUT_DIR}/stderr.log" || exit_code=$?

# Capture the pytest summary line for evidence.
summary="$(grep -E "passed|failed|error" "${OUT_DIR}/stdout.log" | tail -1)"
log_evidence "pytest: ${summary:-<no summary line>}"

if [[ ${exit_code} -eq 0 ]]; then
    log_finish PASS 0
else
    log_evidence ""
    log_evidence "Last 40 lines of pytest output:"
    tail -40 "${OUT_DIR}/stdout.log" >> "${EVIDENCE_FILE}"
    log_finish FAIL "${exit_code}"
fi
