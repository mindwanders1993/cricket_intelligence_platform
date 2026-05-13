#!/usr/bin/env bash
# 03_lint — run ruff, black --check, isort --check-only.
#
# Tier: T1 (shell only)
# Mode: pre-push, pre-pr, milestone
#
# Mirrors `make lint`. Records exit codes per tool so failures are
# attributable.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "${SCRIPT_DIR}/../lib/log.sh"

log_init "${1:?usage: 03_lint.sh <out_dir>}"
cd "${REPO_ROOT}"

if ! command -v poetry >/dev/null 2>&1; then
    log_evidence "poetry not on PATH — cannot run lint"
    log_finish SKIP 0
fi

overall=0

run_tool() {
    local name="$1"
    shift
    local exit_code=0
    "$@" >> "${OUT_DIR}/stdout.log" 2>> "${OUT_DIR}/stderr.log" || exit_code=$?
    if [[ ${exit_code} -eq 0 ]]; then
        log_evidence "${name}: PASS"
    else
        log_evidence "${name}: FAIL (exit ${exit_code})"
        overall=1
    fi
}

run_tool "ruff"  poetry run ruff check .
run_tool "black" poetry run black --check .
run_tool "isort" poetry run isort --check-only .

log_evidence ""
if [[ ${overall} -eq 0 ]]; then
    log_evidence "All lint checks passed."
    log_finish PASS 0
else
    log_evidence "Lint failures — see stdout.log for diffs."
    log_finish FAIL 1
fi
