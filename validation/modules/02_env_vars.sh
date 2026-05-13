#!/usr/bin/env bash
# 02_env_vars — confirm .env.example declares every required key.
#
# Tier: T1 (shell only)
# Mode: pre-push, pre-pr, milestone

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "${SCRIPT_DIR}/../lib/log.sh"

log_init "${1:?usage: 02_env_vars.sh <out_dir>}"
cd "${REPO_ROOT}"

ENV_FILE=".env.example"

# Keys the platform needs at boot. Keep in sync with settings.py +
# infra/compose/compose.base.yml.
REQUIRED_KEYS=(
    POSTGRES_USER
    POSTGRES_PASSWORD
    POSTGRES_DB
    MINIO_ROOT_USER
    MINIO_ROOT_PASSWORD
    AIRFLOW_ADMIN_PASSWORD
)

if [[ ! -f "${ENV_FILE}" ]]; then
    log_evidence "ERROR: ${ENV_FILE} not found"
    log_finish FAIL 1
fi

log_evidence "Inspecting: ${ENV_FILE}"
log_evidence ""

missing=()
for key in "${REQUIRED_KEYS[@]}"; do
    if grep -qE "^${key}=" "${ENV_FILE}"; then
        log_evidence "FOUND   ${key}"
    else
        log_evidence "MISSING ${key}"
        missing+=("${key}")
    fi
done

log_evidence ""
log_evidence "Summary: $((${#REQUIRED_KEYS[@]} - ${#missing[@]}))/${#REQUIRED_KEYS[@]} required keys present"

if [[ ${#missing[@]} -eq 0 ]]; then
    log_finish PASS 0
else
    log_finish FAIL 1
fi
