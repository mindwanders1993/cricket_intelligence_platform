#!/usr/bin/env bash
# Load .env into the current shell.
#
# Usage:
#   source "${SCRIPT_DIR}/../lib/env.sh"
#   load_env             # sources .env from REPO_ROOT (required REPO_ROOT)
#   load_env --optional  # SKIPs if missing rather than failing the module

load_env() {
    local mode="${1:-required}"
    local env_file="${REPO_ROOT:?load_env: REPO_ROOT not set}/.env"

    if [[ ! -f "${env_file}" ]]; then
        if [[ "${mode}" == "--optional" ]]; then
            log_evidence "WARN: .env not found at ${env_file} — using shell environment only"
            return 0
        fi
        log_evidence "ERROR: .env not found at ${env_file}"
        log_evidence "Run 'cp .env.example .env' and fill in values."
        log_finish FAIL 1
    fi

    set -a
    # shellcheck disable=SC1090
    source "${env_file}"
    set +a
}

# Localhost overrides that mirror Makefile's LOCAL_ENV — needed when running
# Python clients from the host against services exposed via docker compose.
local_env_exports() {
    export POSTGRES_HOST=localhost
    export MINIO_S3_ENDPOINT=http://localhost:9000
    export ICEBERG_REST_URI=http://localhost:8181
}
