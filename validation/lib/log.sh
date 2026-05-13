#!/usr/bin/env bash
# Validation module logging helpers.
#
# Each module sources this file and uses:
#   source "$(dirname "${BASH_SOURCE[0]}")/../lib/log.sh"
#   log_init "${1}"                    # 1st arg is the output directory
#   log_evidence "key: value"          # append a line to evidence.txt
#   log_finish PASS|FAIL|SKIP [code]   # write result.json and exit
#
# All modules must call log_init first and log_finish last.

log_init() {
    OUT_DIR="${1:?log_init: missing output directory}"
    MODULE_NAME="$(basename "${OUT_DIR}")"
    STARTED_AT="$(date +%s)"
    EVIDENCE_FILE="${OUT_DIR}/evidence.txt"
    mkdir -p "${OUT_DIR}"
    : > "${EVIDENCE_FILE}"
    : > "${OUT_DIR}/stdout.log"
    : > "${OUT_DIR}/stderr.log"
    export OUT_DIR MODULE_NAME STARTED_AT EVIDENCE_FILE
}

log_evidence() {
    printf '%s\n' "$*" >> "${EVIDENCE_FILE}"
}

log_finish() {
    local status="${1:?log_finish: missing status}"
    local exit_code="${2:-0}"
    local ended_at
    ended_at="$(date +%s)"
    local duration=$((ended_at - STARTED_AT))

    cat > "${OUT_DIR}/result.json" <<EOF
{
  "module": "${MODULE_NAME}",
  "status": "${status}",
  "started_at": ${STARTED_AT},
  "ended_at": ${ended_at},
  "duration_seconds": ${duration},
  "exit_code": ${exit_code},
  "evidence_path": "evidence.txt"
}
EOF

    local marker
    case "${status}" in
        PASS) marker="✓" ;;
        FAIL) marker="✗" ;;
        SKIP) marker="○" ;;
        *)    marker="?" ;;
    esac
    printf '%s %-32s %-5s (%ds)\n' "${marker}" "${MODULE_NAME}" "${status}" "${duration}"

    exit "${exit_code}"
}
