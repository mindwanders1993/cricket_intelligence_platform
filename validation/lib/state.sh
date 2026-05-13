#!/usr/bin/env bash
# Cross-module state for a single validation run.
#
# State is stored in <run_dir>/_state/<key>. Values are plain strings;
# anything structured should be JSON-encoded in the key file.
#
# Requires OUT_DIR to be set by log_init.

state_dir() {
    printf '%s' "$(dirname "${OUT_DIR}")/_state"
}

state_set() {
    local key="${1:?state_set: missing key}"
    local value="${2:-}"
    local dir
    dir="$(state_dir)"
    mkdir -p "${dir}"
    printf '%s' "${value}" > "${dir}/${key}"
}

state_get() {
    local key="${1:?state_get: missing key}"
    local default="${2:-}"
    local path
    path="$(state_dir)/${key}"
    if [[ -f "${path}" ]]; then
        cat "${path}"
    else
        printf '%s' "${default}"
    fi
}

state_has() {
    local key="${1:?state_has: missing key}"
    [[ -f "$(state_dir)/${key}" ]]
}
