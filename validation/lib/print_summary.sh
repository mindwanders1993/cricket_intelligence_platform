#!/usr/bin/env bash
# Print a summary table for a validation run directory.
# Usage: print_summary.sh <run_dir>

set -uo pipefail

RUN_DIR="${1:?usage: print_summary.sh <run_dir>}"

if [[ ! -d "${RUN_DIR}" ]]; then
    echo "Run directory not found: ${RUN_DIR}" >&2
    exit 2
fi

pass=0
fail=0
skip=0

printf '\n%-32s %-6s %-8s\n' "Module" "Status" "Time"
printf '%-32s %-6s %-8s\n' "--------------------------------" "------" "--------"

# Modules are subdirectories; sort to preserve numeric module ordering.
for module_dir in "${RUN_DIR}"/*/; do
    result="${module_dir}result.json"
    [[ -f "${result}" ]] || continue

    module=$(sed -n 's/.*"module": *"\([^"]*\)".*/\1/p' "${result}")
    status=$(sed -n 's/.*"status": *"\([^"]*\)".*/\1/p' "${result}")
    duration=$(sed -n 's/.*"duration_seconds": *\([0-9]*\).*/\1/p' "${result}")

    printf '%-32s %-6s %ss\n' "${module}" "${status}" "${duration}"

    case "${status}" in
        PASS) pass=$((pass + 1)) ;;
        FAIL) fail=$((fail + 1)) ;;
        SKIP) skip=$((skip + 1)) ;;
    esac
done

printf '\nResult: %d passed, %d failed, %d skipped\n' "${pass}" "${fail}" "${skip}"
printf 'Detail: %s\n' "${RUN_DIR}"
