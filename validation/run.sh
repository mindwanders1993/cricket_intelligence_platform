#!/usr/bin/env bash
# Validation orchestrator.
#
# Usage:
#   bash validation/run.sh [mode]
#
# Modes are defined by files in validation/modes/.
# Default mode: pre-push.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

MODE="${1:-pre-push}"
MODE_FILE="${SCRIPT_DIR}/modes/${MODE}.txt"

if [[ ! -f "${MODE_FILE}" ]]; then
    echo "Unknown mode: ${MODE}" >&2
    echo "Available modes:" >&2
    for f in "${SCRIPT_DIR}"/modes/*.txt; do
        [[ -f "${f}" ]] && echo "  $(basename "${f}" .txt)" >&2
    done
    exit 2
fi

RUN_ID="$(date +%Y-%m-%d_%H-%M-%S)"
RUN_DIR="${SCRIPT_DIR}/runs/${RUN_ID}"
mkdir -p "${RUN_DIR}"

# Maintain a stable "latest" symlink for grep-friendly access.
ln -sfn "${RUN_ID}" "${SCRIPT_DIR}/runs/latest"

cat <<EOF
═══════════════════════════════════════════════════════════════
 Validation run: ${MODE}
 Run id:         ${RUN_ID}
 Output:         ${RUN_DIR}
═══════════════════════════════════════════════════════════════
EOF

overall_status=0

# Read the mode file, skipping comments and blanks.
while IFS= read -r mod || [[ -n "${mod}" ]]; do
    mod="${mod%%#*}"          # strip inline comments
    mod="${mod// /}"           # strip whitespace
    [[ -z "${mod}" ]] && continue

    module_dir="${RUN_DIR}/${mod}"
    mkdir -p "${module_dir}"

    module_script="${SCRIPT_DIR}/modules/${mod}.sh"
    if [[ ! -f "${module_script}" ]]; then
        printf '✗ %-32s %-5s (script missing: %s)\n' "${mod}" "FAIL" "${module_script}"
        overall_status=1
        continue
    fi

    if ! bash "${module_script}" "${module_dir}"; then
        overall_status=1
        # Halt only on foundation failures — downstream modules
        # would just produce noise.
        case "${mod}" in
            01_*|05_*|06_*)
                echo ""
                echo "Foundation module ${mod} failed — halting remaining modules."
                break
                ;;
        esac
    fi
done < "${MODE_FILE}"

echo ""
bash "${SCRIPT_DIR}/lib/print_summary.sh" "${RUN_DIR}"

exit "${overall_status}"
