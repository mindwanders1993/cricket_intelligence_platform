#!/usr/bin/env bash
# 12_minio_landing — assert the landed CSV objects exist for the current snapshot.
#
# Tier: T1 (shell only)
# Mode: pre-pr, milestone
#
# Depends on: 09_run_pipeline (reads snapshot_date from state)
#
# Landing path convention (from ingest_cricsheet_register.py):
#   s3://cricket-landing/register_staging/snapshot_date=<date>/<hash>/<filename>

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "${SCRIPT_DIR}/../lib/log.sh"
# shellcheck source=../lib/env.sh
source "${SCRIPT_DIR}/../lib/env.sh"
# shellcheck source=../lib/state.sh
source "${SCRIPT_DIR}/../lib/state.sh"

log_init "${1:?usage: 12_minio_landing.sh <out_dir>}"
cd "${REPO_ROOT}"

load_env

if ! command -v docker >/dev/null 2>&1; then
    log_evidence "docker not on PATH"
    log_finish SKIP 0
fi

SNAPSHOT_DATE="$(state_get snapshot_date "$(date +%Y-%m-%d)")"
LANDING_PREFIX="cricket-landing/register_csv/snapshot_date=${SNAPSHOT_DATE}/"

log_evidence "Listing s3://${LANDING_PREFIX}"
log_evidence ""

MC_RUN=(docker run --rm --network compose_default \
    -e MC_HOST_local="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@compose-minio-1:9000" \
    minio/mc)

listing="$("${MC_RUN[@]}" ls --recursive "local/${LANDING_PREFIX}" 2>&1 || true)"
echo "${listing}" > "${OUT_DIR}/stdout.log"

if [[ -z "${listing}" ]]; then
    log_evidence "No objects under ${LANDING_PREFIX}"
    log_finish FAIL 1
fi

people_found=0
names_found=0
while IFS= read -r line; do
    [[ -z "${line}" ]] && continue
    log_evidence "  ${line}"
    [[ "${line}" == *"people.csv"* ]] && people_found=1
    [[ "${line}" == *"names.csv"* ]] && names_found=1
done <<< "${listing}"

log_evidence ""
log_evidence "people.csv present: $([[ ${people_found} -eq 1 ]] && echo yes || echo no)"
log_evidence "names.csv  present: $([[ ${names_found}  -eq 1 ]] && echo yes || echo no)"

if [[ ${people_found} -eq 1 && ${names_found} -eq 1 ]]; then
    log_finish PASS 0
else
    log_finish FAIL 1
fi
