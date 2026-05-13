#!/usr/bin/env bash
# 06_bootstrap — run `make bootstrap` and verify buckets + control schema.
#
# Tier: T1 (shell only)
# Mode: pre-pr, milestone
# Foundation: failure halts the run.
#
# Asserts:
#   - All expected MinIO buckets exist (cricket-landing, -bronze, -silver,
#     -gold, iceberg-warehouse, mlflow-artifacts)
#   - PostgreSQL `control` schema exists
#   - control.register_ingestion_log table exists
#   - control.register_schema_versions table exists

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "${SCRIPT_DIR}/../lib/log.sh"
# shellcheck source=../lib/env.sh
source "${SCRIPT_DIR}/../lib/env.sh"

log_init "${1:?usage: 06_bootstrap.sh <out_dir>}"
cd "${REPO_ROOT}"

load_env

# Run the bootstrap target (idempotent).
log_evidence "Running: make bootstrap"
exit_code=0
make bootstrap >> "${OUT_DIR}/stdout.log" 2>> "${OUT_DIR}/stderr.log" || exit_code=$?
if [[ ${exit_code} -ne 0 ]]; then
    log_evidence "make bootstrap: FAIL (exit ${exit_code})"
    log_finish FAIL "${exit_code}"
fi
log_evidence "make bootstrap: OK"
log_evidence ""

# Assert MinIO buckets via mc-in-docker (avoids requiring host mc).
EXPECTED_BUCKETS=(
    cricket-landing
    cricket-bronze
    cricket-silver
    cricket-gold
    iceberg-warehouse
    mlflow-artifacts
)
MC_RUN=(docker run --rm --network compose_default \
    -e MC_HOST_local="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@compose-minio-1:9000" \
    minio/mc)

missing_buckets=()
mc_listing="$("${MC_RUN[@]}" ls local 2>/dev/null || true)"
echo "${mc_listing}" >> "${OUT_DIR}/stdout.log"
for bucket in "${EXPECTED_BUCKETS[@]}"; do
    if echo "${mc_listing}" | grep -qE "\b${bucket}/?\s*$"; then
        log_evidence "  ✓ s3://${bucket}/"
    else
        log_evidence "  ✗ s3://${bucket}/ (MISSING)"
        missing_buckets+=("${bucket}")
    fi
done

# Assert PostgreSQL control schema + tables.
log_evidence ""
log_evidence "PostgreSQL control schema:"

PSQL=(docker exec compose-postgres-1 psql \
    -U "${POSTGRES_USER:-postgres}" \
    -d "${POSTGRES_DB:-cricket_platform}" \
    -tAc)

EXPECTED_TABLES=(
    register_ingestion_log
    register_schema_versions
)
missing_tables=()
for tbl in "${EXPECTED_TABLES[@]}"; do
    found=$("${PSQL[@]}" "SELECT to_regclass('control.${tbl}') IS NOT NULL;" 2>/dev/null)
    if [[ "${found}" == "t" ]]; then
        log_evidence "  ✓ control.${tbl}"
    else
        log_evidence "  ✗ control.${tbl} (MISSING)"
        missing_tables+=("${tbl}")
    fi
done

log_evidence ""
log_evidence "Summary: ${#missing_buckets[@]} buckets missing, ${#missing_tables[@]} control tables missing"

if [[ ${#missing_buckets[@]} -eq 0 && ${#missing_tables[@]} -eq 0 ]]; then
    log_finish PASS 0
else
    log_finish FAIL 1
fi
