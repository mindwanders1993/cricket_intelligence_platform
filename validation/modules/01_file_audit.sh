#!/usr/bin/env bash
# 01_file_audit — verify the completed-inventory paths exist.
#
# Tier: T1 (shell only)
# Mode: pre-push, pre-pr, milestone
#
# Fails the run if any expected file is missing. This is a foundation
# module — its failure halts downstream modules in run.sh.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "${SCRIPT_DIR}/../lib/log.sh"

log_init "${1:?usage: 01_file_audit.sh <out_dir>}"
cd "${REPO_ROOT}"

# Inventory of files the validation prompt expects to exist.
# Keep this list in sync with the validation prompt's "completed
# implementation inventory" section.
PATHS=(
    # Foundation
    ".env.example"
    "pyproject.toml"
    "Makefile"
    ".pre-commit-config.yaml"
    "infra/compose/compose.base.yml"
    "infra/compose/compose.dev.yml"
    "infra/bootstrap/create-buckets.sh"
    "infra/bootstrap/init-metastore.sql"
    "conf/base/storage.yaml"
    "conf/base/airflow.yaml"
    "conf/base/polars.yaml"
    "conf/base/duckdb.yaml"
    # Common
    "src/cip/common/settings.py"
    "src/cip/common/logging.py"
    "src/cip/common/exceptions.py"
    "src/cip/common/contracts/enums.py"
    "src/cip/common/contracts/naming.py"
    # IO
    "src/cip/ingestion/io/minio.py"
    # Contract doc
    "docs/architecture/source-warehouse-contracts.md"
    # Register pipeline
    "src/cip/ingestion/register/download.py"
    "src/cip/ingestion/register/normalize.py"
    "src/cip/ingestion/register/parse.py"
    "src/cip/ingestion/jobs/ingest_cricsheet_register.py"
    "src/cip/transform/shared/readers.py"
    "src/cip/transform/shared/writers.py"
    "src/cip/transform/shared/partitioning.py"
    "src/cip/transform/shared/iceberg.py"
    "src/cip/transform/polars/bronze/register_loader.py"
    "src/cip/transform/polars/silver/persons.py"
    "orchestration/airflow/dags/dag_ingest_cricsheet_register.py"
    "check_tables.py"
    # Tests
    "tests/unit/ingestion/register/test_normalize.py"
    "tests/unit/ingestion/register/test_parse.py"
    "tests/unit/transform/polars/bronze/test_register_loader.py"
)

found=0
missing=0
missing_paths=()

for path in "${PATHS[@]}"; do
    if [[ -e "${path}" ]]; then
        log_evidence "FOUND   ${path}"
        found=$((found + 1))
    else
        log_evidence "MISSING ${path}"
        missing=$((missing + 1))
        missing_paths+=("${path}")
    fi
done

log_evidence ""
log_evidence "Summary: ${found} found, ${missing} missing (of ${#PATHS[@]} expected)"

if [[ ${missing} -eq 0 ]]; then
    log_finish PASS 0
else
    log_evidence ""
    log_evidence "Missing paths:"
    for p in "${missing_paths[@]}"; do
        log_evidence "  - ${p}"
    done
    log_finish FAIL 1
fi
