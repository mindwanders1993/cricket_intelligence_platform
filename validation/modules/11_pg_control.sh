#!/usr/bin/env bash
# 11_pg_control — inspect control.register_ingestion_log for the current snapshot.
#
# Tier: T1 (shell only)
# Mode: pre-pr, milestone
#
# Depends on: 09_run_pipeline (reads snapshot_date from state)
#
# Pass if at least one SUCCESS row exists for the snapshot date.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "${SCRIPT_DIR}/../lib/log.sh"
# shellcheck source=../lib/env.sh
source "${SCRIPT_DIR}/../lib/env.sh"
# shellcheck source=../lib/state.sh
source "${SCRIPT_DIR}/../lib/state.sh"

log_init "${1:?usage: 11_pg_control.sh <out_dir>}"
cd "${REPO_ROOT}"

load_env

if ! docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "compose-postgres-1"; then
    log_evidence "compose-postgres-1 not running"
    log_finish SKIP 0
fi

SNAPSHOT_DATE="$(state_get snapshot_date "$(date +%Y-%m-%d)")"
log_evidence "Querying control.register_ingestion_log for snapshot_date=${SNAPSHOT_DATE}"
log_evidence ""

PSQL=(docker exec compose-postgres-1 psql \
    -U "${POSTGRES_USER:-postgres}" \
    -d "${POSTGRES_DB:-cricket_platform}" \
    -tA -F$'\t')

# Latest rows for today's snapshot — emit a small detail table.
"${PSQL[@]}" -c "SELECT source_file, status, row_count, started_at, completed_at
                 FROM control.register_ingestion_log
                 WHERE snapshot_date = '${SNAPSHOT_DATE}'
                 ORDER BY started_at DESC
                 LIMIT 20;" > "${OUT_DIR}/stdout.log" 2> "${OUT_DIR}/stderr.log" || true

if [[ ! -s "${OUT_DIR}/stdout.log" ]]; then
    log_evidence "No rows found in control.register_ingestion_log for snapshot_date=${SNAPSHOT_DATE}"
    log_finish FAIL 1
fi

log_evidence "Recent rows (source_file | status | row_count | started_at | completed_at):"
while IFS= read -r row; do
    log_evidence "  ${row}"
done < "${OUT_DIR}/stdout.log"

# Pass criterion: at least one SUCCESS row for the snapshot.
success_count=$("${PSQL[@]}" -c \
    "SELECT COUNT(*) FROM control.register_ingestion_log
     WHERE snapshot_date = '${SNAPSHOT_DATE}' AND status = 'SUCCESS';" 2>/dev/null \
    | tr -d '[:space:]')

log_evidence ""
log_evidence "SUCCESS rows for ${SNAPSHOT_DATE}: ${success_count:-0}"

if [[ "${success_count:-0}" -gt 0 ]]; then
    log_finish PASS 0
else
    log_finish FAIL 1
fi
