#!/usr/bin/env bash
# 15_airflow_e2e — trigger the DAG via Airflow REST API and verify it completes.
#
# Tier: T1 (shell only)
# Mode: milestone (heavy — 2-5 minutes)
#
# Uses a synthetic snapshot_date of 9999-01-01 to isolate from real data.
# Teardown removes the synthetic row from control.register_ingestion_log so
# subsequent runs are not blocked by the idempotency guard. The Iceberg
# partition for 9999-01-01 is left in place (harmless; flag for manual cleanup
# if it accumulates).
#
# Uses the Airflow REST API (localhost:8080/api/v1) instead of the CLI to
# avoid the execution_date vs run_id ambiguity in Airflow 2.x CLI commands.
# Pre-trigger cleanup removes any queued/running runs so max_active_runs=1
# does not block the validation trigger.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "${SCRIPT_DIR}/../lib/log.sh"
# shellcheck source=../lib/env.sh
source "${SCRIPT_DIR}/../lib/env.sh"

log_init "${1:?usage: 15_airflow_e2e.sh <out_dir>}"
cd "${REPO_ROOT}"

load_env

DAG_ID="dag_ingest_cricsheet_register"
SCHEDULER="compose-airflow-scheduler-1"
POSTGRES="compose-postgres-1"
SYNTHETIC_DATE="9999-01-01"
RUN_ID="validation_$(date +%Y%m%d_%H%M%S)"
TIMEOUT_SECONDS=300
POLL_INTERVAL=10

AF_BASE="http://localhost:${AIRFLOW_WEBSERVER_PORT:-8080}/api/v1"
AF_USER="${AIRFLOW_ADMIN_USER:-admin}"
AF_PASS="${AIRFLOW_ADMIN_PASSWORD:-admin}"

# Helper: authenticated curl to Airflow REST API.
af_api() {
    local path="${1}"; shift
    curl -sf --max-time 15 \
        -u "${AF_USER}:${AF_PASS}" \
        "${AF_BASE}/${path}" "$@"
}

# Helper: extract a top-level string field from a JSON object (no jq required).
json_str() {
    python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('${1}',''))" 2>/dev/null || true
}

if ! command -v docker >/dev/null 2>&1; then
    log_evidence "docker not on PATH"
    log_finish SKIP 0
fi

if ! docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "${SCHEDULER}"; then
    log_evidence "${SCHEDULER} not running"
    log_finish SKIP 0
fi

# Verify REST API is reachable.
if ! af_api "health" > "${OUT_DIR}/health.log" 2>&1; then
    log_evidence "Airflow REST API not reachable at ${AF_BASE}"
    log_finish SKIP 0
fi

PSQL=(docker exec "${POSTGRES}" psql \
    -U "${POSTGRES_USER:-postgres}" \
    -d "${POSTGRES_DB:-cricket_platform}" \
    -tA)

log_evidence "DAG:           ${DAG_ID}"
log_evidence "Run id:        ${RUN_ID}"
log_evidence "Snapshot date: ${SYNTHETIC_DATE} (synthetic)"
log_evidence ""

# ---------------------------------------------------------------------------
# Pre-trigger: clear stale queued/running runs (max_active_runs=1 guard)
# ---------------------------------------------------------------------------
log_evidence "Pre-trigger: clearing stale queued/running runs"
for stuck_state in queued running; do
    stale_json="$(af_api "dags/${DAG_ID}/dagRuns?state=${stuck_state}&limit=10" 2>/dev/null || true)"
    stale_runs="$(echo "${stale_json}" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for r in data.get('dag_runs', []):
    print(r['dag_run_id'])
" 2>/dev/null || true)"
    while IFS= read -r stale_id; do
        [[ -z "${stale_id}" ]] && continue
        log_evidence "  deleting stale ${stuck_state} run: ${stale_id}"
        af_api "dags/${DAG_ID}/dagRuns/${stale_id}" -X DELETE > /dev/null 2>&1 || true
    done <<< "${stale_runs}"
done
log_evidence "  ✓ pre-trigger cleanup done"
log_evidence ""

# ---------------------------------------------------------------------------
# Step 1 — trigger via REST API
# ---------------------------------------------------------------------------
log_evidence "Step 1: trigger"
trigger_response="$(af_api "dags/${DAG_ID}/dagRuns" \
    -X POST \
    -H "Content-Type: application/json" \
    -d "{\"dag_run_id\":\"${RUN_ID}\",\"conf\":{\"snapshot_date\":\"${SYNTHETIC_DATE}\",\"force\":true}}" \
    2>&1 || true)"
echo "${trigger_response}" > "${OUT_DIR}/trigger.log"

initial_state="$(echo "${trigger_response}" | json_str state)"
if [[ -z "${initial_state}" ]]; then
    log_evidence "  ✗ trigger failed — no state in response"
    cat "${OUT_DIR}/trigger.log" >> "${EVIDENCE_FILE}"
    log_finish FAIL 1
fi
log_evidence "  ✓ triggered (initial state: ${initial_state})"

# ---------------------------------------------------------------------------
# Step 2 — poll for completion via REST API
# ---------------------------------------------------------------------------
log_evidence ""
log_evidence "Step 2: poll (timeout ${TIMEOUT_SECONDS}s)"
elapsed=0
final_state=""
while (( elapsed < TIMEOUT_SECONDS )); do
    state="$(af_api "dags/${DAG_ID}/dagRuns/${RUN_ID}" 2>/dev/null | json_str state)"
    log_evidence "  [${elapsed}s] state=${state:-<empty>}"
    if [[ "${state}" == "success" || "${state}" == "failed" ]]; then
        final_state="${state}"
        break
    fi
    sleep "${POLL_INTERVAL}"
    (( elapsed += POLL_INTERVAL ))
done
log_evidence "  final DAG state: ${final_state:-<timeout>}"
log_evidence "  elapsed:         ${elapsed}s"

# ---------------------------------------------------------------------------
# Step 3 — task states via REST API
# ---------------------------------------------------------------------------
log_evidence ""
log_evidence "Step 3: task states"
task_json="$(af_api "dags/${DAG_ID}/dagRuns/${RUN_ID}/taskInstances" 2>/dev/null || true)"
echo "${task_json}" > "${OUT_DIR}/task_states.log"

failed_tasks=0
while IFS=$'\t' read -r task_id task_state; do
    [[ -z "${task_id}" ]] && continue
    log_evidence "  ${task_id}: ${task_state:-unknown}"
    case "${task_state}" in
        success|skipped) : ;;
        failed|upstream_failed|removed) failed_tasks=$((failed_tasks + 1)) ;;
    esac
done < <(echo "${task_json}" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for ti in data.get('task_instances', []):
    print(ti.get('task_id','') + '\t' + (ti.get('state') or 'none'))
" 2>/dev/null || true)

# ---------------------------------------------------------------------------
# Step 4 — teardown (best effort)
# ---------------------------------------------------------------------------
log_evidence ""
log_evidence "Step 4: teardown (synthetic ${SYNTHETIC_DATE})"
"${PSQL[@]}" -c "DELETE FROM control.register_ingestion_log
                 WHERE snapshot_date='${SYNTHETIC_DATE}';" \
    > "${OUT_DIR}/teardown.log" 2>&1 || true
log_evidence "  ✓ removed control.register_ingestion_log rows for ${SYNTHETIC_DATE}"
log_evidence "  ⚠ Iceberg partition _snapshot_date=${SYNTHETIC_DATE} remains in bronze/silver tables"
log_evidence "    (cleanup is a manual PyIceberg operation — defer to v3)"

# ---------------------------------------------------------------------------
# Final verdict
# ---------------------------------------------------------------------------
log_evidence ""
log_evidence "Verdict:"
log_evidence "  DAG final state: ${final_state:-<timeout>}"
log_evidence "  Failed tasks:    ${failed_tasks}"

if [[ "${final_state}" == "success" && ${failed_tasks} -eq 0 ]]; then
    log_finish PASS 0
else
    log_finish FAIL 1
fi
