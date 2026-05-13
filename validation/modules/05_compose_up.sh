#!/usr/bin/env bash
# 05_compose_up — bring up the local stack and wait for services to be healthy.
#
# Tier: T1 (shell only)
# Mode: pre-pr, milestone
# Foundation: failure halts the run.
#
# Health checks per service:
#   MinIO          GET http://localhost:9000/minio/health/live
#   PostgreSQL     pg_isready inside compose-postgres-1
#   Iceberg REST   TCP probe on localhost:8181 (image has no curl/wget)
#   Airflow web    GET http://localhost:8080/health

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "${SCRIPT_DIR}/../lib/log.sh"
# shellcheck source=../lib/env.sh
source "${SCRIPT_DIR}/../lib/env.sh"

log_init "${1:?usage: 05_compose_up.sh <out_dir>}"
cd "${REPO_ROOT}"

if ! command -v docker >/dev/null 2>&1; then
    log_evidence "docker not on PATH"
    log_finish FAIL 1
fi

if [[ ! -f .env ]]; then
    log_evidence ".env not found — copy from .env.example first"
    log_finish FAIL 1
fi

load_env

# Bring up the stack.
log_evidence "Running: make up"
exit_code=0
make up >> "${OUT_DIR}/stdout.log" 2>> "${OUT_DIR}/stderr.log" || exit_code=$?
if [[ ${exit_code} -ne 0 ]]; then
    log_evidence "make up: FAIL (exit ${exit_code})"
    log_finish FAIL "${exit_code}"
fi
log_evidence "make up: started"

# Healthcheck loop.
TIMEOUT_SECONDS=180
elapsed=0
interval=5

health_minio=0
health_postgres=0
health_iceberg=0
health_airflow=0

check_minio() {
    curl -sf --max-time 2 http://localhost:9000/minio/health/live >/dev/null 2>&1
}
check_postgres() {
    docker exec compose-postgres-1 pg_isready -U "${POSTGRES_USER:-postgres}" >/dev/null 2>&1
}
check_iceberg() {
    bash -c '</dev/tcp/localhost/8181' 2>/dev/null
}
check_airflow() {
    curl -sf --max-time 2 http://localhost:8080/health >/dev/null 2>&1
}

while (( elapsed < TIMEOUT_SECONDS )); do
    (( health_minio == 1 ))    || check_minio    && health_minio=1
    (( health_postgres == 1 )) || check_postgres && health_postgres=1
    (( health_iceberg == 1 ))  || check_iceberg  && health_iceberg=1
    (( health_airflow == 1 ))  || check_airflow  && health_airflow=1

    if (( health_minio == 1 && health_postgres == 1 && health_iceberg == 1 && health_airflow == 1 )); then
        break
    fi
    sleep "${interval}"
    (( elapsed += interval ))
done

log_evidence ""
log_evidence "Service health (after ${elapsed}s):"
(( health_minio == 1 )) && log_evidence "  ✓ minio" || log_evidence "  ✗ minio (still unhealthy)"
(( health_postgres == 1 )) && log_evidence "  ✓ postgres" || log_evidence "  ✗ postgres (still unhealthy)"
(( health_iceberg == 1 )) && log_evidence "  ✓ iceberg" || log_evidence "  ✗ iceberg (still unhealthy)"
(( health_airflow == 1 )) && log_evidence "  ✓ airflow" || log_evidence "  ✗ airflow (still unhealthy)"

# Capture compose ps for evidence
docker compose --env-file .env -f infra/compose/compose.base.yml -f infra/compose/compose.dev.yml ps \
    >> "${OUT_DIR}/stdout.log" 2>&1 || true

unhealthy=0
(( health_minio == 1 )) || unhealthy=$((unhealthy + 1))
(( health_postgres == 1 )) || unhealthy=$((unhealthy + 1))
(( health_iceberg == 1 )) || unhealthy=$((unhealthy + 1))
(( health_airflow == 1 )) || unhealthy=$((unhealthy + 1))

if (( unhealthy == 0 )); then
    log_finish PASS 0
else
    log_evidence ""
    log_evidence "${unhealthy} service(s) failed to become healthy within ${TIMEOUT_SECONDS}s."
    log_evidence "Check 'docker compose ... ps' output in stdout.log."
    log_finish FAIL 1
fi
