#!/usr/bin/env bash
# infra/bootstrap/create-buckets.sh
#
# Bootstrap MinIO buckets and prefixes for the Cricket Intelligence Platform.
# Idempotent — safe to re-run on an existing MinIO instance.
#
# Usage:
#   ./infra/bootstrap/create-buckets.sh                  # uses .env defaults
#   MINIO_ALIAS=local ./infra/bootstrap/create-buckets.sh
#
# Prerequisites:
#   - mc (MinIO Client) installed and on $PATH, OR
#   - Docker available (script falls back to mc via Docker)
#   - MinIO reachable at MINIO_ENDPOINT (default: http://localhost:9000)
#
# Called by:
#   make bootstrap

set -euo pipefail

# ---------------------------------------------------------------------------
# Colour helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Colour

info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ---------------------------------------------------------------------------
# Load .env from repo root (two levels up from this script)
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

if [[ -f "${ENV_FILE}" ]]; then
    info "Loading environment from ${ENV_FILE}"
    set -a
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
    set +a
else
    warn ".env not found at ${REPO_ROOT}/.env — falling back to environment variables"
fi

# ---------------------------------------------------------------------------
# Configuration (all overridable via environment / .env)
# ---------------------------------------------------------------------------
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:?MINIO_ROOT_USER is required}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}"
MINIO_ALIAS="${MINIO_ALIAS:-cricket-local}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-60}"

# ---------------------------------------------------------------------------
# Bucket and prefix definitions
# Mirrors the medallion layer structure from HLD section 8.
# ---------------------------------------------------------------------------

# Core data lake buckets
BUCKETS=(
    "cricket-landing"       # Raw zips + extracted source files
    "cricket-bronze"        # Minimally processed, append-only Iceberg tables
    "cricket-silver"        # Normalised, identity-resolved Iceberg tables
    "cricket-gold"          # dbt-managed star schema marts
    "iceberg-warehouse"     # Iceberg REST catalog warehouse root
    "mlflow-artifacts"      # MLflow model artifacts, plots, and run files
)

# Landing zone prefixes — logical "folders" inside cricket-landing
LANDING_PREFIXES=(
    "cricket-landing/raw_zips/.keep"
    "cricket-landing/extracted_json/.keep"
    "cricket-landing/register_csv/.keep"
)

# Bronze prefixes — one per Bronze Iceberg table namespace
BRONZE_PREFIXES=(
    "cricket-bronze/match_documents/.keep"
    "cricket-bronze/register_people/.keep"
    "cricket-bronze/register_identifiers/.keep"
    "cricket-bronze/register_name_variations/.keep"
)

# Silver prefixes
SILVER_PREFIXES=(
    "cricket-silver/matches/.keep"
    "cricket-silver/innings/.keep"
    "cricket-silver/deliveries/.keep"
    "cricket-silver/wickets/.keep"
    "cricket-silver/teams/.keep"
    "cricket-silver/venues/.keep"
    "cricket-silver/competitions/.keep"
    "cricket-silver/persons/.keep"
    "cricket-silver/person_identifiers/.keep"
    "cricket-silver/match_players/.keep"
    "cricket-silver/match_officials/.keep"
)

# Gold prefixes
GOLD_PREFIXES=(
    "cricket-gold/dim_player/.keep"
    "cricket-gold/dim_match/.keep"
    "cricket-gold/dim_team/.keep"
    "cricket-gold/dim_venue/.keep"
    "cricket-gold/dim_competition/.keep"
    "cricket-gold/dim_date/.keep"
    "cricket-gold/fact_delivery/.keep"
    "cricket-gold/fact_innings/.keep"
    "cricket-gold/fact_match_result/.keep"
    "cricket-gold/fact_player_match/.keep"
)

# ---------------------------------------------------------------------------
# Resolve mc binary — prefer host install, fall back to Docker
# ---------------------------------------------------------------------------
resolve_mc() {
    if command -v mc &>/dev/null; then
        MC_CMD="mc"
        info "Using host mc: $(command -v mc)"
    elif command -v docker &>/dev/null; then
        warn "mc not found on PATH — using mc via Docker (quay.io/minio/minio)"
        MC_CMD="docker run --rm --network host \
            -e MC_HOST_${MINIO_ALIAS}=${MINIO_ENDPOINT} \
            quay.io/minio/minio:RELEASE.2024-11-07T00-52-20Z mc"
    else
        error "Neither mc nor Docker found. Install mc: https://min.io/docs/minio/linux/reference/minio-mc.html"
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# Wait for MinIO to be reachable
# ---------------------------------------------------------------------------
wait_for_minio() {
    info "Waiting for MinIO at ${MINIO_ENDPOINT} (max ${MAX_WAIT_SECONDS}s)..."
    local elapsed=0
    until curl -sf "${MINIO_ENDPOINT}/minio/health/live" &>/dev/null; do
        if (( elapsed >= MAX_WAIT_SECONDS )); then
            error "MinIO did not become healthy within ${MAX_WAIT_SECONDS}s"
            error "Check: docker compose -f infra/compose/compose.base.yml ps"
            exit 1
        fi
        sleep 3
        (( elapsed += 3 ))
        info "  ...still waiting (${elapsed}s elapsed)"
    done
    success "MinIO is healthy at ${MINIO_ENDPOINT}"
}

# ---------------------------------------------------------------------------
# Configure mc alias
# ---------------------------------------------------------------------------
configure_alias() {
    info "Configuring mc alias '${MINIO_ALIAS}' → ${MINIO_ENDPOINT}"
    ${MC_CMD} alias set "${MINIO_ALIAS}" \
        "${MINIO_ENDPOINT}" \
        "${MINIO_ROOT_USER}" \
        "${MINIO_ROOT_PASSWORD}" \
        --api S3v4 \
        --quiet
    success "Alias '${MINIO_ALIAS}' configured"
}

# ---------------------------------------------------------------------------
# Create buckets (idempotent)
# ---------------------------------------------------------------------------
create_buckets() {
    info "Creating buckets..."
    for bucket in "${BUCKETS[@]}"; do
        if ${MC_CMD} ls "${MINIO_ALIAS}/${bucket}" &>/dev/null; then
            warn "  Bucket already exists: ${bucket}"
        else
            ${MC_CMD} mb "${MINIO_ALIAS}/${bucket}"
            success "  Created bucket: ${bucket}"
        fi
    done
}

# ---------------------------------------------------------------------------
# Create prefix placeholders (idempotent)
# Touch a .keep object to materialise the prefix "folder" in MinIO UI.
# ---------------------------------------------------------------------------
create_prefixes() {
    info "Creating prefix placeholders..."

    local all_prefixes=(
        "${LANDING_PREFIXES[@]}"
        "${BRONZE_PREFIXES[@]}"
        "${SILVER_PREFIXES[@]}"
        "${GOLD_PREFIXES[@]}"
    )

    for prefix in "${all_prefixes[@]}"; do
        if ${MC_CMD} ls "${MINIO_ALIAS}/${prefix}" &>/dev/null; then
            warn "  Prefix already exists: ${prefix}"
        else
            # Pipe /dev/null as a zero-byte object
            ${MC_CMD} put /dev/null "${MINIO_ALIAS}/${prefix}" --quiet
            success "  Created prefix: ${prefix}"
        fi
    done
}

# ---------------------------------------------------------------------------
# Set bucket versioning — enabled on landing for safety, disabled elsewhere
# ---------------------------------------------------------------------------
configure_versioning() {
    info "Configuring bucket versioning..."
    ${MC_CMD} version enable "${MINIO_ALIAS}/cricket-landing" --quiet
    success "  Versioning enabled on cricket-landing (immutable raw zone)"

    for bucket in cricket-bronze cricket-silver cricket-gold iceberg-warehouse mlflow-artifacts; do
        ${MC_CMD} version suspend "${MINIO_ALIAS}/${bucket}" --quiet
        info "  Versioning suspended on ${bucket} (Iceberg manages snapshots)"
    done
}

# ---------------------------------------------------------------------------
# Print summary
# ---------------------------------------------------------------------------
print_summary() {
    echo ""
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}  Cricket Intelligence Platform — MinIO Bootstrap Complete${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "  ${CYAN}MinIO Console:${NC}  ${MINIO_ENDPOINT/9000/9001}"
    echo -e "  ${CYAN}Alias:${NC}          ${MINIO_ALIAS}"
    echo ""
    echo -e "  Buckets created:"
    for bucket in "${BUCKETS[@]}"; do
        echo -e "    ${GREEN}✓${NC}  s3://${bucket}/"
    done
    echo ""
    echo -e "  ${CYAN}Next step:${NC} make bootstrap-db   (run init-metastore.sql)"
    echo ""
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    echo ""
    info "=== Cricket Intelligence Platform — MinIO Bootstrap ==="
    info "Repo root: ${REPO_ROOT}"
    echo ""

    resolve_mc
    wait_for_minio
    configure_alias
    create_buckets
    create_prefixes
    configure_versioning
    print_summary
}

main "$@"
