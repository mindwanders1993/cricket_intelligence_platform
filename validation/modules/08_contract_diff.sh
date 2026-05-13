#!/usr/bin/env bash
# 08_contract_diff — AI compares the contract doc against implementation files.
#
# Tier: T2 (T2 Model)
# Mode: milestone
#
# Inputs: docs/architecture/source-warehouse-contracts.md + a curated set of
# implementation files. AI emits a free-form report ending with a VERDICT line.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck source=../lib/log.sh
source "${SCRIPT_DIR}/../lib/log.sh"
# shellcheck source=../lib/ai.sh
source "${SCRIPT_DIR}/../lib/ai.sh"

log_init "${1:?usage: 08_contract_diff.sh <out_dir>}"
cd "${REPO_ROOT}"

CONTRACT_DOC="docs/architecture/source-warehouse-contracts.md"
IMPL_FILES=(
    "src/cip/common/contracts/naming.py"
    "src/cip/common/contracts/enums.py"
    "src/cip/ingestion/register/parse.py"
    "src/cip/ingestion/register/normalize.py"
    "src/cip/transform/polars/bronze/register_loader.py"
    "src/cip/transform/polars/silver/persons.py"
    "infra/bootstrap/init-metastore.sql"
)

if [[ ! -f "${CONTRACT_DOC}" ]]; then
    log_evidence "Contract doc not found: ${CONTRACT_DOC}"
    log_finish FAIL 1
fi

if ! ai_available && [[ "${SKIP_AI:-0}" != "1" ]]; then
    log_evidence "AI CLI not on PATH and SKIP_AI not set"
    log_finish SKIP 0
fi

# Build grounding context: contract first, then each impl file with a header.
CONTEXT_FILE="${OUT_DIR}/context.md"
{
    echo "## CONTRACT DOCUMENT"
    echo ""
    echo "Path: ${CONTRACT_DOC}"
    echo ""
    cat "${CONTRACT_DOC}"
    echo ""
    for f in "${IMPL_FILES[@]}"; do
        echo ""
        echo "## IMPLEMENTATION FILE: ${f}"
        echo ""
        echo '```python'
        if [[ -f "${f}" ]]; then
            cat "${f}"
        else
            echo "FILE MISSING: ${f}"
        fi
        echo '```'
    done
} > "${CONTEXT_FILE}"

context_bytes=$(wc -c < "${CONTEXT_FILE}")
log_evidence "Context size: ${context_bytes} bytes (${#IMPL_FILES[@]} impl files + contract)"

PROMPT=
IFS='' read -r -d '' PROMPT <<'PROMPT_END' || true
You are auditing whether the implementation matches the documented source/warehouse contract.

Examine the CONTRACT DOCUMENT and each IMPLEMENTATION FILE below.

For each section of the contract — source files, schema, metadata columns, partition strategy, idempotency, control schema — answer: does the code match?

Report only concrete discrepancies. For each, cite the file path and the contract section in one sentence. Skip cosmetic differences. If there are no discrepancies in a section, say so in one line.

End your response with exactly one line, all caps:
VERDICT: PASS    (implementation aligns with contract)
VERDICT: DRIFT   (one or more concrete discrepancies exist)
VERDICT: UNCERTAIN (you cannot determine from the provided context)
PROMPT_END

log_evidence ""
log_evidence "Invoking T2 model for contract-vs-code audit..."

ai_exit=0
ai_small "${PROMPT}" < "${CONTEXT_FILE}" > "${OUT_DIR}/ai_response.md" 2> "${OUT_DIR}/ai_stderr.log" || ai_exit=$?

case "${ai_exit}" in
    0) ;;
    99) log_evidence "SKIP_AI=1 set — skipping"; log_finish SKIP 0 ;;
    127) log_evidence "AI CLI missing"; log_finish SKIP 0 ;;
    *) log_evidence "AI call failed (exit ${ai_exit})"
       tail -10 "${OUT_DIR}/ai_stderr.log" >> "${EVIDENCE_FILE}" 2>/dev/null
       log_finish FAIL "${ai_exit}" ;;
esac

verdict="$(ai_verdict "${OUT_DIR}/ai_response.md")"
log_evidence ""
log_evidence "Verdict from AI: ${verdict:-<missing>}"
log_evidence "Full response: ai_response.md"

case "${verdict}" in
    PASS)      log_finish PASS 0 ;;
    DRIFT)     log_finish FAIL 1 ;;
    UNCERTAIN) log_finish FAIL 2 ;;
    *)         log_evidence "AI did not emit a VERDICT line — treating as FAIL"
               log_finish FAIL 3 ;;
esac
