#!/usr/bin/env bash
# Thin wrappers around the Claude and Gemini CLIs for T2/T3 modules.
#
# Usage:
#   cat context_file | ai_small "your prompt"     > response.md
#   cat context_file | ai_big   "your prompt"     > response.md
#
# Conventions:
#   - The prompt is passed as the user message via the CLI's -p flag.
#   - Stdin is appended as additional user content (grounding context).
#   - Output (the assistant's reply) goes to stdout, captured by the caller.
#   - Exits non-zero if the CLI is missing or the call errored — callers
#     should map that to a SKIP or FAIL based on context.
#
# Token-cost gating:
#   - If SKIP_AI=1 is set in the environment, every AI call short-circuits
#     and returns exit code 99. Useful when iterating on the harness itself.

VALIDATION_AI_PROVIDER="${VALIDATION_AI_PROVIDER:-claude}"

if [[ "${VALIDATION_AI_PROVIDER}" == "gemini" ]]; then
    AI_SMALL_MODEL="${VALIDATION_SMALL_MODEL:-gemini-3-flash-preview}"
    AI_BIG_MODEL="${VALIDATION_BIG_MODEL:-gemini-3.1-pro-preview}"
    AI_CLI="gemini"
else
    AI_SMALL_MODEL="${VALIDATION_SMALL_MODEL:-claude-sonnet-4-6}"
    AI_BIG_MODEL="${VALIDATION_BIG_MODEL:-claude-opus-4-7}"
    AI_CLI="claude"
fi

ai_available() {
    command -v "${AI_CLI}" >/dev/null 2>&1
}

ai_call() {
    local model="${1:?ai_call: missing model}"
    local prompt="${2:?ai_call: missing prompt}"

    if [[ "${SKIP_AI:-0}" == "1" ]]; then
        return 99
    fi

    if ! ai_available; then
        echo "ERROR: '${AI_CLI}' CLI not on PATH. Install or set SKIP_AI=1." >&2
        return 127
    fi

    # Stream stdin (context) into the CLI alongside the prompt.
    "${AI_CLI}" -p "${prompt}" --model "${model}"
}

ai_small() {
    ai_call "${AI_SMALL_MODEL}" "$@"
}

ai_big() {
    ai_call "${AI_BIG_MODEL}" "$@"
}

# Parse a VERDICT: <TOKEN> line from a response file.
# Echoes the token (e.g. PASS, DRIFT, FAIL, WARN) or empty if absent.
ai_verdict() {
    local response_file="${1:?ai_verdict: missing response file}"
    grep -oE '^VERDICT:[[:space:]]*[A-Z_]+' "${response_file}" \
        | tail -1 \
        | awk '{print $2}'
}