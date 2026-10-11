#!/usr/bin/env bash
# shellcheck shell=bash
# -----------------------------------------------------------------------------
# platform-env.sh - shared secret loader. This file is SOURCED, never executed.
#
# The vault service renders one file per secret into the in-memory volume
# mounted at /run/secrets/env/<NAME>. This script turns those files into
# environment variables for the current shell and everything it starts.
#
# It is wired into the custom images in three ways:
#   1. each image entrypoint sources it before starting the real process
#      -> the service and all its children get the secrets;
#   2. BASH_ENV points at it -> `docker compose exec <svc> bash -c "..."` works;
#   3. ~/.bashrc sources it  -> interactive `docker compose exec <svc> bash` works.
#
# Rules: idempotent, silent, never calls `exit` (it runs inside other shells),
# and a variable already set on the container always wins over the vault value.
# -----------------------------------------------------------------------------

PLATFORM_SECRETS_ROOT="${PLATFORM_SECRETS_ROOT:-/run/secrets}"

# Block until the vault has rendered the secrets. Returns 1 on timeout.
platform_wait_for_vault() {
    local timeout="${VAULT_WAIT_SECONDS:-180}" waited=0
    while [ ! -f "${PLATFORM_SECRETS_ROOT}/.ready" ]; do
        if [ "${waited}" -ge "${timeout}" ]; then
            echo "[platform] ERROR: secrets not available after ${timeout}s." \
                 "Check the vault service: docker compose logs vault" >&2
            return 1
        fi
        sleep 1
        waited=$((waited + 1))
    done
    return 0
}

# Export every rendered secret as an environment variable (runs only once).
platform_load_secrets() {
    if [ -n "${PLATFORM_ENV_LOADED:-}" ]; then
        return 0
    fi
    if [ ! -f "${PLATFORM_SECRETS_ROOT}/.ready" ]; then
        return 0
    fi
    local file name
    for file in "${PLATFORM_SECRETS_ROOT}"/env/*; do
        [ -f "${file}" ] || continue
        name="${file##*/}"
        # Only valid variable names (the vault already enforces this).
        case "${name}" in
            ''|[0-9]*|*[!A-Za-z0-9_]*) continue ;;
        esac
        # Explicit container environment wins over the vault value.
        if [ -z "${!name+x}" ]; then
            export "${name}=$(cat "${file}")"
        fi
    done
    export PLATFORM_ENV_LOADED=1
    return 0
}

platform_load_secrets
