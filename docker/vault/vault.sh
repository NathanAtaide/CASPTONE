#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# vault - secrets manager of the platform (SOPS + age).
#
# Files (bind-mounted from ./secrets on the host at /vault):
#   secrets.template.yaml  plaintext template, no real secrets (committed)
#   secrets.enc.yaml       encrypted secrets, values encrypted with age (committed)
#   keys/age.key           age private key - the ONLY thing to protect (git-ignored)
#
# Runtime contract (tmpfs volume mounted at /run/secrets in every container):
#   /run/secrets/env/<NAME>   one file per secret, raw value, no trailing newline
#   /run/secrets/.ready       marker written once every secret has been rendered
#
# Commands:
#   serve            (default) initialise if needed, decrypt to tmpfs, stay alive
#   init             create the age key and the encrypted file if they are missing
#   list             print the secret names (never the values)
#   get KEY          print one decrypted value
#   show             print the whole decrypted file
#   set KEY [VALUE]  create/update one secret (prompts when VALUE is omitted)
#   unset KEY        remove one secret
#   edit             open the decrypted file in an editor, re-encrypt on save
# -----------------------------------------------------------------------------
set -euo pipefail

VAULT_DIR="${VAULT_DIR:-/vault}"
ENC_FILE="${VAULT_DIR}/secrets.enc.yaml"
TEMPLATE_FILE="${VAULT_DIR}/secrets.template.yaml"
KEY_FILE="${VAULT_DIR}/keys/age.key"
OUT_DIR="${VAULT_OUT_DIR:-/run/secrets}"
ENV_DIR="${OUT_DIR}/env"
READY_FILE="${OUT_DIR}/.ready"
PLACEHOLDER="CHANGE_ME"
KEY_PATTERN='^[A-Za-z_][A-Za-z0-9_]*$'

# SOPS reads the age identity from this file when decrypting.
export SOPS_AGE_KEY_FILE="${KEY_FILE}"
# Plaintext scratch files (sops edit, init) are kept in RAM, never on disk.
export TMPDIR="/dev/shm"
export EDITOR="${EDITOR:-nano}"

log() { printf '[vault] %s\n' "$*"; }
die() { printf '[vault] ERROR: %s\n' "$*" >&2; exit 1; }

# --- random generators used by the template ---------------------------------

# N random bytes rendered as 2*N hexadecimal characters (URL-safe).
gen_hex() { head -c "$1" /dev/urandom | od -An -v -tx1 | tr -d ' \n'; }

# 32 random bytes as url-safe base64: the format Airflow expects for Fernet keys.
gen_fernet() { head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '\n'; }

# Print the template with every generator token replaced by a fresh value:
#   __GEN_HEX_<n>__   -> n random bytes in hex
#   __GEN_FERNET__    -> Fernet key
render_template() {
    local line
    while IFS= read -r line || [[ -n "${line}" ]]; do
        line="${line%$'\r'}"
        # Comment lines are copied untouched (they only document the tokens).
        if [[ "${line}" =~ ^[[:space:]]*# ]]; then
            printf '%s\n' "${line}"
            continue
        fi
        while [[ "${line}" =~ __GEN_HEX_([0-9]+)__ ]]; do
            line="${line/${BASH_REMATCH[0]}/$(gen_hex "${BASH_REMATCH[1]}")}"
        done
        while [[ "${line}" == *__GEN_FERNET__* ]]; do
            line="${line/__GEN_FERNET__/$(gen_fernet)}"
        done
        printf '%s\n' "${line}"
    done < "${TEMPLATE_FILE}"
}

# --- commands ---------------------------------------------------------------

require_initialised() {
    [[ -s "${KEY_FILE}" ]] || die "age key not found (${KEY_FILE}). Run: docker compose run --rm vault init"
    [[ -s "${ENC_FILE}" ]] || die "encrypted file not found (${ENC_FILE}). Run: docker compose run --rm vault init"
}

validate_key() {
    [[ "${1:-}" =~ ${KEY_PATTERN} ]] || die "invalid secret name '${1:-}' (use letters, digits and underscores, e.g. MYSQL_PASSWORD)"
}

cmd_init() {
    mkdir -p "${VAULT_DIR}/keys"

    if [[ -s "${ENC_FILE}" && ! -s "${KEY_FILE}" ]]; then
        die "found secrets.enc.yaml but the age key (secrets/keys/age.key) is missing. Restore the key from your backup: without it the secrets cannot be decrypted."
    fi

    if [[ ! -s "${KEY_FILE}" ]]; then
        rm -f "${KEY_FILE}"
        age-keygen -o "${KEY_FILE}" 2>/dev/null
        chmod 600 "${KEY_FILE}" 2>/dev/null || true
        log "generated a new age key at secrets/keys/age.key"
        log ">>> BACK THIS FILE UP somewhere safe and never commit it. <<<"
    fi

    if [[ ! -s "${ENC_FILE}" ]]; then
        [[ -f "${TEMPLATE_FILE}" ]] || die "template not found: ${TEMPLATE_FILE}"
        local recipient workdir plain
        recipient="$(age-keygen -y "${KEY_FILE}")"
        workdir="$(mktemp -d /dev/shm/vault.XXXXXX)"
        plain="${workdir}/secrets.yaml"
        render_template > "${plain}"
        sops encrypt --age "${recipient}" "${plain}" > "${ENC_FILE}.tmp"
        rm -rf "${workdir}"
        mv "${ENC_FILE}.tmp" "${ENC_FILE}"
        log "created secrets/secrets.enc.yaml with freshly generated internal passwords"
    fi
}

# Decrypt every secret into the tmpfs volume (one file per key).
cmd_render() {
    require_initialised
    rm -f "${READY_FILE}"
    mkdir -p "${ENV_DIR}"
    chmod 0755 "${OUT_DIR}" "${ENV_DIR}"
    find "${ENV_DIR}" -type f -delete

    local json key pending=()
    # The decrypted document only ever lives in this shell variable (RAM).
    json="$(sops decrypt --output-type json "${ENC_FILE}")" || die "could not decrypt ${ENC_FILE} with ${KEY_FILE}"

    while IFS= read -r key; do
        [[ -n "${key}" ]] || continue
        if [[ ! "${key}" =~ ${KEY_PATTERN} ]]; then
            log "WARNING: skipping '${key}' (not a valid environment variable name)"
            continue
        fi
        jq -j --arg k "${key}" \
            '.[$k] | if . == null then "" elif type == "string" then . else tojson end' \
            <<< "${json}" > "${ENV_DIR}/${key}"
        chmod 0444 "${ENV_DIR}/${key}"
        if [[ "$(cat "${ENV_DIR}/${key}")" == "${PLACEHOLDER}" ]]; then
            pending+=("${key}")
        fi
    done < <(jq -r 'keys_unsorted[]' <<< "${json}")

    date -u '+%Y-%m-%dT%H:%M:%SZ' > "${READY_FILE}"
    log "rendered $(find "${ENV_DIR}" -type f | wc -l | tr -d ' ') secrets into ${ENV_DIR} (tmpfs)"

    if (( ${#pending[@]} > 0 )); then
        log "WARNING: these secrets still hold the placeholder '${PLACEHOLDER}': ${pending[*]}"
        log "         set them with:  docker compose run --rm vault set <NAME>"
        log "         then apply with: docker compose up -d --force-recreate"
    fi
}

cmd_serve() {
    cmd_init
    cmd_render
    log "ready - keeping the in-memory secrets volume mounted"
    # Stay alive so the tmpfs volume is never unmounted; stop quickly on SIGTERM.
    trap 'log "stopping"; exit 0' TERM INT
    while true; do
        sleep 3600 &
        wait $! || true
    done
}

cmd_list() {
    require_initialised
    sops decrypt --output-type json "${ENC_FILE}" | jq -r 'keys_unsorted[]'
}

cmd_get() {
    require_initialised
    validate_key "${1:-}"
    sops decrypt --extract "[\"$1\"]" "${ENC_FILE}"
    echo
}

cmd_show() {
    require_initialised
    sops decrypt "${ENC_FILE}"
}

cmd_set() {
    require_initialised
    validate_key "${1:-}"
    local key="$1" value
    if (( $# >= 2 )); then
        value="$2"
    else
        # Hidden prompt: the value never reaches the shell history.
        read -r -s -p "Value for ${key}: " value || die "no value provided"
        echo
    fi
    # sops expects the value JSON-encoded; jq takes care of the escaping.
    sops set "${ENC_FILE}" "[\"${key}\"]" "$(jq -n --arg v "${value}" '$v')"
    log "saved ${key}. Apply it with: docker compose up -d --force-recreate"
}

cmd_unset() {
    require_initialised
    validate_key "${1:-}"
    sops unset "${ENC_FILE}" "[\"$1\"]"
    log "removed $1. Apply it with: docker compose up -d --force-recreate"
}

cmd_edit() {
    require_initialised
    sops edit "${ENC_FILE}"
    log "done. Apply the changes with: docker compose up -d --force-recreate"
}

usage() {
    cat <<'EOF'
Usage: docker compose run --rm vault <command>

  list             show the secret names (not the values)
  get KEY          print one decrypted value
  show             print every decrypted secret
  set KEY [VALUE]  create/update a secret (hidden prompt when VALUE is omitted)
  unset KEY        delete a secret
  edit             edit all secrets in an editor (re-encrypted on save)
  init             create the age key / encrypted file if missing
  serve            decrypt into /run/secrets and stay alive (service default)

After changing secrets, apply them with: docker compose up -d --force-recreate
EOF
}

command="${1:-serve}"
shift || true
case "${command}" in
    serve)  cmd_serve ;;
    init)   cmd_init ;;
    render) cmd_render ;;
    list)   cmd_list ;;
    get)    cmd_get "$@" ;;
    show)   cmd_show ;;
    set)    cmd_set "$@" ;;
    unset)  cmd_unset "$@" ;;
    edit)   cmd_edit ;;
    help|-h|--help) usage ;;
    *) usage >&2; exit 2 ;;
esac
