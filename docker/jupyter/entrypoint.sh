#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Entrypoint of the Jupyter image.
# Waits for the vault, exports the secrets (JUPYTER_TOKEN, MYSQL_*, LAKE_S3_*,
# API tokens) and starts JupyterLab. Kernels and terminals inherit the
# environment, so notebooks reach the lake and MLflow without typing passwords.
# -----------------------------------------------------------------------------
set -euo pipefail

# shellcheck source=/dev/null
source /usr/local/bin/platform-env.sh
platform_wait_for_vault
platform_load_secrets

# JupyterLab reads its access token from JUPYTER_TOKEN. Refuse to start without
# one instead of silently falling back to a random token printed in the logs.
if [[ -z "${JUPYTER_TOKEN:-}" ]]; then
    echo "[jupyter] ERROR: JUPYTER_TOKEN is missing from the vault." >&2
    echo "[jupyter]        Fix: docker compose run --rm vault set JUPYTER_TOKEN" >&2
    exit 1
fi

exec "$@"
