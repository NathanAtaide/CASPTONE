#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Entrypoint wrapper of the Airflow image.
# 1. waits for the vault and exports every secret as an environment variable
#    (so tasks can read MYSQL_*, LAKE_S3_*, API tokens, ...);
# 2. publishes the source MySQL as an Airflow Connection (conn_id mysql_source);
# 3. hands over to the official Airflow entrypoint with the original arguments.
# -----------------------------------------------------------------------------
set -euo pipefail

# shellcheck source=/dev/null
source /usr/local/bin/platform-env.sh
platform_wait_for_vault
platform_load_secrets

# Airflow reads connections from AIRFLOW_CONN_<ID> variables. Building the JSON
# with Python keeps passwords with special characters intact.
if [[ -z "${AIRFLOW_CONN_MYSQL_SOURCE:-}" && -n "${MYSQL_HOST:-}" && "${MYSQL_HOST}" != "CHANGE_ME" ]]; then
    mysql_connection="$(python - <<'PY' || true
import json
import os

port = os.environ.get("MYSQL_PORT") or "3306"
print(json.dumps({
    "conn_type": "mysql",
    "host": os.environ["MYSQL_HOST"],
    "port": int(port) if port.isdigit() else 3306,
    "schema": os.environ.get("MYSQL_DATABASE", ""),
    "login": os.environ.get("MYSQL_USER", ""),
    "password": os.environ.get("MYSQL_PASSWORD", ""),
}))
PY
)"
    if [[ -n "${mysql_connection}" ]]; then
        export AIRFLOW_CONN_MYSQL_SOURCE="${mysql_connection}"
    fi
    unset mysql_connection
fi

exec /entrypoint "$@"
