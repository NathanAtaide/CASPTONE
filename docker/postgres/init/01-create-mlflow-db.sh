#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Runs ONCE, when the Postgres data volume is initialised for the first time
# (official image hook: /docker-entrypoint-initdb.d).
#
# The image itself creates the "airflow" role/database (POSTGRES_USER/DB). This
# script adds a separate role and database for the MLflow backend store so the
# two applications do not share credentials.
#
# No `set -u` / `exit` here on purpose: depending on the file permissions the
# official entrypoint may *source* this script instead of executing it.
# -----------------------------------------------------------------------------

mlflow_user="${MLFLOW_DB_USER:-mlflow}"
mlflow_db="${MLFLOW_DB_NAME:-mlflow}"
# Password rendered by the vault into the in-memory secrets volume.
mlflow_password="$(cat /run/secrets/env/MLFLOW_DB_PASSWORD)"

# psql variables (:"name" = identifier, :'name' = literal) keep any special
# character in the password safely quoted.
psql -v ON_ERROR_STOP=1 \
     --username "${POSTGRES_USER}" \
     --dbname "${POSTGRES_DB}" \
     -v mlflow_user="${mlflow_user}" \
     -v mlflow_db="${mlflow_db}" \
     -v mlflow_password="${mlflow_password}" <<'EOSQL'
CREATE ROLE :"mlflow_user" LOGIN PASSWORD :'mlflow_password';
CREATE DATABASE :"mlflow_db" OWNER :"mlflow_user";
EOSQL

echo "[postgres-init] created role and database for MLflow (${mlflow_db})"
