#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# One-shot initialisation of Airflow (service "airflow-init"). Idempotent: it
# runs on every `docker compose up` and only changes what is missing.
#   1. waits for Postgres and migrates the metadata database
#   2. creates the admin user and keeps its password in sync with the vault
#   3. creates the pools that protect shared resources
# Secrets (AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD) are already in the
# environment: the image entrypoint loaded them from the vault.
# -----------------------------------------------------------------------------
set -euo pipefail

: "${AIRFLOW_ADMIN_USER:?AIRFLOW_ADMIN_USER is missing from the vault}"
: "${AIRFLOW_ADMIN_PASSWORD:?AIRFLOW_ADMIN_PASSWORD is missing from the vault}"

echo "[airflow-init] waiting for the metadata database"
airflow db check --retry 30 --retry-delay 2

echo "[airflow-init] migrating the metadata database"
airflow db migrate

echo "[airflow-init] ensuring admin user '${AIRFLOW_ADMIN_USER}'"
# `users create` is a no-op when the user already exists; `reset-password`
# then makes the vault the single source of truth for the password.
airflow users create \
    --username "${AIRFLOW_ADMIN_USER}" \
    --password "${AIRFLOW_ADMIN_PASSWORD}" \
    --firstname "Platform" \
    --lastname "Admin" \
    --role "Admin" \
    --email "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}" || true
airflow users reset-password \
    --username "${AIRFLOW_ADMIN_USER}" \
    --password "${AIRFLOW_ADMIN_PASSWORD}"

echo "[airflow-init] ensuring pools"
# Limits concurrent extractions against the production MySQL.
airflow pools set mysql_source "${MYSQL_SOURCE_POOL_SLOTS:-2}" \
    "Concurrent queries allowed against the source MySQL"
# The DuckDB warehouse file accepts a single writer process at a time.
airflow pools set duckdb_warehouse 1 \
    "Serialises dbt runs that write to the DuckDB warehouse file"

echo "[airflow-init] done"
