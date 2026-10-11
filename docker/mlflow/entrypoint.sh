#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Entrypoint of the MLflow image.
# 1. waits for the vault and loads the secrets;
# 2. builds the Postgres backend URI (password URL-encoded) and the S3 settings
#    of the data lake from those secrets;
# 3. starts the tracking server, which proxies artifact access - clients such as
#    Jupyter only need MLFLOW_TRACKING_URI, never the lake credentials.
# -----------------------------------------------------------------------------
set -euo pipefail

# shellcheck source=/dev/null
source /usr/local/bin/platform-env.sh
platform_wait_for_vault
platform_load_secrets

: "${MLFLOW_DB_PASSWORD:?MLFLOW_DB_PASSWORD is missing from the vault}"
: "${LAKE_S3_ACCESS_KEY:?LAKE_S3_ACCESS_KEY is missing from the vault}"
: "${LAKE_S3_SECRET_KEY:?LAKE_S3_SECRET_KEY is missing from the vault}"

# Credentials and endpoint used by boto3 inside the server to reach the lake.
export AWS_ACCESS_KEY_ID="${LAKE_S3_ACCESS_KEY}"
export AWS_SECRET_ACCESS_KEY="${LAKE_S3_SECRET_KEY}"
export AWS_DEFAULT_REGION="${LAKE_S3_REGION:-us-east-1}"
if [[ "${LAKE_S3_USE_SSL:-false}" == "true" ]]; then
    export MLFLOW_S3_ENDPOINT_URL="https://${LAKE_S3_ENDPOINT:-minio:9000}"
else
    export MLFLOW_S3_ENDPOINT_URL="http://${LAKE_S3_ENDPOINT:-minio:9000}"
fi

# SQLAlchemy URI of the backend store; Python handles the URL-encoding.
backend_store_uri="$(python - <<'PY'
import os
from urllib.parse import quote

print("postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
    user=quote(os.environ.get("MLFLOW_DB_USER", "mlflow"), safe=""),
    password=quote(os.environ["MLFLOW_DB_PASSWORD"], safe=""),
    host=os.environ.get("POSTGRES_HOST", "postgres"),
    port=os.environ.get("POSTGRES_PORT", "5432"),
    database=os.environ.get("MLFLOW_DB_NAME", "mlflow"),
))
PY
)"

echo "[mlflow] backend store : postgresql://${MLFLOW_DB_USER:-mlflow}@${POSTGRES_HOST:-postgres}/${MLFLOW_DB_NAME:-mlflow}"
echo "[mlflow] artifact store: s3://${LAKE_BUCKET_MLFLOW:-mlflow}/ via ${MLFLOW_S3_ENDPOINT_URL}"

exec mlflow server \
    --host 0.0.0.0 \
    --port 5000 \
    --workers "${PLATFORM_MLFLOW_WORKERS:-2}" \
    --backend-store-uri "${backend_store_uri}" \
    --artifacts-destination "s3://${LAKE_BUCKET_MLFLOW:-mlflow}/" \
    --serve-artifacts
