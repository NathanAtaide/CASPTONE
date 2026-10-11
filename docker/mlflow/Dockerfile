# -----------------------------------------------------------------------------
# mlflow image - MLflow tracking server + model registry.
#   backend store  : PostgreSQL (runs, params, metrics, registered models)
#   artifact store : s3://<mlflow bucket> on the data lake (model binaries)
# Both live in Docker volumes, so nothing is lost when containers are recreated.
# Build context: ./docker
# -----------------------------------------------------------------------------
ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim

ARG MLFLOW_VERSION=3.17.0

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY mlflow/requirements.txt /tmp/requirements.txt
RUN pip install "mlflow==${MLFLOW_VERSION}" -r /tmp/requirements.txt

COPY common/platform-env.sh /usr/local/bin/platform-env.sh
COPY mlflow/entrypoint.sh   /usr/local/bin/platform-entrypoint.sh
# Strip Windows line endings defensively and make the scripts executable.
RUN sed -i 's/\r$//' /usr/local/bin/platform-env.sh /usr/local/bin/platform-entrypoint.sh \
    && chmod 0755 /usr/local/bin/platform-env.sh /usr/local/bin/platform-entrypoint.sh \
    && echo 'source /usr/local/bin/platform-env.sh' >> /root/.bashrc

# Every bash started in the container (exec, scripts) loads the vault secrets.
ENV BASH_ENV=/usr/local/bin/platform-env.sh

EXPOSE 5000
ENTRYPOINT ["/usr/local/bin/platform-entrypoint.sh"]
