# -----------------------------------------------------------------------------
# airflow image - official Apache Airflow image extended with:
#   * DuckDB (processing engine) + its httpfs/mysql extensions pre-installed
#   * dbt-core + dbt-duckdb in an ISOLATED virtualenv (/opt/dbt-venv) so dbt's
#     dependencies can never conflict with Airflow's
#   * the forecasting stack (torch CPU, scikit-learn, xgboost, mlflow client,
#     holidays) in another isolated virtualenv (/opt/ml-venv), for the same reason
#   * the platform secret loader (vault -> environment variables)
# Used by: airflow-init, api-server, scheduler, dag-processor, triggerer and
# lake-init (bucket bootstrap).
# Build context: ./docker
# -----------------------------------------------------------------------------
ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root

COPY common/platform-env.sh      /usr/local/bin/platform-env.sh
COPY airflow/entrypoint.sh       /usr/local/bin/platform-entrypoint.sh
COPY airflow/airflow-init.sh     /usr/local/bin/airflow-init.sh
COPY airflow/airflow-db-uri.py   /usr/local/bin/airflow-db-uri

# - strip Windows line endings defensively and make the scripts executable
# - create the platform folders owned by the airflow user (group 0 writable so
#   an arbitrary AIRFLOW_UID keeps working, same convention as the base image)
# - load the secrets in interactive shells (`docker compose exec ... bash`)
RUN sed -i 's/\r$//' \
        /usr/local/bin/platform-env.sh \
        /usr/local/bin/platform-entrypoint.sh \
        /usr/local/bin/airflow-init.sh \
        /usr/local/bin/airflow-db-uri \
    && chmod 0755 \
        /usr/local/bin/platform-env.sh \
        /usr/local/bin/platform-entrypoint.sh \
        /usr/local/bin/airflow-init.sh \
        /usr/local/bin/airflow-db-uri \
    && mkdir -p \
        /opt/platform/src \
        /opt/platform/dbt \
        /opt/platform/dbt-runtime \
        /opt/platform/duckdb \
        /opt/platform/exports \
        /opt/dbt-venv \
        /opt/ml-venv \
    && chown -R airflow:0 /opt/platform /opt/dbt-venv /opt/ml-venv \
    && chmod -R g+rwX /opt/platform /opt/dbt-venv /opt/ml-venv \
    && echo 'source /usr/local/bin/platform-env.sh' >> /home/airflow/.bashrc

USER airflow

# Libraries used by the DAG tasks, installed next to Airflow.
COPY airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# dbt in its own virtualenv. PIP_USER is forced off because a virtualenv cannot
# receive "--user" installs.
COPY airflow/requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/dbt-venv \
    && PIP_USER=false /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt \
    && chmod -R g+rwX /opt/dbt-venv

# Forecasting libraries (ML/DL models + MLflow client) in their own virtualenv.
# The forecasting DAGs run `/opt/ml-venv/bin/python -m lakehouse.forecasting ...`.
COPY airflow/requirements-ml.txt /tmp/requirements-ml.txt
RUN python -m venv /opt/ml-venv \
    && PIP_USER=false /opt/ml-venv/bin/pip install --no-cache-dir -r /tmp/requirements-ml.txt \
    && chmod -R g+rwX /opt/ml-venv

# Download the DuckDB extensions at build time (~/.duckdb/extensions) so the
# containers never need internet access to load them. Airflow and the dbt
# virtualenv pin the same DuckDB version, therefore they share this cache.
RUN python -c "import duckdb; con = duckdb.connect(); [con.install_extension(name) for name in ('httpfs', 'mysql')]; print('duckdb', duckdb.__version__, 'extensions installed')"

# BASH_ENV: every non-interactive bash inside the container loads the secrets.
# PYTHONPATH: makes the shared `lakehouse` package (./src) and the helper
# modules that live next to the DAGs (caps_assets.py) importable everywhere.
# ML_PYTHON: interpreter of the forecasting virtualenv (used by the ml_forecast_* DAGs).
ENV BASH_ENV=/usr/local/bin/platform-env.sh \
    PYTHONPATH=/opt/platform/src:/opt/airflow/dags \
    DBT_BIN=/opt/dbt-venv/bin/dbt \
    ML_PYTHON=/opt/ml-venv/bin/python \
    GIT_PYTHON_REFRESH=quiet

# Same init process as the base image, but going through the secret loader first.
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/usr/local/bin/platform-entrypoint.sh"]
CMD []
