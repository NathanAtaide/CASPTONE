# -----------------------------------------------------------------------------
# jupyter image - JupyterLab workspace for exploration and ML modelling.
# Ships DuckDB (same version as the pipelines), the MLflow client and the usual
# scientific stack. The shared `lakehouse` package is mounted at /opt/platform/src.
# Build context: ./docker
# -----------------------------------------------------------------------------
ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# tini reaps kernel processes; git is handy for notebooks under version control.
RUN apt-get update \
    && apt-get install -y --no-install-recommends tini git \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 1000 --shell /bin/bash jovyan

COPY jupyter/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY common/platform-env.sh /usr/local/bin/platform-env.sh
COPY jupyter/entrypoint.sh  /usr/local/bin/platform-entrypoint.sh
# Strip Windows line endings defensively, make the scripts executable and load
# the secrets in the terminals opened from JupyterLab.
RUN sed -i 's/\r$//' /usr/local/bin/platform-env.sh /usr/local/bin/platform-entrypoint.sh \
    && chmod 0755 /usr/local/bin/platform-env.sh /usr/local/bin/platform-entrypoint.sh \
    && mkdir -p /home/jovyan/work /home/jovyan/exports /opt/platform/src \
    && echo 'source /usr/local/bin/platform-env.sh' >> /home/jovyan/.bashrc \
    && chown -R jovyan:jovyan /home/jovyan

USER jovyan
WORKDIR /home/jovyan/work

# Pre-install the DuckDB extensions so notebooks work without internet access.
RUN python -c "import duckdb; con = duckdb.connect(); [con.install_extension(name) for name in ('httpfs', 'mysql')]; print('duckdb', duckdb.__version__, 'extensions installed')"

# BASH_ENV: non-interactive shells load the vault secrets.
# PYTHONPATH: makes the shared `lakehouse` package importable in notebooks.
ENV BASH_ENV=/usr/local/bin/platform-env.sh \
    PYTHONPATH=/opt/platform/src \
    SHELL=/bin/bash

EXPOSE 8888
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/platform-entrypoint.sh"]
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--ServerApp.root_dir=/home/jovyan/work"]
