"""
### dbt_medallion - bronze -> silver -> gold with dbt + DuckDB

Runs the dbt project mounted at `/opt/platform/dbt`. DuckDB reads the bronze
Parquet files from the data lake, applies the transformations and writes the
silver and gold models back to the lake as Parquet.

1. `dbt_example_selector` - checks whether the example dataset exists in bronze;
   while it does not, the bundled example models are skipped so they cannot
   break the run of your own models;
2. `dbt_build_silver` - builds and tests every model tagged `silver`;
3. `dbt_build_gold` - builds and tests every model tagged `gold`, then updates
   the `lake_gold` asset, which starts `gold_export_powerbi`.

**Schedule:** data-aware - it starts whenever a bronze ingestion finishes
(`bronze_mysql_ingestion` or `example_api_to_bronze`). It can also be triggered
manually.

dbt lives in its own virtualenv (`/opt/dbt-venv`) so its dependencies never
conflict with Airflow. The tasks use the single-slot `duckdb_warehouse` pool
because the DuckDB file accepts only one writer process at a time.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from caps_assets import BRONZE_EXAMPLE_API, BRONZE_MYSQL, GOLD

DBT_BIN = os.environ.get("DBT_BIN", "/opt/dbt-venv/bin/dbt")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/platform/dbt")

# Extra dbt arguments computed at run time by `dbt_example_selector` (Jinja template).
EXTRA_ARGUMENTS = "{{ ti.xcom_pull(task_ids='dbt_example_selector') or '' }}"


def dbt_command(arguments: str) -> str:
    """Shell command that runs dbt inside the project folder.

    The lake credentials and DuckDB settings reach dbt through the environment
    inherited from the Airflow worker (see dbt/profiles.yml).
    """
    return f"cd {DBT_PROJECT_DIR} && {DBT_BIN} --no-use-colors {arguments} {EXTRA_ARGUMENTS}"


with DAG(
    dag_id="dbt_medallion",
    description="Transforms bronze into silver and gold with dbt + DuckDB.",
    # Start when either bronze source has been refreshed.
    schedule=(BRONZE_MYSQL | BRONZE_EXAMPLE_API),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"pool": "duckdb_warehouse"},
    tags=["dbt", "silver", "gold"],
    doc_md=__doc__,
) as dag:

    @task
    def dbt_example_selector() -> str:
        """Return the dbt arguments that skip the example models when needed.

        The example models read `s3://<bronze>/open_meteo/hourly_weather/`. If the
        example DAG never ran, those files do not exist and the models would fail,
        so they are excluded until the data shows up.
        """
        from lakehouse import LakeConfig, connect
        from lakehouse.duck import sql_literal

        pattern = LakeConfig.from_env().uri("bronze", "open_meteo", "hourly_weather") + "/*/*.parquet"
        con = connect()
        try:
            files = con.execute(f"SELECT count(*) FROM glob({sql_literal(pattern)})").fetchone()[0]
        finally:
            con.close()
        # Always return a string: it is appended to the dbt command line.
        return "" if files else "--exclude tag:example"

    # `dbt build` = run the models and their tests, in dependency order.
    build_silver = BashOperator(
        task_id="dbt_build_silver",
        bash_command=dbt_command("build --select tag:silver"),
    )

    build_gold = BashOperator(
        task_id="dbt_build_gold",
        bash_command=dbt_command("build --select tag:gold"),
        outlets=[GOLD],
    )

    dbt_example_selector() >> build_silver >> build_gold
