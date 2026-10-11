"""
### bronze_mysql_ingestion - batch ingestion MySQL -> bronze

Extracts the tables listed in `config/mysql_tables.yml` from the source MySQL
and stores them as Parquet in the **bronze** bucket of the data lake:

`s3://bronze/mysql/<table>/ingestion_date=YYYY-MM-DD/<file>.parquet`

* one mapped task per table (the `mysql_source` pool limits how many run at once);
* `full` tables are re-extracted entirely, `incremental` tables only bring rows
  newer than what bronze already holds (watermark column);
* when at least one table is loaded, the `lake_bronze_mysql` asset is updated,
  which starts the `dbt_medallion` DAG.

**To add a table:** edit `airflow/dags/config/mysql_tables.yml` - no code needed.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow.sdk import dag, get_current_context, task

from caps_assets import BRONZE_MYSQL

log = logging.getLogger(__name__)

CONFIG_FILE = Path(__file__).parent / "config" / "mysql_tables.yml"


def _load_config() -> dict:
    """Read the ingestion plan. Parsed on every DAG refresh, so keep it small."""
    with CONFIG_FILE.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


_CONFIG = _load_config()
# Plain dicts (JSON-serialisable) so they can be passed to dynamically mapped tasks.
TABLES: list[dict] = [dict(table) for table in (_CONFIG.get("tables") or [])]


@dag(
    dag_id="bronze_mysql_ingestion",
    description="Batch ingestion of the source MySQL tables into the bronze layer.",
    schedule=_CONFIG.get("schedule"),  # cron from mysql_tables.yml (null = manual)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["bronze", "mysql", "ingestion"],
    doc_md=__doc__,
)
def bronze_mysql_ingestion():
    @task(pool="mysql_source", map_index_template="{{ table_name }}")
    def ingest_table(table: dict, dag_run=None) -> dict:
        """Extract one table from MySQL and write it to bronze."""
        from lakehouse import mysql_source
        from lakehouse.naming import partition_date, safe_name

        spec = mysql_source.TableSpec.from_dict(table)
        # Shows the table name (instead of an index) in the Airflow UI.
        get_current_context()["table_name"] = spec.name

        return mysql_source.ingest_table(
            spec,
            batch_id=safe_name(dag_run.run_id),
            ingestion_date=str(partition_date(getattr(dag_run, "run_after", None))),
        )

    @task(outlets=[BRONZE_MYSQL])
    def publish(results: list[dict]) -> dict:
        """Summarise the run and signal that bronze was refreshed (asset event)."""
        results = list(results)
        total_rows = sum(result["rows"] for result in results)
        for result in results:
            log.info("%s (%s): %s rows -> %s", result["table"], result["mode"], result["rows"], result["target"])
        log.info("Ingested %s tables, %s rows in total.", len(results), total_rows)
        return {"tables": len(results), "rows": total_rows}

    # With an empty table list the mapped task and `publish` are skipped, so
    # nothing is triggered downstream.
    publish(ingest_table.expand(table=TABLES))


bronze_mysql_ingestion()
