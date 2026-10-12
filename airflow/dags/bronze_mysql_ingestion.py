"""
### bronze_mysql_ingestion - MySQL -> bronze, in batches

Copies the tables of the source MySQL into the **bronze** layer as Parquet:

`s3://bronze/mysql/<dataset>/ingestion_date=YYYY-MM-DD/<table>-<load_id>-<n>.parquet`

* **Batches.** Each table is walked in key order and written `chunk_size` rows
  at a time, one Parquet file per batch. The position is saved after every
  batch, so a retry or the next run **continues where the previous one stopped**
  and, once a table is complete, later runs only bring its new rows.
* **Unified tables.** Tables split in MySQL (`emails`, `emails_1`, ...) are
  loaded into a single bronze dataset. The column `_source_entity` tells which
  table each row came from.
* **Plan.** `plan` reads `config/mysql_tables.yml`, lists the MySQL tables and
  decides which table feeds which dataset. Its log shows the result.
* **Load.** `load_table` runs once per source table (the `mysql_source` pool
  limits how many run at the same time).
* **Publish.** When new rows arrived, the `lake_bronze_mysql` asset is updated,
  which starts `dbt_medallion`.

**Parameters when triggering manually**

| Parameter | Use |
|---|---|
| `dry_run` | Only show the plan in the log, load nothing. |
| `datasets` | Load only these datasets (empty = all). |
| `max_chunks_per_table` | Stop each table after this many batches (0 = until it is complete). |
| `full_refresh` | Forget the saved position and reload the selected tables from the start. |

Test the mechanism first with the `bronze_mysql_ingestion_test` DAG.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow.sdk import Param, dag, get_current_context, task

from caps_assets import BRONZE_MYSQL

log = logging.getLogger(__name__)

CONFIG_FILE = Path(__file__).parent / "config" / "mysql_tables.yml"


def _load_config() -> dict:
    """Read the ingestion plan (small file, parsed on every DAG refresh and run)."""
    with CONFIG_FILE.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


@dag(
    dag_id="bronze_mysql_ingestion",
    description="Loads the source MySQL tables into the bronze layer, in batches.",
    schedule=_load_config().get("schedule"),  # cron from mysql_tables.yml (null = manual)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # two runs must never move the same cursor at the same time
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["bronze", "mysql", "ingestion"],
    doc_md=__doc__,
    params={
        "dry_run": Param(
            False,
            type="boolean",
            title="Dry run",
            description="Only log which tables would be loaded into which dataset.",
        ),
        "datasets": Param(
            [],
            type="array",
            items={"type": "string"},
            title="Datasets",
            description="Bronze datasets to load (e.g. emails, product). Empty = all.",
        ),
        "max_chunks_per_table": Param(
            0,
            type="integer",
            minimum=0,
            title="Max batches per table",
            description="Stop each table after this many batches. 0 = run until the table is complete.",
        ),
        "full_refresh": Param(
            False,
            type="boolean",
            title="Full refresh",
            description="Forget the saved position and reload the selected tables from the start.",
        ),
    },
)
def bronze_mysql_ingestion():
    @task(pool="mysql_source")
    def plan(params: dict | None = None) -> list[dict]:
        """Decide which source tables are loaded into which bronze dataset."""
        # Imported inside the task so DAG parsing stays fast and dependency-free.
        from lakehouse import mysql_batch

        params = params or {}
        items = mysql_batch.resolve_work_items(_load_config(), mysql_batch.list_base_tables())

        selected = {name for name in (params.get("datasets") or []) if name}
        if selected:
            known = {item["dataset"] for item in items}
            unknown = sorted(selected - known)
            if unknown:
                raise ValueError(f"Unknown datasets: {unknown}. Available: {sorted(known)}")
            items = [item for item in items if item["dataset"] in selected]

        # Run-level parameters travel inside each item to the mapped task.
        max_chunks = int(params.get("max_chunks_per_table") or 0)
        for item in items:
            item["full_refresh"] = bool(params.get("full_refresh"))
            if max_chunks:
                item["max_chunks"] = max_chunks

        log.info("Ingestion plan (%s source tables):", len(items))
        for item in items:
            log.info(
                "  %-24s <- %-24s mode=%s chunk_size=%s",
                item["dataset"], item["table"], item["mode"], item["chunk_size"],
            )

        if params.get("dry_run"):
            log.info("dry_run is on: nothing will be loaded.")
            return []
        return items

    @task(pool="mysql_source", map_index_template="{{ work_label }}")
    def load_table(item: dict, dag_run=None) -> dict:
        """Load one source table into its bronze dataset (continues from the saved position)."""
        from lakehouse import mysql_batch
        from lakehouse.naming import partition_date, safe_name

        # Shows "dataset <- table" (instead of an index) in the Airflow UI.
        label = item["table"] if item["table"] == item["dataset"] else f"{item['dataset']} <- {item['table']}"
        get_current_context()["work_label"] = label

        return mysql_batch.run_work_item(
            item,
            batch_id=safe_name(dag_run.run_id),
            ingestion_date=str(partition_date(getattr(dag_run, "run_after", None))),
        )

    @task(outlets=[BRONZE_MYSQL])
    def publish(results: list[dict]) -> dict:
        """Summarise the run and signal that bronze has new data (asset event)."""
        results = list(results)
        total_rows = sum(result["rows"] for result in results)

        for result in results:
            log.info(
                "%s <- %s: %s rows in %s batches%s",
                result["dataset"], result["table"], result["rows"], result.get("chunks", 1),
                "" if result.get("complete", True) else "  (NOT complete yet)",
            )
        pending = sorted({result["table"] for result in results if not result.get("complete", True)})
        if pending:
            log.warning("Tables not complete yet - trigger the DAG again to continue: %s", pending)
        log.info("Loaded %s rows from %s source tables.", total_rows, len(results))

        if total_rows == 0:
            # Skipping means no asset event, so dbt is not started for nothing.
            try:
                from airflow.sdk.exceptions import AirflowSkipException
            except ImportError:  # older Airflow 3 releases
                from airflow.exceptions import AirflowSkipException
            raise AirflowSkipException("No new rows in MySQL - bronze was not changed.")

        return {"tables": len(results), "rows": total_rows, "pending": pending}

    # With an empty plan (dry run) the mapped task and `publish` are skipped.
    publish(load_table.expand(item=plan()))


bronze_mysql_ingestion()
