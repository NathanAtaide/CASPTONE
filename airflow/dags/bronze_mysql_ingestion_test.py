"""
### bronze_mysql_ingestion_test - try the batch load on a small table

Runs the **same batch loading code** as `bronze_mysql_ingestion`, but on one
small table and into a separate test area, so the mechanism can be checked
before it is used on every table.

1. `plan` - checks that the tables exist;
2. `load_table` - loads each table in batches (default: 1000 rows per batch, so
   even a small table produces several files);
3. `verify` - compares `COUNT(*)` in MySQL with the rows written to bronze and
   lists the rows of each batch file. Fails when a complete load does not match.

**Where the data goes:** `s3://bronze/_test/mysql/<dataset>/...` - never mixed
with the real datasets, and it does not start `dbt_medallion`.

**Suggested tests** (all with table `product`)

| Test | How | Expected |
|---|---|---|
| Full load in batches | trigger with the defaults | several files, `verify` reports a match |
| Stop and continue | trigger with `max_chunks = 2`, then again with `max_chunks = 0` and `full_refresh = false` | first run stops after 2 batches, second one continues and matches |
| Unify tables | `tables = ["sessions_1", "sessions_2"]`, `dataset = "sessions"` | both tables in the same dataset folder |
"""
from __future__ import annotations

import logging
from datetime import datetime

from airflow.sdk import Param, dag, get_current_context, task

log = logging.getLogger(__name__)

# Folder under the bronze bucket used only by this DAG.
TEST_PREFIX = "_test/mysql"


@dag(
    dag_id="bronze_mysql_ingestion_test",
    description="Test of the batch load MySQL -> bronze on a small table (isolated test area).",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["test", "bronze", "mysql", "ingestion"],
    doc_md=__doc__,
    params={
        "tables": Param(
            ["product"],
            type="array",
            items={"type": "string"},
            title="Tables",
            description="Source tables to load. More than one = they are unified in the same dataset.",
        ),
        "dataset": Param(
            "",
            type="string",
            title="Dataset",
            description="Name of the bronze dataset. Empty = name of the first table.",
        ),
        "chunk_size": Param(
            1000,
            type="integer",
            minimum=1,
            title="Rows per batch",
            description="Small on purpose, so a small table is split in several batches.",
        ),
        "max_chunks": Param(
            0,
            type="integer",
            minimum=0,
            title="Max batches per table",
            description="Stop after this many batches. 0 = until the table is complete.",
        ),
        "chunk_column": Param(
            "",
            type="string",
            title="Chunk column",
            description="Column used to walk the table. Empty = detect it from the indexes (primary key).",
        ),
        "full_refresh": Param(
            True,
            type="boolean",
            title="Start from scratch",
            description="True = reload from the start. False = continue from the saved position.",
        ),
    },
)
def bronze_mysql_ingestion_test():
    @task(pool="mysql_source")
    def plan(params: dict | None = None) -> list[dict]:
        """Build the list of tables to load, in the same format the real DAG uses."""
        from lakehouse import mysql_batch

        params = params or {}
        tables = [str(name).strip() for name in (params.get("tables") or []) if str(name).strip()]
        if not tables:
            raise ValueError("Inform at least one table in the 'tables' parameter.")

        available = mysql_batch.list_base_tables()
        missing = [name for name in tables if name not in available]
        if missing:
            raise ValueError(f"Tables not found in MySQL: {missing}. Available: {available}")

        dataset = (params.get("dataset") or "").strip() or tables[0]
        items = [
            {
                "dataset": dataset,
                "table": table,
                "mode": "batch",
                "chunk_size": int(params.get("chunk_size") or 1000),
                "chunk_column": (params.get("chunk_column") or "").strip() or None,
                "max_chunks": int(params.get("max_chunks") or 0),
                "full_refresh": bool(params.get("full_refresh")),
                "columns": [],
                "where": None,
            }
            for table in tables
        ]
        for item in items:
            log.info("Will load %s into test dataset %s (%s rows per batch)", item["table"], dataset, item["chunk_size"])
        return items

    @task(pool="mysql_source", map_index_template="{{ work_label }}")
    def load_table(item: dict, dag_run=None) -> dict:
        """Load one table in batches into the test area."""
        from lakehouse import mysql_batch
        from lakehouse.naming import partition_date, safe_name

        get_current_context()["work_label"] = item["table"]
        return mysql_batch.run_work_item(
            item,
            batch_id=safe_name(dag_run.run_id),
            ingestion_date=str(partition_date(getattr(dag_run, "run_after", None))),
            prefix=TEST_PREFIX,
        )

    @task(pool="mysql_source")
    def verify(results: list[dict]) -> list[dict]:
        """Compare MySQL with bronze and show the rows of each batch file."""
        from lakehouse import mysql_batch

        reports = []
        failures = []
        for result in list(results):
            report = mysql_batch.verify_table(result["dataset"], result["table"], prefix=TEST_PREFIX)
            reports.append(report)

            log.info(
                "%s (load %s): MySQL has %s rows, bronze has %s rows in %s files",
                report["table"], report["load_id"], report["mysql_rows"], report["bronze_rows"], len(report["files"]),
            )
            for item in report["files"]:
                log.info("    %8s rows  %s", item["rows"], item["file"])

            if not report["complete"]:
                log.warning(
                    "%s is NOT complete yet (%s of %s rows). Trigger again with full_refresh = false to continue.",
                    report["table"], report["bronze_rows"], report["mysql_rows"],
                )
            elif report["match"]:
                log.info("%s: OK - bronze matches MySQL.", report["table"])
            else:
                failures.append(report["table"])

        if failures:
            raise ValueError(
                f"Row counts differ between MySQL and bronze for: {failures}. "
                f"See the numbers above (rows changing in MySQL during the test also cause this)."
            )
        return reports

    verify(load_table.expand(item=plan()))


bronze_mysql_ingestion_test()
