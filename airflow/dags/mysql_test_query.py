"""
### mysql_test_query - connectivity test against the source MySQL

Manual DAG that proves the whole path **vault -> Airflow -> DuckDB -> MySQL** works:

1. `list_tables` - lists the tables of the configured database;
2. `preview_table` - queries one table and prints its columns and first rows in
   the task log.

**How to use:** trigger the DAG. Leave `table` empty to preview the first table
found, or type a table name. Nothing is written anywhere - it is read-only.

**Requires** the `MYSQL_*` secrets in the vault (`docker compose run --rm vault set MYSQL_HOST` ...).
"""
from __future__ import annotations

import logging
from datetime import datetime

from airflow.sdk import Param, dag, task

log = logging.getLogger(__name__)


@dag(
    dag_id="mysql_test_query",
    description="Smoke test: lists the MySQL tables and previews one of them.",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test", "mysql", "bronze"],
    doc_md=__doc__,
    params={
        "table": Param(
            "",
            type="string",
            title="Table",
            description="Table to preview. Leave empty to use the first table of the database.",
        ),
        "limit": Param(
            10,
            type="integer",
            minimum=1,
            maximum=1000,
            title="Rows",
            description="Number of rows to fetch.",
        ),
    },
)
def mysql_test_query():
    # The `mysql_source` pool caps concurrent queries against the production database.
    @task(pool="mysql_source")
    def list_tables() -> list[str]:
        """Return the table names of the configured MySQL database."""
        # Imported inside the task so DAG parsing stays fast and dependency-free.
        from lakehouse import mysql_source

        tables = mysql_source.list_tables()
        if not tables:
            raise ValueError("Connected to MySQL, but the configured database has no tables.")

        log.info("Found %s tables/views:", len(tables))
        for table in tables:
            log.info(
                "  - %s (%s, ~%s rows)",
                table["table_name"],
                table["table_type"],
                table["approx_rows"],
            )
        return [table["table_name"] for table in tables]

    @task(pool="mysql_source")
    def preview_table(tables: list[str], params: dict | None = None) -> dict:
        """Query one table and log its schema and a sample of rows."""
        from lakehouse import mysql_source

        params = params or {}
        table = (params.get("table") or "").strip() or tables[0]
        if table not in tables:
            raise ValueError(f"Table '{table}' not found. Available tables: {tables}")

        preview = mysql_source.preview_table(table, limit=int(params.get("limit", 10)))

        log.info("Columns of %s (types as mapped by DuckDB):", table)
        for column in preview["columns"]:
            log.info("  - %s: %s", column["name"], column["type"])
        log.info("First %s rows of %s:\n%s", preview["row_count"], table, preview["rendered"])

        return {
            "table": table,
            "columns": [column["name"] for column in preview["columns"]],
            "rows_previewed": preview["row_count"],
        }

    preview_table(list_tables())


mysql_test_query()
