"""Batch extraction from the source MySQL into the bronze layer.

Flow of one table::

    MySQL --(DuckDB mysql scanner, streamed)--> Parquet (zstd)
          s3://<bronze>/mysql/<table>/ingestion_date=YYYY-MM-DD/<file>.parquet

Why it is fast: the query runs inside MySQL (``mysql_query`` pushes the whole
statement down), rows are streamed into DuckDB's columnar engine and written
directly to the lake - nothing is materialised in Python.

Two load modes:

* ``full``        - snapshot of the whole table on every run. The file is always
                    ``snapshot.parquet``, so re-running the same day overwrites it.
* ``incremental`` - only rows whose ``watermark_column`` is greater than the
                    highest value already stored in bronze. One file per batch.
                    The first run (empty bronze) loads the whole table.

Every row receives lineage columns: ``_ingested_at`` (UTC), ``_source_system``,
``_source_entity`` and ``_batch_id``.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import duckdb

from .config import LakeConfig, MySQLConfig
from .duck import MYSQL_ALIAS, connect, sql_identifier, sql_literal
from .naming import utc_timestamp

log = logging.getLogger(__name__)

SOURCE_SYSTEM = "mysql"
VALID_MODES = ("full", "incremental")

# DuckDB type-name prefixes whose values are written unquoted in MySQL SQL.
_NUMERIC_PREFIXES = (
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT",
    "FLOAT", "DOUBLE", "DECIMAL",
)


@dataclass(frozen=True)
class TableSpec:
    """How one MySQL table is ingested (one entry of mysql_tables.yml)."""

    name: str
    mode: str = "full"
    watermark_column: str | None = None
    columns: list[str] = field(default_factory=list)   # empty = all columns
    where: str | None = None                           # extra filter, MySQL syntax

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Table entry without 'name'.")
        if self.mode not in VALID_MODES:
            raise ValueError(f"Table '{self.name}': mode must be one of {VALID_MODES}, got '{self.mode}'.")
        if self.mode == "incremental" and not self.watermark_column:
            raise ValueError(f"Table '{self.name}': incremental mode requires 'watermark_column'.")

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "TableSpec":
        return cls(
            name=str(raw.get("name", "")).strip(),
            mode=str(raw.get("mode", "full")).strip().lower(),
            watermark_column=raw.get("watermark_column") or None,
            columns=[str(column) for column in (raw.get("columns") or [])],
            where=raw.get("where") or None,
        )


def _mysql_identifier(name: str) -> str:
    """Quote an identifier using MySQL backticks."""
    return "`" + str(name).replace("`", "``") + "`"


def _mysql_string(value: str) -> str:
    """Quote a string literal for MySQL (backslashes and quotes escaped)."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def _pushdown(inner_sql: str) -> str:
    """DuckDB table expression that executes ``inner_sql`` inside MySQL."""
    return f"mysql_query({sql_literal(MYSQL_ALIAS)}, {sql_literal(inner_sql)})"


def list_tables(con: duckdb.DuckDBPyConnection | None = None) -> list[dict[str, Any]]:
    """List the tables/views of the configured MySQL database."""
    own_connection = con is None
    con = con or connect(mysql=True)
    try:
        inner = (
            "SELECT TABLE_NAME AS table_name, TABLE_TYPE AS table_type, TABLE_ROWS AS approx_rows "
            "FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME"
        )
        rows = con.execute(
            "SELECT CAST(table_name AS VARCHAR), CAST(table_type AS VARCHAR), CAST(approx_rows AS BIGINT) "
            f"FROM {_pushdown(inner)}"
        ).fetchall()
        return [{"table_name": row[0], "table_type": row[1], "approx_rows": row[2]} for row in rows]
    finally:
        if own_connection:
            con.close()


def preview_table(
    table: str,
    limit: int = 10,
    con: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, Any]:
    """Fetch the first rows of a table (the LIMIT is executed by MySQL).

    Returns a dict with the column names/types as mapped by DuckDB, the number
    of rows fetched and a text rendering of the sample for logs.
    """
    own_connection = con is None
    con = con or connect(mysql=True)
    try:
        inner = f"SELECT * FROM {_mysql_identifier(table)} LIMIT {int(limit)}"
        relation = con.sql(f"SELECT * FROM {_pushdown(inner)}")
        columns = [
            {"name": name, "type": str(dtype)}
            for name, dtype in zip(relation.columns, relation.types)
        ]
        row_count = con.execute(f"SELECT count(*) FROM {_pushdown(inner)}").fetchone()[0]
        return {
            "table": table,
            "columns": columns,
            "row_count": row_count,
            "rendered": str(relation),
        }
    finally:
        if own_connection:
            con.close()


def _resolve_watermark(
    con: duckdb.DuckDBPyConnection,
    bronze_glob: str,
    spec: TableSpec,
    batch_id: str,
) -> str | None:
    """Return the highest watermark already in bronze, as a MySQL SQL literal.

    Files written by the current batch are ignored so that re-running a batch
    extracts the same rows again (idempotent retries). Returns ``None`` when
    bronze holds no data yet, which triggers a full first load.
    """
    files = con.execute(f"SELECT count(*) FROM glob({sql_literal(bronze_glob)})").fetchone()[0]
    if files == 0:
        return None

    column = sql_identifier(spec.watermark_column)
    source = (
        f"read_parquet({sql_literal(bronze_glob)}, union_by_name = true, hive_partitioning = false) "
        f"WHERE _batch_id <> {sql_literal(batch_id)}"
    )
    type_name, has_value = con.execute(
        f"SELECT typeof(max({column})), max({column}) IS NOT NULL FROM {source}"
    ).fetchone()
    if not has_value:
        return None

    # Let DuckDB format the value in a way MySQL parses unambiguously.
    type_name = str(type_name).upper()
    quoted = True
    if type_name.startswith("TIMESTAMP WITH TIME ZONE"):
        # MySQL TIMESTAMP columns: compare in UTC (the session time zone we set).
        expression = f"strftime(timezone('UTC', max({column})), '%Y-%m-%d %H:%M:%S.%f')"
    elif type_name.startswith("TIMESTAMP"):
        expression = f"strftime(CAST(max({column}) AS TIMESTAMP), '%Y-%m-%d %H:%M:%S.%f')"
    elif type_name == "DATE":
        expression = f"strftime(max({column}), '%Y-%m-%d')"
    elif type_name.startswith(_NUMERIC_PREFIXES):
        expression = f"CAST(max({column}) AS VARCHAR)"
        quoted = False
    else:
        expression = f"CAST(max({column}) AS VARCHAR)"

    value = con.execute(f"SELECT {expression} FROM {source}").fetchone()[0]
    return _mysql_string(value) if quoted else str(value)


def ingest_table(spec: TableSpec, *, batch_id: str, ingestion_date: str) -> dict[str, Any]:
    """Copy one MySQL table (or its new rows) into the bronze layer.

    Args:
        spec: what to extract and how.
        batch_id: file-name-safe identifier of this run (see ``naming.safe_name``).
        ingestion_date: ``YYYY-MM-DD`` value of the ``ingestion_date=`` partition.

    Returns:
        Summary of the load (table, mode, rows, target, watermark).
    """
    lake = LakeConfig.from_env()
    mysql = MySQLConfig.from_env()
    table_root = lake.uri("bronze", SOURCE_SYSTEM, spec.name)

    con = connect(mysql=True)
    try:
        watermark = None
        if spec.mode == "incremental":
            watermark = _resolve_watermark(con, f"{table_root}/*/*.parquet", spec, batch_id)

        # Statement executed by MySQL itself.
        select_list = ", ".join(_mysql_identifier(column) for column in spec.columns) or "*"
        inner = f"SELECT {select_list} FROM {_mysql_identifier(spec.name)}"
        predicates = []
        if spec.where:
            predicates.append(f"({spec.where})")
        if watermark is not None:
            predicates.append(f"{_mysql_identifier(spec.watermark_column)} > {watermark}")
        if predicates:
            inner += " WHERE " + " AND ".join(predicates)

        file_name = "snapshot.parquet" if spec.mode == "full" else f"{batch_id}.parquet"
        target = f"{table_root}/ingestion_date={ingestion_date}/{file_name}"

        log.info("Extracting %s.%s (%s) -> %s", mysql.database, spec.name, spec.mode, target)
        log.info("MySQL statement: %s", inner)

        rows = con.execute(
            f"""
            COPY (
                SELECT
                    src.*,
                    TIMESTAMP {sql_literal(utc_timestamp())}              AS _ingested_at,
                    {sql_literal(SOURCE_SYSTEM)}                          AS _source_system,
                    {sql_literal(mysql.database + '.' + spec.name)}       AS _source_entity,
                    {sql_literal(batch_id)}                               AS _batch_id
                FROM {_pushdown(inner)} AS src
            ) TO {sql_literal(target)} (FORMAT parquet, COMPRESSION zstd)
            """
        ).fetchone()[0]

        log.info("Wrote %s rows to %s", rows, target)
        return {
            "table": spec.name,
            "mode": spec.mode,
            "rows": int(rows),
            "target": target,
            "watermark": watermark,
        }
    finally:
        con.close()
