"""Batch (chunked) loading of MySQL tables into the bronze layer.

Large tables are copied little by little instead of in one long query::

    MySQL table --(key ranges of <chunk_size> rows)--> one Parquet file per batch
    s3://<bronze>/mysql/<dataset>/ingestion_date=YYYY-MM-DD/<table>-<load_id>-<n>.parquet

How a table is split
    The rows are walked in the order of an indexed column (the "chunk column":
    the primary key by default). Each batch is the key range ``(cursor, upper]``
    where ``upper`` is the key found ``chunk_size`` rows after the cursor. The
    boundary lookup and the extraction both run inside MySQL on that index, so
    a batch never scans the whole table.

Resuming
    After every batch the cursor is saved in a small JSON state file on the lake
    (``s3://<bronze>/_state/mysql/<dataset>/<table>.json``). A retry, or the next
    DAG run, continues from the last finished batch. Once the end of the table
    is reached, later runs only bring rows whose key is above the cursor: new
    rows of an auto-increment key, or changed rows when the chunk column is an
    ``updated_at`` timestamp.

Unifying split tables
    One dataset can be fed by several source tables (``emails``, ``emails_1``,
    ...). Each table keeps its own cursor and writes its own files, all in the
    same dataset folder, so the bronze dataset is the union of them. The source
    table of every row is kept in ``_source_entity``.

Lineage columns added to every row: ``_ingested_at`` (UTC), ``_source_system``,
``_source_entity``, ``_batch_id`` and ``_load_id``. The load id identifies one
complete pass over a source table and changes when the table is reloaded with
``full_refresh``.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import duckdb

from . import mysql_source, objects
from .config import LakeConfig, MySQLConfig
from .duck import connect, sql_literal
from .mysql_source import (
    SOURCE_SYSTEM,
    _mysql_identifier,
    _mysql_string,
    _pushdown,
    key_text_expression,
    mysql_literal,
)
from .naming import utc_timestamp

log = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 250_000
# Folder of the datasets under the bronze bucket. The test DAG uses "_test/mysql".
DEFAULT_PREFIX = SOURCE_SYSTEM
# Bump when the layout of the state file changes (forces a clean reload).
STATE_VERSION = 1
MODES = ("batch", "full", "incremental")


@dataclass(frozen=True)
class BatchOptions:
    """How one source table is loaded in batches."""

    chunk_size: int = DEFAULT_CHUNK_SIZE      # rows per batch (= per Parquet file)
    chunk_column: str | None = None           # None = detect it from the table indexes
    max_chunks: int = 0                       # batches per run; 0 = until the table ends
    full_refresh: bool = False                # forget the cursor and reload from the start
    columns: tuple[str, ...] = ()             # empty = all columns
    where: str | None = None                  # extra filter, MySQL syntax
    prefix: str = DEFAULT_PREFIX              # folder under the bronze bucket


# ---------------------------------------------------------------------------
# Locations
# ---------------------------------------------------------------------------

def dataset_root(lake: LakeConfig, prefix: str, dataset: str) -> str:
    """Folder that holds every Parquet file of a bronze dataset."""
    return lake.uri("bronze", prefix, dataset)


def state_uri(lake: LakeConfig, prefix: str, dataset: str, table: str) -> str:
    """State file of one source table. Kept outside the dataset folder so that
    readers globbing ``<dataset>/*/*.parquet`` never see it."""
    return lake.uri("bronze", "_state", prefix, dataset, f"{table}.json")


# ---------------------------------------------------------------------------
# Planning: which source tables feed which dataset
# ---------------------------------------------------------------------------

def list_base_tables(con: duckdb.DuckDBPyConnection | None = None) -> list[str]:
    """Names of the real tables (views excluded) of the configured MySQL database."""
    return [
        table["table_name"]
        for table in mysql_source.list_tables(con)
        if str(table["table_type"]).upper() == "BASE TABLE"
    ]


def _work_item(dataset: str, table: str, settings: dict[str, Any]) -> dict[str, Any]:
    """One unit of work for the DAG: load ``table`` into ``dataset``."""
    return {
        "dataset": dataset,
        "table": table,
        "mode": str(settings.get("mode") or "batch").strip().lower(),
        "chunk_size": int(settings.get("chunk_size") or DEFAULT_CHUNK_SIZE),
        "chunk_column": settings.get("chunk_column") or None,
        "max_chunks": int(settings.get("max_chunks_per_run") or 0),
        "columns": [str(column) for column in (settings.get("columns") or [])],
        "where": settings.get("where") or None,
        "watermark_column": settings.get("watermark_column") or None,
        "full_refresh": False,
    }


def resolve_work_items(config: dict[str, Any], table_names: list[str]) -> list[dict[str, Any]]:
    """Turn the YAML configuration into a list of (dataset, source table) loads.

    Rules:
      * a dataset lists its source tables with ``tables``, or selects them with
        ``pattern`` (regular expression matched against the whole table name),
        or - with neither - reads the table that has the dataset's own name;
      * with ``discover: true`` every remaining table becomes its own dataset;
      * ``exclude`` (regular expressions) removes tables from discovery and patterns;
      * a table can feed only one dataset.
    """
    defaults = dict(config.get("defaults") or {})
    exclusions = [re.compile(str(pattern)) for pattern in (config.get("exclude") or [])]
    available = [
        name for name in table_names
        if not any(pattern.fullmatch(name) for pattern in exclusions)
    ]

    items: list[dict[str, Any]] = []
    owner: dict[str, str] = {}   # source table -> dataset that claimed it

    for raw in config.get("datasets") or []:
        dataset = str(raw.get("name") or "").strip()
        if not dataset:
            raise ValueError("Dataset entry without 'name' in the ingestion config.")

        if raw.get("pattern"):
            expression = re.compile(str(raw["pattern"]))
            tables = sorted(name for name in available if expression.fullmatch(name))
            if not tables:
                raise ValueError(f"Dataset '{dataset}': no table matches the pattern '{raw['pattern']}'.")
        elif raw.get("tables"):
            tables = [str(name) for name in raw["tables"]]
        else:
            tables = [dataset]

        missing = [name for name in tables if name not in table_names]
        if missing:
            raise ValueError(f"Dataset '{dataset}': tables not found in MySQL: {missing}. Available: {table_names}")

        settings = {**defaults, **{key: value for key, value in raw.items() if key not in ("name", "tables", "pattern")}}
        mode = str(settings.get("mode") or "batch").strip().lower()
        if mode not in MODES:
            raise ValueError(f"Dataset '{dataset}': mode must be one of {MODES}, got '{mode}'.")
        if mode != "batch" and tables != [dataset]:
            raise ValueError(
                f"Dataset '{dataset}': mode '{mode}' reads a single table with the same name as the "
                f"dataset. Use mode 'batch' to rename a dataset or to unify several tables."
            )
        if mode == "incremental" and not settings.get("watermark_column"):
            raise ValueError(f"Dataset '{dataset}': mode 'incremental' requires 'watermark_column'.")

        for table in tables:
            if table in owner:
                raise ValueError(f"Table '{table}' is assigned to both '{owner[table]}' and '{dataset}'.")
            owner[table] = dataset
            items.append(_work_item(dataset, table, settings))

    if config.get("discover"):
        for table in available:
            if table not in owner:
                owner[table] = table
                items.append(_work_item(table, table, defaults))

    return items


# ---------------------------------------------------------------------------
# Chunk key
# ---------------------------------------------------------------------------

def detect_chunk_key(
    con: duckdb.DuckDBPyConnection,
    table: str,
    chunk_column: str | None = None,
) -> dict[str, Any] | None:
    """Choose the column used to walk a table in batches.

    Without ``chunk_column`` the first column of the best index is used:
    primary key, then a unique index, then any other index. Returns ``None``
    when the table has no index at all (it is then copied in a single pass).

    Returns:
        ``{"column": str, "nullable": bool, "origin": str}`` or ``None``.
    """
    table_literal = _mysql_string(table)

    if chunk_column:
        inner = (
            "SELECT COLUMN_NAME AS column_name, IS_NULLABLE AS is_nullable "
            "FROM information_schema.COLUMNS "
            f"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = {table_literal} "
            f"AND COLUMN_NAME = {_mysql_string(chunk_column)}"
        )
        row = con.execute(
            f"SELECT CAST(column_name AS VARCHAR), CAST(is_nullable AS VARCHAR) FROM {_pushdown(inner)}"
        ).fetchone()
        if row is None:
            raise ValueError(f"Table '{table}' has no column named '{chunk_column}' (chunk_column).")
        return {"column": row[0], "nullable": str(row[1]).upper() == "YES", "origin": "configured"}

    inner = (
        "SELECT s.COLUMN_NAME AS column_name, c.IS_NULLABLE AS is_nullable, "
        "s.INDEX_NAME AS index_name, s.NON_UNIQUE AS non_unique "
        "FROM information_schema.STATISTICS s "
        "JOIN information_schema.COLUMNS c "
        "ON c.TABLE_SCHEMA = s.TABLE_SCHEMA AND c.TABLE_NAME = s.TABLE_NAME AND c.COLUMN_NAME = s.COLUMN_NAME "
        f"WHERE s.TABLE_SCHEMA = DATABASE() AND s.TABLE_NAME = {table_literal} "
        "AND s.SEQ_IN_INDEX = 1 AND s.INDEX_TYPE = 'BTREE' "
        "ORDER BY (s.INDEX_NAME = 'PRIMARY') DESC, s.NON_UNIQUE ASC, s.INDEX_NAME ASC "
        "LIMIT 1"
    )
    row = con.execute(
        "SELECT CAST(column_name AS VARCHAR), CAST(is_nullable AS VARCHAR), "
        f"CAST(index_name AS VARCHAR), CAST(non_unique AS BIGINT) FROM {_pushdown(inner)}"
    ).fetchone()
    if row is None:
        return None

    if row[2] == "PRIMARY":
        origin = "primary key"
    elif row[3] == 0:
        origin = f"unique index {row[2]}"
    else:
        origin = f"index {row[2]}"
    return {"column": row[0], "nullable": str(row[1]).upper() == "YES", "origin": origin}


def _key_type(con: duckdb.DuckDBPyConnection, table: str, column: str) -> str:
    """DuckDB type that the chunk column is mapped to (e.g. BIGINT, VARCHAR)."""
    inner = f"SELECT {_mysql_identifier(column)} AS k FROM {_mysql_identifier(table)} LIMIT 0"
    relation = con.sql(f"SELECT k FROM {_pushdown(inner)}")
    return str(relation.types[0]).upper()


def _fetch_key(con: duckdb.DuckDBPyConnection, inner_sql: str, key_type: str) -> str | None:
    """Run a MySQL statement returning one key ``k`` and render it as a MySQL literal."""
    expression, kind = key_text_expression(key_type, "k")
    row = con.execute(f"SELECT {expression} FROM {_pushdown(inner_sql)}").fetchone()
    if row is None or row[0] is None:
        return None
    return mysql_literal(str(row[0]), kind)


def _where(*predicates: str | None) -> str:
    """Build a WHERE clause from the predicates that are not empty."""
    active = [predicate for predicate in predicates if predicate]
    return " WHERE " + " AND ".join(active) if active else ""


def _next_upper(
    con: duckdb.DuckDBPyConnection,
    table_sql: str,
    column_sql: str,
    cursor: str | None,
    chunk_size: int,
    key_type: str,
    extra: str | None,
) -> str | None:
    """Upper key of the next batch, or ``None`` when no rows are left.

    The key is the one ``chunk_size`` rows after the cursor. When fewer rows
    remain, the batch is closed at the current maximum so that it has a fixed
    upper bound (rows inserted meanwhile are picked up by the next batch).
    """
    lower = f"{column_sql} > {cursor}" if cursor is not None else f"{column_sql} IS NOT NULL"
    scope = _where(lower, extra)
    upper = _fetch_key(
        con,
        f"SELECT {column_sql} AS k FROM {table_sql}{scope} ORDER BY {column_sql} LIMIT 1 OFFSET {chunk_size - 1}",
        key_type,
    )
    if upper is None:
        upper = _fetch_key(
            con,
            f"SELECT {column_sql} AS k FROM {table_sql}{scope} ORDER BY {column_sql} DESC LIMIT 1",
            key_type,
        )
    return upper


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def _copy_chunk(
    con: duckdb.DuckDBPyConnection,
    *,
    inner: str,
    target: str,
    source_entity: str,
    batch_id: str,
    load_id: str,
) -> int:
    """Run ``inner`` inside MySQL and stream the result into one Parquet file."""
    rows = con.execute(
        f"""
        COPY (
            SELECT
                src.*,
                TIMESTAMP {sql_literal(utc_timestamp())}   AS _ingested_at,
                {sql_literal(SOURCE_SYSTEM)}               AS _source_system,
                {sql_literal(source_entity)}               AS _source_entity,
                {sql_literal(batch_id)}                    AS _batch_id,
                {sql_literal(load_id)}                     AS _load_id
            FROM {_pushdown(inner)} AS src
        ) TO {sql_literal(target)} (FORMAT parquet, COMPRESSION zstd)
        """
    ).fetchone()[0]
    return int(rows)


def _new_state(dataset: str, table: str, chunk_column: str | None) -> dict[str, Any]:
    """State of a load that has not copied anything yet."""
    now = datetime.now(timezone.utc)
    return {
        "version": STATE_VERSION,
        "dataset": dataset,
        "source_table": table,
        "chunk_column": chunk_column,
        "key_type": None,
        "load_id": now.strftime("%Y%m%dT%H%M%S"),
        "cursor": None,              # MySQL literal of the last key copied
        "nulls_done": False,         # rows with a NULL key already copied
        "chunks": 0,                 # key-range batches written so far
        "rows": 0,                   # rows written so far
        "complete": False,           # the end of the table was reached in the last run
        "refresh_batch_id": None,    # run that requested the last full_refresh
        "started_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": None,
        "last_batch_id": None,
    }


def load_table(
    dataset: str,
    table: str,
    *,
    batch_id: str,
    ingestion_date: str,
    options: BatchOptions | None = None,
) -> dict[str, Any]:
    """Copy one MySQL table into a bronze dataset, batch by batch.

    Continues from the cursor saved by previous runs and stops at the end of
    the table or after ``options.max_chunks`` batches, whichever comes first.

    Args:
        dataset: name of the bronze dataset (folder) that receives the rows.
        table: source table in MySQL.
        batch_id: file-name-safe identifier of this run (``naming.safe_name``).
        ingestion_date: ``YYYY-MM-DD`` value of the ``ingestion_date=`` partition.
        options: batch settings; defaults to :class:`BatchOptions`.

    Returns:
        Summary of this run: rows and batches written now, totals of the load,
        whether the table is complete and where the files are.
    """
    options = options or BatchOptions()
    if options.chunk_size < 1:
        raise ValueError("chunk_size must be at least 1.")

    lake = LakeConfig.from_env()
    mysql = MySQLConfig.from_env()
    client = objects.s3_client(lake)

    root = dataset_root(lake, options.prefix, dataset)
    state_location = state_uri(lake, options.prefix, dataset, table)
    partition = f"{root}/ingestion_date={ingestion_date}"
    source_entity = f"{mysql.database}.{table}"

    table_sql = _mysql_identifier(table)
    select_list = ", ".join(_mysql_identifier(column) for column in options.columns) or "*"
    extra = f"({options.where})" if options.where else None

    con = connect(mysql=True)
    try:
        key = detect_chunk_key(con, table, options.chunk_column)
        chunk_column = key["column"] if key else None

        # --- load or (re)start the state -----------------------------------
        state = objects.read_json(state_location, client) or {}
        restart_reason = None
        if not state:
            restart_reason = "first load"
        elif state.get("version") != STATE_VERSION:
            restart_reason = "state file format changed"
        elif state.get("chunk_column") != chunk_column:
            restart_reason = f"chunk column changed from {state.get('chunk_column')} to {chunk_column}"
        elif options.full_refresh and state.get("refresh_batch_id") != batch_id:
            # The batch id check keeps a retry of the same run from restarting again.
            restart_reason = "full_refresh requested"
        if restart_reason:
            state = _new_state(dataset, table, chunk_column)
            state["refresh_batch_id"] = batch_id if options.full_refresh else None
            log.info("%s: starting load %s (%s)", source_entity, state["load_id"], restart_reason)
        else:
            log.info(
                "%s: resuming load %s after %s rows (cursor: %s)",
                source_entity, state["load_id"], state["rows"], state["cursor"],
            )

        def save() -> None:
            state["updated_at"] = utc_timestamp()
            state["last_batch_id"] = batch_id
            objects.write_json(state_location, state, client)

        def copy(inner: str, index: int) -> int:
            target = f"{partition}/{table}-{state['load_id']}-{index:06d}.parquet"
            return _copy_chunk(
                con,
                inner=inner,
                target=target,
                source_entity=source_entity,
                batch_id=batch_id,
                load_id=state["load_id"],
            )

        chunks_now = 0
        rows_now = 0
        limit_reached = False

        if key is None:
            # --- no index: the table cannot be split, copy it in one pass ---
            if state["complete"]:
                log.info(
                    "%s has no index and was already copied in load %s. "
                    "Trigger with full_refresh to copy it again.",
                    source_entity, state["load_id"],
                )
            else:
                log.warning("%s has no index: copying it in a single pass (no batches).", source_entity)
                rows = copy(f"SELECT {select_list} FROM {table_sql}{_where(extra)}", 0)
                state.update(rows=rows, chunks=1, complete=True)
                save()
                chunks_now, rows_now = 1, rows
        else:
            column_sql = _mysql_identifier(chunk_column)
            key_type = _key_type(con, table, chunk_column)
            state["key_type"] = key_type
            log.info(
                "%s: batches of %s rows by %s (%s, %s)",
                source_entity, options.chunk_size, chunk_column, key["origin"], key_type,
            )

            # --- rows with a NULL key are out of reach of range filters -----
            if key["nullable"] and not state["nulls_done"]:
                null_scope = _where(f"{column_sql} IS NULL", extra)
                probe = f"SELECT 1 AS found FROM {table_sql}{null_scope} LIMIT 1"
                if con.execute(f"SELECT count(*) FROM {_pushdown(probe)}").fetchone()[0]:
                    rows = copy(f"SELECT {select_list} FROM {table_sql}{null_scope}", 0)
                    state["rows"] += rows
                    rows_now += rows
                    chunks_now += 1
                    log.info("%s: %s rows with NULL %s", source_entity, rows, chunk_column)
                state["nulls_done"] = True
                save()

            # --- key ranges, one batch at a time ----------------------------
            while True:
                if options.max_chunks and chunks_now >= options.max_chunks:
                    limit_reached = True
                    break

                cursor = state["cursor"]
                upper = _next_upper(con, table_sql, column_sql, cursor, options.chunk_size, key_type, extra)
                if upper is None:
                    break  # nothing left after the cursor
                if upper == cursor:
                    raise RuntimeError(
                        f"{source_entity}: the cursor did not advance past {cursor}. Column "
                        f"'{chunk_column}' ({key_type}) cannot be used for batching; set another "
                        f"'chunk_column' for this dataset."
                    )

                lower = f"{column_sql} > {cursor}" if cursor is not None else None
                scope = _where(lower, f"{column_sql} <= {upper}", extra)
                index = state["chunks"] + 1
                rows = copy(f"SELECT {select_list} FROM {table_sql}{scope}", index)
                if rows == 0:
                    raise RuntimeError(
                        f"{source_entity}: batch {index} ({cursor} -> {upper}) returned no rows. "
                        f"Set another 'chunk_column' for this dataset."
                    )

                # The batch only counts once its file exists: save the cursor after it.
                state["cursor"] = upper
                state["chunks"] = index
                state["rows"] += rows
                save()
                chunks_now += 1
                rows_now += rows
                log.info(
                    "%s: batch %s done - %s rows (%s -> %s), %s rows so far",
                    source_entity, index, rows, cursor, upper, state["rows"],
                )

            state["complete"] = not limit_reached
            save()

        if limit_reached:
            log.info(
                "%s: stopped after %s batches (max_chunks). Run again to continue.",
                source_entity, chunks_now,
            )
        elif rows_now:
            log.info("%s: complete - %s rows in this run, %s in total", source_entity, rows_now, state["rows"])
        else:
            log.info("%s: up to date, nothing new to load", source_entity)

        return {
            "dataset": dataset,
            "table": table,
            "mode": "batch",
            "rows": rows_now,
            "chunks": chunks_now,
            "total_rows": state["rows"],
            "total_chunks": state["chunks"],
            "complete": bool(state["complete"]),
            "load_id": state["load_id"],
            "chunk_column": chunk_column,
            "target": root,
        }
    finally:
        con.close()


def run_work_item(
    item: dict[str, Any],
    *,
    batch_id: str,
    ingestion_date: str,
    prefix: str = DEFAULT_PREFIX,
) -> dict[str, Any]:
    """Execute one item produced by :func:`resolve_work_items`.

    ``batch`` items use :func:`load_table`; ``full`` and ``incremental`` items
    use the single-query loaders of :mod:`lakehouse.mysql_source`.
    """
    mode = item.get("mode") or "batch"
    if mode == "batch":
        return load_table(
            item["dataset"],
            item["table"],
            batch_id=batch_id,
            ingestion_date=ingestion_date,
            options=BatchOptions(
                chunk_size=int(item.get("chunk_size") or DEFAULT_CHUNK_SIZE),
                chunk_column=item.get("chunk_column") or None,
                max_chunks=int(item.get("max_chunks") or 0),
                full_refresh=bool(item.get("full_refresh")),
                columns=tuple(item.get("columns") or ()),
                where=item.get("where") or None,
                prefix=prefix,
            ),
        )

    spec = mysql_source.TableSpec(
        name=item["table"],
        mode=mode,
        watermark_column=item.get("watermark_column") or None,
        columns=list(item.get("columns") or []),
        where=item.get("where") or None,
    )
    result = mysql_source.ingest_table(spec, batch_id=batch_id, ingestion_date=ingestion_date)
    return {
        "dataset": item["dataset"],
        "table": item["table"],
        "mode": mode,
        "rows": result["rows"],
        "chunks": 1,
        "complete": True,
        "target": result["target"],
    }


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def verify_table(dataset: str, table: str, *, prefix: str = DEFAULT_PREFIX) -> dict[str, Any]:
    """Compare the rows of a source table with what its current load wrote to bronze.

    Counts every row of the table in MySQL (``COUNT(*)``), so use it on tables
    of moderate size. Only meaningful for loads without a ``where`` filter.

    Returns:
        Row counts on both sides, whether they match, and the rows per file.
    """
    lake = LakeConfig.from_env()
    mysql = MySQLConfig.from_env()
    state = objects.read_json(state_uri(lake, prefix, dataset, table)) or {}
    report: dict[str, Any] = {
        "dataset": dataset,
        "table": table,
        "load_id": state.get("load_id"),
        "complete": bool(state.get("complete")),
        "mysql_rows": None,
        "bronze_rows": 0,
        "match": False,
        "files": [],
    }
    if not state:
        return report

    pattern = f"{dataset_root(lake, prefix, dataset)}/*/*.parquet"
    con = connect(mysql=True)
    try:
        count_sql = f"SELECT COUNT(*) AS n FROM {_mysql_identifier(table)}"
        report["mysql_rows"] = int(
            con.execute(f"SELECT CAST(n AS BIGINT) FROM {_pushdown(count_sql)}").fetchone()[0]
        )

        if con.execute(f"SELECT count(*) FROM glob({sql_literal(pattern)})").fetchone()[0]:
            files = con.execute(
                f"""
                SELECT filename, count(*) AS row_count
                FROM read_parquet({sql_literal(pattern)}, union_by_name = true, hive_partitioning = false, filename = true)
                WHERE _source_entity = {sql_literal(mysql.database + '.' + table)}
                  AND _load_id = {sql_literal(state['load_id'])}
                GROUP BY filename
                ORDER BY filename
                """
            ).fetchall()
            report["files"] = [{"file": row[0], "rows": int(row[1])} for row in files]
            report["bronze_rows"] = sum(item["rows"] for item in report["files"])
    finally:
        con.close()

    report["match"] = report["mysql_rows"] == report["bronze_rows"]
    return report
