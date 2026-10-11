"""DuckDB sessions pre-configured for the lakehouse.

DuckDB is the processing engine of the platform. :func:`connect` returns a
connection that can already:

* read/write Parquet on the data lake through ``s3://<bucket>/...`` (httpfs);
* optionally query the source MySQL as the catalog ``mysql_src`` (read-only),
  streaming rows straight into DuckDB's vectorised engine.

Credentials come from the vault (see :mod:`lakehouse.config`) and are stored in
DuckDB's in-memory secret manager - they are never written to disk.
"""
from __future__ import annotations

import logging

import duckdb

from .config import LakeConfig, MySQLConfig, setting

log = logging.getLogger(__name__)

# Catalog name under which the source MySQL is attached.
MYSQL_ALIAS = "mysql_src"


def sql_literal(value: object) -> str:
    """Render a value as a single-quoted SQL string literal (quotes doubled)."""
    return "'" + str(value).replace("'", "''") + "'"


def sql_identifier(name: str) -> str:
    """Render a name as a double-quoted DuckDB identifier."""
    return '"' + str(name).replace('"', '""') + '"'


def _load_extension(con: duckdb.DuckDBPyConnection, name: str) -> None:
    """Load an extension; download it first if the image did not pre-install it."""
    try:
        con.load_extension(name)
    except duckdb.Error:
        log.info("DuckDB extension '%s' not installed yet - downloading it", name)
        con.install_extension(name)
        con.load_extension(name)


def _configure_engine(con: duckdb.DuckDBPyConnection) -> None:
    """Apply the resource limits defined in .env (DUCKDB_MEMORY_LIMIT/THREADS)."""
    memory_limit = setting("DUCKDB_MEMORY_LIMIT")
    threads = setting("DUCKDB_THREADS")
    if memory_limit:
        con.execute(f"SET memory_limit = {sql_literal(memory_limit)}")
    if threads and threads.isdigit():
        con.execute(f"SET threads = {int(threads)}")
    # Larger-than-memory operations spill here instead of failing.
    con.execute(f"SET temp_directory = {sql_literal(setting('DUCKDB_TEMP_DIR', '/tmp/duckdb_tmp'))}")
    # Row order is irrelevant for our batch copies; disabling it lowers memory use.
    con.execute("SET preserve_insertion_order = false")


def configure_lake(con: duckdb.DuckDBPyConnection, lake: LakeConfig | None = None) -> None:
    """Register the data-lake credentials so ``s3://`` paths work."""
    lake = lake or LakeConfig.from_env()
    _load_extension(con, "httpfs")
    con.execute(
        f"""
        CREATE OR REPLACE SECRET lake_s3 (
            TYPE s3,
            KEY_ID {sql_literal(lake.access_key)},
            SECRET {sql_literal(lake.secret_key)},
            ENDPOINT {sql_literal(lake.endpoint)},
            REGION {sql_literal(lake.region)},
            URL_STYLE {sql_literal(lake.url_style)},
            USE_SSL {'true' if lake.use_ssl else 'false'}
        )
        """
    )


def attach_mysql(con: duckdb.DuckDBPyConnection, mysql: MySQLConfig | None = None) -> None:
    """Attach the source MySQL, read-only, as the catalog ``mysql_src``."""
    mysql = mysql or MySQLConfig.from_env()
    _load_extension(con, "mysql")

    # Optional tuning. Each setting is applied independently so an extension
    # version that does not know one of them does not break the connection.
    for statement in (
        # Read MySQL TIMESTAMP columns (and compare pushed-down literals) in UTC.
        "SET mysql_session_time_zone = '+00:00'",
        # MySQL "zero dates" (0000-00-00) cannot be represented: load them as NULL.
        "SET mysql_incomplete_dates_as_nulls = true",
    ):
        try:
            con.execute(statement)
        except duckdb.Error as error:
            log.debug("Skipping optional MySQL setting (%s): %s", statement, error)

    parameters = [
        "TYPE mysql",
        f"HOST {sql_literal(mysql.host)}",
        f"PORT {int(mysql.port)}",
        f"DATABASE {sql_literal(mysql.database)}",
        f"USER {sql_literal(mysql.user)}",
        f"PASSWORD {sql_literal(mysql.password)}",
    ]
    if mysql.ssl_mode:
        parameters.append(f"SSL_MODE {sql_literal(mysql.ssl_mode)}")
    con.execute(f"CREATE OR REPLACE SECRET mysql_src_secret ({', '.join(parameters)})")
    # Empty connection string: every parameter comes from the secret above.
    con.execute(f"ATTACH '' AS {sql_identifier(MYSQL_ALIAS)} (TYPE mysql, SECRET mysql_src_secret, READ_ONLY)")


def connect(
    *,
    mysql: bool = False,
    database: str = ":memory:",
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection wired to the lake.

    Args:
        mysql: also attach the source MySQL as ``mysql_src`` (needs MYSQL_* secrets).
        database: DuckDB database file, or ``":memory:"`` (default). Pipelines
            use in-memory sessions: the data itself lives as Parquet on the lake.
        read_only: open the database file in read-only mode.

    Example:
        >>> con = connect()
        >>> con.sql("SELECT count(*) FROM read_parquet('s3://gold/*.parquet')").show()
    """
    con = duckdb.connect(database=database, read_only=read_only)
    try:
        _configure_engine(con)
        configure_lake(con)
        if mysql:
            attach_mysql(con)
    except Exception:
        con.close()
        raise
    return con
