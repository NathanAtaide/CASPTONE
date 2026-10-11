"""Staging area for data that does not come from a database (APIs, files).

The staging area is a landing zone made of Parquet files, kept in its own
bucket and separate from the medallion layers::

    API / file --> s3://<staging>/<source>/<entity>/load_date=YYYY-MM-DD/<batch>.parquet
               --> s3://<bronze>/<source>/<entity>/ingestion_date=YYYY-MM-DD/<batch>.parquet

Step 1 (:func:`write_records`) only converts the payload to Parquet, untouched.
Step 2 (:func:`promote_to_bronze`) imports that file into the data lake and adds
the lineage columns. Staging files expire automatically (STAGING_RETENTION_DAYS),
bronze keeps the history.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from typing import Any, Iterable

from .config import LakeConfig
from .duck import connect, sql_literal
from .naming import utc_timestamp

log = logging.getLogger(__name__)


def staging_path(lake: LakeConfig, source: str, entity: str, load_date: str, batch_id: str) -> str:
    """Location of one batch in the staging area."""
    return lake.uri("staging", source, entity, f"load_date={load_date}", f"{batch_id}.parquet")


def bronze_path(lake: LakeConfig, source: str, entity: str, ingestion_date: str, batch_id: str) -> str:
    """Location of one batch in the bronze layer."""
    return lake.uri("bronze", source, entity, f"ingestion_date={ingestion_date}", f"{batch_id}.parquet")


def write_records(
    records: Iterable[dict[str, Any]],
    *,
    source: str,
    entity: str,
    batch_id: str,
    load_date: str,
) -> dict[str, Any]:
    """Write a list of JSON-like records to the staging area as one Parquet file.

    The records are spooled to a temporary JSON-lines file and converted by
    DuckDB, which infers the column types (nested objects become STRUCT/LIST).

    Returns:
        Summary with the number of rows and the staging path.
    """
    lake = LakeConfig.from_env()
    target = staging_path(lake, source, entity, load_date, batch_id)

    handle = tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", prefix="staging_", delete=False, encoding="utf-8"
    )
    try:
        count = 0
        with handle:
            for record in records:
                handle.write(json.dumps(record, default=str, ensure_ascii=False))
                handle.write("\n")
                count += 1
        if count == 0:
            raise ValueError(f"No records to stage for {source}/{entity}.")

        con = connect()
        try:
            rows = con.execute(
                f"""
                COPY (
                    SELECT *
                    FROM read_json({sql_literal(handle.name)}, format = 'newline_delimited', sample_size = -1)
                ) TO {sql_literal(target)} (FORMAT parquet, COMPRESSION zstd)
                """
            ).fetchone()[0]
        finally:
            con.close()
    finally:
        try:
            os.unlink(handle.name)
        except OSError:
            pass

    log.info("Staged %s rows at %s", rows, target)
    return {"source": source, "entity": entity, "rows": int(rows), "staging_path": target}


def promote_to_bronze(
    *,
    source: str,
    entity: str,
    batch_id: str,
    load_date: str,
) -> dict[str, Any]:
    """Import one staged batch into the bronze layer of the data lake.

    The data is copied as-is; only the lineage columns are added
    (``_ingested_at``, ``_source_system``, ``_source_entity``, ``_batch_id``,
    ``_source_file``). Re-running the same batch overwrites its own file.
    """
    lake = LakeConfig.from_env()
    origin = staging_path(lake, source, entity, load_date, batch_id)
    target = bronze_path(lake, source, entity, load_date, batch_id)

    con = connect()
    try:
        rows = con.execute(
            f"""
            COPY (
                SELECT
                    src.*,
                    TIMESTAMP {sql_literal(utc_timestamp())}   AS _ingested_at,
                    {sql_literal(source)}                      AS _source_system,
                    {sql_literal(entity)}                      AS _source_entity,
                    {sql_literal(batch_id)}                    AS _batch_id,
                    {sql_literal(origin)}                      AS _source_file
                FROM read_parquet({sql_literal(origin)}, hive_partitioning = false) AS src
            ) TO {sql_literal(target)} (FORMAT parquet, COMPRESSION zstd)
            """
        ).fetchone()[0]
    finally:
        con.close()

    log.info("Promoted %s rows from %s to %s", rows, origin, target)
    return {"source": source, "entity": entity, "rows": int(rows), "bronze_path": target}
