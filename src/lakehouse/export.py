"""Export gold datasets from the lake to Parquet files on the host (Power BI).

    s3://<gold>/<dataset>.parquet  -->  <EXPORT_DIR>/powerbi/<dataset>.parquet

``EXPORT_DIR`` is bind-mounted to ``./data/exports`` on the host, so the files
leave Docker and can be opened by Power BI Desktop ("Get data > Parquet" or
"Folder"). Each file is written to a temporary name and renamed at the end, so
a reader never sees a half-written file. Files use Snappy compression, the
most widely supported codec among BI tools.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import LakeConfig, setting
from .duck import connect, sql_literal

log = logging.getLogger(__name__)

DEFAULT_SUBDIR = "powerbi"
MANIFEST_NAME = "_manifest.json"


def export_directory(subdir: str = DEFAULT_SUBDIR) -> Path:
    """Folder (inside the container) that maps to ./data/exports/<subdir> on the host."""
    return Path(setting("EXPORT_DIR", "/opt/platform/exports")) / subdir


def list_gold_datasets(con=None) -> list[str]:
    """Names of the datasets available in the gold layer (one Parquet per dbt model)."""
    lake = LakeConfig.from_env()
    own_connection = con is None
    con = con or connect()
    try:
        rows = con.execute(
            f"SELECT file FROM glob({sql_literal(lake.uri('gold') + '/*.parquet')}) ORDER BY file"
        ).fetchall()
    finally:
        if own_connection:
            con.close()
    # 's3://gold/gld_sales.parquet' -> 'gld_sales'
    return [row[0].rsplit("/", 1)[-1].rsplit(".", 1)[0] for row in rows]


def export_gold(datasets: list[str] | None = None, *, subdir: str = DEFAULT_SUBDIR) -> list[dict[str, Any]]:
    """Export gold datasets to local Parquet files.

    Args:
        datasets: dataset names to export; ``None``/empty exports every gold dataset.
        subdir: folder created under EXPORT_DIR.

    Returns:
        One summary per exported dataset (name, rows, file).
    """
    lake = LakeConfig.from_env()
    target_dir = export_directory(subdir)
    target_dir.mkdir(parents=True, exist_ok=True)

    con = connect()
    try:
        available = list_gold_datasets(con)
        selected = list(datasets) if datasets else available
        missing = sorted(set(selected) - set(available))
        if missing:
            raise ValueError(f"Gold datasets not found: {missing}. Available: {available}")

        results: list[dict[str, Any]] = []
        for name in selected:
            source = lake.uri("gold", f"{name}.parquet")
            final_path = target_dir / f"{name}.parquet"
            temporary_path = target_dir / f"{name}.parquet.tmp"

            rows = con.execute(
                f"""
                COPY (SELECT * FROM read_parquet({sql_literal(source)}))
                TO {sql_literal(temporary_path.as_posix())} (FORMAT parquet, COMPRESSION snappy)
                """
            ).fetchone()[0]
            try:
                # Atomic swap: Power BI either sees the old file or the new one.
                os.replace(temporary_path, final_path)
            except OSError as error:
                raise RuntimeError(
                    f"Could not replace {final_path.name}: the file is probably open in another "
                    f"program (close Power BI / wait for its refresh and run the export again)."
                ) from error

            log.info("Exported %s rows of %s to %s", rows, name, final_path)
            results.append({"dataset": name, "rows": int(rows), "file": str(final_path)})
    finally:
        con.close()

    # Small manifest so anyone looking at the folder knows how fresh the files are.
    manifest = {
        "exported_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "datasets": {item["dataset"]: {"rows": item["rows"]} for item in results},
    }
    (target_dir / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return results
