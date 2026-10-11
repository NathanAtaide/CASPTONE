"""
### gold_export_powerbi - gold -> Parquet files outside Docker (Power BI)

Copies the gold datasets from the data lake to the host folder
`./data/exports/powerbi/` (one `<dataset>.parquet` per gold model), ready to be
loaded in Power BI Desktop with *Get data > Parquet* or *Get data > Folder*.

* **Schedule:** data-aware - runs every time `dbt_medallion` refreshes the gold
  layer (`lake_gold` asset). It can also be triggered manually.
* **What is exported:** every gold dataset by default. Trigger with the
  `datasets` parameter to export only some of them.
* Files are replaced atomically and a `_manifest.json` records when the export
  happened and how many rows each file has.
"""
from __future__ import annotations

import logging
from datetime import datetime

from airflow.sdk import Param, dag, task

from caps_assets import GOLD

log = logging.getLogger(__name__)


@dag(
    dag_id="gold_export_powerbi",
    description="Exports the gold layer to Parquet files on the host for Power BI.",
    schedule=[GOLD],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "export", "powerbi"],
    doc_md=__doc__,
    params={
        "datasets": Param(
            [],
            type="array",
            items={"type": "string"},
            title="Datasets",
            description="Gold datasets to export (e.g. gld_sales_daily). Empty = all.",
        ),
    },
)
def gold_export_powerbi():
    @task(retries=1)
    def export_to_host(params: dict | None = None) -> list[dict]:
        """Write the selected gold datasets to ./data/exports/powerbi on the host."""
        from lakehouse import export as lake_export

        selected = [name for name in ((params or {}).get("datasets") or []) if name]
        results = lake_export.export_gold(selected or None)

        if not results:
            log.warning("The gold layer has no datasets yet - nothing was exported.")
        for result in results:
            log.info("%s: %s rows -> ./data/exports/powerbi/%s.parquet", result["dataset"], result["rows"], result["dataset"])
        return results

    export_to_host()


gold_export_powerbi()
