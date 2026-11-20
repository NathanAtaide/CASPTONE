"""
### gold_scd2_simulation - simulate a SCD type 2 change on a gold dimension

The source has a single snapshot, so `gld_dim_product` and `gld_dim_customer` are loaded with
every row valid (1900-01-01 -> 9999-12-31). This DAG simulates what a later load would do when
one member changes, **without touching the dbt output**:

1. `plan` - validates the change against the SCD type of each column (type 0 is refused) and
   picks the base files: the latest files of the scenario, or the gold layer on the first run
   (or with `reset`);
2. `apply_change` - closes / updates the current version and opens the new one, then writes the
   whole dimension to `s3://gold/_simulation/<scenario>/<run>/<dimension>.parquet`;
3. `rebuild_fact` - writes `gld_fact_sales` with the keys of that member looked up again by
   version key + order date, so orders before the change keep the old version and orders from
   the change on point to the new one;
4. `publish` - records the run in `s3://gold/_simulation/<scenario>/_state.json` and copies the
   latest files of the scenario to `./data/exports/scd2_simulation/<scenario>/` on the host.

Runs of the same `scenario` build on each other (version 1 -> 2 -> 3). The Power BI export of the
gold layer never reads `_simulation/`.

**Rules**

| Dimension | Natural key | Type 2 | Type 1 | Effective date |
|---|---|---|---|---|
| `gld_dim_product` | `prod_cod` | `active`, `fashiontype`, `active_category` | - | parameter `effective_date` |
| `gld_dim_customer` | `customer_id` | `subscription_date` after a cancellation (re-subscription) | `country`, `subscription_source`; cancellation and first subscription update the version in place | the dates in `changes` |

**Examples of `changes`**

* product: `{"fashiontype": "Basic"}` or `{"active": false}` with `effective_date` 2025-11-15;
* customer cancels: `{"unsubscription_date": "2025-03-01"}`;
* customer re-subscribes (new version): `{"subscription_date": "2025-06-01"}`;
* both in one run: `{"unsubscription_date": "2025-03-01", "subscription_date": "2025-06-01"}`.

**Schedule:** manual only. No pool: the tasks use in-memory DuckDB sessions.
"""
from __future__ import annotations

import logging
from datetime import datetime

from airflow.sdk import Param, dag, task

log = logging.getLogger(__name__)


@dag(
    dag_id="gold_scd2_simulation",
    description="Simulates a SCD2 change on gld_dim_product / gld_dim_customer in a separate area of the lake.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "scd2", "simulation"],
    doc_md=__doc__,
    params={
        "scenario": Param(
            "demo",
            type="string",
            pattern=r"^[A-Za-z0-9_-]{1,64}$",
            title="Scenario",
            description="Folder of the simulation. Runs of the same scenario build on each other.",
        ),
        "dimension": Param(
            "gld_dim_product",
            type="string",
            enum=["gld_dim_product", "gld_dim_customer"],
            title="Dimension",
        ),
        "natural_key": Param(
            "241859_BNL",
            type="string",
            title="Natural key",
            description="prod_cod (e.g. 241859_BNL) for gld_dim_product; customer_id (e.g. 1100724) for gld_dim_customer.",
        ),
        "changes": Param(
            {"fashiontype": "Basic"},
            type="object",
            title="Changes",
            description='New values, e.g. {"fashiontype": "Basic"} or {"unsubscription_date": "2025-03-01"}.',
        ),
        "effective_date": Param(
            "2025-11-15",
            type="string",
            format="date",
            title="Effective date",
            description="First day of the new version (gld_dim_product only; customers use the dates in changes).",
        ),
        "reset": Param(
            False,
            type="boolean",
            title="Reset",
            description="Start again from the gold layer, discarding the previous runs of the scenario.",
        ),
        "rebuild_fact": Param(
            True,
            type="boolean",
            title="Rebuild gld_fact_sales",
            description="Write the fact with the keys of the changed member looked up again.",
        ),
        "export_to_host": Param(
            True,
            type="boolean",
            title="Export to the host",
            description="Copy the latest files of the scenario to ./data/exports/scd2_simulation/<scenario>/.",
        ),
    },
)
def gold_scd2_simulation():
    @task
    def plan(params: dict | None = None, dag_run=None) -> dict:
        """Validate the change and choose the base and target files."""
        from lakehouse import scd2
        from lakehouse.naming import safe_name

        p = params or {}
        return scd2.plan_change(
            dimension=p.get("dimension"),
            natural_key=p.get("natural_key"),
            changes=p.get("changes"),
            effective_date=p.get("effective_date"),
            scenario=p.get("scenario"),
            batch_id=safe_name(dag_run.run_id),
            reset=bool(p.get("reset")),
            rebuild_fact=bool(p.get("rebuild_fact", True)),
            export_to_host=bool(p.get("export_to_host", True)),
        )

    @task
    def apply_change(plan: dict) -> dict:
        """Close / update the current version, open the new one and write the dimension."""
        from lakehouse import scd2

        return scd2.apply_change(plan)

    @task
    def rebuild_fact(plan: dict, change: dict) -> dict:
        """Write gld_fact_sales with the member's keys resolved again by order date."""
        from lakehouse import scd2

        return scd2.rebuild_fact(plan, change)

    @task
    def publish(plan: dict, change: dict, fact: dict) -> dict:
        """Record the run in the scenario state and export the latest files to the host."""
        from lakehouse import scd2

        summary = scd2.publish(plan, change, fact)
        log.info("Scenario %s now has %s change(s). Latest files: %s",
                 summary["scenario"], summary["changes_in_scenario"], summary["latest"])
        return summary

    planned = plan()
    change = apply_change(planned)
    fact = rebuild_fact(planned, change)
    publish(planned, change, fact)


gold_scd2_simulation()
