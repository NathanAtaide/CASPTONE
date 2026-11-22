"""
### ml_forecast_next_month - forecast of the next month with the champion of each target

Uses the champions registered by `ml_forecast_selection` in the MLflow model
registry (`caps_forecast_<target>`, alias `champion`) to forecast the month
after the last day of data in silver (January 2026 with the current data), for
**orders**, **net units** and **gross sales** of Online ES + Online PT.

Each champion is a "forecast method": it is retrained with the data up to the
cutoff of the month (last day of M-2, the protocol the backtest validated) and
then forecasts every day of the month.

**Trigger parameters** (Trigger DAG w/ config):
* `paid_media_budget_eur` - planned paid media of the month, **Online ES + Online
  PT together**, in EUR (premise: the marketing budget of the next month is
  known; January 2026: 438070). Needed when a champion uses the paid media of
  the month (scenario C models). The total is split between the markets by
  their share of the spend in the same month one year before, and each
  market's budget is spread over the days with the daily profile of the same
  weekdays one year before;
* `paid_media_budget_es_eur` / `paid_media_budget_pt_eur` - optional amounts per
  market (they replace the split of the total);
* `month` - `YYYY-MM`; empty = the month after the last day of data;
* `targets` - targets to forecast (default: all three).

When `slv_events` has no calendar for the month, the events of the same month
one year before are repeated (fixed campaigns and Clearance keep their dates);
the forecast records this in `notes`.

Output (gold, then `gold_export_powerbi` through the `lake_gold_forecast` asset):
`gld_forecast_next_month` (monthly, per market and ES + PT) and
`gld_forecast_next_month_daily`. Forecasting the same month again replaces its
rows; other months are kept. Each forecast is also an MLflow run
`forecast_<YYYY-MM>` in the experiment of its target.

**Schedule:** manual (the budget is a business input of each run).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param, get_current_context, task

from caps_assets import GOLD_FORECAST

ML_PYTHON = os.environ.get("ML_PYTHON", "/opt/ml-venv/bin/python")
CLI = f'"{ML_PYTHON}" -m lakehouse.forecasting'
TARGETS = ["orders", "units", "sales"]

# Empty string when the parameter is null, so the CLI receives "" (= not given).
_NUM = "{{{{ params.{0} if params.{0} is not none else '' }}}}"

with DAG(
    dag_id="ml_forecast_next_month",
    description="Forecasts the next month of orders, units and gross sales with the registered champions.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["ml", "mlflow", "forecast", "gold"],
    doc_md=__doc__,
    params={
        "paid_media_budget_eur": Param(None, type=["null", "number"], minimum=0, title="Paid media budget (ES + PT, EUR)",
                                       description="Planned paid media of the month for Online ES + Online PT together, "
                                                   "e.g. 438070 for January 2026. Needed by champions of scenario C."),
        "paid_media_budget_es_eur": Param(None, type=["null", "number"], minimum=0, title="Budget Online ES (EUR, optional)",
                                          description="Optional: budget of Online ES (replaces the split of the total)."),
        "paid_media_budget_pt_eur": Param(None, type=["null", "number"], minimum=0, title="Budget Online PT (EUR, optional)",
                                          description="Optional: budget of Online PT (replaces the split of the total)."),
        "month": Param("", type="string", pattern=r"^(\d{4}-(0[1-9]|1[0-2]))?$", title="Month (YYYY-MM)",
                       description="Month to forecast. Empty = the month after the last day of data in silver."),
        "targets": Param(TARGETS, type="array", items={"type": "string", "enum": TARGETS}, title="Targets"),
    },
) as dag:

    @task
    def targets_to_forecast(params: dict | None = None) -> list[str]:
        targets = [t for t in (params or {}).get("targets") or TARGETS if t in TARGETS]
        if not targets:
            raise ValueError(f"No valid target selected. Options: {TARGETS}")
        return targets

    @task.bash(
        env={"FC_RUN_ID": "{{ run_id }}", "FC_MONTH": "{{ params.month or '' }}",
             "FC_BUDGET": _NUM.format("paid_media_budget_eur"), "FC_BUDGET_ES": _NUM.format("paid_media_budget_es_eur"),
             "FC_BUDGET_PT": _NUM.format("paid_media_budget_pt_eur")},
        append_env=True,
        map_index_template="{{ forecast_target_label }}",
        max_active_tis_per_dagrun=1,
        execution_timeout=timedelta(minutes=60),
    )
    def forecast(target: str) -> str:
        """Load the champion from the MLflow registry, retrain it up to M-2 and forecast the month."""
        get_current_context()["forecast_target_label"] = target
        return (f'{CLI} forecast --run-id "$FC_RUN_ID" --target {target} --month "$FC_MONTH" '
                f'--budget-total "$FC_BUDGET" --budget-es "$FC_BUDGET_ES" --budget-pt "$FC_BUDGET_PT"')

    publish = BashOperator(
        task_id="publish_forecast",
        bash_command=f'{CLI} publish-forecast --run-id "$FC_RUN_ID" --targets "$FC_TARGETS"',
        env={"FC_RUN_ID": "{{ run_id }}", "FC_TARGETS": "{{ params.targets | join(',') }}"},
        append_env=True,
        outlets=[GOLD_FORECAST],
        execution_timeout=timedelta(minutes=15),
    )

    forecast.expand(target=targets_to_forecast()) >> publish
