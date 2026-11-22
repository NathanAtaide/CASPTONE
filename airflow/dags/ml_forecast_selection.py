"""
### ml_forecast_selection - backtest of every forecasting model, MLflow comparison and champion per target

Integrates the studies of `ml_models/` (Baseline, Machine Learning and Deep
Learning) into the platform. For each target - **orders**, **net units** and
**gross sales** of Online ES + Online PT - every candidate model is retrained
with the protocol of the studies and evaluated on the same backtest window:

* window: the last `backtest_months` complete months of data (default 5 =
  August to December 2025, the Test window of the studies);
* cutoff: month M is forecast with data up to the last day of M-2, monthly refit;
* candidates (`lakehouse/forecasting/catalog.py`): Baseline 3 monthly (the
  reference) and daily; ML C.4 Random Forest, D.1 Gradient Boosting, F.1 XGBoost
  by category group (units/sales); DL MLP A, 50/50 Baseline 3 + MLP A, MLP C,
  and for orders MLP C + campaigns and LSTM C + campaigns.

**Champion rule (single ranking):** lowest monthly WAPE of ES + PT; models
within 0.5 pp are tied and the tie is broken by |bias|, then daily WAPE, then RMSE.
The scenario C models use the paid media of the forecast month (premise: the
marketing budget of the next month is known).

Tasks:
1. `prepare` - builds the inputs from silver, checks whether the data changed
   since the last selection (otherwise the run stops at `data_changed`), saves
   the inputs to `s3://gold/_forecast/selection/<run>/` and opens one MLflow
   parent run per target;
2. `run_job` (mapped, one per target x job) - backtest, one MLflow child run per model;
3. `select_champions` - leaderboard, registers the champion of each target as
   `caps_forecast_<target>` (alias `champion`) and writes the gold tables
   `gld_forecast_backtest_monthly`, `gld_forecast_backtest_daily`,
   `gld_forecast_model_metrics` and `gld_forecast_champion`, then updates the
   `lake_gold_forecast` asset (-> `gold_export_powerbi`).

**MLflow** (http://localhost:5000): experiments `caps_forecast_orders`,
`caps_forecast_units`, `caps_forecast_sales` - open the parent run
`selection_<run>` and compare its child runs.

**Schedule:** data-aware on `lake_gold` (after `dbt_medallion`); a run whose
silver data is unchanged is skipped in seconds. Trigger it manually with
`force = true` to re-run anyway. The models run in the isolated virtualenv
`/opt/ml-venv` (torch CPU, scikit-learn, xgboost, MLflow client). A full run
takes about 20 minutes on 2 CPUs (the LSTM alone takes about 12).
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param, get_current_context, task

from caps_assets import GOLD, GOLD_FORECAST

ML_PYTHON = os.environ.get("ML_PYTHON", "/opt/ml-venv/bin/python")
CLI = f'"{ML_PYTHON}" -m lakehouse.forecasting'
TARGETS = ["orders", "units", "sales"]
# Neural networks use one thread each and the random forest two: 2 jobs at a time fit a 2-CPU host.
MAX_PARALLEL_JOBS = int(os.environ.get("FORECAST_MAX_PARALLEL_JOBS", "2"))
SAFE_TOKEN = re.compile(r"^[a-z0-9_]+$")


def _last_json(output: str) -> dict:
    """The CLI prints its JSON summary as the last line of stdout (the BashOperator XCom)."""
    return json.loads(str(output).strip().splitlines()[-1])


with DAG(
    dag_id="ml_forecast_selection",
    description="Backtests the Baseline, ML and DL forecasting models, logs them to MLflow and registers a champion per target.",
    schedule=[GOLD],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["ml", "mlflow", "forecast", "gold"],
    doc_md=__doc__,
    params={
        "force": Param(False, type="boolean", title="Force",
                       description="Run even if the silver data did not change since the last successful selection."),
        "backtest_months": Param(5, type="integer", minimum=1, maximum=8, title="Backtest months",
                                 description="Last complete months used to compare the models (5 = Aug-Dec 2025). "
                                             "Baseline 3 needs 16 months of history, so at most 8 with data since 2024-01."),
        "targets": Param(TARGETS, type="array", items={"type": "string", "enum": TARGETS}, title="Targets",
                         description="Targets to evaluate (default: all three)."),
    },
) as dag:

    prepare = BashOperator(
        task_id="prepare",
        bash_command=(f'{CLI} prepare --run-id "$FC_RUN_ID" --backtest-months "$FC_BACKTEST_MONTHS" '
                      f'--targets "$FC_TARGETS" --force "$FC_FORCE"'),
        env={"FC_RUN_ID": "{{ run_id }}", "FC_BACKTEST_MONTHS": "{{ params.backtest_months }}",
             "FC_TARGETS": "{{ params.targets | join(',') }}", "FC_FORCE": "{{ params.force }}"},
        append_env=True,
        execution_timeout=timedelta(minutes=20),
    )

    @task.short_circuit
    def data_changed(prepare_output: str) -> bool:
        """Stop here (downstream tasks skipped) when the data is the same as in the last selection."""
        summary = _last_json(prepare_output)
        if summary.get("skip"):
            print(f"Nothing to do: {summary.get('reason')} (batch {summary.get('previous_batch_id')}). "
                  f"Trigger with force=true to run anyway.")
        return not summary.get("skip")

    @task
    def plan(prepare_output: str) -> list[dict]:
        """One entry per target x job, slowest jobs first."""
        jobs = _last_json(prepare_output)["jobs"]
        for item in jobs:
            if not (SAFE_TOKEN.match(item["target"]) and SAFE_TOKEN.match(item["job"])):
                raise ValueError(f"Unexpected job name: {item}")
        return jobs

    @task.bash(
        env={"FC_RUN_ID": "{{ run_id }}"},
        append_env=True,
        map_index_template="{{ forecast_job_label }}",
        max_active_tis_per_dagrun=MAX_PARALLEL_JOBS,
        execution_timeout=timedelta(minutes=90),
    )
    def run_job(target: str, job: str) -> str:
        """Backtest one job (one or two models) of one target and log it to MLflow."""
        get_current_context()["forecast_job_label"] = f"{target} / {job}"
        return f'{CLI} run-job --run-id "$FC_RUN_ID" --target {target} --job {job}'

    select = BashOperator(
        task_id="select_champions",
        bash_command=f'{CLI} select --run-id "$FC_RUN_ID"',
        env={"FC_RUN_ID": "{{ run_id }}"},
        append_env=True,
        outlets=[GOLD_FORECAST],
        execution_timeout=timedelta(minutes=30),
    )

    jobs = plan(prepare.output)
    data_changed(prepare.output) >> jobs
    run_job.expand_kwargs(jobs) >> select
