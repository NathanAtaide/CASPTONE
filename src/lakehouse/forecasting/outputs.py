"""Tables written to the gold layer for Power BI (``s3://<gold>/gld_forecast_*.parquet``).

They are written by the forecasting DAGs (Python, not dbt), declared as the dbt
source ``gold_forecast`` and copied to ``./data/exports/powerbi/`` by the
``gold_export_powerbi`` DAG like every other gold dataset.

Conventions of the star schema: keys INTEGER (``*_date_key`` = YYYYMMDD,
relates to ``gld_dim_date[date_key]``), simple types only, metrics as
fractions (0.08 = 8%). ``market`` is ``Online ES``, ``Online PT`` or
``ES + PT``; ``is_total`` marks the ES + PT rows - filter on it before summing,
otherwise the markets are counted twice.
"""
from __future__ import annotations

import pandas as pd

from ..config import LakeConfig
from ..duck import sql_identifier, sql_literal

BACKTEST_MONTHLY = "gld_forecast_backtest_monthly"
BACKTEST_DAILY = "gld_forecast_backtest_daily"
MODEL_METRICS = "gld_forecast_model_metrics"
CHAMPION = "gld_forecast_champion"
NEXT_MONTH = "gld_forecast_next_month"
NEXT_MONTH_DAILY = "gld_forecast_next_month_daily"

_MODEL_COLS = {"target": "VARCHAR", "target_label": "VARCHAR", "model_key": "VARCHAR", "model_name": "VARCHAR",
               "family": "VARCHAR", "algorithm": "VARCHAR", "scenario": "VARCHAR", "uses_paid_media": "BOOLEAN",
               "is_champion": "BOOLEAN", "is_baseline_reference": "BOOLEAN"}

SCHEMAS: dict[str, dict[str, str]] = {
    BACKTEST_MONTHLY: {
        "selection_batch_id": "VARCHAR", **_MODEL_COLS, "market": "VARCHAR", "is_total": "BOOLEAN",
        "month_start": "DATE", "month_date_key": "INTEGER", "month_label": "VARCHAR",
        "forecast": "DOUBLE", "forecast_seed_min": "DOUBLE", "forecast_seed_max": "DOUBLE", "actual": "DOUBLE",
        "error": "DOUBLE", "abs_error": "DOUBLE", "error_pct": "DOUBLE", "data_end": "DATE",
    },
    BACKTEST_DAILY: {
        "selection_batch_id": "VARCHAR", **_MODEL_COLS, "market": "VARCHAR", "is_total": "BOOLEAN",
        "date": "DATE", "date_key": "INTEGER", "month_start": "DATE",
        "forecast": "DOUBLE", "forecast_seed_min": "DOUBLE", "forecast_seed_max": "DOUBLE", "actual": "DOUBLE",
        "error": "DOUBLE", "abs_error": "DOUBLE",
    },
    MODEL_METRICS: {
        "selection_batch_id": "VARCHAR", "selected_at": "TIMESTAMP", **_MODEL_COLS, "level": "VARCHAR",
        "rank_wape": "INTEGER", "within_tie_of_best": "BOOLEAN", "backtest_start": "DATE", "backtest_end": "DATE",
        "backtest_months": "INTEGER", "data_end": "DATE",
        "wape_monthly": "DOUBLE", "bias_monthly": "DOUBLE", "rmse_monthly": "DOUBLE", "r2_monthly": "DOUBLE",
        "wape_daily": "DOUBLE", "bias_daily": "DOUBLE", "rmse_daily": "DOUBLE", "r2_daily": "DOUBLE",
        "wape_monthly_es": "DOUBLE", "bias_monthly_es": "DOUBLE", "wape_monthly_pt": "DOUBLE", "bias_monthly_pt": "DOUBLE",
        "wape_seed_mean": "DOUBLE", "wape_seed_min": "DOUBLE", "wape_seed_max": "DOUBLE",
        "baseline_wape_monthly": "DOUBLE", "wape_vs_baseline_pp": "DOUBLE", "beats_baseline": "BOOLEAN",
        "training_seconds": "DOUBLE", "mlflow_experiment": "VARCHAR", "mlflow_run_id": "VARCHAR",
    },
    CHAMPION: {
        "selection_batch_id": "VARCHAR", "selected_at": "TIMESTAMP", "target": "VARCHAR", "target_label": "VARCHAR",
        "model_key": "VARCHAR", "model_name": "VARCHAR", "family": "VARCHAR", "algorithm": "VARCHAR",
        "scenario": "VARCHAR", "uses_paid_media": "BOOLEAN",
        "wape_monthly": "DOUBLE", "bias_monthly": "DOUBLE", "rmse_monthly": "DOUBLE", "wape_daily": "DOUBLE",
        "baseline_wape_monthly": "DOUBLE", "wape_vs_baseline_pp": "DOUBLE", "beats_baseline": "BOOLEAN",
        "runner_up_model_key": "VARCHAR", "runner_up_wape_monthly": "DOUBLE",
        "best_ml_dl_model_key": "VARCHAR", "best_ml_dl_model_name": "VARCHAR", "best_ml_dl_wape_monthly": "DOUBLE",
        "registered_model_name": "VARCHAR", "registered_model_version": "INTEGER", "mlflow_run_id": "VARCHAR",
        "backtest_start": "DATE", "backtest_end": "DATE", "data_end": "DATE", "selection_rule": "VARCHAR",
    },
    NEXT_MONTH: {
        "forecast_batch_id": "VARCHAR", "generated_at": "TIMESTAMP", "target": "VARCHAR", "target_label": "VARCHAR",
        "forecast_month": "DATE", "month_date_key": "INTEGER", "month_label": "VARCHAR",
        "market": "VARCHAR", "is_total": "BOOLEAN",
        "forecast": "DOUBLE", "forecast_seed_min": "DOUBLE", "forecast_seed_max": "DOUBLE",
        "model_key": "VARCHAR", "model_name": "VARCHAR", "family": "VARCHAR", "scenario": "VARCHAR",
        "uses_paid_media": "BOOLEAN", "paid_media_budget_eur": "DOUBLE",
        "registered_model_name": "VARCHAR", "registered_model_version": "INTEGER", "selection_batch_id": "VARCHAR",
        "backtest_wape_monthly": "DOUBLE", "cutoff_date": "DATE", "data_end": "DATE", "notes": "VARCHAR",
        "mlflow_run_id": "VARCHAR",
    },
    NEXT_MONTH_DAILY: {
        "forecast_batch_id": "VARCHAR", "target": "VARCHAR", "target_label": "VARCHAR", "forecast_month": "DATE",
        "date": "DATE", "date_key": "INTEGER", "market": "VARCHAR", "is_total": "BOOLEAN",
        "forecast": "DOUBLE", "forecast_seed_min": "DOUBLE", "forecast_seed_max": "DOUBLE",
        "model_key": "VARCHAR", "paid_media_day_eur": "DOUBLE", "registered_model_version": "INTEGER",
    },
}


def gold_uri(name: str) -> str:
    return LakeConfig.from_env().uri("gold", f"{name}.parquet")


def date_key(values) -> pd.Series:
    s = pd.to_datetime(pd.Series(values))
    return (s.dt.year * 10000 + s.dt.month * 100 + s.dt.day).astype("Int64")


def write(con, name: str, frame: pd.DataFrame) -> int:
    """Write ``frame`` with the column order and types of :data:`SCHEMAS` (missing columns = null)."""
    schema = SCHEMAS[name]
    frame = frame.copy()
    for col in schema:
        if col not in frame.columns:
            frame[col] = None
    frame = frame[list(schema)]
    con.register("fc_gold", frame)
    try:
        select = ", ".join(f"CAST({sql_identifier(c)} AS {t}) AS {sql_identifier(c)}" for c, t in schema.items())
        con.execute(f"COPY (SELECT {select} FROM fc_gold) TO {sql_literal(gold_uri(name))} (FORMAT parquet, COMPRESSION zstd)")
    finally:
        con.unregister("fc_gold")
    return int(len(frame))


def read(con, name: str) -> pd.DataFrame | None:
    """Current content of a gold table, or None when it was never written."""
    uri = gold_uri(name)
    # A glob without wildcard does not check that the object exists on S3: list the prefix instead.
    files = {r[0] for r in con.execute(f"SELECT file FROM glob({sql_literal(uri + '*')})").fetchall()}
    if uri not in files:
        return None
    return con.execute(f"SELECT * FROM read_parquet({sql_literal(uri)})").df()
