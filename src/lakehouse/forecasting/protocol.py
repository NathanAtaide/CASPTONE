"""Forecasting protocol shared by every model of the studies in ``ml_models/``.

The rules below come from the Baseline, ML and Deep Learning notebooks and are
the same for every model, so the comparison measures the models and not the
information they were given:

* scope: the markets Online ES and Online PT, data from 2024-01-01;
* cutoff: month M is forecast with data up to the last day of M-2 (a forecast
  for September is made during August, with data up to 31 July);
* monthly refit: every month is forecast by a model trained again with all the
  data available at its cutoff (rolling origin);
* the monthly total of ES + PT is the number every model is judged on.
"""
from __future__ import annotations

import pandas as pd

MARKETS: tuple[str, ...] = ("Online ES", "Online PT")
TOTAL = "ES + PT"                       # label of the sum of the two markets
COUNTRY = {"Online ES": "ES", "Online PT": "PT"}
DATA_START = pd.Timestamp("2024-01-01")

TARGETS: tuple[str, ...] = ("orders", "units", "sales")
TARGET_LABELS = {
    "orders": "Orders",
    "units": "Net units",
    "sales": "Gross sales (EUR)",
}
TARGET_DEFINITIONS = {
    "orders": "orders with at least one SALE line and net units (SALE + CANCELLED) above zero, by order date",
    "units": "units of the SALE lines plus the CANCELLED lines (negative), of orders with a SALE line, by order date",
    "sales": "amount with discount and without VAT (EUR) of the SALE and CANCELLED lines, of orders with a SALE line, by order date",
}

# Number of recent months of the backtest window (the last complete months of data).
# 5 reproduces the Test window of the studies (August to December 2025).
DEFAULT_BACKTEST_MONTHS = 5
# Baseline 3 needs the same month of the previous year plus 3 months of growth:
# 16 complete months of history before the month it forecasts.
MIN_HISTORY_MONTHS = 16


def month_of(value) -> pd.Period:
    """Monthly period of a date, a 'YYYY-MM' string or a period."""
    if isinstance(value, pd.Period):
        return value.asfreq("M")
    return pd.Period(str(value)[:7], "M")


def month_start(month) -> pd.Timestamp:
    return month_of(month).start_time.normalize()


def month_end(month) -> pd.Timestamp:
    return month_of(month).end_time.normalize()


def month_days(month) -> pd.DatetimeIndex:
    return pd.date_range(month_start(month), month_end(month), freq="D")


def cutoff_of(month) -> pd.Timestamp:
    """Last day of data allowed to forecast ``month``: the last day of M-2."""
    return month_end(month_of(month) - 2)


def last_complete_month(data_end) -> pd.Period:
    """Last month whose every day is in the data."""
    data_end = pd.Timestamp(data_end).normalize()
    month = month_of(data_end)
    return month if data_end == month_end(month) else month - 1


def backtest_window(data_end, n_months: int) -> list[pd.Period]:
    """The ``n_months`` last complete months, oldest first."""
    if n_months < 1:
        raise ValueError("backtest_months must be at least 1.")
    last = last_complete_month(data_end)
    months = [last - i for i in range(n_months)][::-1]
    first_allowed = month_of(DATA_START) + MIN_HISTORY_MONTHS
    if months[0] < first_allowed:
        raise ValueError(
            f"The backtest window starts in {months[0]}, but the Baseline 3 (the reference every model is "
            f"compared with) only exists from {first_allowed} with data since {DATA_START.date()}. "
            f"Use at most {(last - first_allowed).n + 1} months."
        )
    return months


def next_month(data_end) -> pd.Period:
    """Month right after the month of the last day of data (the month the forecast DAG predicts)."""
    return month_of(pd.Timestamp(data_end)) + 1
