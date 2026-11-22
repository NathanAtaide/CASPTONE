"""Candidate models. Every job returns :class:`ModelOutput` objects with the same shape.

* ``daily``   - mark (Online ES / Online PT / ES + PT), day, forecast, seed_min, seed_max
* ``monthly`` - mark, month (first day), forecast, seed_min, seed_max
* ``seeds``   - mark, month, seed, forecast (monthly forecast of each seed, neural networks only)

``forecast`` is the forecast that would be used: the mean of the seeds for the
neural networks. Models working on the ES + PT total (C.4, D.1, F.1) only have
``ES + PT`` rows; models without a daily forecast (Baseline 3 monthly, 50/50
blend) have an empty ``daily`` table.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from ..protocol import MARKETS, TOTAL

DAILY_COLUMNS = ["mark", "day", "forecast", "seed_min", "seed_max"]
MONTHLY_COLUMNS = ["mark", "month", "forecast", "seed_min", "seed_max"]
SEED_COLUMNS = ["mark", "month", "seed", "forecast"]


@dataclass
class ModelOutput:
    model_key: str
    daily: pd.DataFrame = field(default_factory=lambda: pd.DataFrame(columns=DAILY_COLUMNS))
    monthly: pd.DataFrame = field(default_factory=lambda: pd.DataFrame(columns=MONTHLY_COLUMNS))
    seeds: pd.DataFrame = field(default_factory=lambda: pd.DataFrame(columns=SEED_COLUMNS))
    details: dict[str, pd.DataFrame] = field(default_factory=dict)   # extra tables logged to MLflow
    params: dict = field(default_factory=dict)                       # extra params logged to MLflow
    warnings: list[str] = field(default_factory=list)
    seconds: float = 0.0


def _month(days: pd.Series) -> pd.Series:
    return days.dt.to_period("M").dt.to_timestamp()


def from_daily_seeds(key: str, frame: pd.DataFrame) -> ModelOutput:
    """frame: mark, day, seed, forecast (one row per market, day and seed)."""
    tot = frame.groupby(["day", "seed"], as_index=False)["forecast"].sum().assign(mark=TOTAL)
    full = pd.concat([frame, tot], ignore_index=True)
    daily = (full.groupby(["mark", "day"])["forecast"].agg(forecast="mean", seed_min="min", seed_max="max")
             .reset_index())
    seeds = (full.assign(month=_month(full["day"])).groupby(["mark", "month", "seed"], as_index=False)["forecast"].sum())
    monthly = (seeds.groupby(["mark", "month"])["forecast"].agg(forecast="mean", seed_min="min", seed_max="max")
               .reset_index())
    return ModelOutput(key, daily=daily[DAILY_COLUMNS], monthly=monthly[MONTHLY_COLUMNS], seeds=seeds[SEED_COLUMNS])


def from_monthly_seeds(key: str, frame: pd.DataFrame) -> ModelOutput:
    """frame: mark, month, seed, forecast (monthly models with seeds, e.g. the 50/50 blend)."""
    tot = frame.groupby(["month", "seed"], as_index=False)["forecast"].sum().assign(mark=TOTAL)
    seeds = pd.concat([frame, tot], ignore_index=True)
    monthly = (seeds.groupby(["mark", "month"])["forecast"].agg(forecast="mean", seed_min="min", seed_max="max")
               .reset_index())
    return ModelOutput(key, monthly=monthly[MONTHLY_COLUMNS], seeds=seeds[SEED_COLUMNS])


def from_daily(key: str, frame: pd.DataFrame) -> ModelOutput:
    """frame: mark, day, forecast (deterministic models; mark may be TOTAL only)."""
    frame = frame.copy()
    if set(frame["mark"]) <= set(MARKETS) and not frame.empty:
        tot = frame.groupby("day", as_index=False)["forecast"].sum().assign(mark=TOTAL)
        frame = pd.concat([frame, tot], ignore_index=True)
    frame["seed_min"] = np.nan
    frame["seed_max"] = np.nan
    monthly = (frame.assign(month=_month(frame["day"])).groupby(["mark", "month"], as_index=False)["forecast"].sum())
    monthly["seed_min"] = np.nan
    monthly["seed_max"] = np.nan
    return ModelOutput(key, daily=frame[DAILY_COLUMNS], monthly=monthly[MONTHLY_COLUMNS])


def from_monthly(key: str, frame: pd.DataFrame) -> ModelOutput:
    """frame: mark, month, forecast (deterministic monthly models, per market)."""
    tot = frame.groupby("month", as_index=False)["forecast"].sum().assign(mark=TOTAL)
    monthly = pd.concat([frame, tot], ignore_index=True)
    monthly["seed_min"] = np.nan
    monthly["seed_max"] = np.nan
    return ModelOutput(key, monthly=monthly[MONTHLY_COLUMNS])


class Timer:
    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.seconds = time.perf_counter() - self.start
        return False
