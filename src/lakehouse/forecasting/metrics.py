"""Error metrics, leaderboard and the champion rule.

Metrics (same formulas as the notebooks, stored as fractions: 0.08 = 8%):

* WAPE = sum |forecast - actual| / sum actual   (the selection metric)
* Bias = sum (forecast - actual) / sum actual   (negative = under-forecast)
* RMSE = sqrt(mean((forecast - actual)^2))      (in orders / units / euros)
* R2   = 1 - SSE / SST                          (needs at least 2 points)

Every model is judged on the **monthly totals of ES + PT** over the backtest
window (the protocol of the Baseline and Deep Learning notebooks). Daily
metrics are reported for the models that forecast days.

Champion rule (one per target, single ranking with every family):
1. lowest monthly WAPE;
2. models within ``TIE_PP`` percentage points of the best are a technical tie
   (README of the ML study), broken by the smallest |bias|, then by the lowest
   daily WAPE (models without a daily forecast go last), then by the RMSE.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .protocol import MARKETS, TOTAL

TIE_PP = 0.5
SELECTION_RULE = (f"lowest monthly WAPE (ES + PT) on the backtest window; models within {TIE_PP} pp of the best "
                  "are tied and the tie is broken by |bias|, then daily WAPE, then RMSE")


def scores(actual, forecast) -> dict[str, float]:
    a, f = np.asarray(actual, float), np.asarray(forecast, float)
    if len(a) == 0 or a.sum() == 0:
        return {"wape": np.nan, "bias": np.nan, "rmse": np.nan, "r2": np.nan, "n": len(a)}
    err = f - a
    sst = ((a - a.mean()) ** 2).sum()
    return {"wape": float(np.abs(err).sum() / a.sum()), "bias": float(err.sum() / a.sum()),
            "rmse": float(np.sqrt(np.mean(err ** 2))),
            "r2": float(1 - (err ** 2).sum() / sst) if len(a) >= 2 and sst > 0 else np.nan, "n": int(len(a))}


def actuals(daily_inputs: pd.DataFrame, target: str, months) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Actual values of the backtest window: daily and monthly, per market and ES + PT."""
    starts = {m.start_time for m in months}
    d = daily_inputs[["mark", "day", target]].rename(columns={target: "actual"})
    d = d[d["day"].dt.to_period("M").dt.to_timestamp().isin(starts)]
    tot = d.groupby("day", as_index=False)["actual"].sum().assign(mark=TOTAL)
    daily = pd.concat([d, tot], ignore_index=True)
    monthly = (daily.assign(month=daily["day"].dt.to_period("M").dt.to_timestamp())
               .groupby(["mark", "month"], as_index=False)["actual"].sum())
    return daily, monthly


def evaluate(output, daily_actual: pd.DataFrame, monthly_actual: pd.DataFrame, months) -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    """Metrics of one model plus its monthly and daily tables joined with the actual values."""
    starts = sorted(m.start_time for m in months)
    mon = output.monthly.merge(monthly_actual, on=["mark", "month"], how="left")
    covered = set(mon.loc[mon["mark"] == TOTAL, "month"])
    missing = [str(s.date())[:7] for s in starts if s not in covered]
    if missing:
        raise ValueError(f"{output.model_key} has no forecast for {missing}: every model must cover the whole window.")
    m = {}
    tot = mon[mon["mark"] == TOTAL].sort_values("month")
    for k, v in scores(tot["actual"], tot["forecast"]).items():
        m[f"{k}_monthly"] = v
    for mk, tag in ((MARKETS[0], "es"), (MARKETS[1], "pt")):
        part = mon[mon["mark"] == mk]
        sc = scores(part["actual"], part["forecast"]) if not part.empty else {"wape": np.nan, "bias": np.nan}
        m[f"wape_monthly_{tag}"] = sc["wape"]
        m[f"bias_monthly_{tag}"] = sc["bias"]

    day = output.daily.merge(daily_actual, on=["mark", "day"], how="left") if not output.daily.empty else output.daily.assign(actual=np.nan)
    dt = day[day["mark"] == TOTAL] if not day.empty else day
    sc = scores(dt["actual"], dt["forecast"]) if not dt.empty else {"wape": np.nan, "bias": np.nan, "rmse": np.nan, "r2": np.nan}
    for k in ("wape", "bias", "rmse", "r2"):
        m[f"{k}_daily"] = sc[k]

    # Spread between seeds (neural networks): WAPE of each seed's monthly ES + PT forecast.
    if not output.seeds.empty:
        s = output.seeds[output.seeds["mark"] == TOTAL].merge(monthly_actual[monthly_actual["mark"] == TOTAL], on=["mark", "month"])
        per_seed = s.groupby("seed").apply(lambda g: scores(g["actual"], g["forecast"])["wape"], include_groups=False)
        m.update(wape_seed_mean=float(per_seed.mean()), wape_seed_min=float(per_seed.min()), wape_seed_max=float(per_seed.max()))
    else:
        m.update(wape_seed_mean=np.nan, wape_seed_min=np.nan, wape_seed_max=np.nan)

    for frame in (mon, day):
        if not frame.empty:
            frame["error"] = frame["forecast"] - frame["actual"]
            frame["abs_error"] = frame["error"].abs()
            frame["error_pct"] = frame["error"] / frame["actual"].replace(0, np.nan)
    return m, mon, day


def leaderboard(rows: list[dict]) -> pd.DataFrame:
    """rows: one dict per model with model_key, is_baseline_reference and the metrics of :func:`evaluate`."""
    lb = pd.DataFrame(rows)
    lb["abs_bias_monthly"] = lb["bias_monthly"].abs()
    lb = lb.sort_values(["wape_monthly", "abs_bias_monthly"]).reset_index(drop=True)
    lb["rank_wape"] = np.arange(1, len(lb) + 1)
    best = float(lb["wape_monthly"].min())
    lb["within_tie_of_best"] = lb["wape_monthly"] <= best + TIE_PP / 100 + 1e-12
    tied = lb[lb["within_tie_of_best"]].assign(_daily=lambda d: d["wape_daily"].fillna(np.inf))
    champion = tied.sort_values(["abs_bias_monthly", "_daily", "rmse_monthly"]).iloc[0]["model_key"]
    lb["is_champion"] = lb["model_key"] == champion
    ref = lb[lb["is_baseline_reference"]]
    base_wape = float(ref["wape_monthly"].iloc[0]) if not ref.empty else np.nan
    lb["baseline_wape_monthly"] = base_wape
    lb["wape_vs_baseline_pp"] = 100 * (lb["wape_monthly"] - base_wape)
    lb["beats_baseline"] = lb["wape_monthly"] < base_wape
    return lb.drop(columns=["abs_bias_monthly"])
