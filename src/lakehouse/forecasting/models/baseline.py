"""Baseline 3 (``ml_models/Baseline/Baseline_Models_EN.ipynb``, section 5).

No model: the forecast of month M is the value of the same month one year
before multiplied by the recent growth, i.e. the sum of the K = 3 months up to
the cutoff divided by the same 3 months one year earlier.

* monthly version (the reference of every study): monthly totals;
* daily version: each day copies the day 364 days before (same weekday) and the
  growth compares the same K months day by day with the days 364 days before.
"""
from __future__ import annotations

import pandas as pd

from ..catalog import job_specs
from ..data import Inputs
from ..protocol import MARKETS, cutoff_of, month_days, month_of
from . import ModelOutput, Timer, from_daily, from_monthly


def b3_monthly(series: pd.Series, month, k: int = 3) -> float | None:
    """series: daily values of one market (index = day) up to at least the cutoff."""
    month = month_of(month)
    hist = series[series.index <= cutoff_of(month)]
    mt = hist.groupby(hist.index.to_period("M")).sum()
    last = month - 2
    recent = [last - i for i in range(k)]
    prior = [p - 12 for p in recent]
    base = month - 12
    if any(p not in mt.index for p in recent + prior + [base]):
        return None
    den = mt[prior].sum()
    if den == 0:
        return None
    return float(mt[base] * mt[recent].sum() / den)


def b3_daily(series: pd.Series, month, k: int = 3) -> pd.Series | None:
    """Daily forecast of the month (index = day) or None without enough history."""
    month = month_of(month)
    cutoff = cutoff_of(month)
    hist = series[series.index <= cutoff]
    shift = pd.Timedelta(days=364)
    win = pd.date_range((month - 2 - (k - 1)).start_time, cutoff, freq="D")
    days = month_days(month)
    if hist.empty or (win - shift).min() < hist.index.min() or (days - shift).min() < hist.index.min():
        return None
    den = hist.reindex(win - shift).sum()
    if den == 0:
        return None
    growth = hist.reindex(win).sum() / den
    return pd.Series(hist.reindex(days - shift).to_numpy() * growth, index=days)


def b3_monthly_by_market(inputs: Inputs, target: str, months, k: int = 3) -> dict[tuple[str, pd.Period], float]:
    """{(market, month): Baseline 3 monthly} - also used by the 50/50 blend."""
    out = {}
    for mk in MARKETS:
        s = inputs.daily[inputs.daily["mark"] == mk].set_index("day")[target]
        for m in months:
            v = b3_monthly(s, m, k)
            if v is None:
                raise ValueError(f"Baseline 3 has no history to forecast {m} ({mk}): it needs the same month one year "
                                 f"before and 3 months of growth before the cutoff.")
            out[(mk, month_of(m))] = v
    return out


def run(inputs: Inputs, target: str, months) -> list[ModelOutput]:
    specs = {s.key: s for s in job_specs(target, "baseline")}
    outputs = []
    if "b3_monthly" in specs:
        with Timer() as t:
            k = int(specs["b3_monthly"].config["k_months"])
            values = b3_monthly_by_market(inputs, target, months, k)
            frame = pd.DataFrame([{"mark": mk, "month": m.start_time, "forecast": v} for (mk, m), v in values.items()])
            out = from_monthly("b3_monthly", frame)
            out.details["growth"] = _growth_table(inputs, target, months, k)
        out.seconds = t.seconds
        outputs.append(out)
    if "b3_daily" in specs:
        with Timer() as t:
            k = int(specs["b3_daily"].config["k_months"])
            parts = []
            for mk in MARKETS:
                s = inputs.daily[inputs.daily["mark"] == mk].set_index("day")[target]
                for m in months:
                    fc = b3_daily(s, m, k)
                    if fc is None:
                        raise ValueError(f"Baseline 3 (daily) has no history to forecast {m} ({mk}).")
                    parts.append(pd.DataFrame({"mark": mk, "day": fc.index, "forecast": fc.to_numpy()}))
            out = from_daily("b3_daily", pd.concat(parts, ignore_index=True))
        out.seconds = t.seconds
        outputs.append(out)
    return outputs


def _growth_table(inputs: Inputs, target: str, months, k: int) -> pd.DataFrame:
    """Growth applied in every month (for reading the baseline in MLflow)."""
    rows = []
    for mk in MARKETS:
        s = inputs.daily[inputs.daily["mark"] == mk].set_index("day")[target]
        mt = s.groupby(s.index.to_period("M")).sum()
        for m in months:
            m = month_of(m)
            recent = [m - 2 - i for i in range(k)]
            prior = [p - 12 for p in recent]
            rows.append({"mark": mk, "month": str(m), "same_month_last_year": float(mt.get(m - 12, float("nan"))),
                         "growth": float(mt[recent].sum() / mt[prior].sum()),
                         "growth_months": ",".join(str(p) for p in recent)})
    return pd.DataFrame(rows)
