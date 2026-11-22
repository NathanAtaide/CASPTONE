"""Tree models of ``ml_models/ML/ML sobre silver.ipynb``: C.4, D.1 and F.1.

Shared design (README_ML_models_silver.md, section 3):

* inputs: yearly lag (364 days, same weekday), the most recent week known at
  the cutoff (lags 62 to 68) and the six fixed campaigns relative to the
  previous year (+1 / -1 / 0);
* target: deviation = (y / y_364) / recent growth, where the recent growth is
  the sum of the 28 days ending 62 days before the day over the same 28 days
  one year earlier; the forecast is converted back with deviation x growth x y_364;
* settings chosen in every refit by time-series cross-validation inside the
  training days only (up to 3 mini-tests: train, 30-day gap, validate 28 days).

C.4 and D.1 work on the daily total ES + PT. F.1 works on the daily series of
the 6 product category groups (one model, the group as an input, rows weighted
by the group size) and sums the groups.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..catalog import spec
from ..data import CATEGORY_GROUPS, Inputs, ml_campaign_active
from ..protocol import TOTAL, cutoff_of, month_end, month_of, month_start
from . import ModelOutput, Timer, from_daily

RECENT_LAGS = list(range(62, 69))
YEARLY_LAG = 364
GROWTH_WINDOW = 28
GROWTH_END_LAG = 62
CV_FOLDS, CV_VALID_DAYS, CV_GAP_DAYS, CV_MIN_TRAIN = 3, 28, 30, 28
LAG_COLS = [f"lag_{lag}" for lag in RECENT_LAGS + [YEARLY_LAG]]

C4_GRID = [{"max_depth": d, "min_samples_leaf": leaf} for d in (4, 6, 8) for leaf in (7, 14)]
C4_DEFAULT = {"max_depth": 6, "min_samples_leaf": 7}
C4_TREES_CV, C4_TREES_FINAL = 200, 500
D1_FIXED = dict(learning_rate=0.05, subsample=0.8, min_samples_leaf=7, random_state=42)
D1_DEPTHS, D1_MAX_TREES = (2, 3), 600
D1_DEFAULT = {"max_depth": 3, "n_estimators": 300}
F1_FIXED = dict(learning_rate=0.05, subsample=0.8, colsample_bytree=0.8, min_child_weight=7,
                tree_method="hist", random_state=42, n_jobs=1)
F1_GRID = [{"max_depth": d, "reg_lambda": lam} for d in (2, 3) for lam in (1, 10)]
F1_MAX_TREES, F1_TREE_STEP = 600, 50
F1_DEFAULT = {"max_depth": 3, "reg_lambda": 1, "n_estimators": 300}


def wape_pct(actual, forecast) -> float:
    a, f = np.asarray(actual, float), np.asarray(forecast, float)
    return float(100 * np.abs(f - a).sum() / a.sum())


def _campaigns_vs_last_year(inputs: Inputs, index: pd.DatetimeIndex) -> pd.DataFrame:
    active = ml_campaign_active(inputs.events, index)
    return (active - active.shift(YEARLY_LAG).fillna(0).astype(int)).add_suffix("_vs_ly")


def _total_series(inputs: Inputs, target: str) -> pd.Series:
    s = inputs.daily.groupby("day")[target].sum(min_count=1)
    return s.asfreq("D")


def _add_lag_features(frame: pd.DataFrame, value: str = "y") -> pd.DataFrame:
    s = frame[value]
    for lag in RECENT_LAGS + [YEARLY_LAG]:
        frame[f"lag_{lag}"] = s.shift(lag)
    recent = s.rolling(GROWTH_WINDOW).sum().shift(GROWTH_END_LAG)
    frame["recent_growth"] = recent / recent.shift(YEARLY_LAG).replace(0, np.nan)
    frame["deviation"] = (s / frame[f"lag_{YEARLY_LAG}"].replace(0, np.nan)) / frame["recent_growth"]
    return frame


def total_frame(inputs: Inputs, target: str) -> tuple[pd.DataFrame, list[str]]:
    """Daily ES + PT frame with the inputs and the deviation target of C.4 / D.1."""
    frame = pd.DataFrame({"y": _total_series(inputs, target)})
    frame = _add_lag_features(frame)
    camp = _campaigns_vs_last_year(inputs, frame.index)
    frame = frame.join(camp)
    return frame, LAG_COLS + list(camp.columns)


def cv_folds(n_days: int):
    """Positions (train, validation) of up to 3 mini-tests, the last one ending at the cutoff."""
    folds = []
    for k in range(CV_FOLDS):
        valid_end = n_days - k * CV_VALID_DAYS
        valid_start = valid_end - CV_VALID_DAYS
        train_end = valid_start - CV_GAP_DAYS
        if train_end < CV_MIN_TRAIN:
            break
        folds.append((np.arange(0, train_end), np.arange(valid_start, valid_end)))
    return folds[::-1]


def _split(frame: pd.DataFrame, month, features: list[str]):
    month = month_of(month)
    cutoff = cutoff_of(month)
    train = frame[frame.index <= cutoff]
    test = frame[(frame.index >= month_start(month)) & (frame.index <= month_end(month))]
    # Leakage check: every lag of the month refers to a day on or before the cutoff.
    assert (test.index - pd.Timedelta(days=min(RECENT_LAGS))).max() <= cutoff, "information leak: lag after the cutoff"
    if len(test) != month.days_in_month:
        raise ValueError(f"The frame has {len(test)} days of {month}, expected {month.days_in_month}.")
    if test[features + ["recent_growth"]].isna().any().any():
        bad = [c for c in features + ["recent_growth"] if test[c].isna().any()]
        raise ValueError(f"Missing inputs to forecast {month}: {bad}")
    train_t = train.dropna(subset=features + ["deviation"])
    return cutoff, train_t, test


def _to_volume(deviation, part: pd.DataFrame) -> np.ndarray:
    return deviation * part["recent_growth"].to_numpy() * part[f"lag_{YEARLY_LAG}"].to_numpy()


# ---------------------------------------------------------------------------- C.4
def _c4_fit(train_part, apply_part, features, params, n_trees):
    from sklearn.ensemble import RandomForestRegressor

    model = RandomForestRegressor(n_estimators=n_trees, **params, max_features=0.5, random_state=42, n_jobs=-1)
    model.fit(train_part[features], train_part["deviation"])
    return _to_volume(model.predict(apply_part[features]), apply_part), model


def _c4_choose(train_t, features):
    folds = cv_folds(len(train_t))
    if not folds:
        return C4_DEFAULT, np.nan, 0
    scores = []
    for params in C4_GRID:
        wapes = []
        for tr, va in folds:
            fc, _ = _c4_fit(train_t.iloc[tr], train_t.iloc[va], features, params, C4_TREES_CV)
            wapes.append(wape_pct(train_t.iloc[va]["y"], fc))
        scores.append(np.mean(wapes))
    best = int(np.argmin(scores))
    return C4_GRID[best], float(scores[best]), len(folds)


# ---------------------------------------------------------------------------- D.1
def _d1_choose(train_t, features):
    from sklearn.ensemble import GradientBoostingRegressor

    folds = cv_folds(len(train_t))
    if not folds:
        return D1_DEFAULT, np.nan, 0
    best = (None, np.inf)
    for depth in D1_DEPTHS:
        wape_by_trees = []
        for tr, va in folds:
            part_tr, part_va = train_t.iloc[tr], train_t.iloc[va]
            model = GradientBoostingRegressor(n_estimators=D1_MAX_TREES, max_depth=depth, **D1_FIXED)
            model.fit(part_tr[features], part_tr["deviation"])
            actual = part_va["y"].to_numpy()
            wape_by_trees.append([100 * np.abs(_to_volume(dev, part_va) - actual).sum() / actual.sum()
                                  for dev in model.staged_predict(part_va[features])])
        mean_wape = np.mean(wape_by_trees, axis=0)
        n_trees = int(np.argmin(mean_wape)) + 1
        if mean_wape[n_trees - 1] < best[1]:
            best = ({"max_depth": depth, "n_estimators": n_trees}, float(mean_wape[n_trees - 1]))
    return best[0], best[1], len(folds)


def _d1_fit(train_part, apply_part, features, params):
    from sklearn.ensemble import GradientBoostingRegressor

    model = GradientBoostingRegressor(**params, **D1_FIXED)
    model.fit(train_part[features], train_part["deviation"])
    return _to_volume(model.predict(apply_part[features]), apply_part), model


def _run_total_model(key: str, inputs: Inputs, target: str, months) -> ModelOutput:
    with Timer() as t:
        frame, features = total_frame(inputs, target)
        parts, chosen, importance, growth = [], [], [], []
        for m in months:
            cutoff, train_t, test = _split(frame, m, features)
            if key == "ml_c4_rf":
                params, cv_wape, n_folds = _c4_choose(train_t, features)
                fc, model = _c4_fit(train_t, test, features, params, C4_TREES_FINAL)
            else:
                params, cv_wape, n_folds = _d1_choose(train_t, features)
                fc, model = _d1_fit(train_t, test, features, params)
            parts.append(pd.DataFrame({"mark": TOTAL, "day": test.index, "forecast": fc}))
            chosen.append({"month": str(month_of(m)), "cutoff": str(cutoff.date()), "cv_folds": n_folds,
                           **params, "cv_wape_pct": cv_wape})
            importance.append(pd.Series(model.feature_importances_, index=features, name=str(month_of(m))))
            growth.append({"month": str(month_of(m)), "train_from": str(train_t.index.min().date()),
                           "train_days": int(len(train_t)), "recent_growth_mean": float(test["recent_growth"].mean())})
        out = from_daily(key, pd.concat(parts, ignore_index=True))
        out.details["chosen_settings"] = pd.DataFrame(chosen)
        out.details["feature_importance"] = (pd.concat(importance, axis=1).assign(mean=lambda d: d.mean(axis=1))
                                             .sort_values("mean", ascending=False).rename_axis("feature").reset_index())
        out.details["training"] = pd.DataFrame(growth)
    out.seconds = t.seconds
    return out


def run_c4(inputs: Inputs, target: str, months) -> list[ModelOutput]:
    spec(target, "ml_c4_rf")
    return [_run_total_model("ml_c4_rf", inputs, target, months)]


def run_d1(inputs: Inputs, target: str, months) -> list[ModelOutput]:
    spec(target, "ml_d1_gb")
    return [_run_total_model("ml_d1_gb", inputs, target, months)]


# ---------------------------------------------------------------------------- F.1
def _group_col(group: str) -> str:
    return "grp_" + "".join(ch if ch.isalnum() else "_" for ch in group.lower()).replace("__", "_")


GROUP_COLS = {g: _group_col(g) for g in CATEGORY_GROUPS}


def group_frame(inputs: Inputs, target: str) -> tuple[pd.DataFrame, list[str]]:
    """One row per day and category group (long table of the F.1 notebook)."""
    wide = (inputs.groups.pivot_table(index="day", columns="group", values=target, aggfunc="sum", fill_value=0)
            .reindex(columns=list(CATEGORY_GROUPS), fill_value=0))
    full_index = pd.date_range(inputs.daily["day"].min(), inputs.daily["day"].max(), freq="D")
    wide = wide.reindex(full_index)
    known = full_index <= inputs.data_end
    wide.loc[known] = wide.loc[known].fillna(0)          # days of the data without sales = 0; future days stay null
    camp = _campaigns_vs_last_year(inputs, full_index)
    parts = []
    for g in CATEGORY_GROUPS:
        s = wide[g]
        part = pd.DataFrame({"day": full_index, "group": g, "value": s.to_numpy()})
        for lag in RECENT_LAGS + [YEARLY_LAG]:
            part[f"lag_{lag}"] = s.shift(lag).to_numpy()
        recent = s.rolling(GROWTH_WINDOW).sum().shift(GROWTH_END_LAG)
        recent_last_year = recent.shift(YEARLY_LAG)
        part["recent_growth"] = (recent / recent_last_year.replace(0, np.nan)).to_numpy()
        part["weight"] = (recent_last_year / GROWTH_WINDOW).to_numpy()
        part["deviation"] = part["value"] / part[f"lag_{YEARLY_LAG}"].replace(0, np.nan) / part["recent_growth"]
        parts.append(part)
    long = pd.concat(parts, ignore_index=True).merge(camp, left_on="day", right_index=True, how="left")
    for g, col in GROUP_COLS.items():
        long[col] = (long["group"] == g).astype(int)
    long["growth_for_forecast"] = long["recent_growth"].fillna(1.0)
    return long, LAG_COLS + list(camp.columns) + list(GROUP_COLS.values())


def _f1_to_volume(deviation, part):
    return np.clip(deviation * part["growth_for_forecast"].to_numpy() * part[f"lag_{YEARLY_LAG}"].to_numpy(), 0, None)


def _f1_wape_total(part, forecast):
    daily = part.assign(fc=forecast).groupby("day")[["value", "fc"]].sum()
    return 100 * np.abs(daily["fc"] - daily["value"]).sum() / daily["value"].sum()


def _f1_cv_folds(days):
    n_days, folds = len(days), []
    for k in range(CV_FOLDS):
        valid_end = n_days - k * CV_VALID_DAYS
        valid_start = valid_end - CV_VALID_DAYS
        train_end = valid_start - CV_GAP_DAYS
        if train_end < CV_MIN_TRAIN:
            break
        folds.append((days[:train_end], days[valid_start:valid_end]))
    return folds[::-1]


def _f1_choose(train_t, known, features):
    from xgboost import XGBRegressor

    folds = _f1_cv_folds(np.sort(train_t["day"].unique()))
    if not folds:
        return F1_DEFAULT, np.nan, 0
    tree_counts = list(range(F1_TREE_STEP, F1_MAX_TREES + 1, F1_TREE_STEP))
    best = (None, np.inf)
    for params in F1_GRID:
        wape_by_trees = []
        for tr_days, va_days in folds:
            part_tr = train_t[train_t["day"].isin(tr_days)]
            part_va = known[known["day"].isin(va_days)]
            model = XGBRegressor(n_estimators=F1_MAX_TREES, **params, **F1_FIXED)
            model.fit(part_tr[features], part_tr["deviation"], sample_weight=part_tr["weight"])
            wape_by_trees.append([_f1_wape_total(part_va, _f1_to_volume(
                model.predict(part_va[features], iteration_range=(0, n)), part_va)) for n in tree_counts])
        mean_wape = np.mean(wape_by_trees, axis=0)
        i = int(np.argmin(mean_wape))
        if mean_wape[i] < best[1]:
            best = ({**params, "n_estimators": tree_counts[i]}, float(mean_wape[i]))
    return best[0], best[1], len(folds)


def run_f1(inputs: Inputs, target: str, months) -> list[ModelOutput]:
    from xgboost import XGBRegressor

    spec(target, "ml_f1_xgb")
    if target not in ("units", "sales"):
        raise ValueError("F.1 forecasts units and sales only: an order cannot be split by category group.")
    with Timer() as t:
        long, features = group_frame(inputs, target)
        parts, groups_out, chosen, importance = [], [], [], []
        for m in months:
            month = month_of(m)
            cutoff = cutoff_of(month)
            assert (month_end(month) - pd.Timedelta(days=min(RECENT_LAGS))) <= cutoff, "information leak"
            period = long[long["day"] <= cutoff]
            known = period.dropna(subset=LAG_COLS)
            train_t = known[np.isfinite(known["deviation"]) & (known["weight"] > 0)]
            known = known[known["day"] >= train_t["day"].min()]
            test = long[(long["day"] >= month_start(month)) & (long["day"] <= month_end(month))].dropna(subset=LAG_COLS)
            if test["day"].nunique() != month.days_in_month:
                raise ValueError(f"F.1 has inputs for {test['day'].nunique()} days of {month}, expected {month.days_in_month}.")
            params, cv_wape, n_folds = _f1_choose(train_t, known, features)
            model = XGBRegressor(**params, **F1_FIXED)
            model.fit(train_t[features], train_t["deviation"], sample_weight=train_t["weight"])
            fc = _f1_to_volume(model.predict(test[features]), test)
            by_group = test[["day", "group", "value"]].rename(columns={"value": "actual"}).assign(forecast=fc, month=str(month))
            groups_out.append(by_group)
            daily = by_group.groupby("day", as_index=False)["forecast"].sum()
            parts.append(daily.assign(mark=TOTAL))
            chosen.append({"month": str(month), "cutoff": str(cutoff.date()), "cv_folds": n_folds, **params,
                           "cv_wape_pct": cv_wape, "train_from": str(train_t["day"].min().date()),
                           "train_days": int(train_t["day"].nunique())})
            importance.append(pd.Series(model.feature_importances_, index=features, name=str(month)))
        out = from_daily("ml_f1_xgb", pd.concat(parts, ignore_index=True)[["mark", "day", "forecast"]])
        out.details["chosen_settings"] = pd.DataFrame(chosen)
        out.details["group_forecasts"] = pd.concat(groups_out, ignore_index=True)
        out.details["feature_importance"] = (pd.concat(importance, axis=1).assign(mean=lambda d: d.mean(axis=1))
                                             .sort_values("mean", ascending=False).rename_axis("feature").reset_index())
    out.seconds = t.seconds
    return [out]
