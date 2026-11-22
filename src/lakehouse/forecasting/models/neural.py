"""Neural networks of ``ml_models/Deep Learning`` (orders, gross sales and units).

Same training method in the three notebooks: one network per market, Adam
(lr 1e-3, weight decay 1e-4), batches of 32 days, 5 seeds (0 to 4) whose
forecasts are averaged. The network reads one day at a time (MLP) or a window
of 28 days (LSTM) and forecasts the day; days are summed to the month.

Rules (premises P4-P11 of the READMEs):
* training rows: every day of the months up to M-2 (from 2024-01-01);
* inputs and target standardised with the training rows of the step only
  (a constant column gets standard deviation 1);
* the level (28-day average at the cutoff) and the paid media of the target
  month are clipped to the minimum and maximum seen in training;
* rows without a level (Jan and Feb 2024) stay in training, filled with the
  training mean plus a 0/1 "no history" column;
* forecasts below zero become zero.

Scenario C uses the paid media of the month being forecast (``spend_day``,
``spend_month``): actual spend in the backtest, the planned budget in the
forecast of a future month.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..catalog import DL_FEATURE_GROUPS, job_specs
from ..data import DL_CAMPAIGN_COLUMNS, Inputs, clearance_days, dl_campaign_days, national_holidays
from ..protocol import MARKETS, cutoff_of, month_of
from . import ModelOutput, Timer, from_daily_seeds, from_monthly_seeds
from .baseline import b3_monthly_by_market

SEEDS = [0, 1, 2, 3, 4]
FIXED = {"lr": 1e-3, "weight_decay": 1e-4, "batch": 32, "huber_delta": 1.0}
NO_HISTORY_COLS = ["level_28d"]
CLIP_COLS = ["level_28d", "spend_day", "spend_month"]
LEVEL_WINDOW = 28


def feature_table(inputs: Inputs, target: str) -> pd.DataFrame:
    """One row per market and day with every candidate input (the ``feat`` table of the notebooks)."""
    d = inputs.daily[["mark", "day", target, "spend"]].rename(columns={target: "y", "spend": "spend_day"}).copy()
    d = d.sort_values(["mark", "day"]).reset_index(drop=True)
    d["month"] = d["day"].dt.to_period("M")
    d["cutoff"] = (d["month"] - 2).dt.end_time.dt.normalize()

    # Recent level measured at the cutoff of the row (28-day moving average, complete window only).
    r28 = d.groupby("mark")["y"].transform(lambda s: s.rolling(LEVEL_WINDOW, min_periods=LEVEL_WINDOW).mean())
    look = pd.Series(r28.to_numpy(), index=pd.MultiIndex.from_arrays([d["mark"], d["day"]]))
    d["level_28d"] = look.reindex(pd.MultiIndex.from_arrays([d["mark"], d["cutoff"]])).to_numpy()

    # Spend of the whole month, only when every day of the month has spend.
    grp = d.groupby(["mark", "month"])["spend_day"]
    d["spend_month"] = grp.transform("sum").where(grp.transform("count") == grp.transform("size"))

    dow = d["day"].dt.dayofweek
    for k in range(7):
        d[f"dow_{k}"] = (dow == k).astype(int)
    mo = d["day"].dt.month
    d["month_sin"] = np.sin(2 * np.pi * mo / 12)
    d["month_cos"] = np.cos(2 * np.pi * mo / 12)
    d["eom"] = (d["day"].dt.day >= 26).astype(int)
    hol = national_holidays(range(d["day"].min().year, d["day"].max().year + 1))
    d["hol_wd"] = [int((day in hol[mk]) and day.dayofweek < 5) for mk, day in zip(d["mark"], d["day"])]

    start, end = d["day"].min(), d["day"].max()
    camp = dl_campaign_days(inputs.events, start, end).set_index(["mark", "day"])["family"]
    fam = pd.Series(camp.reindex(pd.MultiIndex.from_arrays([d["mark"], d["day"]])).to_numpy(), index=d.index)
    for family, col in DL_CAMPAIGN_COLUMNS.items():
        d[col] = (fam == family).astype(int)
    saldos = clearance_days(inputs.events, start, end)
    d["flag_saldos"] = [int((mk, day) in saldos) for mk, day in zip(d["mark"], d["day"])]
    return d


def columns_of(groups: list[str]) -> list[str]:
    return [c for g in groups for c in DL_FEATURE_GROUPS[g]]


def _design(df: pd.DataFrame, cols: list[str], fill: dict) -> np.ndarray:
    X = df[cols].copy()
    flags = []
    for c in cols:
        if c in NO_HISTORY_COLS:
            flags.append(X[c].isna().to_numpy(float))
            X[c] = X[c].fillna(fill[c])
    X = X.to_numpy(float)
    return np.column_stack([X] + flags) if flags else X


def _check_finite(X: np.ndarray, cols: list[str], what: str) -> None:
    if not np.isfinite(X).all():
        bad = sorted({cols[j] for j in np.where(~np.isfinite(X).all(axis=0))[0] if j < len(cols)})
        hint = " (the paid media budget of the month is needed by the scenario C models)" if {"spend_day", "spend_month"} & set(bad) else ""
        raise ValueError(f"Missing values in the inputs of {what}: {bad}{hint}")


# ---------------------------------------------------------------------------- torch
def _torch():
    import torch

    torch.set_num_threads(1)   # tiny networks: one thread is faster and always gives the same result
    return torch


def _loss(torch, name: str):
    nn = torch.nn
    return {"l1": nn.L1Loss, "mse": nn.MSELoss, "huber": lambda: nn.HuberLoss(delta=FIXED["huber_delta"])}[name]()


def _make_mlp(torch, d: int, layers):
    nn = torch.nn
    mods, prev = [], d
    for h in layers:
        mods += [nn.Linear(prev, h), nn.ReLU()]
        prev = h
    return nn.Sequential(*mods, nn.Linear(prev, 1))


def _train(torch, net, X, y, loss, epochs, seed):
    gen = torch.Generator().manual_seed(seed)
    opt = torch.optim.Adam(net.parameters(), lr=FIXED["lr"], weight_decay=FIXED["weight_decay"])
    lossf = _loss(torch, loss)
    Xt = torch.from_numpy(np.ascontiguousarray(X, dtype=np.float32))
    yt = torch.from_numpy(np.ascontiguousarray(y, dtype=np.float32)).reshape(-1, 1)
    n, bs = len(Xt), FIXED["batch"]
    for _ in range(epochs):
        perm = torch.randperm(n, generator=gen)
        for i in range(0, n, bs):
            idx = perm[i:i + bs]
            opt.zero_grad()
            lossf(net(Xt[idx]), yt[idx]).backward()
            opt.step()
    return net


def _predict(torch, net, X) -> np.ndarray:
    net.eval()
    with torch.no_grad():
        return net(torch.from_numpy(np.ascontiguousarray(X, dtype=np.float32))).numpy().ravel()


def fit_mlp(X, y, loss: str, layers, epochs: int, seed: int):
    torch = _torch()
    torch.manual_seed(seed)          # the seed fixes the initial weights and the order of the rows in each epoch
    net = _make_mlp(torch, X.shape[1], layers)
    return _train(torch, net, X, y, loss, epochs, seed)


def fit_lstm(Xw, y, loss: str, hidden: int, epochs: int, seed: int):
    torch = _torch()
    nn = torch.nn

    class LSTMNet(nn.Module):
        def __init__(self, d, h):
            super().__init__()
            self.lstm = nn.LSTM(d, h, num_layers=1, batch_first=True)
            self.out = nn.Linear(h, 1)

        def forward(self, x):
            o, _ = self.lstm(x)
            return self.out(o[:, -1, :])

    torch.manual_seed(seed)
    net = LSTMNet(Xw.shape[2], hidden)
    return _train(torch, net, Xw, y, loss, epochs, seed)


# ---------------------------------------------------------------------------- forecasts
def mlp_forecast(feat: pd.DataFrame, month, groups: list[str], loss: str, layers, epochs: int,
                 seeds=SEEDS) -> tuple[pd.DataFrame, list[dict]]:
    """Daily forecast of every day of ``month``: mark, day, seed, forecast."""
    torch = _torch()
    month = month_of(month)
    cutoff = cutoff_of(month)
    tr_all = feat[feat["month"] <= month - 2]
    assert tr_all["day"].max() <= cutoff, "information leak: training rows after the cutoff"
    tg_all = feat[feat["month"] == month]
    cols = columns_of(groups)
    out, info = [], []
    for mk in MARKETS:
        tr = tr_all[tr_all["mark"] == mk]
        tg = tg_all[tg_all["mark"] == mk].copy()
        if len(tg) != month.days_in_month:
            raise ValueError(f"The feature table has {len(tg)} days of {month} for {mk}.")
        for c in [c for c in cols if c in CLIP_COLS]:
            tg[c] = tg[c].clip(tr[c].min(), tr[c].max())
        fill = {c: float(tr[c].mean()) for c in cols if c in NO_HISTORY_COLS}
        Xtr, Xtg = _design(tr, cols, fill), _design(tg, cols, fill)
        _check_finite(Xtr, cols, f"training ({mk})")
        _check_finite(Xtg, cols, f"{month} ({mk})")
        mu, sd = Xtr.mean(0), Xtr.std(0)
        sd[sd == 0] = 1.0
        ytr = tr["y"].to_numpy(float)
        ymu, ysd = ytr.mean(), ytr.std()
        for s in seeds:
            net = fit_mlp((Xtr - mu) / sd, (ytr - ymu) / ysd, loss, layers, epochs, s)
            pred = _predict(torch, net, (Xtg - mu) / sd) * ysd + ymu
            out.append(pd.DataFrame({"mark": mk, "day": tg["day"].to_numpy(), "seed": s, "forecast": np.maximum(pred, 0.0)}))
        info.append({"month": str(month), "mark": mk, "cutoff": str(cutoff.date()), "train_rows": int(len(tr)),
                     "first_train_day": str(tr["day"].min().date()), "inputs": len(cols) + len(fill)})
    return pd.concat(out, ignore_index=True), info


def lstm_forecast(feat: pd.DataFrame, month, groups: list[str], loss: str, hidden: int, window: int, epochs: int,
                  seeds=SEEDS) -> tuple[pd.DataFrame, list[dict]]:
    from numpy.lib.stride_tricks import sliding_window_view

    torch = _torch()
    month = month_of(month)
    cutoff = cutoff_of(month)
    cols = columns_of(groups)
    assert not set(cols) & set(NO_HISTORY_COLS), "the LSTM only reads inputs known in advance"
    out, info = [], []
    for mk in MARKETS:
        mdf = feat[(feat["mark"] == mk) & (feat["month"] <= month)].sort_values("day").reset_index(drop=True).copy()
        assert (mdf["day"].diff().dropna() == pd.Timedelta(days=1)).all(), "the daily series must be continuous"
        trm = (mdf["month"] <= month - 2).to_numpy()
        tgm = (mdf["month"] == month).to_numpy()
        tr = mdf[trm]
        assert tr["day"].max() <= cutoff, "information leak: training days after the cutoff"
        for c in [c for c in cols if c in CLIP_COLS]:
            mdf[c] = mdf[c].clip(tr[c].min(), tr[c].max())
        X = mdf[cols].to_numpy(float)
        _check_finite(X[trm], cols, f"training ({mk})")
        # The windows of the target days also read the days before the month (M-1); mdf ends with month M.
        first_needed = max(int(np.argmax(tgm)) - (window - 1), 0)
        _check_finite(X[first_needed:], cols, f"the {window}-day windows of {month} ({mk})")
        mu, sd = X[trm].mean(0), X[trm].std(0)
        sd[sd == 0] = 1.0
        Xn = (X - mu) / sd
        Xp = np.vstack([np.zeros((window - 1, Xn.shape[1])), Xn])     # before the first day: the training mean (0)
        Xw = sliding_window_view(Xp, (window, Xn.shape[1]))[:, 0]      # (days, window, inputs); window of day i ends on day i
        ytr = mdf.loc[trm, "y"].to_numpy(float)
        ymu, ysd = ytr.mean(), ytr.std()
        for s in seeds:
            net = fit_lstm(Xw[trm], (ytr - ymu) / ysd, loss, hidden, epochs, s)
            pred = _predict(torch, net, Xw[tgm]) * ysd + ymu
            out.append(pd.DataFrame({"mark": mk, "day": mdf.loc[tgm, "day"].to_numpy(), "seed": s,
                                     "forecast": np.maximum(pred, 0.0)}))
        info.append({"month": str(month), "mark": mk, "cutoff": str(cutoff.date()), "train_rows": int(trm.sum()),
                     "first_train_day": str(tr["day"].min().date()), "inputs": len(cols), "window": window})
    return pd.concat(out, ignore_index=True), info


# ---------------------------------------------------------------------------- jobs
def _cfg(spec) -> dict:
    c = spec.config
    return {"groups": c["groups"].split(","), "loss": c["loss"],
            "layers": [int(x) for x in str(c.get("layers", "")).split("x") if x],
            "epochs": int(c["epochs"]), "hidden": int(c.get("hidden", 32)), "window": int(c.get("window", 28))}


def run(inputs: Inputs, target: str, months, job: str) -> list[ModelOutput]:
    specs = {s.key: s for s in job_specs(target, job)}
    main = specs[job]
    cfg = _cfg(main)
    with Timer() as t:
        feat = feature_table(inputs, target)
        parts, info = [], []
        for m in months:
            if main.algorithm.startswith("LSTM"):
                fc, i = lstm_forecast(feat, m, cfg["groups"], cfg["loss"], cfg["hidden"], cfg["window"], cfg["epochs"])
            else:
                fc, i = mlp_forecast(feat, m, cfg["groups"], cfg["loss"], cfg["layers"], cfg["epochs"])
            parts.append(fc)
            info += i
        daily_seeds = pd.concat(parts, ignore_index=True)
        out = from_daily_seeds(main.key, daily_seeds)
        out.details["training"] = pd.DataFrame(info)
        out.params = {"input_columns": ",".join(columns_of(cfg["groups"])), "seeds_used": ",".join(map(str, SEEDS))}
    out.seconds = t.seconds
    outputs = [out]

    if "dl_blend_a" in specs:
        with Timer() as tb:
            b3 = b3_monthly_by_market(inputs, target, months, 3)
            seeds = (daily_seeds.assign(month=daily_seeds["day"].dt.to_period("M"))
                     .groupby(["mark", "month", "seed"], as_index=False)["forecast"].sum())
            w = float(specs["dl_blend_a"].config["weight_b3"])
            seeds["forecast"] = [w * b3[(mk, m)] + (1 - w) * f for mk, m, f in zip(seeds["mark"], seeds["month"], seeds["forecast"])]
            seeds["month"] = seeds["month"].dt.to_timestamp()
            blend = from_monthly_seeds("dl_blend_a", seeds)
            blend.params = {"mlp": main.key, "mlp_config": f"{cfg['loss']}_{'x'.join(map(str, cfg['layers']))}_e{cfg['epochs']}"}
        blend.seconds = t.seconds + tb.seconds
        outputs.append(blend)
    return outputs
