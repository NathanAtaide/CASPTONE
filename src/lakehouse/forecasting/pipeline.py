"""Steps of the forecasting DAGs (called through ``python -m lakehouse.forecasting``).

Selection (DAG ``ml_forecast_selection``)::

    prepare  -> run_job (one per target x job) -> select

* ``prepare`` builds the inputs from silver once, decides the backtest window,
  skips the run when the data did not change since the last selection, saves
  the inputs to the lake and opens one MLflow parent run per target;
* ``run_job`` runs one job (1-2 models) for one target on the backtest window,
  logs every model as a child run and saves its forecasts to the lake;
* ``select`` builds the leaderboard, picks and registers the champion of each
  target and writes the gold tables for Power BI.

Next month (DAG ``ml_forecast_next_month``)::

    forecast (one per target) -> publish_forecast

Working files live in ``s3://<gold>/_forecast/`` (not exported to Power BI):
``selection/<batch>/`` (manifest, inputs, results), ``next_month/<batch>/``
and ``_state.json`` (fingerprint of the data of the last selection).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from ..config import LakeConfig
from ..duck import connect, sql_literal
from ..naming import safe_name
from ..objects import read_json, write_json
from . import catalog, outputs, tracking
from .data import Inputs, budget_by_market
from .metrics import SELECTION_RULE, actuals, evaluate, leaderboard
from .protocol import (DEFAULT_BACKTEST_MONTHS, TARGET_LABELS, TARGETS, TOTAL, backtest_window, cutoff_of,
                       month_end, month_of, month_start, next_month)

log = logging.getLogger(__name__)

AREA = "_forecast"


def area(*parts: str) -> str:
    return LakeConfig.from_env().uri("gold", AREA, *parts)


def state_uri() -> str:
    return area("_state.json")


def _con():
    con = connect()
    con.execute("SET enable_progress_bar = false")
    return con


def _write_frame(con, frame: pd.DataFrame, uri: str) -> None:
    con.register("fc_frame", frame)
    try:
        con.execute(f"COPY fc_frame TO {sql_literal(uri)} (FORMAT parquet, COMPRESSION zstd)")
    finally:
        con.unregister("fc_frame")


def _read_frame(con, uri: str, dates=("day", "month")) -> pd.DataFrame:
    frame = con.execute(f"SELECT * FROM read_parquet({sql_literal(uri)})").df()
    for c in dates:
        if c in frame.columns and len(frame):
            frame[c] = pd.to_datetime(frame[c])
    return frame


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)


def run_models(inputs: Inputs, target: str, job: str, months):
    from .models import baseline, neural, trees

    if job == "baseline":
        return baseline.run(inputs, target, months)
    if job == "ml_c4_rf":
        return trees.run_c4(inputs, target, months)
    if job == "ml_d1_gb":
        return trees.run_d1(inputs, target, months)
    if job == "ml_f1_xgb":
        return trees.run_f1(inputs, target, months)
    if job.startswith("dl_"):
        return neural.run(inputs, target, months, job)
    raise ValueError(f"Unknown job '{job}'.")


# ============================================================================ selection
def prepare(run_id: str, backtest_months: int = DEFAULT_BACKTEST_MONTHS, targets=None, force: bool = False) -> dict:
    batch = safe_name(run_id)
    targets = [t for t in (targets or TARGETS) if t]
    unknown = sorted(set(targets) - set(TARGETS))
    if unknown:
        raise ValueError(f"Unknown targets {unknown}. Expected a subset of {list(TARGETS)}.")
    con = _con()
    try:
        inputs = Inputs.from_silver(con)
        months = backtest_window(inputs.data_end, int(backtest_months))
        fingerprint = inputs.fingerprint(catalog.CATALOG_VERSION, [str(m) for m in months], sorted(targets))
        plan = [{"target": t, "job": j} for t in targets for j in catalog.jobs(t)]
        plan.sort(key=lambda p: -catalog.JOB_WEIGHT.get(p["job"], 1))
        base = {"batch_id": batch, "data_end": str(inputs.data_end.date()), "months": [str(m) for m in months],
                "targets": targets, "fingerprint": fingerprint}

        state = read_json(state_uri())
        if not force and state and state.get("fingerprint") == fingerprint:
            log.info("The silver data did not change since selection %s - nothing to do.", state.get("batch_id"))
            return {**base, "skip": True, "jobs": [], "previous_batch_id": state.get("batch_id"),
                    "reason": "same data, catalogue and window as the last successful selection"}

        manifest_uri = area("selection", batch, "manifest.json")
        manifest = read_json(manifest_uri)
        if manifest and manifest.get("fingerprint") == fingerprint:
            log.info("Reusing the manifest of batch %s (task retried).", batch)
        else:
            inputs.save(con, area("selection", batch, "inputs"))
            tracking.silence_mlflow()
            c = tracking.client()
            tags = {"data_end": base["data_end"], "backtest_window": f"{months[0]}..{months[-1]}",
                    "data_fingerprint": fingerprint}
            parents = {t: tracking.create_parent(c, t, batch, tags) for t in targets}
            manifest = {**base, "parents": parents, "warnings": inputs.warnings, "summary": inputs.summary(),
                        "created_at": str(_utc_now())}
            write_json(manifest_uri, manifest)
        return {**base, "skip": False, "jobs": plan, "warnings": inputs.warnings}
    finally:
        con.close()


def _manifest(batch: str) -> dict:
    manifest = read_json(area("selection", batch, "manifest.json"))
    if not manifest:
        raise RuntimeError(f"Batch {batch} has no manifest: run the 'prepare' step first.")
    return manifest


def run_job(run_id: str, target: str, job: str) -> dict:
    batch = safe_name(run_id)
    manifest = _manifest(batch)
    months = [month_of(m) for m in manifest["months"]]
    tracking.silence_mlflow()
    con = _con()
    try:
        inputs = Inputs.load(con, area("selection", batch, "inputs"), manifest)
        results = run_models(inputs, target, job, months)
        daily_actual, monthly_actual = actuals(inputs.daily, target, months)
        c = tracking.client()
        data_tags = {"data_end": manifest["data_end"], "backtest_window": f"{months[0]}..{months[-1]}",
                     "data_fingerprint": manifest["fingerprint"]}
        summary = []
        for out in results:
            spec = catalog.spec(target, out.model_key)
            metrics, mon, day = evaluate(out, daily_actual, monthly_actual, months)
            rid = tracking.log_candidate(c, target=target, batch_id=batch, parent_run_id=manifest["parents"][target],
                                         spec=spec, output=out, metrics=metrics, monthly=mon, daily=day, months=months,
                                         data_tags=data_tags)
            folder = area("selection", batch, "results", target, out.model_key)
            _write_frame(con, mon, f"{folder}/monthly.parquet")
            _write_frame(con, day if not day.empty else pd.DataFrame(columns=["mark", "day", "forecast"]), f"{folder}/daily.parquet")
            write_json(f"{folder}/summary.json", {"target": target, "job": job, "model_key": out.model_key,
                                                  "metrics": {k: (None if v is None or (isinstance(v, float) and np.isnan(v)) else v)
                                                              for k, v in metrics.items()},
                                                  "mlflow_run_id": rid, "seconds": out.seconds, "warnings": out.warnings})
            log.info("%s / %s: monthly WAPE %.2f%%, bias %+.2f%% (%.0f s)", target, out.model_key,
                     100 * metrics["wape_monthly"], 100 * metrics["bias_monthly"], out.seconds)
            summary.append({"model_key": out.model_key, "wape_monthly": round(metrics["wape_monthly"], 6),
                            "bias_monthly": round(metrics["bias_monthly"], 6), "mlflow_run_id": rid})
        return {"batch_id": batch, "target": target, "job": job, "models": summary}
    finally:
        con.close()


def _spec_columns(target: str, spec: catalog.ModelSpec) -> dict:
    return {"target": target, "target_label": TARGET_LABELS[target], "model_key": spec.key, "model_name": spec.name,
            "family": spec.family, "algorithm": spec.algorithm, "scenario": spec.scenario,
            "uses_paid_media": spec.uses_paid_media, "is_baseline_reference": spec.is_baseline_reference}


def _merge_gold(con, name: str, new: pd.DataFrame, keys: list[str]) -> int:
    """Replace in the gold table the rows that have the same ``keys`` as ``new``; keep the others."""
    old = outputs.read(con, name)
    if old is not None and len(old) and len(new):
        k_new = set(map(tuple, new[keys].astype(str).to_numpy()))
        keep = ~pd.Series([tuple(r) in k_new for r in old[keys].astype(str).to_numpy()], index=old.index)
        new = pd.concat([old[keep], new], ignore_index=True)
    return outputs.write(con, name, new)


def select(run_id: str) -> dict:
    batch = safe_name(run_id)
    manifest = _manifest(batch)
    months = [month_of(m) for m in manifest["months"]]
    selected_at = _utc_now()
    tracking.silence_mlflow()
    c = tracking.client()
    con = _con()
    gold = {outputs.BACKTEST_MONTHLY: [], outputs.BACKTEST_DAILY: [], outputs.MODEL_METRICS: [], outputs.CHAMPION: []}
    champions = {}
    try:
        for target in manifest["targets"]:
            rows, monthly_all, daily_all = [], [], []
            for spec in catalog.specs(target):
                folder = area("selection", batch, "results", target, spec.key)
                summary = read_json(f"{folder}/summary.json")
                if not summary:
                    raise RuntimeError(f"{target} / {spec.key} has no result in batch {batch}: its job did not finish.")
                mon = _read_frame(con, f"{folder}/monthly.parquet")
                day = _read_frame(con, f"{folder}/daily.parquet")
                m = {k: (np.nan if v is None else v) for k, v in summary["metrics"].items()}
                rows.append({**_spec_columns(target, spec), "level": spec.level, **m,
                             "mlflow_run_id": summary["mlflow_run_id"], "training_seconds": summary["seconds"]})
                monthly_all.append(mon.assign(model_key=spec.key))
                if len(day):
                    daily_all.append(day.assign(model_key=spec.key))
            lb = leaderboard(rows)
            champ = lb[lb["is_champion"]].iloc[0]
            champ_spec = catalog.spec(target, champ["model_key"])
            monthly = pd.concat(monthly_all, ignore_index=True)
            version = tracking.register_champion(target, run_id=champ["mlflow_run_id"], spec=champ_spec, batch_id=batch,
                                                 metrics=champ.to_dict(), data_end=manifest["data_end"], months=months)
            tracking.finish_selection(c, target=target, parent_run_id=manifest["parents"][target], lb=lb,
                                      monthly=monthly.merge(lb[["model_key", "model_name"]], on="model_key"),
                                      selection_rule=SELECTION_RULE, champion_version=version)

            flags = lb.set_index("model_key")[["is_champion"]]
            common = {"selection_batch_id": batch, "data_end": pd.Timestamp(manifest["data_end"])}
            for frame, kind in ((monthly, "monthly"), (pd.concat(daily_all, ignore_index=True) if daily_all else None, "daily")):
                if frame is None:
                    continue
                specs = {s.key: _spec_columns(target, s) for s in catalog.specs(target)}
                meta = pd.DataFrame([{"model_key": k, **v} for k, v in specs.items()])
                f = frame.merge(meta, on="model_key").join(flags, on="model_key")
                f = f.rename(columns={"mark": "market", "seed_min": "forecast_seed_min", "seed_max": "forecast_seed_max"})
                f["is_total"] = f["market"] == TOTAL
                if kind == "monthly":
                    f["month_start"] = f["month"]
                    f["month_date_key"] = outputs.date_key(f["month"]).to_numpy()
                    f["month_label"] = f["month"].dt.strftime("%Y-%m")
                    gold[outputs.BACKTEST_MONTHLY].append(f.assign(**common))
                else:
                    f["date"] = f["day"]
                    f["date_key"] = outputs.date_key(f["day"]).to_numpy()
                    f["month_start"] = f["day"].dt.to_period("M").dt.to_timestamp()
                    gold[outputs.BACKTEST_DAILY].append(f.assign(selection_batch_id=batch))

            gold[outputs.MODEL_METRICS].append(lb.assign(
                selection_batch_id=batch, selected_at=selected_at, backtest_start=months[0].start_time,
                backtest_end=month_end(months[-1]), backtest_months=len(months), data_end=pd.Timestamp(manifest["data_end"]),
                mlflow_experiment=tracking.experiment_name(target)))
            others = lb[~lb["is_champion"]].sort_values("rank_wape")
            ml_dl = lb[lb["family"] != catalog.BASELINE].sort_values(["wape_monthly"])
            gold[outputs.CHAMPION].append(pd.DataFrame([{
                **{k: champ[k] for k in ("target", "target_label", "model_key", "model_name", "family", "algorithm",
                                         "scenario", "uses_paid_media", "wape_monthly", "bias_monthly", "rmse_monthly",
                                         "wape_daily", "baseline_wape_monthly", "wape_vs_baseline_pp", "beats_baseline",
                                         "mlflow_run_id")},
                "selection_batch_id": batch, "selected_at": selected_at,
                "runner_up_model_key": others.iloc[0]["model_key"] if len(others) else None,
                "runner_up_wape_monthly": others.iloc[0]["wape_monthly"] if len(others) else None,
                "best_ml_dl_model_key": ml_dl.iloc[0]["model_key"] if len(ml_dl) else None,
                "best_ml_dl_model_name": ml_dl.iloc[0]["model_name"] if len(ml_dl) else None,
                "best_ml_dl_wape_monthly": ml_dl.iloc[0]["wape_monthly"] if len(ml_dl) else None,
                "registered_model_name": tracking.registered_name(target), "registered_model_version": int(version),
                "backtest_start": months[0].start_time, "backtest_end": month_end(months[-1]),
                "data_end": pd.Timestamp(manifest["data_end"]), "selection_rule": SELECTION_RULE}]))
            champions[target] = {"model_key": champ["model_key"], "model_name": champ["model_name"],
                                 "wape_monthly": round(float(champ["wape_monthly"]), 6),
                                 "bias_monthly": round(float(champ["bias_monthly"]), 6),
                                 "baseline_wape_monthly": round(float(champ["baseline_wape_monthly"]), 6),
                                 "registered_version": version,
                                 "best_ml_dl": ml_dl.iloc[0]["model_key"] if len(ml_dl) else None}
            log.info("%s: champion %s (monthly WAPE %.2f%%; Baseline 3 %.2f%%) -> %s v%s", target, champ["model_name"],
                     100 * champ["wape_monthly"], 100 * champ["baseline_wape_monthly"], tracking.registered_name(target), version)

        written = {}
        for name, frames in gold.items():
            if frames:
                written[name] = _merge_gold(con, name, pd.concat(frames, ignore_index=True), ["target"])
        write_json(state_uri(), {"fingerprint": manifest["fingerprint"], "batch_id": batch,
                                 "finished_at": str(_utc_now()), "champions": champions})
        return {"batch_id": batch, "champions": champions, "gold_rows": written}
    finally:
        con.close()


# ============================================================================ next month
def forecast_month(target: str, model_key: str, *, month=None, budget_total: float | None = None,
                   budget_es: float | None = None, budget_pt: float | None = None) -> tuple[pd.DataFrame, dict]:
    """Retrain ``model_key`` with the data up to the cutoff of ``month`` (M-2) and forecast the month.

    ``month`` defaults to the month after the last day of data. Returns a long table (monthly and daily rows
    per market and ES + PT) and the assumptions used.
    """
    spec = catalog.spec(target, model_key)
    con = _con()
    try:
        inputs = Inputs.from_silver(con)
    finally:
        con.close()
    month = month_of(month) if month else next_month(inputs.data_end)
    cutoff = cutoff_of(month)
    if month_start(month) <= inputs.data_end:
        raise ValueError(f"{month} already has data in silver (data until {inputs.data_end.date()}): the forecast is for a "
                         f"future month. Use the selection DAG to evaluate past months.")
    if inputs.data_end < cutoff:
        raise ValueError(f"Forecasting {month} needs the data up to {cutoff.date()} (M-2), but silver ends on {inputs.data_end.date()}.")
    notes: list[str] = []
    budget = None
    if spec.uses_paid_media:
        budget, notes = budget_by_market(month, inputs, total=budget_total, es=budget_es, pt=budget_pt)
        if budget is None:
            raise ValueError(f"The champion of {target} ({spec.name}) uses the paid media of the forecast month (scenario C): "
                             f"trigger the DAG with paid_media_budget_eur (ES + PT) for {month}.")
    elif budget_total is not None or budget_es is not None or budget_pt is not None:
        notes.append(f"The champion ({spec.name}) does not use paid media: the budget given was not needed.")
    ext = inputs.extend_to(month, budget=budget)
    notes += ext.assumptions + ext.warnings
    result = next(o for o in run_models(ext, target, spec.job, [month]) if o.model_key == model_key)

    spend = ext.daily[(ext.daily["day"] >= month_start(month)) & (ext.daily["day"] <= month_end(month))]
    rows = []
    for r in result.monthly.itertuples():
        b = None
        if budget:
            b = sum(budget.values()) if r.mark == TOTAL else budget.get(r.mark)
        rows.append({"level": "month", "mark": r.mark, "period": str(pd.Timestamp(r.month).date()),
                     "forecast": float(r.forecast), "seed_min": r.seed_min, "seed_max": r.seed_max,
                     "paid_media_eur": b})
    day_spend = spend.groupby("day")["spend"].sum(min_count=1)
    mark_spend = spend.set_index(["mark", "day"])["spend"]
    for r in result.daily.itertuples():
        s = day_spend.get(r.day) if r.mark == TOTAL else mark_spend.get((r.mark, r.day))
        rows.append({"level": "day", "mark": r.mark, "period": str(pd.Timestamp(r.day).date()),
                     "forecast": float(r.forecast), "seed_min": r.seed_min, "seed_max": r.seed_max,
                     "paid_media_eur": (float(s) if (budget and s is not None and not pd.isna(s)) else None)})
    frame = pd.DataFrame(rows)
    meta = {"month": str(month), "cutoff": str(cutoff.date()), "data_end": str(inputs.data_end.date()),
            "model_key": model_key, "budget": budget, "budget_total": budget_total, "notes": notes}
    for k in ("month", "cutoff", "data_end", "model_key"):
        frame[k] = meta[k]
    frame["notes"] = " | ".join(notes)
    return frame, meta


def forecast(run_id: str, target: str, month=None, budget_total=None, budget_es=None, budget_pt=None) -> dict:
    """Forecast the next month with the registered champion of ``target`` (alias ``champion``)."""
    import mlflow

    batch = safe_name(run_id)
    tracking.silence_mlflow()
    tracking.client()
    try:
        version = tracking.champion_version(target)
    except Exception as error:
        raise RuntimeError(f"No champion registered for {target} ({tracking.registered_name(target)}@"
                           f"{tracking.CHAMPION_ALIAS}): run the ml_forecast_selection DAG first.") from error
    model = mlflow.pyfunc.load_model(f"models:/{tracking.registered_name(target)}@{tracking.CHAMPION_ALIAS}")
    request = pd.DataFrame([{"month": str(month or ""),
                             "paid_media_budget_eur": np.nan if budget_total is None else float(budget_total),
                             "paid_media_budget_es_eur": np.nan if budget_es is None else float(budget_es),
                             "paid_media_budget_pt_eur": np.nan if budget_pt is None else float(budget_pt)}])
    frame = model.predict(request)
    first = frame.iloc[0]
    budget = {}
    for mk in ("Online ES", "Online PT"):
        v = frame[(frame["level"] == "month") & (frame["mark"] == mk)]["paid_media_eur"]
        if len(v) and pd.notna(v.iloc[0]):
            budget[mk] = float(v.iloc[0])
    meta = {"month": first["month"], "cutoff": first["cutoff"], "data_end": first["data_end"],
            "budget": budget or None, "budget_total": budget_total,
            "notes": [n for n in str(first["notes"]).split(" | ") if n]}
    rid = tracking.log_forecast_run(target, batch_id=batch, version=version, frame=frame, meta=meta)
    con = _con()
    try:
        folder = area("next_month", batch, target)
        _write_frame(con, frame, f"{folder}/forecast.parquet")
        summary = {"target": target, "month": meta["month"], "model_key": version.tags.get("model_key"),
                   "registered_version": str(version.version), "selection_batch_id": version.tags.get("selection_batch_id"),
                   "backtest_wape_monthly": float(version.tags.get("wape_monthly", "nan")),
                   "mlflow_run_id": rid, **{k: meta[k] for k in ("cutoff", "data_end", "notes", "budget")}}
        write_json(f"{folder}/summary.json", summary)
    finally:
        con.close()
    total = frame[(frame["level"] == "month") & (frame["mark"] == TOTAL)]["forecast"]
    log.info("%s %s: %s = %.0f (ES + PT)", target, meta["month"], version.tags.get("model_key"), float(total.iloc[0]))
    return {"batch_id": batch, "target": target, "month": meta["month"], "model_key": version.tags.get("model_key"),
            "registered_version": str(version.version), "forecast_es_pt": round(float(total.iloc[0]), 2),
            "notes": meta["notes"]}


def publish_forecast(run_id: str, targets=None) -> dict:
    batch = safe_name(run_id)
    generated_at = _utc_now()
    con = _con()
    monthly_rows, daily_rows, done = [], [], {}
    try:
        for target in (targets or TARGETS):
            folder = area("next_month", batch, target)
            summary = read_json(f"{folder}/summary.json")
            if not summary:
                continue
            frame = _read_frame(con, f"{folder}/forecast.parquet", dates=())
            spec = catalog.spec(target, summary["model_key"])
            month = pd.Timestamp(summary["month"] + "-01")
            common = {"forecast_batch_id": batch, "target": target, "target_label": TARGET_LABELS[target],
                      "forecast_month": month, "model_key": spec.key, "registered_model_version": int(summary["registered_version"])}
            mon = frame[frame["level"] == "month"]
            monthly_rows.append(pd.DataFrame({
                **common, "generated_at": generated_at, "month_date_key": int(outputs.date_key([month]).iloc[0]),
                "month_label": summary["month"], "market": mon["mark"].to_numpy(), "is_total": (mon["mark"] == TOTAL).to_numpy(),
                "forecast": mon["forecast"].to_numpy(), "forecast_seed_min": mon["seed_min"].to_numpy(),
                "forecast_seed_max": mon["seed_max"].to_numpy(), "model_name": spec.name, "family": spec.family,
                "scenario": spec.scenario, "uses_paid_media": spec.uses_paid_media,
                "paid_media_budget_eur": mon["paid_media_eur"].to_numpy(),
                "registered_model_name": tracking.registered_name(target),
                "selection_batch_id": summary.get("selection_batch_id"),
                "backtest_wape_monthly": summary.get("backtest_wape_monthly"),
                "cutoff_date": pd.Timestamp(summary["cutoff"]), "data_end": pd.Timestamp(summary["data_end"]),
                "notes": " | ".join(summary.get("notes") or []), "mlflow_run_id": summary["mlflow_run_id"]}))
            day = frame[frame["level"] == "day"]
            if len(day):
                days = pd.to_datetime(day["period"])
                daily_rows.append(pd.DataFrame({
                    **common, "date": days.to_numpy(), "date_key": outputs.date_key(days).to_numpy(),
                    "market": day["mark"].to_numpy(), "is_total": (day["mark"] == TOTAL).to_numpy(),
                    "forecast": day["forecast"].to_numpy(), "forecast_seed_min": day["seed_min"].to_numpy(),
                    "forecast_seed_max": day["seed_max"].to_numpy(), "paid_media_day_eur": day["paid_media_eur"].to_numpy()}))
            done[target] = {"month": summary["month"], "model_key": spec.key}
        if not done:
            raise RuntimeError(f"Batch {batch} has no forecast to publish: the forecast tasks did not finish.")
        written = {outputs.NEXT_MONTH: _merge_gold(con, outputs.NEXT_MONTH, pd.concat(monthly_rows, ignore_index=True),
                                                   ["target", "forecast_month"])}
        if daily_rows:
            new = pd.concat(daily_rows, ignore_index=True)
            # Months forecast again replace their old daily rows (also when the new champion has no daily forecast).
            written[outputs.NEXT_MONTH_DAILY] = _merge_gold(con, outputs.NEXT_MONTH_DAILY, new, ["target", "forecast_month"])
        return {"batch_id": batch, "published": done, "gold_rows": written}
    finally:
        con.close()
