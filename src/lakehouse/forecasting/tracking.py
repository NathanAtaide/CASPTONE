"""MLflow: experiments, runs, charts, registry and the "forecast method" model.

Layout on the tracking server (http://mlflow:5000, UI on http://localhost:5000):

* one experiment per target: ``caps_forecast_orders``, ``caps_forecast_units``,
  ``caps_forecast_sales``;
* one parent run per selection (``selection_<batch>``) with the leaderboard and
  the comparison chart, and one child run per candidate model with the same
  metric names, so the models can be compared side by side (select the child
  runs > Compare);
* one run per forecast of a future month (``forecast_<YYYY-MM>``);
* registered models ``caps_forecast_<target>``: every selection registers its
  champion as a new version and moves the alias ``champion`` to it.

The registered model is a pyfunc *method*: the models of the studies are
retrained every month with the data up to the cutoff (M-2), so what is
registered is the recipe (model key + catalogue version), not frozen weights.
``predict`` takes the month (and, for scenario C, the paid media budget) and
retrains and forecasts with the data of the silver layer. It needs the
``lakehouse`` package and the lake credentials (Airflow and Jupyter have both).
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from ..config import setting
from .catalog import CATALOG_VERSION, ModelSpec
from .protocol import TARGET_DEFINITIONS, TARGET_LABELS, TOTAL

log = logging.getLogger(__name__)

EXPERIMENT_PREFIX = "caps_forecast_"
REGISTERED_PREFIX = "caps_forecast_"
CHAMPION_ALIAS = "champion"
PIPELINE_VERSION = "1.0"
MODEL_REQUIREMENTS = ["torch==2.15.0", "scikit-learn==1.9.1", "xgboost-cpu==3.4.1", "holidays==0.108",
                      "duckdb==1.5.6", "pandas==3.0.6", "numpy==2.5.4", "mlflow-skinny==3.17.0"]


def tracking_uri() -> str:
    return setting("MLFLOW_TRACKING_URI", "http://mlflow:5000")


def client():
    import mlflow
    from mlflow.tracking import MlflowClient

    mlflow.set_tracking_uri(tracking_uri())
    return MlflowClient(tracking_uri=tracking_uri())


def experiment_name(target: str) -> str:
    return EXPERIMENT_PREFIX + target


def registered_name(target: str) -> str:
    return REGISTERED_PREFIX + target


def experiment_id(c, target: str) -> str:
    name = experiment_name(target)
    exp = c.get_experiment_by_name(name)
    if exp is not None:
        if exp.lifecycle_stage == "deleted":
            c.restore_experiment(exp.experiment_id)
        return exp.experiment_id
    return c.create_experiment(name, tags={"target": target, "target_definition": TARGET_DEFINITIONS[target],
                                           "project": "caps", "created_by": "ml_forecast_selection"})


def _now_ms() -> int:
    return int(time.time() * 1000)


def _clean(value) -> str:
    text = str(value)
    return text if len(text) <= 5000 else text[:4997] + "..."


def log_flat(c, run_id: str, *, params: dict | None = None, metrics: dict | None = None, tags: dict | None = None) -> None:
    from mlflow.entities import Metric, Param, RunTag

    ts = _now_ms()
    p = [Param(k, _clean(v)) for k, v in (params or {}).items() if v is not None]
    m = [Metric(k, float(v), ts, 0) for k, v in (metrics or {}).items()
         if v is not None and np.isfinite(float(v))]
    t = [RunTag(k, _clean(v)) for k, v in (tags or {}).items() if v is not None]
    for i in range(0, max(len(p), len(m), len(t)), 100):   # log_batch limits
        c.log_batch(run_id, metrics=m[i:i + 100], params=p[i:i + 100], tags=t[i:i + 100])


def log_frames(c, run_id: str, frames: dict[str, pd.DataFrame], folder: str) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        for name, frame in frames.items():
            if frame is None or frame.empty:
                continue
            path = Path(tmp) / f"{name}.csv"
            frame.to_csv(path, index=False)
            c.log_artifact(run_id, str(path), folder or None)


def log_json(c, run_id: str, payload: dict, name: str, folder: str = "") -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / name
        path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        c.log_artifact(run_id, str(path), folder or None)


def _fmt_number(v, _pos=None):
    return f"{v:,.0f}".replace(",", ".")


def log_figure(c, run_id: str, fig, name: str, folder: str = "charts") -> None:
    import matplotlib.pyplot as plt

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / name
        fig.savefig(path, dpi=130, bbox_inches="tight")
        c.log_artifact(run_id, str(path), folder)
    plt.close(fig)


def model_chart(target: str, spec: ModelSpec, monthly: pd.DataFrame):
    """Actual (bars) against the forecast (line, with the seed range) of the ES + PT monthly totals."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    d = monthly[monthly["mark"] == TOTAL].sort_values("month")
    x = np.arange(len(d))
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.bar(x, d["actual"], color="#4F81BD", width=0.6, label="Actual")
    ax.plot(x, d["forecast"], color="#C0504D", marker="o", linewidth=2, label=spec.name)
    if d["seed_min"].notna().any():
        ax.fill_between(x, d["seed_min"], d["seed_max"], color="#C0504D", alpha=0.15, label="Range of the seeds")
    ax.set_xticks(x)
    ax.set_xticklabels([m.strftime("%b %Y") for m in d["month"]])
    ax.yaxis.set_major_formatter(FuncFormatter(_fmt_number))
    ax.set_title(f"{spec.name}: {TARGET_LABELS[target]} (Online ES + Online PT, monthly)")
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False)
    return fig


def comparison_chart(target: str, monthly: pd.DataFrame, lb: pd.DataFrame):
    """Actual against every candidate: champion bold, Baseline 3 dashed."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    tot = monthly[monthly["mark"] == TOTAL]
    months = sorted(tot["month"].unique())
    x = np.arange(len(months))
    actual = tot.groupby("month")["actual"].first().reindex(months)
    fig, ax = plt.subplots(figsize=(11, 5.5))
    ax.bar(x, actual, color="#D9D9D9", width=0.65, label="Actual")
    for row in lb.itertuples():
        d = tot[tot["model_key"] == row.model_key].set_index("month")["forecast"].reindex(months)
        style = dict(linewidth=3.2, color="#C0504D", zorder=5) if row.is_champion else dict(linewidth=1.3, alpha=0.8)
        if row.is_baseline_reference:
            style.update(linestyle="--", color="#404040")
        ax.plot(x, d, marker="o", label=f"{row.model_name} ({100 * row.wape_monthly:.1f}%)", **style)
    ax.set_xticks(x)
    ax.set_xticklabels([pd.Timestamp(m).strftime("%b %Y") for m in months])
    ax.yaxis.set_major_formatter(FuncFormatter(_fmt_number))
    ax.set_title(f"{TARGET_LABELS[target]}: candidates against the actual (ES + PT; monthly WAPE in brackets)")
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False, fontsize=8, ncol=2, loc="upper left")
    return fig


# ---------------------------------------------------------------------------- runs
def create_parent(c, target: str, batch_id: str, tags: dict) -> str:
    exp = experiment_id(c, target)
    run = c.create_run(exp, run_name=f"selection_{batch_id}"[:250],
                       tags={"kind": "selection", "batch_id": batch_id, "target": target,
                             "pipeline_version": PIPELINE_VERSION, "catalog_version": CATALOG_VERSION, **tags})
    return run.info.run_id


def delete_previous_children(c, target: str, batch_id: str, model_key: str) -> None:
    """A retried task replaces the run it logged before instead of duplicating it."""
    exp = experiment_id(c, target)
    for r in c.search_runs([exp], filter_string=f"tags.batch_id = '{batch_id}' and tags.model_key = '{model_key}' "
                                                 f"and tags.kind = 'candidate'"):
        c.delete_run(r.info.run_id)


def log_candidate(c, *, target: str, batch_id: str, parent_run_id: str, spec: ModelSpec, output, metrics: dict,
                  monthly: pd.DataFrame, daily: pd.DataFrame, months, data_tags: dict) -> str:
    delete_previous_children(c, target, batch_id, spec.key)
    exp = experiment_id(c, target)
    run = c.create_run(exp, run_name=spec.key, tags={
        "mlflow.parentRunId": parent_run_id, "kind": "candidate", "batch_id": batch_id, "target": target,
        "model_key": spec.key, "model_name": spec.name, "family": spec.family, "scenario": spec.scenario,
        "uses_paid_media": str(spec.uses_paid_media).lower(), "is_baseline_reference": str(spec.is_baseline_reference).lower(),
        "mlflow.note.content": spec.description, **data_tags})
    rid = run.info.run_id
    try:
        tot = monthly[monthly["mark"] == TOTAL].sort_values("month")
        per_month = {f"error_pct_{m.strftime('%Y_%m')}": e for m, e in zip(tot["month"], tot["error_pct"])}
        log_flat(c, rid, params={**spec.as_params(), **output.params, "target": target,
                                 "backtest_months": ",".join(str(m) for m in months),
                                 "cutoff_rule": "data up to the last day of M-2, monthly refit",
                                 "target_definition": TARGET_DEFINITIONS[target]},
                 metrics={**metrics, **per_month, "training_seconds": output.seconds})
        log_frames(c, rid, {"monthly": monthly, "daily": daily, "seeds": output.seeds}, "predictions")
        log_frames(c, rid, output.details, "details")
        if output.warnings:
            log_json(c, rid, {"warnings": output.warnings}, "warnings.json")
        log_figure(c, rid, model_chart(target, spec, monthly), "monthly_actual_vs_forecast.png")
        c.set_terminated(rid, "FINISHED")
    except Exception:
        c.set_terminated(rid, "FAILED")
        raise
    return rid


def finish_selection(c, *, target: str, parent_run_id: str, lb: pd.DataFrame, monthly: pd.DataFrame,
                     selection_rule: str, champion_version: str | None) -> None:
    champ = lb[lb["is_champion"]].iloc[0]
    for row in lb.itertuples():
        c.set_tag(row.mlflow_run_id, "rank_wape", str(row.rank_wape))
        c.set_tag(row.mlflow_run_id, "champion", str(bool(row.is_champion)).lower())
    log_flat(c, parent_run_id,
             params={"selection_rule": selection_rule, "candidates": ",".join(lb["model_key"]),
                     "champion": champ["model_key"]},
             metrics={"champion_wape_monthly": champ["wape_monthly"], "champion_bias_monthly": champ["bias_monthly"],
                      "baseline_wape_monthly": champ["baseline_wape_monthly"],
                      "champion_wape_vs_baseline_pp": champ["wape_vs_baseline_pp"], "candidates": len(lb)},
             tags={"champion_model": champ["model_key"], "champion_name": champ["model_name"],
                   "champion_registered_version": champion_version or "-",
                   "mlflow.note.content": f"Champion: {champ['model_name']} - monthly WAPE {100 * champ['wape_monthly']:.2f}% "
                                          f"(Baseline 3 monthly {100 * champ['baseline_wape_monthly']:.2f}%)."})
    cols = ["rank_wape", "model_key", "model_name", "family", "scenario", "uses_paid_media", "is_champion",
            "wape_monthly", "bias_monthly", "rmse_monthly", "r2_monthly", "wape_daily", "wape_monthly_es",
            "wape_monthly_pt", "wape_seed_min", "wape_seed_max", "wape_vs_baseline_pp", "mlflow_run_id"]
    log_frames(c, parent_run_id, {"leaderboard": lb[cols], "monthly_all_models": monthly}, "")
    log_figure(c, parent_run_id, comparison_chart(target, monthly, lb), "comparison_monthly.png")
    c.set_terminated(parent_run_id, "FINISHED")


# ---------------------------------------------------------------------------- registry
def register_champion(target: str, *, run_id: str, spec: ModelSpec, batch_id: str, metrics: dict,
                      data_end: str, months) -> str:
    """Log the forecast method in the champion's run, register it and move the alias ``champion``."""
    import mlflow
    from mlflow.models import ModelSignature
    from mlflow.types import ColSpec, Schema

    c = client()
    name = registered_name(target)
    signature = ModelSignature(
        inputs=Schema([ColSpec("string", "month", required=False),
                       ColSpec("double", "paid_media_budget_eur", required=False),
                       ColSpec("double", "paid_media_budget_es_eur", required=False),
                       ColSpec("double", "paid_media_budget_pt_eur", required=False)]),
        outputs=Schema([ColSpec("string", "level"), ColSpec("string", "mark"), ColSpec("string", "period"),
                        ColSpec("double", "forecast"), ColSpec("double", "seed_min"), ColSpec("double", "seed_max")]),
    )
    with mlflow.start_run(run_id=run_id):
        info = mlflow.pyfunc.log_model(
            name="forecast_method",
            python_model=ForecastMethodModel(target=target, model_key=spec.key, catalog_version=CATALOG_VERSION),
            signature=signature, pip_requirements=MODEL_REQUIREMENTS, registered_model_name=name,
            metadata={"target": target, "model_key": spec.key, "selection_batch_id": batch_id},
        )
    version = getattr(info, "registered_model_version", None)
    if version is None:
        version = max(int(v.version) for v in c.search_model_versions(f"name='{name}'") if v.run_id == run_id)
    version = str(version)
    c.update_registered_model(name, description=(
        f"Champion forecast method for {TARGET_LABELS[target]} (Online ES + Online PT). Alias '{CHAMPION_ALIAS}' = "
        f"model chosen by the last selection. predict(DataFrame(month='YYYY-MM', paid_media_budget_eur=...)) retrains "
        f"with the data up to the cutoff (M-2) and forecasts the month."))
    for k, v in {"model_key": spec.key, "model_name": spec.name, "family": spec.family, "scenario": spec.scenario,
                 "uses_paid_media": str(spec.uses_paid_media).lower(), "selection_batch_id": batch_id,
                 "data_end": data_end, "backtest_window": f"{months[0]}..{months[-1]}",
                 "wape_monthly": f"{metrics['wape_monthly']:.6f}", "bias_monthly": f"{metrics['bias_monthly']:.6f}",
                 "catalog_version": CATALOG_VERSION}.items():
        c.set_model_version_tag(name, version, k, v)
    c.set_registered_model_alias(name, CHAMPION_ALIAS, version)
    return version


def champion_version(target: str):
    c = client()
    return c.get_model_version_by_alias(registered_name(target), CHAMPION_ALIAS)


def log_forecast_run(target: str, *, batch_id: str, version, frame: pd.DataFrame, meta: dict) -> str:
    c = client()
    exp = experiment_id(c, target)
    month = meta["month"]
    run = c.create_run(exp, run_name=f"forecast_{month}", tags={
        "kind": "forecast", "batch_id": batch_id, "target": target, "forecast_month": month,
        "model_key": version.tags.get("model_key", ""), "registered_model": registered_name(target),
        "registered_version": str(version.version), "selection_batch_id": version.tags.get("selection_batch_id", ""),
        "mlflow.note.content": " | ".join(meta.get("notes", [])) or "-"})
    rid = run.info.run_id
    monthly = frame[frame["level"] == "month"]
    totals = monthly.set_index("mark")["forecast"].to_dict()
    log_flat(c, rid, params={"month": month, "cutoff": meta["cutoff"], "data_end": meta["data_end"],
                             "paid_media_budget_eur": meta.get("budget_total"),
                             "paid_media_budget_es_eur": (meta.get("budget") or {}).get("Online ES"),
                             "paid_media_budget_pt_eur": (meta.get("budget") or {}).get("Online PT")},
             metrics={"forecast_es_pt": totals.get(TOTAL), "forecast_es": totals.get("Online ES"),
                      "forecast_pt": totals.get("Online PT")})
    log_frames(c, rid, {"forecast": frame}, "")
    c.set_terminated(rid, "FINISHED")
    return rid


# ---------------------------------------------------------------------------- pyfunc
# The explicit signature of register_champion describes the input; MLflow's hint about type hints does not apply.
warnings.filterwarnings("ignore", message=".*Add type hints to the `predict` method.*")
try:
    import mlflow.pyfunc as _pyfunc
    _Base = _pyfunc.PythonModel
except Exception:   # mlflow not installed (e.g. light environments): the class is only needed with MLflow
    _Base = object


class ForecastMethodModel(_Base):
    """Registered "forecast method": retrains the champion with the data up to M-2 and forecasts month M."""

    def __init__(self, target: str, model_key: str, catalog_version: str):
        self.target = target
        self.model_key = model_key
        self.catalog_version = catalog_version

    def predict(self, context, model_input, params=None):
        from .pipeline import forecast_month

        frames = []
        for row in pd.DataFrame(model_input).to_dict("records"):
            def num(key):
                v = row.get(key)
                return None if v is None or (isinstance(v, float) and np.isnan(v)) else float(v)
            frame, _meta = forecast_month(self.target, self.model_key, month=(row.get("month") or None),
                                          budget_total=num("paid_media_budget_eur"),
                                          budget_es=num("paid_media_budget_es_eur"),
                                          budget_pt=num("paid_media_budget_pt_eur"))
            frames.append(frame)
        return pd.concat(frames, ignore_index=True)


def silence_mlflow() -> None:
    """Keep the task logs readable (the MLflow client is chatty about git and env managers)."""
    os.environ.setdefault("GIT_PYTHON_REFRESH", "quiet")
    os.environ.setdefault("MLFLOW_ENABLE_ASYNC_LOGGING", "false")
    for name in ("mlflow", "urllib3", "git"):
        logging.getLogger(name).setLevel(logging.WARNING)
    # Informational only: the model is a small recipe object, logged without an input example on purpose
    # (predicting an example would retrain the champion while logging it).
    logging.getLogger("mlflow.pyfunc").setLevel(logging.ERROR)
    warnings.filterwarnings("ignore", message=".*An input example was not provided.*")
