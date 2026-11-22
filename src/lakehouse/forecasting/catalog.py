"""Catalogue of the candidate models of each target.

Every entry is one model selected in the studies of ``ml_models/`` (the
notebooks are the source of the settings; nothing is tuned again here except
the time-series cross-validation that the ML models do inside every refit):

* Baseline (``ml_models/Baseline``): Baseline 3 = same month of the previous
  year x recent growth, in its monthly version (the reference every study
  compares with) and its daily version.
* Machine learning (``ml_models/ML``): C.4 Random Forest, D.1 Gradient Boosting
  and F.1 XGBoost by product category group (units and sales only: an order
  cannot be split by category).
* Deep learning (``ml_models/Deep Learning``): the final models of the orders,
  gross sales and units notebooks. Scenario A uses only information known at
  the cutoff; scenario C also uses the paid media spend of the month being
  forecast (actual spend in the backtest, the planned budget in production).

A *job* is the unit of work of one Airflow task: it produces one or more
models (the Baseline job produces both B3 versions; the MLP A job also produces
the 50/50 blend with Baseline 3, which reuses the same networks).
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field

from .protocol import TARGETS

# Bump when a model definition changes: it is part of the data fingerprint and of the MLflow tags.
CATALOG_VERSION = "1.0"

BASELINE = "Baseline"
ML = "Machine Learning"
DL = "Deep Learning"

# Groups of input columns of the deep-learning models (same names as the notebooks).
DL_FEATURE_GROUPS: dict[str, list[str]] = {
    "weekday": [f"dow_{k}" for k in range(7)],
    "month_of_year": ["month_sin", "month_cos"],
    "end_of_month": ["eom"],
    "holiday": ["hol_wd"],
    "campaigns": ["camp_black_friday", "camp_christmas", "camp_mothers_day",
                  "camp_reyes", "camp_valentines", "camp_easter"],
    "saldos": ["flag_saldos"],
    "level": ["level_28d"],
    "paid_month": ["spend_day", "spend_month"],
}


@dataclass(frozen=True)
class ModelSpec:
    key: str                  # stable id used in MLflow, in the lake and in Power BI
    name: str                 # display name
    family: str               # Baseline / Machine Learning / Deep Learning
    algorithm: str
    scenario: str             # "A" = no paid media of the forecast month, "C" = uses it, "-" = baseline
    uses_paid_media: bool     # needs the paid media (budget) of the month being forecast
    level: str                # market / total / category_group: where the model works
    job: str                  # job (Airflow task) that produces it
    source: str               # notebook it comes from
    description: str
    config: dict = field(default_factory=dict)
    is_baseline_reference: bool = False

    def as_params(self) -> dict:
        """Flat dictionary for MLflow params."""
        out = {k: v for k, v in asdict(self).items() if k != "config"}
        for k, v in self.config.items():
            out[f"cfg_{k}"] = v
        return out


_B3 = dict(k_months=3)

_BASELINES = [
    ModelSpec(
        key="b3_monthly", name="Baseline 3 (monthly)", family=BASELINE, algorithm="same month last year x recent growth",
        scenario="-", uses_paid_media=False, level="market", job="baseline", source="Baseline/Baseline_Models_EN.ipynb",
        description="Value of the same month one year before x (last 3 months up to the cutoff / same 3 months one year earlier).",
        config=_B3, is_baseline_reference=True,
    ),
    ModelSpec(
        key="b3_daily", name="Baseline 3 (daily)", family=BASELINE, algorithm="same weekday last year x recent growth",
        scenario="-", uses_paid_media=False, level="market", job="baseline", source="Baseline/Baseline_Models_EN.ipynb",
        description="Each day copies the day 364 days before (same weekday) x the growth of the last 3 months up to the cutoff.",
        config=_B3,
    ),
]

_ML_COMMON = dict(yearly_lag=364, recent_lags="62-68", growth_window=28, growth_end_lag=62,
                  cv_folds=3, cv_valid_days=28, cv_gap_days=30, cv_min_train=28,
                  target_design="deviation from the previous year adjusted for recent growth")

_ML_C4 = ModelSpec(
    key="ml_c4_rf", name="C.4 Random Forest", family=ML, algorithm="RandomForestRegressor",
    scenario="A", uses_paid_media=False, level="total", job="ml_c4_rf", source="ML/ML sobre silver.ipynb",
    description="Random Forest on the daily total ES + PT; depth and leaf size chosen by time-series CV in every refit.",
    config=dict(_ML_COMMON, param_grid="max_depth 4/6/8 x min_samples_leaf 7/14", max_features=0.5,
                trees_cv=200, trees_final=500, default="max_depth 6, min_samples_leaf 7", random_state=42),
)
_ML_D1 = ModelSpec(
    key="ml_d1_gb", name="D.1 Gradient Boosting", family=ML, algorithm="GradientBoostingRegressor",
    scenario="A", uses_paid_media=False, level="total", job="ml_d1_gb", source="ML/ML sobre silver.ipynb",
    description="Gradient Boosting on the daily total ES + PT; depth and number of trees chosen by time-series CV.",
    config=dict(_ML_COMMON, learning_rate=0.05, subsample=0.8, min_samples_leaf=7, depths="2/3",
                max_trees=600, default="max_depth 3, 300 trees", random_state=42),
)
_ML_F1 = ModelSpec(
    key="ml_f1_xgb", name="F.1 XGBoost by category group", family=ML, algorithm="XGBRegressor",
    scenario="A", uses_paid_media=False, level="category_group", job="ml_f1_xgb", source="ML/ML sobre silver.ipynb",
    description="XGBoost on the daily series of 6 product category groups (one model, group as input, weighted by size); "
                "the daily total is the sum of the groups.",
    config=dict(_ML_COMMON, learning_rate=0.05, subsample=0.8, colsample_bytree=0.8, min_child_weight=7,
                param_grid="max_depth 2/3 x reg_lambda 1/10", max_trees=600, tree_step=50,
                default="max_depth 3, reg_lambda 1, 300 trees", sample_weight="group volume in the previous year",
                random_state=42),
)

_DL_FIXED = dict(optimizer="Adam", lr=1e-3, weight_decay=1e-4, batch=32, huber_delta=1.0, seeds="0,1,2,3,4",
                 one_model_per_market=True, clip="spend and level of the target month clipped to the training range")


def _mlp(key, name, scenario, groups, loss, layers, epochs, source, description):
    return ModelSpec(
        key=key, name=name, family=DL, algorithm="MLP (PyTorch)", scenario=scenario,
        uses_paid_media="paid_month" in groups, level="market", job=key, source=source, description=description,
        config=dict(_DL_FIXED, groups=",".join(groups), loss=loss, layers="x".join(map(str, layers)), epochs=epochs),
    )


def _blend(source, description):
    return ModelSpec(
        key="dl_blend_a", name="50/50 Baseline 3 + MLP A", family=DL, algorithm="0.5 x Baseline 3 monthly + 0.5 x MLP A",
        scenario="A", uses_paid_media=False, level="market", job="dl_mlp_a", source=source, description=description,
        config=dict(weight_b3=0.5, weight_mlp=0.5),
    )


_ORDERS_NB = "Deep Learning/Orders (README_Deep_Learning_Orders.md)"
_SALES_NB = "Deep Learning/Sales/Deep_Learning_Vendas_EN.ipynb"
_UNITS_NB = "Deep Learning/Units/Deep_Learning_Unidades_EN_VF.ipynb"

CATALOG: dict[str, list[ModelSpec]] = {
    "orders": [
        *_BASELINES, _ML_C4, _ML_D1,
        _mlp("dl_mlp_a", "MLP A", "A", ["weekday", "end_of_month", "holiday", "campaigns", "saldos"],
             "mse", [32], 300, _ORDERS_NB, "Reference of scenario A in the orders Test protocol (mse_32_e300)."),
        _mlp("dl_mlp_c", "MLP C", "C", ["weekday", "month_of_year", "end_of_month", "holiday", "paid_month", "saldos"],
             "huber", [32], 100, _ORDERS_NB, "Rank 1 of scenario C (huber_32_e100), recommended reference model of the study."),
        _mlp("dl_mlp_c_campaigns", "MLP C + campaigns", "C",
             ["weekday", "month_of_year", "end_of_month", "holiday", "paid_month", "saldos", "campaigns"],
             "huber", [32], 100, _ORDERS_NB, "Rank 2 of scenario C: MLP C plus the six calendar campaigns."),
        ModelSpec(
            key="dl_lstm_c_campaigns", name="LSTM C + campaigns", family=DL, algorithm="LSTM (PyTorch)", scenario="C",
            uses_paid_media=True, level="market", job="dl_lstm_c_campaigns", source=_ORDERS_NB,
            description="Rank 3 of scenario C: one LSTM layer reading a 28-day window of the MLP C + campaigns inputs.",
            config=dict(_DL_FIXED, groups="weekday,month_of_year,end_of_month,holiday,paid_month,saldos,campaigns",
                        loss="huber", hidden=32, window=28, epochs=300),
        ),
    ],
    "sales": [
        *_BASELINES, _ML_C4, _ML_D1, _ML_F1,
        _mlp("dl_mlp_a", "MLP A", "A",
             ["weekday", "month_of_year", "end_of_month", "holiday", "campaigns", "saldos", "level"],
             "mse", [32, 16], 100, _SALES_NB, "T1 of the Test protocol (mse_32x16_e100)."),
        _blend(_SALES_NB, "T2: final model of scenario A (50/50 average of monthly Baseline 3 and MLP A, same seed)."),
        _mlp("dl_mlp_c", "MLP C", "C", ["weekday", "end_of_month", "holiday", "saldos", "paid_month"],
             "huber", [32], 100, _SALES_NB, "T5: final model of scenario C (huber_32_e100), registered rank 1."),
    ],
    "units": [
        *_BASELINES, _ML_C4, _ML_D1, _ML_F1,
        _mlp("dl_mlp_a", "MLP A", "A", ["weekday", "end_of_month", "holiday", "saldos"],
             "mse", [32], 100, _UNITS_NB, "Network of scenario A (mse_32_e100)."),
        _blend(_UNITS_NB, "Final method of scenario A (50/50 average of monthly Baseline 3 and MLP A, same seed)."),
        _mlp("dl_mlp_c", "MLP C", "C", ["weekday", "end_of_month", "holiday", "saldos", "paid_month"],
             "huber", [32], 300, _UNITS_NB, "Final method of scenario C (huber_32_e300)."),
    ],
}

# Rough share of the run time of each job, used to start the slow ones first.
JOB_WEIGHT = {"dl_lstm_c_campaigns": 10, "ml_f1_xgb": 4, "ml_d1_gb": 4, "dl_mlp_a": 3, "dl_mlp_c": 3,
              "dl_mlp_c_campaigns": 2, "ml_c4_rf": 2, "baseline": 0}


def specs(target: str) -> list[ModelSpec]:
    if target not in CATALOG:
        raise ValueError(f"Unknown target '{target}'. Expected one of {list(TARGETS)}.")
    return CATALOG[target]


def spec(target: str, model_key: str) -> ModelSpec:
    for s in specs(target):
        if s.key == model_key:
            return s
    raise ValueError(f"Model '{model_key}' is not a candidate for '{target}'. Candidates: {[s.key for s in specs(target)]}")


def jobs(target: str) -> list[str]:
    """Jobs of a target, slowest first."""
    names = list(dict.fromkeys(s.job for s in specs(target)))
    return sorted(names, key=lambda j: -JOB_WEIGHT.get(j, 1))


def job_specs(target: str, job: str) -> list[ModelSpec]:
    out = [s for s in specs(target) if s.job == job]
    if not out:
        raise ValueError(f"Job '{job}' has no model for '{target}'. Jobs: {jobs(target)}")
    return out


def reference(target: str) -> ModelSpec:
    return next(s for s in specs(target) if s.is_baseline_reference)
