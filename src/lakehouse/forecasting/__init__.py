"""Demand forecasting of the CAPS platform: backtest, comparison and champion selection.

Integrates the studies of ``ml_models/`` (Baseline, Machine Learning and Deep
Learning) into the platform: every candidate model is re-run with the same
protocol on the silver layer, logged to MLflow, compared with the Baseline 3
and the best model of each target (orders, units, gross sales) is registered
as the champion and used to forecast the next month.

    protocol   scope, cutoff rule (M-2), backtest window
    catalog    candidate models of each target and their settings
    data       model inputs built from the silver layer (and future months)
    models/    baseline, tree models (ML) and neural networks (DL)
    metrics    WAPE, bias, RMSE, R2, leaderboard and champion rule
    tracking   MLflow runs, registry and the pyfunc "forecast method" model
    outputs    Parquet tables written to the gold layer (Power BI)
    pipeline   the steps called by the Airflow DAGs
    cli        ``python -m lakehouse.forecasting <step>`` (used by the DAGs)

The heavy libraries (torch, scikit-learn, xgboost, mlflow) are only imported by
the modules that need them, so ``protocol``/``catalog`` stay light. In Airflow
they live in the isolated virtualenv ``/opt/ml-venv``.
"""
