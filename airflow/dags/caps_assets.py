"""Airflow Assets that chain the pipelines of the platform (data-aware scheduling).

Instead of fixed times, downstream DAGs start when the data they need has been
refreshed:

    bronze_mysql_ingestion  --produces-->  BRONZE_MYSQL  ----+
    example_api_to_bronze   --produces-->  BRONZE_EXAMPLE_API +--> dbt_medallion
    public_holidays_to_bronze --produces--> BRONZE_PUBLIC_HOLIDAYS -+
    open_meteo_daily_to_bronze --produces--> BRONZE_OPEN_METEO_DAILY -+
    dbt_medallion           --produces-->  GOLD  ------------------> gold_export_powerbi
                                            GOLD  ------------------> ml_forecast_selection
    ml_forecast_selection   --produces-->  GOLD_FORECAST  ---------> gold_export_powerbi
    ml_forecast_next_month  --produces-->  GOLD_FORECAST  ---------> gold_export_powerbi

This module only declares the assets; it is imported by the DAG files and is
listed in .airflowignore so the DAG processor does not parse it as a DAG.
"""
from __future__ import annotations

from airflow.sdk import Asset

# Raw data extracted from the source MySQL, stored as Parquet in the bronze bucket.
BRONZE_MYSQL = Asset("lake_bronze_mysql")

# Raw data of the example API, promoted from the staging area to bronze.
BRONZE_EXAMPLE_API = Asset("lake_bronze_example_api")

# Public holidays (Nager.Date API, plus Hebcal for Israel), promoted from the staging area to bronze.
BRONZE_PUBLIC_HOLIDAYS = Asset("lake_bronze_public_holidays")

# Daily weather per marketplace country (Open-Meteo API), promoted from staging to bronze.
BRONZE_OPEN_METEO_DAILY = Asset("lake_bronze_open_meteo_daily")

# Business-ready datasets produced by dbt in the gold bucket.
GOLD = Asset("lake_gold")

# Forecasting tables written to the gold bucket by the ml_forecast_* DAGs (gld_forecast_*.parquet).
# A separate asset on purpose: ml_forecast_selection is scheduled on GOLD, so producing GOLD
# itself would start it again in a loop.
GOLD_FORECAST = Asset("lake_gold_forecast")
