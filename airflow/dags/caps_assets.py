"""Airflow Assets that chain the pipelines of the platform (data-aware scheduling).

Instead of fixed times, downstream DAGs start when the data they need has been
refreshed:

    bronze_mysql_ingestion  --produces-->  BRONZE_MYSQL  ----+
    example_api_to_bronze   --produces-->  BRONZE_EXAMPLE_API +--> dbt_medallion
    dbt_medallion           --produces-->  GOLD  ------------------> gold_export_powerbi

This module only declares the assets; it is imported by the DAG files and is
listed in .airflowignore so the DAG processor does not parse it as a DAG.
"""
from __future__ import annotations

from airflow.sdk import Asset

# Raw data extracted from the source MySQL, stored as Parquet in the bronze bucket.
BRONZE_MYSQL = Asset("lake_bronze_mysql")

# Raw data of the example API, promoted from the staging area to bronze.
BRONZE_EXAMPLE_API = Asset("lake_bronze_example_api")

# Business-ready datasets produced by dbt in the gold bucket.
GOLD = Asset("lake_gold")
