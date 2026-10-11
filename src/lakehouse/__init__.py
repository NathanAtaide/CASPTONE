"""Shared lakehouse helpers for the CAPS data platform.

One small package used by BOTH the Airflow DAGs and the Jupyter notebooks, so
pipelines and exploration always talk to the lake the same way:

    config        settings/secrets resolution and lake path conventions
    duck          DuckDB sessions already wired to the lake (and to MySQL)
    mysql_source  batch extraction MySQL -> bronze
    staging       API payloads -> staging (Parquet) -> bronze
    export        gold -> Parquet files on the host (Power BI)
    bootstrap     bucket creation (run by the lake-init service)
    naming        batch ids and partition dates
"""
from .config import LakeConfig, MissingSettingError, MySQLConfig, setting
from .duck import connect

__all__ = ["LakeConfig", "MissingSettingError", "MySQLConfig", "connect", "setting"]
