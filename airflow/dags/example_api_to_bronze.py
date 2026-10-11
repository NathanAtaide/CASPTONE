"""
### example_api_to_bronze - API -> staging (Parquet) -> bronze

Reference implementation of the **staging area** pattern for data that comes
from APIs. It calls a public weather API (Open-Meteo, no key required) so the
whole platform can be tried end to end before your own sources are configured.

1. `extract_to_staging` - calls the API and lands the payload, untouched, as a
   Parquet file in `s3://staging/open_meteo/hourly_weather/load_date=.../`;
2. `promote_to_bronze` - imports that file into the data lake
   (`s3://bronze/open_meteo/hourly_weather/ingestion_date=.../`) adding the
   lineage columns, and updates the `lake_bronze_example_api` asset, which
   starts `dbt_medallion`.

**To build your own API pipeline:** copy this file, change `SOURCE`, `ENTITY`
and the request, and read the API key from the vault with
`setting("MY_API_TOKEN", required=True)`.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.sdk import Param, dag, task

from caps_assets import BRONZE_EXAMPLE_API

log = logging.getLogger(__name__)

# <source>/<entity> define the folder of the dataset in staging and bronze.
SOURCE = "open_meteo"
ENTITY = "hourly_weather"

API_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_FIELDS = ["temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m"]


@dag(
    dag_id="example_api_to_bronze",
    description="Example: public API -> staging area (Parquet) -> bronze layer.",
    schedule=None,  # manual trigger; use a cron expression for a real source
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["example", "api", "staging", "bronze"],
    doc_md=__doc__,
    params={
        "location": Param("sao_paulo", type="string", description="Label stored with the rows."),
        "latitude": Param(-23.55, type="number", minimum=-90, maximum=90),
        "longitude": Param(-46.63, type="number", minimum=-180, maximum=180),
        "past_days": Param(7, type="integer", minimum=0, maximum=92, description="Days of history to fetch."),
    },
)
def example_api_to_bronze():
    @task(retries=2, retry_delay=timedelta(minutes=1))
    def extract_to_staging(params: dict | None = None, dag_run=None) -> dict:
        """Call the API and land the response as Parquet in the staging area."""
        import requests

        from lakehouse import staging
        from lakehouse.naming import partition_date, safe_name

        params = params or {}

        # Real APIs need credentials. Keep them in the vault and read them here:
        #   from lakehouse import setting
        #   headers = {"Authorization": f"Bearer {setting('MY_API_TOKEN', required=True)}"}
        response = requests.get(
            API_URL,
            params={
                "latitude": params.get("latitude", -23.55),
                "longitude": params.get("longitude", -46.63),
                "hourly": ",".join(HOURLY_FIELDS),
                "past_days": params.get("past_days", 7),
                "forecast_days": 1,
                "timezone": "UTC",
            },
            timeout=60,
        )
        response.raise_for_status()
        payload = response.json()

        # The API returns one array per variable; reshape it into one record per hour.
        hourly = payload["hourly"]
        records = []
        for index, moment in enumerate(hourly["time"]):
            record = {
                "location": params.get("location", "sao_paulo"),
                "latitude": payload.get("latitude"),
                "longitude": payload.get("longitude"),
                # "2026-01-31T13:00" -> "2026-01-31T13:00:00" (unambiguous ISO timestamp, UTC)
                "observed_at": f"{moment}:00",
            }
            for field in HOURLY_FIELDS:
                record[field] = hourly[field][index]
            records.append(record)

        batch_id = safe_name(dag_run.run_id)
        load_date = str(partition_date(getattr(dag_run, "run_after", None)))
        staged = staging.write_records(
            records,
            source=SOURCE,
            entity=ENTITY,
            batch_id=batch_id,
            load_date=load_date,
        )
        # Passed to the next task so it promotes exactly this batch.
        return {**staged, "batch_id": batch_id, "load_date": load_date}

    @task(outlets=[BRONZE_EXAMPLE_API])
    def promote_to_bronze(staged: dict) -> dict:
        """Import the staged file into the bronze layer of the data lake."""
        from lakehouse import staging

        return staging.promote_to_bronze(
            source=SOURCE,
            entity=ENTITY,
            batch_id=staged["batch_id"],
            load_date=staged["load_date"],
        )

    promote_to_bronze(extract_to_staging())


example_api_to_bronze()
