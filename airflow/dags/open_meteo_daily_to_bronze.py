"""
### open_meteo_daily_to_bronze - daily weather API -> staging (Parquet) -> bronze

Brings the **daily weather** (temperature, rain, snow, sunshine) of the three
largest cities of every marketplace country. The gold model
`gld_weather_country_daily` averages them into one value per country and day,
so it can be tested as a driver of daily online sales.

Source: [Open-Meteo historical weather API](https://open-meteo.com/en/docs/historical-weather-api)
(free for non-commercial use, no key required), endpoint
`GET https://archive-api.open-meteo.com/v1/archive`.
Locations, variables and schedule live in `config/weather_locations.yml`.

1. `extract_to_staging` - calls the API once per location for the date window
   and lands one row per city and day, as returned, in
   `s3://staging/open_meteo/daily_weather/load_date=.../`;
2. `promote_to_bronze` - imports that file into the data lake
   (`s3://bronze/open_meteo/daily_weather/ingestion_date=.../`) adding the
   lineage columns, and updates the `lake_bronze_open_meteo_daily` asset,
   which starts `dbt_medallion`.

**Date window:**

- daily runs fetch the last `lookback_days` days up to yesterday. The archive
  publishes with a delay of a few days, so the most recent days may arrive
  empty and are filled by the next runs (silver keeps the latest row per
  country, city and date with `latest_by_key`);
- for the **first load (backfill)**, trigger manually with `start_date`
  (e.g. `2020-03-01`, the first order in the source data) and optionally
  `end_date`.

Dates are local to each city (`timezone=auto`), so a day matches the calendar
day of the orders in that country.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow.sdk import Param, dag, task

from caps_assets import BRONZE_OPEN_METEO_DAILY

log = logging.getLogger(__name__)

# <source>/<entity> define the folder of the dataset in staging and bronze.
SOURCE = "open_meteo"
ENTITY = "daily_weather"

API_URL = "https://archive-api.open-meteo.com/v1/archive"
CONFIG_FILE = Path(__file__).parent / "config" / "weather_locations.yml"


def _load_config() -> dict:
    """Read the locations, variables and schedule of the DAG."""
    with CONFIG_FILE.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


@dag(
    dag_id="open_meteo_daily_to_bronze",
    description="Daily weather per marketplace country (Open-Meteo API) -> staging area (Parquet) -> bronze layer.",
    schedule=_load_config().get("schedule"),  # cron from weather_locations.yml (null = manual)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["api", "staging", "bronze"],
    doc_md=__doc__,
    params={
        "start_date": Param("", type="string", description="Backfill start (YYYY-MM-DD). Empty = use lookback_days."),
        "end_date": Param("", type="string", description="Backfill end (YYYY-MM-DD). Empty = yesterday."),
        "lookback_days": Param(10, type="integer", minimum=1, maximum=366),
    },
)
def open_meteo_daily_to_bronze():
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def extract_to_staging(params: dict | None = None, dag_run=None) -> dict:
        """Call the API for every location and land the daily weather in staging."""
        # Heavy imports stay inside the task so DAG parsing is fast.
        from datetime import date

        import requests

        from lakehouse import staging
        from lakehouse.naming import partition_date, safe_name

        params = params or {}
        config = _load_config()
        variables = config.get("daily_variables") or []
        locations = config.get("locations") or []
        if not variables or not locations:
            raise ValueError(f"{CONFIG_FILE.name} must list daily_variables and locations.")

        today = partition_date(getattr(dag_run, "run_after", None))
        end = date.fromisoformat(params["end_date"]) if params.get("end_date") else today - timedelta(days=1)
        if params.get("start_date"):
            start = date.fromisoformat(params["start_date"])
        else:
            start = end - timedelta(days=int(params.get("lookback_days", 10)) - 1)
        if start > end:
            raise ValueError(f"start_date ({start}) is after end_date ({end}).")

        records: list[dict] = []
        with requests.Session() as session:
            for location in locations:
                response = session.get(
                    API_URL,
                    params={
                        "latitude": location["latitude"],
                        "longitude": location["longitude"],
                        "start_date": str(start),
                        "end_date": str(end),
                        "daily": ",".join(variables),
                        "timezone": "auto",
                    },
                    timeout=120,
                )
                response.raise_for_status()
                payload = response.json()

                # The API returns one array per variable; reshape it into one record per day.
                daily = payload["daily"]
                for index, day in enumerate(daily["time"]):
                    record = {
                        "country_code": location["country_code"],
                        "city": location["city"],
                        "latitude": payload.get("latitude"),
                        "longitude": payload.get("longitude"),
                        "timezone": payload.get("timezone"),
                        "date": day,
                    }
                    for variable in variables:
                        record[variable] = daily.get(variable, [None] * len(daily["time"]))[index]
                    records.append(record)

        log.info("Fetched %d city-days for %d locations, %s to %s.",
                 len(records), len(locations), start, end)

        batch_id = safe_name(dag_run.run_id)
        load_date = str(today)
        staged = staging.write_records(
            records,
            source=SOURCE,
            entity=ENTITY,
            batch_id=batch_id,
            load_date=load_date,
        )
        # Passed to the next task so it promotes exactly this batch.
        return {**staged, "batch_id": batch_id, "load_date": load_date}

    @task(outlets=[BRONZE_OPEN_METEO_DAILY])
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


open_meteo_daily_to_bronze()
