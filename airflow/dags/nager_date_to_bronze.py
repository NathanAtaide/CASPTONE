"""
### nager_date_to_bronze - public holidays API -> staging (Parquet) -> bronze

Brings the **public holidays** of the countries where the company sells online,
so the silver/gold models can flag holidays, bridges and pre-holiday days when
explaining and forecasting daily online sales per marketplace.

Source: [Nager.Date](https://date.nager.at) public API (free, no key required),
endpoint `GET /api/v3/PublicHolidays/{year}/{countryCode}`.

1. `extract_to_staging` - calls the API once per country and year and lands
   every holiday, untouched, as one Parquet file in
   `s3://staging/nager_date/public_holidays/load_date=.../`;
2. `promote_to_bronze` - imports that file into the data lake
   (`s3://bronze/nager_date/public_holidays/ingestion_date=.../`) adding the
   lineage columns, and updates the `lake_bronze_nager_date` asset, which
   starts `dbt_medallion`.

**Parameters:**

- `countries` - ISO 3166-1 alpha-2 codes. The marketplace codes of the source
  data use `UK` for the United Kingdom; the API (and ISO) use `GB`.
- `start_year` / `end_year` - the order history starts in 2020; future years are
  included because holidays are known in advance and feed the forecast.

A country/year the API does not cover is logged as a warning and skipped; the
run fails only if no holiday at all is returned.

**Schedule:** monthly. Holidays rarely change, so a monthly refresh is enough
to pick up corrections and the following year's calendar.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.sdk import Param, dag, task

from caps_assets import BRONZE_NAGER_DATE

log = logging.getLogger(__name__)

# <source>/<entity> define the folder of the dataset in staging and bronze.
SOURCE = "nager_date"
ENTITY = "public_holidays"

API_URL = "https://date.nager.at/api/v3/PublicHolidays/{year}/{country_code}"

# Countries of the marketplaces in the source data (order_cab.MARK_COD), e.g.
# "Online PT", "ECI ES", "Zalando PL", "TRENDYOL TR". "Online UK" maps to GB.
DEFAULT_COUNTRIES = [
    "PT", "ES", "FR", "PL", "IT", "GB", "DE", "TR", "IE", "NL",
    "HU", "BE", "IL", "CH", "AT", "MX", "LV", "CZ", "SE", "SI",
    "EE", "DK", "LT", "LU", "PE", "SK", "FI", "CL", "MA",
]


@dag(
    dag_id="nager_date_to_bronze",
    description="Public holidays (Nager.Date API) -> staging area (Parquet) -> bronze layer.",
    schedule="0 4 1 * *",  # 04:00 on the 1st of every month, in PLATFORM_TIMEZONE
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["api", "staging", "bronze"],
    doc_md=__doc__,
    params={
        "countries": Param(
            DEFAULT_COUNTRIES,
            type="array",
            items={"type": "string", "minLength": 2, "maxLength": 2},
            description="ISO 3166-1 alpha-2 country codes (use GB, not UK).",
        ),
        "start_year": Param(2020, type="integer", minimum=1975, maximum=2100),
        "end_year": Param(2027, type="integer", minimum=1975, maximum=2100),
    },
)
def nager_date_to_bronze():
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def extract_to_staging(params: dict | None = None, dag_run=None) -> dict:
        """Call the API for every country and year and land the holidays in staging."""
        # Heavy imports stay inside the task so DAG parsing is fast.
        import requests

        from lakehouse import staging
        from lakehouse.naming import partition_date, safe_name

        params = params or {}
        countries = [str(c).strip().upper() for c in params.get("countries", DEFAULT_COUNTRIES)]
        start_year = int(params.get("start_year", 2020))
        end_year = int(params.get("end_year", 2027))
        if start_year > end_year:
            raise ValueError(f"start_year ({start_year}) is after end_year ({end_year}).")

        records: list[dict] = []
        skipped: list[str] = []
        with requests.Session() as session:
            for country_code in countries:
                for year in range(start_year, end_year + 1):
                    url = API_URL.format(year=year, country_code=country_code)
                    response = session.get(url, timeout=60)

                    # 204 = no holidays for that year, 404 = country not supported.
                    if response.status_code in (204, 404):
                        log.warning("No holidays returned for %s %s (HTTP %s); skipped.",
                                    country_code, year, response.status_code)
                        skipped.append(f"{country_code}-{year}")
                        continue
                    response.raise_for_status()

                    for holiday in response.json():
                        # Keep the payload as returned; only record what was requested.
                        records.append(
                            {**holiday, "requested_country_code": country_code, "requested_year": year}
                        )

        if not records:
            raise RuntimeError("The API returned no holidays for any country/year requested.")
        log.info("Fetched %d holidays for %s, %d-%d (skipped: %s).",
                 len(records), ",".join(countries), start_year, end_year, skipped or "none")

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
        return {**staged, "batch_id": batch_id, "load_date": load_date, "skipped": skipped}

    @task(outlets=[BRONZE_NAGER_DATE])
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


nager_date_to_bronze()
