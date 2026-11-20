"""
### public_holidays_to_bronze - public holidays APIs -> staging (Parquet) -> bronze

Brings, once a year, the **public holidays** of every marketplace country, so the
silver model `slv_holidays` can be joined with the daily sales to measure the
effect of the holidays and of the days around them.

Two free public APIs, no key required:

- [Nager.Date](https://date.nager.at), `GET /api/v3/PublicHolidays/{year}/{countryCode}`,
  for every country except Israel;
- [Hebcal](https://www.hebcal.com/home/195/jewish-calendar-rest-api) for Israel
  (`IL`), which Nager.Date does not cover (it answers HTTP 204). The call uses the
  Israeli schedule (`i=on`) with the major and modern holidays; `slv_holidays`
  keeps only the official days of rest.

1. `extract_nager_date` / `extract_hebcal` - call each API once per country and
   year and land the answers, untouched, as one Parquet file per API in
   `s3://staging/nager_date/public_holidays/load_date=.../` and
   `s3://staging/hebcal/israel_holidays/load_date=.../`;
2. `promote_to_bronze` - imports both files into the data lake
   (`s3://bronze/<api>/<dataset>/ingestion_date=.../`) adding the lineage
   columns, and updates the `lake_bronze_public_holidays` asset, which starts
   `dbt_medallion`.

**Rate limit:** the calls to an API are spaced by `request_interval_seconds`, and
an answer HTTP 429 (Too Many Requests) or 5xx is retried with exponential
backoff, honouring the `Retry-After` header. If the API still refuses, the task
fails and Airflow retries it 10 minutes later.

**Parameters:**

- `start_year` / `end_year` - years to load (both inclusive). Empty `start_year`
  = the year of the run; empty `end_year` = the year of the run (or `start_year`,
  if later);
- `countries` - ISO 3166-1 alpha-2 codes. Repeated codes are ignored and the
  marketplace code `UK` is read as `GB`;
- `request_interval_seconds` - pause between two calls to the same API.

A Nager.Date country/year without data is logged as a warning and skipped.

**Schedule:** yearly, on 1 January at 04:00 (`PLATFORM_TIMEZONE`): loads the
holidays of the year that starts.

**Backfill:** trigger manually with `start_year = 2024` (loads 2024 up to the
current year). Loading a year again is safe: `slv_holidays` keeps, for each
country and year, only the most recent load.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.sdk import Param, dag, task

from caps_assets import BRONZE_PUBLIC_HOLIDAYS

log = logging.getLogger(__name__)

# <source>/<entity> define the folder of each dataset in staging and bronze.
NAGER_SOURCE = "nager_date"
NAGER_ENTITY = "public_holidays"
HEBCAL_SOURCE = "hebcal"
HEBCAL_ENTITY = "israel_holidays"

NAGER_URL = "https://date.nager.at/api/v3/PublicHolidays/{year}/{country_code}"
HEBCAL_URL = "https://www.hebcal.com/hebcal"

# Israel is fetched from Hebcal: Nager.Date has no data for it.
HEBCAL_COUNTRY = "IL"

# Hebcal fields sent only on some items. They are always written (null when
# absent) so every bronze file has the same columns.
HEBCAL_OPTIONAL_FIELDS = ("subcat", "yomtov", "title_orig", "hebrew", "memo")

# Marketplace codes that differ from ISO 3166-1 alpha-2 ("Online UK" -> GB).
COUNTRY_ALIASES = {"UK": "GB"}

# Countries of the marketplaces in the source data (order_cab.MARK_COD), e.g.
# "Online PT", "ECI ES", "Zalando PL", "TRENDYOL TR".
DEFAULT_COUNTRIES = [
    "PT", "GB", "ES", "DE", "CH", "MX", "IL", "SK", "MA", "FI",
    "IT", "FR", "TR", "LU", "IE", "AT", "LV", "SI", "PL", "HU",
    "PE", "NL", "CZ", "BE", "EE", "DK", "SE", "CL", "LT",
]

USER_AGENT = "caps-lakehouse/1.0 (yearly public holidays ingestion)"


def _requested_countries(params: dict) -> list[str]:
    """Upper-case ISO codes of the run, aliases resolved and duplicates removed."""
    countries: list[str] = []
    for raw in params.get("countries") or DEFAULT_COUNTRIES:
        code = str(raw).strip().upper()
        code = COUNTRY_ALIASES.get(code, code)
        if code and code not in countries:
            countries.append(code)
    return countries


def _year_window(params: dict, dag_run) -> list[int]:
    """Years to load: the parameters, or the year of the run when they are empty.

    The year comes from `run_after` in PLATFORM_TIMEZONE, never from
    `logical_date`, which is None in manually triggered runs.
    """
    from lakehouse.naming import partition_date

    run_year = partition_date(getattr(dag_run, "run_after", None)).year
    start_year = int(params.get("start_year") or run_year)
    end_year = int(params.get("end_year") or max(start_year, run_year))
    if start_year > end_year:
        raise ValueError(f"start_year ({start_year}) is after end_year ({end_year}).")
    return list(range(start_year, end_year + 1))


def _http_session():
    """requests session that retries HTTP 429 and 5xx answers with exponential backoff.

    urllib3 waits the time given by the Retry-After header of a 429/503 answer;
    without the header it retries at once, then after 4, 8, 16, 32 and 64 seconds.
    After the last attempt the error answer is returned (raise_on_status=False),
    so raise_for_status() reports the real HTTP status.
    """
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    retry = Retry(
        total=6,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _stage(records: list[dict], *, source: str, entity: str, dag_run) -> dict:
    """Write the records of one API to the staging area as one Parquet file."""
    from lakehouse import staging
    from lakehouse.naming import partition_date, safe_name

    batch_id = safe_name(dag_run.run_id)
    load_date = str(partition_date(getattr(dag_run, "run_after", None)))
    staged = staging.write_records(
        records,
        source=source,
        entity=entity,
        batch_id=batch_id,
        load_date=load_date,
    )
    # Passed to promote_to_bronze so it promotes exactly this batch.
    return {**staged, "batch_id": batch_id, "load_date": load_date}


@dag(
    dag_id="public_holidays_to_bronze",
    description="Yearly public holidays (Nager.Date + Hebcal APIs) -> staging area (Parquet) -> bronze layer.",
    schedule="0 4 1 1 *",  # 04:00 on 1 January, in PLATFORM_TIMEZONE
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["api", "staging", "bronze"],
    doc_md=__doc__,
    params={
        "start_year": Param(
            None,
            type=["null", "integer"],
            minimum=1975,
            maximum=2100,
            description="First year to load. Empty = the year of the run. Backfill: 2024.",
        ),
        "end_year": Param(
            None,
            type=["null", "integer"],
            minimum=1975,
            maximum=2100,
            description="Last year to load. Empty = the year of the run (or start_year, if later).",
        ),
        "countries": Param(
            DEFAULT_COUNTRIES,
            type="array",
            items={"type": "string", "minLength": 2, "maxLength": 2},
            description="ISO 3166-1 alpha-2 country codes. IL is loaded from Hebcal, the others from Nager.Date.",
        ),
        "request_interval_seconds": Param(
            1.0,
            type="number",
            minimum=0,
            maximum=60,
            description="Pause between two calls to the same API (avoids HTTP 429 Too Many Requests).",
        ),
    },
)
def public_holidays_to_bronze():
    @task(retries=2, retry_delay=timedelta(minutes=10))
    def extract_nager_date(params: dict | None = None, dag_run=None) -> dict | None:
        """Call Nager.Date for every country and year and land the holidays in staging."""
        # Heavy imports stay inside the task so DAG parsing is fast.
        import time

        params = params or {}
        countries = [c for c in _requested_countries(params) if c != HEBCAL_COUNTRY]
        if not countries:
            log.info("No country to fetch from Nager.Date in this run.")
            return None
        years = _year_window(params, dag_run)
        interval = float(params.get("request_interval_seconds", 1.0))

        records: list[dict] = []
        skipped: list[str] = []
        calls = 0
        with _http_session() as session:
            for year in years:
                for country_code in countries:
                    if calls:
                        time.sleep(interval)  # space the calls: the API answers 429 to bursts
                    calls += 1
                    response = session.get(
                        NAGER_URL.format(year=year, country_code=country_code), timeout=60
                    )

                    # 204 = no holidays for that year, 404 = country not supported.
                    if response.status_code in (204, 404):
                        log.warning("Nager.Date returned no holidays for %s %s (HTTP %s); skipped.",
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
            raise RuntimeError("Nager.Date returned no holidays for any country/year requested.")
        log.info("Fetched %d holidays from Nager.Date for %d countries, %d-%d, in %d calls (skipped: %s).",
                 len(records), len(countries), years[0], years[-1], calls, skipped or "none")

        staged = _stage(records, source=NAGER_SOURCE, entity=NAGER_ENTITY, dag_run=dag_run)
        return {**staged, "skipped": skipped}

    @task(retries=2, retry_delay=timedelta(minutes=10))
    def extract_hebcal(params: dict | None = None, dag_run=None) -> dict | None:
        """Call Hebcal for Israel, once per year, and land the holidays in staging."""
        import time

        params = params or {}
        if HEBCAL_COUNTRY not in _requested_countries(params):
            log.info("%s is not requested in this run: Hebcal is not called.", HEBCAL_COUNTRY)
            return None
        years = _year_window(params, dag_run)
        interval = float(params.get("request_interval_seconds", 1.0))

        records: list[dict] = []
        with _http_session() as session:
            for index, year in enumerate(years):
                if index:
                    time.sleep(interval)
                response = session.get(
                    HEBCAL_URL,
                    params={
                        "v": "1",
                        "cfg": "json",
                        "year": str(year),  # Gregorian year
                        "month": "x",       # the whole year
                        "i": "on",          # Israeli schedule (one-day festivals)
                        "maj": "on",        # major holidays (Rosh Hashana, Pesach, ...)
                        "mod": "on",        # modern holidays (Yom HaAtzma'ut, ...)
                        "min": "off",
                        "nx": "off",
                        "mf": "off",
                        "ss": "off",
                        "s": "off",
                        "c": "off",
                        "lg": "s",          # English names, Sephardic transliteration
                    },
                    timeout=60,
                )
                response.raise_for_status()

                for item in response.json().get("items", []):
                    # Torah readings: irrelevant here and nested with a shape that varies by item.
                    item.pop("leyning", None)
                    records.append(
                        {
                            **{field: None for field in HEBCAL_OPTIONAL_FIELDS},
                            **item,
                            "requested_country_code": HEBCAL_COUNTRY,
                            "requested_year": year,
                        }
                    )

        if not records:
            raise RuntimeError("Hebcal returned no holidays for the years requested.")
        log.info("Fetched %d items from Hebcal for %s, %d-%d.",
                 len(records), HEBCAL_COUNTRY, years[0], years[-1])

        return _stage(records, source=HEBCAL_SOURCE, entity=HEBCAL_ENTITY, dag_run=dag_run)

    @task(outlets=[BRONZE_PUBLIC_HOLIDAYS])
    def promote_to_bronze(nager: dict | None, hebcal: dict | None) -> list[dict]:
        """Import the staged files into the bronze layer of the data lake."""
        from lakehouse import staging

        batches = [batch for batch in (nager, hebcal) if batch]
        if not batches:
            raise RuntimeError("No country was requested: there is nothing to promote.")
        return [
            staging.promote_to_bronze(
                source=batch["source"],
                entity=batch["entity"],
                batch_id=batch["batch_id"],
                load_date=batch["load_date"],
            )
            for batch in batches
        ]

    promote_to_bronze(extract_nager_date(), extract_hebcal())


public_holidays_to_bronze()
