{#
  slv_example_weather_hourly - SILVER (example)

  Cleans the raw API data stored in bronze:
    * keeps one row per (location, hour): the most recently ingested version,
      because every run of the API DAG re-sends the last days;
    * gives the columns business names and explicit types;
    * drops rows without a timestamp.

  Written to s3://<silver bucket>/slv_example_weather_hourly.parquet
#}
{{ config(tags=['example']) }}

with latest as (

    -- Bronze is append-only: de-duplicate on the natural key of the dataset.
    select *
    from {{ latest_by_key(source('bronze_open_meteo', 'hourly_weather'), ['location', 'observed_at']) }} as bronze

)

select
    cast(location as varchar)                as location,
    cast(latitude as double)                 as latitude,
    cast(longitude as double)                as longitude,
    cast(observed_at as timestamp)           as observed_at,            -- UTC
    cast(temperature_2m as double)           as temperature_c,
    cast(relative_humidity_2m as double)     as relative_humidity_pct,
    cast(precipitation as double)            as precipitation_mm,
    cast(wind_speed_10m as double)           as wind_speed_kmh,
    _ingested_at,
    _batch_id
from latest
where observed_at is not null
