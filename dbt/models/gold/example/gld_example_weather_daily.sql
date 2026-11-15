{#
  gld_example_weather_daily - GOLD (example)

  Business-ready aggregate: one row per location and day with the indicators a
  dashboard needs. Built only from silver, never directly from bronze.

  Written to s3://<gold bucket>/gld_example_weather_daily.parquet and exported
  to ./data/exports/powerbi/ by the gold_export_powerbi DAG.
#}
{{ config(tags=['example']) }}

select
    location,
    cast(observed_at as date)                 as observation_date,
    count(*)                                  as hours_observed,
    round(avg(temperature_c), 2)              as avg_temperature_c,
    min(temperature_c)                        as min_temperature_c,
    max(temperature_c)                        as max_temperature_c,
    round(avg(relative_humidity_pct), 2)      as avg_relative_humidity_pct,
    round(sum(precipitation_mm), 2)           as total_precipitation_mm,
    max(wind_speed_kmh)                       as max_wind_speed_kmh
from {{ ref('slv_example_weather_hourly') }}
group by
    location,
    cast(observed_at as date)
order by
    location,
    observation_date
