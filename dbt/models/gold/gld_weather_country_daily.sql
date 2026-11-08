{#
  gld_weather_country_daily - GOLD

  One row per marketplace country and day: the simple average of the daily
  weather of the three largest cities of the country (slv_weather_daily).
  Ready to be joined with the daily online sales by country_code and date.

  Notes for the join with the sales data:
    * country_code is ISO 3166-1 alpha-2: the marketplace "Online UK" is GB;
    * the most recent days can still be missing in the source archive;
      cities_with_data shows how many cities fed each average.

  Written to s3://<gold bucket>/gld_weather_country_daily.parquet and exported
  to ./data/exports/powerbi/ by the gold_export_powerbi DAG.
#}

select
    country_code,
    weather_date,
    count(temperature_mean_c)                 as cities_with_data,
    round(avg(temperature_max_c), 2)          as avg_temperature_max_c,
    round(avg(temperature_min_c), 2)          as avg_temperature_min_c,
    round(avg(temperature_mean_c), 2)         as avg_temperature_mean_c,
    round(avg(precipitation_mm), 2)           as avg_precipitation_mm,
    round(avg(rain_mm), 2)                    as avg_rain_mm,
    round(avg(snowfall_cm), 2)                as avg_snowfall_cm,
    round(avg(precipitation_hours), 2)        as avg_precipitation_hours,
    round(avg(sunshine_hours), 2)             as avg_sunshine_hours
from {{ ref('slv_weather_daily') }}
group by
    country_code,
    weather_date
order by
    country_code,
    weather_date
