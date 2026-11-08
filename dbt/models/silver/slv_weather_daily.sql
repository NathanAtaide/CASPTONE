{#
  slv_weather_daily - SILVER

  Daily weather per city (three largest cities of every marketplace country),
  from the Open-Meteo API:
    * keeps one row per (country, city, day): the most recently ingested
      version, because every run of the API DAG re-sends the last days and the
      archive fills recent days with a few days of delay;
    * gives the columns business names, units and explicit types
      (sunshine is converted from seconds to hours);
    * drops rows without a country, city or date.

  Written to s3://<silver bucket>/slv_weather_daily.parquet
#}

with latest as (

    -- Bronze is append-only: de-duplicate on the natural key of the dataset.
    select *
    from {{ latest_by_key(source('bronze_open_meteo', 'daily_weather'), ['country_code', 'city', 'date']) }} as bronze

)

select
    cast(upper(trim(country_code)) as varchar)  as country_code,
    cast(trim(city) as varchar)                 as city,
    cast(date as date)                          as weather_date,           -- local day of the city
    cast(latitude as double)                    as latitude,
    cast(longitude as double)                   as longitude,
    cast(temperature_2m_max as double)          as temperature_max_c,
    cast(temperature_2m_min as double)          as temperature_min_c,
    cast(temperature_2m_mean as double)         as temperature_mean_c,
    cast(precipitation_sum as double)           as precipitation_mm,
    cast(rain_sum as double)                    as rain_mm,
    cast(snowfall_sum as double)                as snowfall_cm,
    cast(precipitation_hours as double)         as precipitation_hours,
    cast(sunshine_duration as double) / 3600.0  as sunshine_hours,
    _ingested_at,
    _batch_id
from latest
where country_code is not null
  and city is not null
  and date is not null
