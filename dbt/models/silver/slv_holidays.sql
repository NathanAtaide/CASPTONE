{#
  slv_holidays - SILVER

  Public holidays of every marketplace country: one row per country, day and
  holiday. Made to be joined with the daily sales (country_code +
  holiday_date_key, or slv_calendar.date_key) to measure the effect of the
  holidays and of the days around them.

  Two bronze sources, loaded once a year by the public_holidays_to_bronze DAG:
    * bronze_nager_date.public_holidays - Nager.Date API, every country but Israel;
    * bronze_hebcal.israel_holidays     - Hebcal API, Israel only.

  Rules:
    * every load re-sends whole years, so for each country and year only the
      rows of the most recent batch are kept (latest_batch_by): a holiday the
      API drops or renames in a later load also leaves this model;
    * Nager.Date can send the same holiday twice on the same day (Switzerland:
      once as Public for some cantons, once as Observance for others). Those
      rows become one, with the regions and the types of all of them;
    * Israel: Hebcal sends every Jewish and modern holiday, and only the
      official days of rest are kept: the yom tov days (Rosh Hashana I and II,
      Yom Kippur, Sukkot I, Shmini Atzeret, Pesach I and VII, Shavuot) and
      Independence Day (Yom HaAtzma'ut). They are nationwide and of type Public;
    * rows without country, date or name are dropped.

  Written to s3://<silver bucket>/slv_holidays.parquet
#}

with nager as (

    -- Bronze is append-only: keep, per country and year, the most recent load.
    select *
    from {{ latest_batch_by(source('bronze_nager_date', 'public_holidays'), ['requested_country_code', 'requested_year']) }} as bronze

),

hebcal as (

    select *
    from {{ latest_batch_by(source('bronze_hebcal', 'israel_holidays'), ['requested_country_code', 'requested_year']) }} as bronze

),

nager_holidays as (

    -- One row per country, day and holiday name: repeated holidays are merged.
    -- list_distinct also drops the nulls; an empty list becomes '' and then null.
    select
        upper(trim("countryCode"))                                          as country_code,
        cast("date" as date)                                                as holiday_date,
        trim("name")                                                        as holiday_name,
        array_to_string(list_sort(list_distinct(list(trim("localName")))), ' / ') as local_name,
        bool_or(coalesce("global", false))                                  as is_nationwide,
        array_to_string(
            list_sort(list_distinct(flatten(list("counties") filter (where "counties" is not null)))), ', '
        )                                                                   as subdivision_codes,
        array_to_string(
            list_sort(list_distinct(flatten(list("types") filter (where "types" is not null)))), ', '
        )                                                                   as holiday_types,
        coalesce(bool_or(list_contains("types", 'Public')), false)          as is_public,
        min(try_cast("launchYear" as integer))                              as launch_year,
        'nager_date'                                                        as source_system,
        max(_ingested_at)                                                   as _ingested_at,
        max(_batch_id)                                                      as _batch_id
    from nager
    where "countryCode" is not null
      and "date" is not null
      and "name" is not null
    group by 1, 2, 3

),

hebcal_holidays as (

    -- Israel: only the official days of rest.
    select
        upper(trim(requested_country_code))                                 as country_code,
        cast("date" as date)                                                as holiday_date,
        -- "Rosh Hashana 5786" -> "Rosh Hashana": the Hebrew year is not part of the name.
        regexp_replace(trim(coalesce(title_orig, title)), ' [0-9]{4}$', '') as holiday_name,
        trim(hebrew)                                                        as local_name,
        true                                                                as is_nationwide,
        cast(null as varchar)                                               as subdivision_codes,
        'Public'                                                            as holiday_types,
        true                                                                as is_public,
        cast(null as integer)                                               as launch_year,
        'hebcal'                                                            as source_system,
        _ingested_at,
        _batch_id
    from hebcal
    where requested_country_code is not null
      and "date" is not null
      and title is not null
      and (
          coalesce(yomtov, false)                                           -- festival days of rest
          or (subcat = 'modern' and coalesce(title_orig, title) ilike 'Yom HaAtzma%')  -- Independence Day
      )

),

holidays as (

    select * from nager_holidays
    union all by name
    select * from hebcal_holidays

)

select
    cast(country_code || '-' || strftime(holiday_date, '%Y%m%d') || '-' || holiday_name as varchar)
                                                                            as holiday_id,
    cast(country_code as varchar)                                           as country_code,
    cast(holiday_date as date)                                              as holiday_date,
    {{ date_to_key('holiday_date') }}                                       as holiday_date_key,
    cast(year(holiday_date) as integer)                                     as holiday_year,
    cast(holiday_name as varchar)                                           as holiday_name,
    cast(nullif(local_name, '') as varchar)                                 as local_name,
    cast(is_nationwide as boolean)                                          as is_nationwide,
    cast(case when not is_nationwide then nullif(subdivision_codes, '') end as varchar)
                                                                            as subdivision_codes,
    cast(nullif(holiday_types, '') as varchar)                              as holiday_types,
    cast(is_public as boolean)                                              as is_public,
    cast(launch_year as integer)                                            as launch_year,
    cast(source_system as varchar)                                          as source_system,
    cast(_ingested_at as timestamp)                                         as _ingested_at,
    cast(_batch_id as varchar)                                              as _batch_id
from holidays
