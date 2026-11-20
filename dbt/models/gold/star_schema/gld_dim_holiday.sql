{#
  gld_dim_holiday - GOLD (star schema dimension)

  Holidays per country: one row per holiday in a country on a date, nationwide or regional,
  from slv_holidays (Nager.Date API + Hebcal API for Israel).

    * Natural key holiday_id = country_code-YYYYMMDD-holiday_name, built in slv_holidays (it
      changes if the API renames the holiday).
    * Used only by gld_fact_event_paid_media.holiday_sk, matching the day and the country of the
      market (country_code = gld_dim_market.country_code). It is not related to gld_dim_date
      (no snowflake): holiday_date / holiday_date_key are attributes.
    * The surrogate key follows holiday_date, country_code and holiday_id, so the holidays of a
      new year are appended at the end.
    * Row 0 = no holiday (holiday_id 'No holiday', dates null).
    * SCD type 0. launch_year, source_system and the lineage columns stay in silver.

  Written to s3://<gold bucket>/gld_dim_holiday.parquet
#}

with holidays as (

    select
        row_number() over (order by holiday_date, country_code, holiday_id) as holiday_sk,
        holiday_id,
        country_code,
        is_nationwide,
        subdivision_codes,
        holiday_date,
        holiday_date_key,
        holiday_year,
        holiday_name,
        local_name,
        holiday_types,
        is_public
    from {{ ref('slv_holidays') }}

),

members as (

    select * from holidays

    union all by name

    -- Row 0 (no holiday). The columns not listed here are null.
    select
        0               as holiday_sk,
        'No holiday'    as holiday_id,
        'n/a'           as country_code,
        0               as holiday_date_key,
        0               as holiday_year,
        'No holiday'    as holiday_name

)

select
    cast(holiday_sk as integer)         as holiday_sk,
    cast(holiday_id as varchar)         as holiday_id,
    cast(country_code as varchar)       as country_code,
    cast(is_nationwide as boolean)      as is_nationwide,
    cast(subdivision_codes as varchar)  as subdivision_codes,
    cast(holiday_date as date)          as holiday_date,
    cast(holiday_date_key as integer)   as holiday_date_key,
    cast(holiday_year as integer)       as holiday_year,
    cast(holiday_name as varchar)       as holiday_name,
    cast(local_name as varchar)         as local_name,
    cast(holiday_types as varchar)      as holiday_types,
    cast(is_public as boolean)          as is_public
from members
