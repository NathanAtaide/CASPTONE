{#
  gld_dim_date - GOLD (star schema dimension, conformed)

  Date dimension shared by gld_fact_sales (order_date_key) and gld_fact_event_paid_media
  (date_key). One row per day between the dbt vars gold_date_start and gold_date_end (both
  inclusive, default 2020-01-01 -> 2026-12-31), plus row 0 = unknown date.

  Every attribute comes from slv_calendar, generated in the pipeline from calendar_date. Changes
  in relation to slv_calendar:
    * date_key is INTEGER (YYYYMMDD) and the small numbers are TINYINT / SMALLINT, as in the spec;
    * date_label is the ISO text YYYY-MM-DD (slv_calendar has DD/MM/YYYY).

  Row 0: date_key = 0, calendar_date and every other date / number are null, the texts are
  'Unknown'. The facts point to it when the date of a row is missing or outside the range.

  Written to s3://<gold bucket>/gld_dim_date.parquet
#}
{%- set date_start = var('gold_date_start', '2020-01-01') -%}
{%- set date_end = var('gold_date_end', '2026-12-31') -%}

with calendar as (

    select
        calendar_date,
        date_key,
        day_of_month,
        day_of_week,
        day_of_week_name,
        day_of_week_short,
        day_of_year,
        strftime(calendar_date, '%Y-%m-%d')     as date_label,
        iso_week_of_year,
        iso_year,
        iso_year_week,
        week_start_date,
        week_end_date,
        month_number,
        month_name,
        month_short,
        year_month_number,
        year_month,
        year_month_label,
        month_start_date,
        month_end_date,
        days_in_month,
        is_month_start,
        is_month_end,
        quarter_number,
        quarter_label,
        year_quarter_number,
        year_quarter,
        quarter_start_date,
        quarter_end_date,
        semester_number,
        year_semester,
        year_number,
        year_start_date,
        year_end_date,
        is_leap_year,
        is_weekend
    from {{ ref('slv_calendar') }}
    where calendar_date between date '{{ date_start }}' and date '{{ date_end }}'

),

members as (

    select * from calendar

    union all by name

    -- Row 0 (unknown date). The columns not listed here are null.
    select
        0               as date_key,
        'Unknown'       as day_of_week_name,
        'Unknown'       as day_of_week_short,
        'Unknown'       as date_label,
        'Unknown'       as iso_year_week,
        'Unknown'       as month_name,
        'Unknown'       as month_short,
        'Unknown'       as year_month,
        'Unknown'       as year_month_label,
        'Unknown'       as quarter_label,
        'Unknown'       as year_quarter,
        'Unknown'       as year_semester

)

select
    cast(calendar_date as date)             as calendar_date,
    cast(date_key as integer)               as date_key,
    cast(day_of_month as tinyint)           as day_of_month,
    cast(day_of_week as tinyint)            as day_of_week,
    cast(day_of_week_name as varchar)       as day_of_week_name,
    cast(day_of_week_short as varchar)      as day_of_week_short,
    cast(day_of_year as smallint)           as day_of_year,
    cast(date_label as varchar)             as date_label,
    cast(iso_week_of_year as tinyint)       as iso_week_of_year,
    cast(iso_year as smallint)              as iso_year,
    cast(iso_year_week as varchar)          as iso_year_week,
    cast(week_start_date as date)           as week_start_date,
    cast(week_end_date as date)             as week_end_date,
    cast(month_number as tinyint)           as month_number,
    cast(month_name as varchar)             as month_name,
    cast(month_short as varchar)            as month_short,
    cast(year_month_number as integer)      as year_month_number,
    cast(year_month as varchar)             as year_month,
    cast(year_month_label as varchar)       as year_month_label,
    cast(month_start_date as date)          as month_start_date,
    cast(month_end_date as date)            as month_end_date,
    cast(days_in_month as tinyint)          as days_in_month,
    cast(is_month_start as boolean)         as is_month_start,
    cast(is_month_end as boolean)           as is_month_end,
    cast(quarter_number as tinyint)         as quarter_number,
    cast(quarter_label as varchar)          as quarter_label,
    cast(year_quarter_number as integer)    as year_quarter_number,
    cast(year_quarter as varchar)           as year_quarter,
    cast(quarter_start_date as date)        as quarter_start_date,
    cast(quarter_end_date as date)          as quarter_end_date,
    cast(semester_number as tinyint)        as semester_number,
    cast(year_semester as varchar)          as year_semester,
    cast(year_number as smallint)           as year_number,
    cast(year_start_date as date)           as year_start_date,
    cast(year_end_date as date)             as year_end_date,
    cast(is_leap_year as boolean)           as is_leap_year,
    cast(is_weekend as boolean)             as is_weekend
from members
