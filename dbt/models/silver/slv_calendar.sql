{#
  slv_calendar - SILVER

  Date dimension (calendar) following the Kimball recommendations:
    * grain: exactly one row per calendar day, with no gaps between the first
      and the last day;
    * keys: `date_key` (BIGINT YYYYMMDD) is the classic Kimball smart key and
      the relationship key: the other silver models carry it in their
      *_date_key columns, and the gold layer / Power BI relate them on it.
      `calendar_date` (DATE) is the same day as a date, for SQL joins on dates;
    * every attribute is pre-computed (no date math needed downstream) and
      every label has a matching numeric column to sort by;
    * descriptive labels in English (day, month and quarter names);
    * no holidays and no business-day flags: the markets are in several
      countries, so a single national holiday list does not fit them (the
      Brazilian holidays this model carried before were removed).

  This model does not read the bronze layer: the rows are generated in DuckDB.
  The range is set with dbt vars (both inclusive):
    calendar_start_date - default 2020-01-01
    calendar_end_date   - default 31 December of (current year + 5)
  Example: dbt build --select slv_calendar --vars '{calendar_start_date: "2018-01-01"}'

  Written to s3://<silver bucket>/slv_calendar.parquet
#}
{%- set start_date = var('calendar_start_date', '2020-01-01') -%}
{%- set end_date = var('calendar_end_date', none) -%}

with date_spine as (

    -- One row per day between the configured bounds (both inclusive).
    select cast(generate_series as date) as calendar_date
    from generate_series(
        cast('{{ start_date }}' as date),
        {% if end_date -%}
        cast('{{ end_date }}' as date),
        {%- else -%}
        make_date(cast(year(current_date) as integer) + 5, 12, 31),
        {%- endif %}
        interval 1 day
    )

),

calendar as (

    select
        d.calendar_date,

        -- Day
        cast(day(d.calendar_date) as integer)                      as day_of_month,
        cast(isodow(d.calendar_date) as integer)                   as day_of_week,       -- ISO: 1 = Monday ... 7 = Sunday
        cast(dayofyear(d.calendar_date) as integer)                as day_of_year,

        -- ISO week
        cast(weekofyear(d.calendar_date) as integer)               as iso_week_of_year,
        cast(isoyear(d.calendar_date) as integer)                  as iso_year,
        -- isodow() returns BIGINT and DuckDB only has DATE +/- INTEGER: cast before the date math.
        cast(d.calendar_date - cast(isodow(d.calendar_date) - 1 as integer) as date) as week_start_date,
        cast(d.calendar_date + cast(7 - isodow(d.calendar_date) as integer) as date) as week_end_date,

        -- Month / quarter / semester / year
        cast(month(d.calendar_date) as integer)                    as month_number,
        cast(quarter(d.calendar_date) as integer)                  as quarter_number,
        cast(case when month(d.calendar_date) <= 6 then 1 else 2 end as integer) as semester_number,
        cast(year(d.calendar_date) as integer)                     as year_number
    from date_spine as d

),

enriched as (

    select
        *,
        day_of_week >= 6                                           as is_weekend
    from calendar

)

select
    -- Keys
    cast(calendar_date as date)                                                    as calendar_date,
    cast(year_number * 10000 + month_number * 100 + day_of_month as bigint)        as date_key,

    -- Day
    cast(day_of_month as integer)                                                  as day_of_month,
    cast(day_of_week as integer)                                                   as day_of_week,
    cast(['Monday', 'Tuesday', 'Wednesday', 'Thursday',
          'Friday', 'Saturday', 'Sunday'][day_of_week] as varchar)                    as day_of_week_name,
    cast(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'][day_of_week] as varchar) as day_of_week_short,
    cast(day_of_year as integer)                                                   as day_of_year,
    cast(strftime(calendar_date, '%d/%m/%Y') as varchar)                           as date_label,

    -- Week (ISO 8601: weeks start on Monday; week 1 holds the first Thursday)
    cast(iso_week_of_year as integer)                                              as iso_week_of_year,
    cast(iso_year as integer)                                                      as iso_year,
    cast(iso_year as varchar) || '-W' || lpad(cast(iso_week_of_year as varchar), 2, '0') as iso_year_week,
    cast(week_start_date as date)                                                  as week_start_date,
    cast(week_end_date as date)                                                    as week_end_date,

    -- Month
    cast(month_number as integer)                                                  as month_number,
    cast(['January', 'February', 'March', 'April', 'May', 'June', 'July',
          'August', 'September', 'October', 'November', 'December'][month_number] as varchar) as month_name,
    cast(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul',
          'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][month_number] as varchar)             as month_short,
    cast(year_number * 100 + month_number as integer)                              as year_month_number,
    cast(strftime(calendar_date, '%Y-%m') as varchar)                              as year_month,
    cast(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul',
          'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][month_number] || ' ' || cast(year_number as varchar) as varchar) as year_month_label,
    cast(make_date(year_number, month_number, 1) as date)                          as month_start_date,
    cast(last_day(calendar_date) as date)                                          as month_end_date,
    cast(day(last_day(calendar_date)) as integer)                                  as days_in_month,
    cast(day_of_month = 1 as boolean)                                              as is_month_start,
    cast(calendar_date = last_day(calendar_date) as boolean)                       as is_month_end,

    -- Quarter
    cast(quarter_number as integer)                                                as quarter_number,
    'Q' || cast(quarter_number as varchar)                                     as quarter_label,
    cast(year_number * 10 + quarter_number as integer)                             as year_quarter_number,
    cast(year_number as varchar) || '-Q' || cast(quarter_number as varchar)                       as year_quarter,
    cast(make_date(year_number, (quarter_number - 1) * 3 + 1, 1) as date)          as quarter_start_date,
    cast(last_day(make_date(year_number, quarter_number * 3, 1)) as date)          as quarter_end_date,

    -- Semester
    cast(semester_number as integer)                                               as semester_number,
    cast(year_number as varchar) || '-S' || cast(semester_number as varchar)                      as year_semester,

    -- Year
    cast(year_number as integer)                                                   as year_number,
    cast(make_date(year_number, 1, 1) as date)                                     as year_start_date,
    cast(make_date(year_number, 12, 31) as date)                                   as year_end_date,
    cast((year_number % 4 = 0 and year_number % 100 <> 0) or year_number % 400 = 0 as boolean) as is_leap_year,

    -- Weekend
    cast(is_weekend as boolean)                                                    as is_weekend
from enriched
