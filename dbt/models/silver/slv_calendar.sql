{#
  slv_calendar - SILVER

  Date dimension (calendar) following the Kimball recommendations:
    * grain: exactly one row per calendar day, with no gaps between the first
      and the last day;
    * key: `calendar_date` (type DATE) is the column the other models join on.
      `date_key` (integer YYYYMMDD) is kept as the classic Kimball smart key
      for tools that prefer an integer key;
    * every attribute is pre-computed (no date math needed downstream) and
      every label has a matching numeric column to sort by;
    * descriptive labels in English (day, month, quarter and holiday names);
    * Brazilian national holidays (fixed by federal law and the Easter-based
      ones) with the business-day flags derived from them.

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

years as (

    select distinct cast(year(calendar_date) as integer) as year_number
    from date_spine

),

easter as (

    -- Easter Sunday per year (anonymous Gregorian algorithm, Meeus/Jones/Butcher).
    select
        year_number,
        make_date(
            year_number,
            (h + l - 7 * m + 114) // 31,
            ((h + l - 7 * m + 114) % 31) + 1
        ) as easter_sunday
    from (
        select
            year_number, a, h, l,
            (a + 11 * h + 22 * l) // 451 as m
        from (
            select
                year_number, a, h,
                (32 + 2 * e + 2 * (c // 4) - h - (c % 4)) % 7 as l
            from (
                select
                    year_number, a, c, e,
                    (19 * a + b - (b // 4) - ((b - ((b + 8) // 25) + 1) // 3) + 15) % 30 as h
                from (
                    select
                        year_number,
                        year_number % 19  as a,
                        year_number // 100 as b,
                        year_number % 100 as c,
                        (year_number // 100) % 4 as e
                    from years
                ) as step_1
            ) as step_2
        ) as step_3
    ) as step_4

),

holiday_list as (

    -- Fixed national holidays (Brazilian Laws 662/1949, 6.802/1980, 10.607/2002 and 14.759/2023).
    select make_date(y.year_number, f.month_number, f.day_number) as holiday_date,
           f.holiday_name,
           'national holiday' as holiday_type
    from years as y
    cross join (
        values
            (1,  1,  'New Year''s Day', 0),
            (4,  21, 'Tiradentes Day', 0),
            (5,  1,  'Labour Day', 0),
            (9,  7,  'Independence Day', 0),
            (10, 12, 'Our Lady of Aparecida', 0),
            (11, 2,  'All Souls'' Day', 0),
            (11, 15, 'Proclamation of the Republic', 0),
            (11, 20, 'Black Consciousness Day', 2024),
            (12, 25, 'Christmas Day', 0)
    ) as f(month_number, day_number, holiday_name, valid_from_year)
    where y.year_number >= f.valid_from_year

    union all

    -- Easter-based dates, classified as in the yearly federal government ordinance.
    select cast(e.easter_sunday + o.offset_days as date), o.holiday_name, o.holiday_type
    from easter as e
    cross join (
        values
            (-48, 'Carnival Monday',  'optional holiday'),
            (-47, 'Carnival Tuesday', 'optional holiday'),
            (-2,  'Good Friday',      'national holiday'),
            (60,  'Corpus Christi',   'optional holiday')
    ) as o(offset_days, holiday_name, holiday_type)

),

holidays as (

    -- Two holidays can fall on the same day (e.g. Good Friday on 21 April, Tiradentes Day):
    -- keep one row per date, the stronger type wins.
    select
        holiday_date,
        string_agg(holiday_name, ' / ' order by holiday_name) as holiday_name,
        case
            when bool_or(holiday_type = 'national holiday') then 'national holiday'
            else 'optional holiday'
        end as holiday_type
    from holiday_list
    group by holiday_date

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
        cast(d.calendar_date - (isodow(d.calendar_date) - 1) as date) as week_start_date,
        cast(d.calendar_date + (7 - isodow(d.calendar_date)) as date) as week_end_date,

        -- Month / quarter / semester / year
        cast(month(d.calendar_date) as integer)                    as month_number,
        cast(quarter(d.calendar_date) as integer)                  as quarter_number,
        cast(case when month(d.calendar_date) <= 6 then 1 else 2 end as integer) as semester_number,
        cast(year(d.calendar_date) as integer)                     as year_number,

        h.holiday_name,
        h.holiday_type
    from date_spine as d
    left join holidays as h
        on h.holiday_date = d.calendar_date

),

enriched as (

    select
        *,
        day_of_week >= 6                                           as is_weekend,
        coalesce(holiday_type = 'national holiday', false)         as is_holiday,
        coalesce(holiday_type = 'optional holiday', false)        as is_optional_holiday,
        -- Business day: weekday that is neither a national holiday nor an optional one.
        day_of_week <= 5 and holiday_type is null                  as is_business_day
    from calendar

)

select
    -- Keys
    cast(calendar_date as date)                                                    as calendar_date,
    cast(year_number * 10000 + month_number * 100 + day_of_month as integer)       as date_key,

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

    -- Weekend, holidays and business days
    cast(is_weekend as boolean)                                                    as is_weekend,
    cast(is_holiday as boolean)                                                    as is_holiday,
    cast(is_optional_holiday as boolean)                                           as is_optional_holiday,
    cast(holiday_name as varchar)                                                  as holiday_name,
    cast(holiday_type as varchar)                                                  as holiday_type,
    cast(is_business_day as boolean)                                               as is_business_day,
    cast(case when is_business_day then
        sum(case when is_business_day then 1 else 0 end)
            over (partition by year_number, month_number order by calendar_date)
    end as integer)                                                                as business_day_of_month,
    cast(sum(case when is_business_day then 1 else 0 end)
            over (partition by year_number, month_number) as integer)              as business_days_in_month
from enriched
