{#
  gld_fact_event_paid_media - GOLD (star schema fact)

  Marketing context per market and day: paid media spend, commercial events and holidays.
  Grain: one market x one day x one event x one holiday. A row exists when there is paid media,
  an event OR a holiday on that day and market (a day with an event or a holiday and no spend
  still gets its rows).
  Primary key: date_key + mark_sk + event_sk + holiday_sk.

  Load rules (spec 4.2):
    1. multi-day events: one row per day of the event, START_DATE to END_DATE (inclusive). A 4-day
       event gives 4 rows with the same event_sk;
    2. events of market 'All' (parfois.com) are spread over every Online market (mark_cod
       'Online ...'), except 'Online ND';
    3. holidays per market: a holiday belongs to a country and the fact needs a market, so each
       holiday gives one row for EVERY market of that country (gld_dim_holiday.country_code =
       gld_dim_market.country_code), regional holidays included. E.g. a holiday of Spain -> one
       row for Online ES and one for ECI ES. This link does not exist in the source;
    4. overlaps: two events, or two holidays, on the same market and day = two rows. With events
       AND holidays on the same day, every event is combined with every holiday;
    5. no double counting: amount_paid_media is written once per market x day, on the row with
       the lowest event_sk and then the lowest holiday_sk. The other rows get 0, so SUM is safe;
    6. absence: a day without event -> event_sk = 0; a day without holiday -> holiday_sk = 0;
    7. only days of gld_dim_date are kept (vars gold_date_start / gold_date_end); events missing
       in gld_dim_event (excluded on purpose, e.g. the Iberian blackout) are left out.

  It is also the source of gld_dim_campaign_running for gld_fact_sales.

  amount_paid_media is DECIMAL(18,5), the same type as the amounts of gld_fact_sales; it holds the
  DECIMAL(18,3) of slv_paid_media without rounding.

  Written to s3://<gold bucket>/gld_fact_event_paid_media.parquet
#}
{%- set all_markets_like = 'Online %' -%}
{%- set all_markets_excluded = ['Online ND'] -%}

with days as (

    select calendar_date, date_key
    from {{ ref('gld_dim_date') }}
    where date_key <> 0

),

markets as (

    select mark_sk, mark_cod, country_code
    from {{ ref('gld_dim_market') }}
    where mark_sk <> 0

),

-- ---------------------------------------------------------------------------------------
-- Paid media: one amount per market and day
-- ---------------------------------------------------------------------------------------

paid_media as (

    select
        days.date_key,
        coalesce(markets.mark_sk, 0)                    as mark_sk,
        sum(spend.amount_paid_media)                    as amount_paid_media
    from {{ ref('slv_paid_media') }} as spend
    inner join days
        on days.calendar_date = spend.paid_media_date
    left join markets
        on markets.mark_cod = spend.marketplace
    group by
        days.date_key,
        coalesce(markets.mark_sk, 0)

),

-- ---------------------------------------------------------------------------------------
-- Events: one row per day of the event and market
-- ---------------------------------------------------------------------------------------

events as (

    select
        dim_event.event_sk,
        source_events.mark_cod,
        source_events.start_date,
        source_events.end_date
    from {{ ref('slv_events') }} as source_events
    inner join {{ ref('gld_dim_event') }} as dim_event
        on dim_event.event = {{ event_key('source_events.event') }}
    where dim_event.event_sk <> 0

),

event_markets as (

    -- Events of one market.
    select
        events.event_sk,
        coalesce(markets.mark_sk, 0)    as mark_sk,
        events.start_date,
        events.end_date
    from events
    left join markets
        on markets.mark_cod = events.mark_cod
    where events.mark_cod <> 'All'

    union all

    -- Events of every market ('All'): one copy per Online market.
    select
        events.event_sk,
        markets.mark_sk,
        events.start_date,
        events.end_date
    from events
    cross join markets
    where events.mark_cod = 'All'
      and markets.mark_cod like '{{ all_markets_like }}'
      and markets.mark_cod not in ('{{ all_markets_excluded | join("', '") }}')

),

event_days as (

    -- distinct: the same event registered twice for a market and day is one row.
    select distinct
        days.date_key,
        event_markets.mark_sk,
        event_markets.event_sk
    from event_markets
    inner join days
        on days.calendar_date between event_markets.start_date and event_markets.end_date

),

-- ---------------------------------------------------------------------------------------
-- Holidays: one row per holiday and market of the holiday's country
-- ---------------------------------------------------------------------------------------

holiday_days as (

    select distinct
        days.date_key,
        markets.mark_sk,
        holidays.holiday_sk
    from {{ ref('gld_dim_holiday') }} as holidays
    inner join days
        on days.calendar_date = holidays.holiday_date
    inner join markets
        on markets.country_code = holidays.country_code
    where holidays.holiday_sk <> 0

),

-- ---------------------------------------------------------------------------------------
-- Grain: market x day x event x holiday
-- ---------------------------------------------------------------------------------------

market_days as (

    select date_key, mark_sk from paid_media
    union
    select date_key, mark_sk from event_days
    union
    select date_key, mark_sk from holiday_days

),

grid as (

    select
        market_days.date_key,
        market_days.mark_sk,
        coalesce(event_days.event_sk, 0)        as event_sk,
        coalesce(holiday_days.holiday_sk, 0)    as holiday_sk
    from market_days
    left join event_days
        on  event_days.date_key = market_days.date_key
        and event_days.mark_sk = market_days.mark_sk
    left join holiday_days
        on  holiday_days.date_key = market_days.date_key
        and holiday_days.mark_sk = market_days.mark_sk

)

select
    cast(grid.date_key as integer)      as date_key,
    cast(grid.mark_sk as integer)       as mark_sk,
    cast(grid.event_sk as integer)      as event_sk,
    cast(grid.holiday_sk as integer)    as holiday_sk,
    cast(
        case
            when row_number() over (
                partition by grid.date_key, grid.mark_sk
                order by grid.event_sk, grid.holiday_sk
            ) = 1
                then coalesce(paid_media.amount_paid_media, 0)
            else 0
        end as decimal(18, 5)
    )                                   as amount_paid_media
from grid
left join paid_media
    on  paid_media.date_key = grid.date_key
    and paid_media.mark_sk = grid.mark_sk
