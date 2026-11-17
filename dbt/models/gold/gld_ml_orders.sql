{#
  gld_ml_orders - GOLD (machine learning training set)

  One row per day, market (mark_cod) and channel (order_chanel_type): the number of distinct
  PARFOIS orders of the day, with calendar, paid media, autoregressive and campaign features,
  to forecast daily orders. Not part of the star schema: built only to train ML / DL models.

  Why a table of its own, without product categories: one order can hold products of several
  categories, so orders counted per category count it once per category (about +34% in Online
  ES and PT). Market, channel and date come from the order header, so here every order is
  counted exactly once and the rows add up to the real orders of a day or of a month.

  Scope: the same order lines as gld_ml_sales (order_origin = 'PARFOIS', type = 'SALE', a
  product of slv_product outside the 5 non-retail categories). An order counts on the day,
  market and channel of its header when it has at least one of those lines.

  Date spine: every market x channel that had at least one order gets one row per day, from its
  first order up to the last order day in the data; days without orders hold 0. As in
  gld_ml_sales, the lags and windows run on the complete spine (ROWS frames: one row = one
  day) and a rolling feature is null until its window is complete.

  Features (t = order_date):
    * orders_count_lag_1d / _7d - orders at t-1 / t-7; rolling 7-day average over t-7..t-1;
    * paid media and campaign events are per market and day, so they are read from
      gld_ml_sales (the same value in all its rows of that market and day). Both tables
      always carry the same values; the rules are documented in gld_ml_sales.sql.

  Written to s3://<gold bucket>/gld_ml_orders.parquet, ordered by order_date. The hooks keep
  the row order while this model is written (see gld_ml_sales.sql).
#}
{{ config(
    pre_hook="set preserve_insertion_order = true",
    post_hook="set preserve_insertion_order = false"
) }}

with

-- ---------------------------------------------------------------------------------------
-- Daily distinct orders per market and channel
-- ---------------------------------------------------------------------------------------

order_lines as (

    -- Same scope as gld_ml_sales: keep the joins and filters of both models in sync.
    select
        cab.order_date,
        cab.mark_cod,
        cab.order_chanel_type,
        det.order_id
    from {{ ref('slv_order_det') }} as det
    inner join {{ ref('slv_order_cab') }} as cab
        on cab.order_id = det.order_id
    inner join {{ ref('slv_product') }} as prod
        on prod.prod_spk = det.prod_sfk
    where upper(trim(cab.order_origin)) = 'PARFOIS'
      and upper(trim(det.type)) = 'SALE'
      and cab.order_date is not null
      and cab.mark_cod is not null
      and cab.order_chanel_type is not null
      and prod.cat_des_en is not null
      and prod.cat_des_en not in ('Informática', 'Marketing', 'Others', 'Consumable', 'Works')

),

daily_orders as (

    select
        order_date,
        mark_cod,
        order_chanel_type,
        cast(count(distinct order_id) as integer) as orders_count
    from order_lines
    group by
        order_date,
        mark_cod,
        order_chanel_type

),

-- ---------------------------------------------------------------------------------------
-- Date spine: every series gets every day of the data range
-- ---------------------------------------------------------------------------------------

date_range as (

    select
        min(order_date) as first_date,
        max(order_date) as last_date
    from daily_orders

),

date_spine as (

    select
        cal.calendar_date,
        cal.year_number,
        cal.month_number,
        cal.day_of_week,
        cal.is_weekend
    from {{ ref('slv_calendar') }} as cal
    inner join date_range
        on cal.calendar_date between date_range.first_date and date_range.last_date

),

series as (

    select
        mark_cod,
        order_chanel_type,
        min(order_date) as first_order_date
    from daily_orders
    group by
        mark_cod,
        order_chanel_type

),

grid as (

    select
        spine.calendar_date                 as order_date,
        spine.year_number,
        spine.month_number,
        spine.day_of_week,
        spine.is_weekend,
        series.mark_cod,
        series.order_chanel_type,
        series.first_order_date,
        coalesce(orders.orders_count, 0)    as orders_count
    from series
    cross join date_spine as spine
    left join daily_orders as orders
        on  orders.order_date = spine.calendar_date
        and orders.mark_cod = series.mark_cod
        and orders.order_chanel_type = series.order_chanel_type

),

-- ---------------------------------------------------------------------------------------
-- Autoregressive features of each series (complete spine: one row = one day)
-- ---------------------------------------------------------------------------------------

series_features as (

    select
        grid.*,
        lag(orders_count, 1) over w_series                      as orders_count_lag_1d,
        lag(orders_count, 7) over w_series                      as orders_count_lag_7d,
        case when count(*) over w_prev_7d = 7
            then avg(orders_count) over w_prev_7d end           as orders_count_rolling_7d_avg
    from grid
    window
        w_series as (
            partition by mark_cod, order_chanel_type
            order by order_date
        ),
        w_prev_7d as (
            partition by mark_cod, order_chanel_type
            order by order_date
            rows between 7 preceding and 1 preceding
        )

),

-- ---------------------------------------------------------------------------------------
-- Paid media and campaign features per market and day, from gld_ml_sales
-- ---------------------------------------------------------------------------------------

market_features as (

    -- One row per market and day (the group by guarantees the join cannot duplicate rows).
    select
        order_date,
        mark_cod,
        any_value(daily_market_paid_media)      as daily_market_paid_media,
        any_value(paid_media_rolling_7d_avg)    as paid_media_rolling_7d_avg,
        any_value(macro_event_type)             as macro_event_type,
        any_value(general_campaign_type)        as general_campaign_type,
        any_value(promo_urgency_level)          as promo_urgency_level,
        any_value(active_campaigns_count)       as active_campaigns_count
    from {{ ref('gld_ml_sales') }}
    group by
        order_date,
        mark_cod

)

-- ---------------------------------------------------------------------------------------
-- Final table
-- ---------------------------------------------------------------------------------------

select
    -- Granularity keys
    cast(sf.order_date as date)                                         as order_date,
    cast(sf.mark_cod as varchar)                                        as mark_cod,
    cast(sf.order_chanel_type as varchar)                               as order_chanel_type,

    -- Calendar
    cast(sf.year_number as integer)                                     as year,
    cast(sf.month_number as integer)                                    as month,
    cast(sf.day_of_week as integer)                                     as day_of_week,
    cast(case when sf.is_weekend then 1 else 0 end as integer)          as is_weekend,

    -- Target
    cast(sf.orders_count as integer)                                    as total_orders_count,

    -- Paid media and lags
    cast(mf.daily_market_paid_media as double)                          as daily_market_paid_media,
    cast(mf.paid_media_rolling_7d_avg as double)                        as paid_media_rolling_7d_avg,
    cast(sf.orders_count_lag_1d as integer)                             as orders_count_lag_1d,
    cast(sf.orders_count_lag_7d as integer)                             as orders_count_lag_7d,
    cast(sf.orders_count_rolling_7d_avg as double)                      as orders_count_rolling_7d_avg,

    -- Campaign events
    cast(coalesce(mf.macro_event_type, 'NONE') as varchar)              as macro_event_type,
    cast(coalesce(mf.general_campaign_type, 'NONE') as varchar)         as general_campaign_type,
    cast(coalesce(mf.promo_urgency_level, 0) as integer)                as promo_urgency_level,
    cast(coalesce(mf.active_campaigns_count, 0) as integer)             as active_campaigns_count
from series_features as sf
left join market_features as mf
    on  mf.order_date = sf.order_date
    and mf.mark_cod = sf.mark_cod
-- Rows before the first order of a series only served the lags and windows.
where sf.order_date >= sf.first_order_date
order by
    sf.order_date,
    sf.mark_cod,
    sf.order_chanel_type
