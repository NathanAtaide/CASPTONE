{#
  gld_ml_sales - GOLD (machine learning training set)

  One row per day, market (mark_cod), channel (order_chanel_type) and product category
  (cat_des_en): the daily demand of PARFOIS sales, with calendar, price, paid media,
  autoregressive and campaign features for the forecasting models (units and orders).
  Not part of the star schema: built only to train ML / DL models.

  Scope:
    * orders with order_origin = 'PARFOIS' and order lines with type = 'SALE';
    * the 5 non-retail categories are left out (Informática, Marketing, Others, Consumable,
      Works); the other categories are mapped to 5 macro groups. A category outside the
      mapping keeps its rows with a null macro_cat_group (the not_null test warns about it);
    * lines without market, channel or category are left out: they have no series to go to.

  Date spine:
    * every market x channel x category that sold at least once gets one row per day, from its
      first sale up to the last order day in the data. Days without sales hold 0 in the
      targets, so the models see the zero-demand days;
    * the lags and rolling windows are computed on the complete spine (from the first order
      day of the data), so "t-1" is always the previous calendar day and the days before the
      first sale of a series count as zero sales. That is why the windows use ROWS frames;
    * a rolling feature is null until its window is complete (the first 7 or 30 days of data).

  Features (t = order_date):
    * avg_unit_base_price - gross revenue / units on days with sales; on zero-sale days, the
      last known price of the series;
    * daily_market_paid_media - spend of the market (slv_paid_media.marketplace = mark_cod)
      on day t, repeated across channels and categories. 0 when the market spent nothing that
      day; null outside the dates covered by slv_paid_media. Rolling average over t-6..t;
    * qty_sold_lag_1d / _7d - units at t-1 / t-7; rolling 7-day averages over t-7..t-1;
    * cat_share_market_rolling_30d - units of the category in the market (all channels) over
      t-30..t-1, divided by all units of the market over the same days;
    * events - the slv_events rows of the market plus the rows of market 'All'. Each event
      name becomes the key of one of the 33 is_* flags of the profiling (lower case, accents
      removed, '%' -> 'pct', any other symbol -> '_'): 'Black Friday App 20% all + 50% selected'
      -> black_friday_app_20pct_all_50pct_selected. Names outside the 33 flags are ignored.
      Several flags on the same day:
        - macro_event_type: the holiday with the highest urgency; on a tie, the first in
          BLACK_FRIDAY, CHRISTMAS, REYES, MOTHERS_DAY, VALENTINES, EASTER, SINGLES_DAY;
        - general_campaign_type: PRICE_PROMO_CLEARANCE > BASKET_OR_SHIPPING > SEASONAL_COLLECTION;
        - promo_urgency_level: the highest urgency of the active flags (0 when none);
        - active_campaigns_count: number of distinct flags active that day.

  Written to s3://<gold bucket>/gld_ml_sales.parquet, ordered by order_date.
#}

with

-- ---------------------------------------------------------------------------------------
-- Mappings from the profiling document
-- ---------------------------------------------------------------------------------------

category_groups as (

    select *
    from (
        values
            ('Stainless Steel',  'JEWELLERY_AND_WATCHES'),
            ('Watches',          'JEWELLERY_AND_WATCHES'),
            ('Jewellery',        'JEWELLERY_AND_WATCHES'),
            ('Silver',           'JEWELLERY_AND_WATCHES'),
            ('Wallet',           'BAGS_AND_SMALL_GOODS'),
            ('Belts',            'BAGS_AND_SMALL_GOODS'),
            ('Fragrances',       'BAGS_AND_SMALL_GOODS'),
            ('Hand Bag',         'BAGS_AND_SMALL_GOODS'),
            ('Waterproof',       'SUMMER_AND_TRAVEL'),
            ('Travel',           'SUMMER_AND_TRAVEL'),
            ('Sunglasses',       'SUMMER_AND_TRAVEL'),
            ('Scarves',          'WINTER_AND_APPAREL'),
            ('Hats',             'WINTER_AND_APPAREL'),
            ('Apparel',          'WINTER_AND_APPAREL'),
            ('Footwear',         'WINTER_AND_APPAREL'),
            ('Winter Textiles',  'WINTER_AND_APPAREL'),
            ('Night',            'PARTY_AND_LIFESTYLE'),
            ('Home',             'PARTY_AND_LIFESTYLE'),
            ('Hair Accessories', 'PARTY_AND_LIFESTYLE'),
            ('Party',            'PARTY_AND_LIFESTYLE')
    ) as t (cat_des_en, macro_cat_group)

),

event_flags as (

    -- The 33 is_* flags (table 4.2 of the profiling), keyed without the "is_" prefix.
    select *
    from (
        values
            ('black_friday',                                        'BLACK_FRIDAY', 'NONE',                  2),
            ('black_friday_get_your_basket_ready',                  'BLACK_FRIDAY', 'NONE',                  1),
            ('black_friday_app_20pct_all_50pct_selected',           'BLACK_FRIDAY', 'NONE',                  2),
            ('black_friday_store_web_app_20pct_all_50pct_selected', 'BLACK_FRIDAY', 'NONE',                  2),
            ('black_friday_web_app_20pct_all_50pct_selected',       'BLACK_FRIDAY', 'NONE',                  2),
            ('black_friday_store_web_app_50pct_selected',           'BLACK_FRIDAY', 'NONE',                  2),
            ('gift_guide',                                          'CHRISTMAS',    'NONE',                  1),
            ('last_days_christmas',                                 'CHRISTMAS',    'NONE',                  3),
            ('last_hours_christmas',                                'CHRISTMAS',    'NONE',                  4),
            ('last_days_reyes',                                     'REYES',        'NONE',                  3),
            ('last_hours_reyes',                                    'REYES',        'NONE',                  4),
            ('last_days_mothers_day',                               'MOTHERS_DAY',  'NONE',                  3),
            ('last_hours_mothers_day',                              'MOTHERS_DAY',  'NONE',                  4),
            ('last_days_valentines',                                'VALENTINES',   'NONE',                  3),
            ('last_hours_valentines',                               'VALENTINES',   'NONE',                  4),
            ('easter_weeks',                                        'EASTER',       'NONE',                  2),
            ('holy_week',                                           'EASTER',       'NONE',                  2),
            ('free_shipping_singles_day',                           'SINGLES_DAY',  'BASKET_OR_SHIPPING',    2),
            ('free_shipping',                                       'NONE',         'BASKET_OR_SHIPPING',    2),
            ('get_your_basket_ready',                               'NONE',         'BASKET_OR_SHIPPING',    1),
            ('avant_premiere',                                      'NONE',         'SEASONAL_COLLECTION',   1),
            ('capsule',                                             'NONE',         'SEASONAL_COLLECTION',   2),
            ('campaign_ss',                                         'NONE',         'SEASONAL_COLLECTION',   2),
            ('refresh_campaign_ss',                                 'NONE',         'SEASONAL_COLLECTION',   2),
            ('campaign_fw',                                         'NONE',         'SEASONAL_COLLECTION',   2),
            ('refresh_campaign_fw',                                 'NONE',         'SEASONAL_COLLECTION',   2),
            ('back_to_work',                                        'NONE',         'SEASONAL_COLLECTION',   2),
            ('special_prices_ss',                                   'NONE',         'PRICE_PROMO_CLEARANCE', 2),
            ('special_prices_fw',                                   'NONE',         'PRICE_PROMO_CLEARANCE', 2),
            ('petit_prix',                                          'NONE',         'PRICE_PROMO_CLEARANCE', 2),
            ('clearance',                                           'NONE',         'PRICE_PROMO_CLEARANCE', 2),
            ('clearance_extended',                                  'NONE',         'PRICE_PROMO_CLEARANCE', 3),
            ('stock_1',                                             'NONE',         'PRICE_PROMO_CLEARANCE', 3)
    ) as t (flag_key, macro_event_type, general_campaign_type, promo_urgency_level)

),

-- ---------------------------------------------------------------------------------------
-- Daily sales per market, channel and category
-- ---------------------------------------------------------------------------------------

sales_lines as (

    select
        cab.order_date,
        cab.mark_cod,
        cab.order_chanel_type,
        prod.cat_des_en,
        det.order_id,
        det.prod_qty,
        det.amount_fixed_rate_wo_disc_wo_vat,
        det.amount_fixed_rate_w_disc_wo_vat
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

daily_sales as (

    select
        order_date,
        mark_cod,
        order_chanel_type,
        cat_des_en,
        cast(sum(prod_qty) as integer)                      as qty_sold,
        cast(count(distinct order_id) as integer)           as orders_count,
        sum(amount_fixed_rate_wo_disc_wo_vat)               as gross_revenue,
        sum(amount_fixed_rate_w_disc_wo_vat)                as net_revenue
    from sales_lines
    group by
        order_date,
        mark_cod,
        order_chanel_type,
        cat_des_en

),

-- ---------------------------------------------------------------------------------------
-- Date spine: every series gets every day of the data range
-- ---------------------------------------------------------------------------------------

date_range as (

    select
        min(order_date) as first_date,
        max(order_date) as last_date
    from daily_sales

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
        cat_des_en,
        min(order_date) as first_sale_date
    from daily_sales
    group by
        mark_cod,
        order_chanel_type,
        cat_des_en

),

markets as (

    select distinct mark_cod
    from series

),

grid as (

    select
        spine.calendar_date                             as order_date,
        spine.year_number,
        spine.month_number,
        spine.day_of_week,
        spine.is_weekend,
        series.mark_cod,
        series.order_chanel_type,
        series.cat_des_en,
        series.first_sale_date,
        coalesce(sales.qty_sold, 0)                     as qty_sold,
        coalesce(sales.orders_count, 0)                 as orders_count,
        coalesce(sales.gross_revenue, 0)                as gross_revenue,
        coalesce(sales.net_revenue, 0)                  as net_revenue,
        -- Price of the day; null on zero-sale days (filled in series_features).
        sales.gross_revenue / nullif(sales.qty_sold, 0) as day_unit_base_price
    from series
    cross join date_spine as spine
    left join daily_sales as sales
        on  sales.order_date = spine.calendar_date
        and sales.mark_cod = series.mark_cod
        and sales.order_chanel_type = series.order_chanel_type
        and sales.cat_des_en = series.cat_des_en

),

-- ---------------------------------------------------------------------------------------
-- Autoregressive features of each series (complete spine: one row = one day)
-- ---------------------------------------------------------------------------------------

series_features as (

    select
        grid.*,
        lag(qty_sold, 1) over w_series                          as qty_sold_lag_1d,
        lag(qty_sold, 7) over w_series                          as qty_sold_lag_7d,
        case when count(*) over w_prev_7d = 7
            then avg(qty_sold) over w_prev_7d end               as qty_sold_rolling_7d_avg,
        case when count(*) over w_prev_7d = 7
            then avg(orders_count) over w_prev_7d end           as orders_count_rolling_7d_avg,
        coalesce(
            day_unit_base_price,
            last_value(day_unit_base_price ignore nulls) over w_past
        )                                                       as avg_unit_base_price
    from grid
    window
        w_series as (
            partition by mark_cod, order_chanel_type, cat_des_en
            order by order_date
        ),
        w_prev_7d as (
            partition by mark_cod, order_chanel_type, cat_des_en
            order by order_date
            rows between 7 preceding and 1 preceding
        ),
        w_past as (
            partition by mark_cod, order_chanel_type, cat_des_en
            order by order_date
            rows between unbounded preceding and 1 preceding
        )

),

-- ---------------------------------------------------------------------------------------
-- Category share of the market over the 30 previous days
-- ---------------------------------------------------------------------------------------

market_daily as (

    select
        order_date,
        mark_cod,
        cast(sum(qty_sold) as bigint) as market_qty
    from grid
    group by
        order_date,
        mark_cod

),

category_market_daily as (

    select
        order_date,
        mark_cod,
        cat_des_en,
        cast(sum(qty_sold) as bigint) as category_qty
    from grid
    group by
        order_date,
        mark_cod,
        cat_des_en

),

market_rolling as (

    select
        order_date,
        mark_cod,
        sum(market_qty) over w_prev_30d     as market_qty_30d,
        count(*) over w_prev_30d            as days_30d
    from market_daily
    window w_prev_30d as (
        partition by mark_cod
        order by order_date
        rows between 30 preceding and 1 preceding
    )

),

category_rolling as (

    select
        order_date,
        mark_cod,
        cat_des_en,
        sum(category_qty) over w_prev_30d   as category_qty_30d
    from category_market_daily
    window w_prev_30d as (
        partition by mark_cod, cat_des_en
        order by order_date
        rows between 30 preceding and 1 preceding
    )

),

-- ---------------------------------------------------------------------------------------
-- Paid media per market and day
-- ---------------------------------------------------------------------------------------

paid_media_range as (

    select
        min(paid_media_date) as first_date,
        max(paid_media_date) as last_date
    from {{ ref('slv_paid_media') }}

),

paid_media_daily as (

    select
        paid_media_date,
        marketplace,
        sum(amount_paid_media) as amount_paid_media
    from {{ ref('slv_paid_media') }}
    where paid_media_date is not null
      and marketplace is not null
    group by
        paid_media_date,
        marketplace

),

market_paid_media as (

    select
        spine.calendar_date                                 as order_date,
        markets.mark_cod,
        case
            when spine.calendar_date between paid_media_range.first_date and paid_media_range.last_date
                then coalesce(pm.amount_paid_media, 0)
        end                                                 as daily_market_paid_media
    from markets
    cross join date_spine as spine
    cross join paid_media_range
    left join paid_media_daily as pm
        on  pm.marketplace = markets.mark_cod
        and pm.paid_media_date = spine.calendar_date

),

paid_media_features as (

    select
        order_date,
        mark_cod,
        daily_market_paid_media,
        case when count(daily_market_paid_media) over w_last_7d = 7
            then avg(daily_market_paid_media) over w_last_7d end   as paid_media_rolling_7d_avg
    from market_paid_media
    window w_last_7d as (
        partition by mark_cod
        order by order_date
        rows between 6 preceding and current row
    )

),

-- ---------------------------------------------------------------------------------------
-- Campaign events per market and day
-- ---------------------------------------------------------------------------------------

event_days as (

    select
        events.mark_cod     as event_market,
        spine.calendar_date as order_date,
        flags.flag_key,
        flags.macro_event_type,
        flags.general_campaign_type,
        flags.promo_urgency_level
    from {{ ref('slv_events') }} as events
    inner join event_flags as flags
        on flags.flag_key = trim(
            regexp_replace(
                strip_accents(lower(replace(events.event, '%', 'pct'))),
                '[^a-z0-9]+', '_', 'g'
            ),
            '_'
        )
    inner join date_spine as spine
        on spine.calendar_date between events.start_date and events.end_date

),

market_event_days as (

    -- Events of the market itself plus the events registered for every market ('All').
    select distinct
        markets.mark_cod,
        event_days.order_date,
        event_days.flag_key,
        event_days.macro_event_type,
        event_days.general_campaign_type,
        event_days.promo_urgency_level,
        case event_days.macro_event_type
            when 'BLACK_FRIDAY' then 7
            when 'CHRISTMAS'    then 6
            when 'REYES'        then 5
            when 'MOTHERS_DAY'  then 4
            when 'VALENTINES'   then 3
            when 'EASTER'       then 2
            when 'SINGLES_DAY'  then 1
            else 0
        end                                     as holiday_priority,
        case event_days.general_campaign_type
            when 'PRICE_PROMO_CLEARANCE' then 3
            when 'BASKET_OR_SHIPPING'    then 2
            when 'SEASONAL_COLLECTION'   then 1
            else 0
        end                                     as campaign_priority
    from markets
    inner join event_days
        on event_days.event_market in (markets.mark_cod, 'All')

),

market_events as (

    select
        mark_cod,
        order_date,
        max_by(macro_event_type, promo_urgency_level * 10 + holiday_priority)
            filter (where holiday_priority > 0)                     as macro_event_type,
        max_by(general_campaign_type, campaign_priority)
            filter (where campaign_priority > 0)                    as general_campaign_type,
        max(promo_urgency_level)                                    as promo_urgency_level,
        count(distinct flag_key)                                    as active_campaigns_count
    from market_event_days
    group by
        mark_cod,
        order_date

)

-- ---------------------------------------------------------------------------------------
-- Final table: the 25 columns of the profiling
-- ---------------------------------------------------------------------------------------

select
    -- Granularity and categorization keys
    cast(sf.order_date as date)                                         as order_date,
    cast(sf.mark_cod as varchar)                                        as mark_cod,
    cast(sf.order_chanel_type as varchar)                               as order_chanel_type,
    cast(sf.cat_des_en as varchar)                                      as cat_des_en,
    cast(cg.macro_cat_group as varchar)                                 as macro_cat_group,

    -- Calendar
    cast(sf.year_number as integer)                                     as year,
    cast(sf.month_number as integer)                                    as month,
    cast(sf.day_of_week as integer)                                     as day_of_week,
    cast(case when sf.is_weekend then 1 else 0 end as integer)          as is_weekend,

    -- Targets and price context
    cast(sf.qty_sold as integer)                                        as total_prod_qty_sold,
    cast(sf.orders_count as integer)                                    as total_orders_count,
    cast(sf.gross_revenue as double)                                    as gross_revenue_wo_disc,
    cast(sf.net_revenue as double)                                      as net_revenue_w_disc,
    cast(sf.avg_unit_base_price as double)                              as avg_unit_base_price,

    -- Paid media, lags and category weight
    cast(pm.daily_market_paid_media as double)                          as daily_market_paid_media,
    cast(pm.paid_media_rolling_7d_avg as double)                        as paid_media_rolling_7d_avg,
    cast(sf.qty_sold_lag_1d as integer)                                 as qty_sold_lag_1d,
    cast(sf.qty_sold_lag_7d as integer)                                 as qty_sold_lag_7d,
    cast(sf.qty_sold_rolling_7d_avg as double)                          as qty_sold_rolling_7d_avg,
    cast(sf.orders_count_rolling_7d_avg as double)                      as orders_count_rolling_7d_avg,
    cast(case when mr.days_30d = 30
        then cr.category_qty_30d / nullif(mr.market_qty_30d, 0)
    end as double)                                                      as cat_share_market_rolling_30d,

    -- Campaign events
    cast(coalesce(ev.macro_event_type, 'NONE') as varchar)              as macro_event_type,
    cast(coalesce(ev.general_campaign_type, 'NONE') as varchar)         as general_campaign_type,
    cast(coalesce(ev.promo_urgency_level, 0) as integer)                as promo_urgency_level,
    cast(coalesce(ev.active_campaigns_count, 0) as integer)             as active_campaigns_count
from series_features as sf
left join category_groups as cg
    on cg.cat_des_en = sf.cat_des_en
left join paid_media_features as pm
    on  pm.order_date = sf.order_date
    and pm.mark_cod = sf.mark_cod
left join market_rolling as mr
    on  mr.order_date = sf.order_date
    and mr.mark_cod = sf.mark_cod
left join category_rolling as cr
    on  cr.order_date = sf.order_date
    and cr.mark_cod = sf.mark_cod
    and cr.cat_des_en = sf.cat_des_en
left join market_events as ev
    on  ev.order_date = sf.order_date
    and ev.mark_cod = sf.mark_cod
-- Rows before the first sale of a series only served the lags and windows.
where sf.order_date >= sf.first_sale_date
order by
    sf.order_date,
    sf.mark_cod,
    sf.order_chanel_type,
    sf.cat_des_en
