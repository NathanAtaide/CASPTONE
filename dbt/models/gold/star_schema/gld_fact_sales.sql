{#
  gld_fact_sales - GOLD (star schema fact)

  Every sale, return and cancellation. Grain: one product version (prod_sfk) in one order
  (order_id) with one movement type (SALE, RETURN or CANCELLED).
  Primary key: order_id + prod_sk + type_sk. Scope: every order, market and channel.

  Load rules (spec 4.1):
    1. the movement date (MOV_DATE_SFK) does not reach gold, so the lines that share order +
       product + type (e.g. two returns on different days) are SUMMED to keep the key.
       n_source_lines keeps how many lines of slv_order_det formed the row (almost always 1);
    2. all the rows of an order share what comes from the header (order_id, order_date_key,
       customer_sk, mark_sk, origin_sk, is_registered, campaign_running_sk) and differ in what
       comes from the lines (prod_sk, type_sk and the 3 measures);
    3. quantities keep the source sign: 2 units = prod_qty 2; a return of 1 unit = -1.

  Key lookups:
    * prod_sk       - gld_dim_product: prod_spk = prod_sfk AND order date between valid_from and
                      valid_to (version valid on the order date). Inferred members cover the
                      products missing in slv_product;
    * type_sk       - gld_dim_type on type;
    * order_date_key- order_date of the header (YYYYMMDD), the only date of the fact;
    * customer_sk   - gld_dim_customer: customer_id = customer_id_fk AND order date between
                      valid_from and valid_to. customer_id_fk = -1 (franchise, no ID) -> 0;
    * mark_sk       - gld_dim_market on mark_cod;
    * origin_sk     - gld_dim_origin on order_origin + order_chanel_type;
    * is_registered - customer_type = 'Registered' at the moment of the order (describes the
                      order, not the customer);
    * campaign_running_sk - the flags of gld_fact_event_paid_media for mark_sk x order_date_key:
                      any amount_paid_media > 0, any event_sk <> 0, any NATIONWIDE holiday. No row
                      in that fact = 'No' (1). Unknown market or date = 0. A return keeps the
                      context of its order date, not of the day it came back.
  A line whose order has no header in slv_order_cab keeps its measures with every header key = 0
  (its product version is then the open one, valid_to 9999-12-31).

  Amounts are in EUR without VAT, DECIMAL(18,5) like silver, so the totals of the fact match
  slv_order_det to the cent (rounding each row to 2 decimals added ~730 EUR over 63.6 million).

  Written to s3://<gold bucket>/gld_fact_sales.parquet
#}

with lines as (

    select
        order_id,
        prod_sfk,
        type,
        sum(prod_qty)                           as prod_qty,
        sum(amount_fixed_rate_w_disc_wo_vat)    as amount_fixed_rate_w_disc_wo_vat,
        sum(amount_fixed_rate_wo_disc_wo_vat)   as amount_fixed_rate_wo_disc_wo_vat,
        count(*)                                as n_source_lines
    from {{ ref('slv_order_det') }}
    group by
        order_id,
        prod_sfk,
        type

),

-- ---------------------------------------------------------------------------------------
-- Header keys, resolved once per order
-- ---------------------------------------------------------------------------------------

orders as (

    select
        cab.order_id,
        cab.order_date,
        dates.date_key                          as order_date_key,
        cab.customer_id_fk,
        coalesce(cab.customer_type = 'Registered', false)   as is_registered,
        coalesce(markets.mark_sk, 0)            as mark_sk,
        coalesce(origins.origin_sk, 0)          as origin_sk
    from {{ ref('slv_order_cab') }} as cab
    left join {{ ref('gld_dim_date') }} as dates
        on  dates.date_key = cab.order_date_key
        and dates.date_key <> 0
    left join {{ ref('gld_dim_market') }} as markets
        on markets.mark_cod = cab.mark_cod
    left join {{ ref('gld_dim_origin') }} as origins
        on  origins.order_origin = cab.order_origin
        and origins.order_chanel_type = cab.order_chanel_type

),

order_customers as (

    -- Version of the customer valid on the order date.
    select
        orders.order_id,
        customers.customer_sk
    from orders
    inner join {{ ref('gld_dim_customer') }} as customers
        on  customers.customer_id = orders.customer_id_fk
        and orders.order_date between customers.valid_from and customers.valid_to
    where orders.customer_id_fk <> -1
      and customers.customer_sk <> 0

),

market_day_context as (

    select
        context.date_key,
        context.mark_sk,
        bool_or(context.amount_paid_media > 0)                                      as has_paid_media,
        bool_or(context.event_sk <> 0)                                              as has_event,
        bool_or(context.holiday_sk <> 0 and coalesce(holidays.is_nationwide, false)) as is_holiday
    from {{ ref('gld_fact_event_paid_media') }} as context
    left join {{ ref('gld_dim_holiday') }} as holidays
        on holidays.holiday_sk = context.holiday_sk
    group by
        context.date_key,
        context.mark_sk

),

headers as (

    select
        orders.order_id,
        orders.order_date,
        coalesce(orders.order_date_key, 0)      as order_date_key,
        coalesce(order_customers.customer_sk, 0) as customer_sk,
        orders.mark_sk,
        orders.origin_sk,
        orders.is_registered,
        case
            when orders.mark_sk = 0 or orders.order_date_key is null then 0
            else coalesce(campaigns.campaign_running_sk, 0)
        end                                     as campaign_running_sk
    from orders
    left join order_customers
        on order_customers.order_id = orders.order_id
    left join market_day_context as context
        on  context.date_key = orders.order_date_key
        and context.mark_sk = orders.mark_sk
    left join {{ ref('gld_dim_campaign_running') }} as campaigns
        on  campaigns.has_paid_media = coalesce(context.has_paid_media, false)
        and campaigns.has_event = coalesce(context.has_event, false)
        and campaigns.is_holiday = coalesce(context.is_holiday, false)

)

-- ---------------------------------------------------------------------------------------
-- Final table: lines + header keys
-- ---------------------------------------------------------------------------------------

select
    cast(lines.order_id as varchar)                                     as order_id,
    cast(coalesce(products.prod_sk, 0) as integer)                      as prod_sk,
    cast(coalesce(types.type_sk, 0) as integer)                         as type_sk,
    cast(coalesce(headers.order_date_key, 0) as integer)                as order_date_key,
    cast(coalesce(headers.customer_sk, 0) as integer)                   as customer_sk,
    cast(coalesce(headers.mark_sk, 0) as integer)                       as mark_sk,
    cast(coalesce(headers.origin_sk, 0) as integer)                     as origin_sk,
    cast(coalesce(headers.is_registered, false) as boolean)             as is_registered,
    cast(coalesce(headers.campaign_running_sk, 0) as integer)           as campaign_running_sk,
    cast(coalesce(lines.prod_qty, 0) as integer)                        as prod_qty,
    cast(coalesce(lines.amount_fixed_rate_w_disc_wo_vat, 0) as decimal(18, 5))
                                                                        as amount_fixed_rate_w_disc_wo_vat,
    cast(coalesce(lines.amount_fixed_rate_wo_disc_wo_vat, 0) as decimal(18, 5))
                                                                        as amount_fixed_rate_wo_disc_wo_vat,
    cast(lines.n_source_lines as integer)                               as n_source_lines
from lines
left join headers
    on headers.order_id = lines.order_id
left join {{ ref('gld_dim_product') }} as products
    on  products.prod_spk = lines.prod_sfk
    and coalesce(headers.order_date, date '9999-12-31') between products.valid_from and products.valid_to
    and products.prod_sk <> 0
left join {{ ref('gld_dim_type') }} as types
    on types.type = lines.type
