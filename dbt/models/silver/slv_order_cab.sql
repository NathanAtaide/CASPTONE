{#
  slv_order_cab - SILVER

  One row per order (order header), from the source table parfois.order_cab:
    * ORDER_ID stays text: it is numeric for PARFOIS but alphanumeric for Franchise
      (e.g. Y67_906-1-R-7-263304_20251028) (rule 1.4);
    * the text timestamp ORDER_DATE is split into order_date (DATE) and order_time (TIME) (rule 1.4),
      plus the BIGINT order_date_key to relate to slv_calendar.date_key;
    * the source repeats some ORDER_IDs: one row per order is kept. The repeated rows seen so far
      are identical; if they ever differ, the row with a fulfillment status and then the latest
      date wins (rule 2.1.1);
    * orders placed before `min_order_date` are dropped, as are orders whose date cannot be
      read (rule 2.1.2).

  Written to s3://<silver bucket>/slv_order_cab.parquet
#}
{%- set min_order_date = '2024-01-01' -%}

with latest as (

    -- Bronze is append-only: keep only the rows of the latest load of the source table.
    select *
    from {{ latest_load(source('bronze_mysql', 'order_cab')) }} as bronze

),

typed as (

    select
        cast(trim(latest.order_id) as varchar)              as order_id,
        {{ text_to_date('latest.order_date') }}             as order_date,
        {{ text_to_time('latest.order_date') }}             as order_time,
        cast(latest.order_origin as varchar)                as order_origin,
        cast(latest.order_chanel_type as varchar)           as order_chanel_type,
        cast(latest.customer_type as varchar)               as customer_type,
        cast(latest.customer_id_fk as bigint)               as customer_id_fk,
        cast(latest.mark_cod as varchar)                    as mark_cod,
        cast(latest.fulfillment_status_id as varchar)       as fulfillment_status_id,
        latest._ingested_at,
        latest._batch_id
    from latest
    where latest.order_id is not null

),

deduplicated as (

    select *
    from typed
    qualify row_number() over (
        partition by order_id
        order by
            fulfillment_status_id is null,
            order_date desc nulls last,
            order_time desc nulls last,
            _ingested_at desc
    ) = 1

)

select
    order_id,
    order_date,
    order_time,
    {{ date_to_key('order_date') }}     as order_date_key,
    order_origin,
    order_chanel_type,
    customer_type,
    customer_id_fk,
    mark_cod,
    fulfillment_status_id,
    _ingested_at,
    _batch_id
from deduplicated
where order_date >= date '{{ min_order_date }}'
