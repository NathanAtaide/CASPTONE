{#
  slv_order_det - SILVER

  Order lines (sales and returns), from the source table parfois.order_det:
    * PROD_QTY (1.0 / -1.0) becomes INTEGER (rule 1.5);
    * MOV_DATE_SFK (number YYYYMMDD) becomes the DATE mov_date (rule 1.5), plus the BIGINT
      mov_date_key to relate to slv_calendar.date_key;
    * the two AMOUNT_* columns become DECIMAL(18,5), so sums do not accumulate the rounding
      error of DOUBLE (rule 1.5);
    * rows equal in every column are one row repeated by the source: one copy is kept. The
      comparison uses the raw values, before any cast (rule 2.2.1);
    * returns made in 2024 of orders placed before 2024 are dropped: those orders are not in
      slv_order_cab (rule 2.1.2), so their returns would have no order header. Returns of
      orders placed from 2024 onwards are kept (rule 2.2.2).

  The order date check reads the order_cab bronze dataset (silver models read bronze only),
  with the same date parsing and the same "latest date wins" choice as slv_order_cab.

  Written to s3://<silver bucket>/slv_order_det.parquet
#}
{%- set min_order_date = '2024-01-01' -%}

with latest as (

    -- Bronze is append-only: keep only the rows of the latest load of the source table.
    select *
    from {{ latest_load(source('bronze_mysql', 'order_det')) }} as bronze

),

orders_before_2024 as (

    select trim(bronze.order_id) as order_id
    from {{ latest_load(source('bronze_mysql', 'order_cab')) }} as bronze
    where bronze.order_id is not null
    group by trim(bronze.order_id)
    having max({{ text_to_date('bronze.order_date') }}) < date '{{ min_order_date }}'

),

deduplicated as (

    -- GROUP BY treats nulls as equal, so this is exactly "drop the repeated rows".
    select
        type,
        order_id,
        prod_sfk,
        prod_qty,
        mov_date_sfk,
        amount_fixed_rate_w_disc_wo_vat,
        amount_fixed_rate_wo_disc_wo_vat,
        max(_ingested_at)                          as _ingested_at,
        arg_max(_batch_id, _ingested_at)           as _batch_id
    from latest
    group by
        type,
        order_id,
        prod_sfk,
        prod_qty,
        mov_date_sfk,
        amount_fixed_rate_w_disc_wo_vat,
        amount_fixed_rate_wo_disc_wo_vat

)

select
    cast(type as varchar)                                       as type,
    cast(trim(order_id) as varchar)                             as order_id,
    cast(prod_sfk as bigint)                                    as prod_sfk,
    cast(prod_qty as integer)                                   as prod_qty,
    {{ yyyymmdd_to_date('mov_date_sfk') }}                      as mov_date,
    {{ date_to_key(yyyymmdd_to_date('mov_date_sfk')) }}         as mov_date_key,
    cast(amount_fixed_rate_w_disc_wo_vat as decimal(18, 5))     as amount_fixed_rate_w_disc_wo_vat,
    cast(amount_fixed_rate_wo_disc_wo_vat as decimal(18, 5))    as amount_fixed_rate_wo_disc_wo_vat,
    _ingested_at,
    _batch_id
from deduplicated
-- Rule 2.2.2. coalesce(..., false): a null in a tested column must never drop a row by accident.
where not coalesce(
        upper(trim(deduplicated.type)) = 'RETURN'
        and year({{ yyyymmdd_to_date('deduplicated.mov_date_sfk') }}) = 2024
        and trim(deduplicated.order_id) in (select order_id from orders_before_2024),
        false
      )
