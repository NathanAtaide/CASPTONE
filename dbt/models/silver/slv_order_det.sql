{#
  slv_order_det - SILVER

  Order lines (sales and returns), from the source table parfois.order_det:
    * PROD_QTY (1.0 / -1.0) becomes INTEGER (rule 1.5);
    * MOV_DATE_SFK (number YYYYMMDD) becomes the DATE mov_date (rule 1.5), plus the BIGINT
      mov_date_key to relate to slv_calendar.date_key;
    * the two AMOUNT_* columns become DECIMAL(18,5), so sums do not accumulate the rounding
      error of DOUBLE (rule 1.5);
    * rows equal in every column are one row repeated by the source: one copy is kept. The
      comparison uses the raw values, before any cast (rule 2.2.1).

  Written to s3://<silver bucket>/slv_order_det.parquet
#}

with latest as (

    -- Bronze is append-only: keep only the rows of the latest load of the source table.
    select *
    from {{ latest_load(source('bronze_mysql', 'order_det')) }} as bronze

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
