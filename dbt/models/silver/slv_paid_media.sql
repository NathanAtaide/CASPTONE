{#
  slv_paid_media - SILVER

  Paid media spend per marketplace and day, from the source table parfois.paid_media:
    * DATE_SFK (number YYYYMMDD) becomes the DATE paid_media_date (rule 1.6), plus the BIGINT
      paid_media_date_key to relate to slv_calendar.date_key;
    * AMOUNT_PAID_MEDIA becomes DECIMAL(18,3) (rule 1.6).

  Written to s3://<silver bucket>/slv_paid_media.parquet
#}

with latest as (

    -- Bronze is append-only: keep only the rows of the latest load of the source table.
    select *
    from {{ latest_load(source('bronze_mysql', 'paid_media')) }} as bronze

)

select
    {{ yyyymmdd_to_date('date_sfk') }}                as paid_media_date,
    {{ date_to_key(yyyymmdd_to_date('date_sfk')) }}   as paid_media_date_key,
    cast(marketplace as varchar)                      as marketplace,
    cast(amount_paid_media as decimal(18, 3))         as amount_paid_media,
    _ingested_at,
    _batch_id
from latest
