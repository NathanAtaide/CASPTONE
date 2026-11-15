{#
  slv_emails - SILVER

  E-mail sends per customer and day, from the union of the source tables parfois.emails,
  parfois.emails_1, ... (the table of each row is in _source_entity):
    * SEND_DATE_SFK (number YYYYMMDD) becomes the DATE send_date (rule 1.2), plus the BIGINT
      send_date_key to relate to slv_calendar.date_key;
    * OPENS, CLICKS and UNSUBS are counts: DOUBLE becomes INTEGER (rule 1.2);
    * only rows whose CUSTOMER_ID_FK exists in the customers table are kept; rows without a
      customer are dropped as well (rule 2.5.1).

  The customer check reads the customers bronze dataset (silver models read bronze only).
  The `customers` CTE applies the same rules as slv_customers (macro customer_has_data, rule
  2.7.1), so e-mails of customers dropped there are dropped here too.

  Written to s3://<silver bucket>/slv_emails.parquet

  DuckDB 1.5 workaround: the customer check is a join, and DuckDB pushes it into the bronze scan
  as a bloom filter. The emails dataset unions several tables (union_by_name), and that push
  fails with "Can't convert TableFilterType (BLOOM_FILTER) from global to local indexes". The
  hooks switch that optimization off for this model only. Remove them once DuckDB fixes it.
#}
{{ config(
    pre_hook="set disabled_optimizers = 'join_filter_pushdown'",
    post_hook="reset disabled_optimizers"
) }}

with latest as (

    -- Bronze is append-only: keep only the rows of the latest load of each source table.
    select *
    from {{ latest_load(source('bronze_mysql', 'emails')) }} as bronze

),

customers as (

    select distinct cast(customer_id as bigint) as customer_id
    from {{ latest_load(source('bronze_mysql', 'customers')) }} as bronze
    where customer_id is not null
      and {{ customer_has_data('bronze') }}

)

select
    cast(emails.customer_id_fk as bigint)          as customer_id_fk,
    {{ yyyymmdd_to_date('emails.send_date_sfk') }} as send_date,
    {{ date_to_key(yyyymmdd_to_date('emails.send_date_sfk')) }} as send_date_key,
    cast(emails.sent as integer)                   as sent,
    cast(emails.opens as integer)                  as opens,
    cast(emails.clicks as integer)                 as clicks,
    cast(emails.unsubs as integer)                 as unsubs,
    emails._source_entity,
    emails._ingested_at,
    emails._batch_id
from latest as emails
where emails.customer_id_fk in (select customer_id from customers)
