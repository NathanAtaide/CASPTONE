{#
  slv_customers - SILVER

  One row per customer of the source table parfois.customers, typed:
    * the text timestamps CREATION_DATE, SUBSCRIPTION_DATE and UNSUBSCRIPTION_DATE are split
      into a DATE and a TIME column each; the time is null when the source holds only the date
      (rule 1.1);
    * BIRTHDAY, a number in the YYYYMMDD format (19850312.0), becomes a DATE (rule 1.1);
    * 'NaN' and blank texts in COUNTRY and SUBSCRIPTION_SOURCE become null (rule 1.1);
    * each date has a BIGINT key YYYYMMDD (*_date_key) to relate to slv_calendar.date_key.
      BIRTHDAY has none: it is an attribute, not a date to analyse on the calendar;
    * customers whose CREATION_DATE, BIRTHDAY, UNSUBSCRIPTION_DATE, SUBSCRIPTION_DATE,
      SUBSCRIPTION_SOURCE and COUNTRY are all null are empty records and are dropped
      (rule 2.7.1, macro customer_has_data). slv_emails and slv_sessions use the same macro.

  Written to s3://<silver bucket>/slv_customers.parquet
#}

with latest as (

    -- Bronze is append-only: keep only the rows of the latest load of the source table.
    select *
    from {{ latest_load(source('bronze_mysql', 'customers')) }} as bronze

)

select
    cast(latest.customer_id as bigint)                      as customer_id,
    {{ text_to_date('latest.creation_date') }}              as creation_date,
    {{ text_to_time('latest.creation_date') }}              as creation_time,
    {{ date_to_key(text_to_date('latest.creation_date')) }} as creation_date_key,
    {{ text_to_date('latest.subscription_date') }}          as subscription_date,
    {{ text_to_time('latest.subscription_date') }}          as subscription_time,
    {{ date_to_key(text_to_date('latest.subscription_date')) }} as subscription_date_key,
    {{ text_to_date('latest.unsubscription_date') }}        as unsubscription_date,
    {{ text_to_time('latest.unsubscription_date') }}        as unsubscription_time,
    {{ date_to_key(text_to_date('latest.unsubscription_date')) }} as unsubscription_date_key,
    cast({{ clean_text('latest.country') }} as varchar)     as country,
    {{ yyyymmdd_to_date('latest.birthday') }}               as birthday,
    cast({{ clean_text('latest.subscription_source') }} as varchar) as subscription_source,
    cast(latest.email_domain as varchar)                    as email_domain,
    latest._ingested_at,
    latest._batch_id
from latest
where latest.customer_id is not null
  -- Rule 2.7.1: drop empty records (all six descriptive columns null).
  and {{ customer_has_data('latest') }}
