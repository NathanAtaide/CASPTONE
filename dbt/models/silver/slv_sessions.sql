{#
  slv_sessions - SILVER

  Web and app sessions, from the union of the source tables parfois.sessions_1,
  parfois.sessions_2, ... (the table of each row is in _source_entity). A session with more
  than one order has one row per order, so session_sfk alone is not unique.
    * DATE_SFK (number YYYYMMDD) becomes the DATE session_date (rule 1.8), plus the BIGINT
      session_date_key to relate to slv_calendar.date_key;
    * TIMESTAMP holds only the time of day ('13:01:41'): it becomes the TIME session_time,
      which also drops the reserved word as a column name (rule 1.8);
    * ORDER_ID arrives as DOUBLE (900004671511.0): it becomes text, like ORDER_ID in the
      order tables (rule 1.8);
    * sessions of logged-in users (CUSTOMER_ID_FK other than -1) whose customer does not exist
      in the customers table are dropped. Anonymous sessions (-1) are kept (rule 2.4.1).

  The customer check reads the customers bronze dataset (silver models read bronze only).
  The `customers` CTE applies the same rules as slv_customers (macro customer_has_data, rule
  2.7.1), so logged-in sessions of customers dropped there are dropped here too.

  Written to s3://<silver bucket>/slv_sessions.parquet
#}

with latest as (

    -- Bronze is append-only: keep only the rows of the latest load of each source table.
    select *
    from {{ latest_load(source('bronze_mysql', 'sessions')) }} as bronze

),

customers as (

    select distinct cast(customer_id as bigint) as customer_id
    from {{ latest_load(source('bronze_mysql', 'customers')) }} as bronze
    where customer_id is not null
      and {{ customer_has_data('bronze') }}

)

select
    {{ yyyymmdd_to_date('sessions.date_sfk') }}             as session_date,
    {{ date_to_key(yyyymmdd_to_date('sessions.date_sfk')) }} as session_date_key,
    try_cast(trim(cast(sessions."TIMESTAMP" as varchar)) as time) as session_time,
    cast(sessions.channel_desc as varchar)                  as channel_desc,
    cast(sessions.country_cod as varchar)                   as country_cod,
    cast(sessions.client_sfk as bigint)                     as client_sfk,
    cast(sessions.customer_id_fk as bigint)                 as customer_id_fk,
    cast(sessions.session_sfk as bigint)                    as session_sfk,
    cast(sessions.platform_desc as varchar)                 as platform_desc,
    cast(try_cast(sessions.order_id as bigint) as varchar)  as order_id,
    cast(sessions.user_type_sfk as bigint)                  as user_type_sfk,
    sessions._source_entity,
    sessions._ingested_at,
    sessions._batch_id
from latest as sessions
where sessions.customer_id_fk is null
   or sessions.customer_id_fk = -1
   or sessions.customer_id_fk in (select customer_id from customers)
