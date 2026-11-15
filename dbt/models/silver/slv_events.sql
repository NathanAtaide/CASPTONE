{#
  slv_events - SILVER

  One row per commercial event, market and period, from the source table parfois.events:
    * START_DATE and END_DATE become DATE (rule 1.3), plus the BIGINT keys start_date_key and
      end_date_key to relate to slv_calendar.date_key;
    * repeated inner spaces in EVENT are collapsed: 'Last Days  Christmas' becomes
      'Last Days Christmas' (rule 2.3.1);
    * 'Free Shipping Singles Day 20' is the same event as 'Free Shipping Singles Day' (rule 2.3.2);
    * a null MARK_COD means the event ran in every market: it becomes 'All' (rule 2.3.3);
    * an END_DATE before the START_DATE was typed with the wrong year: it is moved to the next
      year, e.g. 2025-12-26 -> 2025-01-06 becomes 2025-12-26 -> 2026-01-06 (rule 2.3.5);
    * the Christmas events of 2025 were registered for two markets only, by mistake. Every
      market of `christmas_2025_markets` that lacks a Christmas event in 2025 receives a copy
      of its own 2024 row with both dates moved one year ahead; the 2024 rows are kept.
      The copies have is_imputed = true (rule 2.3.4);
    * rows that become identical after these fixes are kept once (a source row wins over a copy).

  Written to s3://<silver bucket>/slv_events.parquet
#}
{%- set christmas_events = ['Last Days Christmas', 'Last Hours Christmas'] -%}
{%- set christmas_2025_markets = [
    'Online IT', 'Online FR', 'Online IE', 'Online AT', 'Online LU', 'Online LV',
    'Online SI', 'Online UK', 'Online DE', 'Online SK', 'Online CH', 'Online FI',
    'Online MX', 'Online BE', 'Online NL', 'Online CZ', 'Online DK', 'Online EE',
    'Online LT', 'Online SE', 'Online HU', 'Online PL'
] -%}

with latest as (

    -- Bronze is append-only: keep only the rows of the latest load of the source table.
    select *
    from {{ latest_load(source('bronze_mysql', 'events')) }} as bronze

),

typed as (

    select
        coalesce({{ clean_text('mark_cod') }}, 'All')                    as mark_cod,
        cast(mark_des as varchar)                                        as mark_des,
        regexp_replace(trim(cast(event as varchar)), '\s+', ' ', 'g')    as event,
        try_cast(trim(cast(start_date as varchar)) as date)              as start_date,
        try_cast(trim(cast(end_date as varchar)) as date)                as end_date,
        _ingested_at,
        _batch_id
    from latest

),

fixed as (

    select
        mark_cod,
        mark_des,
        case
            when event = 'Free Shipping Singles Day 20' then 'Free Shipping Singles Day'
            else event
        end                                                              as event,
        start_date,
        case
            when end_date < start_date then cast(end_date + interval 1 year as date)
            else end_date
        end                                                              as end_date,
        false                                                            as is_imputed,
        _ingested_at,
        _batch_id
    from typed

),

christmas_2025 as (

    select
        e2024.mark_cod,
        e2024.mark_des,
        e2024.event,
        cast(e2024.start_date + interval 1 year as date)                 as start_date,
        cast(e2024.end_date + interval 1 year as date)                   as end_date,
        true                                                             as is_imputed,
        e2024._ingested_at,
        e2024._batch_id
    from fixed as e2024
    where e2024.event in ('{{ christmas_events | join("', '") }}')
      and e2024.mark_cod in ('{{ christmas_2025_markets | join("', '") }}')
      and year(e2024.start_date) = 2024
      and not exists (
          select 1
          from fixed as e2025
          where e2025.mark_cod = e2024.mark_cod
            and e2025.event = e2024.event
            and year(e2025.start_date) = 2025
      )

),

unioned as (

    select * from fixed
    union all by name
    select * from christmas_2025

)

select
    cast(mark_cod as varchar)       as mark_cod,
    cast(mark_des as varchar)       as mark_des,
    cast(event as varchar)          as event,
    cast(start_date as date)        as start_date,
    {{ date_to_key('start_date') }} as start_date_key,
    cast(end_date as date)          as end_date,
    {{ date_to_key('end_date') }}   as end_date_key,
    cast(is_imputed as boolean)     as is_imputed,
    _ingested_at,
    _batch_id
from unioned
qualify row_number() over (
    partition by mark_cod, mark_des, event, start_date, end_date
    order by is_imputed, _ingested_at desc
) = 1
