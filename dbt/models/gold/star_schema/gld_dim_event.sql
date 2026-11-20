{#
  gld_dim_event - GOLD (star schema dimension)

  Which commercial event was running: one row per event. START_DATE, END_DATE and MARK_COD of
  slv_events build the rows of gld_fact_event_paid_media and are not kept here.

    * event is the key of the event name (macro event_key): 'Black Friday App 20% all + 50%
      selected' -> 'black_friday_app_20pct_all_50pct_selected'. It is the same key gld_ml_sales
      uses for its 33 flags. ('Black Friday' -> 'black_friday', all in lower case.)
    * macro_event_type is the calendar moment the event belongs to (team grouping). NONE = an
      event not tied to the calendar (seasonal campaigns, free shipping, special prices,
      clearance). SCD type 1: changing the list below rewrites it.
    * The surrogate keys 0-33 are fixed (spec 5.8). An event that shows up later in slv_events
      gets the next free key with macro_event_type NONE, so no event is lost silently. Events of
      `excluded_events` are left out on purpose and never reach the fact.
    * Row 0 = no event / unknown (NO_EVENT).

  Written to s3://<gold bucket>/gld_dim_event.parquet
#}
{#- Not commercial events: the Iberian blackout of 2025-04-28 (Online PT and Online ES). -#}
{%- set excluded_events = ['apagao_iberico'] -%}

with fixed_events as (

    select *
    from (
        values
            (0,  'No event',                                             'NO_EVENT'),
            (1,  'black_friday_get_your_basket_ready',                   'BLACK_FRIDAY'),
            (2,  'black_friday',                                         'BLACK_FRIDAY'),
            (3,  'black_friday_app_20pct_all_50pct_selected',            'BLACK_FRIDAY'),
            (4,  'black_friday_store_web_app_20pct_all_50pct_selected',  'BLACK_FRIDAY'),
            (5,  'black_friday_web_app_20pct_all_50pct_selected',        'BLACK_FRIDAY'),
            (6,  'black_friday_store_web_app_50pct_selected',            'BLACK_FRIDAY'),
            (7,  'gift_guide',                                           'CHRISTMAS'),
            (8,  'last_days_christmas',                                  'CHRISTMAS'),
            (9,  'last_hours_christmas',                                 'CHRISTMAS'),
            (10, 'last_days_reyes',                                      'REYES'),
            (11, 'last_hours_reyes',                                     'REYES'),
            (12, 'last_days_mothers_day',                                'MOTHERS_DAY'),
            (13, 'last_hours_mothers_day',                               'MOTHERS_DAY'),
            (14, 'last_days_valentines',                                 'VALENTINES'),
            (15, 'last_hours_valentines',                                'VALENTINES'),
            (16, 'easter_weeks',                                         'EASTER'),
            (17, 'holy_week',                                            'EASTER'),
            (18, 'free_shipping_singles_day',                            'SINGLES_DAY'),
            (19, 'get_your_basket_ready',                                'NONE'),
            (20, 'free_shipping',                                        'NONE'),
            (21, 'avant_premiere',                                       'NONE'),
            (22, 'capsule',                                              'NONE'),
            (23, 'campaign_ss',                                          'NONE'),
            (24, 'refresh_campaign_ss',                                  'NONE'),
            (25, 'campaign_fw',                                          'NONE'),
            (26, 'refresh_campaign_fw',                                  'NONE'),
            (27, 'back_to_work',                                         'NONE'),
            (28, 'special_prices_ss',                                    'NONE'),
            (29, 'special_prices_fw',                                    'NONE'),
            (30, 'petit_prix',                                           'NONE'),
            (31, 'clearance',                                            'NONE'),
            (32, 'clearance_extended',                                   'NONE'),
            (33, 'stock_1',                                              'NONE')
    ) as events (event_sk, event, macro_event_type)

),

source_events as (

    select distinct {{ event_key('event') }} as event
    from {{ ref('slv_events') }}
    where event is not null

),

new_events as (

    select
        (select max(event_sk) from fixed_events)
            + row_number() over (order by source_events.event)  as event_sk,
        source_events.event,
        'NONE'                                                  as macro_event_type
    from source_events
    anti join fixed_events
        on fixed_events.event = source_events.event
    where source_events.event not in ('{{ excluded_events | join("', '") }}')

)

select
    cast(event_sk as integer)           as event_sk,
    cast(event as varchar)              as event,
    cast(macro_event_type as varchar)   as macro_event_type
from (
    select * from fixed_events
    union all
    select * from new_events
)
