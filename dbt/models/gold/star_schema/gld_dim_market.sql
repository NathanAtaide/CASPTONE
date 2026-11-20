{#
  gld_dim_market - GOLD (star schema dimension, conformed)

  Where the sale was registered: one row per market code (MARK_COD), shared by gld_fact_sales
  and gld_fact_event_paid_media.

    * The surrogate keys 0-34 are fixed (spec 5.2), so they never change between runs. A market
      code that shows up later in slv_order_cab, slv_paid_media or slv_events gets the next free
      key, in alphabetical order.
    * country_code is the last word of mark_cod (Online ES -> ES, ECI ES -> ES), with UK -> GB to
      match the ISO code of slv_holidays. It is the country of the market, not of the customer.
    * country_name comes from gld_ref_countries. 'Online ND' (paid media not assigned to a
      market) keeps its own row, ND / Not Defined, instead of the unknown member, so it can be
      re-assigned later when the root cause is found.
    * Row 0 = unknown market (mark_cod 'Unknown', country_code 'n/a').
    * SCD: mark_cod and country_code type 0, country_name type 1 (rebuilt every run).

  The market 'All' of slv_events is not a market: those events are spread over the markets in
  gld_fact_event_paid_media.

  Written to s3://<gold bucket>/gld_dim_market.parquet
#}

with fixed_markets as (

    select *
    from (
        values
            (0,  'Unknown'),
            (1,  'Online PT'),
            (2,  'Online UK'),
            (3,  'ECI ES'),
            (4,  'Online DE'),
            (5,  'Online CH'),
            (6,  'Online MX'),
            (7,  'Online IL'),
            (8,  'Online SK'),
            (9,  'Online MA'),
            (10, 'Online FI'),
            (11, 'Online IT'),
            (12, 'Online FR'),
            (13, 'HEPSIBURADA TR'),
            (14, 'Online TR'),
            (15, 'Online LU'),
            (16, 'Online IE'),
            (17, 'Online AT'),
            (18, 'Online LV'),
            (19, 'Online SI'),
            (20, 'Online PL'),
            (21, 'Zalando PL'),
            (22, 'Online HU'),
            (23, 'Online PE'),
            (24, 'Online ES'),
            (25, 'Online NL'),
            (26, 'TRENDYOL TR'),
            (27, 'Online CZ'),
            (28, 'Online BE'),
            (29, 'Online EE'),
            (30, 'Online DK'),
            (31, 'Online SE'),
            (32, 'Online CL'),
            (33, 'Online LT'),
            (34, 'Online ND')
    ) as markets (mark_sk, mark_cod)

),

source_markets as (

    select mark_cod from {{ ref('slv_order_cab') }} where mark_cod is not null
    union
    select marketplace from {{ ref('slv_paid_media') }} where marketplace is not null
    union
    select mark_cod from {{ ref('slv_events') }} where mark_cod is not null and mark_cod <> 'All'

),

new_markets as (

    -- Codes the spec does not list yet: appended after the fixed keys.
    select
        (select max(mark_sk) from fixed_markets)
            + row_number() over (order by source_markets.mark_cod)  as mark_sk,
        source_markets.mark_cod
    from source_markets
    anti join fixed_markets
        on fixed_markets.mark_cod = source_markets.mark_cod

),

markets as (

    select
        mark_sk,
        mark_cod,
        case
            when mark_sk = 0 then 'n/a'
            when upper(string_split(trim(mark_cod), ' ')[-1]) = 'UK' then 'GB'
            else upper(string_split(trim(mark_cod), ' ')[-1])
        end as country_code
    from (
        select mark_sk, mark_cod from fixed_markets
        union all
        select mark_sk, mark_cod from new_markets
    )

)

select
    cast(markets.mark_sk as integer)        as mark_sk,
    cast(markets.mark_cod as varchar)       as mark_cod,
    cast(markets.country_code as varchar)   as country_code,
    cast(
        case
            when markets.mark_sk = 0 then 'Unknown'
            when markets.country_code = 'ND' then 'Not Defined'
            else coalesce(countries.country_name, 'Unknown')
        end as varchar
    )                                       as country_name
from markets
left join {{ ref('gld_ref_countries') }} as countries
    on countries.country_code = markets.country_code
