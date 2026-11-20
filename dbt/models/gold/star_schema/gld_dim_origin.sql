{#
  gld_dim_origin - GOLD (star schema dimension)

  How the order came in: one row per combination of ORDER_ORIGIN (who sold) x ORDER_CHANEL_TYPE
  (platform) that exists in slv_order_cab. No cartesian product: PARFOIS Franchise, ECI and
  ZALANDO only send ND as platform, so they have one row each.

    * The surrogate keys 0-6 are fixed (spec 5.3). A combination that shows up later gets the
      next free key, in alphabetical order.
    * The source spelling CHANEL is kept in the column name.
    * Row 0 = unknown origin. SCD type 0.

  Written to s3://<gold bucket>/gld_dim_origin.parquet
#}

with fixed_origins as (

    select *
    from (
        values
            (0, 'Unknown',           'Unknown'),
            (1, 'PARFOIS',           'APP'),
            (2, 'PARFOIS',           'DESKTOP'),
            (3, 'PARFOIS',           'STORE'),
            (4, 'PARFOIS Franchise', 'ND'),
            (5, 'ECI',               'ND'),
            (6, 'ZALANDO',           'ND')
    ) as origins (origin_sk, order_origin, order_chanel_type)

),

source_origins as (

    select distinct
        order_origin,
        order_chanel_type
    from {{ ref('slv_order_cab') }}
    where order_origin is not null
      and order_chanel_type is not null

),

new_origins as (

    select
        (select max(origin_sk) from fixed_origins)
            + row_number() over (order by source_origins.order_origin, source_origins.order_chanel_type) as origin_sk,
        source_origins.order_origin,
        source_origins.order_chanel_type
    from source_origins
    anti join fixed_origins
        on  fixed_origins.order_origin = source_origins.order_origin
        and fixed_origins.order_chanel_type = source_origins.order_chanel_type

)

select
    cast(origin_sk as integer)          as origin_sk,
    cast(order_origin as varchar)       as order_origin,
    cast(order_chanel_type as varchar)  as order_chanel_type
from (
    select * from fixed_origins
    union all
    select * from new_origins
)
