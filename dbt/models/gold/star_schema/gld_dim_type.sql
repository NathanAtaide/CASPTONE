{#
  gld_dim_type - GOLD (star schema dimension)

  Movement type of an order line: one row per value of slv_order_det.type.

    * SALE = sale; RETURN = the item left the warehouse and came back; CANCELLED = the item was
      ordered but never left the warehouse (client definition).
    * The surrogate keys 0-3 are fixed (spec 5.4). A type that shows up later gets the next
      free key, with type_desc equal to the source value.
    * Row 0 = unknown type (UNK). SCD type 0.

  Written to s3://<gold bucket>/gld_dim_type.parquet
#}

with fixed_types as (

    select *
    from (
        values
            (0, 'UNK',       'Unknown'),
            (1, 'SALE',      'Sale'),
            (2, 'RETURN',    'Return'),
            (3, 'CANCELLED', 'Cancellation')
    ) as types (type_sk, type, type_desc)

),

new_types as (

    select
        (select max(type_sk) from fixed_types)
            + row_number() over (order by source_types.type)   as type_sk,
        source_types.type,
        source_types.type                                       as type_desc
    from (
        select distinct type
        from {{ ref('slv_order_det') }}
        where type is not null
    ) as source_types
    anti join fixed_types
        on fixed_types.type = source_types.type

)

select
    cast(type_sk as integer)    as type_sk,
    cast(type as varchar)       as type,
    cast(type_desc as varchar)  as type_desc
from (
    select * from fixed_types
    union all
    select * from new_types
)
