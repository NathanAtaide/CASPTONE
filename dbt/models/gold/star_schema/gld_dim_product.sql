{#
  gld_dim_product - GOLD (star schema dimension, SCD type 2)

  What was sold: one row per version of a SKU (prod_cod).

  The source is already versioned: every prod_cod has one or more prod_spk (the version key of
  the source, target of slv_order_det.prod_sfk) and exactly one of them has active_record = true,
  always the highest prod_spk. The source has no validity dates, so:
    * every row of slv_product is valid from 1900-01-01 to 9999-12-31 (the first version of a
      SCD2 record starts on 1900-01-01 so that every order finds a version);
    * is_current = active_record: exactly one current row per prod_cod, as in the source;
    * active = active_record as well. Both stay equal here and only diverge in the
      gold_scd2_simulation DAG, which closes and opens versions on top of this table.
  A fact row finds its version by prod_spk = prod_sfk AND order date between valid_from and
  valid_to. With the dates above that is exactly one row per prod_spk.

  SCD per column (spec 5.5): type 2 = active, fashiontype, active_category; type 0 = the rest.

  Inferred members: a prod_sfk seen in slv_order_det but missing in slv_product (for example a
  non-merchandise item removed by the silver rules 2.6.x) becomes a row with is_inferred = true
  and attributes 'Unknown'. It is replaced by the real product as soon as the product arrives.

  Row 0 = unknown product (prod_spk -1). Missing texts become 'Unknown' and missing flags false,
  because the spec asks for not null attributes. The surrogate key follows prod_spk and
  valid_from, so it is stable as long as the source does not delete products.

  Written to s3://<gold bucket>/gld_dim_product.parquet
#}

with products as (

    select *
    from {{ ref('slv_product') }}

),

inferred_products as (

    select distinct lines.prod_sfk as prod_spk
    from {{ ref('slv_order_det') }} as lines
    anti join products
        on products.prod_spk = lines.prod_sfk
    where lines.prod_sfk is not null

),

versions as (

    select
        prod_spk,
        prod_cod,
        coalesce(cat_des_en, 'Unknown')         as cat_des_en,
        coalesce(gfa_des_en, 'Unknown')         as gfa_des_en,
        coalesce(gfs_des_en, 'Unknown')         as gfs_des_en,
        coalesce(collection_type, 'Unknown')    as collection_type,
        coalesce(sea_cod, 0)                    as sea_cod,
        coalesce(active_record, false)          as active,
        coalesce(fashiontype, 'Unknown')        as fashiontype,
        coalesce(cat_active, false)             as active_category,
        coalesce(tar_des, 'Unknown')            as target_description,
        date '1900-01-01'                       as valid_from,
        date '9999-12-31'                       as valid_to,
        coalesce(active_record, false)          as is_current,
        false                                   as is_inferred
    from products

    union all

    select
        prod_spk,
        'Unknown'                               as prod_cod,
        'Unknown'                               as cat_des_en,
        'Unknown'                               as gfa_des_en,
        'Unknown'                               as gfs_des_en,
        'Unknown'                               as collection_type,
        0                                       as sea_cod,
        false                                   as active,
        'Unknown'                               as fashiontype,
        false                                   as active_category,
        'Unknown'                               as target_description,
        date '1900-01-01'                       as valid_from,
        date '9999-12-31'                       as valid_to,
        true                                    as is_current,
        true                                    as is_inferred
    from inferred_products

),

members as (

    select
        row_number() over (order by prod_spk, valid_from)  as prod_sk,
        *
    from versions

    union all by name

    -- Row 0 (unknown product).
    select
        0                                       as prod_sk,
        -1                                      as prod_spk,
        'Unknown'                               as prod_cod,
        'Unknown'                               as cat_des_en,
        'Unknown'                               as gfa_des_en,
        'Unknown'                               as gfs_des_en,
        'Unknown'                               as collection_type,
        0                                       as sea_cod,
        false                                   as active,
        'Unknown'                               as fashiontype,
        false                                   as active_category,
        'Unknown'                               as target_description,
        date '1900-01-01'                       as valid_from,
        date '9999-12-31'                       as valid_to,
        true                                    as is_current,
        false                                   as is_inferred

)

select
    cast(prod_sk as integer)                as prod_sk,
    cast(prod_spk as bigint)                as prod_spk,
    cast(prod_cod as varchar)               as prod_cod,
    cast(cat_des_en as varchar)             as cat_des_en,
    cast(gfa_des_en as varchar)             as gfa_des_en,
    cast(gfs_des_en as varchar)             as gfs_des_en,
    cast(collection_type as varchar)        as collection_type,
    cast(sea_cod as smallint)               as sea_cod,
    cast(active as boolean)                 as active,
    cast(fashiontype as varchar)            as fashiontype,
    cast(active_category as boolean)        as active_category,
    cast(target_description as varchar)     as target_description,
    cast(valid_from as date)                as valid_from,
    cast(valid_to as date)                  as valid_to,
    cast(is_current as boolean)             as is_current,
    cast(is_inferred as boolean)            as is_inferred
from members
