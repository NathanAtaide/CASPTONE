{#
  slv_product - SILVER

  One row per product (PROD_SPK), from the source table parfois.product:
    * ACTIVE_RECORD (0/1) and CAT_ACTIVE ('Y'/'N') become BOOLEAN; any other value becomes
      null (rule 1.7);
    * SEA_COD is a season code (251), not a quantity: SMALLINT (rule 1.7);
    * products without GFS_DES_EN are dropped (rule 2.6.1);
    * products without TAR_DES, with CAT_DES_EN and FASHIONTYPE 'NÃO DEFINIDO' or 'Basic'
      are dropped (rule 2.6.2);
    * products with FASHIONTYPE 'NÃO DEFINIDO' in the non-merchandise categories listed below
      are dropped (rule 2.6.3);
    * products of category 'Works' without COLLECTION_TYPE are dropped (rule 2.6.4);
    * every product of the categories 'Marketing', 'Consumable', 'Informática' and 'Others' is
      dropped, whatever its FASHIONTYPE (rule 2.6.5).

  The exclusions are wrapped in coalesce(..., false) so that a null in a tested column never
  drops a row by accident (NOT NULL is NULL, which a WHERE treats as false).

  Written to s3://<silver bucket>/slv_product.parquet
#}

with latest as (

    -- Bronze is append-only: keep only the rows of the latest load of the source table.
    select *
    from {{ latest_load(source('bronze_mysql', 'product')) }} as bronze

)

select
    cast(prod_spk as bigint)                    as prod_spk,
    cast(prod_cod as varchar)                   as prod_cod,
    cast(cat_des_en as varchar)                 as cat_des_en,
    cast(gfa_des_en as varchar)                 as gfa_des_en,
    cast(gfs_des_en as varchar)                 as gfs_des_en,
    cast(collection_type as varchar)            as collection_type,
    cast(sea_cod as smallint)                   as sea_cod,
    cast(case active_record
        when 1 then true
        when 0 then false
    end as boolean)                             as active_record,
    cast(case upper(trim(cat_active))
        when 'Y' then true
        when 'N' then false
    end as boolean)                             as cat_active,
    cast(fashiontype as varchar)                as fashiontype,
    cast(tar_des as varchar)                    as tar_des,
    _ingested_at,
    _batch_id
from latest
where prod_spk is not null
  -- Rule 2.6.1
  and gfs_des_en is not null
  -- Rule 2.6.2
  and not coalesce(
        tar_des is null
        and cat_des_en is not null
        and fashiontype in ('NÃO DEFINIDO', 'Basic'),
        false
      )
  -- Rule 2.6.3
  and not coalesce(
        fashiontype = 'NÃO DEFINIDO'
        and cat_des_en in ('Marketing', 'Others', 'INT - BARAM', 'Consumable', 'Works', 'Informática'),
        false
      )
  -- Rule 2.6.4
  and not coalesce(cat_des_en = 'Works' and collection_type is null, false)
  -- Rule 2.6.5
  and not coalesce(cat_des_en in ('Marketing', 'Consumable', 'Informática', 'Others'), false)
