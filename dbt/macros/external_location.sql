{#
  external_location - overrides the dbt-duckdb macro that decides WHERE an
  "external" model is written.

  Rule: one bucket per medallion layer, one Parquet file per model.

      models/silver/slv_orders.sql  ->  s3://<LAKE_BUCKET_SILVER>/slv_orders.parquet
      models/gold/gld_sales.sql     ->  s3://<LAKE_BUCKET_GOLD>/gld_sales.parquet

  The layer is the schema of the model (set per folder in dbt_project.yml) and
  the bucket name comes from the LAKE_BUCKET_<LAYER> environment variable, so
  models never hard-code a path. A single model can still choose another place
  with the `location` config.

  Note: a model created outside models/silver or models/gold has the schema
  "main" and would target a bucket that does not exist - the run then fails
  with a "bucket not found" error. Keep models inside a layer folder.
#}
{%- macro external_location(relation, config) -%}
  {%- set layer = relation.schema | lower -%}
  {%- set bucket = env_var('LAKE_BUCKET_' ~ (layer | upper), layer) -%}
  {%- if config.get('options', {}).get('partition_by') is none -%}
    {%- set format = config.get('format', 'parquet') -%}
    s3://{{ bucket }}/{{ relation.identifier }}.{{ format }}
  {%- else -%}
    s3://{{ bucket }}/{{ relation.identifier }}
  {%- endif -%}
{%- endmacro -%}
