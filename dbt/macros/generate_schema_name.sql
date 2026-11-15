{#
  generate_schema_name - overrides dbt's default schema naming.

  Default dbt behaviour : <target schema>_<custom schema>   -> main_silver
  Behaviour here        : <custom schema>                   -> silver

  The schema of a model is therefore exactly its medallion layer (silver/gold),
  which the external_location macro uses to pick the bucket on the data lake.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
