{#
  Helpers to read the bronze layer from silver models.

  Bronze is append-only: every ingestion adds files, so the same business row
  can appear several times. These macros return a parenthesised sub-query with
  the "current" version of the data, ready to be used in a FROM clause.

  latest_by_key(relation, key_columns, order_by)
      For tables ingested in INCREMENTAL mode (or API batches): keeps the most
      recent row of each key. Example, inside a model:
          from latest_by_key(source('bronze_mysql', 'orders'), ['id'])

  latest_snapshot(relation)
      For tables ingested in FULL mode: keeps only the rows of the most recent
      snapshot (highest ingestion_date partition). Example, inside a model:
          from latest_snapshot(source('bronze_mysql', 'customers'))

  latest_load(relation)
      For tables ingested in BATCH mode: keeps only the rows of the most recent
      load of each source table (highest _load_id per _source_entity). Without
      a full_refresh there is a single load and nothing is removed, so it is
      safe to use always. Example, inside a model:
          from latest_load(source('bronze_mysql', 'emails'))

  The macros can be nested, e.g. a batch table walked by an updated_at column
  repeats the rows that changed:
          from latest_by_key(latest_load(source('bronze_mysql', 'orders')), ['id'])

  (In a model the calls above are written between double curly braces.)
#}

{% macro latest_by_key(relation, key_columns, order_by='_ingested_at desc') -%}
(
    select *
    from {{ relation }}
    qualify row_number() over (
        partition by {{ key_columns | join(', ') }}
        order by {{ order_by }}
    ) = 1
)
{%- endmacro %}


{% macro latest_snapshot(relation) -%}
(
    select *
    from {{ relation }}
    where ingestion_date = (select max(ingestion_date) from {{ relation }})
)
{%- endmacro %}


{% macro latest_load(relation) -%}
(
    select *
    from {{ relation }}
    qualify _load_id = max(_load_id) over (partition by _source_entity)
)
{%- endmacro %}
