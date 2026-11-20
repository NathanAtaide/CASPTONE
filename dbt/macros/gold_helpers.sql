{#
  Helpers shared by the gold models of the star schema (models/gold/star_schema/).

  event_key(column)
      Turns an event name of slv_events into the key stored in gld_dim_event.event: lower case,
      accents removed, '%' -> 'pct', every run of other symbols -> '_', no '_' at the ends.
          'Black Friday App 20% all + 50% selected' -> 'black_friday_app_20pct_all_50pct_selected'
          'Avant-Premiere'                          -> 'avant_premiere'
      It is the same rule gld_ml_sales uses to map the events to its 33 flags, so both models
      always agree on the names.

  (In a model the call above is written between double curly braces.)
#}

{% macro event_key(column) -%}
    trim(
        regexp_replace(
            strip_accents(lower(replace(cast({{ column }} as varchar), '%', 'pct'))),
            '[^a-z0-9]+', '_', 'g'
        ),
        '_'
    )
{%- endmacro %}
