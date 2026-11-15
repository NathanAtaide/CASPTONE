{#
  Helpers to clean and type raw columns in silver models.

  clean_text(column)
      Trims the text and turns the placeholders left by exports ('', 'NaN', 'None', 'null',
      any case) into a real null. Returns VARCHAR.

  yyyymmdd_to_date(column)
      Converts a number or text in the YYYYMMDD format (20240922, 19850312.0) into a DATE.
      Values that are not a valid date (0, -1, 20240231) become null instead of failing the run.

  text_to_date(column) / text_to_time(column)
      Split a text timestamp ('YYYY-MM-DD HH:MM:SS', optionally with fractions of a second)
      into its DATE and its TIME. When the text holds only the date ('2023-03-12'), the time
      is null rather than a made-up 00:00:00. Invalid texts become null.

  date_to_key(column)
      Turns a DATE into the BIGINT date key YYYYMMDD (2024-09-22 -> 20240922), the column the
      gold layer uses to relate a model to slv_calendar.date_key. Null date -> null key.
      The argument can also be the output of another macro:
          date_to_key(yyyymmdd_to_date('send_date_sfk'))  as send_date_key

  The argument is the column name as text, qualified with the table alias when the model
  re-uses the same name for the output column:
          text_to_date('latest.order_date')  as order_date,
          text_to_time('latest.order_date')  as order_time

  (In a model the calls above are written between double curly braces.)
#}

{% macro clean_text(column) -%}
    case
        when lower(trim(cast({{ column }} as varchar))) in ('', 'nan', 'none', 'null') then null
        else trim(cast({{ column }} as varchar))
    end
{%- endmacro %}


{% macro yyyymmdd_to_date(column) -%}
    try_cast(
        regexp_replace(
            cast(try_cast({{ column }} as bigint) as varchar),
            '^([0-9]{4})([0-9]{2})([0-9]{2})$',
            '\1-\2-\3'
        ) as date
    )
{%- endmacro %}


{% macro text_to_date(column) -%}
    cast(try_cast({{ clean_text(column) }} as timestamp) as date)
{%- endmacro %}


{% macro date_to_key(column) -%}
    cast(strftime(cast({{ column }} as date), '%Y%m%d') as bigint)
{%- endmacro %}


{% macro text_to_time(column) -%}
    case
        when length({{ clean_text(column) }}) > 10
            then cast(try_cast({{ clean_text(column) }} as timestamp) as time)
    end
{%- endmacro %}
