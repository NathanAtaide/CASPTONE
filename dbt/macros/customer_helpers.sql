{#
  Business rules on customers shared by several silver models.

  customer_has_data(alias)
      Rule 2.7.1: a customer is kept only when at least one of CREATION_DATE, BIRTHDAY,
      UNSUBSCRIPTION_DATE, SUBSCRIPTION_DATE, SUBSCRIPTION_SOURCE and COUNTRY has a value.
      Rows where all six are null are empty records and are dropped. "Null" is judged after
      cleaning, as in slv_customers: 'NaN', blank texts and unreadable dates count as null.

      The argument is the alias of the relation holding the raw bronze columns. Use it in
      slv_customers and in every model that checks whether a customer exists (slv_emails,
      slv_sessions), so all of them agree on which customers exist:
          where customer_id is not null and customer_has_data('bronze')

  (In a model the call above is written between double curly braces.)
#}

{% macro customer_has_data(alias) -%}
    (
        {{ text_to_date(alias ~ '.creation_date') }} is not null
        or {{ yyyymmdd_to_date(alias ~ '.birthday') }} is not null
        or {{ text_to_date(alias ~ '.unsubscription_date') }} is not null
        or {{ text_to_date(alias ~ '.subscription_date') }} is not null
        or {{ clean_text(alias ~ '.subscription_source') }} is not null
        or {{ clean_text(alias ~ '.country') }} is not null
    )
{%- endmacro %}
