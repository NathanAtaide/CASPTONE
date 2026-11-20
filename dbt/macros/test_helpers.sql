{#
  Generic data tests of the project (used in the .yml files under data_tests).

  unique_combination(model, combination)
      Fails when two rows share the same values in all the columns of `combination`. Used for
      composite primary keys (fact tables) and, with a `where` config, for rules such as "one
      current version per natural key":
          data_tests:
            - unique_combination:
                arguments:
                  combination: [order_id, prod_sk, type_sk]

  scd2_versions(model, version_key, valid_from='valid_from', valid_to='valid_to')
      Checks the validity intervals of a type 2 dimension, per value of `version_key`:
        * the first version starts on 1900-01-01 and the last one ends on 9999-12-31;
        * valid_to is never before valid_from;
        * each version starts the day after the previous one ends (no gap, no overlap).
      Together they guarantee that a fact looking up `version_key` + a date finds exactly one row.
#}

{% test unique_combination(model, combination) %}

select
    {{ combination | join(', ') }},
    count(*) as rows_with_this_key
from {{ model }}
group by {{ combination | join(', ') }}
having count(*) > 1

{% endtest %}


{% test scd2_versions(model, version_key, valid_from='valid_from', valid_to='valid_to') %}

with versions as (

    select
        {{ version_key }}                                       as version_key,
        {{ valid_from }}                                        as valid_from,
        {{ valid_to }}                                          as valid_to,
        lag({{ valid_to }}) over (
            partition by {{ version_key }} order by {{ valid_from }}
        )                                                       as previous_valid_to,
        row_number() over (
            partition by {{ version_key }} order by {{ valid_from }}
        )                                                       as version_number,
        row_number() over (
            partition by {{ version_key }} order by {{ valid_from }} desc
        )                                                       as versions_left
    from {{ model }}

)

select *
from versions
where valid_to < valid_from
   or (version_number = 1 and valid_from <> date '1900-01-01')
   or (versions_left = 1 and valid_to <> date '9999-12-31')
   or (previous_valid_to is not null and valid_from <> previous_valid_to + 1)

{% endtest %}
