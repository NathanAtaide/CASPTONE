{#
  gld_dim_customer - GOLD (star schema dimension, SCD type 2)

  Who bought: one row per version of a customer (customer_id). No names or e-mails. birthday is
  personal data: use it for segmentation and birthday-period analysis, always aggregated.

  Versions (team rules). The source is a single snapshot, so the versions come from its dates:
    * newsletter_status is type 1 inside a version: subscribing sets it to Subscribed and
      cancelling updates it to Unsubscribed, without a new row. So:
        - no subscription date            -> one version, Not subscribed;
        - subscription only               -> one version, Subscribed;
        - subscription, then cancellation -> one version, Unsubscribed (also when both happen on
                                             the same day, or when only the cancellation has a date);
    * a new row is created only when the customer subscribes AGAIN: UNSUBSCRIPTION_DATE before
      SUBSCRIPTION_DATE means the customer cancelled and later re-subscribed, and the source kept
      only the new subscription date. That customer gets two versions:
        1. 1900-01-01 -> day before the new subscription: Unsubscribed, unsubscription_date filled,
           subscription_date null (the original date was overwritten in the source);
        2. new subscription date -> 9999-12-31: Subscribed, subscription_date filled,
           unsubscription_date null (the cancellation no longer applies). Same creation_date.
  The first version always starts on 1900-01-01 so that every order finds a version. A fact row
  finds its version by customer_id = customer_id_fk AND order date between valid_from and valid_to.

  SCD per column (spec 5.6): type 0 = customer_id, creation_date, birthday; type 1 = country,
  subscription_source, newsletter_status (per version); type 2 = subscription_date,
  unsubscription_date (as they apply to each version).

  Other rules:
    * country is trimmed; a missing country becomes 'Unknown' (the spec asks for not null);
    * birthday 1900-01-01 is a placeholder of the source and becomes null;
    * inferred members: a customer_id_fk of slv_order_cab missing in slv_customers becomes a row
      with is_inferred = true and attributes 'Unknown';
    * row 0 = unknown customer / no ID (customer_id -1, the franchise orders). The fact maps
      customer_id_fk = -1 to it.
  The surrogate key follows customer_id and valid_from.

  Written to s3://<gold bucket>/gld_dim_customer.parquet
#}

with customers as (

    select
        customer_id,
        creation_date,
        coalesce(nullif(trim(country), ''), 'Unknown')                         as country,
        subscription_date,
        unsubscription_date,
        subscription_source,
        case when birthday = date '1900-01-01' then null else birthday end      as birthday,
        coalesce(unsubscription_date < subscription_date, false)                as resubscribed
    from {{ ref('slv_customers') }}

),

versions as (

    -- Customers that did not re-subscribe: a single version.
    select
        customer_id,
        creation_date,
        country,
        subscription_date,
        unsubscription_date,
        case
            when unsubscription_date is not null then 'Unsubscribed'
            when subscription_date is not null then 'Subscribed'
            else 'Not subscribed'
        end                                     as newsletter_status,
        subscription_source,
        birthday,
        date '1900-01-01'                       as valid_from,
        date '9999-12-31'                       as valid_to,
        true                                    as is_current,
        false                                   as is_inferred
    from customers
    where not resubscribed

    union all

    -- Re-subscription, version 1: the cancelled subscription.
    select
        customer_id,
        creation_date,
        country,
        null                                    as subscription_date,
        unsubscription_date,
        'Unsubscribed'                          as newsletter_status,
        subscription_source,
        birthday,
        date '1900-01-01'                       as valid_from,
        cast(subscription_date - 1 as date)     as valid_to,
        false                                   as is_current,
        false                                   as is_inferred
    from customers
    where resubscribed

    union all

    -- Re-subscription, version 2: the new subscription, open.
    select
        customer_id,
        creation_date,
        country,
        subscription_date,
        null                                    as unsubscription_date,
        'Subscribed'                            as newsletter_status,
        subscription_source,
        birthday,
        subscription_date                       as valid_from,
        date '9999-12-31'                       as valid_to,
        true                                    as is_current,
        false                                   as is_inferred
    from customers
    where resubscribed

    union all

    -- Inferred members: customers of the orders that the customers table does not have.
    select
        orders.customer_id_fk                   as customer_id,
        null                                    as creation_date,
        'Unknown'                               as country,
        null                                    as subscription_date,
        null                                    as unsubscription_date,
        'Unknown'                               as newsletter_status,
        null                                    as subscription_source,
        null                                    as birthday,
        date '1900-01-01'                       as valid_from,
        date '9999-12-31'                       as valid_to,
        true                                    as is_current,
        true                                    as is_inferred
    from (
        select distinct customer_id_fk
        from {{ ref('slv_order_cab') }}
        where customer_id_fk is not null
          and customer_id_fk <> -1
    ) as orders
    anti join customers
        on customers.customer_id = orders.customer_id_fk

),

members as (

    select
        row_number() over (order by customer_id, valid_from)   as customer_sk,
        *
    from versions

    union all by name

    -- Row 0 (unknown customer / no ID). The columns not listed here are null.
    select
        0                                       as customer_sk,
        -1                                      as customer_id,
        'Unknown'                               as country,
        'Unknown'                               as newsletter_status,
        date '1900-01-01'                       as valid_from,
        date '9999-12-31'                       as valid_to,
        true                                    as is_current,
        false                                   as is_inferred

)

select
    cast(customer_sk as integer)            as customer_sk,
    cast(customer_id as bigint)             as customer_id,
    cast(creation_date as date)             as creation_date,
    cast(country as varchar)                as country,
    cast(subscription_date as date)         as subscription_date,
    cast(unsubscription_date as date)       as unsubscription_date,
    cast(newsletter_status as varchar)      as newsletter_status,
    cast(subscription_source as varchar)    as subscription_source,
    cast(birthday as date)                  as birthday,
    cast(valid_from as date)                as valid_from,
    cast(valid_to as date)                  as valid_to,
    cast(is_current as boolean)             as is_current,
    cast(is_inferred as boolean)            as is_inferred
from members
