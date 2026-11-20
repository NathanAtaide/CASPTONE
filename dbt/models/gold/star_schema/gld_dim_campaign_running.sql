{#
  gld_dim_campaign_running - GOLD (star schema dimension)

  The "why" of a sale: which marketing context was running in the market on the order date.
  One row per combination of the 3 flags (8 rows) plus row 0 = unknown. Generated in the
  pipeline, without a source key.

  gld_fact_sales picks the row by looking up mark_sk x order_date_key in
  gld_fact_event_paid_media (rules in gld_fact_sales.sql):
    * has_paid_media - a row of that day and market has amount_paid_media > 0;
    * has_event      - a row of that day and market has event_sk <> 0;
    * is_holiday     - a row of that day and market has holiday_sk <> 0 for a NATIONWIDE holiday.
  Row 0 (flags null) is used when the market or the date of the order is unknown.

  Why the dimension exists: crossing the two facts only through gld_dim_date and gld_dim_market
  filters dates and markets separately, not the date x market pair. This dimension stores the
  right context in every sale.

  Written to s3://<gold bucket>/gld_dim_campaign_running.parquet
#}

select
    cast(campaign_running_sk as integer)    as campaign_running_sk,
    cast(has_paid_media as boolean)         as has_paid_media,
    cast(has_event as boolean)              as has_event,
    cast(is_holiday as boolean)             as is_holiday,
    cast(campaign_label as varchar)         as campaign_label
from (
    values
        (0, null,  null,  null,  'Unknown'),
        (1, false, false, false, 'No'),
        (2, true,  false, false, 'Paid Media'),
        (3, false, true,  false, 'Event'),
        (4, false, false, true,  'Holiday'),
        (5, true,  true,  false, 'Paid Media + Event'),
        (6, true,  false, true,  'Paid Media + Holiday'),
        (7, false, true,  true,  'Event + Holiday'),
        (8, true,  true,  true,  'Paid Media + Event + Holiday')
) as combinations (campaign_running_sk, has_paid_media, has_event, is_holiday, campaign_label)
