{#
  gld_ref_countries - GOLD (star schema lookup)

  ISO 3166-1 alpha-2 country code -> country name, one row per marketplace country. It fills
  gld_dim_market.country_name. A lookup, not a dimension: it has no relationship with the facts.
  To add a country, add one line to the list below.

  Written to s3://<gold bucket>/gld_ref_countries.parquet
#}

select
    cast(country_code as varchar) as country_code,
    cast(country_name as varchar) as country_name
from (
    values
        ('AT', 'Austria'),
        ('BE', 'Belgium'),
        ('CH', 'Switzerland'),
        ('CL', 'Chile'),
        ('CZ', 'Czech Republic'),
        ('DE', 'Germany'),
        ('DK', 'Denmark'),
        ('EE', 'Estonia'),
        ('ES', 'Spain'),
        ('FI', 'Finland'),
        ('FR', 'France'),
        ('HU', 'Hungary'),
        ('IE', 'Ireland'),
        ('IL', 'Israel'),
        ('IT', 'Italy'),
        ('LT', 'Lithuania'),
        ('LU', 'Luxembourg'),
        ('LV', 'Latvia'),
        ('MA', 'Morocco'),
        ('MX', 'Mexico'),
        ('NL', 'Netherlands'),
        ('PE', 'Peru'),
        ('PL', 'Poland'),
        ('PT', 'Portugal'),
        ('SE', 'Sweden'),
        ('SI', 'Slovenia'),
        ('SK', 'Slovakia'),
        ('TR', 'Turkey'),
        ('GB', 'United Kingdom')
) as countries (country_code, country_name)
