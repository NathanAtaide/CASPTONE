# `dbt/models/silver/example/` — exemplo da camada Prata

## Papel na arquitetura

Modelo de demonstração que permite testar o fluxo completo (API → staging → bronze → **silver** →
gold → Power BI → MLflow) antes de existirem modelos próprios. Serve também como referência de
estilo para novos modelos silver.

## Arquivos

| Arquivo | Função |
|---|---|
| `slv_example_weather_hourly.sql` | Lê `bronze_open_meteo.hourly_weather`, mantém a versão mais recente de cada (local, hora), aplica tipos e nomes de negócio. |
| `_example__silver.yml` | Descrição do modelo e das colunas, com testes `not_null` em `location` e `observed_at`. |

## Entrada e saída

| | Local |
|---|---|
| Entrada | `s3://bronze/open_meteo/hourly_weather/ingestion_date=*/*.parquet` (gravado pela DAG `example_api_to_bronze`) |
| Saída | `s3://silver/slv_example_weather_hourly.parquet` |

Colunas de saída: `location`, `latitude`, `longitude`, `observed_at` (UTC), `temperature_c`,
`relative_humidity_pct`, `precipitation_mm`, `wind_speed_kmh`, `_ingested_at`, `_batch_id`.

## Por que há deduplicação

Cada execução da DAG da API traz novamente os últimos dias. Sem a macro `latest_by_key`, a mesma
hora apareceria uma vez por execução. O modelo fica com a linha de `_ingested_at` mais recente.

## Como desligar ou remover

- Desligar: `DBT_ENABLE_EXAMPLES=false` no `.env` + `docker compose up -d`.
- Remover: apague esta pasta, `models/gold/example/` e a fonte `bronze_open_meteo` em
  `models/bronze/_bronze__sources.yml`.
