# `dbt/models/gold/example/` — exemplo da camada Ouro

## Papel na arquitetura

Modelo de demonstração que fecha o fluxo de exemplo: transforma a silver horária em um indicador
diário, que é exportado para o Power BI e pode ser usado como base de *features* no Jupyter.

## Arquivos

| Arquivo | Função |
|---|---|
| `gld_example_weather_daily.sql` | Agrega `slv_example_weather_hourly` em uma linha por local e dia. |
| `_example__gold.yml` | Descrição do modelo e das colunas, com testes `not_null` em `location` e `observation_date`. |

## Entrada e saída

| | Local |
|---|---|
| Entrada | modelo `slv_example_weather_hourly` (camada silver) |
| Saída no lake | `s3://gold/gld_example_weather_daily.parquet` |
| Saída no host | `./data/exports/powerbi/gld_example_weather_daily.parquet` (DAG `gold_export_powerbi`) |

Colunas: `location`, `observation_date`, `hours_observed`, `avg_temperature_c`,
`min_temperature_c`, `max_temperature_c`, `avg_relative_humidity_pct`, `total_precipitation_mm`,
`max_wind_speed_kmh`.

## Como desligar ou remover

Igual ao exemplo da silver: `DBT_ENABLE_EXAMPLES=false` no `.env`, ou apague esta pasta junto com
`models/silver/example/`.
