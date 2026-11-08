# `dbt/models/silver/` — camada Prata

## Papel na arquitetura

A Prata é o dado **tratado e confiável**: uma tabela por entidade de negócio, com tipos corretos, nomes padronizados e sem duplicidades. É a base tanto para a camada Ouro quanto para a exploração e o treinamento de modelos no Jupyter.

Cada modelo desta pasta é gravado em `s3://<bucket silver>/<modelo>.parquet` e recebe automaticamente o *schema* `silver` e a *tag* `silver` (configuração em `dbt_project.yml`).

## O que um modelo silver deve fazer

1. Ler **uma** fonte da bronze (função `source`), passando por `latest_by_key` ou `latest_snapshot` para eliminar versões antigas.
2. Converter tipos explicitamente (`cast`), padronizar nomes e unidades.
3. Descartar registros inválidos (sem chave, sem data etc.).
4. Manter as colunas de linhagem úteis (`_ingested_at`, `_batch_id`).

O que **não** fazer aqui: agregações e regras de negócio específicas de um relatório (isso é Ouro).

## Conteúdo

| Caminho | Função |
|---|---|
| [`example/`](example/README.md) | Modelo de demonstração (`slv_example_weather_hourly`). |
| `slv_weather_daily.sql` + `_weather__silver.yml` | Meteorologia diária por cidade (3 maiores cidades de cada país dos marketplaces), deduplicada e tipada, a partir de `bronze_open_meteo.daily_weather`. |
| `slv_calendar.sql` | Dimensão de datas (calendário) no padrão Kimball: uma linha por dia, sem lacunas, chave `calendar_date` (DATE). Gerada no DuckDB, não lê a bronze. Inclui feriados nacionais e pontos facultativos federais, dias úteis e rótulos em inglês. |
| `_calendar__silver.yml` | Descrição das colunas de `slv_calendar` e testes (`unique` + `not_null` nas chaves). |

### Intervalo da `slv_calendar`

Definido por variáveis do dbt (ambas inclusivas): `calendar_start_date` (padrão `2020-01-01`) e `calendar_end_date` (padrão: 31/12 do ano corrente + 5). Para fixá-las de forma permanente, acrescente um bloco `vars` em `dbt_project.yml`. Feriados estaduais e municipais não estão incluídos.

Seus modelos ficam diretamente nesta pasta (ou em subpastas por domínio, ex.: `vendas/`), com o prefixo `slv_`.

## Esqueleto de um modelo (tabela do MySQL em modo incremental)

Arquivo `slv_pedidos.sql`: um `select` com os `cast` das colunas, lendo de `latest_by_key(source('bronze_mysql', 'pedidos'), ['id'])` escrito entre chaves duplas do Jinja e seguido de `as bronze`. O exemplo completo, pronto para copiar, está em `dbt/README.md`.

Ao lado, um arquivo `_pedidos__silver.yml` com a descrição das colunas e ao menos o teste `not_null` na chave.

## Execução

A DAG `dbt_medallion` roda `dbt build --select tag:silver`: constrói todos os modelos desta pasta e executa seus testes antes de passar para a Ouro.
