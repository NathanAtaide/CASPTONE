# `dbt/models/gold/` — camada Ouro

## Papel na arquitetura

A Ouro é o dado **pronto para consumo**: indicadores, agregações e visões de negócio. É o que
alimenta o Power BI (via exportação em Parquet) e as *features* dos modelos de ML.

Cada modelo desta pasta é gravado em `s3://<bucket gold>/<modelo>.parquet` e recebe
automaticamente o *schema* `gold` e a *tag* `gold` (configuração em `dbt_project.yml`).

## O que um modelo gold deve fazer

1. Ler apenas modelos silver (ou outros gold) com a função `ref`.
2. Aplicar regras de negócio, junções entre entidades e agregações.
3. Entregar colunas com nomes e granularidade pensados para quem consome (um relatório, um modelo).

## Conteúdo

| Caminho | Função |
|---|---|
| [`example/`](example/README.md) | Modelo de demonstração (`gld_example_weather_daily`). |
| [`star_schema/`](star_schema/README.md) | **Star schema** (Kimball) de vendas online e marketing, para o Power BI: fatos `gld_fact_sales` e `gld_fact_event_paid_media`, dimensões `gld_dim_*` (data, mercado, origem, tipo, produto e cliente em SCD2, campanha, evento, feriado) e o *lookup* `gld_ref_countries`. |
| `gld_weather_country_daily.sql` + `_weather__gold.yml` | Meteorologia diária por país: média simples das 3 maiores cidades (`slv_weather_daily`). Junta-se às vendas por `country_code` e data. |
| `gld_ml_sales.sql` + `_ml_sales__gold.yml` | **Só para treino de ML/DL**, fora do *star schema*. Uma linha por dia, mercado, canal e categoria das vendas PARFOIS (`type = 'SALE'`), sobre um calendário completo (dia sem venda = 0). Alvos: unidades e faturamento; *features* de calendário, preço, mídia paga, *lags*, janelas móveis e campanhas (`slv_events` mapeado para as 33 flags do *profiling*). Não tem pedidos: um pedido com várias categorias seria contado uma vez por categoria (≈ +34%). Ordenada por `order_date`. |
| `gld_ml_orders.sql` + `_ml_orders__gold.yml` | **Só para treino de ML/DL**. Uma linha por dia, mercado e canal com os pedidos distintos do dia (mesmo escopo da `gld_ml_sales`), sobre um calendário completo (dia sem pedido = 0). Sem categoria, então as linhas somam o total real de pedidos. Alvo: pedidos; *features* de calendário, *lags* e janela móvel de pedidos; mídia paga e campanhas lidas da `gld_ml_sales`. Ordenada por `order_date`. |

Seus modelos ficam diretamente nesta pasta (ou em subpastas por assunto), com o prefixo `gld_`.

### Tabelas da gold que não são do dbt

As tabelas `gld_forecast_*` (backtest, métricas, campeão e previsão do mês seguinte dos modelos de
ML/DL) são gravadas em `s3://<bucket gold>/` pelas DAGs `ml_forecast_selection` e
`ml_forecast_next_month` (pacote `lakehouse.forecasting`), não por modelos desta pasta. Estão
declaradas como fonte `gold_forecast` em `models/bronze/_bronze__sources.yml` e vão para o Power BI
pela mesma exportação. Não crie um modelo dbt com um desses nomes: ele sobrescreveria o arquivo.

As duas tabelas `gld_ml_*` ligam `preserve_insertion_order` do DuckDB durante a própria execução
(`pre_hook` / `post_hook`). O perfil do dbt desliga essa opção para gastar menos memória, e sem ela
o `ORDER BY order_date` do modelo não chegaria garantido ao arquivo Parquet.

## Da Ouro para o Power BI

Quando a DAG `dbt_medallion` termina, o asset `lake_gold` é atualizado e a DAG
`gold_export_powerbi` copia **cada arquivo desta camada** para `./data/exports/powerbi/` no host.
Ou seja: criar um modelo aqui já é suficiente para ele chegar ao Power BI na próxima execução.

Cuidados para o Power BI ler bem o Parquet:

- prefira tipos simples: inteiros, `double`, `decimal`, `varchar`, `date`, `timestamp`, `boolean`;
- evite colunas aninhadas (`struct`, `list`, `map`) e `hugeint`/`uuid` — converta com `cast`;
- mantenha a granularidade em uma linha por combinação de dimensões.

## Execução

A DAG `dbt_medallion` roda `dbt build --select tag:gold` depois que a silver foi construída e
testada com sucesso.
