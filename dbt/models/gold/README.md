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

Seus modelos ficam diretamente nesta pasta (ou em subpastas por assunto), com o prefixo `gld_`.

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
