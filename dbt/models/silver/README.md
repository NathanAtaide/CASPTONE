# `dbt/models/silver/` — camada Prata

## Papel na arquitetura

A Prata é o dado **tratado e confiável**: uma tabela por entidade de negócio, com tipos corretos,
nomes padronizados e sem duplicidades. É a base tanto para a camada Ouro quanto para a exploração e
o treinamento de modelos no Jupyter.

Cada modelo desta pasta é gravado em `s3://<bucket silver>/<modelo>.parquet` e recebe
automaticamente o *schema* `silver` e a *tag* `silver` (configuração em `dbt_project.yml`).

## O que um modelo silver deve fazer

1. Ler **uma** fonte da bronze (função `source`), passando por `latest_by_key` ou `latest_snapshot`
   para eliminar versões antigas.
2. Converter tipos explicitamente (`cast`), padronizar nomes e unidades.
3. Descartar registros inválidos (sem chave, sem data etc.).
4. Manter as colunas de linhagem úteis (`_ingested_at`, `_batch_id`).

O que **não** fazer aqui: agregações e regras de negócio específicas de um relatório (isso é Ouro).

## Conteúdo

| Caminho | Função |
|---|---|
| [`example/`](example/README.md) | Modelo de demonstração (`slv_example_weather_hourly`). |

Seus modelos ficam diretamente nesta pasta (ou em subpastas por domínio, ex.: `vendas/`), com o
prefixo `slv_`.

## Esqueleto de um modelo (tabela do MySQL em modo incremental)

Arquivo `slv_pedidos.sql`: um `select` com os `cast` das colunas, lendo de
`latest_by_key(source('bronze_mysql', 'pedidos'), ['id'])` escrito entre chaves duplas do Jinja e
seguido de `as bronze`. O exemplo completo, pronto para copiar, está em `dbt/README.md`.

Ao lado, um arquivo `_pedidos__silver.yml` com a descrição das colunas e ao menos o teste
`not_null` na chave.

## Execução

A DAG `dbt_medallion` roda `dbt build --select tag:silver`: constrói todos os modelos desta pasta
e executa seus testes antes de passar para a Ouro.
