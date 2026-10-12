# `dbt/models/bronze/` — camada Bronze (fontes)

## Papel na arquitetura

A Bronze é o **dado bruto**, exatamente como saiu da origem (MySQL ou API), guardado em Parquet no
bucket `bronze` do MinIO. Quem grava a bronze são as DAGs de ingestão do Airflow — **o dbt não
constrói nada aqui**: esta pasta apenas *declara* onde os arquivos estão, para que os modelos silver
possam referenciá-los.

Por isso não há arquivos `.sql` nesta pasta, só a declaração de fontes.

## Arquivos

| Arquivo | Função |
|---|---|
| `_bronze__sources.yml` | Declara as fontes `bronze_mysql` (tabelas do MySQL) e `bronze_open_meteo` (API de exemplo). |

## Como uma fonte é resolvida

Cada fonte tem um `external_location` (recurso do dbt-duckdb) com o padrão de caminho:

```
s3://<bucket bronze>/<sistema de origem>/<tabela>/ingestion_date=AAAA-MM-DD/*.parquet
```

Quando um modelo usa a função `source` com o nome da fonte e da tabela, o dbt troca a chamada por
um `read_parquet(...)` que lê **todas as partições** daquela tabela direto do lake. Consequências:

- a coluna `ingestion_date` (vinda do nome da pasta) fica disponível para filtros;
- colunas novas na origem não quebram a leitura (`union_by_name`);
- como a bronze acumula histórico, o modelo silver deve usar as macros `latest_load`,
  `latest_by_key` ou `latest_snapshot` para ficar só com o estado atual (ver `macros/README.md`).

## Como declarar um dataset do MySQL

No `_bronze__sources.yml`, a fonte `bronze_mysql` já lista os datasets conhecidos (`customers`,
`emails`, `events`, `order_cab`, `order_det`, `paid_media`, `product`, `sessions`). Para incluir
outro, acrescente um item em `tables:`:

```yaml
      - name: pedidos
        description: "Pedidos."
```

O `name` é o nome do **dataset** da bronze (a pasta em `s3://bronze/mysql/`), pois é ele que forma
o caminho no bucket. Para tabelas carregadas sozinhas, é o próprio nome da tabela; para tabelas
unificadas, é o nome definido em `airflow/dags/config/mysql_tables.yml` (ex.: `emails` reúne
`emails`, `emails_1`, `emails_2`...). A coluna `_source_entity` informa a tabela de origem de cada
linha.

## Observação

Se uma fonte declarada ainda não tiver arquivos no bucket (ingestão nunca executada), os modelos
que dependem dela falham com erro de "nenhum arquivo encontrado". Rode a ingestão primeiro.
