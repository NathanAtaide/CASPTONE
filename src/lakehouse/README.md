# `src/lakehouse/` — pacote Python do lakehouse

## Papel na arquitetura

Biblioteca interna que concentra **toda a lógica de acesso a dados** da plataforma. As DAGs e os
notebooks não montam conexões nem caminhos "na mão": chamam funções daqui. O motor por trás de
todas elas é o **DuckDB**.

```
MySQL ──mysql_source──► bronze ──(dbt)──► silver ──(dbt)──► gold ──export──► ./data/exports (Power BI)
API ──staging.write_records──► staging ──staging.promote_to_bronze──► bronze
```

## Módulos

| Módulo | Função | Quem usa |
|---|---|---|
| `config.py` | Resolve configurações e segredos (`setting`), descreve o lake (`LakeConfig`) e o MySQL (`MySQLConfig`). | todos |
| `duck.py` | `connect()`: sessão DuckDB já configurada para o MinIO e, opcionalmente, com o MySQL anexado como `mysql_src`. | todos |
| `mysql_source.py` | Lista tabelas, pré-visualiza e **ingere tabelas do MySQL na bronze** (modos `full` e `incremental`). | DAGs `mysql_test_query`, `bronze_mysql_ingestion` |
| `staging.py` | **Staging area**: grava registros de API como Parquet e os promove para a bronze. | DAG `example_api_to_bronze` |
| `export.py` | Exporta datasets da gold para Parquet no host. | DAG `gold_export_powerbi` |
| `bootstrap.py` | Cria os buckets e a regra de expiração da staging (`python -m lakehouse.bootstrap`). | serviço `lake-init` |
| `naming.py` | Identificador de lote (`safe_name`) e data de partição (`partition_date`). | DAGs |

## Convenções de caminho no lake

| Camada | Caminho | Quem grava |
|---|---|---|
| staging | `s3://staging/<fonte>/<entidade>/load_date=AAAA-MM-DD/<lote>.parquet` | `staging.write_records` |
| bronze (APIs) | `s3://bronze/<fonte>/<entidade>/ingestion_date=AAAA-MM-DD/<lote>.parquet` | `staging.promote_to_bronze` |
| bronze (MySQL) | `s3://bronze/mysql/<tabela>/ingestion_date=AAAA-MM-DD/snapshot.parquet` (full) ou `<lote>.parquet` (incremental) | `mysql_source.ingest_table` |
| silver | `s3://silver/<modelo>.parquet` | dbt |
| gold | `s3://gold/<modelo>.parquet` | dbt |

Os nomes dos buckets vêm de `LAKE_BUCKET_*` (`.env`); use `LakeConfig.from_env().uri("gold", "x.parquet")`
em vez de escrever o caminho fixo.

Toda linha que entra na bronze ganha colunas de linhagem: `_ingested_at` (UTC), `_source_system`,
`_source_entity`, `_batch_id` (e `_source_file` quando vem da staging).

## Como as configurações são resolvidas

`setting("NOME")` procura, nesta ordem: variável de ambiente → arquivo `/run/secrets/env/NOME`
(gravado pelo cofre) → valor padrão. Com `required=True`, valor ausente ou ainda `CHANGE_ME` gera
um erro que já diz qual comando do cofre executar.

## Por que a ingestão do MySQL é performática

- A consulta é executada **dentro do MySQL** (`mysql_query` envia a instrução inteira, incluindo
  filtro de marca d'água e lista de colunas).
- As linhas são transmitidas em *streaming* para o motor colunar do DuckDB e gravadas direto em
  Parquet (zstd) no MinIO — nada passa por pandas nem fica inteiro na memória.
- O dbt lê a bronze em Parquet, então as transformações **não voltam a consultar o banco de
  produção**.
- O limite de memória e de threads de cada sessão vem de `DUCKDB_MEMORY_LIMIT` / `DUCKDB_THREADS`.

## Modos de ingestão do MySQL

| Modo | Comportamento | Reexecução |
|---|---|---|
| `full` | copia a tabela inteira a cada execução | sobrescreve o `snapshot.parquet` do dia |
| `incremental` | traz só linhas com `watermark_column` **maior** que o máximo já existente na bronze; a primeira carga é completa | o lote ignora os próprios arquivos ao calcular a marca d'água, então reexecutar extrai as mesmas linhas |

Limitações do modo incremental: não detecta exclusões no MySQL; linhas alteradas só são capturadas
se a coluna de marca d'água mudar (use `updated_at`); usa comparação estrita (`>`), então linhas
gravadas depois com exatamente o mesmo valor do último máximo não entram. Para tabelas pequenas ou
sem coluna confiável, prefira `full`.

## Exemplos

```python
from lakehouse import connect, LakeConfig, setting

con = connect()                                   # DuckDB + MinIO
con.sql("SELECT * FROM read_parquet('s3://gold/gld_example_weather_daily.parquet')").df()

con = connect(mysql=True)                         # + MySQL (somente leitura)
con.sql("SELECT * FROM mysql_query('mysql_src', 'SELECT * FROM clientes LIMIT 5')").show()

token = setting("API_VENDAS_TOKEN", required=True)
```

## Diagnóstico

As funções de ingestão registram no log a instrução enviada ao MySQL e o caminho de destino; em
caso de erro, o log da task no Airflow mostra exatamente o que foi executado.
