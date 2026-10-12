# `dbt/` — transformações (bronze → silver → gold)

## Papel na arquitetura

Projeto **dbt Core** com o adaptador **dbt-duckdb**. O dbt gerencia as transformações (SQL
versionado, dependências entre modelos, testes e documentação) e o **DuckDB** é o motor que as
executa. Os dados não ficam no DuckDB: ele lê a bronze em Parquet no MinIO e grava silver e gold
de volta no MinIO, também em Parquet.

```
s3://bronze/...  ──source()──►  models/silver/*.sql  ──ref()──►  models/gold/*.sql
   (Parquet)                    s3://silver/<modelo>.parquet      s3://gold/<modelo>.parquet
```

Quem executa: a DAG `dbt_medallion` do Airflow (`dbt build --select tag:silver`, depois `tag:gold`).

## Estrutura

| Caminho | Função |
|---|---|
| `dbt_project.yml` | Configuração do projeto: materialização `external` e, por pasta, o *schema* e a *tag* de cada camada. |
| `profiles.yml` | Conexão: arquivo DuckDB, extensão `httpfs` e credenciais do lake (lidas de variáveis de ambiente). |
| [`macros/`](macros/README.md) | Macros que definem onde cada modelo é gravado e ajudam a ler a bronze. |
| [`models/`](models/README.md) | Os modelos, organizados por camada (`bronze/` = fontes, `silver/`, `gold/`). |

`target/` e `logs/` do dbt são gravados em `/opt/platform/dbt-runtime` dentro do container
(variáveis `DBT_TARGET_PATH` / `DBT_LOG_PATH`), não nesta pasta.

## Como a camada define o destino

A pasta do modelo decide tudo — o modelo em si só contém o `SELECT`:

| Pasta | *schema* | *tag* | Arquivo gerado |
|---|---|---|---|
| `models/silver/` | `silver` | `silver` | `s3://<LAKE_BUCKET_SILVER>/<modelo>.parquet` |
| `models/gold/` | `gold` | `gold` | `s3://<LAKE_BUCKET_GOLD>/<modelo>.parquet` |

Isso é feito pela materialização `external` do dbt-duckdb combinada com a macro
`external_location` deste projeto. Além do Parquet, o dbt cria no arquivo DuckDB uma *view* com o
nome do modelo apontando para ele — é ela que `ref()` usa.

## O arquivo DuckDB

`/opt/platform/duckdb/warehouse.duckdb` (volume `duckdb-data`) é só o **catálogo e a área de
trabalho** do motor. Pode ser apagado sem perda de dados: o próximo `dbt build` recria as *views*.
Ele aceita um único processo escritor, por isso as tasks de dbt usam o *pool* `duckdb_warehouse`.

## Desempenho com dados do MySQL

- O dbt **nunca consulta o MySQL**: lê os Parquets da bronze, que são colunares e comprimidos.
- O DuckDB lê do MinIO apenas as colunas e os grupos de linhas necessários.
- Memória e paralelismo vêm de `DUCKDB_MEMORY_LIMIT` e `DUCKDB_THREADS` (`.env`); quando a memória
  não basta, o DuckDB usa disco temporário em vez de falhar.

## Como adicionar um modelo a partir de uma tabela do MySQL

1. Tabela em `airflow/dags/config/mysql_tables.yml` (ingestão) e em
   `models/bronze/_bronze__sources.yml` (fonte do dbt).
2. Crie `models/silver/slv_pedidos.sql`:

   ```sql
   select
       cast(id as bigint)            as pedido_id,
       cast(cliente_id as bigint)    as cliente_id,
       cast(valor_total as decimal(18, 2)) as valor_total,
       cast(created_at as timestamp) as criado_em
   from {{ latest_load(source('bronze_mysql', 'pedidos')) }} as bronze
   ```

   `latest_load` é a macro para tabelas carregadas em lotes (modo `batch`, o padrão). Para o modo
   `incremental` use `latest_by_key(source(...), ['id'])` e para o modo `full`,
   `latest_snapshot(source(...))`. Detalhes em [`macros/README.md`](macros/README.md).
3. Crie `models/gold/gld_vendas_diarias.sql` usando `{{ ref('slv_pedidos') }}`.
4. Documente e teste em um `.yml` ao lado do modelo (veja os exemplos em `models/*/example/`).

## Rodando o dbt manualmente

```bash
docker compose exec airflow-scheduler bash
cd $DBT_PROJECT_DIR
$DBT_BIN debug                              # valida perfil e conexão
$DBT_BIN build --select slv_pedidos+        # um modelo e seus dependentes
$DBT_BIN build                              # tudo
$DBT_BIN docs generate                      # documentação (em $DBT_TARGET_PATH)
```

Não rode o dbt manualmente enquanto a DAG `dbt_medallion` estiver executando (o arquivo DuckDB
fica bloqueado para o segundo processo).

## Modelos de exemplo

As pastas `models/silver/example/` e `models/gold/example/` trazem um fluxo de demonstração
(clima horário de uma API pública). Para desligá-los, defina `DBT_ENABLE_EXAMPLES=false` no `.env`
e rode `docker compose up -d`.

Eles leem dados que só existem depois que a DAG `example_api_to_bronze` roda. A DAG
`dbt_medallion` verifica isso e **pula os exemplos automaticamente** enquanto esses dados não
existirem, para não atrapalhar os seus modelos. Ao rodar o dbt manualmente nessa situação, use
`$DBT_BIN build --exclude tag:example`.

## Limitação conhecida

A materialização `external` reescreve o arquivo inteiro a cada execução (não há modo incremental
para ela no dbt-duckdb). Para tabelas muito grandes, particione o modelo (opção `partition_by` da
materialização `external`, ver documentação do dbt-duckdb) ou avalie um formato de tabela como
DuckLake/Iceberg.
