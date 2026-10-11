# `airflow/dags/` — DAGs da plataforma

## Papel na arquitetura

Cada arquivo `.py` aqui é uma pipeline. As DAGs só **orquestram**: a lógica de dados fica no pacote
[`lakehouse`](../../src/lakehouse/README.md) e as transformações no projeto [`dbt`](../../dbt/README.md).

A pasta é montada em `/opt/airflow/dags`; um arquivo novo ou alterado é detectado sozinho pelo
`airflow-dag-processor` (em geral em menos de um minuto), sem reiniciar nada.

## DAGs

| Arquivo / DAG | Camada | Disparo | O que faz |
|---|---|---|---|
| `mysql_test_query.py` | origem | manual | **Teste de conexão**: lista as tabelas do MySQL e mostra no log as colunas e as primeiras linhas de uma tabela. Não grava nada. |
| `bronze_mysql_ingestion.py` | bronze | *cron* (`config/mysql_tables.yml`) | Ingestão em batelada das tabelas configuradas: MySQL → Parquet em `s3://bronze/mysql/...`. Uma task por tabela. |
| `example_api_to_bronze.py` | staging → bronze | manual | Exemplo do padrão de **staging area**: API pública → Parquet em `s3://staging/...` → importação para `s3://bronze/...`. |
| `dbt_medallion.py` | silver, gold | quando a bronze é atualizada | Roda o dbt: `build` dos modelos `silver` e depois dos `gold` (com testes). Pula os modelos de exemplo enquanto os dados de exemplo não existirem na bronze. |
| `gold_export_powerbi.py` | gold → host | quando a gold é atualizada | Exporta a gold para `./data/exports/powerbi/*.parquet` (fora do Docker). |

Arquivos de apoio:

| Arquivo | Função |
|---|---|
| `caps_assets.py` | Declara os *Assets* (`lake_bronze_mysql`, `lake_bronze_example_api`, `lake_gold`) que encadeiam as DAGs. |
| [`config/`](config/README.md) | Configuração lida pelas DAGs (lista de tabelas do MySQL). |
| `.airflowignore` | Diz ao Airflow quais arquivos desta pasta **não** são DAGs. |

## Primeiro teste (conexão com o MySQL)

1. Configure os segredos `MYSQL_*` no cofre e recrie os containers (ver `secrets/README.md`).
2. No Airflow, abra `mysql_test_query` → **Trigger**. Opcionalmente informe `table` e `limit`.
3. Abra o log da task `preview_table`: aparecem as colunas (com o tipo que o DuckDB atribuiu) e as
   primeiras linhas.

Se os segredos não estiverem preenchidos, a task falha com a mensagem dizendo qual `vault set` executar.

## Roteiro ponta a ponta com o exemplo

1. Ative `dbt_medallion` e `gold_export_powerbi`.
2. Dispare `example_api_to_bronze`.
3. Ao terminar, `dbt_medallion` inicia sozinha; em seguida `gold_export_powerbi`.
4. Resultado: `data/exports/powerbi/gld_example_weather_daily.parquet` no seu disco.

## Como criar uma nova DAG

- **Nova tabela do MySQL:** não precisa de DAG — adicione a tabela em `config/mysql_tables.yml`.
- **Nova API:** copie `example_api_to_bronze.py`, troque `SOURCE`, `ENTITY`, a chamada HTTP e leia
  o token com `setting("MINHA_API_TOKEN", required=True)`. Para o dbt reagir a ela, crie um Asset em
  `caps_assets.py` e inclua-o no `schedule` de `dbt_medallion.py`.
- **Qualquer outra:** crie um `.py` com `from airflow.sdk import dag, task`.

Convenções usadas nas DAGs existentes:

- `import` das bibliotecas pesadas (`lakehouse`, `requests`) **dentro** da task, para o *parsing*
  das DAGs ficar rápido;
- tasks que consultam o MySQL usam `pool="mysql_source"`; tasks de dbt usam `pool="duckdb_warehouse"`;
- o identificador do lote vem do `run_id` e a data de partição de `partition_date(...)`, de modo
  que reexecutar uma task sobrescreve o próprio arquivo em vez de duplicar dados;
- comentários e *docstrings* em inglês; a descrição no topo de cada arquivo aparece na interface do
  Airflow (aba *Docs*).
