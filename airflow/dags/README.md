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
| `bronze_mysql_ingestion.py` | bronze | *cron* (`config/mysql_tables.yml`) | Ingestão **em lotes** das tabelas do MySQL: percorre cada tabela pela chave e grava um Parquet por lote em `s3://bronze/mysql/<dataset>/...`. Continua de onde parou e unifica tabelas divididas (`emails_1`, `emails_2`...) em um dataset. Uma task por tabela de origem. |
| `bronze_mysql_ingestion_test.py` | bronze (área de teste) | manual | **Teste da carga em lotes** em uma tabela pequena (padrão `product`): mesmo código da DAG acima, gravando em `s3://bronze/_test/mysql/...`, com conferência de contagem MySQL × bronze. Não dispara o dbt. |
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

## Carga da bronze em lotes

Ordem recomendada na primeira vez:

1. **`bronze_mysql_ingestion_test`** → *Trigger* com os valores padrão. Carrega a tabela `product`
   em lotes de 1000 linhas na área de teste. O log da task `verify` mostra as linhas no MySQL, as
   linhas na bronze e as linhas de cada arquivo; a task falha se uma carga completa não bater.
2. Teste a retomada: dispare com `max_chunks = 2` (para após 2 lotes) e depois com
   `max_chunks = 0` e `full_refresh = false` (continua de onde parou).
3. **`bronze_mysql_ingestion`** → *Trigger* com `dry_run = true` e confira no log da task `plan`
   quais tabelas vão para quais datasets.
4. Dispare sem `dry_run`. Para carregar aos poucos, use `datasets` (só alguns datasets) e/ou
   `max_chunks_per_table` (limite de lotes por tabela por execução); cada execução continua da
   posição salva.

A posição de cada tabela fica em `s3://bronze/_state/mysql/<dataset>/<tabela>.json`. Para recomeçar
uma tabela do zero, dispare com `full_refresh = true` (e `datasets` preenchido, para não recarregar
tudo).

## Roteiro ponta a ponta com o exemplo

1. Ative `dbt_medallion` e `gold_export_powerbi`.
2. Dispare `example_api_to_bronze`.
3. Ao terminar, `dbt_medallion` inicia sozinha; em seguida `gold_export_powerbi`.
4. Resultado: `data/exports/powerbi/gld_example_weather_daily.parquet` no seu disco.

## Como criar uma nova DAG

- **Nova tabela do MySQL:** não precisa de DAG. Com `discover: true` em `config/mysql_tables.yml`
  ela já é carregada na próxima execução; edite o arquivo só para unificar tabelas ou mudar o lote.
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
