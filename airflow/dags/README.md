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
| `nager_date_to_bronze.py` | staging → bronze | *cron* mensal (dia 1, 04:00) | **Feriados públicos** dos países dos marketplaces (29 países, lista em `DEFAULT_COUNTRIES`) via API Nager.Date → `s3://staging/nager_date/public_holidays/...` → `s3://bronze/nager_date/public_holidays/...`. Parâmetros: `countries`, `start_year`, `end_year`. Dispara o dbt. |
| `open_meteo_daily_to_bronze.py` | staging → bronze | *cron* diário (`config/weather_locations.yml`) | **Meteorologia diária** das 3 maiores cidades de cada país dos marketplaces via API Open-Meteo (histórico) → `s3://staging/open_meteo/daily_weather/...` → `s3://bronze/open_meteo/daily_weather/...`. Execuções diárias trazem os últimos `lookback_days` dias; para a carga inicial, disparar com `start_date`. Dispara o dbt. |
| `dbt_medallion.py` | silver, gold | quando a bronze é atualizada | Roda o dbt: `build` dos modelos `silver` e depois dos `gold` (com testes). Pula os modelos de exemplo enquanto os dados de exemplo não existirem na bronze. |
| `gold_export_powerbi.py` | gold → host | quando a gold é atualizada (dbt ou previsão) | Exporta a gold para `./data/exports/powerbi/*.parquet` (fora do Docker). |
| `ml_forecast_selection.py` | silver → gold + MLflow | quando a gold é atualizada (pula se a silver não mudou) | **Seleção dos modelos de previsão**: retreina e testa (backtest dos últimos 5 meses, corte M-2) a Baseline 3 e os modelos de ML e DL de `ml_models/` para pedidos, unidades e faturamento; registra cada modelo como run no MLflow (`caps_forecast_<alvo>`), registra o campeão de cada alvo no *model registry* (alias `champion`) e grava `gld_forecast_backtest_*`, `gld_forecast_model_metrics` e `gld_forecast_champion`. Uma task mapeada por alvo × *job*; roda em `/opt/ml-venv`. Parâmetros: `force`, `backtest_months`, `targets`. |
| `ml_forecast_next_month.py` | gold + MLflow | manual | **Previsão do mês seguinte** com os campeões registrados (retreino até M-2). Parâmetro principal: `paid_media_budget_eur` (orçamento de mídia paga do mês, ES + PT; ex.: 438070 para jan/2026), exigido quando um campeão usa a mídia paga do mês. Grava `gld_forecast_next_month` e `gld_forecast_next_month_daily`. |
| `gold_scd2_simulation.py` | gold (área de simulação) | manual | **Simula uma mudança SCD2** em `gld_dim_product` ou `gld_dim_customer`: fecha/atualiza a versão atual, abre a nova e regrava a `gld_fact_sales` com as chaves daquele membro buscadas de novo pela data do pedido. Grava em `s3://gold/_simulation/<cenário>/<execução>/` e copia o resultado para `./data/exports/scd2_simulation/<cenário>/`; nunca altera a saída do dbt. Lógica em `lakehouse.scd2`. |

Arquivos de apoio:

| Arquivo | Função |
|---|---|
| `caps_assets.py` | Declara os *Assets* (`lake_bronze_mysql`, `lake_bronze_example_api`, `lake_bronze_nager_date`, `lake_bronze_open_meteo_daily`, `lake_gold`, `lake_gold_forecast`) que encadeiam as DAGs. `lake_gold_forecast` é separado de `lake_gold` de propósito: a `ml_forecast_selection` é disparada por `lake_gold`, e produzi-lo criaria um ciclo. |
| [`config/`](config/README.md) | Configuração lida pelas DAGs (lista de tabelas do MySQL, locais da meteorologia). |
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

## Modelos de previsão (ML/DL)

1. Ative `ml_forecast_selection` e dispare com `force = true` na primeira vez (~20 min em 2 CPUs).
   Depois ela roda sozinha a cada atualização da gold e termina em segundos quando a silver não mudou.
2. Compare os modelos no MLflow (http://localhost:5000): experimentos `caps_forecast_orders`,
   `caps_forecast_units`, `caps_forecast_sales` → run `selection_<execução>` → runs filhos → *Compare*.
3. Ative `ml_forecast_next_month` e dispare com o orçamento do mês, por exemplo
   `{"paid_media_budget_eur": 438070}` para janeiro de 2026.
4. Com `gold_export_powerbi` ativa, as tabelas `gld_forecast_*` chegam a `./data/exports/powerbi/`.

Detalhes do protocolo, dos candidatos e das tabelas: [`lakehouse/forecasting`](../../src/lakehouse/forecasting/README.md).

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

