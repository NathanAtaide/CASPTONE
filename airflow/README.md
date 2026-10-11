# `airflow/` — orquestração

## Papel na arquitetura

O Airflow é o **orquestrador** da plataforma: decide *quando* e *em que ordem* cada etapa roda —
ingestão em batelada do MySQL, coleta de APIs, transformações dbt e exportação para o Power BI.

Esta pasta contém apenas o que é específico das pipelines (as DAGs). A imagem do Airflow está em
[`docker/airflow/`](../docker/airflow/README.md) e a lógica de dados reutilizável em
[`src/lakehouse/`](../src/lakehouse/README.md).

## Conteúdo

| Pasta | Função |
|---|---|
| [`dags/`](dags/README.md) | As DAGs (arquivos `.py`) e sua configuração. Montada em `/opt/airflow/dags`. |

## Serviços do Airflow no `docker-compose.yml`

| Serviço | Função |
|---|---|
| `airflow-init` | Roda uma vez por `up`: migra o banco, cria o usuário admin e os *pools*. |
| `airflow-apiserver` | Interface web e API — http://localhost:8080 |
| `airflow-scheduler` | Agenda as DAGs e **executa as tasks** (`LocalExecutor`). |
| `airflow-dag-processor` | Lê os arquivos de `dags/` e registra as DAGs. |
| `airflow-triggerer` | Atende tasks *deferrable* (sensores assíncronos). |

Metadados ficam no Postgres (volume `postgres-data`); logs das tasks no volume `airflow-logs`.

## Acesso

- URL: http://localhost:8080
- Usuário: valor de `AIRFLOW_ADMIN_USER` (padrão `admin`)
- Senha: `docker compose run --rm vault get AIRFLOW_ADMIN_PASSWORD`

As DAGs são criadas **pausadas**. Ative (toggle) as que quiser agendar; para testar, use *Trigger*.

## Como as DAGs se encadeiam

O encadeamento é por **Assets** (agendamento orientado a dados), não por horário:

```
bronze_mysql_ingestion ──► asset lake_bronze_mysql ───────┐
                                                          ├─► dbt_medallion ──► asset lake_gold ──► gold_export_powerbi
example_api_to_bronze ───► asset lake_bronze_example_api ─┘
```

Quando uma ingestão termina, o dbt roda; quando o dbt termina, a exportação roda. Para isso
funcionar, as DAGs `dbt_medallion` e `gold_export_powerbi` precisam estar **ativas** (não pausadas).

## Configuração relevante (definida no compose / `.env`)

| Variável | Efeito |
|---|---|
| `PLATFORM_TIMEZONE` | fuso dos agendamentos *cron* e das datas de partição |
| `AIRFLOW_PARALLELISM` | máximo de tasks simultâneas |
| `MYSQL_SOURCE_POOL_SLOTS` | extrações simultâneas contra o MySQL |
| `DUCKDB_MEMORY_LIMIT`, `DUCKDB_THREADS` | recursos de cada sessão DuckDB |
