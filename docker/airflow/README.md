# `docker/airflow/` — imagem do orquestrador

## Papel na arquitetura

Imagem `caps/airflow`, usada por todos os serviços do Airflow e pelo `lake-init`. É onde as
**pipelines executam**: as tasks rodam DuckDB (ingestão e exportação) e dbt (transformações).

Base: imagem oficial `apache/airflow:<AIRFLOW_VERSION>-python<PYTHON_VERSION>`.

## Arquivos

| Arquivo | Função |
|---|---|
| `Dockerfile` | Estende a imagem oficial com DuckDB, dbt (em virtualenv separado) e o carregador de segredos. |
| `requirements.txt` | Pacotes instalados **junto do Airflow** (usados pelas tasks): `duckdb`, `pytz`, `boto3`... |
| `requirements-dbt.txt` | Pacotes do virtualenv **isolado** do dbt (`/opt/dbt-venv`): `dbt-core`, `dbt-duckdb`. |
| `entrypoint.sh` | Espera o cofre, exporta os segredos, cria a conexão `mysql_source` e chama o *entrypoint* oficial. |
| `airflow-db-uri.py` | Imprime a URI do banco de metadados lendo a senha do cofre (usado por `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_CMD`). |
| `airflow-init.sh` | Inicialização única: migra o banco, cria/sincroniza o usuário admin e cria os *pools*. |

## Decisões de projeto

- **dbt em virtualenv separado.** `dbt-core` e Airflow têm dependências que conflitam com
  frequência. O dbt fica em `/opt/dbt-venv` e é chamado pelo caminho `/opt/dbt-venv/bin/dbt`.
- **Extensões do DuckDB baixadas no build** (`httpfs`, `mysql`): os containers não dependem de
  internet para carregá-las. Airflow e dbt usam a mesma versão do DuckDB e compartilham o cache.
- **Segredos do Airflow via `*_CMD`.** A senha do banco, a Fernet key e as chaves de assinatura
  são lidas do volume do cofre no momento do uso. Funciona nos serviços, nos *healthchecks* e em
  `docker compose exec`, e nada aparece em `docker inspect`.
- **Executor `LocalExecutor`**: as tasks rodam como processos do `airflow-scheduler`. Simples e
  suficiente para um nó; para escalar, troca-se por Celery/Kubernetes sem alterar as DAGs.

## Caminhos dentro do container

| Caminho | Origem | Uso |
|---|---|---|
| `/opt/airflow/dags` | `./airflow/dags` (host) | DAGs |
| `/opt/platform/src` | `./src` (host) | pacote Python `lakehouse` (está no `PYTHONPATH`) |
| `/opt/platform/dbt` | `./dbt` (host) | projeto dbt |
| `/opt/platform/exports` | `./data/exports` (host) | Parquets para o Power BI |
| `/opt/platform/duckdb` | volume `duckdb-data` | arquivo `warehouse.duckdb` usado pelo dbt |
| `/opt/platform/dbt-runtime` | efêmero | `target/` e `logs/` do dbt |
| `/opt/airflow/logs` | volume `airflow-logs` | logs das tasks |
| `/run/secrets` | volume `vault-secrets` (RAM) | segredos |

## *Pools* criados pelo `airflow-init.sh`

| Pool | Slots | Motivo |
|---|---|---|
| `mysql_source` | `MYSQL_SOURCE_POOL_SLOTS` (padrão 2) | limita consultas simultâneas ao MySQL de produção |
| `duckdb_warehouse` | 1 | o arquivo DuckDB do dbt aceita um único processo escritor |

## Como adicionar uma biblioteca Python às tasks

1. Adicione em `requirements.txt` (ou em `requirements-dbt.txt` se for um pacote dbt).
2. `docker compose build airflow-init && docker compose up -d`

## Comandos úteis

```bash
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler bash                       # shell com os segredos carregados
docker compose exec airflow-scheduler bash -c 'cd $DBT_PROJECT_DIR && $DBT_BIN debug'
docker compose logs -f airflow-scheduler
```
