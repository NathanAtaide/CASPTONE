# `docker/postgres/init/` — scripts de primeira inicialização do Postgres

Esta pasta é montada em `/docker-entrypoint-initdb.d` do container `postgres`. A imagem oficial
executa, em ordem alfabética, os arquivos `.sh` e `.sql` que encontrar aqui — **somente quando o
volume `postgres-data` está vazio** (primeira subida). Arquivos de outros tipos, como este README,
são ignorados.

| Arquivo | Função |
|---|---|
| `01-create-mlflow-db.sh` | Cria o usuário e o banco `mlflow` (backend do MLflow), com a senha `MLFLOW_DB_PASSWORD` lida do cofre. |

Para adicionar outro banco de metadados, crie um novo script numerado (`02-...sh`). Em um volume
já existente o script não roda sozinho: execute os comandos manualmente com
`docker compose exec postgres psql -U airflow -d airflow`.

Contexto geral: [`../README.md`](../README.md).
