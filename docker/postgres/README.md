# `docker/postgres/` — banco de metadados

## Papel na arquitetura

O PostgreSQL **não guarda dados de negócio**. Ele é o banco de metadados local de duas aplicações:

| Banco | Usuário | Quem usa | O que guarda |
|---|---|---|---|
| `airflow` | `airflow` | Airflow | DAGs, execuções, estado das tasks, usuários, *pools*, XComs |
| `mlflow` | `mlflow` | MLflow | experimentos, execuções, parâmetros, métricas, Model Registry |

Os dados ficam no volume Docker `postgres-data`.

A imagem é a oficial (`POSTGRES_IMAGE` no `.env`), sem customização; esta pasta só fornece o script
de inicialização.

## Arquivos

| Arquivo | Função |
|---|---|
| `init/01-create-mlflow-db.sh` | Cria o usuário e o banco `mlflow`. A imagem oficial executa tudo o que está em `/docker-entrypoint-initdb.d` **apenas na primeira inicialização do volume**. |

O usuário/banco `airflow` é criado pela própria imagem (`POSTGRES_USER`, `POSTGRES_DB`), com a
senha lida de `/run/secrets/env/POSTGRES_PASSWORD` (`POSTGRES_PASSWORD_FILE`).

## Observações importantes

- **As senhas são gravadas na criação do volume.** Alterar `POSTGRES_PASSWORD` ou
  `MLFLOW_DB_PASSWORD` no cofre depois disso **não** altera o banco. Para trocar:
  ```bash
  docker compose exec postgres psql -U airflow -d airflow -c "ALTER ROLE airflow PASSWORD 'nova';"
  docker compose run --rm vault set POSTGRES_PASSWORD
  docker compose up -d --force-recreate
  ```
- O Postgres **não é publicado** em porta do host; só os containers da plataforma o acessam.
- Backup: `docker compose exec postgres pg_dumpall -U airflow > backup.sql`
- Recomeçar do zero (apaga histórico do Airflow e do MLflow): `docker compose down -v`.
