# `docker/` — imagens e scripts de infraestrutura

## Papel na arquitetura

Tudo o que é necessário para **construir os containers** da plataforma. O `docker-compose.yml`
(na raiz) usa esta pasta como *build context* (`context: ./docker`), por isso os Dockerfiles
referenciam os arquivos como `airflow/...`, `common/...` etc.

Nenhum dado e nenhum segredo ficam aqui: só receitas de imagem e scripts de inicialização.

## Conteúdo

| Pasta | Imagem / uso | Serviços do compose |
|---|---|---|
| [`vault/`](vault/README.md) | `caps/vault` — cofre de senhas (SOPS + age) | `vault` |
| [`airflow/`](airflow/README.md) | `caps/airflow` — Airflow + DuckDB + dbt | `airflow-*`, `lake-init` |
| [`mlflow/`](mlflow/README.md) | `caps/mlflow` — servidor de tracking e registro de modelos | `mlflow` |
| [`jupyter/`](jupyter/README.md) | `caps/jupyter` — JupyterLab para ML | `jupyter` |
| [`postgres/`](postgres/README.md) | script de inicialização da imagem oficial `postgres` | `postgres` |
| [`common/`](common/README.md) | script compartilhado que carrega os segredos | copiado para airflow, mlflow e jupyter |

Imagens usadas **sem** customização: `postgres` (oficial) e MinIO (ver abaixo).

## Como as imagens se relacionam

```
                  ┌────────── common/platform-env.sh (carrega segredos) ──────────┐
                  ▼                          ▼                                    ▼
vault ──tmpfs──► airflow (+duckdb, dbt)    mlflow (+psycopg2, boto3)    jupyter (+duckdb, mlflow, sklearn)
/run/secrets      │                          │                                    │
                  └────────── mesma versão do DuckDB e do Python 3.12 ────────────┘
```

- **Uma versão de DuckDB** (`duckdb==1.5.6`) em Airflow, dbt e Jupyter: todos leem e gravam os
  mesmos Parquets e usam as mesmas extensões (`httpfs`, `mysql`), já baixadas no *build*.
- **Um Python** (`PYTHON_VERSION` no `.env`): um modelo treinado no Jupyter pode ser carregado
  por uma task do Airflow sem incompatibilidade de *pickle*.

## Comandos úteis

```bash
docker compose build                 # constrói todas as imagens
docker compose build airflow-init    # reconstrói só a imagem do Airflow
docker compose build --no-cache jupyter
docker compose up -d --build         # constrói o que mudou e sobe
```

Depois de alterar um `requirements*.txt` ou um script desta pasta, é preciso **reconstruir** a
imagem correspondente (as pastas `airflow/dags`, `src`, `dbt` e `notebooks` da raiz são montadas
como volume e **não** exigem rebuild).

Versões das imagens base ficam no `.env` da raiz (`AIRFLOW_VERSION`, `PYTHON_VERSION`,
`MLFLOW_VERSION`, `SOPS_VERSION`, `POSTGRES_IMAGE`, `MINIO_IMAGE`).

## Sobre a imagem do MinIO

Em 2026 a MinIO, Inc. removeu as imagens `minio/minio` do Docker Hub e fechou o acesso anônimo no
Quay. Por isso o `.env` aponta para `cgr.dev/chainguard/minio:latest`: o servidor MinIO
recompilado e distribuído gratuitamente pela Chainguard (sem login).

Pontos de atenção:

- **Só a tag `latest` é gratuita.** Para ter builds reproduzíveis, depois do primeiro `pull` fixe
  a imagem pelo *digest*: `docker image inspect cgr.dev/chainguard/minio:latest --format '{{index .RepoDigests 0}}'`
  e copie o resultado para `MINIO_IMAGE` no `.env`.
- A imagem não tem shell nem `curl`, então o serviço `minio` não tem *healthcheck*: quem garante
  que o lake está pronto é o serviço `lake-init` (tenta conectar até conseguir e cria os buckets).
- O container roda como `root` (`user: "0:0"` no compose) para sempre conseguir gravar no volume
  `minio-data`, independente do UID da imagem escolhida.

Como todo o restante da plataforma fala apenas **API S3**, o MinIO pode ser trocado sem mudar
código — basta alterar `MINIO_IMAGE` (e, se o servidor não for MinIO, as variáveis de credencial do
serviço no compose). Alternativas conhecidas: MinIO AIStor (`quay.io/minio/aistor/minio`, produto
comercial — verifique a licença), RustFS, SeaweedFS, Garage ou, em nuvem, o próprio AWS S3.
