# `docker/jupyter/` — imagem do JupyterLab

## Papel na arquitetura

Imagem `caps/jupyter`: o **ambiente de ciência de dados**. É onde se exploram os dados do lake e
se criam e avaliam modelos preditivos em Python, registrando tudo no MLflow.

## Arquivos

| Arquivo | Função |
|---|---|
| `Dockerfile` | `python:<PYTHON_VERSION>-slim` + JupyterLab + bibliotecas; usuário não-root `jovyan` (UID 1000). |
| `requirements.txt` | JupyterLab, DuckDB, pandas, pyarrow, scikit-learn, matplotlib, cliente MLflow. |
| `entrypoint.sh` | Carrega os segredos do cofre e inicia o JupyterLab (exige `JUPYTER_TOKEN`). |

## O que já vem pronto no ambiente

| Recurso | Como usar no notebook |
|---|---|
| Data lake (MinIO) | `from lakehouse import connect; con = connect()` → `con.sql("SELECT ... FROM read_parquet('s3://gold/...')")` |
| MySQL de origem | `connect(mysql=True)` → catálogo `mysql_src` (somente leitura) |
| MLflow | `import mlflow` — `MLFLOW_TRACKING_URI` já aponta para `http://mlflow:5000` |
| Segredos / tokens de API | `from lakehouse import setting; setting("API_X_TOKEN")` |

## Pastas dentro do container

| Caminho | Origem | Persistência |
|---|---|---|
| `/home/jovyan/work` | `./notebooks` (host) | os notebooks ficam no seu disco e podem ir para o Git |
| `/home/jovyan/exports` | `./data/exports` (host) | mesma pasta de exportação usada pelo Airflow |
| `/opt/platform/src` | `./src` (host, somente leitura) | pacote `lakehouse` |

Arquivos gravados **fora** dessas pastas são perdidos quando o container é recriado.

## Acesso

http://localhost:8888 — o token é o segredo `JUPYTER_TOKEN`:

```bash
docker compose run --rm vault get JUPYTER_TOKEN
```

## Como adicionar bibliotecas

1. Inclua o pacote em `requirements.txt` (há exemplos comentados: `xgboost`, `lightgbm`...).
2. `docker compose build jupyter && docker compose up -d jupyter`

Um `pip install` feito dentro do notebook funciona, mas some quando o container é recriado.

## Observação

Mantenha `duckdb==` igual ao das imagens do Airflow e `mlflow==` igual a `MLFLOW_VERSION` do `.env`.
