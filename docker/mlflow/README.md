# `docker/mlflow/` — imagem do MLflow

## Papel na arquitetura

Imagem `caps/mlflow`: o **servidor de tracking e o Model Registry**. É onde ficam o histórico de
experimentos (parâmetros, métricas, tags), as versões dos modelos e os arquivos dos modelos
treinados no Jupyter — garantindo rastreabilidade e comparação entre modelos.

## Arquivos

| Arquivo | Função |
|---|---|
| `Dockerfile` | `python:<PYTHON_VERSION>-slim` + `mlflow==<MLFLOW_VERSION>` + dependências. |
| `requirements.txt` | `psycopg2-binary` (Postgres) e `boto3` (S3/MinIO). |
| `entrypoint.sh` | Carrega os segredos, monta a URI do Postgres e inicia `mlflow server`. |

## Onde cada coisa é persistida

| O quê | Onde | Volume Docker |
|---|---|---|
| Experimentos, execuções, parâmetros, métricas, Model Registry | banco `mlflow` no Postgres | `postgres-data` |
| Arquivos dos modelos e demais artefatos | bucket `s3://mlflow/` no MinIO | `minio-data` |

Os dois volumes sobrevivem a `docker compose down` e à recriação dos containers. Só são apagados
com `docker compose down -v`.

## Como os clientes acessam

- O servidor roda com `--serve-artifacts`: ele **faz proxy** do acesso aos artefatos. O cliente
  (Jupyter, uma task do Airflow) só precisa de `MLFLOW_TRACKING_URI=http://mlflow:5000` — não
  precisa das credenciais do lake.
- Interface web: http://localhost:5000 (porta em `MLFLOW_PORT`).
- `MLFLOW_SERVER_ALLOWED_HOSTS` (definida no compose a partir de `MLFLOW_ALLOWED_HOSTS` do `.env`)
  lista os nomes de host aceitos pelo servidor. Se você acessar por outro nome/IP, adicione-o lá,
  senão o MLflow recusa a requisição.

## Observações

- **Sem autenticação**: o MLflow open source não exige login. Por isso a porta é publicada apenas
  em `127.0.0.1` (`BIND_ADDRESS`). Não exponha em rede sem colocar um proxy com autenticação.
- **Versões alinhadas**: mantenha `MLFLOW_VERSION` (`.env`) igual ao `mlflow==` de
  `docker/jupyter/requirements.txt`.
- **Atualização de versão**: ao subir a versão do MLflow com um banco já existente, rode a
  migração do esquema uma vez:
  `docker compose run --rm --entrypoint bash mlflow -c 'mlflow db upgrade "postgresql+psycopg2://${MLFLOW_DB_USER}:${MLFLOW_DB_PASSWORD}@${POSTGRES_HOST}:5432/${MLFLOW_DB_NAME}"'`
