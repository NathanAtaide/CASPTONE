# `notebooks/` — notebooks do Jupyter

## Papel na arquitetura

Área de trabalho de **ciência de dados**: exploração dos dados do lake e criação/avaliação de
modelos preditivos. Esta pasta é montada no container `jupyter` em `/home/jovyan/work`, portanto
os notebooks ficam **no seu disco** (e podem ser versionados no Git) — não se perdem quando o
container é recriado.

Os **modelos treinados** e seus parâmetros não ficam aqui: vão para o MLflow (Postgres + MinIO,
ambos em volumes Docker).

## Arquivos

| Arquivo | Função |
|---|---|
| `01_quickstart_lake_mlflow.ipynb` | Roteiro completo: ler a silver com DuckDB → treinar um `RandomForestRegressor` → registrar parâmetros, métricas e modelo no MLflow → carregar o modelo do Model Registry. |

## Como usar

1. Abra http://localhost:8888 e informe o token: `docker compose run --rm vault get JUPYTER_TOKEN`
2. Para o quickstart funcionar, rode antes a DAG `example_api_to_bronze` no Airflow e espere a
   `dbt_medallion` terminar (isso cria os dados de exemplo).
3. Execute as células em ordem e veja o resultado em http://localhost:5000 (experimento
   `example_weather_temperature`).

## O que está disponível em qualquer notebook

```python
from lakehouse import connect, LakeConfig, setting
import mlflow

con = connect()                       # DuckDB já autenticado no MinIO
lake = LakeConfig.from_env()
df = con.sql(f"SELECT * FROM read_parquet('{lake.uri('gold', 'meu_modelo.parquet')}')").df()

con_mysql = connect(mysql=True)       # opcional: MySQL de origem como catálogo mysql_src (leitura)
token = setting("API_X_TOKEN")        # segredos do cofre, sem digitar senha

mlflow.set_experiment("meu_experimento")   # MLFLOW_TRACKING_URI já está configurada
```

## Boas práticas

- **Agregue no DuckDB, não no pandas**: filtre e resuma no SQL e só então chame `.df()`.
- **Registre tudo no MLflow** (`log_params`, `log_metrics`, `log_model`): é o que garante a
  rastreabilidade e a comparação entre versões de modelo.
- Use `registered_model_name=` para versionar o modelo no Model Registry.
- Numere os notebooks (`02_...`, `03_...`) e limpe as saídas antes de versionar, para não gravar
  dados no Git.
- Arquivos que precisem sair do Docker: grave em `/home/jovyan/exports` (= `./data/exports`).
