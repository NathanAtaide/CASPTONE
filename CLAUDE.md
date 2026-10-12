# CAPS — contexto da plataforma para assistentes de IA

Este arquivo é o *briefing* da plataforma de dados CAPS para quem vai desenvolver nela com um
assistente de IA. O Claude Code o carrega sozinho quando é aberto nesta pasta; em outras
ferramentas (chat), anexe ou cole este arquivo junto com o pedido.

**Para o assistente:** você vai criar ou alterar pipelines dentro de uma arquitetura que já
existe. O objetivo é que o resultado se encaixe nela sem retrabalho: mesmos caminhos no lake,
mesmas convenções, mesmas bibliotecas. Com acesso ao repositório, leia também o `README.md` da
pasta em que vai mexer e um arquivo existente parecido antes de escrever. Sem acesso, este arquivo
deve bastar. Você não conhece o esquema do banco de origem: se faltarem nomes de tabelas, colunas,
chaves ou regras de negócio, pergunte em vez de supor.

---

## 1. A plataforma em uma tela

Arquitetura medalhão rodando em Docker Compose. Os dados ficam em Parquet num data lake S3
(MinIO); o DuckDB é o motor de processamento; o dbt gerencia as transformações; o Airflow orquestra.

| Componente | Versão | Papel |
|---|---|---|
| Airflow | 3.3.2 · Python 3.12 · `LocalExecutor` | Orquestra. As tasks rodam no container `airflow-scheduler`. |
| DuckDB | 1.5.6 | Motor de ingestão, transformação e exportação. |
| dbt-core / dbt-duckdb | 1.12.5 / 1.11.0 | Transformações silver e gold. Instalado em `/opt/dbt-venv`, separado do Airflow. |
| MinIO | API S3 | Data lake: buckets `staging`, `bronze`, `silver`, `gold`, `mlflow`. |
| PostgreSQL | 17 | Apenas metadados do Airflow e do MLflow. Nunca dados de negócio. |
| JupyterLab + MLflow | MLflow 3.17.0 | Exploração, treino e versionamento de modelos. |
| Cofre | SOPS + age | Segredos descriptografados em `/run/secrets/env/<NOME>` e exportados como variáveis de ambiente. |

Origem dos dados: um MySQL em nuvem, **somente leitura**, e APIs externas.

## 2. Camadas e caminhos

| Camada | Caminho | Quem grava | Conteúdo |
|---|---|---|---|
| staging | `s3://staging/<fonte>/<entidade>/load_date=AAAA-MM-DD/<lote>.parquet` | DAG de API | Resposta da API como veio. Expira sozinha (30 dias). |
| bronze (API) | `s3://bronze/<fonte>/<entidade>/ingestion_date=AAAA-MM-DD/<lote>.parquet` | DAG de API | Cópia da staging + colunas de linhagem. |
| bronze (MySQL) | `s3://bronze/mysql/<dataset>/ingestion_date=AAAA-MM-DD/<tabela>-<load_id>-<lote>.parquet` | DAG `bronze_mysql_ingestion` | Tabela bruta + colunas de linhagem, um arquivo por lote. |
| silver | `s3://silver/<modelo>.parquet` | dbt | Dado limpo, tipado, sem duplicidade. Um modelo por entidade. |
| gold | `s3://gold/<modelo>.parquet` | dbt | Agregações e visões de negócio. |
| exportação | `./data/exports/powerbi/<modelo>.parquet` (host) | DAG `gold_export_powerbi` | Cópia da gold para o Power BI. |

Fatos que os modelos precisam respeitar:

- A bronze é **somente-acréscimo**: cada ingestão adiciona arquivos. A mesma linha de negócio
  aparece várias vezes, então a silver sempre deduplica (macros da seção 5).
- Colunas de linhagem em toda linha da bronze: `_ingested_at` (timestamp UTC), `_source_system`,
  `_source_entity`, `_batch_id` e, quando veio da staging, `_source_file`. A leitura via dbt também
  expõe `ingestion_date` (vinda do nome da pasta).
- Um **dataset** da bronze é uma pasta com vários Parquets, não um arquivo. Tabelas divididas no
  MySQL são unificadas num dataset só: hoje `emails` reúne `emails`, `emails_1`, `emails_2`... e
  `sessions` reúne `sessions_1`, `sessions_2`... A tabela de origem de cada linha está em
  `_source_entity` (`<banco>.<tabela>`). Os identificadores podem se repetir entre as tabelas de
  origem; confirme com a pessoa antes de usar só o `id` como chave nesses datasets.
- Tabelas do MySQL entram por padrão em modo `batch`: a tabela é percorrida pela chave em lotes,
  a posição é salva em `s3://bronze/_state/mysql/<dataset>/<tabela>.json` e as execuções seguintes
  continuam dali. Essas linhas têm também a coluna `_load_id` (muda a cada recarga completa).
  Existem ainda os modos `full` (snapshot diário em `snapshot.parquet`) e `incremental` (marca
  d'água em uma consulta). A configuração está em `airflow/dags/config/mysql_tables.yml`, com
  `discover: true`: toda tabela do banco vira um dataset, salvo as unificadas.
- `s3://bronze/_test/` é a área da DAG `bronze_mysql_ingestion_test`; não é fonte de dados.
- Os nomes dos buckets vêm de variáveis `LAKE_BUCKET_*`. No código Python use
  `LakeConfig.from_env().uri("silver", "x.parquet")`; não escreva `s3://silver/...` fixo.

## 3. Onde cada tipo de trabalho deve ir

Esta é a decisão mais importante. A maior parte dos pedidos **não** precisa de uma DAG nova.

| Pedido | Onde fazer | DAG nova? |
|---|---|---|
| Limpeza, tipagem, padronização, deduplicação, filtros de qualidade | modelo SQL em `dbt/models/silver/` | Não. A `dbt_medallion` já roda tudo com a tag `silver`. |
| Regra de negócio, junções entre entidades, agregações, indicadores | modelo SQL em `dbt/models/gold/` | Não. Idem, tag `gold`. |
| Validação de dados | `data_tests` no `.yml` do modelo | Não. `dbt build` roda os testes. |
| Trazer mais uma tabela do MySQL | já é carregada sozinha (`discover: true`); declare o dataset em `dbt/models/bronze/_bronze__sources.yml`. Edite `airflow/dags/config/mysql_tables.yml` só para unificar tabelas ou mudar o lote | Não. |
| Nova API ou arquivo externo | nova DAG no padrão staging → bronze (seção 4) + fonte no dbt | Sim. |
| Levar um dado ao Power BI | criar o modelo gold; a exportação é automática | Não. |
| Processamento que SQL não resolve (parsing complexo, aplicar um modelo de ML, chamada externa por linha) | task Python que lê uma camada e grava Parquet na seguinte com DuckDB | Sim. |
| Rodar um subconjunto do dbt em outro horário | DAG com `BashOperator` no padrão da `dbt_medallion.py` | Sim. |
| Função usada por mais de uma DAG ou por notebooks | `src/lakehouse/` | — |

Por que limpeza vai para o dbt e não para tasks Python: o dbt dá linhagem, testes e documentação,
e o DuckDB processa o Parquet inteiro sem carregar tudo em memória. Uma DAG que faz limpeza com
pandas duplica esse papel e não escala.

Quando um dataset silver ou gold for gravado por uma task Python (fora do dbt), declare-o como
*source* do dbt para que os modelos seguintes possam usá-lo, e diga isso na resposta.

## 4. Regras para DAGs (Airflow 3)

O projeto usa a API do Airflow 3. Padrões do Airflow 2 quebram no *parsing*.

- Imports: `from airflow.sdk import dag, task, DAG, Asset, Param, get_current_context`.
  Operadores vêm de `airflow.providers.standard.operators...` (ex.: `BashOperator`).
- Não use: `airflow.decorators`, `airflow.models`, `airflow.operators.bash/python`, `Dataset`,
  `schedule_interval`, `execution_date`, SubDAGs, nem acesso direto ao banco de metadados.
- `logical_date` pode ser `None` em execuções disparadas por asset ou manualmente. Para identificar
  o lote e a data use `dag_run.run_id` e `dag_run.run_after` com os helpers de `lakehouse.naming`.
- Imports pesados (`lakehouse`, `requests`, `duckdb`) ficam **dentro** da task, para o *parsing*
  das DAGs continuar rápido.
- `start_date=datetime(2026, 1, 1)`, `catchup=False`, `max_active_runs=1`, `tags` com a camada,
  `doc_md=__doc__` (o docstring do módulo, em Markdown, aparece na interface).
- Agendamento: *cron* (no fuso `PLATFORM_TIMEZONE`, hoje `America/Sao_Paulo`), `None` para manual,
  ou orientado a dados com Assets.
- **Assets** encadeiam as DAGs e são declarados em `airflow/dags/caps_assets.py`:
  `BRONZE_MYSQL` (`lake_bronze_mysql`), `BRONZE_EXAMPLE_API` (`lake_bronze_example_api`) e `GOLD`
  (`lake_gold`). Quem produz usa `outlets=[...]`; quem consome usa `schedule=[...]` ou
  `schedule=(A | B)`. Para o dbt reagir a uma fonte nova, crie o Asset e inclua-o no `schedule` de
  `dbt_medallion.py`.
- **Pools** (já existem): `pool="mysql_source"` em toda task que consulta o MySQL, para proteger o
  banco de produção; `pool="duckdb_warehouse"` em toda task que roda dbt, porque o arquivo DuckDB
  do dbt aceita um escritor por vez. Tasks que usam `connect()` em memória não precisam de pool.
- **Idempotência:** o caminho de saída deriva do lote (`safe_name(dag_run.run_id)`) e da data de
  partição, de modo que reexecutar uma task sobrescreve o próprio arquivo em vez de duplicar.
- **XCom** carrega apenas resumos pequenos e serializáveis em JSON (caminhos, contagens). Dados
  trafegam pelo lake.
- **Volume** se processa no DuckDB com SQL (`COPY (SELECT ...) TO 's3://...'`). Evite trazer
  tabelas inteiras para pandas ou listas Python.
- **MySQL:** só leitura, via `connect(mysql=True)`. Para o filtro rodar no MySQL use
  `mysql_query('mysql_src', '<SQL do MySQL>')`.
- **Segredos:** `setting("NOME", required=True)`. Nunca escreva valores em código, YAML ou `.env`,
  nem os registre em log. Se a DAG precisar de um segredo novo, informe o comando
  `docker compose run --rm vault set NOME` na resposta.
- **Bibliotecas disponíveis nas tasks:** `duckdb`, `pytz`, `boto3`, `requests`, `PyYAML`, `pandas`
  e os providers da imagem oficial do Airflow. O cliente do MLflow e o scikit-learn só existem no
  Jupyter. Qualquer outra dependência exige incluir em `docker/airflow/requirements.txt` e
  reconstruir a imagem: avise isso explicitamente em vez de assumir que está instalada.
- Arquivos: DAGs em `airflow/dags/<nome>.py`; configuração em `airflow/dags/config/`; um módulo
  auxiliar que não é DAG precisa entrar em `airflow/dags/.airflowignore`.
- As DAGs nascem pausadas. Diga na resposta quais precisam ser ativadas.

Esqueleto de uma DAG de API (staging → bronze):

```python
"""
### my_source_to_bronze - <one-line purpose>

<What it reads, what it writes and when it runs. Rendered in the Airflow UI.>
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from caps_assets import BRONZE_MY_SOURCE  # declare the new Asset in caps_assets.py

log = logging.getLogger(__name__)

SOURCE = "my_source"   # folder of the source system in staging/bronze
ENTITY = "my_entity"   # folder of the dataset


@dag(
    dag_id="my_source_to_bronze",
    description="<short description>",
    schedule="0 3 * * *",  # cron in PLATFORM_TIMEZONE, a list of Assets, or None
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["api", "staging", "bronze"],
    doc_md=__doc__,
)
def my_source_to_bronze():
    @task
    def extract_to_staging(dag_run=None) -> dict:
        """Call the API and land the payload as Parquet in the staging area."""
        # Heavy imports stay inside the task so DAG parsing is fast.
        import requests

        from lakehouse import setting, staging
        from lakehouse.naming import partition_date, safe_name

        token = setting("MY_SOURCE_TOKEN", required=True)  # read from the vault
        response = requests.get(
            "https://api.example.com/items",
            headers={"Authorization": f"Bearer {token}"},
            timeout=60,
        )
        response.raise_for_status()
        records = response.json()["items"]  # list of dicts, one per row

        batch_id = safe_name(dag_run.run_id)
        load_date = str(partition_date(getattr(dag_run, "run_after", None)))
        staged = staging.write_records(
            records, source=SOURCE, entity=ENTITY, batch_id=batch_id, load_date=load_date
        )
        return {**staged, "batch_id": batch_id, "load_date": load_date}

    @task(outlets=[BRONZE_MY_SOURCE])
    def promote_to_bronze(staged: dict) -> dict:
        """Import the staged file into bronze and signal the refresh."""
        from lakehouse import staging

        return staging.promote_to_bronze(
            source=SOURCE,
            entity=ENTITY,
            batch_id=staged["batch_id"],
            load_date=staged["load_date"],
        )

    promote_to_bronze(extract_to_staging())


my_source_to_bronze()
```

Task Python que transforma dados entre camadas (use só quando SQL no dbt não resolver):

```python
@task
def build_dataset(dag_run=None) -> dict:
    from lakehouse import LakeConfig, connect
    from lakehouse.duck import sql_literal

    lake = LakeConfig.from_env()
    source = lake.uri("silver", "slv_orders.parquet")
    target = lake.uri("gold", "gld_orders_scored.parquet")

    con = connect()  # in-memory DuckDB already authenticated on the lake
    try:
        rows = con.execute(
            f"COPY (SELECT * FROM read_parquet({sql_literal(source)})) "
            f"TO {sql_literal(target)} (FORMAT parquet, COMPRESSION zstd)"
        ).fetchone()[0]
    finally:
        con.close()
    return {"rows": int(rows), "target": target}
```

## 5. Regras para o dbt

Projeto `caps_lakehouse` em `dbt/`, adaptador dbt-duckdb, dialeto SQL do DuckDB.

- **A pasta define a camada.** Um modelo em `models/silver/` recebe o *schema* `silver`, a *tag*
  `silver` e é gravado em `s3://silver/<modelo>.parquet`; em `models/gold/`, o equivalente. Isso
  vem de `dbt_project.yml` e das macros `generate_schema_name` e `external_location`. Por isso o
  modelo contém apenas o `SELECT`: não declare `materialized` nem `location`.
- **Dependências:** silver lê só a bronze com `source()`; gold lê só silver ou gold com `ref()`.
  Gold nunca lê a bronze direto.
- **Leitura da bronze sempre por uma das macros:**
  `latest_load(source(...))` para tabelas do MySQL em modo `batch`, o padrão (fica a carga
  `_load_id` mais recente de cada tabela de origem);
  `latest_by_key(source(...), ['chave'])` para tabelas `incremental` e lotes de API (fica a linha
  de `_ingested_at` mais recente por chave); `latest_snapshot(source(...))` para tabelas `full`
  (fica a partição `ingestion_date` mais recente). As macros podem ser aninhadas:
  `latest_by_key(latest_load(source(...)), ['chave'])` quando a tabela `batch` repete linhas
  alteradas.
- **Fontes** são declaradas em `models/bronze/_bronze__sources.yml`. A fonte `bronze_mysql` já tem
  o padrão de caminho e lista os datasets atuais (`customers`, `emails`, `events`, `order_cab`,
  `order_det`, `paid_media`, `product`, `sessions`); o `name` é o nome do **dataset**, não o da
  tabela. Fonte de API nova: copie o bloco `bronze_open_meteo` trocando o caminho.
- **Nomes:** `slv_<entidade>.sql`, `gld_<assunto>.sql`. Ao lado, um `.yml` começando por `_` com a
  descrição do modelo e das colunas e, no mínimo, `data_tests: [not_null]` nas chaves (a chave é
  `data_tests`, não `tests`).
- **Tipos explícitos** com `cast` em toda coluna da silver. Na gold prefira tipos simples
  (inteiros, `double`, `decimal`, `varchar`, `date`, `timestamp`, `boolean`): colunas aninhadas,
  `hugeint` e `uuid` atrapalham o Power BI.
- Cada modelo é reescrito por inteiro a cada execução; não existe modo incremental para a
  materialização `external`.
- A *tag* `example` é reservada aos modelos de demonstração (`models/*/example/`).
- Os `README.md` dentro de `dbt/models/` e `dbt/macros/` não podem conter delimitadores Jinja
  (chaves duplas, chave-porcentagem).

Esqueleto de um modelo silver (`dbt/models/silver/slv_orders.sql`):

```sql
{#
  slv_orders - SILVER
  One row per order: latest version of each id, typed and renamed.
#}
with latest as (

    -- Bronze is append-only: keep only the rows of the latest load of each source table.
    select *
    from {{ latest_load(source('bronze_mysql', 'orders')) }} as bronze

)

select
    cast(id as bigint)                  as order_id,
    cast(customer_id as bigint)         as customer_id,
    cast(total_amount as decimal(18,2)) as total_amount,
    cast(created_at as timestamp)       as created_at,
    _ingested_at
from latest
where id is not null
```

E o `.yml` ao lado (`dbt/models/silver/_orders__silver.yml`):

```yaml
version: 2

models:
  - name: slv_orders
    description: "Orders, de-duplicated and typed."
    columns:
      - name: order_id
        description: "Primary key of the order."
        data_tests:
          - not_null
```

## 6. Pacote `lakehouse` (`src/lakehouse/`)

Disponível nas tasks do Airflow e nos notebooks (`PYTHONPATH`). Use estas funções em vez de montar
conexões e caminhos manualmente; lógica nova e reutilizável entra aqui.

| Chamada | O que faz |
|---|---|
| `setting(name, default=None, *, required=False)` | Lê configuração ou segredo (variável de ambiente → arquivo do cofre → padrão). |
| `LakeConfig.from_env()` → `.uri(layer, *parts)`, `.bucket(layer)`, `.endpoint_url` | Caminhos e credenciais do lake. Camadas: `staging`, `bronze`, `silver`, `gold`, `mlflow`. |
| `connect(*, mysql=False, database=":memory:", read_only=False)` | Conexão DuckDB já autenticada no MinIO. Com `mysql=True`, anexa o MySQL como catálogo `mysql_src`. |
| `duck.sql_literal(v)`, `duck.sql_identifier(n)` | Aspas seguras ao montar SQL com valores e nomes. |
| `mysql_source.list_tables()`, `.preview_table(table, limit=10)` | Inspeção do MySQL. |
| `mysql_source.ingest_table(TableSpec, *, batch_id, ingestion_date)` | Extrai uma tabela para a bronze em uma consulta (modos `full`/`incremental`). |
| `mysql_batch.load_table(dataset, table, *, batch_id, ingestion_date, options=BatchOptions(...))` | Carga em lotes com retomada (modo `batch`). |
| `mysql_batch.resolve_work_items(config, tables)`, `.run_work_item(item, ...)`, `.verify_table(dataset, table)` | Plano tabela → dataset a partir do YAML, execução de um item e conferência de contagens. |
| `objects.read_json(uri)`, `objects.write_json(uri, payload)` | Pequenos arquivos de estado em JSON no lake. |
| `staging.write_records(records, *, source, entity, batch_id, load_date)` | Lista de dicts → Parquet na staging. |
| `staging.promote_to_bronze(*, source, entity, batch_id, load_date)` | Staging → bronze com colunas de linhagem. |
| `export.export_gold(datasets=None, *, subdir="powerbi")`, `export.list_gold_datasets()` | Gold → Parquet no host. |
| `naming.safe_name(run_id)`, `naming.partition_date(dt)`, `naming.utc_timestamp()` | Identificador de lote, data de partição no fuso da plataforma, carimbo UTC. |

Caminhos dentro do container do Airflow: DAGs em `/opt/airflow/dags`, pacote em
`/opt/platform/src`, projeto dbt em `/opt/platform/dbt` (`$DBT_PROJECT_DIR`), binário do dbt em
`$DBT_BIN`, exportações em `/opt/platform/exports` (`$EXPORT_DIR`).

## 7. Convenções do repositório

- **Toda pasta tem um `README.md` em português** explicando o papel dela na arquitetura e cada
  arquivo. Ao criar um arquivo, acrescente-o à tabela do README da pasta; ao criar uma pasta, crie
  o README dela.
- **Código, comentários e docstrings em inglês.**
- Fim de linha LF em tudo o que roda em container.
- `.env` guarda só configuração não sensível. Segredos ficam apenas no cofre.
- Não altere `docker-compose.yml`, Dockerfiles ou `requirements*.txt` para atender um pedido de
  pipeline sem dizer claramente que a mudança exige `docker compose build`.

## 8. Como validar

O assistente pode estar numa sessão sem Docker. Nesse caso não afirme que algo foi testado:
entregue os comandos abaixo para a pessoa rodar na máquina onde a plataforma está no ar e peça a
saída em caso de erro.

```bash
# DAG files that failed to import
docker compose exec airflow-scheduler airflow dags list-import-errors

# Run one DAG end to end, in the foreground
docker compose exec airflow-scheduler airflow dags test <dag_id>

# Build and test one dbt model and everything downstream of it
docker compose exec airflow-scheduler bash -c 'cd $DBT_PROJECT_DIR && $DBT_BIN build --select <model>+'
```

Para conferir um resultado no lake, numa célula do Jupyter:

```python
from lakehouse import LakeConfig, connect

lake = LakeConfig.from_env()
connect().sql(f"SELECT * FROM read_parquet('{lake.uri('silver', '<model>.parquet')}') LIMIT 10").show()
```

Não rode o dbt manualmente enquanto a DAG `dbt_medallion` estiver em execução: o arquivo DuckDB
fica bloqueado para o segundo processo.

**Estado em 12/10/2026 (atualize ou apague este parágrafo):** as imagens constroem e os serviços
sobem. A execução das DAGs, do pacote `lakehouse` e do projeto dbt ainda não foi confirmada num
ambiente real. Trate o código existente como referência de estilo, não como código comprovado; se
um erro apontar para ele, corrija na origem em vez de contornar na DAG nova.

## 9. O que informar ao pedir uma pipeline

Para o pedido sair certo de primeira, inclua:

1. **Objetivo** em uma frase e quem consome o resultado (Power BI, modelo de ML, outra pipeline).
2. **Origem:** nome da tabela no MySQL ou da API, com as **colunas e tipos** (cole o log da task
   `preview_table` da DAG `mysql_test_query`) e algumas linhas de exemplo sem dados sensíveis.
3. **Chave primária** e, se a carga for incremental, a coluna de marca d'água (`id`, `updated_at`).
4. **Regras de limpeza:** o que é valor inválido, como tratar nulos, duplicidades, formatos de
   data, unidades, padronização de textos.
5. **Saída desejada:** nome do modelo, colunas, granularidade (uma linha por quê?) e camada.
6. **Regras de negócio** das agregações (fórmulas, filtros, períodos).
7. **Frequência** e dependências (depois de qual ingestão deve rodar).

Se algum desses itens faltar e mudar o resultado, o assistente deve perguntar antes de escrever.
