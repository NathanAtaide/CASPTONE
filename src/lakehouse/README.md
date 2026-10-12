# `src/lakehouse/` — pacote Python do lakehouse

## Papel na arquitetura

Biblioteca interna que concentra **toda a lógica de acesso a dados** da plataforma. As DAGs e os
notebooks não montam conexões nem caminhos "na mão": chamam funções daqui. O motor por trás de
todas elas é o **DuckDB**.

```
MySQL ──mysql_source──► bronze ──(dbt)──► silver ──(dbt)──► gold ──export──► ./data/exports (Power BI)
API ──staging.write_records──► staging ──staging.promote_to_bronze──► bronze
```

## Módulos

| Módulo | Função | Quem usa |
|---|---|---|
| `config.py` | Resolve configurações e segredos (`setting`), descreve o lake (`LakeConfig`) e o MySQL (`MySQLConfig`). | todos |
| `duck.py` | `connect()`: sessão DuckDB já configurada para o MinIO e, opcionalmente, com o MySQL anexado como `mysql_src`. | todos |
| `mysql_source.py` | Lista tabelas, pré-visualiza e ingere uma tabela do MySQL na bronze em **uma consulta** (modos `full` e `incremental`). Também traz os utilitários de SQL do MySQL usados pelo `mysql_batch`. | DAGs `mysql_test_query`, `bronze_mysql_ingestion` |
| `mysql_batch.py` | **Carga em lotes** do MySQL para a bronze: resolve quais tabelas alimentam quais datasets, percorre cada tabela pela chave, grava um Parquet por lote, guarda a posição e retoma de onde parou. Inclui a conferência de contagens. | DAGs `bronze_mysql_ingestion`, `bronze_mysql_ingestion_test` |
| `objects.py` | Lê e grava pequenos arquivos JSON no lake (estado das cargas). | `mysql_batch` |
| `staging.py` | **Staging area**: grava registros de API como Parquet e os promove para a bronze. | DAG `example_api_to_bronze` |
| `export.py` | Exporta datasets da gold para Parquet no host. | DAG `gold_export_powerbi` |
| `bootstrap.py` | Cria os buckets e a regra de expiração da staging (`python -m lakehouse.bootstrap`). | serviço `lake-init` |
| `naming.py` | Identificador de lote (`safe_name`) e data de partição (`partition_date`). | DAGs |

## Convenções de caminho no lake

| Camada | Caminho | Quem grava |
|---|---|---|
| staging | `s3://staging/<fonte>/<entidade>/load_date=AAAA-MM-DD/<lote>.parquet` | `staging.write_records` |
| bronze (APIs) | `s3://bronze/<fonte>/<entidade>/ingestion_date=AAAA-MM-DD/<lote>.parquet` | `staging.promote_to_bronze` |
| bronze (MySQL, lotes) | `s3://bronze/mysql/<dataset>/ingestion_date=AAAA-MM-DD/<tabela>-<load_id>-<nº do lote>.parquet` | `mysql_batch.load_table` |
| estado das cargas em lotes | `s3://bronze/_state/mysql/<dataset>/<tabela>.json` | `mysql_batch.load_table` |
| bronze (MySQL, consulta única) | `s3://bronze/mysql/<tabela>/ingestion_date=AAAA-MM-DD/snapshot.parquet` (full) ou `<lote>.parquet` (incremental) | `mysql_source.ingest_table` |
| área de teste da carga em lotes | `s3://bronze/_test/mysql/<dataset>/...` (estado em `s3://bronze/_state/_test/mysql/...`) | DAG `bronze_mysql_ingestion_test` |
| silver | `s3://silver/<modelo>.parquet` | dbt |
| gold | `s3://gold/<modelo>.parquet` | dbt |

Os nomes dos buckets vêm de `LAKE_BUCKET_*` (`.env`); use `LakeConfig.from_env().uri("gold", "x.parquet")`
em vez de escrever o caminho fixo.

Toda linha que entra na bronze ganha colunas de linhagem: `_ingested_at` (UTC), `_source_system`,
`_source_entity`, `_batch_id` (e `_source_file` quando vem da staging). A carga em lotes acrescenta
`_load_id`, que identifica uma passagem completa por uma tabela de origem e muda a cada
`full_refresh`.

Um **dataset** da bronze é uma pasta, não um arquivo: a carga em lotes grava vários Parquets nela
e todos são lidos juntos. É assim que tabelas divididas no MySQL (`emails`, `emails_1`, ...) viram
um dataset só — cada linha guarda a tabela de origem em `_source_entity`.

## Como as configurações são resolvidas

`setting("NOME")` procura, nesta ordem: variável de ambiente → arquivo `/run/secrets/env/NOME`
(gravado pelo cofre) → valor padrão. Com `required=True`, valor ausente ou ainda `CHANGE_ME` gera
um erro que já diz qual comando do cofre executar.

## Por que a ingestão do MySQL é performática

- A consulta é executada **dentro do MySQL** (`mysql_query` envia a instrução inteira, incluindo
  filtro de marca d'água e lista de colunas).
- As linhas são transmitidas em *streaming* para o motor colunar do DuckDB e gravadas direto em
  Parquet (zstd) no MinIO — nada passa por pandas nem fica inteiro na memória.
- O dbt lê a bronze em Parquet, então as transformações **não voltam a consultar o banco de
  produção**.
- O limite de memória e de threads de cada sessão vem de `DUCKDB_MEMORY_LIMIT` / `DUCKDB_THREADS`.

## Carga em lotes (`mysql_batch`)

Para tabelas grandes, uma única consulta longa é frágil: se cair, recomeça do zero. A carga em
lotes resolve isso percorrendo a tabela na ordem de uma coluna indexada (por padrão a chave
primária, detectada em `information_schema`):

1. pergunta ao MySQL qual é a chave que está `chunk_size` linhas depois da posição atual;
2. extrai o intervalo `(posição, chave]` e grava um Parquet;
3. salva a nova posição no arquivo de estado e repete.

Consequências práticas:

- **Retomada:** uma falha, um *retry* ou a próxima execução continuam do último lote concluído.
  Um lote interrompido é regravado no mesmo arquivo, sem duplicar.
- **Cargas seguintes:** com a tabela completa, cada execução traz só as linhas de chave acima da
  posição salva.
- **Limite por execução:** `max_chunks` permite carregar poucos lotes por vez.
- **`full_refresh`:** recomeça do zero com um novo `_load_id`; os arquivos antigos permanecem e a
  macro `latest_load` do dbt seleciona a carga mais recente.
- **Linhas com chave nula** são carregadas uma vez, em um lote próprio.
- **Tabela sem índice** não pode ser dividida com eficiência e é copiada de uma só vez.

`mysql_batch.verify_table` compara o `COUNT(*)` do MySQL com as linhas da carga atual na bronze.

## Modos de ingestão do MySQL

| Modo | Comportamento | Reexecução |
|---|---|---|
| `batch` (padrão) | percorre a tabela em lotes pela chave e guarda a posição | continua de onde parou |
| `full` | copia a tabela inteira a cada execução | sobrescreve o `snapshot.parquet` do dia |
| `incremental` | traz só linhas com `watermark_column` **maior** que o máximo já existente na bronze; a primeira carga é completa | o lote ignora os próprios arquivos ao calcular a marca d'água, então reexecutar extrai as mesmas linhas |

Limitações do modo incremental: não detecta exclusões no MySQL; linhas alteradas só são capturadas
se a coluna de marca d'água mudar (use `updated_at`); usa comparação estrita (`>`), então linhas
gravadas depois com exatamente o mesmo valor do último máximo não entram. Para tabelas pequenas ou
sem coluna confiável, prefira `full`.

## Exemplos

```python
from lakehouse import connect, LakeConfig, setting

con = connect()                                   # DuckDB + MinIO
con.sql("SELECT * FROM read_parquet('s3://gold/gld_example_weather_daily.parquet')").df()

con = connect(mysql=True)                         # + MySQL (somente leitura)
con.sql("SELECT * FROM mysql_query('mysql_src', 'SELECT * FROM clientes LIMIT 5')").show()

token = setting("API_VENDAS_TOKEN", required=True)
```

## Diagnóstico

As funções de ingestão registram no log a instrução enviada ao MySQL e o caminho de destino; em
caso de erro, o log da task no Airflow mostra exatamente o que foi executado.
