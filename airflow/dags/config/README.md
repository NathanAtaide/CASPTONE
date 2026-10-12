# `airflow/dags/config/` — configuração das DAGs

## Papel na arquitetura

Separa **o que** é ingerido do **como** é ingerido. Para mudar quais tabelas do MySQL vão para a
camada bronze, como são agrupadas e o tamanho dos lotes, edita-se um YAML — sem escrever Python.

## Arquivos

| Arquivo | Lido por | Conteúdo |
|---|---|---|
| `mysql_tables.yml` | DAG `bronze_mysql_ingestion` | agendamento, padrões de carga e datasets da bronze |

## `mysql_tables.yml`

```yaml
schedule: "0 2 * * *"        # cron no fuso PLATFORM_TIMEZONE; null = só manual

defaults:
  mode: batch                # carga em lotes (padrão)
  chunk_size: 250000         # linhas por lote = linhas por arquivo Parquet
  max_chunks_per_run: 0      # lotes por tabela a cada execução; 0 = até completar

discover: true               # toda tabela não citada abaixo vira um dataset com o próprio nome
exclude: []                  # expressões regulares de tabelas que não devem ser carregadas

datasets:
  - name: emails             # emails, emails_1, emails_2... -> um dataset só
    pattern: "emails(_[0-9]+)?"
  - name: sessions
    pattern: "sessions(_[0-9]+)?"
```

### Como as tabelas viram datasets

- Com `discover: true`, **toda tabela do banco é carregada**. A lista `datasets` só precisa conter
  o que foge do padrão: tabelas a unificar ou com configuração própria.
- `pattern` é uma expressão regular comparada com o **nome inteiro** da tabela. Todas as tabelas
  que casarem são gravadas no mesmo dataset; a coluna `_source_entity` guarda a tabela de origem
  de cada linha. Tabelas novas que casem com o padrão (ex.: `sessions_9`) entram sozinhas.
- `tables: [a, b, c]` é a alternativa explícita ao `pattern`.
- Uma tabela só pode alimentar um dataset; se dois a reivindicarem, a DAG falha na task `plan`.

Para conferir o resultado sem carregar nada, dispare a DAG com `dry_run = true` e leia o log da
task `plan`.

### Campos de um dataset

| Campo | Padrão | Descrição |
|---|---|---|
| `name` | — | nome do dataset na bronze (`s3://bronze/mysql/<name>/`) |
| `pattern` / `tables` | tabela com o mesmo nome | tabelas de origem |
| `mode` | `batch` | `batch`, `full` ou `incremental` (ver abaixo) |
| `chunk_size` | `defaults` | linhas por lote |
| `chunk_column` | detectada | coluna usada para percorrer a tabela em lotes |
| `max_chunks_per_run` | `defaults` | limite de lotes por tabela em cada execução |
| `columns` | todas | colunas a extrair |
| `where` | — | filtro adicional, sintaxe MySQL |
| `watermark_column` | — | só para `mode: incremental` |

### Modos de carga

| Modo | Como funciona | Quando usar |
|---|---|---|
| `batch` | Percorre a tabela na ordem de uma coluna indexada, `chunk_size` linhas por vez, um Parquet por lote. Guarda a posição após cada lote: se parar, continua de onde parou; depois de completa, traz só linhas com chave acima da posição salva. | Padrão. Tabelas grandes e cargas iniciais. |
| `full` | Copia a tabela inteira em uma única consulta, substituindo o `snapshot.parquet` do dia. | Tabelas pequenas que mudam e precisam refletir exclusões. |
| `incremental` | Uma consulta com as linhas de `watermark_column` acima do máximo já existente na bronze. | Tabelas médias com coluna de alteração. |

`full` e `incremental` leem uma única tabela com o mesmo nome do dataset. Para unificar tabelas ou
renomear, use `batch`.

### A coluna de lote (`chunk_column`)

Se não for informada, a DAG escolhe a primeira coluna da chave primária; na falta dela, de um
índice único; depois, de qualquer índice. O log da task `load_table` mostra a escolha.

- Tabela **sem índice nenhum**: não há como dividi-la com eficiência, então é copiada de uma vez.
- Com chave **auto-incremento**, as execuções seguintes trazem as linhas novas, mas **não** as
  linhas antigas que foram alteradas. Para capturar alterações, use uma coluna de data de
  atualização que seja indexada (`chunk_column: updated_at`).
- A coluna deve ser indexada; sem índice, cada lote varre a tabela inteira.
- Exclusões feitas no MySQL não são refletidas. Para refazer a cópia, dispare com `full_refresh`.

### Tamanho do lote

`chunk_size` é a quantidade de linhas por arquivo. Para tabelas com linhas largas (textos longos,
como corpo de e-mail), prefira lotes menores (50 000 a 100 000); para tabelas estreitas, lotes
maiores (500 000 a 1 000 000) reduzem o número de arquivos.

## Depois de alterar o arquivo

1. Salve — o Airflow recarrega a DAG sozinho.
2. Dispare com `dry_run = true` para conferir o plano.
3. Declare o dataset como *source* do dbt em `dbt/models/bronze/_bronze__sources.yml` (o `name`
   da fonte é o nome do **dataset**, não o da tabela) e crie o modelo silver.

Dica: valide o mecanismo com a DAG `bronze_mysql_ingestion_test` antes da primeira carga completa.
