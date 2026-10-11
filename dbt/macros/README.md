# `dbt/macros/` — macros do projeto dbt

## Papel na arquitetura

Macros são funções Jinja reutilizadas pelos modelos. As deste projeto implementam as **convenções
da arquitetura medalhão** para que cada modelo contenha apenas o seu `SELECT`: onde o resultado é
gravado no lake e como ler a bronze sem duplicidades.

## Arquivos

| Arquivo | Macro | Função |
|---|---|---|
| `generate_schema_name.sql` | `generate_schema_name` | Sobrescreve o padrão do dbt para que o *schema* de um modelo seja exatamente o nome da camada (`silver`, `gold`) em vez de `main_silver`. |
| `external_location.sql` | `external_location` | Sobrescreve a macro do dbt-duckdb que define o destino dos modelos `external`: `s3://<bucket da camada>/<modelo>.parquet`. O bucket vem de `LAKE_BUCKET_SILVER` / `LAKE_BUCKET_GOLD`. |
| `bronze_helpers.sql` | `latest_by_key` | Devolve uma subconsulta com a versão mais recente de cada chave (para tabelas em modo `incremental` e lotes de API). |
| `bronze_helpers.sql` | `latest_snapshot` | Devolve uma subconsulta só com o snapshot mais recente (para tabelas em modo `full`). |

## Por que `latest_by_key` e `latest_snapshot` existem

A bronze é somente-acréscimo: cada ingestão adiciona arquivos e nada é apagado. A mesma linha de
negócio pode, portanto, aparecer várias vezes. A camada silver deve enxergar apenas o estado atual:

| Modo de ingestão da tabela | Macro a usar no `FROM` do modelo silver | Critério |
|---|---|---|
| `incremental` | `latest_by_key(fonte, ['coluna_chave'])` | última linha por chave, ordenando por `_ingested_at` |
| `full` | `latest_snapshot(fonte)` | linhas da partição `ingestion_date` mais recente |

Nos modelos, a chamada é escrita entre chaves duplas do Jinja e seguida de um apelido
(`... as bronze`). Há um exemplo completo em `models/silver/example/slv_example_weather_hourly.sql`
e no `README.md` da raiz do projeto dbt.

## Como as duas primeiras macros trabalham juntas

1. `dbt_project.yml` atribui `+schema: silver` (ou `gold`) conforme a pasta do modelo.
2. `generate_schema_name` faz o *schema* final ser exatamente `silver` / `gold`.
3. A materialização `external` chama `external_location`, que monta o caminho no bucket da camada.

Um modelo colocado fora de `models/silver` ou `models/gold` fica com o *schema* `main` e a execução
falha por não existir esse bucket — é o comportamento esperado para evitar gravações fora das camadas.
