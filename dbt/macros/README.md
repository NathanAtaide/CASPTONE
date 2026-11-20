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
| `bronze_helpers.sql` | `latest_load` | Devolve uma subconsulta só com a carga mais recente de cada tabela de origem (para tabelas em modo `batch`). |
| `bronze_helpers.sql` | `latest_batch_by` | Devolve uma subconsulta com todas as linhas do lote mais recente de cada fatia (ex.: país e ano), para APIs que reenviam a fatia inteira a cada carga. Usada em `slv_holidays`. |
| `cleaning_helpers.sql` | `clean_text` | Remove espaços das pontas e transforma `''`, `'NaN'`, `'None'` e `'null'` (qualquer caixa) em nulo. |
| `cleaning_helpers.sql` | `yyyymmdd_to_date` | Converte um número ou texto AAAAMMDD (`20240922`, `19850312.0`) em DATE; valor inválido vira nulo. |
| `customer_helpers.sql` | `customer_has_data` | Condição da regra 2.7.1: o cliente tem ao menos um dos campos de cadastro preenchido. Usada em `slv_customers`, `slv_emails` e `slv_sessions` para os três concordarem sobre quais clientes existem. |
| `cleaning_helpers.sql` | `date_to_key` | Converte uma DATE na chave BIGINT AAAAMMDD (`2024-09-22` → `20240922`) usada nos relacionamentos com `slv_calendar.date_key`. |
| `cleaning_helpers.sql` | `text_to_date` / `text_to_time` | Separam um texto de data e hora (`AAAA-MM-DD HH:MM:SS`) em DATE e TIME. A hora fica nula quando o texto só tem a data. |
| `gold_helpers.sql` | `event_key` | Normaliza o nome de um evento de `slv_events` na chave da `gld_dim_event` (`Black Friday App 20% all + 50% selected` → `black_friday_app_20pct_all_50pct_selected`). Mesma regra da `gld_ml_sales`. |
| `test_helpers.sql` | teste `unique_combination` | Teste genérico: falha quando duas linhas repetem a combinação de colunas. Usado nas PKs compostas das fatos e, com `config: where`, em "uma versão corrente por chave natural". |
| `test_helpers.sql` | teste `scd2_versions` | Teste genérico das dimensões SCD2: por chave de versão, a primeira versão começa em 1900-01-01, a última termina em 9999-12-31 e cada uma começa no dia seguinte ao fim da anterior. |

## Macros de limpeza (`cleaning_helpers.sql`)

Usadas nos modelos silver para tipar as colunas da origem. Recebem o nome da coluna como texto. Se
o modelo reaproveita o nome da coluna de origem para a coluna de saída (`order_date` vira
`order_date` DATE e `order_time` TIME), qualifique a coluna com o apelido da tabela
(`'latest.order_date'`) para deixar claro que as duas macros leem o texto original. As conversões
usam `try_cast`: um valor fora do formato vira nulo em vez de interromper o `dbt build`, por isso
os modelos têm testes `not_null` nas colunas que não podem ficar vazias.

## Por que `latest_by_key` e `latest_snapshot` existem

A bronze é somente-acréscimo: cada ingestão adiciona arquivos e nada é apagado. A mesma linha de
negócio pode, portanto, aparecer várias vezes. A camada silver deve enxergar apenas o estado atual:

| Modo de ingestão da tabela | Macro a usar no `FROM` do modelo silver | Critério |
|---|---|---|
| `batch` (padrão) | `latest_load(fonte)` | linhas da carga (`_load_id`) mais recente de cada tabela de origem; só remove algo depois de um `full_refresh` |
| `batch` com `chunk_column` de data de alteração | `latest_by_key(latest_load(fonte), ['coluna_chave'])` | idem, e fica a versão mais recente de cada chave |
| `incremental` | `latest_by_key(fonte, ['coluna_chave'])` | última linha por chave, ordenando por `_ingested_at` |
| `full` | `latest_snapshot(fonte)` | linhas da partição `ingestion_date` mais recente |
| API que reenvia fatias completas (ex.: feriados de um país e ano) | `latest_batch_by(fonte, ['coluna_da_fatia', ...])` | todas as linhas do lote (`_batch_id`) mais recente de cada fatia; o que a API deixou de enviar sai da silver |

Em datasets que unificam várias tabelas (`emails`, `sessions`), os identificadores podem se repetir
entre as tabelas de origem. Se for o caso, a chave do modelo silver é a combinação de
`_source_entity` com o identificador.

Nos modelos, a chamada é escrita entre chaves duplas do Jinja e seguida de um apelido
(`... as bronze`). Há um exemplo completo em `models/silver/example/slv_example_weather_hourly.sql`
e no `README.md` da raiz do projeto dbt.

## Como as duas primeiras macros trabalham juntas

1. `dbt_project.yml` atribui `+schema: silver` (ou `gold`) conforme a pasta do modelo.
2. `generate_schema_name` faz o *schema* final ser exatamente `silver` / `gold`.
3. A materialização `external` chama `external_location`, que monta o caminho no bucket da camada.

Um modelo colocado fora de `models/silver` ou `models/gold` fica com o *schema* `main` e a execução
falha por não existir esse bucket — é o comportamento esperado para evitar gravações fora das camadas.
