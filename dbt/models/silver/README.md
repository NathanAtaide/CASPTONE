# `dbt/models/silver/` — camada Prata

## Papel na arquitetura

A Prata é o dado **tratado e confiável**: uma tabela por entidade de negócio, com tipos corretos, nomes padronizados e sem duplicidades. É a base tanto para a camada Ouro quanto para a exploração e o treinamento de modelos no Jupyter.

Cada modelo desta pasta é gravado em `s3://<bucket silver>/<modelo>.parquet` e recebe automaticamente o *schema* `silver` e a *tag* `silver` (configuração em `dbt_project.yml`).

## O que um modelo silver deve fazer

1. Ler **uma** fonte da bronze (função `source`), passando por `latest_load`, `latest_by_key` ou `latest_snapshot` para eliminar versões antigas.
2. Converter tipos explicitamente (`cast`), padronizar nomes e unidades.
3. Descartar registros inválidos (sem chave, sem data etc.).
4. Manter as colunas de linhagem úteis (`_ingested_at`, `_batch_id`).

O que **não** fazer aqui: agregações e regras de negócio específicas de um relatório (isso é Ouro).

## Conteúdo

| Caminho | Função |
|---|---|
| [`example/`](example/README.md) | Modelo de demonstração (`slv_example_weather_hourly`). |
| `slv_weather_daily.sql` + `_weather__silver.yml` | Meteorologia diária por cidade (3 maiores cidades de cada país dos marketplaces), deduplicada e tipada, a partir de `bronze_open_meteo.daily_weather`. |
| `slv_calendar.sql` | Dimensão de datas (calendário) no padrão Kimball: uma linha por dia, sem lacunas, chaves `date_key` (BIGINT AAAAMMDD, usada nos relacionamentos) e `calendar_date` (DATE). Gerada no DuckDB, não lê a bronze. Inclui feriados nacionais e pontos facultativos federais, dias úteis e rótulos em inglês. |
| `_calendar__silver.yml` | Descrição das colunas de `slv_calendar` e testes (`unique` + `not_null` nas chaves). |
| `slv_customers.sql` + `_customers__silver.yml` | Clientes (`bronze_mysql.customers`), tipados. Chave `customer_id`. |
| `slv_emails.sql` + `_emails__silver.yml` | Envios de e-mail por cliente e dia (`bronze_mysql.emails`), só de clientes existentes. |
| `slv_events.sql` + `_events__silver.yml` | Eventos comerciais por mercado e período (`bronze_mysql.events`), com nomes normalizados e correções de datas. |
| `slv_order_cab.sql` + `_order_cab__silver.yml` | Cabeçalho dos pedidos (`bronze_mysql.order_cab`), um por `order_id`, a partir de 2024. |
| `slv_order_det.sql` + `_order_det__silver.yml` | Linhas dos pedidos, vendas e devoluções (`bronze_mysql.order_det`), sem as linhas repetidas. |
| `slv_paid_media.sql` + `_paid_media__silver.yml` | Investimento em mídia paga por marketplace e dia (`bronze_mysql.paid_media`). |
| `slv_product.sql` + `_product__silver.yml` | Produtos (`bronze_mysql.product`), um por `prod_spk`, sem os itens que não são mercadoria. |
| `slv_sessions.sql` + `_sessions__silver.yml` | Sessões web e app (`bronze_mysql.sessions`), sem sessões de clientes logados inexistentes. |

### Regras de limpeza das tabelas do MySQL

Os comentários dos modelos citam o número de cada regra. Colunas em `snake_case` minúsculo, com o
nome da origem; uma coluna de data e hora vira duas (`<nome>_date` DATE e `<nome>_time` TIME) e
as chaves de data `*_SFK` (número AAAAMMDD) viram DATE com nome de data. Toda coluna de data de
análise tem ao lado uma chave `<nome>_date_key` BIGINT no formato AAAAMMDD (ex.: `order_date_key =
20240922`), que é a coluna dos relacionamentos com `slv_calendar.date_key` na camada Ouro e no
Power BI. A data de nascimento (`birthday`) não tem chave: é atributo do cliente. Cada chave tem um
teste `relationships` com `slv_calendar` em nível de aviso: ele não bloqueia a Ouro, mas mostra as
datas fora do intervalo do calendário (ver "Intervalo da `slv_calendar`"). As conversões usam as
macros de `dbt/macros/cleaning_helpers.sql`: um valor que não pode ser convertido vira nulo em vez
de interromper a execução, e os testes `not_null` acusam o problema.

| Regra | Modelo | O que faz |
|---|---|---|
| 1.1 | `slv_customers` | `CREATION_DATE`, `SUBSCRIPTION_DATE`, `UNSUBSCRIPTION_DATE` → `*_date` + `*_time` (hora nula quando a origem só tem a data); `BIRTHDAY` (19850312) → DATE; `'NaN'` em `COUNTRY` e `SUBSCRIPTION_SOURCE` → nulo. |
| 1.2 | `slv_emails` | `SEND_DATE_SFK` → `send_date`; `OPENS`, `CLICKS`, `UNSUBS` → INTEGER. |
| 1.3 | `slv_events` | `START_DATE`, `END_DATE` → DATE. |
| 1.4 | `slv_order_cab` | `ORDER_ID` continua texto (Franchise é alfanumérico); `ORDER_DATE` → `order_date` + `order_time`. |
| 1.5 | `slv_order_det` | `PROD_QTY` → INTEGER; `MOV_DATE_SFK` → `mov_date`; `AMOUNT_*` → DECIMAL(18,5). |
| 1.6 | `slv_paid_media` | `DATE_SFK` → `paid_media_date`; `AMOUNT_PAID_MEDIA` → DECIMAL(18,3). |
| 1.7 | `slv_product` | `ACTIVE_RECORD` (0/1) e `CAT_ACTIVE` (Y/N) → BOOLEAN; `SEA_COD` → SMALLINT. |
| 1.8 | `slv_sessions` | `DATE_SFK` → `session_date`; `TIMESTAMP` → `session_time` (TIME); `ORDER_ID` (DOUBLE) → texto. |
| 2.1.1 | `slv_order_cab` | Uma linha por `order_id`; se as repetidas diferirem, fica a com status de *fulfillment* e depois a de data mais recente. |
| 2.1.2 | `slv_order_cab` | Remove pedidos com `order_date` anterior a 2024-01-01. |
| 2.2.1 | `slv_order_det` | Remove linhas idênticas em todas as colunas, mantendo uma. |
| 2.2.2 | `slv_order_det` | Remove as devoluções (`TYPE = 'RETURN'`) com `mov_date` em 2024 de pedidos feitos antes de 2024 (pedidos que a regra 2.1.2 tira da `slv_order_cab`). Devoluções de pedidos de 2024 em diante ficam. A data do pedido vem da fonte `order_cab` da bronze. |
| 2.3.1 | `slv_events` | Espaços repetidos no nome do evento viram um só (`Last Days  Christmas` → `Last Days Christmas`). |
| 2.3.2 | `slv_events` | `Free Shipping Singles Day 20` → `Free Shipping Singles Day`. |
| 2.3.3 | `slv_events` | `MARK_COD` nulo → `All` (evento em todos os mercados). |
| 2.3.4 | `slv_events` | Christmas 2025: cada mercado da lista no modelo sem o evento em 2025 recebe uma cópia da sua linha de 2024 com as datas + 1 ano (`is_imputed = true`). As linhas de 2024 continuam. |
| 2.3.5 | `slv_events` | `END_DATE` menor que `START_DATE` → `END_DATE` + 1 ano. |
| 2.4.1 | `slv_sessions` | Remove sessões de clientes logados (`customer_id_fk` diferente de -1) que não existem em customers. Sessões anônimas (-1) ficam. |
| 2.5.1 | `slv_emails` | Remove e-mails cujo `customer_id_fk` não existe em customers (ou é nulo). |
| 2.6.1 | `slv_product` | Remove produtos com `GFS_DES_EN` nulo. |
| 2.6.2 | `slv_product` | Remove produtos com `TAR_DES` nulo, `CAT_DES_EN` preenchido e `FASHIONTYPE` em (`NÃO DEFINIDO`, `Basic`). |
| 2.6.3 | `slv_product` | Remove produtos com `FASHIONTYPE = 'NÃO DEFINIDO'` e `CAT_DES_EN` em (Marketing, Others, INT - BARAM, Consumable, Works, Informática). |
| 2.6.4 | `slv_product` | Remove produtos com `CAT_DES_EN = 'Works'` e `COLLECTION_TYPE` nulo. |
| 2.6.5 | `slv_product` | Remove todos os produtos das categorias Marketing, Consumable, Informática e Others, qualquer que seja o `FASHIONTYPE`. |
| 2.7.1 | `slv_customers` (e, por consequência, `slv_emails` e `slv_sessions`) | Remove clientes com `CREATION_DATE`, `BIRTHDAY`, `UNSUBSCRIPTION_DATE`, `SUBSCRIPTION_DATE`, `SUBSCRIPTION_SOURCE` e `COUNTRY` **todos** nulos (depois da limpeza: `'NaN'` e data ilegível contam como nulo). Macro `customer_has_data`. |

As regras 2.4.1 e 2.5.1 consultam a fonte `customers` da bronze, porque um modelo silver lê só a
bronze. Para as três concordarem sobre quais clientes existem, a CTE `customers` de `slv_emails` e
`slv_sessions` aplica a mesma macro `customer_has_data` de `slv_customers`: e-mails e sessões
logadas dos clientes removidos pela regra 2.7.1 também saem. Uma regra nova que descarte clientes
deve entrar nessa macro (`dbt/macros/customer_helpers.sql`).

A `slv_emails` desliga a otimização `join_filter_pushdown` do DuckDB durante a própria execução
(`pre_hook` / `post_hook`). Com ela ligada, o DuckDB 1.5 falha ao levar o filtro de clientes para a
leitura do dataset `emails`, que une várias tabelas (erro `Can't convert TableFilterType
(BLOOM_FILTER) from global to local indexes`). Retire os *hooks* quando uma versão nova do DuckDB
corrigir o problema.

### Intervalo da `slv_calendar`

Definido por variáveis do dbt (ambas inclusivas): `calendar_start_date` (padrão `2020-01-01`) e `calendar_end_date` (padrão: 31/12 do ano corrente + 5). O bloco `vars` de `dbt_project.yml` fixa hoje o início em `2000-01-01`, para cobrir as datas de cadastro e inscrição dos clientes; se os testes `relationships` das colunas `*_date_key` ainda acusarem datas fora do calendário, antecipe esse valor. Feriados estaduais e municipais não estão incluídos.

Seus modelos ficam diretamente nesta pasta (ou em subpastas por domínio, ex.: `vendas/`), com o prefixo `slv_`.

## Esqueleto de um modelo (tabela do MySQL em modo incremental)

Arquivo `slv_pedidos.sql`: um `select` com os `cast` das colunas, lendo de `latest_by_key(source('bronze_mysql', 'pedidos'), ['id'])` escrito entre chaves duplas do Jinja e seguido de `as bronze`. O exemplo completo, pronto para copiar, está em `dbt/README.md`.

Ao lado, um arquivo `_pedidos__silver.yml` com a descrição das colunas e ao menos o teste `not_null` na chave.

## Execução

A DAG `dbt_medallion` roda `dbt build --select tag:silver`: constrói todos os modelos desta pasta e executa seus testes antes de passar para a Ouro.
