# `data/exports/` — arquivos Parquet para o Power BI

## Papel na arquitetura

Destino final do fluxo de dados analíticos: a DAG `gold_export_powerbi` copia a camada **Ouro** do
data lake para cá, em Parquet, **fora do Docker**. É desta pasta que o Power BI importa os dados.

Dentro dos containers ela aparece como `/opt/platform/exports` (Airflow) e `/home/jovyan/exports`
(Jupyter).

## Estrutura gerada

```
data/exports/
├── powerbi/                           # gold_export_powerbi: um Parquet por modelo da gold
│   ├── gld_fact_sales.parquet         # star schema (11 arquivos gld_fact_* e gld_dim_*)
│   ├── ...
│   ├── gld_ml_sales.parquet           # outros modelos gold (ML, meteorologia): fora do star schema
│   ├── gld_forecast_*.parquet         # previsão: backtest, métricas e campeão de cada modelo, mês seguinte
│   └── _manifest.json                 # data/hora (UTC) da exportação e nº de linhas por arquivo
└── scd2_simulation/                   # gold_scd2_simulation
    └── <cenário>/
        ├── gld_dim_product.parquet    # (ou gld_dim_customer) última versão simulada da dimensão
        ├── gld_fact_sales.parquet     # fato com as chaves do membro alterado buscadas de novo
        └── _history.json              # mudanças aplicadas no cenário
```

Para ver uma simulação SCD2 no Power BI, use os arquivos de `scd2_simulation/<cenário>/` no lugar
dos mesmos arquivos de `powerbi/` e mantenha as demais tabelas (parâmetro `UsarSimulacao`, descrito
no README do star schema).

- Cada execução **substitui** os arquivos (troca atômica: o Power BI nunca lê um arquivo pela metade).
- Compressão Snappy, a mais compatível com ferramentas de BI.

## Como importar no Power BI Desktop

O modelo do Power BI (quais arquivos carregar, consultas Power Query com parâmetros, tipos,
relacionamentos, tabela de data, colunas ocultas, medidas DAX e simulação SCD2) está descrito em
[`dbt/models/gold/star_schema/README.md`](../../dbt/models/gold/star_schema/README.md), seção
*Integração com o Power BI*. Resumo:

- carregue um arquivo por tabela com o conector **Parquet** (`Parquet.Document(File.Contents(...))`),
  com a pasta num parâmetro (`PastaGold`). Não use *Obter dados → Pasta* com *Combinar arquivos*: ele
  empilharia todas as tabelas numa só;
- carregue só os 11 arquivos `gld_fact_*` e `gld_dim_*`;
- o Power BI Desktop roda no Windows: leve esta pasta até ele (cópia, pasta de rede ou
  OneDrive/SharePoint).

Para atualizar o relatório, basta **Atualizar** no Power BI depois que a DAG rodar.

## Quando a exportação acontece

- Automaticamente, sempre que a `dbt_medallion` atualiza a camada gold ou uma DAG de previsão
  (`ml_forecast_selection`, `ml_forecast_next_month`) grava as tabelas `gld_forecast_*` (a DAG
  `gold_export_powerbi` precisa estar ativa no Airflow). As colunas dessas tabelas estão em
  [`src/lakehouse/forecasting/README.md`](../../src/lakehouse/forecasting/README.md).
- Manualmente: *Trigger* na DAG; o parâmetro `datasets` permite exportar só alguns modelos.

## Observações

- Se o Power BI estiver com o arquivo aberto durante a troca, a task falha com uma mensagem
  explicando; rode a DAG novamente após a atualização do relatório.
- Em hosts Linux, a pasta precisa ser gravável pelo usuário do container: `AIRFLOW_UID` no `.env`
  igual ao `id -u` do dono da pasta (hoje 1000), seguido de `docker compose up -d`.
- Os arquivos desta pasta não vão para o Git.
