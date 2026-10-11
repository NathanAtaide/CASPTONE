# `data/exports/` — arquivos Parquet para o Power BI

## Papel na arquitetura

Destino final do fluxo de dados analíticos: a DAG `gold_export_powerbi` copia a camada **Ouro** do
data lake para cá, em Parquet, **fora do Docker**. É desta pasta que o Power BI importa os dados.

Dentro dos containers ela aparece como `/opt/platform/exports` (Airflow) e `/home/jovyan/exports`
(Jupyter).

## Estrutura gerada

```
data/exports/
└── powerbi/
    ├── gld_example_weather_daily.parquet   # um arquivo por modelo gold
    ├── ...
    └── _manifest.json                      # data/hora (UTC) da exportação e nº de linhas por arquivo
```

- Cada execução **substitui** os arquivos (troca atômica: o Power BI nunca lê um arquivo pela metade).
- Compressão Snappy, a mais compatível com ferramentas de BI.

## Como importar no Power BI Desktop

- Um arquivo: **Obter dados → Parquet** e informe o caminho completo, por exemplo
  `C:\Users\<você>\Downloads\CAPS\data\exports\powerbi\gld_example_weather_daily.parquet`.
- Vários arquivos: **Obter dados → Pasta**, aponte para `...\data\exports\powerbi` e filtre a
  extensão `.parquet`.

Para atualizar o relatório, basta **Atualizar** no Power BI depois que a DAG rodar.

## Quando a exportação acontece

- Automaticamente, sempre que a `dbt_medallion` atualiza a camada gold (a DAG
  `gold_export_powerbi` precisa estar ativa no Airflow).
- Manualmente: *Trigger* na DAG; o parâmetro `datasets` permite exportar só alguns modelos.

## Observações

- Se o Power BI estiver com o arquivo aberto durante a troca, a task falha com uma mensagem
  explicando; rode a DAG novamente após a atualização do relatório.
- Em hosts Linux, a pasta precisa ser gravável pelo usuário do container (`AIRFLOW_UID` no `.env`).
- Os arquivos desta pasta não vão para o Git.
