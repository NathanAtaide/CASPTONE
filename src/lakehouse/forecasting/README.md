# `src/lakehouse/forecasting/` — previsão de demanda (Baseline, ML e DL)

## Papel na arquitetura

Integra à plataforma os estudos da pasta `ml_models/` (Baseline, Machine Learning e Deep Learning).
Para cada alvo — **pedidos**, **unidades líquidas** e **faturamento bruto** de Online ES + Online PT —
o pacote retreina todos os modelos candidatos com o mesmo protocolo, mede o erro numa mesma janela,
registra tudo no MLflow, escolhe um **campeão por alvo** e usa o campeão para prever o próximo mês.

```
silver ──Inputs.from_silver──► backtest de cada candidato ──► MLflow (runs + registry)
                                         │                          │
                                         ▼                          ▼
                       gold: gld_forecast_backtest_*      caps_forecast_<alvo>@champion
                             gld_forecast_model_metrics             │
                             gld_forecast_champion                  ▼
                                                          previsão do mês seguinte
                                                          gold: gld_forecast_next_month*
```

Quem chama: as DAGs `ml_forecast_selection` e `ml_forecast_next_month`, pela linha de comando
`python -m lakehouse.forecasting <passo>` dentro do virtualenv `/opt/ml-venv` da imagem do Airflow
(torch CPU, scikit-learn, xgboost, cliente MLflow, holidays). Também pode ser usado num notebook
que tenha essas bibliotecas.

## Protocolo (igual ao dos notebooks)

| Regra | Valor |
|---|---|
| Escopo | Online ES e Online PT, dados desde 2024-01-01 |
| Corte | o mês M é previsto com dados até o último dia de M-2, com retreino mensal |
| Janela do backtest | os últimos `backtest_months` meses completos (padrão 5 = ago–dez/2025, o *Test* dos estudos) |
| Alvos | pedidos com ao menos uma linha SALE e unidades líquidas > 0; unidades = SALE + CANCELLED; faturamento = valor com desconto e sem IVA das linhas SALE + CANCELLED (definições da Baseline e do DL) |
| Métrica de escolha | **WAPE mensal** do total ES + PT; empate técnico até 0,5 pp desempata por \|viés\|, depois WAPE diário, depois RMSE |
| Ranking | **único**: Baseline, ML e DL competem juntos. Os modelos de cenário C usam a mídia paga do mês previsto (premissa: o orçamento de marketing do mês seguinte é conhecido) |

O notebook de ML contava só as linhas SALE; aqui todos os modelos usam a mesma definição para a
comparação ser justa (diferença de cerca de 0,6% nas unidades).

## Candidatos (`catalog.py`)

| Chave | Modelo | Família | Alvos |
|---|---|---|---|
| `b3_monthly` | Baseline 3 mensal — **referência** de todos os estudos | Baseline | todos |
| `b3_daily` | Baseline 3 diária (dia de 364 dias antes × crescimento) | Baseline | todos |
| `ml_c4_rf` | C.4 Random Forest (total diário ES + PT, CV temporal) | ML | todos |
| `ml_d1_gb` | D.1 Gradient Boosting | ML | todos |
| `ml_f1_xgb` | F.1 XGBoost por grupo de categoria | ML | unidades, faturamento |
| `dl_mlp_a` | MLP cenário A (sem mídia paga) | DL | todos |
| `dl_blend_a` | média 50/50 da Baseline 3 mensal e do MLP A | DL | unidades, faturamento |
| `dl_mlp_c` | MLP cenário C (com a mídia paga do mês) | DL | todos |
| `dl_mlp_c_campaigns` | MLP C + campanhas fixas | DL | pedidos |
| `dl_lstm_c_campaigns` | LSTM C + campanhas (janela de 28 dias) | DL | pedidos |

Hiperparâmetros: os escolhidos nos notebooks (não há nova grade). Os modelos de ML refazem a
validação cruzada temporal a cada retreino, como no notebook. As redes usam 5 sementes e a previsão é
a média delas.

A execução reproduz os notebooks: Baseline 3 (pedidos 10,4%, unidades 15,2%, faturamento 12,5%),
MLP C de pedidos 7,8%, MLP C de faturamento 6,8%, média 50/50 de faturamento 8,6%, MLP C de unidades
15,9% (WAPE mensal ES + PT, ago–dez/2025).

## Resultado da seleção de 22/11/2026 (dados até 31/12/2025, backtest ago–dez/2025)

WAPE mensal ES + PT (viés entre parênteses); a Baseline 3 mensal é a referência dos estudos.

| Alvo | Campeão | 2º lugar | Melhor ML/DL | Baseline 3 mensal |
|---|---|---|---|---|
| Pedidos | **LSTM C + campanhas** 6,81% (−6,8%) | Baseline 3 diária 7,38% | — | 10,39% |
| Unidades | **Baseline 3 diária** 8,19% (+6,7%) | F.1 XGBoost 11,71% | F.1 XGBoost 11,71% | 15,16% |
| Faturamento | **MLP C** 6,76% (+0,7%) | Baseline 3 diária 8,50% | — | 12,50% |

Nas redes, o WAPE é o da previsão usada (média das 5 sementes); a média dos WAPE de cada semente,
que os notebooks reportam, fica em `wape_seed_mean` (LSTM de pedidos: sementes de 6,4% a 9,4%).
A seleção completa levou cerca de 20 minutos (o LSTM, 12).

## Arquivos

| Arquivo | Função |
|---|---|
| `protocol.py` | Escopo, regra de corte (M-2), janela do backtest, mês seguinte. |
| `catalog.py` | Candidatos de cada alvo e suas configurações; `CATALOG_VERSION`. |
| `data.py` | `Inputs`: tabela diária por mercado, grupos de categoria e calendário de eventos lidos da silver; extensão para um mês futuro (calendário repetido do ano anterior quando falta, orçamento de mídia distribuído por dia); feriados nacionais. |
| [`models/`](models/README.md) | Baseline 3, modelos de árvore (C.4, D.1, F.1) e redes neurais (MLP, LSTM, média 50/50). |
| `metrics.py` | WAPE, viés, RMSE, R², avaliação de um modelo, *leaderboard* e regra do campeão. |
| `tracking.py` | MLflow: experimentos, runs pai/filhos, gráficos, registro do campeão e o modelo pyfunc `ForecastMethodModel`. |
| `outputs.py` | Esquema e gravação das tabelas `gld_forecast_*` na gold. |
| `pipeline.py` | Passos das DAGs: `prepare`, `run_job`, `select`, `forecast`, `publish_forecast`. |
| `cli.py`, `__main__.py` | Linha de comando `python -m lakehouse.forecasting <passo>`; a última linha do stdout é um JSON (XCom). |

## MLflow

| Item | Conteúdo |
|---|---|
| Experimentos | `caps_forecast_orders`, `caps_forecast_units`, `caps_forecast_sales` |
| Run pai `selection_<execução>` | *leaderboard*, gráfico comparativo, campeão, regra de escolha |
| Runs filhos (um por modelo) | parâmetros do modelo, métricas (`wape_monthly`, `bias_monthly`, `rmse_monthly`, `r2_monthly`, `wape_daily`, ..., `wape_monthly_es/pt`, `wape_seed_*`, `error_pct_AAAA_MM`), previsões, detalhes (parâmetros escolhidos na CV, importâncias, treino) e gráfico |
| Run `forecast_<AAAA-MM>` | previsão do mês seguinte com o orçamento usado |
| Registry | `caps_forecast_<alvo>`; cada seleção registra o campeão como nova versão e move o alias `champion` |

O modelo registrado é um **método** (pyfunc): os modelos dos estudos são retreinados todo mês, então
o que se registra é a receita (chave do modelo + versão do catálogo). Uso:

```python
import mlflow, pandas as pd
model = mlflow.pyfunc.load_model("models:/caps_forecast_sales@champion")
model.predict(pd.DataFrame([{"month": "2026-01", "paid_media_budget_eur": 438070.0}]))
```

`predict` lê a silver, treina com os dados até o corte (M-2) e devolve a previsão mensal e diária
por mercado. Precisa do pacote `lakehouse`, das credenciais do lake e das bibliotecas do
`/opt/ml-venv`.

## Área de trabalho no lake

| Caminho | Conteúdo |
|---|---|
| `s3://gold/_forecast/_state.json` | impressão digital dos dados da última seleção (para pular execuções sem mudança) |
| `s3://gold/_forecast/selection/<execução>/` | `manifest.json`, `inputs/` (os dados que todos os modelos leram) e `results/<alvo>/<modelo>/` |
| `s3://gold/_forecast/next_month/<execução>/<alvo>/` | previsão do mês seguinte antes de ir para a gold |

A pasta `_forecast/` não é exportada para o Power BI (só os `*.parquet` da raiz da gold).

## Tabelas na gold (Power BI)

Gravadas por Python, declaradas como fonte `gold_forecast` do dbt e exportadas pela
`gold_export_powerbi`. Chaves `INTEGER`, métricas como fração (0,08 = 8%), `*_date_key` = AAAAMMDD
(relaciona com `gld_dim_date[date_key]`). A coluna `market` vale `Online ES`, `Online PT` ou
`ES + PT`; filtre `is_total` antes de somar, senão os mercados são contados duas vezes.

| Tabela | Grão |
|---|---|
| `gld_forecast_backtest_monthly` | alvo × modelo × mês do backtest × mercado: previsão, faixa das sementes, real, erro |
| `gld_forecast_backtest_daily` | alvo × modelo × dia × mercado (modelos com previsão diária) |
| `gld_forecast_model_metrics` | alvo × modelo: métricas, posição, campeão, comparação com a Baseline 3, run do MLflow |
| `gld_forecast_champion` | alvo: campeão, vice, melhor modelo de ML/DL, Baseline 3, versão registrada |
| `gld_forecast_next_month` | alvo × mês previsto × mercado: previsão do campeão, orçamento usado, notas |
| `gld_forecast_next_month_daily` | alvo × dia × mercado: previsão diária do campeão e mídia paga do dia |

As tabelas da seleção guardam a última seleção de cada alvo (o histórico fica no MLflow); as do mês
seguinte acumulam um conjunto por alvo e mês previsto (prever o mesmo mês de novo substitui).

No `Parfois Dashboard.pbip`, a página **Forecast** usa `gld_forecast_champion`,
`gld_forecast_model_metrics`, `gld_forecast_backtest_monthly` e `gld_forecast_next_month` (tabelas
`Forecast Target`, `Forecast Model`, `Forecast Backtest` e `Forecast Next Month`) e mostra sempre o
mês previsto mais recente. As tabelas diárias ainda não têm visual. Ver a seção 8 do README principal.

## Limitações

- Os modelos de cenário C dependem do orçamento de mídia paga do mês. No backtest usam o gasto real;
  na previsão, o orçamento informado na DAG, distribuído entre mercados e dias pelo perfil do mesmo
  mês do ano anterior. A diferença entre gasto planejado e real não foi medida.
- A janela tem só 5 meses e uma Black Friday: diferenças de 1 ou 2 pontos entre modelos estão dentro
  do ruído.
- Quando `slv_events` não tem o calendário do mês previsto, o do mesmo mês do ano anterior é repetido
  (datas de calendário); campanhas móveis (Páscoa, Black Friday) podem cair em outros dias.
