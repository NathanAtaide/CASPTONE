# `src/lakehouse/forecasting/models/` — modelos candidatos

## Papel na arquitetura

Implementação dos modelos dos estudos de `ml_models/`, portada dos notebooks sem mudar a lógica
(mesmas variáveis, mesmos hiperparâmetros, mesmas sementes). Cada *job* recebe os `Inputs`, o alvo e
os meses a prever e devolve um ou mais `ModelOutput` com o mesmo formato:

| Tabela | Colunas |
|---|---|
| `daily` | `mark` (Online ES / Online PT / ES + PT), `day`, `forecast`, `seed_min`, `seed_max` |
| `monthly` | `mark`, `month` (primeiro dia), `forecast`, `seed_min`, `seed_max` |
| `seeds` | `mark`, `month`, `seed`, `forecast` (só redes neurais) |

`forecast` é a previsão usada: a média das 5 sementes nas redes. Modelos que trabalham no total
(C.4, D.1, F.1) só têm linhas `ES + PT`; modelos sem previsão diária (Baseline 3 mensal, média 50/50)
têm `daily` vazio.

## Arquivos

| Arquivo | Origem | Conteúdo |
|---|---|---|
| `__init__.py` | — | `ModelOutput` e os conversores para o formato comum. |
| `baseline.py` | `ml_models/Baseline/Baseline_Models_EN.ipynb` (seção 5) | Baseline 3 mensal e diária. |
| `trees.py` | `ml_models/ML/ML sobre silver.ipynb` | C.4 Random Forest, D.1 Gradient Boosting e F.1 XGBoost por grupo de categoria (alvo = desvio face ao ano anterior ajustado pelo crescimento recente; parâmetros escolhidos por CV temporal a cada retreino). |
| `neural.py` | `ml_models/Deep Learning/*` | Tabela de variáveis (`feature_table`), MLP, LSTM e a média 50/50 com a Baseline 3 (PyTorch, CPU, uma thread). |

## Como acrescentar um modelo

1. Implemente a função do *job* aqui (devolvendo `ModelOutput`).
2. Declare o modelo em `catalog.py` (chave, família, cenário, se usa mídia paga, configuração).
3. Ligue o *job* em `pipeline.run_models`.
4. Suba `CATALOG_VERSION`: a próxima seleção roda mesmo sem mudança nos dados.

A DAG `ml_forecast_selection` cria as tasks a partir do catálogo, então não precisa ser alterada.
