# `dbt/models/` — modelos por camada

## Papel na arquitetura

Aqui vive a **arquitetura medalhão** em forma de SQL. Cada subpasta é uma camada e a pasta em que
um modelo está determina onde o resultado é gravado no data lake.

| Pasta | Camada | Conteúdo | Gravado em |
|---|---|---|---|
| [`bronze/`](bronze/README.md) | Bronze | **Fontes** (declaração dos Parquets brutos). O dbt só lê. | — (gravado pelas DAGs de ingestão) |
| [`silver/`](silver/README.md) | Prata | Dados limpos, tipados, sem duplicidade, um modelo por entidade. | bucket `silver` |
| [`gold/`](gold/README.md) | Ouro | Agregações e visões de negócio prontas para BI e ML. | bucket `gold` |

## Regras de dependência

```
bronze (source)  ──►  silver (ref)  ──►  gold
```

- Modelos **silver** leem apenas a bronze (função `source`).
- Modelos **gold** leem apenas a silver ou outros gold (função `ref`) — nunca a bronze direto.

Seguir essa regra mantém a linhagem legível e permite que a DAG rode a silver inteira antes da gold.

## Convenções

| Item | Convenção |
|---|---|
| Nome de modelo | prefixo da camada: `slv_<entidade>`, `gld_<assunto>` |
| Arquivo de fontes | `_bronze__sources.yml` |
| Documentação/testes | um `.yml` ao lado dos modelos, começando por `_` |
| Idioma | SQL e comentários em inglês; nomes de coluna de negócio a critério do time |
| Testes mínimos | `not_null` nas colunas-chave de cada modelo |

Nenhum modelo declara materialização nem caminho: isso vem de `dbt_project.yml` e das macros.

As subpastas `example/` contêm o fluxo de demonstração e podem ser desligadas
(`DBT_ENABLE_EXAMPLES=false`) ou apagadas quando não forem mais úteis.
