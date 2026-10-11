# `data/` — dados que saem do Docker

## Papel na arquitetura

Ponto de troca de arquivos entre a plataforma (dentro do Docker) e o seu computador. **Não é o
data lake**: staging, bronze, silver e gold ficam no MinIO, no volume Docker `minio-data`. Aqui
ficam apenas os arquivos **exportados** para consumo por ferramentas externas, como o Power BI.

## Conteúdo

| Pasta | Função |
|---|---|
| [`exports/`](exports/README.md) | Parquets gerados pela DAG `gold_export_powerbi`. |

## Onde cada dado realmente mora

| Dado | Local | Persiste após `docker compose down`? |
|---|---|---|
| Staging / Bronze / Prata / Ouro / artefatos do MLflow | volume `minio-data` | sim |
| Metadados do Airflow e do MLflow | volume `postgres-data` | sim |
| Catálogo do DuckDB usado pelo dbt | volume `duckdb-data` | sim (recriável) |
| Logs das tasks | volume `airflow-logs` | sim |
| Exportações para BI | `./data/exports` (esta pasta, no host) | sim |
| Notebooks | `./notebooks` (no host) | sim |
| Segredos descriptografados | volume `vault-secrets` (RAM) | **não** (proposital) |

Os volumes só são apagados com `docker compose down -v`.

## Backup dos volumes

```bash
# Exemplo: copiar o conteúdo do lake para um .tar na pasta atual
docker run --rm -v caps_minio-data:/data -v "${PWD}:/backup" alpine tar czf /backup/minio-data.tgz -C /data .
```

O conteúdo desta pasta é ignorado pelo Git (ver `.gitignore`), exceto os arquivos `README.md`.
