# `airflow/dags/config/` — configuração das DAGs

## Papel na arquitetura

Separa **o que** é ingerido do **como** é ingerido. Para incluir uma tabela do MySQL na camada
bronze basta editar um YAML — sem escrever Python.

## Arquivos

| Arquivo | Lido por | Conteúdo |
|---|---|---|
| `mysql_tables.yml` | DAG `bronze_mysql_ingestion` | agendamento e lista de tabelas a extrair do MySQL |

## `mysql_tables.yml`

```yaml
schedule: "0 2 * * *"        # cron no fuso PLATFORM_TIMEZONE; null = só manual

tables:
  - name: clientes           # snapshot completo a cada execução
    mode: full

  - name: pedidos            # só linhas novas/alteradas
    mode: incremental
    watermark_column: updated_at

  - name: itens_pedido
    mode: incremental
    watermark_column: id
    columns: [id, pedido_id, produto_id, quantidade, preco_unitario]   # opcional
    where: "quantidade > 0"                                            # opcional, sintaxe MySQL
```

| Campo | Obrigatório | Descrição |
|---|---|---|
| `name` | sim | nome da tabela no banco configurado em `MYSQL_DATABASE` |
| `mode` | não (padrão `full`) | `full` = tabela inteira; `incremental` = apenas linhas acima da marca d'água |
| `watermark_column` | se `incremental` | coluna que só cresce (id auto-incremento, `updated_at`) |
| `columns` | não | colunas a extrair; padrão = todas |
| `where` | não | filtro adicional executado no MySQL |

Cada tabela vira uma task mapeada da DAG e grava em
`s3://bronze/mysql/<tabela>/ingestion_date=AAAA-MM-DD/`.

## Depois de adicionar uma tabela

1. Salve o arquivo — o Airflow recarrega a DAG sozinho.
2. Declare a tabela como *source* do dbt em `dbt/models/bronze/_bronze__sources.yml`.
3. Crie o modelo silver correspondente em `dbt/models/silver/`.

Dica: use a DAG `mysql_test_query` para conferir o nome da tabela e as colunas antes.
