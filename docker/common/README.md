# `docker/common/` — carregador de segredos compartilhado

## Papel na arquitetura

Contém o script que liga o **cofre** aos **processos**: transforma os arquivos que o serviço
`vault` grava em `/run/secrets/env/` em variáveis de ambiente. É copiado para as imagens
`airflow`, `mlflow` e `jupyter` como `/usr/local/bin/platform-env.sh`.

(Postgres e MinIO não precisam dele: leem o arquivo do segredo diretamente via variáveis `*_FILE`.)

## Arquivo

| Arquivo | Função |
|---|---|
| `platform-env.sh` | Script para ser carregado com `source`. Define `platform_wait_for_vault` (espera o marcador `/run/secrets/.ready`) e `platform_load_secrets` (exporta cada arquivo como variável). |

## Como é acionado dentro dos containers

| Situação | Mecanismo |
|---|---|
| Processo principal do serviço (scheduler, JupyterLab, MLflow) e tudo que ele inicia (tasks, kernels) | o *entrypoint* da imagem faz `source` antes de iniciar o processo |
| `docker compose exec <serviço> bash -c "..."` e scripts bash | variável `BASH_ENV` aponta para o script |
| `docker compose exec <serviço> bash` (interativo) e terminais do Jupyter | linha `source` no `~/.bashrc` |

## Regras do script

- **Idempotente**: marca `PLATFORM_ENV_LOADED=1` e não recarrega.
- **Nunca chama `exit`**: ele roda dentro de outros shells.
- **Variável já definida no container vence** o valor do cofre (permite sobrescrever no compose).
- Preserva valores com espaços, aspas, `$` e múltiplas linhas (lê o arquivo inteiro, sem *parsing*).
- Ignora arquivos cujo nome não seja um nome de variável válido.

## Observação

As variáveis só mudam quando o container é recriado. Depois de alterar um segredo:
`docker compose up -d --force-recreate`.
