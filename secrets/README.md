# `secrets/` — cofre de senhas da plataforma

## Papel na arquitetura

Esta pasta guarda **todas** as credenciais da plataforma (conexão com o MySQL de origem,
senhas internas do Postgres/MinIO/Airflow/Jupyter e tokens de APIs) em **um único arquivo
criptografado**. O serviço `vault` do `docker-compose.yml` descriptografa esse arquivo a cada
`docker compose up` e entrega os valores aos demais containers — você **não digita nenhuma senha
ao iniciar o Docker**.

| Arquivo | O que é | Vai para o Git? |
|---|---|---|
| `secrets.template.yaml` | Modelo em texto puro, **sem segredos reais**. Define quais chaves existem e quais são geradas automaticamente. | Sim |
| `secrets.enc.yaml` | Os segredos de verdade, com os **valores criptografados** (SOPS + age). Criado no primeiro `up`. | Sim (é seguro) |
| `keys/age.key` | Chave privada age que abre o arquivo acima. **É a única coisa que precisa ser protegida.** Criada no primeiro `up`. | **Nunca** (está no `.gitignore`) |

> **Faça backup de `keys/age.key`** (gerenciador de senhas, cofre da empresa, pendrive).
> Sem ela o `secrets.enc.yaml` não pode ser aberto. Com ela, qualquer pessoa abre.

## Como funciona

```
secrets.enc.yaml ──┐
                   ├─► container "vault" (sops decrypt) ─► volume tmpfs /run/secrets (só RAM)
keys/age.key ──────┘                                          ├─ /run/secrets/env/MYSQL_PASSWORD
                                                              ├─ /run/secrets/env/LAKE_S3_SECRET_KEY
                                                              ├─ ...  (um arquivo por segredo)
                                                              └─ /run/secrets/.ready
```

1. **Primeiro `docker compose up`**: o `vault` gera `keys/age.key`, copia o template, troca os
   marcadores `__GEN_HEX_n__` / `__GEN_FERNET__` por valores aleatórios e grava `secrets.enc.yaml`.
2. **Todo `up`**: o `vault` descriptografa o arquivo para um volume **tmpfs** (memória RAM —
   o texto puro nunca é gravado em disco) e fica vivo para manter esse volume montado.
3. **Consumo**: cada container monta o volume em `/run/secrets` (somente leitura):
   - Postgres e MinIO leem direto do arquivo (`POSTGRES_PASSWORD_FILE`, `MINIO_ROOT_*_FILE`);
   - Airflow, Jupyter e MLflow carregam tudo como **variáveis de ambiente** com o mesmo nome da
     chave (script `docker/common/platform-env.sh`);
   - o pacote Python `lakehouse` (`setting("NOME")`) lê da variável de ambiente ou do arquivo.

## Uso no dia a dia

Todos os comandos rodam dentro do container, então funcionam igual em Windows, macOS e Linux:

```bash
docker compose run --rm vault list                 # nomes das chaves (sem valores)
docker compose run --rm vault get AIRFLOW_ADMIN_PASSWORD
docker compose run --rm vault show                 # tudo descriptografado
docker compose run --rm vault set MYSQL_PASSWORD   # pergunta o valor sem ecoar na tela
docker compose run --rm vault set MYSQL_HOST meu-banco.exemplo.com
docker compose run --rm vault edit                 # abre o arquivo inteiro no nano
docker compose run --rm vault unset API_ANTIGA_TOKEN
```

Depois de alterar qualquer segredo, aplique nos containers:

```bash
docker compose up -d --force-recreate
```

### Primeira configuração (obrigatória para usar o MySQL)

```bash
docker compose run --rm vault set MYSQL_HOST
docker compose run --rm vault set MYSQL_DATABASE
docker compose run --rm vault set MYSQL_USER
docker compose run --rm vault set MYSQL_PASSWORD
docker compose up -d --force-recreate
```

### Adicionando o token de uma API nova

```bash
docker compose run --rm vault set API_VENDAS_TOKEN
docker compose up -d --force-recreate
```

Na DAG ou no notebook:

```python
from lakehouse import setting
token = setting("API_VENDAS_TOKEN", required=True)
```

## Por que SOPS + age (e não só Docker Secrets)

Cenários avaliados para o requisito "arquivo criptografado, sem redigitar senhas a cada início":

| Opção | Avaliação |
|---|---|
| Arquivo `.env` | Texto puro em disco e visível em `docker inspect`. Descartado. |
| **Docker Secrets no Compose** | Sem Swarm, um "secret" é apenas um arquivo em **texto puro** montado em `/run/secrets`. Não há criptografia em repouso. Sozinho não atende. |
| Docker Secrets no Swarm | Criptografa em repouso, mas obriga a rodar em modo Swarm e a recadastrar os segredos por comando. Complexidade sem ganho para este cenário. |
| HashiCorp Vault | Robusto, mas precisa de *unseal* a cada reinício (digitar chaves) ou de um KMS de nuvem. Contraria o requisito e é pesado para um ambiente local. |
| **SOPS + age (escolhido)** | Um arquivo versionável com valores criptografados + uma chave local. Sobe sem interação e os nomes das chaves continuam legíveis em *diffs*. |

A solução **reaproveita o contrato do Docker Secrets** (arquivos em `/run/secrets`, variáveis
`*_FILE`), então os containers não sabem de onde o segredo veio. Isso deixa a migração para nuvem
simples:

- **Mesma abordagem com KMS**: o SOPS criptografa com AWS KMS, GCP KMS ou Azure Key Vault no lugar
  da chave age — troca-se apenas o destinatário do arquivo, sem mexer nos serviços.
- **Secrets nativos** (Swarm, Kubernetes, ECS): basta montar os segredos em `/run/secrets/env/` e
  remover o serviço `vault`.

## Limites que você precisa conhecer

- **Modelo de confiança**: todos os containers da plataforma montam o mesmo volume e enxergam todos
  os segredos. É adequado para um ambiente de um time; não isola um serviço do outro.
- **Quem tem acesso ao Docker da máquina lê os segredos** (via `docker exec`). A criptografia
  protege o arquivo em repouso, em backups e no Git — não protege de um administrador da máquina.
- **`POSTGRES_PASSWORD` e `MLFLOW_DB_PASSWORD` só são aplicadas na criação do volume do Postgres.**
  Para trocá-las depois, altere também dentro do banco (`ALTER ROLE ... PASSWORD ...`).
- `AIRFLOW_FERNET_KEY` criptografa conexões/variáveis salvas pela interface do Airflow; trocá-la
  torna ilegível o que já foi salvo.
- Rotação da chave age: `docker compose run --rm --entrypoint sh vault` e, lá dentro,
  `sops rotate` / `sops updatekeys` (consulte a documentação do SOPS).
