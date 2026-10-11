# `docker/vault/` — imagem do cofre de senhas

## Papel na arquitetura

Container mínimo (Alpine + [SOPS](https://github.com/getsops/sops) + [age](https://github.com/FiloSottile/age))
que é o **primeiro serviço a subir**. Ele descriptografa `secrets/secrets.enc.yaml` para um volume
em memória (`vault-secrets`, montado em `/run/secrets`) e todos os outros serviços só iniciam
depois que ele fica saudável (`depends_on: vault: service_healthy`).

A explicação funcional do cofre (como usar, por que SOPS + age) está em
[`secrets/README.md`](../../secrets/README.md). Aqui fica a parte técnica da imagem.

## Arquivos

| Arquivo | Função |
|---|---|
| `Dockerfile` | Alpine + `bash`, `age`, `jq`, `nano` e o binário do `sops` (versão em `SOPS_VERSION`). |
| `vault.sh` | *Entrypoint* e CLI. Instalado na imagem como `/usr/local/bin/vault`. |

## Comandos do `vault.sh`

| Comando | O que faz |
|---|---|
| `serve` (padrão do serviço) | `init` se necessário → `render` → fica vivo segurando o volume tmpfs. |
| `init` | Gera `keys/age.key` e cria `secrets.enc.yaml` a partir do template (se não existirem). |
| `render` | Descriptografa e grava um arquivo por chave em `/run/secrets/env/` + marcador `.ready`. |
| `list` / `get KEY` / `show` | Consulta nomes, um valor ou tudo. |
| `set KEY [VALOR]` / `unset KEY` / `edit` | Altera o arquivo criptografado. |

Uso: `docker compose run --rm vault <comando>`.

## Detalhes de implementação

- **Por que o container fica vivo:** um volume tmpfs do Docker é desmontado (e apagado) quando o
  último container que o usa para. Mantendo o `vault` rodando, os segredos continuam disponíveis
  para containers que reiniciem depois.
- **Contrato de saída:** `/run/secrets/env/<NOME>` contém o valor cru, sem quebra de linha final,
  com permissão `0444`. `/run/secrets/.ready` só é criado no fim — é ele que o *healthcheck* e os
  *entrypoints* das outras imagens verificam.
- **Texto puro só em RAM:** o JSON descriptografado vive numa variável do shell; arquivos
  temporários do `sops edit`/`init` vão para `/dev/shm` (`TMPDIR`).
- **Geração de senhas:** `__GEN_HEX_<n>__` → `n` bytes de `/dev/urandom` em hexadecimal;
  `__GEN_FERNET__` → 32 bytes em base64 *url-safe* (formato exigido pelo Airflow).
- **Nomes válidos:** apenas `[A-Za-z_][A-Za-z0-9_]*`, pois cada chave vira variável de ambiente.
- **Aviso de pendências:** no log aparecem os nomes das chaves que ainda estão com `CHANGE_ME`.

## Diagnóstico

```bash
docker compose logs vault          # mostra o que foi gerado/renderizado e as pendências
docker compose exec vault ls /run/secrets/env
```

Erro `could not decrypt`: a `keys/age.key` presente não é a que criptografou o arquivo — restaure
a chave correta do backup.
