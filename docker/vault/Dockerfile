# -----------------------------------------------------------------------------
# vault image - tiny Alpine container with SOPS + age.
# It decrypts ./secrets/secrets.enc.yaml into an in-memory volume at start-up
# and doubles as the CLI used to list/get/set/edit secrets.
# Build context: ./docker
# -----------------------------------------------------------------------------
FROM alpine:3.22

ARG SOPS_VERSION=3.13.3
# Provided automatically by BuildKit (amd64 / arm64).
ARG TARGETARCH

RUN set -eux; \
    apk add --no-cache bash age jq nano curl ca-certificates; \
    arch="${TARGETARCH:-}"; \
    if [ -z "${arch}" ]; then \
        case "$(apk --print-arch)" in \
            x86_64)  arch=amd64 ;; \
            aarch64) arch=arm64 ;; \
            *) echo "unsupported architecture: $(apk --print-arch)" >&2; exit 1 ;; \
        esac; \
    fi; \
    curl -fsSL -o /usr/local/bin/sops \
        "https://github.com/getsops/sops/releases/download/v${SOPS_VERSION}/sops-v${SOPS_VERSION}.linux.${arch}"; \
    chmod 0755 /usr/local/bin/sops; \
    sops --version

COPY vault/vault.sh /usr/local/bin/vault
# Strip Windows line endings defensively (the repo may be edited on Windows).
RUN sed -i 's/\r$//' /usr/local/bin/vault && chmod 0755 /usr/local/bin/vault

ENTRYPOINT ["/usr/local/bin/vault"]
CMD ["serve"]
