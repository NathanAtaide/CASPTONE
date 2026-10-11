"""Settings, secrets and path conventions of the lakehouse.

Every value is resolved in the same order:

1. environment variable (set by docker-compose or exported from the vault);
2. file rendered by the vault at ``/run/secrets/env/<NAME>``;
3. the default passed by the caller.

Step 2 makes the package self-sufficient: it works in Airflow tasks, Jupyter
kernels and one-off ``docker compose exec``/``run`` commands, even when the
process did not go through an image entrypoint.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# Directory where the vault renders one file per secret (in-memory volume).
SECRETS_DIR = Path(os.environ.get("PLATFORM_SECRETS_ROOT", "/run/secrets")) / "env"

# Value used in secrets.template.yaml for secrets the user still has to fill in.
PLACEHOLDER = "CHANGE_ME"

# Logical layer name -> environment variable holding its bucket name.
_BUCKET_VARIABLES = {
    "staging": "LAKE_BUCKET_STAGING",
    "bronze": "LAKE_BUCKET_BRONZE",
    "silver": "LAKE_BUCKET_SILVER",
    "gold": "LAKE_BUCKET_GOLD",
    "mlflow": "LAKE_BUCKET_MLFLOW",
}


class MissingSettingError(RuntimeError):
    """Raised when a required setting/secret is absent or still a placeholder."""


def setting(name: str, default: str | None = None, *, required: bool = False) -> str | None:
    """Return the value of a setting or secret.

    Args:
        name: variable name, e.g. ``MYSQL_PASSWORD`` or ``API_SALES_TOKEN``.
        default: value returned when the setting is not defined or empty.
        required: raise :class:`MissingSettingError` instead of returning an
            empty value or the ``CHANGE_ME`` placeholder.
    """
    value = os.environ.get(name)
    if value is None:
        secret_file = SECRETS_DIR / name
        if secret_file.is_file():
            value = secret_file.read_text(encoding="utf-8")
    if value is None or value.strip() == "":
        value = default
    if required and (value is None or value == PLACEHOLDER):
        raise MissingSettingError(
            f"'{name}' is not configured. Store it in the vault with "
            f"`docker compose run --rm vault set {name}` and apply it with "
            f"`docker compose up -d --force-recreate`."
        )
    return value


def as_bool(value: str | bool | None) -> bool:
    """Interpret the usual truthy spellings found in environment variables."""
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class LakeConfig:
    """Connection settings and naming conventions of the S3 data lake (MinIO)."""

    endpoint: str        # host:port, without scheme (DuckDB format)
    access_key: str
    secret_key: str
    region: str
    use_ssl: bool
    url_style: str       # "path" for MinIO, "vhost" for AWS S3
    buckets: dict[str, str]

    @classmethod
    def from_env(cls) -> "LakeConfig":
        return cls(
            endpoint=setting("LAKE_S3_ENDPOINT", "minio:9000"),
            access_key=setting("LAKE_S3_ACCESS_KEY", required=True),
            secret_key=setting("LAKE_S3_SECRET_KEY", required=True),
            region=setting("LAKE_S3_REGION", "us-east-1"),
            use_ssl=as_bool(setting("LAKE_S3_USE_SSL", "false")),
            url_style=setting("LAKE_S3_URL_STYLE", "path"),
            buckets={layer: setting(variable, layer) for layer, variable in _BUCKET_VARIABLES.items()},
        )

    @property
    def endpoint_url(self) -> str:
        """Endpoint with scheme, as expected by boto3 and MLflow."""
        scheme = "https" if self.use_ssl else "http"
        return f"{scheme}://{self.endpoint}"

    def bucket(self, layer: str) -> str:
        """Bucket name of a layer (staging, bronze, silver, gold, mlflow)."""
        try:
            return self.buckets[layer]
        except KeyError:
            raise ValueError(f"Unknown lake layer '{layer}'. Expected one of {sorted(self.buckets)}.") from None

    def uri(self, layer: str, *parts: str) -> str:
        """Build an ``s3://`` URI inside a layer, e.g. ``uri("bronze", "mysql", "orders")``."""
        segments = [self.bucket(layer), *(str(part).strip("/") for part in parts if str(part))]
        return "s3://" + "/".join(segments)


@dataclass(frozen=True)
class MySQLConfig:
    """Connection settings of the source MySQL database (read-only)."""

    host: str
    port: int
    database: str
    user: str
    password: str
    ssl_mode: str | None

    @classmethod
    def from_env(cls) -> "MySQLConfig":
        port = setting("MYSQL_PORT", "3306")
        ssl_mode = setting("MYSQL_SSL_MODE")
        return cls(
            host=setting("MYSQL_HOST", required=True),
            port=int(port) if port.isdigit() else 3306,
            database=setting("MYSQL_DATABASE", required=True),
            user=setting("MYSQL_USER", required=True),
            password=setting("MYSQL_PASSWORD", required=True),
            ssl_mode=ssl_mode.strip().lower() if ssl_mode else None,
        )
