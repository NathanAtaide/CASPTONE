#!/usr/bin/env python3
"""Print the SQLAlchemy URI of the Airflow metadata database.

Airflow calls this script through AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_CMD every
time it needs the connection string. The password is read from the vault tmpfs
(or from the environment when already exported) and URL-encoded, so it is never
stored in docker-compose.yml, in .env or in the image.
"""
import os
import sys
from urllib.parse import quote

SECRETS_DIR = os.path.join(os.environ.get("PLATFORM_SECRETS_ROOT", "/run/secrets"), "env")


def read_setting(name, default=None):
    """Return an environment variable, falling back to the vault file."""
    value = os.environ.get(name)
    if value is None:
        try:
            with open(os.path.join(SECRETS_DIR, name), encoding="utf-8") as handle:
                value = handle.read()
        except OSError:
            value = default
    return value


def main():
    password = read_setting("POSTGRES_PASSWORD")
    if not password:
        sys.exit("POSTGRES_PASSWORD is not available - is the vault service healthy?")

    uri = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
        user=quote(read_setting("POSTGRES_USER", "airflow"), safe=""),
        password=quote(password, safe=""),
        host=read_setting("POSTGRES_HOST", "postgres"),
        port=read_setting("POSTGRES_PORT", "5432"),
        database=read_setting("AIRFLOW_DB_NAME", "airflow"),
    )
    # No trailing newline: Airflow uses the command output verbatim.
    sys.stdout.write(uri)


if __name__ == "__main__":
    main()
