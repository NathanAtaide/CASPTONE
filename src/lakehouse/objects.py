"""Small JSON objects stored on the data lake.

Used for pipeline state that must survive between DAG runs (for example the
cursor of a batch load). The data itself is always read and written by DuckDB;
this module only handles tiny control files, through the standard S3 API.
"""
from __future__ import annotations

import json
from typing import Any

from .config import LakeConfig


def s3_client(lake: LakeConfig | None = None):
    """Build a boto3 S3 client for the lake endpoint.

    Same connection settings as ``lakehouse.bootstrap``, with a few retries
    because state files are written while long extractions are running.
    """
    import boto3
    from botocore.config import Config

    lake = lake or LakeConfig.from_env()
    return boto3.client(
        "s3",
        endpoint_url=lake.endpoint_url,
        aws_access_key_id=lake.access_key,
        aws_secret_access_key=lake.secret_key,
        region_name=lake.region,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path" if lake.url_style == "path" else "virtual"},
            retries={"max_attempts": 3, "mode": "standard"},
            connect_timeout=5,
            read_timeout=30,
        ),
    )


def split_uri(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket/some/key`` into ``("bucket", "some/key")``."""
    if not uri.startswith("s3://"):
        raise ValueError(f"Not an s3:// URI: {uri}")
    bucket, _, key = uri[len("s3://"):].partition("/")
    if not bucket or not key:
        raise ValueError(f"URI without bucket or key: {uri}")
    return bucket, key


def read_json(uri: str, client=None) -> dict[str, Any] | None:
    """Return the JSON object stored at ``uri``, or ``None`` when it does not exist."""
    from botocore.exceptions import ClientError

    client = client or s3_client()
    bucket, key = split_uri(uri)
    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in ("NoSuchKey", "404", "NotFound"):
            return None
        raise
    return json.loads(body.decode("utf-8"))


def write_json(uri: str, payload: dict[str, Any], client=None) -> None:
    """Store ``payload`` as a JSON object at ``uri`` (replaces any previous version)."""
    client = client or s3_client()
    bucket, key = split_uri(uri)
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )
