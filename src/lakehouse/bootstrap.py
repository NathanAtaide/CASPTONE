"""Create the data-lake buckets (run by the one-shot ``lake-init`` service).

    python -m lakehouse.bootstrap

* waits until the object store answers (it also acts as the readiness gate for
  the services that depend on the lake);
* creates the staging / bronze / silver / gold / mlflow buckets when missing;
* sets an expiration rule on the staging bucket (STAGING_RETENTION_DAYS).

Only the standard S3 API is used, so the same code works against MinIO, any
other S3-compatible server or AWS S3. Safe to run on every start-up.
"""
from __future__ import annotations

import sys
import time

from .config import LakeConfig, setting

WAIT_SECONDS = 180
RETRY_INTERVAL = 3


def _log(message: str) -> None:
    print(f"[lake-init] {message}", flush=True)


def _client(lake: LakeConfig):
    """Build a boto3 S3 client for the lake endpoint."""
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=lake.endpoint_url,
        aws_access_key_id=lake.access_key,
        aws_secret_access_key=lake.secret_key,
        region_name=lake.region,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path" if lake.url_style == "path" else "virtual"},
            retries={"max_attempts": 1, "mode": "standard"},
            connect_timeout=3,
            read_timeout=15,
        ),
    )


def ensure_buckets() -> None:
    """Wait for the object store, then create buckets and the staging lifecycle."""
    lake = LakeConfig.from_env()
    client = _client(lake)

    # 1) Wait for the server (it may still be starting).
    deadline = time.monotonic() + WAIT_SECONDS
    while True:
        try:
            existing = {bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])}
            break
        except Exception as error:  # connection refused, DNS not ready, 503 ...
            if time.monotonic() >= deadline:
                _log(f"ERROR: object store at {lake.endpoint_url} not reachable: {error}")
                sys.exit(1)
            _log(f"waiting for the object store at {lake.endpoint_url} ...")
            time.sleep(RETRY_INTERVAL)

    # 2) One bucket per layer.
    for layer, bucket in lake.buckets.items():
        if bucket in existing:
            _log(f"bucket '{bucket}' ({layer}) already exists")
            continue
        arguments = {"Bucket": bucket}
        if lake.region != "us-east-1":
            arguments["CreateBucketConfiguration"] = {"LocationConstraint": lake.region}
        client.create_bucket(**arguments)
        _log(f"bucket '{bucket}' ({layer}) created")

    # 3) Staging is a transient landing zone: expire old files automatically.
    retention = setting("STAGING_RETENTION_DAYS", "30")
    if retention.isdigit() and int(retention) > 0:
        try:
            client.put_bucket_lifecycle_configuration(
                Bucket=lake.bucket("staging"),
                LifecycleConfiguration={
                    "Rules": [
                        {
                            "ID": "expire-staging-files",
                            "Status": "Enabled",
                            "Filter": {"Prefix": ""},
                            "Expiration": {"Days": int(retention)},
                        }
                    ]
                },
            )
            _log(f"staging files expire after {retention} days")
        except Exception as error:  # not every S3 server implements lifecycle rules
            _log(f"WARNING: could not set the staging retention rule: {error}")

    _log("data lake ready")


if __name__ == "__main__":
    ensure_buckets()
