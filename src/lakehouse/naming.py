"""Naming helpers shared by every pipeline (batch ids and partition dates)."""
from __future__ import annotations

import re
from datetime import date, datetime, timezone

from .config import setting

_UNSAFE_CHARACTERS = re.compile(r"[^A-Za-z0-9_.-]+")


def safe_name(value: str) -> str:
    """Turn any identifier (e.g. an Airflow run_id) into a file-name-safe token."""
    cleaned = _UNSAFE_CHARACTERS.sub("_", str(value)).strip("_.")
    return cleaned or "batch"


def partition_date(moment: datetime | None = None) -> date:
    """Date used for the ``ingestion_date=`` / ``load_date=`` partitions.

    The moment is converted to PLATFORM_TIMEZONE so a run at 22:00 local time is
    filed under the local calendar day, not under the next UTC day.
    """
    moment = moment or datetime.now(timezone.utc)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    try:
        from zoneinfo import ZoneInfo

        return moment.astimezone(ZoneInfo(setting("PLATFORM_TIMEZONE", "UTC"))).date()
    except Exception:  # unknown timezone or missing tz database -> fall back to UTC
        return moment.astimezone(timezone.utc).date()


def utc_timestamp() -> str:
    """Current UTC time formatted for a DuckDB ``TIMESTAMP '...'`` literal."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
