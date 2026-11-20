"""Simulation of SCD type 2 changes on the gold dimensions of the star schema.

dbt rebuilds ``gld_dim_product`` and ``gld_dim_customer`` from a single snapshot of the source,
so they carry no real history yet. This module applies one simulated change to one member of a
dimension, the way a later load would, and writes the result to a separate area of the lake.
The dbt output is never modified::

    s3://<gold>/_simulation/<scenario>/<batch_id>/<dimension>.parquet     new dimension
    s3://<gold>/_simulation/<scenario>/<batch_id>/gld_fact_sales.parquet  fact, keys re-resolved
    s3://<gold>/_simulation/<scenario>/<batch_id>/change.json             what the run did
    s3://<gold>/_simulation/<scenario>/_state.json                        latest files + history

The Power BI export copies only the top-level ``gold/*.parquet`` files, so ``_simulation`` never
reaches it. :func:`publish` copies the latest files of a scenario to
``<EXPORT_DIR>/scd2_simulation/<scenario>/`` instead (``./data/exports/scd2_simulation/`` on the
host). The simulated fact keeps the keys of every other dimension, so it can be used with the
simulated dimension plus the regular gold files.

Runs of the same scenario build on its latest files, so changes can be chained (version 1 -> 2
-> 3). ``reset`` starts again from the dbt output.

Rules per dimension (the dbt models describe the initial load):

``gld_dim_product`` - natural key ``prod_cod``; type 2 = ``active``, ``fashiontype``,
``active_category``; every other attribute is type 0. A change closes the current row of the
SKU (``valid_to`` = effective date - 1, ``is_current`` = false) and opens a new row with the same
``prod_spk``, new ``prod_sk`` and ``valid_from`` = effective date.

``gld_dim_customer`` - natural key ``customer_id``; type 1 = ``country``,
``subscription_source`` (rewritten in every version); type 0 = ``creation_date``, ``birthday``.
Newsletter (team rules):

* ``unsubscription_date``: the current version is updated in place (status Unsubscribed);
* ``subscription_date`` of a customer who had cancelled: re-subscription, a new version from that
  date (status Subscribed, cancellation cleared in the new version);
* ``subscription_date`` of anyone else: updated in place (status Subscribed).

The dates of the change are the effective dates, so ``effective_date`` is not used here.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

from . import objects
from .config import LakeConfig
from .duck import connect, sql_identifier, sql_literal
from .export import export_directory

log = logging.getLogger(__name__)

SIMULATION_FOLDER = "_simulation"
STATE_FILE = "_state.json"
FACT_MODEL = "gld_fact_sales"
HOST_SUBDIR = "scd2_simulation"
OPEN_END = date(9999, 12, 31)

# Columns maintained by the SCD2 logic itself; a change can never set them.
MANAGED_COLUMNS = ("valid_from", "valid_to", "is_current", "is_inferred", "newsletter_status")


@dataclass(frozen=True)
class DimensionSpec:
    """How one gold dimension is versioned and how the fact points to it."""

    model: str                      # gold model (= Parquet file name)
    surrogate_key: str              # PK of the dimension, FK in the fact
    natural_key: str                # business key used to pick the member to change
    natural_key_kind: str           # "text" or "int"
    version_key: str                # column the fact looks up together with the order date
    type0: tuple[str, ...]          # never change
    type1: tuple[str, ...]          # overwritten in every version
    type2: tuple[str, ...]          # a change opens a new version (or follows custom rules)
    column_kinds: dict[str, str]    # "text" | "bool" | "date", to parse the values of a change
    needs_effective_date: bool

    @property
    def changeable(self) -> tuple[str, ...]:
        return self.type1 + self.type2


DIMENSIONS: dict[str, DimensionSpec] = {
    "gld_dim_product": DimensionSpec(
        model="gld_dim_product",
        surrogate_key="prod_sk",
        natural_key="prod_cod",
        natural_key_kind="text",
        version_key="prod_spk",
        type0=(
            "prod_spk", "prod_cod", "cat_des_en", "gfa_des_en", "gfs_des_en",
            "collection_type", "sea_cod", "target_description",
        ),
        type1=(),
        type2=("active", "fashiontype", "active_category"),
        column_kinds={"active": "bool", "fashiontype": "text", "active_category": "bool"},
        needs_effective_date=True,
    ),
    "gld_dim_customer": DimensionSpec(
        model="gld_dim_customer",
        surrogate_key="customer_sk",
        natural_key="customer_id",
        natural_key_kind="int",
        version_key="customer_id",
        type0=("customer_id", "creation_date", "birthday"),
        type1=("country", "subscription_source"),
        type2=("subscription_date", "unsubscription_date"),
        column_kinds={
            "country": "text",
            "subscription_source": "text",
            "subscription_date": "date",
            "unsubscription_date": "date",
        },
        needs_effective_date=False,
    ),
}


# ---------------------------------------------------------------------------------------
# Parsing and small helpers
# ---------------------------------------------------------------------------------------

def _spec(dimension: str) -> DimensionSpec:
    try:
        return DIMENSIONS[dimension]
    except KeyError:
        raise ValueError(f"Unknown dimension '{dimension}'. Expected one of {sorted(DIMENSIONS)}.") from None


def _parse_date(value: Any, name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError:
        raise ValueError(f"{name}: '{value}' is not a date in the YYYY-MM-DD format.") from None


def _parse_bool(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"{name}: '{value}' is not a boolean (use true or false).")


def _parse_value(kind: str, value: Any, name: str) -> Any:
    if value is None:
        raise ValueError(f"{name}: a value is required (null is not a change that can be simulated).")
    if kind == "bool":
        return _parse_bool(value, name)
    if kind == "date":
        return _parse_date(value, name)
    text = str(value).strip()
    if not text:
        raise ValueError(f"{name}: the value cannot be empty.")
    return text


def _parse_changes(spec: DimensionSpec, changes: Any) -> dict[str, Any]:
    """Validate a change against the SCD type of each column and parse its values."""
    if isinstance(changes, str):
        changes = json.loads(changes or "{}")
    if not isinstance(changes, dict) or not changes:
        raise ValueError('changes must be a non-empty object, e.g. {"fashiontype": "Basic"}.')
    parsed: dict[str, Any] = {}
    for column, value in changes.items():
        if column in spec.type0:
            raise ValueError(f"{column} is SCD type 0 in {spec.model}: it never changes.")
        if column in MANAGED_COLUMNS or column == spec.surrogate_key:
            raise ValueError(f"{column} is maintained by the SCD2 logic and cannot be set directly.")
        if column not in spec.changeable:
            raise ValueError(f"{spec.model} has no column '{column}' that can change. Use one of {list(spec.changeable)}.")
        parsed[column] = _parse_value(spec.column_kinds[column], value, column)
    return parsed


def _parse_key(spec: DimensionSpec, natural_key: Any) -> str | int:
    text = str(natural_key if natural_key is not None else "").strip()
    if not text:
        raise ValueError(f"natural_key ({spec.natural_key}) is required.")
    if spec.natural_key_kind == "int":
        try:
            return int(text)
        except ValueError:
            raise ValueError(f"natural_key ({spec.natural_key}) must be an integer, got '{text}'.") from None
    return text


def _key_literal(spec: DimensionSpec, key: str | int) -> str:
    return str(int(key)) if spec.natural_key_kind == "int" else sql_literal(key)


def _jsonable(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


def _fetch_dicts(con, sql: str) -> list[dict[str, Any]]:
    cursor = con.execute(sql)
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _exists(con, uri: str) -> bool:
    return con.execute(f"SELECT count(*) FROM glob({sql_literal(uri)})").fetchone()[0] > 0


def _single_current(rows: list[dict[str, Any]]) -> dict[str, Any]:
    current = [row for row in rows if row["is_current"]]
    if len(current) != 1:
        raise ValueError(f"Expected exactly one current version, found {len(current)}.")
    return current[0]


def _check_scenario(scenario: Any) -> str:
    text = str(scenario or "").strip()
    if not text or any(not (ch.isalnum() or ch in "_-") for ch in text) or len(text) > 64:
        raise ValueError("scenario must have 1-64 letters, digits, '_' or '-'.")
    return text


# ---------------------------------------------------------------------------------------
# SCD rules of each dimension: (rows of the member, parsed change, effective date)
#   -> (new rows of the member, actions). New rows have surrogate key None.
# ---------------------------------------------------------------------------------------

def _product_rules(rows, changes, effective_date):
    rows = [dict(row) for row in rows]
    current = _single_current(rows)
    diff = {column: value for column, value in changes.items() if current[column] != value}
    if not diff:
        return rows, ["no change: the current version already has these values"]
    if effective_date <= current["valid_from"]:
        raise ValueError(
            f"effective_date {effective_date} must be after the start of the current version "
            f"({current['valid_from']})."
        )
    new_version = {
        **current,
        **diff,
        "prod_sk": None,
        "valid_from": effective_date,
        "valid_to": OPEN_END,
        "is_current": True,
        "is_inferred": False,
    }
    current["valid_to"] = effective_date - timedelta(days=1)
    current["is_current"] = False
    rows.append(new_version)
    return rows, [
        f"type 2: closed prod_sk {current['prod_sk']} on {current['valid_to']} and opened a new "
        f"version of prod_spk {current['prod_spk']} from {effective_date} with {_jsonable(diff)}"
    ]


def _customer_rules(rows, changes, effective_date):  # effective_date unused: the change dates rule
    rows = [dict(row) for row in rows]
    actions: list[str] = []

    for column in ("country", "subscription_source"):
        if column in changes and any(row[column] != changes[column] for row in rows):
            for row in rows:
                row[column] = changes[column]
            actions.append(f"type 1: {column} = {changes[column]!r} in every version")

    # Same day: the subscription comes first, so subscribing and cancelling on one day ends Unsubscribed.
    events = sorted(
        (changes[column], 0 if column == "subscription_date" else 1, column)
        for column in ("subscription_date", "unsubscription_date")
        if column in changes
    )
    for when, _, column in events:
        current = _single_current(rows)
        if when < current["valid_from"]:
            raise ValueError(
                f"{column} {when} is before the start of the current version ({current['valid_from']}): "
                f"a change can only affect the current version."
            )
        if column == "unsubscription_date":
            if current["subscription_date"] is not None and when < current["subscription_date"]:
                raise ValueError(f"unsubscription_date {when} is before the subscription ({current['subscription_date']}).")
            if current["unsubscription_date"] != when or current["newsletter_status"] != "Unsubscribed":
                current["unsubscription_date"] = when
                current["newsletter_status"] = "Unsubscribed"
                actions.append(f"cancellation on {when}: customer_sk {current['customer_sk']} updated in place (Unsubscribed)")
            continue

        resubscription = current["newsletter_status"] == "Unsubscribed" and (
            current["unsubscription_date"] is None or when > current["unsubscription_date"]
        )
        if resubscription:
            if when <= current["valid_from"]:
                raise ValueError(f"subscription_date {when} must be after the start of the current version ({current['valid_from']}).")
            new_version = {
                **current,
                "customer_sk": None,
                "subscription_date": when,
                "unsubscription_date": None,
                "newsletter_status": "Subscribed",
                "valid_from": when,
                "valid_to": OPEN_END,
                "is_current": True,
            }
            current["valid_to"] = when - timedelta(days=1)
            current["is_current"] = False
            rows.append(new_version)
            actions.append(
                f"type 2 (re-subscription on {when}): closed customer_sk {current['customer_sk']} on "
                f"{current['valid_to']} and opened a new version (Subscribed)"
            )
        elif current["subscription_date"] != when:
            cancelled = current["unsubscription_date"] is not None and current["unsubscription_date"] >= when
            current["subscription_date"] = when
            current["newsletter_status"] = "Unsubscribed" if cancelled else "Subscribed"
            actions.append(
                f"subscription on {when}: customer_sk {current['customer_sk']} updated in place "
                f"({current['newsletter_status']})"
            )

    return rows, actions or ["no change: the customer already has these values"]


RULES: dict[str, Callable] = {
    "gld_dim_product": _product_rules,
    "gld_dim_customer": _customer_rules,
}


# ---------------------------------------------------------------------------------------
# Steps of the simulation (one per task of the gold_scd2_simulation DAG)
# ---------------------------------------------------------------------------------------

def plan_change(
    *,
    dimension: str,
    natural_key: Any,
    changes: Any,
    effective_date: Any = None,
    scenario: str,
    batch_id: str,
    reset: bool = False,
    rebuild_fact: bool = True,
    export_to_host: bool = True,
) -> dict[str, Any]:
    """Validate a change and decide which files it reads and writes.

    Returns a JSON-serializable plan (it travels through XCom).
    """
    spec = _spec(dimension)
    scenario = _check_scenario(scenario)
    key = _parse_key(spec, natural_key)
    parsed = _parse_changes(spec, changes)
    effective = None
    if spec.needs_effective_date:
        if not effective_date:
            raise ValueError(f"effective_date (YYYY-MM-DD) is required for {spec.model}.")
        effective = _parse_date(effective_date, "effective_date")

    lake = LakeConfig.from_env()
    scenario_uri = lake.uri("gold", SIMULATION_FOLDER, scenario)
    state_uri = f"{scenario_uri}/{STATE_FILE}"
    state = {} if reset else (objects.read_json(state_uri) or {})
    history = state.get("history", [])
    if history and history[-1].get("batch_id") == batch_id:
        # This run already published once (cleared and re-run): start from what it started from.
        latest = dict(history[-1].get("latest_before", {}))
    else:
        latest = dict(state.get("latest", {}))

    base_dim = latest.get(spec.model, lake.uri("gold", f"{spec.model}.parquet"))
    base_fact = latest.get(FACT_MODEL, lake.uri("gold", f"{FACT_MODEL}.parquet"))

    con = connect()
    try:
        for uri in [base_dim] + ([base_fact] if rebuild_fact else []):
            if not _exists(con, uri):
                raise FileNotFoundError(f"{uri} does not exist. Run the dbt_medallion DAG (gold) first.")
    finally:
        con.close()

    batch_uri = f"{scenario_uri}/{batch_id}"
    plan = {
        "scenario": scenario,
        "batch_id": batch_id,
        "dimension": spec.model,
        "natural_key_column": spec.natural_key,
        "natural_key": key,
        "changes": _jsonable(parsed),
        "effective_date": _jsonable(effective),
        "reset": bool(reset),
        "rebuild_fact": bool(rebuild_fact),
        "export_to_host": bool(export_to_host),
        "base_dim": base_dim,
        "base_fact": base_fact,
        "target_dim": f"{batch_uri}/{spec.model}.parquet",
        "target_fact": f"{batch_uri}/{FACT_MODEL}.parquet",
        "change_file": f"{batch_uri}/change.json",
        "state_file": state_uri,
        "latest_before": latest,
    }
    log.info("Plan: %s", json.dumps(plan, indent=2))
    return plan


def apply_change(plan: dict[str, Any]) -> dict[str, Any]:
    """Apply the SCD rules to the member and write the new dimension file.

    Only the rows of the member are loaded in memory; the rest of the dimension is streamed
    from the base file to the target file.
    """
    spec = _spec(plan["dimension"])
    changes = _parse_changes(spec, plan["changes"])
    effective = _parse_date(plan["effective_date"], "effective_date") if plan["effective_date"] else None
    sk = spec.surrogate_key
    key_filter = f"{sql_identifier(spec.natural_key)} = {_key_literal(spec, plan['natural_key'])}"
    base = f"read_parquet({sql_literal(plan['base_dim'])})"

    con = connect()
    try:
        columns = [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {base}").fetchall()]
        con.execute(f"CREATE TEMP TABLE member_rows AS SELECT * FROM {base} WHERE {key_filter}")
        before = _fetch_dicts(con, f"SELECT * FROM member_rows ORDER BY {sql_identifier(sk)}")
        if not before:
            raise ValueError(f"{spec.natural_key} = {plan['natural_key']!r} not found in {plan['base_dim']}.")
        if any(row[sk] == 0 for row in before):
            raise ValueError("The unknown member (surrogate key 0) cannot be changed.")

        after, actions = RULES[spec.model](before, changes, effective)
        for action in actions:
            log.info("%s %s: %s", spec.natural_key, plan["natural_key"], action)

        result: dict[str, Any] = {"changed": after != before, "actions": actions}
        if not result["changed"]:
            result.update(before=_jsonable(before), after=_jsonable(before), rows=None, new_surrogate_keys=[])
            return result

        next_sk = con.execute(f"SELECT max({sql_identifier(sk)}) FROM {base}").fetchone()[0]
        new_keys = []
        for row in after:
            if row[sk] is None:
                next_sk += 1
                row[sk] = next_sk
                new_keys.append(next_sk)

        # Same column types as the base file: the table is created from it.
        con.execute("CREATE TEMP TABLE new_member_rows AS SELECT * FROM member_rows LIMIT 0")
        con.executemany(
            f"INSERT INTO new_member_rows ({', '.join(sql_identifier(c) for c in columns)}) "
            f"VALUES ({', '.join('?' for _ in columns)})",
            [[row[c] for c in columns] for row in after],
        )
        rows = con.execute(
            f"""
            COPY (
                SELECT * FROM {base} WHERE NOT ({key_filter})
                UNION ALL BY NAME
                SELECT * FROM new_member_rows
            ) TO {sql_literal(plan['target_dim'])} (FORMAT parquet, COMPRESSION zstd)
            """
        ).fetchone()[0]
    finally:
        con.close()

    for row in after:
        log.info("after: %s", _jsonable(row))
    result.update(
        before=_jsonable(before),
        after=_jsonable(sorted(after, key=lambda row: row[sk])),
        rows=int(rows),
        new_surrogate_keys=new_keys,
    )
    log.info("Wrote %s rows to %s", rows, plan["target_dim"])
    return result


def rebuild_fact(plan: dict[str, Any], change: dict[str, Any]) -> dict[str, Any]:
    """Write gld_fact_sales with the keys of the changed member resolved again.

    Each fact row of the member is mapped back to its version key (prod_spk / customer_id)
    and looked up again by that key and the order date in the new versions. Every other row
    and column is copied unchanged.
    """
    if not plan["rebuild_fact"]:
        return {"written": False, "reason": "rebuild_fact = false"}
    if not change["changed"]:
        return {"written": False, "reason": "the dimension did not change"}

    spec = _spec(plan["dimension"])
    sk, version_key = spec.surrogate_key, spec.version_key
    fact_column = sql_identifier(sk)
    base = f"read_parquet({sql_literal(plan['base_fact'])})"

    con = connect()
    try:
        con.execute("CREATE TEMP TABLE old_keys (sk INTEGER, version_key BIGINT)")
        con.executemany(
            "INSERT INTO old_keys VALUES (?, ?)",
            [[row[sk], row[version_key]] for row in change["before"]],
        )
        con.execute("CREATE TEMP TABLE new_versions (sk INTEGER, version_key BIGINT, valid_from DATE, valid_to DATE)")
        con.executemany(
            "INSERT INTO new_versions VALUES (?, ?, ?, ?)",
            [
                [row[sk], row[version_key], _parse_date(row["valid_from"], "valid_from"), _parse_date(row["valid_to"], "valid_to")]
                for row in change["after"]
            ],
        )

        columns = [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {base}").fetchall()]
        select_list = ", ".join(
            f"coalesce(n.sk, f.{fact_column}) AS {fact_column}" if column == sk else f"f.{sql_identifier(column)}"
            for column in columns
        )
        # Unknown order date (0): the open version, as in the dbt model.
        order_date = (
            "coalesce(cast(try_strptime(cast(f.order_date_key AS VARCHAR), '%Y%m%d') AS DATE), DATE '9999-12-31')"
        )
        source = f"""
            FROM {base} AS f
            LEFT JOIN old_keys AS o ON o.sk = f.{fact_column}
            LEFT JOIN new_versions AS n
                ON n.version_key = o.version_key
               AND {order_date} BETWEEN n.valid_from AND n.valid_to
        """
        per_version = con.execute(
            f"SELECT n.sk, count(*) {source} WHERE n.sk IS NOT NULL GROUP BY n.sk ORDER BY n.sk"
        ).fetchall()
        rekeyed = con.execute(
            f"SELECT count(*) {source} WHERE n.sk IS NOT NULL AND n.sk <> f.{fact_column}"
        ).fetchone()[0]
        rows = con.execute(
            f"COPY (SELECT {select_list} {source}) TO {sql_literal(plan['target_fact'])} (FORMAT parquet, COMPRESSION zstd)"
        ).fetchone()[0]
    finally:
        con.close()

    result = {
        "written": True,
        "rows": int(rows),
        "rows_rekeyed": int(rekeyed),
        "rows_per_version": {str(version): int(count) for version, count in per_version},
    }
    log.info("Fact: %s rows written, %s moved to the new version(s); rows per %s: %s",
             rows, rekeyed, sk, result["rows_per_version"])
    return result


def publish(plan: dict[str, Any], change: dict[str, Any], fact: dict[str, Any]) -> dict[str, Any]:
    """Record the run in the scenario state and copy its latest files to the host."""
    state = {} if plan["reset"] else (objects.read_json(plan["state_file"]) or {})
    history = [entry for entry in state.get("history", []) if entry.get("batch_id") != plan["batch_id"]]
    latest = dict(plan["latest_before"])
    if change["changed"]:
        latest[plan["dimension"]] = plan["target_dim"]
    if fact.get("written"):
        latest[FACT_MODEL] = plan["target_fact"]

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    history.append({
        "batch_id": plan["batch_id"],
        "at_utc": now,
        "dimension": plan["dimension"],
        plan["natural_key_column"]: plan["natural_key"],
        "changes": plan["changes"],
        "effective_date": plan["effective_date"],
        "actions": change["actions"],
        "new_surrogate_keys": change.get("new_surrogate_keys", []),
        "fact_rows_rekeyed": fact.get("rows_rekeyed", 0),
        "latest_before": plan["latest_before"],
    })
    objects.write_json(plan["change_file"], {"plan": plan, "change": change, "fact": fact})
    objects.write_json(plan["state_file"], {
        "scenario": plan["scenario"],
        "updated_at_utc": now,
        "latest": latest,
        "history": history,
    })

    exported = _export_latest(plan["scenario"], latest, history) if plan["export_to_host"] else []
    return {"scenario": plan["scenario"], "latest": latest, "exported": exported, "changes_in_scenario": len(history)}


def _export_latest(scenario: str, latest: dict[str, str], history: list[dict[str, Any]]) -> list[str]:
    """Copy the latest files of a scenario to <EXPORT_DIR>/scd2_simulation/<scenario>/."""
    target_dir = export_directory(HOST_SUBDIR) / scenario
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
    except PermissionError as error:
        raise RuntimeError(
            f"Cannot write to {target_dir}: the folder ./data/exports on the host is not writable by the "
            f"Airflow user (uid {os.getuid()}). Set AIRFLOW_UID in .env to your user id (`id -u`) and run "
            f"`docker compose up -d`, or make the folder writable. The lake files of this run are already "
            f"published; trigger again with export_to_host = false to skip this step."
        ) from error

    # A reset can leave files of the previous runs that are no longer part of the scenario.
    for old_file in target_dir.glob("gld_*.parquet"):
        if old_file.stem not in latest:
            old_file.unlink()

    exported = []
    con = connect()
    try:
        for name, uri in sorted(latest.items()):
            final_path = target_dir / f"{name}.parquet"
            temporary_path = target_dir / f"{name}.parquet.tmp"
            con.execute(
                f"COPY (SELECT * FROM read_parquet({sql_literal(uri)})) "
                f"TO {sql_literal(temporary_path.as_posix())} (FORMAT parquet, COMPRESSION snappy)"
            )
            os.replace(temporary_path, final_path)
            exported.append(str(final_path))
            log.info("Exported %s -> %s", uri, final_path)
    finally:
        con.close()

    (target_dir / "_history.json").write_text(json.dumps(history, indent=2), encoding="utf-8")
    return exported
