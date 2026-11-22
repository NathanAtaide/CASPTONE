"""Inputs of the forecasting models, built once from the silver layer.

One :class:`Inputs` object holds everything the candidate models read, so every
model of a run sees exactly the same snapshot of the data:

* ``daily``  - one row per market (Online ES, Online PT) and day: orders, units,
  gross sales and paid media spend. Days without orders hold 0; spend is null
  where ``slv_paid_media`` has no row;
* ``groups`` - one row per day and product category group (ES + PT summed): units
  and gross sales, for the F.1 model. The groups add up to the daily totals;
* ``events`` - the calendar of ``slv_events`` for ES, PT and ``All``.

The target definitions are the ones of the Baseline and Deep Learning notebooks
(see :data:`protocol.TARGET_DEFINITIONS`); the ML notebook counted only SALE
lines, which differs by about 0,6% - one definition is needed for a fair test.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field, replace

import numpy as np
import pandas as pd

from ..config import LakeConfig
from ..duck import connect, sql_literal
from .protocol import COUNTRY, DATA_START, MARKETS, month_days, month_of

log = logging.getLogger(__name__)

# Product category groups of the F.1 model (ML notebook). Unknown categories go to "Other".
CATEGORY_GROUPS: dict[str, list[str]] = {
    "Apparel": ["Apparel"],
    "Bags & Wallets": ["Hand Bag", "Wallet"],
    "Jewellery": ["Jewellery"],
    "Footwear": ["Footwear"],
    "Accessories": ["Hair Accessories", "Scarves", "Sunglasses", "Hats", "Watches", "Belts",
                    "Winter Textiles", "Stainless Steel", "Silver"],
    "Other": ["Party", "Night", "Travel", "Waterproof", "Fragrances"],
}

# Calendar campaigns known at the start of the year; order = priority when two fall on the same day.
FIXED_CAMPAIGNS = ["Black Friday", "Christmas", "Mothers Day", "Reyes", "Valentines", "Easter and Holy Week"]
DL_CAMPAIGN_COLUMNS = {"Black Friday": "camp_black_friday", "Christmas": "camp_christmas",
                       "Mothers Day": "camp_mothers_day", "Reyes": "camp_reyes",
                       "Valentines": "camp_valentines", "Easter and Holy Week": "camp_easter"}
ML_CAMPAIGN_COLUMNS = {"Black Friday": "camp_black_friday", "Christmas": "camp_christmas",
                       "Mothers Day": "camp_mothers_day", "Reyes": "camp_reyes",
                       "Valentines": "camp_valentines", "Easter and Holy Week": "camp_easter_holy_week"}

SILVER_TABLES = ("slv_order_cab", "slv_order_det", "slv_product", "slv_events", "slv_paid_media")


def event_family(name: str) -> str:
    """Campaign family of an event name (same rules as the notebooks)."""
    clean = " ".join(str(name).split())
    low = clean.lower()
    if low.startswith("black friday"):
        return "Black Friday"
    if low.startswith("clearance"):
        return "Clearance"
    if low.startswith("special prices"):
        return "Special prices"
    if low.startswith("free shipping"):
        return "Free shipping"
    if "valentines" in low:
        return "Valentines"
    if "mothers day" in low:
        return "Mothers Day"
    if "christmas" in low:
        return "Christmas"
    if "reyes" in low:
        return "Reyes"
    if clean in ("Easter Weeks", "Holy Week"):
        return "Easter and Holy Week"
    if clean == "Get your basket ready":
        return "Get your basket ready"
    if clean == "Stock 1":
        return "Stock 1"
    if low.startswith("apag"):
        return "Iberian blackout"
    return "Other"


def _silver(lake: LakeConfig, table: str) -> str:
    return f"read_parquet({sql_literal(lake.uri('silver', table + '.parquet'))})"


@dataclass
class Inputs:
    daily: pd.DataFrame
    groups: pd.DataFrame
    events: pd.DataFrame
    data_end: pd.Timestamp
    warnings: list[str] = field(default_factory=list)
    assumptions: list[str] = field(default_factory=list)

    # ------------------------------------------------------------------ building
    @classmethod
    def from_silver(cls, con=None) -> "Inputs":
        """Read the silver layer and build the model inputs (a few seconds with DuckDB)."""
        lake = LakeConfig.from_env()
        own = con is None
        con = con or connect()
        con.execute("SET enable_progress_bar = false")     # keeps the Airflow task logs readable
        warnings: list[str] = []
        markets = ", ".join(sql_literal(m) for m in MARKETS)
        start = sql_literal(DATA_START.date())
        try:
            # One row per order of the scope (silver already has one row per order; the group by guards it).
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE fc_orders AS
                SELECT order_id, any_value(mark_cod) AS mark, min(order_date) AS day
                FROM {_silver(lake, 'slv_order_cab')}
                WHERE mark_cod IN ({markets}) AND order_date >= DATE {start}
                GROUP BY order_id
            """)
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE fc_lines AS
                SELECT o.mark, o.day, d.order_id, d.type, d.prod_sfk,
                       CAST(d.prod_qty AS DOUBLE) AS qty,
                       CAST(d.amount_fixed_rate_w_disc_wo_vat AS DOUBLE) AS amount
                FROM {_silver(lake, 'slv_order_det')} AS d
                JOIN fc_orders AS o USING (order_id)
                WHERE d.type IN ('SALE', 'CANCELLED')
            """)
            data_end = con.execute("SELECT max(day) FROM fc_orders").fetchone()[0]
            if data_end is None:
                raise RuntimeError("slv_order_cab has no orders of Online ES / Online PT since 2024-01-01.")
            data_end = pd.Timestamp(data_end)

            facts = con.execute("""
                WITH per_order AS (
                    SELECT mark, day, order_id,
                           count(*) FILTER (WHERE type = 'SALE')                        AS sale_lines,
                           coalesce(sum(qty) FILTER (WHERE type = 'SALE'), 0)           AS sale_units,
                           coalesce(sum(qty) FILTER (WHERE type = 'CANCELLED'), 0)      AS cancel_units,
                           coalesce(sum(amount), 0)                                     AS gross_value
                    FROM fc_lines GROUP BY 1, 2, 3)
                SELECT mark, day,
                       count(*) FILTER (WHERE sale_lines > 0 AND round(sale_units + cancel_units) > 0) AS orders,
                       coalesce(sum(sale_units + cancel_units) FILTER (WHERE sale_lines > 0), 0)      AS units,
                       coalesce(sum(gross_value) FILTER (WHERE sale_lines > 0), 0)                    AS sales
                FROM per_order GROUP BY 1, 2
            """).df()

            groups = con.execute(f"""
                WITH sale_orders AS (SELECT DISTINCT order_id FROM fc_lines WHERE type = 'SALE'),
                prod AS (SELECT prod_spk, min(cat_des_en) AS category
                         FROM {_silver(lake, 'slv_product')} GROUP BY prod_spk)
                SELECT l.day, coalesce(p.category, 'WO_CAT') AS category,
                       sum(l.qty) AS units, sum(l.amount) AS sales
                FROM fc_lines AS l
                JOIN sale_orders AS s USING (order_id)
                LEFT JOIN prod AS p ON p.prod_spk = l.prod_sfk
                GROUP BY 1, 2
            """).df()

            spend = con.execute(f"""
                SELECT marketplace AS mark, paid_media_date AS day, sum(CAST(amount_paid_media AS DOUBLE)) AS spend
                FROM {_silver(lake, 'slv_paid_media')}
                WHERE marketplace IN ({markets}) AND paid_media_date IS NOT NULL
                GROUP BY 1, 2
            """).df()

            events = con.execute(f"""
                SELECT mark_cod AS mark, event, start_date, end_date
                FROM {_silver(lake, 'slv_events')}
                WHERE mark_cod IN ({markets}, 'All')
                  AND start_date IS NOT NULL AND end_date IS NOT NULL AND end_date >= start_date
            """).df()
            con.execute("DROP TABLE IF EXISTS fc_lines")
            con.execute("DROP TABLE IF EXISTS fc_orders")
        finally:
            if own:
                con.close()

        # Daily table on a complete spine: every market x every day, 0 when there were no orders.
        spine = pd.MultiIndex.from_product([list(MARKETS), pd.date_range(DATA_START, data_end, freq="D")],
                                           names=["mark", "day"])
        facts["day"] = pd.to_datetime(facts["day"])
        spend["day"] = pd.to_datetime(spend["day"])
        daily = (facts.set_index(["mark", "day"]).reindex(spine).fillna(0.0)
                 .join(spend.set_index(["mark", "day"]), how="left").reset_index())
        daily["orders"] = daily["orders"].astype(float)
        missing_spend = daily.groupby("mark")["spend"].apply(lambda s: int(s.isna().sum()))
        for mk, n in missing_spend.items():
            if n:
                warnings.append(f"slv_paid_media has no spend on {n} days of {mk} between {DATA_START.date()} and {data_end.date()}.")

        # Category groups: map the 19 categories to the 6 groups of F.1.
        lookup = {" ".join(c.lower().split()): g for g, cats in CATEGORY_GROUPS.items() for c in cats}
        groups["group"] = groups["category"].map(lambda c: lookup.get(" ".join(str(c).lower().split())))
        unknown = sorted(groups.loc[groups["group"].isna(), "category"].astype(str).unique())
        if unknown:
            warnings.append(f"Categories without a F.1 group, counted in 'Other': {unknown}")
            groups["group"] = groups["group"].fillna("Other")
        groups["day"] = pd.to_datetime(groups["day"])
        groups = (groups.groupby(["day", "group"], as_index=False)[["units", "sales"]].sum()
                  .sort_values(["day", "group"]).reset_index(drop=True))

        events["start_date"] = pd.to_datetime(events["start_date"])
        events["end_date"] = pd.to_datetime(events["end_date"])
        events["family"] = events["event"].map(event_family)
        events["is_assumed"] = False

        inputs = cls(daily=daily.sort_values(["mark", "day"]).reset_index(drop=True), groups=groups,
                     events=events.sort_values(["mark", "start_date", "event"]).reset_index(drop=True),
                     data_end=data_end, warnings=warnings)
        inputs.check()
        return inputs

    def check(self) -> None:
        """Consistency checks: the category groups add up to the daily totals."""
        tot = self.daily.groupby("day")[["units", "sales"]].sum()
        grp = self.groups.groupby("day")[["units", "sales"]].sum().reindex(tot.index).fillna(0.0)
        for col in ("units", "sales"):
            diff = float((tot[col] - grp[col]).abs().max())
            if diff > 1e-3 * max(1.0, float(tot[col].abs().max())):
                raise RuntimeError(f"The category groups do not add up to the daily {col} (max difference {diff}).")

    # ------------------------------------------------------------------ lake round trip
    def save(self, con, prefix: str) -> None:
        for name in ("daily", "groups", "events"):
            frame = getattr(self, name)
            con.register("fc_frame", frame)
            con.execute(f"COPY fc_frame TO {sql_literal(f'{prefix}/{name}.parquet')} (FORMAT parquet, COMPRESSION zstd)")
            con.unregister("fc_frame")

    @classmethod
    def load(cls, con, prefix: str, meta: dict) -> "Inputs":
        frames = {name: con.execute(f"SELECT * FROM read_parquet({sql_literal(f'{prefix}/{name}.parquet')})").df()
                  for name in ("daily", "groups", "events")}
        for name, cols in (("daily", ["day"]), ("groups", ["day"]), ("events", ["start_date", "end_date"])):
            for c in cols:
                frames[name][c] = pd.to_datetime(frames[name][c])
        return cls(daily=frames["daily"], groups=frames["groups"], events=frames["events"],
                   data_end=pd.Timestamp(meta["data_end"]), warnings=list(meta.get("warnings", [])))

    # ------------------------------------------------------------------ fingerprint
    def fingerprint(self, *extra) -> str:
        """Hash of the data the models read: it changes when the silver layer changes."""
        h = hashlib.sha256()
        d = self.daily.sort_values(["mark", "day"])[["mark", "day", "orders", "units", "sales", "spend"]]
        h.update(d.round({c: 4 for c in ("orders", "units", "sales", "spend")}).to_csv(index=False).encode())
        h.update(self.groups.round({"units": 4, "sales": 4}).to_csv(index=False).encode())
        h.update(self.events[["mark", "event", "start_date", "end_date"]].to_csv(index=False).encode())
        h.update(json.dumps([str(x) for x in extra]).encode())
        return h.hexdigest()[:16]

    def summary(self) -> dict:
        y = self.daily.assign(year=self.daily["day"].dt.year).groupby("year")[["orders", "units", "sales"]].sum()
        return {"data_start": str(DATA_START.date()), "data_end": str(self.data_end.date()),
                "rows_daily": int(len(self.daily)), "rows_groups": int(len(self.groups)),
                "events": int(len(self.events)),
                "totals_by_year": {int(k): {c: round(float(v), 2) for c, v in row.items()} for k, row in y.iterrows()}}

    # ------------------------------------------------------------------ future months
    def extend_to(self, month, *, budget: dict[str, float] | None = None) -> "Inputs":
        """Add the days of a future ``month`` (targets unknown) so the models can forecast it.

        * calendar events: when ``slv_events`` has no event of ES/PT/All in the month, the events of
          the same month one year before are copied with the dates moved one year ahead (fixed
          campaigns and Clearance repeat their calendar dates); this is recorded as an assumption;
        * paid media (only for the models of scenario C): ``budget`` = monthly spend per market.
          Each market's budget is spread over the days with the daily profile of the same weekdays
          one year before (364 days earlier).
        """
        month = month_of(month)
        days = month_days(month)
        if days[-1] <= self.data_end:
            return self
        out = replace(self, warnings=list(self.warnings), assumptions=list(self.assumptions))
        future = pd.MultiIndex.from_product([list(MARKETS), pd.date_range(self.data_end + pd.Timedelta(days=1), days[-1])],
                                            names=["mark", "day"]).to_frame(index=False)
        for c in ("orders", "units", "sales", "spend"):
            future[c] = np.nan
        out.daily = pd.concat([self.daily, future], ignore_index=True).sort_values(["mark", "day"]).reset_index(drop=True)

        # Calendar of the month: copy last year's events if the month has none.
        ev = self.events
        in_month = ev[(ev["end_date"] >= days[0]) & (ev["start_date"] <= days[-1])]
        if in_month.empty:
            prev = ev[(ev["end_date"] >= days[0] - pd.DateOffset(years=1)) & (ev["start_date"] <= days[-1] - pd.DateOffset(years=1))].copy()
            if not prev.empty:
                prev["start_date"] = prev["start_date"] + pd.DateOffset(years=1)
                prev["end_date"] = prev["end_date"] + pd.DateOffset(years=1)
                prev["is_assumed"] = True
                out.events = pd.concat([ev, prev], ignore_index=True)
                names = sorted(set(prev["event"]))
                out.assumptions.append(
                    f"slv_events has no event in {month}: the calendar of {month - 12} was repeated one year later "
                    f"({len(prev)} events: {', '.join(names[:8])}{'...' if len(names) > 8 else ''}).")
            else:
                out.warnings.append(f"slv_events has no event in {month} nor in {month - 12}: campaign and Clearance flags are 0.")

        if budget:
            daily = out.daily.set_index(["mark", "day"])
            for mk in MARKETS:
                amount = budget.get(mk)
                if amount is None:
                    continue
                ref = self.daily[self.daily["mark"] == mk].set_index("day")["spend"].reindex(days - pd.Timedelta(days=364))
                if ref.isna().any() or ref.sum() <= 0:
                    profile = np.full(len(days), 1.0 / len(days))
                    out.assumptions.append(f"{mk}: no spend 364 days before {month}; the budget was spread evenly over the days.")
                else:
                    profile = (ref / ref.sum()).to_numpy()
                daily.loc[[(mk, d) for d in days], "spend"] = float(amount) * profile
            out.daily = daily.reset_index()
        return out


def budget_by_market(month, inputs: Inputs, *, total: float | None = None, es: float | None = None,
                     pt: float | None = None) -> tuple[dict[str, float] | None, list[str]]:
    """Monthly paid media budget of each market from the DAG parameters.

    ``es``/``pt`` win when given; otherwise ``total`` (ES + PT) is split by the share of each market in
    the spend of the same month one year before (Jan 2025: ES 74%, PT 26%).
    """
    notes: list[str] = []
    if es is not None and pt is not None:
        notes.append(f"Paid media budget given per market: Online ES {es:,.2f} EUR, Online PT {pt:,.2f} EUR.")
        return {"Online ES": float(es), "Online PT": float(pt)}, notes
    if total is None:
        return None, notes
    ref_month = month_of(month) - 12
    ref = inputs.daily[(inputs.daily["day"] >= ref_month.start_time) & (inputs.daily["day"] <= ref_month.end_time)]
    spend = ref.groupby("mark")["spend"].sum(min_count=1).reindex(list(MARKETS))
    if spend.isna().any() or spend.sum() <= 0:
        share = {mk: 1.0 / len(MARKETS) for mk in MARKETS}
        notes.append(f"No spend in {ref_month}: the total budget was split evenly between the markets.")
    else:
        share = (spend / spend.sum()).to_dict()
    known = {"Online ES": es, "Online PT": pt}
    out = {}
    for mk in MARKETS:
        out[mk] = float(known[mk]) if known[mk] is not None else float(total) * share[mk]
    if es is not None or pt is not None:
        # One market given: the other gets the rest of the total.
        given = next(mk for mk in MARKETS if known[mk] is not None)
        other = next(mk for mk in MARKETS if mk != given)
        out[other] = float(total) - out[given]
    notes.append("Paid media budget of {}: {:,.2f} EUR (ES + PT), split ES {:,.2f} / PT {:,.2f} by the spend share of {}."
                 .format(month_of(month), float(total), out["Online ES"], out["Online PT"], ref_month))
    return out, notes


def national_holidays(years) -> dict[str, set[pd.Timestamp]]:
    """National holidays of Spain and Portugal (``holidays`` package, pinned in requirements-ml.txt)."""
    import holidays

    out = {}
    for mk, code in COUNTRY.items():
        try:
            h = holidays.country_holidays(code, years=sorted(set(years)), language="en_US")
        except Exception:  # older versions without translations
            h = holidays.country_holidays(code, years=sorted(set(years)))
        out[mk] = {pd.Timestamp(d) for d in h}
    return out


def _expand(events: pd.DataFrame, start, end) -> pd.DataFrame:
    """One row per event day (clipped to [start, end])."""
    rows = []
    for r in events.itertuples(index=False):
        s, e = max(r.start_date, pd.Timestamp(start)), min(r.end_date, pd.Timestamp(end))
        if s > e:
            continue
        for d in pd.date_range(s, e, freq="D"):
            rows.append((r.mark, r.family, r.event, d))
    return pd.DataFrame(rows, columns=["mark", "family", "event", "day"])


def _apply_all(days: pd.DataFrame) -> pd.DataFrame:
    """Events of market 'All' count for both markets."""
    own = days[days["mark"].isin(MARKETS)]
    alls = days[days["mark"] == "All"]
    both = pd.concat([alls.assign(mark=mk) for mk in MARKETS], ignore_index=True) if not alls.empty else alls
    return pd.concat([own, both], ignore_index=True)


def dl_campaign_days(events: pd.DataFrame, start, end) -> pd.DataFrame:
    """Deep-learning notebooks: one fixed campaign per market and day (priority of FIXED_CAMPAIGNS)."""
    days = _apply_all(_expand(events[events["family"].isin(FIXED_CAMPAIGNS)], start, end))
    if days.empty:
        return pd.DataFrame(columns=["mark", "day", "family"])
    days["p"] = days["family"].map({f: i for i, f in enumerate(FIXED_CAMPAIGNS)})
    return (days.sort_values(["mark", "day", "p"]).drop_duplicates(["mark", "day"])[["mark", "day", "family"]]
            .reset_index(drop=True))


def clearance_days(events: pd.DataFrame, start, end) -> set[tuple[str, pd.Timestamp]]:
    """Deep-learning notebooks: days of the event named exactly 'Clearance' (the two sale periods)."""
    days = _apply_all(_expand(events[events["event"] == "Clearance"], start, end))
    return set(zip(days["mark"], days["day"]))


def ml_campaign_active(events: pd.DataFrame, index: pd.DatetimeIndex) -> pd.DataFrame:
    """ML notebook: 0/1 per fixed campaign, active if active in Online ES or Online PT (rows 'All' are not used)."""
    out = pd.DataFrame(0, index=index, columns=list(ML_CAMPAIGN_COLUMNS.values()))
    own = events[events["mark"].isin(MARKETS)]
    for family, col in ML_CAMPAIGN_COLUMNS.items():
        for r in own[own["family"] == family].itertuples(index=False):
            out.loc[r.start_date:r.end_date, col] = 1
    return out
