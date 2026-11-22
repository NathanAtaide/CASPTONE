"""Command line of the forecasting steps, used by the Airflow DAGs through BashOperator.

    $ML_PYTHON -m lakehouse.forecasting prepare  --run-id RUN [--backtest-months 5] [--targets orders,units] [--force]
    $ML_PYTHON -m lakehouse.forecasting run-job  --run-id RUN --target orders --job ml_c4_rf
    $ML_PYTHON -m lakehouse.forecasting select   --run-id RUN
    $ML_PYTHON -m lakehouse.forecasting forecast --run-id RUN --target sales [--month 2026-01] [--budget-total 438070]
    $ML_PYTHON -m lakehouse.forecasting publish-forecast --run-id RUN

Logs go to stderr; the **last line of stdout is a JSON summary**, which the
BashOperator pushes to XCom (small summaries only - the data stays in the lake).
"""
from __future__ import annotations

import argparse
import json
import logging
import sys


def _num(value: str | None) -> float | None:
    if value is None or str(value).strip().lower() in ("", "none", "null"):
        return None
    return float(str(value).replace(",", "."))


def _flag(value: str | None) -> bool:
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _targets(value: str | None) -> list[str] | None:
    if not value:
        return None
    text = value.strip()
    if text.startswith("["):                       # JSON list rendered by Airflow
        return [str(t) for t in json.loads(text)] or None
    return [t.strip() for t in text.split(",") if t.strip()] or None


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m lakehouse.forecasting", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("prepare", help="build the inputs, decide the window and open the MLflow runs")
    p.add_argument("--run-id", required=True)
    p.add_argument("--backtest-months", default="5")
    p.add_argument("--targets", default="")
    p.add_argument("--force", default="false")

    p = sub.add_parser("run-job", help="backtest one job of one target")
    p.add_argument("--run-id", required=True)
    p.add_argument("--target", required=True)
    p.add_argument("--job", required=True)

    p = sub.add_parser("select", help="leaderboard, champion registration and gold tables")
    p.add_argument("--run-id", required=True)

    p = sub.add_parser("forecast", help="forecast the next month with the registered champion")
    p.add_argument("--run-id", required=True)
    p.add_argument("--target", required=True)
    p.add_argument("--month", default="")
    p.add_argument("--budget-total", default="")
    p.add_argument("--budget-es", default="")
    p.add_argument("--budget-pt", default="")

    p = sub.add_parser("publish-forecast", help="write the next-month forecasts to the gold layer")
    p.add_argument("--run-id", required=True)
    p.add_argument("--targets", default="")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    for noisy in ("mlflow", "urllib3", "botocore", "git", "matplotlib"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    from . import pipeline

    if args.command == "prepare":
        result = pipeline.prepare(args.run_id, int(args.backtest_months or 5), _targets(args.targets), _flag(args.force))
    elif args.command == "run-job":
        result = pipeline.run_job(args.run_id, args.target, args.job)
    elif args.command == "select":
        result = pipeline.select(args.run_id)
    elif args.command == "forecast":
        result = pipeline.forecast(args.run_id, args.target, args.month.strip() or None, _num(args.budget_total),
                                   _num(args.budget_es), _num(args.budget_pt))
    else:
        result = pipeline.publish_forecast(args.run_id, _targets(args.targets))
    sys.stdout.flush()
    print(json.dumps(result, default=str, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
