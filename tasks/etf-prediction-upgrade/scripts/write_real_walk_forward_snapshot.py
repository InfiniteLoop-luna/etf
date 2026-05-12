#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import os
import subprocess
import sys

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )
)
DOTENV_PATH = os.path.join(PROJECT_ROOT, ".env")

RUNTIME_OUTPUT_PATH = os.path.join(
    PROJECT_ROOT,
    "tasks",
    "etf-prediction-upgrade",
    "outputs",
    "runtime",
    "ml_prediction_upgrade_walk_forward_snapshot.json",
)


def _load_dotenv_if_present(dotenv_path: str = DOTENV_PATH) -> bool:
    if not os.path.exists(dotenv_path):
        return False

    loaded_any = False
    with open(dotenv_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if not key or key in os.environ:
                continue
            os.environ[key] = value
            loaded_any = True
    return loaded_any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a real-data walk-forward snapshot for the ML prediction upgrade page."
    )
    parser.add_argument("--task-type", choices=("classification", "regression"), default="classification")
    parser.add_argument("--model-kind", choices=("baseline", "sklearn"), default="sklearn")
    parser.add_argument("--classifier", default="logistic")
    parser.add_argument("--regressor", default="ridge")
    parser.add_argument("--target-column")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--max-windows", type=int, default=5)
    parser.add_argument("--min-train-rows", type=int, default=200)
    parser.add_argument("--min-test-rows", type=int, default=50)
    parser.add_argument("--fill-method", choices=("none", "drop", "median"), default="median")
    parser.add_argument("--include-ineligible", action="store_true")
    parser.add_argument("--snapshot-out", default=RUNTIME_OUTPUT_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    _load_dotenv_if_present()

    if not (os.environ.get("ETF_PG_PASSWORD") or os.environ.get("PGPASSWORD")):
        raise SystemExit(
            f"error: missing PostgreSQL password. Set ETF_PG_PASSWORD or PGPASSWORD, or add it to {DOTENV_PATH}"
        )

    os.makedirs(os.path.dirname(os.path.abspath(args.snapshot_out)), exist_ok=True)

    command = [
        sys.executable,
        os.path.join(PROJECT_ROOT, "scripts", "train_ml_stock_v1.py"),
        "--eval-mode",
        "walk-forward",
        "--task-type",
        args.task_type,
        "--model-kind",
        args.model_kind,
        "--fill-method",
        args.fill_method,
        "--min-train-rows",
        str(args.min_train_rows),
        "--min-test-rows",
        str(args.min_test_rows),
        "--max-windows",
        str(args.max_windows),
        "--snapshot-out",
        os.path.abspath(args.snapshot_out),
    ]

    if args.task_type == "classification":
        command.extend(["--classifier", args.classifier])
    else:
        command.extend(["--regressor", args.regressor])

    if args.target_column:
        command.extend(["--target-column", args.target_column])
    if args.start_date:
        command.extend(["--start-date", args.start_date])
    if args.end_date:
        command.extend(["--end-date", args.end_date])
    if args.limit is not None:
        command.extend(["--limit", str(args.limit)])
    if args.include_ineligible:
        command.append("--include-ineligible")

    result = subprocess.run(command, cwd=PROJECT_ROOT, check=False, env=os.environ.copy())
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
