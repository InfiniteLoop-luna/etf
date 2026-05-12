#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import logging
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.ml_stock_dataset import build_universe_dataset, get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Build ml_stock_universe_daily from existing stock source views.")
    parser.add_argument("--start-date", help="Optional inclusive start date, e.g. 2026-01-01 or 20260101")
    parser.add_argument("--end-date", help="Optional inclusive end date, e.g. 2026-05-11 or 20260511")
    parser.add_argument(
        "--min-history-days",
        type=int,
        default=60,
        help="Minimum observed daily bars required for min_history_ok/sample_eligible",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Delete existing rows in the requested range before writing new rows",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    return parser.parse_args()


def main():
    args = parse_args()
    engine = get_engine()
    result = build_universe_dataset(
        engine,
        start_date=args.start_date,
        end_date=args.end_date,
        min_history_days=args.min_history_days,
        rebuild=args.rebuild,
    )

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return

    logger.info(
        "universe built: source_rows=%s rows_written=%s eligible_rows=%s start_date=%s end_date=%s min_history_days=%s rebuild=%s",
        result.get("source_rows"),
        result.get("rows_written"),
        result.get("eligible_rows"),
        result.get("start_date"),
        result.get("end_date"),
        result.get("min_history_days"),
        bool(args.rebuild),
    )


if __name__ == "__main__":
    main()
