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

from src.ml_stock_dataset import build_feature_dataset, get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Build ml_stock_feature_daily from existing stock source views.")
    parser.add_argument("--start-date", help="Optional inclusive start date, e.g. 2026-01-01 or 20260101")
    parser.add_argument("--end-date", help="Optional inclusive end date, e.g. 2026-05-11 or 20260511")
    parser.add_argument(
        "--history-buffer-days",
        type=int,
        default=180,
        help="Calendar-day history buffer used to fetch past bars before the target start date",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Delete existing feature rows in the requested range before writing new rows",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    return parser.parse_args()


def main():
    args = parse_args()
    engine = get_engine()
    result = build_feature_dataset(
        engine,
        start_date=args.start_date,
        end_date=args.end_date,
        rebuild=args.rebuild,
        history_buffer_days=args.history_buffer_days,
    )

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return

    logger.info(
        "features built: target_rows=%s source_rows=%s rows_written=%s complete_rows=%s start_date=%s end_date=%s history_buffer_days=%s rebuild=%s",
        result.get("target_rows"),
        result.get("source_rows"),
        result.get("rows_written"),
        result.get("complete_rows"),
        result.get("start_date"),
        result.get("end_date"),
        result.get("history_buffer_days"),
        bool(args.rebuild),
    )


if __name__ == "__main__":
    main()
