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

from src.ml_stock_dataset import build_sample_dataset, get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Create or refresh ml_stock_sample_daily and optionally load joined training samples."
    )
    parser.add_argument("--start-date", help="Optional inclusive start date, e.g. 2026-01-01 or 20260101")
    parser.add_argument("--end-date", help="Optional inclusive end date, e.g. 2026-05-11 or 20260511")
    parser.add_argument(
        "--load",
        action="store_true",
        help="Read the joined sample dataset after refreshing the SQL view",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional LIMIT applied when --load is used",
    )
    parser.set_defaults(only_eligible=True)
    parser.add_argument(
        "--only-eligible",
        dest="only_eligible",
        action="store_true",
        help="Only read rows where sample_eligible = TRUE (default)",
    )
    parser.add_argument(
        "--include-ineligible",
        dest="only_eligible",
        action="store_false",
        help="Read all joined rows regardless of sample_eligible",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    return parser.parse_args()


def main():
    args = parse_args()
    engine = get_engine()
    result = build_sample_dataset(
        engine,
        start_date=args.start_date,
        end_date=args.end_date,
        only_eligible=args.only_eligible,
        limit=args.limit,
        load=args.load,
    )

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return

    logger.info(
        "sample view ready: view_name=%s view_refreshed=%s load=%s rows_loaded=%s column_count=%s start_date=%s end_date=%s only_eligible=%s limit=%s",
        result.get("view_name"),
        result.get("view_refreshed"),
        result.get("load"),
        result.get("rows_loaded"),
        result.get("column_count"),
        result.get("start_date"),
        result.get("end_date"),
        result.get("only_eligible"),
        result.get("limit"),
    )


if __name__ == "__main__":
    main()
