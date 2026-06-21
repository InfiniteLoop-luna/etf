#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import sys


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.stock_business_exporter import export_stock_business_from_database


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(
        description="Export stock code, current main business, and product/business scope from the project database."
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory for the generated Excel file. Defaults to output/stock_business.",
    )
    parser.add_argument(
        "--include-delisted",
        action="store_true",
        help="Include non-current or delisted rows from vw_ts_stock_basic.",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    file_path, row_count = export_stock_business_from_database(
        output_dir=args.output_dir,
        active_only=not args.include_delisted,
    )

    if args.json:
        print(json.dumps({"output": str(file_path), "rows": row_count}, ensure_ascii=False, indent=2))
    else:
        print(f"导出完成: {file_path}")
        print(f"记录数: {row_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
