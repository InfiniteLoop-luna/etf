#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.fund_estimate_capture import capture_fund_watchlist_closing_estimates
from src.fund_hot_stocks import get_engine


def main() -> int:
    parser = argparse.ArgumentParser(
        description="保存自选基金每日 15:00 收盘估值快照",
    )
    parser.add_argument(
        "--fund-code",
        action="append",
        default=None,
        help="只采集指定基金，可重复传入；默认采集所有用户自选基金并集",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="跳过 15:00 时间检查（仍要求行情时间属于当天且不早于 15:00）",
    )
    args = parser.parse_args()

    summary = capture_fund_watchlist_closing_estimates(
        get_engine(),
        fund_codes=args.fund_code,
        require_after_close=not args.force,
    )
    print(json.dumps(summary, ensure_ascii=False, default=str, indent=2))
    return 1 if summary["status"] == "quote_fetch_failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
