#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.etf_morning_report import BEIJING_TZ, generate_and_save_report
from src.morning_report_notifier import send_serverchan_report_for_users

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def parse_args():
    parser = argparse.ArgumentParser(description="Generate the evidence-backed ETF morning report")
    parser.add_argument("--trade-date", default=None, help="Optional report trade date in YYYY-MM-DD format")
    parser.add_argument("--no-notify", action="store_true", help="Generate without sending configured notifications")
    parser.add_argument("--force-notify", action="store_true", help="Resend even if this trade date was delivered")
    return parser.parse_args()


def is_sse_trading_day_today() -> bool | None:
    """Fail closed when the exchange calendar cannot be verified."""
    try:
        from src.volume_fetcher import _init_tushare

        today = datetime.now(BEIJING_TZ).strftime("%Y%m%d")
        calendar = _init_tushare().trade_cal(
            exchange="SSE",
            start_date=today,
            end_date=today,
        )
        if calendar is None or calendar.empty or "is_open" not in calendar.columns:
            return None
        return bool(int(calendar.iloc[0]["is_open"]))
    except Exception as exc:
        logging.warning("SSE trading calendar check failed; notification suppressed: %s", exc)
        return None


if __name__ == "__main__":
    args = parse_args()
    result = generate_and_save_report(args.trade_date)
    if args.no_notify:
        notification = {"channel": "serverchan", "status": "disabled_by_cli"}
    elif args.force_notify:
        notification = send_serverchan_report_for_users(result, force=True)
    else:
        is_open_day = is_sse_trading_day_today()
        if is_open_day is True:
            notification = send_serverchan_report_for_users(result)
        elif is_open_day is False:
            notification = {"channel": "serverchan", "status": "suppressed_non_trading_day"}
        else:
            notification = {"channel": "serverchan", "status": "suppressed_calendar_unavailable"}
    print(json.dumps({
        "report_trade_date": (result.get("fact_pack") or {}).get("report_trade_date"),
        "saved_at": result.get("saved_at"),
        "report_status": ((result.get("fact_pack") or {}).get("data_quality") or {}).get("report_status"),
        "coverage_score": ((result.get("fact_pack") or {}).get("data_quality") or {}).get("coverage_score"),
        "llm": result.get("llm") or {},
        "warnings": ((result.get("fact_pack") or {}).get("data_quality") or {}).get("warnings") or [],
        "notification": notification,
    }, ensure_ascii=False, indent=2))
    if notification.get("status") in {"failed", "partial_failure"}:
        raise SystemExit(2)
