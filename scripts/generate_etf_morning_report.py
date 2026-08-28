#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.etf_morning_report import generate_and_save_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

if __name__ == "__main__":
    result = generate_and_save_report()
    print(json.dumps({
        "report_trade_date": (result.get("fact_pack") or {}).get("report_trade_date"),
        "saved_at": result.get("saved_at"),
        "llm": result.get("llm") or {},
        "warnings": ((result.get("fact_pack") or {}).get("data_quality") or {}).get("warnings") or [],
    }, ensure_ascii=False, indent=2))
