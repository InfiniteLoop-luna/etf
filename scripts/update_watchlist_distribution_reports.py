#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
按全站自选股并集，增量刷新深度出货分析缓存。
"""
from __future__ import annotations

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def inject_env_from_secrets():
    secrets_path = os.path.join(PROJECT_ROOT, ".streamlit", "secrets.toml")
    if not os.path.exists(secrets_path):
        return
    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib
        except ImportError:
            return

    with open(secrets_path, "rb") as f:
        secrets = tomllib.load(f)

    mapping = {
        "ETF_PG_PASSWORD": secrets.get("ETF_PG_PASSWORD") or secrets.get("PGPASSWORD"),
        "ETF_PG_HOST": secrets.get("ETF_PG_HOST"),
        "ETF_PG_PORT": secrets.get("ETF_PG_PORT"),
        "ETF_PG_USER": secrets.get("ETF_PG_USER"),
        "ETF_PG_DATABASE": secrets.get("ETF_PG_DATABASE"),
        "ETF_PG_URL": secrets.get("ETF_PG_URL") or secrets.get("DATABASE_URL"),
    }
    for key, value in mapping.items():
        if value and not os.environ.get(key):
            os.environ[key] = str(value)


inject_env_from_secrets()

from src.distribution_report_store import get_engine
from src.watchlist_distribution_refresh import refresh_watchlist_distribution_reports


def main():
    engine = get_engine()
    summary = refresh_watchlist_distribution_reports(engine)
    print(
        "watchlist distribution refresh completed: "
        f"processed={summary['processed']} "
        f"generated={summary['generated']} "
        f"skipped={summary['skipped']} "
        f"failed={summary['failed']}"
    )


if __name__ == "__main__":
    main()
