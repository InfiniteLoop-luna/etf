#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
按全站自选股并集，增量刷新个股深度研究报告缓存。
"""
from __future__ import annotations

import argparse
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
        "STOCK_RESEARCH_LLM_API_KEY": secrets.get("STOCK_RESEARCH_LLM_API_KEY"),
        "STOCK_RESEARCH_LLM_ENABLED": secrets.get("STOCK_RESEARCH_LLM_ENABLED"),
        "STOCK_RESEARCH_LLM_BASE_URL": secrets.get("STOCK_RESEARCH_LLM_BASE_URL"),
        "STOCK_RESEARCH_LLM_MODEL": secrets.get("STOCK_RESEARCH_LLM_MODEL"),
        "STOCK_RESEARCH_ENABLE_AKSHARE": secrets.get("STOCK_RESEARCH_ENABLE_AKSHARE"),
        "STOCK_RESEARCH_NEWS_LIMIT": secrets.get("STOCK_RESEARCH_NEWS_LIMIT"),
        "STOCK_RESEARCH_REPORT_LIMIT": secrets.get("STOCK_RESEARCH_REPORT_LIMIT"),
        "STOCK_RESEARCH_MONEY_FLOW_LIMIT": secrets.get("STOCK_RESEARCH_MONEY_FLOW_LIMIT"),
        "STOCK_RESEARCH_LHB_LIMIT": secrets.get("STOCK_RESEARCH_LHB_LIMIT"),
    }
    section = secrets.get("stock_research_llm")
    if isinstance(section, dict):
        mapping.update(
            {
                "STOCK_RESEARCH_LLM_API_KEY": mapping.get("STOCK_RESEARCH_LLM_API_KEY") or section.get("STOCK_RESEARCH_LLM_API_KEY") or section.get("stock_research_llm_api_key"),
                "STOCK_RESEARCH_LLM_ENABLED": mapping.get("STOCK_RESEARCH_LLM_ENABLED") or section.get("STOCK_RESEARCH_LLM_ENABLED") or section.get("stock_research_llm_enabled"),
                "STOCK_RESEARCH_LLM_BASE_URL": mapping.get("STOCK_RESEARCH_LLM_BASE_URL") or section.get("STOCK_RESEARCH_LLM_BASE_URL") or section.get("stock_research_llm_base_url"),
                "STOCK_RESEARCH_LLM_MODEL": mapping.get("STOCK_RESEARCH_LLM_MODEL") or section.get("STOCK_RESEARCH_LLM_MODEL") or section.get("stock_research_llm_model"),
            }
        )
    akshare_section = secrets.get("stock_research_akshare")
    if isinstance(akshare_section, dict):
        mapping.update(
            {
                "STOCK_RESEARCH_ENABLE_AKSHARE": mapping.get("STOCK_RESEARCH_ENABLE_AKSHARE") or akshare_section.get("STOCK_RESEARCH_ENABLE_AKSHARE") or akshare_section.get("stock_research_enable_akshare"),
                "STOCK_RESEARCH_NEWS_LIMIT": mapping.get("STOCK_RESEARCH_NEWS_LIMIT") or akshare_section.get("STOCK_RESEARCH_NEWS_LIMIT") or akshare_section.get("stock_research_news_limit"),
                "STOCK_RESEARCH_REPORT_LIMIT": mapping.get("STOCK_RESEARCH_REPORT_LIMIT") or akshare_section.get("STOCK_RESEARCH_REPORT_LIMIT") or akshare_section.get("stock_research_report_limit"),
                "STOCK_RESEARCH_MONEY_FLOW_LIMIT": mapping.get("STOCK_RESEARCH_MONEY_FLOW_LIMIT") or akshare_section.get("STOCK_RESEARCH_MONEY_FLOW_LIMIT") or akshare_section.get("stock_research_money_flow_limit"),
                "STOCK_RESEARCH_LHB_LIMIT": mapping.get("STOCK_RESEARCH_LHB_LIMIT") or akshare_section.get("STOCK_RESEARCH_LHB_LIMIT") or akshare_section.get("stock_research_lhb_limit"),
            }
        )
    for key, value in mapping.items():
        if value and not os.environ.get(key):
            os.environ[key] = str(value)


inject_env_from_secrets()

from src.stock_research_report_store import get_engine
from src.watchlist_stock_research_refresh import refresh_watchlist_stock_research_reports


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="刷新自选股个股深度研究报告缓存")
    parser.add_argument("--limit", type=int, default=None, help="最多处理多少只股票，便于小批量回填")
    parser.add_argument("--only-code", default=None, help="只处理指定股票代码，支持 000001 或 000001.SZ")
    parser.add_argument("--username", default=None, help="只刷新指定用户的自选股")
    parser.add_argument("--force", action="store_true", help="即使已有最新缓存也强制重新生成")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None):
    args = parse_args(argv)
    engine = get_engine()
    summary = refresh_watchlist_stock_research_reports(
        engine,
        username=args.username,
        limit=args.limit,
        only_code=args.only_code,
        force=args.force,
    )
    print(
        "watchlist stock research refresh completed: "
        f"processed={summary['processed']} "
        f"generated={summary['generated']} "
        f"skipped={summary['skipped']} "
        f"failed={summary['failed']} "
        f"locked={summary['locked']}"
    )


if __name__ == "__main__":
    main()
