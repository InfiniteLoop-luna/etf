#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
融资融券数据初始化与每日更新脚本。
用法:
  python update_margin.py
  python update_margin.py --full
  python update_margin.py --datasets margin,margin_detail
  python update_margin.py --init-tables
"""
import argparse
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def inject_env_from_secrets() -> None:
    secrets_path = os.path.join(PROJECT_ROOT, ".streamlit", "secrets.toml")
    if not os.path.exists(secrets_path):
        return

    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib
        except ImportError:
            mapping = {}
            with open(secrets_path, "r", encoding="utf-8") as file:
                for line in file:
                    line = line.strip()
                    if "=" in line and not line.startswith("#"):
                        key, value = line.split("=", 1)
                        mapping[key.strip()] = value.strip().strip('"').strip("'")
            for key in [
                "ETF_PG_PASSWORD",
                "PGPASSWORD",
                "ETF_PG_HOST",
                "ETF_PG_USER",
                "ETF_PG_DATABASE",
                "ETF_PG_URL",
                "DATABASE_URL",
            ]:
                if key in mapping and not os.environ.get(key):
                    os.environ[key] = mapping[key]
            return

    with open(secrets_path, "rb") as file:
        secrets = tomllib.load(file)

    mappings = {
        "ETF_PG_PASSWORD": secrets.get("ETF_PG_PASSWORD") or secrets.get("PGPASSWORD"),
        "ETF_PG_HOST": secrets.get("ETF_PG_HOST"),
        "ETF_PG_USER": secrets.get("ETF_PG_USER"),
        "ETF_PG_DATABASE": secrets.get("ETF_PG_DATABASE"),
        "ETF_PG_URL": secrets.get("ETF_PG_URL") or secrets.get("DATABASE_URL"),
    }
    for key, value in mappings.items():
        if value and not os.environ.get(key):
            os.environ[key] = str(value)


def main() -> None:
    parser = argparse.ArgumentParser(description="融资融券数据更新工具")
    parser.add_argument("--full", action="store_true", help="全量回补，从 2022-01-01 起")
    parser.add_argument("--start", type=str, default=None, help="自定义起始日期 YYYYMMDD")
    parser.add_argument("--end", type=str, default=None, help="自定义结束日期 YYYYMMDD")
    parser.add_argument("--datasets", type=str, default=None, help="逗号分隔数据集，如 margin,margin_detail")
    parser.add_argument("--init-tables", action="store_true", help="仅初始化数据库表和视图")
    parser.add_argument("--lookback-days", type=int, default=2, help="增量同步回看天数")
    args = parser.parse_args()

    inject_env_from_secrets()

    from src.margin_fetcher import DEFAULT_START_DATE, _get_engine_cached, ensure_all_tables, run_sync

    if args.lookback_days < 0:
        raise ValueError("--lookback-days 不能小于 0")
    os.environ["TUSHARE_MARGIN_LOOKBACK_DAYS"] = str(args.lookback_days)

    if args.init_tables:
        ensure_all_tables(_get_engine_cached())
        print("✅ 融资融券数据库表和视图初始化完成")
        return

    target_datasets = [item.strip() for item in args.datasets.split(",")] if args.datasets else None
    start = DEFAULT_START_DATE if args.full else args.start
    result = run_sync(datasets=target_datasets, start_date=start, end_date=args.end)
    print(f"✅ 融资融券同步完成：{result}")


if __name__ == "__main__":
    main()
