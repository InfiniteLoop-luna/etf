#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Update Tushare lhb landing tables."""

import argparse
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
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
            mapping = {}
            with open(secrets_path, "r", encoding="utf-8") as f:
                for line in f:
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
                "TUSHARE_TOKEN",
            ]:
                if key in mapping and not os.environ.get(key):
                    os.environ[key] = mapping[key]
            return

    with open(secrets_path, "rb") as f:
        secrets = tomllib.load(f)

    env_map = {
        "ETF_PG_PASSWORD": secrets.get("ETF_PG_PASSWORD") or secrets.get("PGPASSWORD"),
        "ETF_PG_HOST": secrets.get("ETF_PG_HOST"),
        "ETF_PG_USER": secrets.get("ETF_PG_USER"),
        "ETF_PG_DATABASE": secrets.get("ETF_PG_DATABASE"),
        "ETF_PG_URL": secrets.get("ETF_PG_URL") or secrets.get("DATABASE_URL"),
        "DATABASE_URL": secrets.get("DATABASE_URL"),
        "TUSHARE_TOKEN": secrets.get("TUSHARE_TOKEN"),
    }
    for key, value in env_map.items():
        if value and not os.environ.get(key):
            os.environ[key] = str(value)


def inject_env_from_dotenv():
    dotenv_path = os.path.join(PROJECT_ROOT, ".env")
    if not os.path.exists(dotenv_path):
        return
    with open(dotenv_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if not os.environ.get(key.strip()):
                os.environ[key.strip()] = value.strip().strip('"').strip("'")


def main():
    parser = argparse.ArgumentParser(description="龙虎榜数据更新工具")
    parser.add_argument("--full", action="store_true", help="全量拉取今年以来数据")
    parser.add_argument("--start", type=str, default=None, help="起始日期 YYYYMMDD")
    parser.add_argument("--end", type=str, default=None, help="结束日期 YYYYMMDD")
    parser.add_argument("--datasets", type=str, default=None, help="逗号分隔数据集：top_list,top_inst")
    parser.add_argument("--init-tables", action="store_true", help="仅初始化 landing tables")
    parser.add_argument("--batch-days", type=int, default=3, help="增量任务每次最多处理的交易日数")
    parser.add_argument("--sleep", type=float, default=0.35, help="单次请求间隔秒数")
    parser.add_argument("--lookback-days", type=int, default=2, help="增量回看天数")
    args = parser.parse_args()

    inject_env_from_secrets()
    inject_env_from_dotenv()

    from src.lhb_sync import current_year_start, ensure_all_tables, get_engine, run_sync

    if args.init_tables:
        engine = get_engine()
        ensure_all_tables(engine)
        print("[OK] lhb landing tables initialized")
        return

    target_datasets = [item.strip() for item in args.datasets.split(",") if item.strip()] if args.datasets else None
    start = args.start or (current_year_start() if args.full else None)

    result = run_sync(
        datasets=target_datasets,
        start_date=start,
        end_date=args.end,
        batch_days=max(1, int(args.batch_days)),
        request_sleep_seconds=max(0.0, float(args.sleep)),
        lookback_days=max(0, int(args.lookback_days)),
    )
    print(f"[OK] lhb sync completed: {result}")


if __name__ == "__main__":
    main()
