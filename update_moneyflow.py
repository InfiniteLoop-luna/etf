#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
资金流向数据初始化 & 每日更新脚本（本地使用）
用法:
  python update_moneyflow.py                    # 增量更新（只拉未入库日期）
  python update_moneyflow.py --full             # 全量初始化（从2026-01-01至今）
  python update_moneyflow.py --init-tables      # 仅建表/建视图
  python update_moneyflow.py --datasets moneyflow,moneyflow_hsgt  # 指定数据集
"""
import os
import sys
import argparse

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def load_pg_password_from_secrets():
    """尝试从 .streamlit/secrets.toml 读取数据库密码"""
    secrets_path = os.path.join(PROJECT_ROOT, ".streamlit", "secrets.toml")
    if not os.path.exists(secrets_path):
        return None
    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib
        except ImportError:
            with open(secrets_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("ETF_PG_PASSWORD") or line.startswith("PGPASSWORD"):
                        parts = line.split("=", 1)
                        if len(parts) == 2:
                            return parts[1].strip().strip('"').strip("'")
            return None

    with open(secrets_path, "rb") as f:
        secrets = tomllib.load(f)

    return (
        secrets.get("ETF_PG_PASSWORD")
        or secrets.get("PGPASSWORD")
        or secrets.get("database", {}).get("ETF_PG_PASSWORD")
        or secrets.get("database", {}).get("password")
    )


def inject_env_from_secrets():
    """将 secrets.toml 中的关键配置注入环境变量（仅在未设置时）"""
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
                        k, v = line.split("=", 1)
                        mapping[k.strip()] = v.strip().strip('"').strip("'")
            for key in ["ETF_PG_PASSWORD", "PGPASSWORD", "ETF_PG_HOST",
                        "ETF_PG_USER", "ETF_PG_DATABASE", "ETF_PG_URL", "DATABASE_URL"]:
                if key in mapping and not os.environ.get(key):
                    os.environ[key] = mapping[key]
            return

    with open(secrets_path, "rb") as f:
        secrets = tomllib.load(f)

    pg_keys = {
        "ETF_PG_PASSWORD": secrets.get("ETF_PG_PASSWORD") or secrets.get("PGPASSWORD"),
        "ETF_PG_HOST": secrets.get("ETF_PG_HOST"),
        "ETF_PG_USER": secrets.get("ETF_PG_USER"),
        "ETF_PG_DATABASE": secrets.get("ETF_PG_DATABASE"),
        "ETF_PG_URL": secrets.get("ETF_PG_URL") or secrets.get("DATABASE_URL"),
    }
    for key, val in pg_keys.items():
        if val and not os.environ.get(key):
            os.environ[key] = str(val)


def main():
    parser = argparse.ArgumentParser(description="资金流向数据更新工具")
    parser.add_argument("--full", action="store_true", help="全量拉取（从2026-01-01起）")
    parser.add_argument("--start", type=str, default=None, help="自定义起始日期 YYYYMMDD")
    parser.add_argument("--end", type=str, default=None, help="自定义结束日期 YYYYMMDD")
    parser.add_argument("--datasets", type=str, default=None,
                        help="逗号分隔数据集，如 moneyflow,moneyflow_hsgt")
    parser.add_argument("--init-tables", action="store_true", help="仅初始化数据库表和视图")
    parser.add_argument("--lookback-days", type=int, default=1,
                        help="增量同步时向前回看 N 天，避免补发/修订数据漏采")
    parser.add_argument("--purge-before-start", action="store_true",
                        help="删除 start 之前的资金流向数据后再重建指定区间")
    args = parser.parse_args()

    inject_env_from_secrets()

    from src.moneyflow_fetcher import DEFAULT_START_DATE, MONEYFLOW_TABLES, get_engine, ensure_all_tables, run_sync
    from sqlalchemy import text
    from datetime import datetime

    if args.init_tables:
        eng = get_engine()
        ensure_all_tables(eng)
        print("✅ 数据库表和视图初始化完成")
        return

    target_ds = [d.strip() for d in args.datasets.split(",")] if args.datasets else None
    start = DEFAULT_START_DATE if args.full else args.start

    if args.lookback_days < 0:
        raise ValueError("--lookback-days 不能小于 0")
    os.environ["TUSHARE_MF_LOOKBACK_DAYS"] = str(args.lookback_days)

    if args.purge_before_start and not start:
        raise ValueError("--purge-before-start 需要配合 --start 或 --full 使用")

    if args.purge_before_start and start:
        eng = get_engine()
        with eng.begin() as conn:
            for table_name in MONEYFLOW_TABLES.values():
                conn.execute(
                    text(f"DELETE FROM {table_name} WHERE trade_date < :cutoff"),
                    {"cutoff": datetime.strptime(start, "%Y%m%d").date()},
                )
        print(f"✅ 已清理 {start} 之前的资金流向数据")

    run_sync(datasets=target_ds, start_date=start, end_date=args.end)


if __name__ == "__main__":
    main()
