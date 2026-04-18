#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
打板情绪与接力监控数据更新脚本（本地 / VPS）

用法：
  python update_limitup_monitor.py --init-tables
  python update_limitup_monitor.py --full
  python update_limitup_monitor.py --start 20240101 --end 20240430
  python update_limitup_monitor.py --datasets limit_list_d,limit_step,limit_list_ths
"""
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
                        k, v = line.split("=", 1)
                        mapping[k.strip()] = v.strip().strip('"').strip("'")
            for key in [
                "ETF_PG_PASSWORD", "PGPASSWORD", "ETF_PG_HOST", "ETF_PG_USER",
                "ETF_PG_DATABASE", "ETF_PG_URL", "DATABASE_URL", "TUSHARE_TOKEN",
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
        "TUSHARE_TOKEN": secrets.get("TUSHARE_TOKEN"),
    }
    for key, val in env_map.items():
        if val and not os.environ.get(key):
            os.environ[key] = str(val)


def inject_env_from_dotenv():
    dotenv_path = os.path.join(PROJECT_ROOT, ".env")
    if not os.path.exists(dotenv_path):
        return
    with open(dotenv_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if not os.environ.get(k.strip()):
                os.environ[k.strip()] = v.strip().strip('"').strip("'")


def main():
    parser = argparse.ArgumentParser(description="打板情绪监控数据更新工具")
    parser.add_argument("--full", action="store_true", help="全量拉取（默认从20240101起）")
    parser.add_argument("--start", type=str, default=None, help="起始日期 YYYYMMDD")
    parser.add_argument("--end", type=str, default=None, help="结束日期 YYYYMMDD")
    parser.add_argument("--datasets", type=str, default=None, help="逗号分隔数据集")
    parser.add_argument("--init-tables", action="store_true", help="仅初始化 landing tables")
    args = parser.parse_args()

    inject_env_from_secrets()
    inject_env_from_dotenv()

    from src.limitup_sync import DEFAULT_START_DATE, ensure_all_tables, get_engine, run_sync

    if args.init_tables:
        eng = get_engine()
        ensure_all_tables(eng)
        print("✅ 打板专题 landing tables 初始化完成")
        return

    target_ds = [d.strip() for d in args.datasets.split(",")] if args.datasets else None
    start = args.start or (DEFAULT_START_DATE if args.full else None)

    result = run_sync(datasets=target_ds, start_date=start, end_date=args.end)
    print(f"[OK] 打板专题同步完成: {result}")


if __name__ == "__main__":
    main()
