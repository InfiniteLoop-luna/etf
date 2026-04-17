#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
公募基金持仓热股数据更新脚本（本地 / VPS）

用法示例：
  python update_fund_hot_stocks.py --init-tables
  python update_fund_hot_stocks.py --sync-basic --sync-portfolio --rebuild-agg
  python update_fund_hot_stocks.py --sync-portfolio --period 20241231 --rebuild-agg
  python update_fund_hot_stocks.py --sync-portfolio --start-period 20240101 --end-period 20250331 --rebuild-agg
  python update_fund_hot_stocks.py --query-top --top-n 30
  python update_fund_hot_stocks.py --query-stock 600519.SH
"""
import argparse
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def inject_env_from_secrets():
    """将 .streamlit/secrets.toml 中关键配置注入环境变量（仅在未设置时）"""
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
        "TUSHARE_TOKEN": secrets.get("TUSHARE_TOKEN"),
    }
    for key, val in env_map.items():
        if val and not os.environ.get(key):
            os.environ[key] = str(val)


def main():
    parser = argparse.ArgumentParser(description="公募基金持仓热股数据更新工具")

    parser.add_argument("--init-tables", action="store_true", help="仅初始化数据库表和视图")

    parser.add_argument("--sync-basic", action="store_true", help="同步 fund_basic")
    parser.add_argument("--sync-portfolio", action="store_true", help="同步 fund_portfolio")
    parser.add_argument("--rebuild-agg", action="store_true", help="重建季度热股聚合表")

    parser.add_argument("--period", type=str, default=None, help="单季度 YYYYMMDD（季度末）")
    parser.add_argument("--start-period", type=str, default=None, help="起始季度 YYYYMMDD")
    parser.add_argument("--end-period", type=str, default=None, help="结束季度 YYYYMMDD")

    parser.add_argument("--query-top", action="store_true", help="执行一次热股榜查询（调试）")
    parser.add_argument("--query-stock", type=str, default=None, help="执行一次单股持仓透视查询（调试）")
    parser.add_argument("--top-n", type=int, default=30, help="查询返回条数")
    parser.add_argument(
        "--order-by",
        type=str,
        default="heat_score",
        choices=["heat_score", "holding_fund_count", "total_mkv", "delta_holding_fund_count", "delta_total_mkv"],
        help="热股榜排序方式",
    )

    args = parser.parse_args()
    inject_env_from_secrets()

    from src.fund_hot_stocks import (
        ensure_all_tables,
        get_engine,
        query_hot_stocks_leaderboard,
        query_stock_fund_holding_detail,
        rebuild_hot_stock_aggregate,
        run_sync,
        sync_fund_basic,
        sync_fund_portfolio,
    )
    from src.volume_fetcher import _init_tushare

    # 默认行为：不传任何开关时，执行一轮 sync+agg
    no_action = not any(
        [
            args.init_tables,
            args.sync_basic,
            args.sync_portfolio,
            args.rebuild_agg,
            args.query_top,
            args.query_stock,
        ]
    )

    engine = get_engine()

    if args.init_tables:
        ensure_all_tables(engine)
        print("[OK] fund_hot_stocks 数据库表、视图、聚合表初始化完成")

    if no_action:
        result = run_sync(
            sync_basic=True,
            sync_portfolio=True,
            rebuild_agg=True,
            period=args.period,
            start_period=args.start_period,
            end_period=args.end_period,
        )
        print(f"[OK] 默认执行完成: {result}")
    else:
        pro = None
        if args.sync_basic or args.sync_portfolio:
            pro = _init_tushare()
            ensure_all_tables(engine)

        if args.sync_basic:
            n = sync_fund_basic(engine, pro)
            print(f"[OK] fund_basic 同步完成，写入 {n} 行")

        if args.sync_portfolio:
            n = sync_fund_portfolio(
                engine,
                pro,
                period=args.period,
                start_period=args.start_period,
                end_period=args.end_period,
            )
            print(f"[OK] fund_portfolio 同步完成，写入 {n} 行")

        if args.rebuild_agg:
            n = rebuild_hot_stock_aggregate(
                engine,
                start_period=args.start_period if not args.period else args.period,
                end_period=args.end_period if not args.period else args.period,
            )
            print(f"[OK] 热股聚合重建完成，写入 {n} 行")

    if args.query_top:
        df = query_hot_stocks_leaderboard(
            period=args.period,
            top_n=args.top_n,
            order_by=args.order_by,
            engine=engine,
        )
        print("\n[QUERY] 热股榜 Top:")
        if df is None or df.empty:
            print("(空)")
        else:
            print(df.to_string(index=False))

    if args.query_stock:
        df = query_stock_fund_holding_detail(
            symbol=args.query_stock,
            period=args.period,
            top_n=args.top_n,
            engine=engine,
        )
        print(f"\n[QUERY] 单股持仓透视: {args.query_stock}")
        if df is None or df.empty:
            print("(空)")
        else:
            print(df.to_string(index=False))


if __name__ == "__main__":
    main()
