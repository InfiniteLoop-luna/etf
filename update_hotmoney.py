#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
游资名录 / 游资明细 更新脚本
"""
import argparse
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


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
    parser = argparse.ArgumentParser(description="游资数据更新工具")
    parser.add_argument("--full", action="store_true", help="全量拉取（明细默认从20240101起）")
    parser.add_argument("--start", type=str, default=None, help="起始日期 YYYYMMDD")
    parser.add_argument("--end", type=str, default=None, help="结束日期 YYYYMMDD")
    parser.add_argument("--datasets", type=str, default=None, help="逗号分隔数据集 hm_list,hm_detail")
    parser.add_argument("--init-tables", action="store_true", help="仅初始化 landing tables")

    # 慢速补数策略参数
    parser.add_argument("--detail-batch-days", type=int, default=1, help="hm_detail 每次最多补几天")
    parser.add_argument("--detail-sleep", type=float, default=35, help="hm_detail 每个交易日请求间隔(秒)")
    parser.add_argument("--detail-lookback-days", type=int, default=0, help="hm_detail 增量回看天数")
    parser.add_argument("--detail-max-days", type=int, default=None, help="hm_detail 本次最多处理天数")
    parser.add_argument("--detail-continue-on-rate-limit", action="store_true", help="遇限频后不提前停止（默认停止）")

    args = parser.parse_args()

    inject_env_from_dotenv()

    from src.hotmoney_sync import DEFAULT_DETAIL_START_DATE, ensure_all_tables, get_engine, run_sync

    if args.init_tables:
        eng = get_engine()
        ensure_all_tables(eng)
        print("✅ 游资 landing tables 初始化完成")
        return

    target_ds = [d.strip() for d in args.datasets.split(",")] if args.datasets else None
    start = args.start or (DEFAULT_DETAIL_START_DATE if args.full else None)

    result = run_sync(
        datasets=target_ds,
        start_date=start,
        end_date=args.end,
        detail_batch_days=max(1, int(args.detail_batch_days)),
        detail_request_sleep_seconds=max(1.0, float(args.detail_sleep)),
        detail_lookback_days=max(0, int(args.detail_lookback_days)),
        detail_stop_on_rate_limit=not args.detail_continue_on_rate_limit,
        detail_max_days=args.detail_max_days,
    )
    print(f"[OK] 游资数据同步完成: {result}")


if __name__ == "__main__":
    main()
