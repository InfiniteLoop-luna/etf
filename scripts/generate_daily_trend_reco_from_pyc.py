#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import glob
import importlib.machinery
import importlib.util
import logging
import os
import sys
from pathlib import Path

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


DEFAULT_PYC_GLOB = "src/__pycache__/daily_trend_recommender*.pyc"
DEFAULT_OUTPUT_DIR = "data/recommendations"


def parse_args():
    parser = argparse.ArgumentParser(description="生成每日趋势推荐，并同步写入 PostgreSQL。")
    parser.add_argument("--pyc-path", default="", help="指定 daily_trend_recommender 的 pyc 路径")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="推荐结果输出目录")
    parser.add_argument("--lookback-days", type=int, default=160, help="回看天数")
    parser.add_argument("--min-rows-per-symbol", type=int, default=60, help="单股票最少样本条数")
    parser.add_argument("--topn", type=int, default=10, help="输出 TopN")
    parser.add_argument("--skip-db", action="store_true", help="只生成文件，不写 PostgreSQL")
    return parser.parse_args()



def resolve_pyc_path(explicit_path: str | None = None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if path.exists():
            return path
        raise FileNotFoundError(f"找不到指定 pyc: {explicit_path}")

    matches = [Path(p) for p in sorted(glob.glob(DEFAULT_PYC_GLOB))]
    if not matches:
        raise FileNotFoundError(f"未找到趋势推荐 pyc，搜索模式: {DEFAULT_PYC_GLOB}")

    cache_tag = getattr(sys.implementation, "cache_tag", "") or ""
    tagged_matches = [p for p in matches if cache_tag and cache_tag in p.name]
    if tagged_matches:
        return tagged_matches[-1]

    matches.sort(key=lambda p: (p.stat().st_mtime, p.name))
    return matches[-1]



def load_reco_module(pyc_path: Path):
    module_name = "daily_trend_recommender_pyc"
    loader = importlib.machinery.SourcelessFileLoader(module_name, str(pyc_path))
    spec = importlib.util.spec_from_loader(module_name, loader)
    if spec is None:
        raise RuntimeError(f"无法为 {pyc_path} 创建模块 spec")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    loader.exec_module(module)
    return module



def summarize_payload_local(payload: dict) -> dict:
    return {
        "trade_date": str(payload.get("trade_date") or ""),
        "generated_at": str(payload.get("generated_at") or ""),
        "universe_size": int(payload.get("universe_size") or 0),
        "top_uptrend": len(payload.get("top_uptrend") or []),
        "top_avoid": len(payload.get("top_avoid") or []),
    }



def main():
    args = parse_args()
    pyc_path = resolve_pyc_path(args.pyc_path)
    logger.info("load pyc: %s", pyc_path)
    module = load_reco_module(pyc_path)

    config = module.RecoConfig(
        lookback_days=int(args.lookback_days),
        min_rows_per_symbol=int(args.min_rows_per_symbol),
        topn=int(args.topn),
    )
    payload = module.generate_daily_trend_recommendations(config)

    output_dir = Path(args.output_dir)
    daily_file, latest_file = module.save_recommendations(payload, output_dir)

    summary = summarize_payload_local(payload)
    print("[OK] trend recommendations generated")
    print("trade_date=", summary.get("trade_date"))
    print("generated_at=", summary.get("generated_at"))
    print("universe_size=", summary.get("universe_size"))
    print("top_uptrend=", summary.get("top_uptrend"))
    print("top_avoid=", summary.get("top_avoid"))
    print("daily_file=", daily_file)
    print("latest_file=", latest_file)

    if args.skip_db:
        print("db_write= skipped")
        return

    from src.trend_reco_store import get_engine, upsert_trend_reco_payload

    source_file = daily_file or latest_file
    engine = get_engine()
    result = upsert_trend_reco_payload(engine, payload, source_file=str(source_file))
    print("db_write= ok")
    print("db_trade_date=", result.get("trade_date"))
    print("db_item_rows=", result.get("item_rows"))


if __name__ == "__main__":
    main()
