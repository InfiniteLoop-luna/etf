#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.trend_reco_store import backfill_directory, get_engine, iter_reco_files

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


DEFAULT_OUTPUT_DIR = "data/recommendations"


def parse_args():
    parser = argparse.ArgumentParser(description="回填每日趋势推荐历史 JSON 到 PostgreSQL。")
    parser.add_argument("--base-dir", default=DEFAULT_OUTPUT_DIR, help="推荐 JSON 根目录")
    parser.add_argument("--include-latest", action="store_true", help="包含 latest_trend_recommendations.json")
    parser.add_argument("--limit", type=int, default=0, help="仅处理前 N 个文件（按文件名升序）")
    parser.add_argument("--dry-run", action="store_true", help="只预览将处理哪些文件，不写数据库")
    parser.add_argument("--json", action="store_true", help="输出 JSON 摘要")
    return parser.parse_args()



def main():
    args = parse_args()
    base_dir = Path(args.base_dir)
    files = iter_reco_files(base_dir, include_latest=args.include_latest)
    if args.limit and args.limit > 0:
        files = files[: int(args.limit)]

    preview = {
        "base_dir": str(base_dir),
        "include_latest": bool(args.include_latest),
        "files": [str(p) for p in files],
        "count": len(files),
    }

    if args.dry_run:
        if args.json:
            print(json.dumps(preview, ensure_ascii=False, indent=2))
        else:
            print(f"base_dir={preview['base_dir']}")
            print(f"include_latest={preview['include_latest']}")
            print(f"count={preview['count']}")
            for fp in preview["files"]:
                print(fp)
        return

    engine = get_engine()
    result = backfill_directory(
        engine,
        base_dir,
        include_latest=args.include_latest,
        limit=int(args.limit) if args.limit else None,
    )

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"files={result.get('files')}")
        print(f"item_rows={result.get('item_rows')}")
        for trade_date in result.get("trade_dates") or []:
            print(f"trade_date={trade_date}")


if __name__ == "__main__":
    main()
