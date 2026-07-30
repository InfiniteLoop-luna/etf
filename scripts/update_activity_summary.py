# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BEIJING_TZ = ZoneInfo("Asia/Shanghai")


def _load_json(path: Path) -> dict | list | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def build_update_activity_summary() -> dict:
    now = datetime.now(BEIJING_TZ)
    last_update = _load_json(PROJECT_ROOT / "last_update.json") or {}
    freshness = _load_json(PROJECT_ROOT / "data" / "funding_freshness_summary.json") or {}

    if not freshness:
        try:
            from scripts.funding_freshness_summary import build_summary as build_funding_freshness_summary
            freshness = build_funding_freshness_summary()
        except Exception:
            freshness = {}

    stale_items = [item for item in (freshness.get("items") or []) if not item.get("ok")]

    summary = {
        "generated_at": now.isoformat(),
        "timezone": "Asia/Shanghai",
        "last_update": {
            "update_date": last_update.get("update_date"),
            "last_update": last_update.get("last_update"),
        },
        "funding_freshness": {
            "target_date": freshness.get("target_date"),
            "all_ok": freshness.get("all_ok"),
            "stale_count": len(stale_items),
            "stale_items": stale_items,
        },
        "notes": [
            "页面更新信息来自 last_update.json",
            "资金链健康结果来自 funding_freshness_summary.json（若缺失则运行时即时生成）",
            "目标日期按北京时间昨天计算",
        ],
    }
    return summary


def main() -> int:
    summary = build_update_activity_summary()
    out_path = PROJECT_ROOT / "data" / "update_activity_summary.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
