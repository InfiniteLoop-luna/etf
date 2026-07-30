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


def _file_meta(path: Path) -> dict:
    if not path.exists():
        return {"exists": False, "mtime": None}
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=BEIJING_TZ).isoformat()
    return {"exists": True, "mtime": mtime}


def build_data_task_status_summary() -> dict:
    now = datetime.now(BEIJING_TZ)
    last_update = _load_json(PROJECT_ROOT / "last_update.json") or {}
    funding_freshness = _load_json(PROJECT_ROOT / "data" / "funding_freshness_summary.json") or {}
    update_activity = _load_json(PROJECT_ROOT / "data" / "update_activity_summary.json") or {}

    summary = {
        "generated_at": now.isoformat(),
        "timezone": "Asia/Shanghai",
        "tasks": [
            {
                "task": "页面更新标记",
                "source": "last_update.json",
                "status": "ok" if last_update else "missing",
                "latest": last_update.get("update_date") or "-",
                "detail": last_update.get("last_update") or "-",
                "file": _file_meta(PROJECT_ROOT / "last_update.json"),
            },
            {
                "task": "资金链健康摘要",
                "source": "data/funding_freshness_summary.json",
                "status": "ok" if funding_freshness.get("all_ok") is True else ("warn" if funding_freshness else "missing"),
                "latest": funding_freshness.get("target_date") or "-",
                "detail": f"all_ok={funding_freshness.get('all_ok')}",
                "file": _file_meta(PROJECT_ROOT / "data" / "funding_freshness_summary.json"),
            },
            {
                "task": "最近更新日志摘要",
                "source": "data/update_activity_summary.json",
                "status": "ok" if update_activity else "missing",
                "latest": ((update_activity.get("last_update") or {}).get("effective_update_date") or "-"),
                "detail": ((update_activity.get("last_update") or {}).get("effective_last_update") or "-"),
                "file": _file_meta(PROJECT_ROOT / "data" / "update_activity_summary.json"),
            },
            {
                "task": "资金链健康卡片源",
                "source": "data/funding_freshness_summary.json",
                "status": "ok" if funding_freshness else "missing",
                "latest": funding_freshness.get("generated_at") or "-",
                "detail": f"stale_count={((update_activity.get('funding_freshness') or {}).get('stale_count'))}",
                "file": _file_meta(PROJECT_ROOT / "data" / "funding_freshness_summary.json"),
            },
        ],
    }
    return summary


def main() -> int:
    summary = build_data_task_status_summary()
    out_path = PROJECT_ROOT / "data" / "data_task_status_summary.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
