from __future__ import annotations

import json
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Callable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
STATUS_PATH = DATA_DIR / "manual_refresh_status.json"


def _now_iso() -> str:
    return datetime.now().isoformat()


def _run_python_script(script_rel_path: str, argv: list[str] | None = None) -> None:
    import runpy
    import sys

    script_path = PROJECT_ROOT / script_rel_path
    old_argv = sys.argv[:]
    try:
        sys.argv = [str(script_path)] + list(argv or [])
        runpy.run_path(str(script_path), run_name="__main__")
    finally:
        sys.argv = old_argv


def _build_refresh_registry() -> dict[str, dict]:
    return {
        "etf_share_size": {
            "label": "ETF 份额数据",
            "runner": lambda: _run_python_script("src/fetch_etf_share_size.py"),
        },
        "etf_category_agg": {
            "label": "ETF 分类聚合",
            "runner": lambda: _run_python_script("src/aggregate_etf_categories.py"),
            "depends_on": ["etf_share_size"],
        },
        "moneyflow": {
            "label": "资金流向",
            "runner": lambda: _run_python_script(
                "update_moneyflow.py",
                ["--datasets", "moneyflow,moneyflow_hsgt,moneyflow_ind_ths,moneyflow_dc_ind", "--lookback-days", "1"],
            ),
        },
        "hotmoney_detail": {
            "label": "游资明细",
            "runner": lambda: _run_python_script(
                "update_hotmoney.py",
                [
                    "--datasets",
                    "hm_detail",
                    "--detail-batch-days",
                    "10",
                    "--detail-sleep",
                    "35",
                    "--detail-lookback-days",
                    "0",
                    "--detail-max-days",
                    "10",
                ],
            ),
        },
        "lhb": {
            "label": "龙虎榜",
            "runner": lambda: _run_python_script(
                "update_lhb_monitor.py",
                ["--datasets", "top_list,top_inst", "--batch-days", "3", "--sleep", "0.35", "--lookback-days", "2"],
            ),
        },
        "limitup": {
            "label": "打板情绪",
            "runner": lambda: _run_python_script(
                "update_limitup_monitor.py",
                ["--datasets", "limit_list_d,limit_step,limit_cpt_list,kpl_list,limit_list_ths"],
            ),
        },
        "margin_detail": {
            "label": "两融明细",
            "runner": lambda: _run_python_script(
                "update_margin.py",
                ["--datasets", "margin,margin_detail", "--lookback-days", "2"],
            ),
        },
    }


def get_refresh_registry() -> dict[str, dict]:
    return _build_refresh_registry()


def load_manual_refresh_status() -> dict:
    if not STATUS_PATH.exists():
        return {
            "status": "idle",
            "selected_keys": [],
            "completed_keys": [],
            "failed_keys": [],
            "message": None,
        }
    try:
        return json.loads(STATUS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {
            "status": "unknown",
            "selected_keys": [],
            "completed_keys": [],
            "failed_keys": [],
            "message": "status file unreadable",
        }


def _save_status(payload: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    STATUS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _refresh_summary_files() -> None:
    _run_python_script("scripts/funding_freshness_summary.py")
    _run_python_script("scripts/update_activity_summary.py")
    _run_python_script("scripts/data_task_status_summary.py")


def _load_funding_freshness_summary_file() -> dict:
    summary_path = DATA_DIR / "funding_freshness_summary.json"
    if not summary_path.exists():
        return {}
    try:
        return json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _expand_selected_keys(selected_keys: list[str], registry: dict[str, dict]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()

    def _add(key: str):
        if key not in registry or key in seen:
            return
        for dep in registry[key].get("depends_on", []) or []:
            _add(dep)
        seen.add(key)
        ordered.append(key)

    for key in selected_keys:
        _add(key)
    return ordered


def run_manual_refresh(selected_keys: list[str]) -> dict:
    registry = get_refresh_registry()
    normalized_keys = [str(key).strip() for key in (selected_keys or []) if str(key).strip() in registry]
    expanded_keys = _expand_selected_keys(normalized_keys, registry)
    if not expanded_keys:
        payload = {
            "status": "idle",
            "started_at": None,
            "finished_at": _now_iso(),
            "selected_keys": [],
            "completed_keys": [],
            "failed_keys": [],
            "message": "no valid refresh targets selected",
        }
        _save_status(payload)
        return payload

    before_summary = _load_funding_freshness_summary_file()
    before_stale_keys = [
        str(item.get("key") or "").strip()
        for item in (before_summary.get("items") or [])
        if not item.get("ok") and str(item.get("key") or "").strip()
    ]

    payload = {
        "status": "running",
        "started_at": _now_iso(),
        "finished_at": None,
        "selected_keys": expanded_keys,
        "requested_keys": normalized_keys,
        "completed_keys": [],
        "failed_keys": [],
        "current_key": None,
        "message": "manual refresh started",
        "before_stale_keys": before_stale_keys,
        "recovered_keys": [],
        "remaining_stale_keys": before_stale_keys,
    }
    _save_status(payload)

    for key in expanded_keys:
        payload["current_key"] = key
        payload["message"] = f"refreshing {key}"
        _save_status(payload)
        try:
            runner: Callable[[], None] = registry[key]["runner"]
            runner()
            payload["completed_keys"].append(key)
        except Exception as exc:
            payload["failed_keys"].append({"key": key, "error": str(exc)})
            payload["message"] = f"refresh {key} failed"
            _save_status(payload)

    try:
        _refresh_summary_files()
    except Exception as exc:
        payload["failed_keys"].append({"key": "summary_files", "error": str(exc)})

    after_summary = _load_funding_freshness_summary_file()
    after_stale_keys = [
        str(item.get("key") or "").strip()
        for item in (after_summary.get("items") or [])
        if not item.get("ok") and str(item.get("key") or "").strip()
    ]
    requested_set = set(expanded_keys)
    after_stale_set = set(after_stale_keys)
    before_stale_set = set(before_stale_keys)
    payload["recovered_keys"] = sorted((before_stale_set & requested_set) - after_stale_set)
    payload["remaining_stale_keys"] = after_stale_keys

    payload["status"] = "success" if not payload["failed_keys"] else "partial_failed"
    payload["finished_at"] = _now_iso()
    payload["current_key"] = None
    payload["message"] = "manual refresh finished"
    _save_status(payload)
    return payload


def trigger_manual_refresh_bg(selected_keys: list[str]) -> bool:
    current = load_manual_refresh_status()
    if current.get("status") == "running":
        return False

    thread = threading.Thread(target=run_manual_refresh, args=(list(selected_keys or []),), daemon=True)
    thread.start()
    return True
