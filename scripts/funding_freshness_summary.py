# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

BEIJING_TZ = ZoneInfo("Asia/Shanghai")

from src.aggregate_etf_categories import get_engine as get_etf_engine, get_latest_agg_date
from src.fetch_etf_share_size import get_latest_date as get_etf_share_latest_date
from src.hotmoney_monitor import get_hotmoney_latest_detail_date
from src.limitup_monitor import get_limitup_latest_date
from src.lhb_sync import get_engine as get_lhb_engine
from src.margin_fetcher import _get_engine_cached as get_margin_engine, get_margin_latest_date
from src.moneyflow_fetcher import get_engine as get_moneyflow_engine, get_moneyflow_latest_date
from src.volume_fetcher import _init_tushare
from sqlalchemy import text


@dataclass
class FreshnessItem:
    key: str
    latest_date: str | None
    target_date: str
    ok: bool
    note: str | None = None


def beijing_today_ymd() -> str:
    return datetime.now(BEIJING_TZ).strftime("%Y%m%d")


def get_latest_open_trade_date_ymd(lookback_days: int = 14, publish_cutoff_hour: int = 23) -> str:
    now = datetime.now(BEIJING_TZ)
    end_date = now.date()
    start_date = end_date - timedelta(days=max(lookback_days, 1))

    open_dates: list[str] = []
    try:
        pro = _init_tushare()
        cal_df = pro.trade_cal(
            exchange="SSE",
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
            is_open="1",
        )
        if cal_df is not None and not cal_df.empty:
            work = cal_df.copy()
            if "is_open" in work.columns:
                work = work[pd.to_numeric(work["is_open"], errors="coerce").fillna(0).astype(int) == 1]
            date_col = "cal_date" if "cal_date" in work.columns else "trade_date"
            if date_col in work.columns and not work.empty:
                open_dates = sorted(
                    {
                        str(value).replace("-", "")[:8]
                        for value in work[date_col].tolist()
                        if str(value).replace("-", "")[:8].isdigit()
                    }
                )
    except Exception:
        open_dates = []

    if open_dates:
        today_ymd = now.strftime("%Y%m%d")
        if today_ymd in open_dates and now.hour < int(publish_cutoff_hour):
            prior_dates = [value for value in open_dates if value < today_ymd]
            if prior_dates:
                return prior_dates[-1]
        return open_dates[-1]

    current = end_date
    while current.weekday() >= 5:
        current -= timedelta(days=1)
    if current == end_date and now.hour < int(publish_cutoff_hour):
        current -= timedelta(days=1)
        while current.weekday() >= 5:
            current -= timedelta(days=1)
    return current.strftime("%Y%m%d")


def _normalize_ymd(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%Y%m%d")
    s = str(value).strip().replace("-", "")
    return s or None


def get_lhb_latest_date() -> str | None:
    engine = get_lhb_engine()
    sql = """
    SELECT TO_CHAR(MAX(dt), 'YYYYMMDD')
    FROM (
      SELECT MAX(trade_date) AS dt FROM ts_lhb_top_list
      UNION ALL
      SELECT MAX(trade_date) AS dt FROM ts_lhb_top_inst
    ) t
    """
    with engine.connect() as conn:
        row = conn.execute(text(sql)).fetchone()
    return row[0] if row and row[0] else None


def build_summary() -> dict:
    target = get_latest_open_trade_date_ymd()
    items = [
        FreshnessItem(
            key="etf_share_size",
            latest_date=_normalize_ymd(get_etf_share_latest_date(get_etf_engine())),
            target_date=target,
            ok=False,
        ),
        FreshnessItem(
            key="etf_category_agg",
            latest_date=_normalize_ymd(get_latest_agg_date(get_etf_engine())),
            target_date=target,
            ok=False,
        ),
        FreshnessItem(
            key="moneyflow",
            latest_date=_normalize_ymd(get_moneyflow_latest_date(get_moneyflow_engine())),
            target_date=target,
            ok=False,
        ),
        FreshnessItem(
            key="hotmoney_detail",
            latest_date=_normalize_ymd(get_hotmoney_latest_detail_date()),
            target_date=target,
            ok=False,
        ),
        FreshnessItem(
            key="lhb",
            latest_date=_normalize_ymd(get_lhb_latest_date()),
            target_date=target,
            ok=False,
        ),
        FreshnessItem(
            key="limitup",
            latest_date=_normalize_ymd(get_limitup_latest_date()),
            target_date=target,
            ok=False,
        ),
        FreshnessItem(
            key="margin_detail",
            latest_date=_normalize_ymd(get_margin_latest_date(get_margin_engine())),
            target_date=target,
            ok=False,
        ),
    ]

    for item in items:
        item.ok = bool(item.latest_date and item.latest_date >= item.target_date)
        if not item.ok:
            item.note = "behind_target"

    summary = {
        "generated_at": datetime.now(BEIJING_TZ).isoformat(),
        "timezone": "Asia/Shanghai",
        "today": beijing_today_ymd(),
        "target_date": target,
        "all_ok": all(item.ok for item in items),
        "items": [asdict(item) for item in items],
    }
    return summary


def main() -> int:
    summary = build_summary()
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    out_path = PROJECT_ROOT / "data" / "funding_freshness_summary.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0 if summary["all_ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
