# -*- coding: utf-8 -*-
"""Date-window helpers for the hot-money Streamlit page."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any


DAILY_QUERY_LABEL = "按日查询"
RECENT_1D_LABEL = "最近1日"
RECENT_5D_LABEL = "最近5日"
RECENT_20D_LABEL = "最近20日"
ALL_INGESTED_LABEL = "全部已入库"
HOTMONEY_WINDOW_OPTIONS = [DAILY_QUERY_LABEL, RECENT_1D_LABEL, RECENT_5D_LABEL, RECENT_20D_LABEL, ALL_INGESTED_LABEL]
HOTMONEY_HISTORY_START = date(2024, 1, 1)


@dataclass(frozen=True)
class HotmoneyDateWindow:
    start_date: date
    end_date: date
    label: str


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        text = str(value).strip()
        if not text:
            return None
        return datetime.strptime(text.replace("-", ""), "%Y%m%d").date()
    except (TypeError, ValueError):
        return None


def _clamp_day(day: date, *, earliest: date, latest: date) -> date:
    if day < earliest:
        return earliest
    if day > latest:
        return latest
    return day


def resolve_hotmoney_detail_date_window(
    *,
    latest_date: str | date,
    detail_window: str,
    selected_date: str | date | None = None,
) -> HotmoneyDateWindow:
    latest_dt = _parse_date(latest_date) or date.today()
    window = detail_window or DAILY_QUERY_LABEL

    if window == DAILY_QUERY_LABEL:
        selected_dt = _parse_date(selected_date) or latest_dt
        selected_dt = _clamp_day(selected_dt, earliest=HOTMONEY_HISTORY_START, latest=latest_dt)
        return HotmoneyDateWindow(
            start_date=selected_dt,
            end_date=selected_dt,
            label=selected_dt.strftime("%Y-%m-%d"),
        )

    if window == RECENT_1D_LABEL:
        start_dt = latest_dt
        label = "最近1日"
    elif window == RECENT_5D_LABEL:
        start_dt = latest_dt - timedelta(days=7)
        label = "最近5日"
    elif window == RECENT_20D_LABEL:
        start_dt = latest_dt - timedelta(days=30)
        label = "最近20日"
    else:
        start_dt = HOTMONEY_HISTORY_START
        label = "全部已入库"

    return HotmoneyDateWindow(
        start_date=max(start_dt, HOTMONEY_HISTORY_START),
        end_date=latest_dt,
        label=label,
    )
