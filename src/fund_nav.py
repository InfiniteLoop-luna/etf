from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd


NAV_DATE_COLUMNS = ("净值日期", "nav_date", "date")
UNIT_NAV_COLUMNS = ("单位净值", "unit_nav", "nav")
DAILY_CHANGE_COLUMNS = ("日增长率", "daily_change_pct", "pct_change")


def _first_existing_column(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    return next((column for column in candidates if column in df.columns), None)


def _optional_float(value: Any) -> float | None:
    number = pd.to_numeric(value, errors="coerce")
    return None if pd.isna(number) else float(number)


def build_latest_fund_nav_snapshot(nav_df: pd.DataFrame | None) -> dict:
    """Return the latest confirmed unit NAV and its one-day change."""
    empty_snapshot = {
        "nav_date": pd.NaT,
        "unit_nav": None,
        "daily_change_pct": None,
        "previous_nav_date": pd.NaT,
        "previous_unit_nav": None,
        "source": "东方财富 / AkShare",
    }
    if nav_df is None or nav_df.empty:
        return empty_snapshot

    date_column = _first_existing_column(nav_df, NAV_DATE_COLUMNS)
    unit_nav_column = _first_existing_column(nav_df, UNIT_NAV_COLUMNS)
    change_column = _first_existing_column(nav_df, DAILY_CHANGE_COLUMNS)
    if date_column is None or unit_nav_column is None:
        return empty_snapshot

    normalized = pd.DataFrame(
        {
            "nav_date": pd.to_datetime(nav_df[date_column], errors="coerce"),
            "unit_nav": pd.to_numeric(nav_df[unit_nav_column], errors="coerce"),
        }
    )
    normalized["daily_change_pct"] = (
        pd.to_numeric(nav_df[change_column], errors="coerce")
        if change_column is not None
        else float("nan")
    )
    normalized = (
        normalized.dropna(subset=["nav_date", "unit_nav"])
        .sort_values("nav_date")
        .drop_duplicates(subset=["nav_date"], keep="last")
        .reset_index(drop=True)
    )
    if normalized.empty:
        return empty_snapshot

    latest = normalized.iloc[-1]
    previous = normalized.iloc[-2] if len(normalized) >= 2 else None
    daily_change_pct = _optional_float(latest["daily_change_pct"])
    previous_unit_nav = (
        _optional_float(previous["unit_nav"]) if previous is not None else None
    )
    latest_unit_nav = _optional_float(latest["unit_nav"])
    if (
        daily_change_pct is None
        and latest_unit_nav is not None
        and previous_unit_nav not in (None, 0.0)
    ):
        daily_change_pct = (latest_unit_nav / previous_unit_nav - 1.0) * 100.0

    return {
        "nav_date": latest["nav_date"],
        "unit_nav": latest_unit_nav,
        "daily_change_pct": daily_change_pct,
        "previous_nav_date": (
            previous["nav_date"] if previous is not None else pd.NaT
        ),
        "previous_unit_nav": previous_unit_nav,
        "source": "东方财富 / AkShare",
    }


def normalize_fund_code_for_nav(raw_code: str) -> str:
    code = str(raw_code or "").strip().upper().split(".", 1)[0]
    if len(code) != 6 or not code.isdigit():
        raise ValueError(f"无效基金代码：{raw_code}")
    return code


def fetch_latest_fund_nav_snapshot(
    fund_code: str,
    *,
    as_of_date: date | None = None,
    lookback_days: int = 45,
    ak_client=None,
) -> dict:
    """Fetch the latest NAV strictly before the current Shanghai calendar day."""
    if ak_client is None:
        import akshare as ak_client

    reference_date = as_of_date or datetime.now(ZoneInfo("Asia/Shanghai")).date()
    end_date = reference_date - timedelta(days=1)
    start_date = end_date - timedelta(days=max(7, int(lookback_days)))
    nav_df = ak_client.fund_etf_fund_info_em(
        fund=normalize_fund_code_for_nav(fund_code),
        start_date=start_date.strftime("%Y%m%d"),
        end_date=end_date.strftime("%Y%m%d"),
    )
    snapshot = build_latest_fund_nav_snapshot(nav_df)
    if snapshot["unit_nav"] is None:
        raise LookupError("最近未查询到已公布的基金净值")
    return snapshot
