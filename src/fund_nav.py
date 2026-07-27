from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests


NAV_DATE_COLUMNS = ("净值日期", "nav_date", "date")
UNIT_NAV_COLUMNS = ("单位净值", "unit_nav", "nav")
DAILY_CHANGE_COLUMNS = ("日增长率", "daily_change_pct", "pct_change")
EASTMONEY_NAV_URL = "https://api.fund.eastmoney.com/f10/lsjz"
EASTMONEY_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    ),
    "Referer": "https://fundf10.eastmoney.com/",
}


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


def fetch_fund_nav_history_eastmoney(
    fund_code: str,
    *,
    start_date: date,
    end_date: date,
    session: requests.sessions.Session | None = None,
    timeout: int = 15,
    empty_retry_attempts: int = 2,
) -> pd.DataFrame:
    fund = normalize_fund_code_for_nav(fund_code)
    http = session or requests
    params = {
        "fundCode": fund,
        "pageIndex": "1",
        "pageSize": "240",
        "startDate": start_date.strftime("%Y-%m-%d"),
        "endDate": end_date.strftime("%Y-%m-%d"),
    }

    payload = None
    rows: list[dict] = []
    total_count = 0
    for _ in range(max(1, int(empty_retry_attempts) + 1)):
        first = http.get(EASTMONEY_NAV_URL, params=params, headers=EASTMONEY_HEADERS, timeout=timeout)
        first.raise_for_status()
        payload = first.json()
        total_count = int((payload.get("TotalCount") or 0)) if isinstance(payload, dict) else 0
        data = (((payload or {}).get("Data") or {}).get("LSJZList") or []) if isinstance(payload, dict) else []
        rows = [item for item in data if isinstance(item, dict)] if isinstance(data, list) else []
        if rows or total_count == 0:
            break

    page_size = int(params["pageSize"])
    total_pages = max(1, (total_count + page_size - 1) // page_size) if total_count else 1

    for page in range(2, total_pages + 1):
        params["pageIndex"] = str(page)
        resp = http.get(EASTMONEY_NAV_URL, params=params, headers=EASTMONEY_HEADERS, timeout=timeout)
        resp.raise_for_status()
        page_payload = resp.json()
        page_rows = (((page_payload or {}).get("Data") or {}).get("LSJZList") or [])
        if isinstance(page_rows, list):
            rows.extend(item for item in page_rows if isinstance(item, dict))

    if not rows:
        return pd.DataFrame(columns=["净值日期", "单位净值", "累计净值", "日增长率", "申购状态", "赎回状态"])

    df = pd.DataFrame(rows)
    rename_map = {
        "FSRQ": "净值日期",
        "净值日期": "净值日期",
        "DWJZ": "单位净值",
        "单位净值": "单位净值",
        "LJJZ": "累计净值",
        "累计净值": "累计净值",
        "JZZZL": "日增长率",
        "日增长率": "日增长率",
        "SGZT": "申购状态",
        "申购状态": "申购状态",
        "SHZT": "赎回状态",
        "赎回状态": "赎回状态",
    }
    df = df.rename(columns=rename_map)
    for col in ["净值日期", "单位净值", "累计净值", "日增长率", "申购状态", "赎回状态"]:
        if col not in df.columns:
            df[col] = None
    return df[["净值日期", "单位净值", "累计净值", "日增长率", "申购状态", "赎回状态"]].copy()


def fetch_fund_nav_history_akshare(
    fund_code: str,
    *,
    start_date: date,
    end_date: date,
    ak_client=None,
) -> pd.DataFrame:
    if ak_client is None:
        import akshare as ak_client

    symbol = normalize_fund_code_for_nav(fund_code)
    try:
        df = ak_client.fund_open_fund_info_em(
            symbol=symbol,
            indicator="单位净值走势",
            period="成立来",
        )
        if df is not None and not df.empty:
            normalized = df.copy()
            if "净值日期" in normalized.columns:
                normalized["净值日期"] = pd.to_datetime(normalized["净值日期"], errors="coerce")
            if "单位净值" in normalized.columns:
                normalized["单位净值"] = pd.to_numeric(normalized["单位净值"], errors="coerce")
            if "日增长率" in normalized.columns:
                normalized["日增长率"] = pd.to_numeric(normalized["日增长率"], errors="coerce")
            mask = (
                normalized["净值日期"].notna()
                if "净值日期" in normalized.columns
                else pd.Series([True] * len(normalized))
            )
            if "净值日期" in normalized.columns:
                mask &= normalized["净值日期"].dt.date.between(start_date, end_date)
            normalized = normalized.loc[mask].copy()
            if not normalized.empty:
                return normalized
    except Exception:
        pass

    return ak_client.fund_etf_fund_info_em(
        fund=symbol,
        start_date=start_date.strftime("%Y%m%d"),
        end_date=end_date.strftime("%Y%m%d"),
    )


def fetch_latest_fund_nav_snapshot(
    fund_code: str,
    *,
    as_of_date: date | None = None,
    lookback_days: int = 45,
    ak_client=None,
    session: requests.sessions.Session | None = None,
) -> dict:
    """Fetch the latest confirmed NAV before the current Shanghai calendar day.

    Comparison uses the previous available disclosed NAV from the upstream
    series, so any non-trading / non-disclosure day is skipped automatically.
    """
    reference_date = as_of_date or datetime.now(ZoneInfo("Asia/Shanghai")).date()
    end_date = reference_date - timedelta(days=1)
    start_date = end_date - timedelta(days=max(7, int(lookback_days)))

    last_error: Exception | None = None
    nav_df: pd.DataFrame | None = None

    try:
        nav_df = fetch_fund_nav_history_eastmoney(
            fund_code,
            start_date=start_date,
            end_date=end_date,
            session=session,
        )
    except Exception as exc:
        last_error = exc

    if nav_df is None or nav_df.empty:
        try:
            nav_df = fetch_fund_nav_history_akshare(
                fund_code,
                start_date=start_date,
                end_date=end_date,
                ak_client=ak_client,
            )
        except Exception as exc:
            last_error = exc

    snapshot = build_latest_fund_nav_snapshot(nav_df)
    if snapshot["unit_nav"] is None:
        if last_error is not None:
            raise LookupError(f"最近未查询到已公布的基金净值: {last_error}") from last_error
        raise LookupError("最近未查询到已公布的基金净值")
    return snapshot
