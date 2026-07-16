from __future__ import annotations

from typing import Iterable

import pandas as pd


CHANGE_LABELS = {
    "new": "新进",
    "increase": "增持",
    "decrease": "减持",
    "stable": "稳定",
}

SORT_FIELDS = {
    "盘中估算": "intraday_estimate_pct",
    "日涨跌幅": "daily_change_pct",
    "Top10 集中度": "top10_ratio",
    "基金规模": "issue_amount",
    "持仓市值": "holding_market_value",
    "披露日期": "latest_end_date",
}


def _first_nonempty(*values, default="-"):
    for value in values:
        if value is None:
            continue
        if isinstance(value, float) and pd.isna(value):
            continue
        text = str(value).strip()
        if text:
            return value
    return default


def _optional_float(value):
    number = pd.to_numeric(value, errors="coerce")
    return None if pd.isna(number) else float(number)


def _optional_timestamp(value):
    timestamp = pd.to_datetime(value, errors="coerce")
    return pd.NaT if pd.isna(timestamp) else timestamp


def build_fund_watchlist_item(
    watchlist_row: pd.Series,
    meta_df: pd.DataFrame,
    holding_df: pd.DataFrame,
    *,
    nav_snapshot: dict | None = None,
    load_error: str = "",
) -> dict:
    """Normalize one saved fund and its latest holding snapshot for the UI."""
    fund_code = str(watchlist_row.get("ts_code") or "").strip().upper()
    meta_row = None
    if meta_df is not None and not meta_df.empty:
        exact = meta_df[
            meta_df["fund_code"].astype(str).str.strip().str.upper() == fund_code
        ]
        meta_row = exact.iloc[0] if not exact.empty else meta_df.iloc[0]

    holding_first = (
        holding_df.iloc[0]
        if holding_df is not None and not holding_df.empty
        else None
    )
    fund_name = str(
        _first_nonempty(
            watchlist_row.get("security_name"),
            meta_row.get("name") if meta_row is not None else None,
            holding_first.get("fund_name") if holding_first is not None else None,
            fund_code,
        )
    )
    management = str(
        _first_nonempty(
            meta_row.get("management") if meta_row is not None else None,
            holding_first.get("management") if holding_first is not None else None,
        )
    )
    fund_type = str(
        _first_nonempty(
            meta_row.get("fund_type") if meta_row is not None else None,
            holding_first.get("fund_type") if holding_first is not None else None,
            holding_first.get("invest_type") if holding_first is not None else None,
        )
    )
    issue_amount = _optional_float(
        meta_row.get("issue_amount") if meta_row is not None else None
    )
    latest_end_date = _optional_timestamp(
        holding_first.get("end_date")
        if holding_first is not None
        else (meta_row.get("latest_end_date") if meta_row is not None else None)
    )
    added_at = _optional_timestamp(watchlist_row.get("created_at"))
    nav_snapshot = nav_snapshot or {}

    holdings = []
    if holding_df is not None and not holding_df.empty:
        for _, row in holding_df.head(10).iterrows():
            flag = str(row.get("holding_change_flag") or "stable").strip().lower()
            market_value = _optional_float(row.get("mkv"))
            holdings.append(
                {
                    "stock_name": str(row.get("stock_name") or row.get("symbol") or "-"),
                    "symbol": str(row.get("symbol") or "-"),
                    "market_value": market_value,
                    "market_value_yi": (
                        market_value / 1e8 if market_value is not None else None
                    ),
                    "weight": _optional_float(row.get("stk_mkv_ratio")),
                    "change_flag": flag,
                    "change_label": CHANGE_LABELS.get(flag, "稳定"),
                }
            )

    valid_market_values = [
        row["market_value_yi"]
        for row in holdings
        if row["market_value_yi"] is not None
    ]
    valid_weights = [row["weight"] for row in holdings if row["weight"] is not None]
    flags = [row["change_flag"] for row in holdings]

    return {
        "fund_code": fund_code,
        "safe_code": "".join(ch if ch.isalnum() else "_" for ch in fund_code),
        "fund_name": fund_name,
        "fund_type": fund_type,
        "management": management,
        "issue_amount": issue_amount,
        "latest_end_date": latest_end_date,
        "added_at": added_at,
        "nav_date": _optional_timestamp(nav_snapshot.get("nav_date")),
        "unit_nav": _optional_float(nav_snapshot.get("unit_nav")),
        "daily_change_pct": _optional_float(
            nav_snapshot.get("daily_change_pct")
        ),
        "nav_source": str(nav_snapshot.get("source") or ""),
        "holding_count": len(holdings),
        "holding_market_value": (
            round(sum(valid_market_values), 2) if valid_market_values else None
        ),
        "top10_ratio": round(sum(valid_weights), 2) if valid_weights else None,
        "new_count": flags.count("new"),
        "increase_count": flags.count("increase"),
        "decrease_count": flags.count("decrease"),
        "stable_count": flags.count("stable"),
        "holdings": holdings,
        "load_error": str(load_error or ""),
    }


def build_fund_watchlist_summary(items: Iterable[dict]) -> dict:
    items = list(items)
    dates = [
        item["latest_end_date"]
        for item in items
        if not pd.isna(item.get("latest_end_date"))
    ]
    ratios = [
        float(item["top10_ratio"])
        for item in items
        if item.get("top10_ratio") is not None
    ]
    return {
        "fund_count": len(items),
        "latest_end_date": max(dates) if dates else pd.NaT,
        "average_top10_ratio": (
            round(sum(ratios) / len(ratios), 2) if ratios else None
        ),
        "positive_change_count": sum(
            int(item.get("new_count", 0)) + int(item.get("increase_count", 0))
            for item in items
        ),
        "decrease_count": sum(int(item.get("decrease_count", 0)) for item in items),
    }


def sort_fund_watchlist_items(items: Iterable[dict], sort_label: str) -> list[dict]:
    field = SORT_FIELDS.get(sort_label, "top10_ratio")

    def sort_key(item):
        value = item.get(field)
        if field == "latest_end_date":
            timestamp = pd.to_datetime(value, errors="coerce")
            return pd.Timestamp.min if pd.isna(timestamp) else timestamp
        number = pd.to_numeric(value, errors="coerce")
        return float("-inf") if pd.isna(number) else float(number)

    return sorted(list(items), key=sort_key, reverse=True)


def build_fund_watchlist_table(items: Iterable[dict]) -> pd.DataFrame:
    rows = []
    for item in items:
        rows.append(
            {
                "基金名称": item["fund_name"],
                "基金代码": item["fund_code"],
                "基金类型": item["fund_type"],
                "净值日期": (
                    item["nav_date"].strftime("%Y-%m-%d")
                    if not pd.isna(item.get("nav_date"))
                    else "-"
                ),
                "前一日净值": item.get("unit_nav"),
                "日涨跌幅(%)": item.get("daily_change_pct"),
                "盘中估算(%)": item.get("intraday_estimate_pct"),
                "实时覆盖权重(%)": item.get("intraday_covered_weight_pct"),
                "实时行情": (
                    f'{int(item.get("intraday_quote_count", 0))}/{int(item.get("intraday_holding_count", item.get("holding_count", 0)))}'
                ),
                "基金规模(亿份)": item["issue_amount"],
                "持仓市值(亿元)": item["holding_market_value"],
                "Top10 集中度(%)": item["top10_ratio"],
                "新进": item["new_count"],
                "增持": item["increase_count"],
                "减持": item["decrease_count"],
                "最新披露": (
                    item["latest_end_date"].strftime("%Y-%m-%d")
                    if not pd.isna(item["latest_end_date"])
                    else "-"
                ),
                "加入日期": (
                    item["added_at"].strftime("%Y-%m-%d")
                    if not pd.isna(item["added_at"])
                    else "-"
                ),
            }
        )
    return pd.DataFrame(rows)
