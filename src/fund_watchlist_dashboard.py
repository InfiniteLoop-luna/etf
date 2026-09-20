from __future__ import annotations

from typing import Iterable

import pandas as pd

from src.stock_subsector_classifier import classify_stock_subsector


CHANGE_LABELS = {
    "new": "新进",
    "increase": "增持",
    "decrease": "减持",
    "stable": "稳定",
}

SORT_FIELDS = {
    "盘中估算": "intraday_estimate_pct",
    "预计增减金额": "estimated_daily_amount",
    "实际持仓金额": "actual_holding_amount",
    "当前持仓收益": "current_holding_profit",
    "日涨跌幅": "daily_change_pct",
    "估值偏差": "estimate_deviation_pct",
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
    estimate_snapshot: dict | None = None,
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
    estimate_snapshot = estimate_snapshot or {}
    nav_date = _optional_timestamp(nav_snapshot.get("nav_date"))
    estimate_date = _optional_timestamp(estimate_snapshot.get("estimate_date"))
    dates_match = (
        not pd.isna(nav_date)
        and not pd.isna(estimate_date)
        and nav_date.date() == estimate_date.date()
    )
    unit_nav = _optional_float(nav_snapshot.get("unit_nav"))
    previous_nav_date = _optional_timestamp(nav_snapshot.get("previous_nav_date"))
    previous_unit_nav = _optional_float(nav_snapshot.get("previous_unit_nav"))
    daily_change_pct = _optional_float(nav_snapshot.get("daily_change_pct"))
    holding_shares = _optional_float(watchlist_row.get("holding_shares"))
    holding_cost_amount = _optional_float(
        watchlist_row.get("holding_cost_amount")
    )
    latest_closing_estimate_pct = _optional_float(
        estimate_snapshot.get("estimate_pct")
    )
    closing_estimate_pct = (
        latest_closing_estimate_pct
        if dates_match
        else None
    )
    estimate_deviation_pct = (
        closing_estimate_pct - daily_change_pct
        if closing_estimate_pct is not None and daily_change_pct is not None
        else None
    )

    holdings = []
    if holding_df is not None and not holding_df.empty:
        for _, row in holding_df.head(10).iterrows():
            flag = str(row.get("holding_change_flag") or "stable").strip().lower()
            market_value = _optional_float(row.get("mkv"))
            subsector = classify_stock_subsector(
                industry=row.get("stock_industry"),
                main_business=row.get("stock_main_business"),
                product=row.get("stock_product"),
                introduction=row.get("stock_introduction"),
            )
            holdings.append(
                {
                    "stock_name": str(row.get("stock_name") or row.get("symbol") or "-"),
                    "symbol": str(row.get("symbol") or "-"),
                    "industry": str(
                        _first_nonempty(row.get("stock_industry"), default="未识别")
                    ),
                    "market": str(
                        _first_nonempty(row.get("stock_market"), default="-")
                    ),
                    "subsector": str(subsector["subsector"]),
                    "subsector_tags": list(subsector["subsector_tags"]),
                    "subsector_tag_text": str(subsector["subsector_tag_text"]),
                    "subsector_evidence": str(subsector["subsector_evidence"]),
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
        "nav_date": nav_date,
        "unit_nav": unit_nav,
        "previous_nav_date": previous_nav_date,
        "previous_unit_nav": previous_unit_nav,
        "daily_change_pct": daily_change_pct,
        "holding_shares": holding_shares,
        "holding_cost_amount": holding_cost_amount,
        "nav_source": str(nav_snapshot.get("source") or ""),
        "closing_estimate_date": estimate_date if dates_match else pd.NaT,
        "closing_estimate_pct": closing_estimate_pct,
        "latest_closing_estimate_date": estimate_date,
        "latest_closing_estimate_pct": latest_closing_estimate_pct,
        "estimate_deviation_pct": estimate_deviation_pct,
        "closing_estimate_covered_weight_pct": (
            _optional_float(estimate_snapshot.get("covered_weight_pct"))
            if dates_match
            else None
        ),
        "closing_estimate_quote_time": (
            _optional_timestamp(estimate_snapshot.get("quote_time"))
            if dates_match
            else pd.NaT
        ),
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


def build_fund_holding_industry_heatmap_frame(holdings: Iterable[dict]) -> pd.DataFrame:
    """Build a clean industry -> stock hierarchy weighted by disclosed holding ratio."""
    rows = []
    for holding in holdings or []:
        weight = _optional_float(holding.get("weight"))
        if weight is None or weight <= 0:
            continue
        industry = str(
            _first_nonempty(holding.get("industry"), default="未识别行业")
        ).strip()
        stock_name = str(
            _first_nonempty(
                holding.get("stock_name"),
                holding.get("symbol"),
                default="未知股票",
            )
        ).strip()
        symbol = str(_first_nonempty(holding.get("symbol"), default="-")).strip()
        rows.append(
            {
                "所属行业": industry,
                "股票名称": stock_name,
                "股票代码": symbol,
                "细分板块": str(
                    _first_nonempty(holding.get("subsector"), default=f"其他·{industry}")
                ),
                "板块标签": str(
                    _first_nonempty(holding.get("subsector_tag_text"), default="-")
                ),
                "分类依据": str(
                    _first_nonempty(holding.get("subsector_evidence"), default="-")
                ),
                "持仓股票": f"{stock_name}（{symbol}）" if symbol != "-" else stock_name,
                "持仓权重(%)": weight,
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=[
                "所属行业", "细分板块", "股票名称", "股票代码", "板块标签", "分类依据",
                "持仓股票", "持仓权重(%)",
            ]
        )
    return (
        pd.DataFrame(rows)
        .sort_values(["所属行业", "细分板块", "持仓权重(%)"], ascending=[True, True, False])
        .reset_index(drop=True)
    )


def build_fund_watchlist_summary(
    items: Iterable[dict],
    *,
    target_date=None,
) -> dict:
    items = list(items)
    target_timestamp = _optional_timestamp(target_date)
    target_day = (
        target_timestamp.normalize() if not pd.isna(target_timestamp) else pd.NaT
    )
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
    estimated_rows = []
    actual_holding_rows = []
    current_holding_profit_rows = []
    for item in items:
        amount = _optional_float(item.get("estimated_daily_amount"))
        estimate_date = _optional_timestamp(item.get("estimated_daily_amount_date"))
        estimate_day = estimate_date.normalize() if not pd.isna(estimate_date) else pd.NaT
        if (
            amount is not None
            and not pd.isna(estimate_day)
            and (pd.isna(target_day) or estimate_day == target_day)
        ):
            estimated_rows.append((estimate_day, amount))
        actual_holding_amount = _optional_float(item.get("actual_holding_amount"))
        actual_holding_date = _optional_timestamp(
            item.get("actual_holding_amount_date")
        )
        if actual_holding_amount is not None:
            actual_holding_rows.append(
                (actual_holding_date, actual_holding_amount)
            )
        current_holding_profit = _optional_float(item.get("current_holding_profit"))
        holding_cost_amount = _optional_float(item.get("holding_cost_amount"))
        if current_holding_profit is not None and holding_cost_amount is not None:
            current_holding_profit_rows.append(
                (current_holding_profit, holding_cost_amount)
            )
    latest_amount_date = (
        max(row[0] for row in estimated_rows) if estimated_rows else pd.NaT
    )
    latest_estimated_amounts = [
        amount for estimate_date, amount in estimated_rows if estimate_date == latest_amount_date
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
        "estimated_daily_amount": (
            sum(latest_estimated_amounts) if latest_estimated_amounts else None
        ),
        "estimated_daily_amount_date": latest_amount_date,
        "estimated_daily_amount_count": len(latest_estimated_amounts),
        "position_count": sum(
            1
            for item in items
            if (_optional_float(item.get("holding_shares")) or 0.0) > 0
        ),
        "actual_holding_amount": (
            sum(row[1] for row in actual_holding_rows)
            if actual_holding_rows
            else None
        ),
        "actual_holding_amount_count": len(actual_holding_rows),
        "actual_holding_amount_min_date": (
            min(row[0] for row in actual_holding_rows if not pd.isna(row[0]))
            if any(not pd.isna(row[0]) for row in actual_holding_rows)
            else pd.NaT
        ),
        "actual_holding_amount_max_date": (
            max(row[0] for row in actual_holding_rows if not pd.isna(row[0]))
            if any(not pd.isna(row[0]) for row in actual_holding_rows)
            else pd.NaT
        ),
        "current_holding_profit": (
            sum(row[0] for row in current_holding_profit_rows)
            if current_holding_profit_rows
            else None
        ),
        "current_holding_profit_count": len(current_holding_profit_rows),
        "current_holding_profit_pct": (
            sum(row[0] for row in current_holding_profit_rows)
            / sum(row[1] for row in current_holding_profit_rows)
            * 100.0
            if current_holding_profit_rows
            and sum(row[1] for row in current_holding_profit_rows) > 0
            else None
        ),
    }


def attach_latest_closing_estimate(item: dict, snapshot: dict | None) -> dict:
    enriched = dict(item)
    snapshot = snapshot or {}
    enriched.update(
        {
            "latest_closing_estimate_date": _optional_timestamp(
                snapshot.get("estimate_date")
            ),
            "latest_closing_estimate_pct": _optional_float(
                snapshot.get("estimate_pct")
            ),
            "latest_closing_estimate_quote_time": _optional_timestamp(
                snapshot.get("quote_time")
            ),
            "latest_closing_estimate_covered_weight_pct": _optional_float(
                snapshot.get("covered_weight_pct")
            ),
        }
    )
    return enriched


def calculate_estimated_daily_amount(
    holding_shares,
    base_unit_nav,
    estimate_pct,
) -> float | None:
    """Return an estimated one-day position change in yuan.

    ``estimate_pct`` is expressed as a percentage (for example ``0.62`` for
    +0.62%).  Missing or non-positive position inputs deliberately return
    ``None`` so the UI can distinguish "not configured" from a real zero
    estimate.
    """
    shares = _optional_float(holding_shares)
    nav = _optional_float(base_unit_nav)
    pct = _optional_float(estimate_pct)
    if shares is None or shares <= 0 or nav is None or nav <= 0 or pct is None:
        return None
    return shares * nav * pct / 100.0


def attach_estimated_daily_amount(
    item: dict,
    *,
    intraday_date=None,
) -> dict:
    """Attach date-aligned personal-position estimate fields to a fund item.

    Intraday estimates take priority.  When a target date is supplied, a saved
    15:00 estimate is accepted only for that same date so an older snapshot is
    never presented as today's value.  If the estimate date equals the latest
    confirmed NAV date, the previous NAV is the correct percentage base.
    """
    enriched = dict(item)
    intraday_pct = _optional_float(item.get("intraday_estimate_pct"))
    closing_pct = _optional_float(item.get("latest_closing_estimate_pct"))
    target_date = _optional_timestamp(intraday_date)

    estimate_pct = None
    estimate_date = pd.NaT
    estimate_source = ""
    if intraday_pct is not None:
        estimate_pct = intraday_pct
        estimate_date = target_date
        if pd.isna(estimate_date):
            estimate_date = _optional_timestamp(item.get("intraday_updated_at"))
        estimate_source = "盘中估算"
    elif closing_pct is not None:
        closing_date = _optional_timestamp(item.get("latest_closing_estimate_date"))
        if (
            pd.isna(target_date)
            or (
                not pd.isna(closing_date)
                and closing_date.date() == target_date.date()
            )
        ):
            estimate_pct = closing_pct
            estimate_date = closing_date
            estimate_source = "15:00估值"

    nav_date = _optional_timestamp(item.get("nav_date"))
    previous_nav_date = _optional_timestamp(item.get("previous_nav_date"))
    base_nav = None
    base_nav_date = pd.NaT
    if not pd.isna(estimate_date) and not pd.isna(nav_date):
        estimate_day = estimate_date.date()
        nav_day = nav_date.date()
        if estimate_day > nav_day:
            base_nav = _optional_float(item.get("unit_nav"))
            base_nav_date = nav_date
        elif estimate_day == nav_day:
            base_nav = _optional_float(item.get("previous_unit_nav"))
            base_nav_date = previous_nav_date

    amount = calculate_estimated_daily_amount(
        item.get("holding_shares"),
        base_nav,
        estimate_pct,
    )
    estimated_unit_nav = (
        base_nav * (1.0 + estimate_pct / 100.0)
        if amount is not None and estimate_pct is not None and base_nav is not None
        else None
    )
    enriched.update(
        {
            "estimated_daily_amount": amount,
            "estimated_daily_amount_pct": estimate_pct if amount is not None else None,
            "estimated_daily_amount_date": estimate_date if amount is not None else pd.NaT,
            "estimated_daily_amount_source": estimate_source if amount is not None else "",
            "estimated_daily_base_nav": base_nav if amount is not None else None,
            "estimated_daily_base_nav_date": base_nav_date if amount is not None else pd.NaT,
            "estimated_unit_nav": estimated_unit_nav,
        }
    )
    return enriched


def attach_current_position_metrics(item: dict) -> dict:
    """Attach current position value and cumulative profit fields.

    The word "actual" is intentionally reserved for the latest NAV published
    by the fund company.  Intraday and 15:00 estimates remain separate in the
    daily-estimate fields and are never folded into cumulative profit.
    Cumulative profit requires an explicit total position cost; screenshots
    may infer that cost, but snapshot amounts themselves are never persisted as
    live values.
    """
    enriched = dict(item)
    shares = _optional_float(item.get("holding_shares"))
    cost_amount = _optional_float(item.get("holding_cost_amount"))
    confirmed_nav = _optional_float(item.get("unit_nav"))
    confirmed_date = _optional_timestamp(item.get("nav_date"))
    effective_nav = confirmed_nav if confirmed_nav is not None and confirmed_nav > 0 else None
    effective_date = confirmed_date if effective_nav is not None else pd.NaT
    effective_source = "最新确认净值" if effective_nav is not None else ""

    actual_holding_amount = (
        shares * effective_nav
        if shares is not None
        and shares > 0
        and effective_nav is not None
        and effective_nav > 0
        else None
    )
    current_holding_profit = (
        actual_holding_amount - cost_amount
        if actual_holding_amount is not None
        and cost_amount is not None
        and cost_amount > 0
        else None
    )
    current_holding_profit_pct = (
        current_holding_profit / cost_amount * 100.0
        if current_holding_profit is not None
        and cost_amount is not None
        and cost_amount > 0
        else None
    )
    enriched.update(
        {
            "actual_holding_amount": actual_holding_amount,
            "actual_holding_amount_nav": effective_nav,
            "actual_holding_amount_date": (
                effective_date if actual_holding_amount is not None else pd.NaT
            ),
            "actual_holding_amount_source": (
                effective_source if actual_holding_amount is not None else ""
            ),
            "current_holding_profit": current_holding_profit,
            "current_holding_profit_pct": current_holding_profit_pct,
        }
    )
    return enriched


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
                "持有份额": item.get("holding_shares"),
                "持仓成本金额(元)": item.get("holding_cost_amount"),
                "实际持仓金额(元)": item.get("actual_holding_amount"),
                "当前持仓收益(元)": item.get("current_holding_profit"),
                "当前持仓收益率(%)": item.get("current_holding_profit_pct"),
                "持仓金额日期": (
                    item["actual_holding_amount_date"].strftime("%Y-%m-%d")
                    if not pd.isna(item.get("actual_holding_amount_date"))
                    else "-"
                ),
                "持仓金额口径": item.get("actual_holding_amount_source") or "-",
                "预计增减金额(元)": item.get("estimated_daily_amount"),
                "金额估值日期": (
                    item["estimated_daily_amount_date"].strftime("%Y-%m-%d")
                    if not pd.isna(item.get("estimated_daily_amount_date"))
                    else "-"
                ),
                "金额估值口径": item.get("estimated_daily_amount_source") or "-",
                "日涨跌幅(%)": item.get("daily_change_pct"),
                "15:00估值(%)": item.get("closing_estimate_pct"),
                "估值偏差(百分点)": item.get("estimate_deviation_pct"),
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
