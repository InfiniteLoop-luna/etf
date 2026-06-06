# -*- coding: utf-8 -*-
"""龙虎榜数据拉取与聚合工具。"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

TOP_LIST_FIELDS = (
    "trade_date,ts_code,name,close,pct_change,turnover_rate,amount,"
    "l_sell,l_buy,l_amount,net_amount,net_rate,amount_rate,float_values,reason"
)
TOP_INST_FIELDS = (
    "trade_date,ts_code,exalter,side,buy,buy_rate,sell,sell_rate,net_buy,reason"
)

TOP_LIST_COLUMNS = [
    "trade_date",
    "ts_code",
    "name",
    "close",
    "pct_change",
    "turnover_rate",
    "amount",
    "l_sell",
    "l_buy",
    "l_amount",
    "net_amount",
    "net_rate",
    "amount_rate",
    "float_values",
    "reason",
]
TOP_INST_COLUMNS = [
    "trade_date",
    "ts_code",
    "exalter",
    "side",
    "buy",
    "buy_rate",
    "sell",
    "sell_rate",
    "net_buy",
    "reason",
]

LHB_STOCK_SUMMARY_COLUMNS = [
    "ts_code",
    "name",
    "hit_count",
    "trade_days",
    "latest_date",
    "latest_date_label",
    "total_buy_yi",
    "total_sell_yi",
    "lhb_amount_yi",
    "net_amount_yi",
    "inst_hit_count",
    "inst_org_count",
    "inst_buy_yi",
    "inst_sell_yi",
    "inst_net_yi",
    "combined_net_yi",
    "avg_pct_change",
    "max_turnover_rate",
    "reasons",
    "net_direction",
    "stock_label",
]


def _parse_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        raise ValueError("date value is required")

    text = str(value).strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"Unsupported date format: {value!r}")
    return datetime.strptime(text, "%Y%m%d").date()


def _format_yyyymmdd(value: date) -> str:
    return value.strftime("%Y%m%d")


def resolve_lhb_date_window(
    start_date: Optional[str | date | datetime] = None,
    end_date: Optional[str | date | datetime] = None,
    today: Optional[date | datetime] = None,
) -> tuple[str, str]:
    """Resolve a Tushare date window and clamp it to the current year-to-date."""
    today_value = _parse_date(today) if today is not None else date.today()
    year_start = date(today_value.year, 1, 1)

    end_value = _parse_date(end_date) if end_date is not None else today_value
    end_value = max(min(end_value, today_value), year_start)

    start_value = _parse_date(start_date) if start_date is not None else year_start
    start_value = max(start_value, year_start)
    if start_value > end_value:
        start_value = end_value

    return _format_yyyymmdd(start_value), _format_yyyymmdd(end_value)


def _ensure_columns(df: Optional[pd.DataFrame], columns: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)

    work = df.copy()
    for column in columns:
        if column not in work.columns:
            work[column] = None
    return work[columns].copy()


def _normalize_trade_date_column(work: pd.DataFrame) -> pd.DataFrame:
    if "trade_date" not in work.columns:
        work["trade_date"] = pd.NaT
        return work

    normalized = work["trade_date"].astype(str).str.replace("-", "", regex=False).str[:8]
    work["trade_date"] = pd.to_datetime(normalized, format="%Y%m%d", errors="coerce")
    return work


def _to_numeric(work: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column in work.columns:
            work[column] = pd.to_numeric(work[column], errors="coerce").fillna(0.0)
    return work


def _first_non_empty(values) -> str:
    for value in values:
        text = str(value or "").strip()
        if text and text.lower() != "nan":
            return text
    return "-"


def _compact_reasons(values, max_items: int = 3) -> str:
    reasons: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text.lower() == "nan":
            continue
        if text not in reasons:
            reasons.append(text)

    if not reasons:
        return "-"
    label = "；".join(reasons[:max_items])
    if len(reasons) > max_items:
        label = f"{label} 等{len(reasons)}类"
    return label


def prepare_lhb_top_list_frame(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    work = _ensure_columns(df, TOP_LIST_COLUMNS)
    if work.empty:
        return work

    work = _normalize_trade_date_column(work)
    text_columns = ["ts_code", "name", "reason"]
    for column in text_columns:
        work[column] = work[column].fillna("").astype(str)

    numeric_columns = [
        "close",
        "pct_change",
        "turnover_rate",
        "amount",
        "l_sell",
        "l_buy",
        "l_amount",
        "net_amount",
        "net_rate",
        "amount_rate",
        "float_values",
    ]
    work = _to_numeric(work, numeric_columns)
    for column in ["amount", "l_sell", "l_buy", "l_amount", "net_amount", "float_values"]:
        work[f"{column}_yi"] = work[column] / 1e8

    return work.sort_values(["trade_date", "ts_code"], ascending=[False, True]).reset_index(drop=True)


def prepare_lhb_inst_frame(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    work = _ensure_columns(df, TOP_INST_COLUMNS)
    if work.empty:
        return work

    work = _normalize_trade_date_column(work)
    for column in ["ts_code", "exalter", "side", "reason"]:
        work[column] = work[column].fillna("").astype(str)

    work = _to_numeric(work, ["buy", "buy_rate", "sell", "sell_rate", "net_buy"])
    for column in ["buy", "sell", "net_buy"]:
        work[f"{column}_yi"] = work[column] / 1e8
    work["side_label"] = work["side"].map({"0": "买入前五", "1": "卖出前五"}).fillna("未标明")

    return work.sort_values(["trade_date", "ts_code"], ascending=[False, True]).reset_index(drop=True)


def _empty_stock_summary() -> pd.DataFrame:
    return pd.DataFrame(columns=LHB_STOCK_SUMMARY_COLUMNS)


def build_lhb_stock_summary(
    top_list_df: Optional[pd.DataFrame],
    inst_df: Optional[pd.DataFrame] = None,
    order_by: str = "hit_count",
) -> pd.DataFrame:
    top_list = prepare_lhb_top_list_frame(top_list_df)
    if top_list.empty:
        return _empty_stock_summary()

    summary = (
        top_list.groupby("ts_code", dropna=False)
        .agg(
            name=("name", _first_non_empty),
            hit_count=("ts_code", "size"),
            trade_days=("trade_date", "nunique"),
            latest_date=("trade_date", "max"),
            total_buy_yi=("l_buy_yi", "sum"),
            total_sell_yi=("l_sell_yi", "sum"),
            lhb_amount_yi=("l_amount_yi", "sum"),
            net_amount_yi=("net_amount_yi", "sum"),
            avg_pct_change=("pct_change", "mean"),
            max_turnover_rate=("turnover_rate", "max"),
            reasons=("reason", _compact_reasons),
        )
        .reset_index()
    )

    inst = prepare_lhb_inst_frame(inst_df)
    if not inst.empty:
        inst_summary = (
            inst.groupby("ts_code", dropna=False)
            .agg(
                inst_hit_count=("ts_code", "size"),
                inst_org_count=("exalter", lambda values: len({str(v).strip() for v in values if str(v).strip()})),
                inst_buy_yi=("buy_yi", "sum"),
                inst_sell_yi=("sell_yi", "sum"),
                inst_net_yi=("net_buy_yi", "sum"),
            )
            .reset_index()
        )
        summary = summary.merge(inst_summary, on="ts_code", how="left")
    else:
        for column in ["inst_hit_count", "inst_org_count", "inst_buy_yi", "inst_sell_yi", "inst_net_yi"]:
            summary[column] = 0

    for column in ["inst_hit_count", "inst_org_count", "inst_buy_yi", "inst_sell_yi", "inst_net_yi"]:
        summary[column] = pd.to_numeric(summary[column], errors="coerce").fillna(0)

    summary["combined_net_yi"] = summary["net_amount_yi"] + summary["inst_net_yi"]
    summary["latest_date_label"] = pd.to_datetime(summary["latest_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    summary["net_direction"] = summary["combined_net_yi"].map(
        lambda value: "净买入" if value > 0 else ("净卖出" if value < 0 else "均衡")
    )
    summary["stock_label"] = summary["name"].fillna("-").astype(str) + "（" + summary["ts_code"].fillna("").astype(str) + "）"

    order_by = (order_by or "hit_count").lower()
    if order_by == "combined_net":
        sort_columns = ["combined_net_yi", "hit_count", "lhb_amount_yi"]
    elif order_by == "inst_net":
        sort_columns = ["inst_net_yi", "inst_hit_count", "hit_count"]
    elif order_by == "lhb_amount":
        sort_columns = ["lhb_amount_yi", "hit_count", "combined_net_yi"]
    else:
        sort_columns = ["hit_count", "trade_days", "lhb_amount_yi"]

    summary = summary.sort_values(sort_columns, ascending=False)
    return summary[LHB_STOCK_SUMMARY_COLUMNS].reset_index(drop=True)


def build_lhb_daily_overview(
    top_list_df: Optional[pd.DataFrame],
    inst_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    top_list = prepare_lhb_top_list_frame(top_list_df)
    if top_list.empty:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "trade_date_label",
                "stock_count",
                "record_count",
                "total_buy_yi",
                "total_sell_yi",
                "net_amount_yi",
                "inst_net_yi",
                "inst_hit_count",
            ]
        )

    daily = (
        top_list.groupby("trade_date", dropna=False)
        .agg(
            stock_count=("ts_code", "nunique"),
            record_count=("ts_code", "size"),
            total_buy_yi=("l_buy_yi", "sum"),
            total_sell_yi=("l_sell_yi", "sum"),
            net_amount_yi=("net_amount_yi", "sum"),
        )
        .reset_index()
    )

    inst = prepare_lhb_inst_frame(inst_df)
    if not inst.empty:
        inst_daily = (
            inst.groupby("trade_date", dropna=False)
            .agg(
                inst_hit_count=("ts_code", "size"),
                inst_net_yi=("net_buy_yi", "sum"),
            )
            .reset_index()
        )
        daily = daily.merge(inst_daily, on="trade_date", how="left")
    else:
        daily["inst_hit_count"] = 0
        daily["inst_net_yi"] = 0.0

    daily["inst_hit_count"] = pd.to_numeric(daily["inst_hit_count"], errors="coerce").fillna(0).astype(int)
    daily["inst_net_yi"] = pd.to_numeric(daily["inst_net_yi"], errors="coerce").fillna(0.0)
    daily["trade_date_label"] = pd.to_datetime(daily["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return daily.sort_values("trade_date").reset_index(drop=True)


def build_lhb_reason_summary(top_list_df: Optional[pd.DataFrame], top_n: int = 12) -> pd.DataFrame:
    top_list = prepare_lhb_top_list_frame(top_list_df)
    if top_list.empty:
        return pd.DataFrame(columns=["reason", "hit_count", "stock_count", "net_amount_yi", "lhb_amount_yi"])

    reason = (
        top_list.groupby("reason", dropna=False)
        .agg(
            hit_count=("ts_code", "size"),
            stock_count=("ts_code", "nunique"),
            net_amount_yi=("net_amount_yi", "sum"),
            lhb_amount_yi=("l_amount_yi", "sum"),
        )
        .reset_index()
    )
    reason["reason"] = reason["reason"].replace("", "未标明")
    return reason.sort_values(["hit_count", "lhb_amount_yi"], ascending=False).head(int(top_n)).reset_index(drop=True)


def load_lhb_trade_dates(pro, start_date: str, end_date: str) -> list[str]:
    try:
        cal_df = pro.trade_cal(exchange="", start_date=start_date, end_date=end_date, is_open="1")
    except Exception as exc:
        logger.warning("trade_cal 拉取失败，回退到工作日序列: %s", exc)
        return [d.strftime("%Y%m%d") for d in pd.bdate_range(start=start_date, end=end_date)]

    if cal_df is None or cal_df.empty:
        return []

    work = cal_df.copy()
    if "is_open" in work.columns:
        work = work[pd.to_numeric(work["is_open"], errors="coerce").fillna(0).astype(int) == 1]

    date_col = "cal_date" if "cal_date" in work.columns else "trade_date"
    if date_col not in work.columns:
        return []

    return sorted(
        {
            str(value).replace("-", "")[:8]
            for value in work[date_col].tolist()
            if str(value).replace("-", "")[:8].isdigit()
        }
    )


def _query_tushare_frame(pro, api_name: str, trade_date: str, ts_code: Optional[str], fields: str) -> pd.DataFrame:
    kwargs = {"trade_date": trade_date, "fields": fields}
    if ts_code:
        kwargs["ts_code"] = ts_code
    api = getattr(pro, api_name)
    df = api(**kwargs)
    if df is None:
        return pd.DataFrame()
    return df


def fetch_lhb_data(
    pro,
    start_date: Optional[str | date | datetime] = None,
    end_date: Optional[str | date | datetime] = None,
    ts_code: Optional[str] = None,
    include_inst: bool = True,
    request_sleep_seconds: float = 0.25,
    max_trade_days: Optional[int] = None,
    today: Optional[date | datetime] = None,
) -> dict:
    """Fetch top_list/top_inst rows for the current year-to-date window."""
    resolved_start, resolved_end = resolve_lhb_date_window(start_date, end_date, today=today)
    trade_dates = load_lhb_trade_dates(pro, resolved_start, resolved_end)
    if max_trade_days is not None and int(max_trade_days) > 0:
        trade_dates = trade_dates[-int(max_trade_days):]

    ts_code_text = str(ts_code or "").strip().upper() or None
    top_list_frames: list[pd.DataFrame] = []
    top_inst_frames: list[pd.DataFrame] = []
    errors: list[dict[str, str]] = []
    sleep_seconds = max(0.0, float(request_sleep_seconds or 0))

    for trade_date in trade_dates:
        try:
            top_list_frames.append(_query_tushare_frame(pro, "top_list", trade_date, ts_code_text, TOP_LIST_FIELDS))
        except Exception as exc:
            errors.append({"api": "top_list", "trade_date": trade_date, "error": str(exc)})

        if sleep_seconds:
            time.sleep(sleep_seconds)

        if include_inst:
            try:
                top_inst_frames.append(_query_tushare_frame(pro, "top_inst", trade_date, ts_code_text, TOP_INST_FIELDS))
            except Exception as exc:
                errors.append({"api": "top_inst", "trade_date": trade_date, "error": str(exc)})

            if sleep_seconds:
                time.sleep(sleep_seconds)

    top_list_raw = pd.concat(top_list_frames, ignore_index=True) if top_list_frames else pd.DataFrame(columns=TOP_LIST_COLUMNS)
    top_inst_raw = pd.concat(top_inst_frames, ignore_index=True) if top_inst_frames else pd.DataFrame(columns=TOP_INST_COLUMNS)

    return {
        "start_date": resolved_start,
        "end_date": resolved_end,
        "trade_dates": trade_dates,
        "top_list": prepare_lhb_top_list_frame(top_list_raw),
        "top_inst": prepare_lhb_inst_frame(top_inst_raw),
        "errors": errors,
    }
