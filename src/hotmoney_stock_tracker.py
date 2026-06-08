# -*- coding: utf-8 -*-
"""Single-stock hot-money operation tracking helpers."""

from __future__ import annotations

from typing import Optional

import pandas as pd
from sqlalchemy.engine import Engine


EVIDENCE_COLUMNS = [
    "trade_date",
    "ts_code",
    "ts_name",
    "actor_name",
    "seat_name",
    "evidence_type",
    "source",
    "confidence",
    "buy_amount_yi",
    "sell_amount_yi",
    "net_amount_yi",
    "abs_net_amount_yi",
    "reason",
]


def normalize_stock_code(raw_value: str) -> str:
    value = str(raw_value or "").strip().upper()
    if not value:
        return ""
    if "." in value:
        code, suffix = value.split(".", 1)
        return f"{code}.{suffix.upper()}"
    if value.isdigit() and len(value) == 6:
        if value.startswith(("4", "8", "9")):
            return f"{value}.BJ"
        if value.startswith(("5", "6")):
            return f"{value}.SH"
        return f"{value}.SZ"
    return str(raw_value or "").strip()


def is_stock_code_like(value: str) -> bool:
    normalized = normalize_stock_code(value)
    if "." not in normalized:
        return False
    code, suffix = normalized.split(".", 1)
    return len(code) == 6 and code.isdigit() and suffix in {"SH", "SZ", "BJ"}


def _empty_evidence_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=EVIDENCE_COLUMNS)


def _numeric_series(work: pd.DataFrame, column: str) -> pd.Series:
    if column not in work.columns:
        return pd.Series([0.0] * len(work), index=work.index)
    return pd.to_numeric(work[column], errors="coerce").fillna(0.0)


def _text_series(work: pd.DataFrame, column: str, fallback: str = "") -> pd.Series:
    if column not in work.columns:
        return pd.Series([fallback] * len(work), index=work.index)
    values = work[column].fillna("").astype(str).str.strip()
    if fallback:
        values = values.replace("", fallback)
    return values


def _compact_unique(values, max_items: int = 4) -> str:
    items: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text.lower() == "nan":
            continue
        if text not in items:
            items.append(text)
    if not items:
        return "-"
    label = "、".join(items[:max_items])
    if len(items) > max_items:
        label = f"{label} 等{len(items)}项"
    return label


def _prepare_direct_hotmoney_evidence(detail_df: pd.DataFrame | None) -> pd.DataFrame:
    if detail_df is None or detail_df.empty:
        return _empty_evidence_frame()

    work = detail_df.copy()
    out = pd.DataFrame(index=work.index)
    out["trade_date"] = pd.to_datetime(_text_series(work, "trade_date"), errors="coerce")
    out["ts_code"] = _text_series(work, "ts_code")
    out["ts_name"] = _text_series(work, "ts_name")
    out["actor_name"] = _text_series(work, "hm_name", "未知游资")
    out["seat_name"] = _text_series(work, "hm_orgs")
    out["evidence_type"] = "direct_hotmoney"
    out["source"] = "hm_detail"
    out["confidence"] = "direct"
    out["buy_amount_yi"] = _numeric_series(work, "buy_amount") / 1e8
    out["sell_amount_yi"] = _numeric_series(work, "sell_amount") / 1e8
    out["net_amount_yi"] = _numeric_series(work, "net_amount") / 1e8
    out["abs_net_amount_yi"] = out["net_amount_yi"].abs()
    out["reason"] = _text_series(work, "tag")
    return out[EVIDENCE_COLUMNS].sort_values(["trade_date", "abs_net_amount_yi"], ascending=[False, False]).reset_index(drop=True)


def _prepare_lhb_seat_evidence(inst_df: pd.DataFrame | None) -> pd.DataFrame:
    if inst_df is None or inst_df.empty:
        return _empty_evidence_frame()

    work = inst_df.copy()
    out = pd.DataFrame(index=work.index)
    out["trade_date"] = pd.to_datetime(_text_series(work, "trade_date"), errors="coerce")
    out["ts_code"] = _text_series(work, "ts_code")
    out["ts_name"] = _text_series(work, "name")
    out["actor_name"] = _text_series(work, "exalter", "未知席位")
    out["seat_name"] = out["actor_name"]
    out["evidence_type"] = "lhb_seat"
    out["source"] = "lhb_top_inst"
    out["confidence"] = "seat_evidence"
    out["buy_amount_yi"] = _numeric_series(work, "buy") / 1e8
    out["sell_amount_yi"] = _numeric_series(work, "sell") / 1e8
    out["net_amount_yi"] = _numeric_series(work, "net_buy") / 1e8
    out["abs_net_amount_yi"] = out["net_amount_yi"].abs()
    out["reason"] = _text_series(work, "reason")
    return out[EVIDENCE_COLUMNS].sort_values(["trade_date", "abs_net_amount_yi"], ascending=[False, False]).reset_index(drop=True)


def _date_label(evidence_df: pd.DataFrame, lhb_top_list_df: pd.DataFrame | None = None) -> str:
    dates: list[pd.Timestamp] = []
    if evidence_df is not None and not evidence_df.empty:
        dates.extend(pd.to_datetime(evidence_df["trade_date"], errors="coerce").dropna().tolist())
    if lhb_top_list_df is not None and not lhb_top_list_df.empty and "trade_date" in lhb_top_list_df.columns:
        dates.extend(pd.to_datetime(lhb_top_list_df["trade_date"], errors="coerce").dropna().tolist())
    if not dates:
        return "-"
    start = min(dates).strftime("%Y-%m-%d")
    end = max(dates).strftime("%Y-%m-%d")
    return start if start == end else f"{start}~{end}"


def _first_non_empty(*frames: pd.DataFrame, column: str) -> str:
    for frame in frames:
        if frame is None or frame.empty or column not in frame.columns:
            continue
        for value in frame[column].tolist():
            text = str(value or "").strip()
            if text and text.lower() != "nan":
                return text
    return ""


def _build_actor_summary(evidence_df: pd.DataFrame) -> pd.DataFrame:
    if evidence_df is None or evidence_df.empty:
        return pd.DataFrame(
            columns=[
                "actor_name",
                "evidence_type",
                "confidence",
                "hit_count",
                "trade_days",
                "seat_count",
                "buy_amount_yi",
                "sell_amount_yi",
                "net_amount_yi",
                "abs_net_amount_yi",
                "latest_date",
                "first_date",
                "reasons",
            ]
        )

    work = evidence_df.copy()
    grouped = work.groupby(["actor_name", "evidence_type", "confidence"], dropna=False)
    summary = grouped.agg(
        hit_count=("actor_name", "size"),
        trade_days=("trade_date", "nunique"),
        seat_count=("seat_name", lambda values: len({str(v).strip() for v in values if str(v).strip()})),
        buy_amount_yi=("buy_amount_yi", "sum"),
        sell_amount_yi=("sell_amount_yi", "sum"),
        net_amount_yi=("net_amount_yi", "sum"),
        abs_net_amount_yi=("abs_net_amount_yi", "sum"),
        latest_date=("trade_date", "max"),
        first_date=("trade_date", "min"),
        reasons=("reason", _compact_unique),
    ).reset_index()
    summary["evidence_rank"] = summary["evidence_type"].map({"direct_hotmoney": 0, "lhb_seat": 1}).fillna(9)
    summary = summary.sort_values(["evidence_rank", "abs_net_amount_yi", "hit_count"], ascending=[True, False, False])
    return summary.drop(columns=["evidence_rank"]).reset_index(drop=True)


def _build_daily_summary(evidence_df: pd.DataFrame) -> pd.DataFrame:
    if evidence_df is None or evidence_df.empty:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "hit_count",
                "direct_actor_count",
                "lhb_seat_count",
                "buy_amount_yi",
                "sell_amount_yi",
                "net_amount_yi",
                "abs_net_amount_yi",
            ]
        )

    work = evidence_df.copy()
    rows = []
    for trade_date, date_rows in work.groupby("trade_date", dropna=False):
        direct_rows = date_rows[date_rows["evidence_type"] == "direct_hotmoney"]
        lhb_rows = date_rows[date_rows["evidence_type"] == "lhb_seat"]
        rows.append(
            {
                "trade_date": trade_date,
                "hit_count": int(len(date_rows)),
                "direct_actor_count": int(direct_rows["actor_name"].nunique()),
                "lhb_seat_count": int(lhb_rows["actor_name"].nunique()),
                "buy_amount_yi": float(date_rows["buy_amount_yi"].sum()),
                "sell_amount_yi": float(date_rows["sell_amount_yi"].sum()),
                "net_amount_yi": float(date_rows["net_amount_yi"].sum()),
                "abs_net_amount_yi": float(date_rows["abs_net_amount_yi"].sum()),
            }
        )
    return pd.DataFrame(rows).sort_values("trade_date").reset_index(drop=True)


def _build_lhb_reason_summary(lhb_top_list_df: pd.DataFrame | None) -> pd.DataFrame:
    if lhb_top_list_df is None or lhb_top_list_df.empty:
        return pd.DataFrame(columns=["trade_date", "reason_count", "net_amount_yi", "lhb_amount_yi", "reasons"])

    work = lhb_top_list_df.copy()
    work["trade_date"] = pd.to_datetime(_text_series(work, "trade_date"), errors="coerce")
    work["net_amount_yi"] = _numeric_series(work, "net_amount") / 1e8
    work["lhb_amount_yi"] = _numeric_series(work, "l_amount") / 1e8
    work["reason"] = _text_series(work, "reason")
    summary = work.groupby("trade_date", dropna=False).agg(
        reason_count=("reason", "size"),
        net_amount_yi=("net_amount_yi", "sum"),
        lhb_amount_yi=("lhb_amount_yi", "sum"),
        reasons=("reason", _compact_unique),
    ).reset_index()
    return summary.sort_values("trade_date", ascending=False).reset_index(drop=True)


def build_single_stock_hotmoney_model(
    hotmoney_detail_df: pd.DataFrame | None,
    *,
    lhb_top_list_df: pd.DataFrame | None = None,
    lhb_inst_df: pd.DataFrame | None = None,
) -> dict:
    direct_evidence = _prepare_direct_hotmoney_evidence(hotmoney_detail_df)
    lhb_evidence = _prepare_lhb_seat_evidence(lhb_inst_df)
    evidence_parts = [frame for frame in [direct_evidence, lhb_evidence] if frame is not None and not frame.empty]
    evidence_detail = pd.concat(evidence_parts, ignore_index=True) if evidence_parts else _empty_evidence_frame()
    if not evidence_detail.empty:
        evidence_detail = evidence_detail.sort_values(["trade_date", "abs_net_amount_yi"], ascending=[False, False]).reset_index(drop=True)

    stock_code = _first_non_empty(evidence_detail, lhb_top_list_df, column="ts_code")
    stock_name = _first_non_empty(evidence_detail, lhb_top_list_df, column="ts_name") or _first_non_empty(lhb_top_list_df, column="name")
    direct_rows = evidence_detail[evidence_detail["evidence_type"] == "direct_hotmoney"] if not evidence_detail.empty else evidence_detail
    lhb_rows = evidence_detail[evidence_detail["evidence_type"] == "lhb_seat"] if not evidence_detail.empty else evidence_detail
    has_direct = direct_rows is not None and not direct_rows.empty
    has_lhb = lhb_rows is not None and not lhb_rows.empty

    if has_direct and has_lhb:
        confidence_label = "direct+seat"
    elif has_direct:
        confidence_label = "direct"
    elif has_lhb:
        confidence_label = "seat"
    else:
        confidence_label = "no_data"

    return {
        "stock_code": stock_code,
        "stock_name": stock_name or stock_code or "-",
        "date_label": _date_label(evidence_detail, lhb_top_list_df),
        "confidence_label": confidence_label,
        "direct_hotmoney_count": int(direct_rows["actor_name"].nunique()) if has_direct else 0,
        "lhb_seat_count": int(lhb_rows["actor_name"].nunique()) if has_lhb else 0,
        "direct_net_yi": float(direct_rows["net_amount_yi"].sum()) if has_direct else 0.0,
        "lhb_seat_net_yi": float(lhb_rows["net_amount_yi"].sum()) if has_lhb else 0.0,
        "total_net_yi": float(evidence_detail["net_amount_yi"].sum()) if not evidence_detail.empty else 0.0,
        "evidence_detail": evidence_detail,
        "actor_summary": _build_actor_summary(evidence_detail),
        "daily_summary": _build_daily_summary(evidence_detail),
        "lhb_reason_summary": _build_lhb_reason_summary(lhb_top_list_df),
    }


def load_single_stock_hotmoney_model(
    *,
    start_date: str,
    end_date: str,
    stock_query: str,
    engine: Optional[Engine] = None,
    limit: int = 5000,
) -> dict:
    from src.hotmoney_monitor import query_hotmoney_detail
    from src.lhb_monitor import query_lhb_top_inst, query_lhb_top_list
    from src.moneyflow_fetcher import _get_engine_cached

    if engine is None:
        engine = _get_engine_cached()

    stock_query_text = str(stock_query or "").strip()
    normalized_code = normalize_stock_code(stock_query_text)
    code_filter = normalized_code if is_stock_code_like(normalized_code) else None

    hotmoney_detail = query_hotmoney_detail(
        start_date,
        end_date,
        ts_code=code_filter,
        stock_keyword=None if code_filter else stock_query_text,
        limit=limit,
        engine=engine,
    )

    if code_filter:
        lhb_top_list = query_lhb_top_list(start_date, end_date, ts_code=code_filter, limit=limit, engine=engine)
        lhb_inst = query_lhb_top_inst(start_date, end_date, ts_code=code_filter, limit=limit, engine=engine)
    else:
        lhb_top_list_all = query_lhb_top_list(start_date, end_date, ts_code=None, limit=limit, engine=engine)
        if lhb_top_list_all.empty:
            lhb_top_list = lhb_top_list_all
            lhb_inst = pd.DataFrame()
        else:
            keyword = stock_query_text.lower()
            lhb_top_list = lhb_top_list_all[
                lhb_top_list_all["ts_code"].astype(str).str.lower().str.contains(keyword, na=False)
                | lhb_top_list_all["name"].astype(str).str.lower().str.contains(keyword, na=False)
            ].copy()
            matched_codes = sorted(lhb_top_list["ts_code"].dropna().astype(str).unique().tolist())
            if matched_codes:
                lhb_inst_all = query_lhb_top_inst(start_date, end_date, ts_code=None, limit=limit, engine=engine)
                lhb_inst = lhb_inst_all[lhb_inst_all["ts_code"].astype(str).isin(matched_codes)].copy()
            else:
                lhb_inst = pd.DataFrame()

    return build_single_stock_hotmoney_model(
        hotmoney_detail,
        lhb_top_list_df=lhb_top_list,
        lhb_inst_df=lhb_inst,
    )
