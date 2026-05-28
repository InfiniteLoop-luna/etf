from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Callable

import pandas as pd
from sqlalchemy.engine import Engine

from src.etf_stats import (
    get_stock_financial_timeseries,
    get_stock_kline_timeseries,
    get_stock_profile,
    get_stock_timeseries,
)
from src.stock_research_akshare_enrichment import build_stock_research_supplemental

logger = logging.getLogger(__name__)


def _normalize_ts_code(ts_code: str | None) -> str:
    return str(ts_code or "").strip().upper()


def _normalize_trade_date(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.notna(parsed):
        return parsed.strftime("%Y-%m-%d")
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text[:10]


def _to_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _records(df: pd.DataFrame | None, limit: int = 20) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    out = df.tail(limit).copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%d").where(out[col].notna(), None)
    return [
        {str(key): (None if pd.isna(value) else value) for key, value in row.items()}
        for row in out.to_dict(orient="records")
    ]


def _profile_dict(profile_df: pd.DataFrame | None, ts_code: str, fallback_name: str) -> dict[str, Any]:
    if profile_df is None or profile_df.empty:
        return {"ts_code": ts_code, "name": fallback_name or ts_code}
    row = profile_df.iloc[0].to_dict()
    row["ts_code"] = row.get("ts_code") or ts_code
    row["name"] = row.get("name") or fallback_name or ts_code
    for key in list(row):
        value = row[key]
        if pd.isna(value):
            row[key] = None
    return row


def _build_price_metrics(ts_df: pd.DataFrame | None, kline_df: pd.DataFrame | None) -> dict[str, Any]:
    source_df = kline_df if kline_df is not None and not kline_df.empty else ts_df
    if source_df is None or source_df.empty or "close" not in source_df.columns:
        return {"data_points": 0}

    df = source_df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    for col in ["open", "high", "low", "vol", "amount", "turnover_rate", "pe_ttm", "pb", "total_mv"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["trade_date", "close"]).sort_values("trade_date")
    if df.empty:
        return {"data_points": 0}

    close = df["close"]
    latest_close = _to_float(close.iloc[-1])
    previous_close = _to_float(close.iloc[-2]) if len(close) >= 2 else None

    def pct_return(days: int) -> float | None:
        if len(close) <= days:
            return None
        base = _to_float(close.iloc[-days - 1])
        latest = latest_close
        if not base or latest is None:
            return None
        return (latest / base - 1.0) * 100.0

    latest_date = df["trade_date"].iloc[-1].strftime("%Y-%m-%d")
    high_52w = _to_float(df["high"].tail(252).max()) if "high" in df.columns else _to_float(close.tail(252).max())
    low_52w = _to_float(df["low"].tail(252).min()) if "low" in df.columns else _to_float(close.tail(252).min())
    drawdown_from_52w_high = None
    if latest_close is not None and high_52w:
        drawdown_from_52w_high = (latest_close / high_52w - 1.0) * 100.0

    volume_ratio_20 = None
    if "vol" in df.columns and len(df) >= 21:
        latest_vol = _to_float(df["vol"].iloc[-1])
        mean_vol = _to_float(df["vol"].tail(21).iloc[:-1].mean())
        if latest_vol is not None and mean_vol:
            volume_ratio_20 = latest_vol / mean_vol

    return {
        "data_points": int(len(df)),
        "latest_trade_date": latest_date,
        "latest_close": latest_close,
        "previous_close": previous_close,
        "ret_1d_pct": None if latest_close is None or not previous_close else (latest_close / previous_close - 1.0) * 100.0,
        "ret_5d_pct": pct_return(5),
        "ret_20d_pct": pct_return(20),
        "ret_60d_pct": pct_return(60),
        "high_52w": high_52w,
        "low_52w": low_52w,
        "drawdown_from_52w_high_pct": drawdown_from_52w_high,
        "volume_ratio_20": volume_ratio_20,
        "latest_pe_ttm": _to_float(df["pe_ttm"].iloc[-1]) if "pe_ttm" in df.columns else None,
        "latest_pb": _to_float(df["pb"].iloc[-1]) if "pb" in df.columns else None,
        "latest_total_mv_yi": (_to_float(df["total_mv"].iloc[-1]) or 0) / 10000.0 if "total_mv" in df.columns else None,
    }


def _build_financial_metrics(financial_df: pd.DataFrame | None, profile: dict[str, Any]) -> dict[str, Any]:
    latest = {
        "roe": _to_float(profile.get("roe")),
        "roa": _to_float(profile.get("roa")),
        "gross_margin": _to_float(profile.get("gross_margin")),
        "debt_to_assets": _to_float(profile.get("debt_to_assets")),
        "total_revenue_yi": (_to_float(profile.get("total_revenue")) or 0) / 100000000.0 if profile.get("total_revenue") is not None else None,
        "net_profit_yi": (_to_float(profile.get("n_income")) or 0) / 100000000.0 if profile.get("n_income") is not None else None,
        "operating_cashflow_yi": (_to_float(profile.get("n_cashflow_act")) or 0) / 100000000.0 if profile.get("n_cashflow_act") is not None else None,
        "fina_end_date": _normalize_trade_date(profile.get("fina_end_date")),
    }
    if financial_df is None or financial_df.empty:
        return {"latest": latest, "history": []}
    df = financial_df.copy()
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")
    df = df.dropna(subset=["end_date"]).sort_values("end_date")
    return {"latest": latest, "history": _records(df, limit=8)}


def build_stock_research_fact_pack(
    ts_code: str,
    stock_name: str = "",
    *,
    engine: Engine,
    asof_trade_date: str | None = None,
    lookback_days: int = 420,
    allow_live_fetch: bool = False,
    supplemental_builder: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a compact evidence payload for LLM-based watchlist stock research."""
    ts_code_key = _normalize_ts_code(ts_code)
    if not ts_code_key:
        raise ValueError("ts_code 不能为空")

    profile_df = pd.DataFrame()
    ts_df = pd.DataFrame()
    financial_df = pd.DataFrame()
    kline_df = pd.DataFrame()
    errors: list[str] = []

    try:
        profile_df = get_stock_profile(ts_code_key, engine=engine)
    except Exception as exc:
        errors.append(f"profile: {exc}")
        logger.warning("Failed to load stock profile for %s: %s", ts_code_key, exc)

    end_date = _normalize_trade_date(asof_trade_date)
    try:
        ts_df = get_stock_timeseries(ts_code_key, end_date=end_date or None, engine=engine)
    except Exception as exc:
        errors.append(f"timeseries: {exc}")
        logger.warning("Failed to load stock timeseries for %s: %s", ts_code_key, exc)

    if ts_df is not None and not ts_df.empty and lookback_days > 0:
        ts_df = ts_df.tail(int(lookback_days)).copy()

    try:
        financial_df = get_stock_financial_timeseries(ts_code_key, engine=engine)
    except Exception as exc:
        errors.append(f"financial: {exc}")
        logger.warning("Failed to load stock financial timeseries for %s: %s", ts_code_key, exc)

    try:
        kline_df = get_stock_kline_timeseries(ts_code_key, end_date=end_date or None, engine=engine)
    except Exception as exc:
        errors.append(f"kline: {exc}")
        logger.warning("Failed to load stock kline for %s: %s", ts_code_key, exc)

    if kline_df is not None and not kline_df.empty and lookback_days > 0:
        kline_df = kline_df.tail(int(lookback_days)).copy()

    profile = _profile_dict(profile_df, ts_code_key, stock_name)
    price_metrics = _build_price_metrics(ts_df, kline_df)
    actual_asof = end_date or price_metrics.get("latest_trade_date") or _normalize_trade_date(profile.get("latest_trade_date"))
    supplemental_builder = supplemental_builder or build_stock_research_supplemental
    try:
        supplemental = supplemental_builder(
            ts_code_key,
            stock_name=profile.get("name") or stock_name or ts_code_key,
            industry=profile.get("industry") or "",
            enabled=allow_live_fetch,
        )
    except Exception as exc:
        errors.append(f"supplemental: {exc}")
        logger.warning("Failed to build stock research supplemental data for %s: %s", ts_code_key, exc)
        supplemental = {}
    supplemental_status = {
        str(name): str((block or {}).get("status") or "missing")
        for name, block in (supplemental or {}).items()
        if isinstance(block, dict)
    }

    return {
        "schema_version": "stock-research-fact-pack-v2",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "asof_trade_date": actual_asof,
        "ts_code": ts_code_key,
        "stock_name": profile.get("name") or stock_name or ts_code_key,
        "profile": {
            key: profile.get(key)
            for key in [
                "ts_code",
                "symbol",
                "name",
                "industry",
                "market",
                "list_date",
                "has_ever_st",
                "main_business",
                "business_scope",
                "website",
                "holder_num",
                "holder_end_date",
            ]
        },
        "price_metrics": price_metrics,
        "financial_metrics": _build_financial_metrics(financial_df, profile),
        "valuation_snapshot": {
            "pe_ttm": profile.get("pe_ttm") or price_metrics.get("latest_pe_ttm"),
            "pb": profile.get("pb") or price_metrics.get("latest_pb"),
            "ps_ttm": profile.get("ps_ttm"),
            "total_mv_yi": (_to_float(profile.get("total_mv")) or 0) / 10000.0 if profile.get("total_mv") is not None else price_metrics.get("latest_total_mv_yi"),
            "circ_mv_yi": (_to_float(profile.get("circ_mv")) or 0) / 10000.0 if profile.get("circ_mv") is not None else None,
        },
        "price_tail": _records(kline_df if kline_df is not None and not kline_df.empty else ts_df, limit=90),
        "daily_factor_tail": _records(ts_df, limit=60),
        "financial_tail": _records(financial_df, limit=8),
        "supplemental": supplemental,
        "data_quality": {
            "profile_rows": int(len(profile_df)) if profile_df is not None else 0,
            "daily_rows": int(len(ts_df)) if ts_df is not None else 0,
            "kline_rows": int(len(kline_df)) if kline_df is not None else 0,
            "financial_rows": int(len(financial_df)) if financial_df is not None else 0,
            "supplemental_enabled": bool(allow_live_fetch),
            "supplemental_status": supplemental_status,
            "errors": errors,
            "allow_live_fetch": bool(allow_live_fetch),
        },
    }
