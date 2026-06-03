# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


ALGORITHM_NAME = "ProfessionalTrendRanker"
ALGORITHM_VERSION = "1.1"


@dataclass(frozen=True)
class RecoConfig:
    lookback_days: int = 160
    min_rows_per_symbol: int = 60
    topn: int = 10
    trade_date: str = ""
    min_close: float = 2.0
    min_avg_amount: float = 20_000.0
    min_listing_days: int = 90
    industry_neutral_weight: float = 0.35
    probability_calibration_anchors: int = 4
    probability_calibration_spacing_bars: int = 15
    probability_calibration_horizon_bars: int = 5
    probability_calibration_bins: int = 6
    probability_calibration_min_samples: int = 500
    probability_calibration_blend: float = 0.45
    exclude_limit_up_from_uptrend: bool = True
    exclude_limit_down_from_uptrend: bool = True
    ensure_source_views: bool = True


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _to_float(value: Any, default: float = np.nan) -> float:
    try:
        if value is None:
            return float(default)
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return float(default)
        return out
    except Exception:
        return float(default)


def _clip(value: float, lower: float, upper: float) -> float:
    return float(max(lower, min(upper, value)))


def _sigmoid(value: float) -> float:
    value = _clip(value, -40.0, 40.0)
    return 1.0 / (1.0 + math.exp(-value))


def _safe_ratio(numerator: float, denominator: float, default: float = np.nan) -> float:
    numerator = _to_float(numerator)
    denominator = _to_float(denominator)
    if pd.isna(numerator) or pd.isna(denominator) or abs(denominator) < 1e-12:
        return float(default)
    return float(numerator / denominator)


def _last_value(series: pd.Series, default: float = np.nan) -> float:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return float(default)
    return float(clean.iloc[-1])


def _window_return(close: pd.Series, bars: int) -> float:
    clean = pd.to_numeric(close, errors="coerce").dropna()
    if len(clean) <= bars:
        return np.nan
    base = float(clean.iloc[-bars - 1])
    last = float(clean.iloc[-1])
    if base <= 0:
        return np.nan
    return last / base - 1.0


def _max_drawdown(close: pd.Series, window: int) -> float:
    clean = pd.to_numeric(close, errors="coerce").dropna().tail(window)
    if clean.empty:
        return np.nan
    running_high = clean.cummax()
    drawdown = clean / running_high - 1.0
    return float(abs(drawdown.min()))


def _trend_slope_quality(close: pd.Series, window: int) -> tuple[float, float]:
    clean = pd.to_numeric(close, errors="coerce").dropna().tail(window)
    if len(clean) < max(10, window // 2) or (clean <= 0).any():
        return np.nan, np.nan

    y = np.log(clean.to_numpy(dtype=float))
    x = np.arange(len(y), dtype=float)
    slope, intercept = np.polyfit(x, y, 1)
    fitted = slope * x + intercept
    ss_res = float(np.sum((y - fitted) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r2 = 0.0 if ss_tot <= 1e-12 else max(0.0, 1.0 - ss_res / ss_tot)
    return float(slope * len(y) * r2), float(r2)


def _rsi(close: pd.Series, window: int = 14) -> float:
    clean = pd.to_numeric(close, errors="coerce").dropna()
    if len(clean) <= window:
        return np.nan
    delta = clean.diff()
    gain = delta.clip(lower=0).rolling(window, min_periods=window).mean()
    loss = (-delta.clip(upper=0)).rolling(window, min_periods=window).mean()
    latest_gain = _last_value(gain)
    latest_loss = _last_value(loss)
    if pd.isna(latest_gain) or pd.isna(latest_loss):
        return np.nan
    if latest_loss <= 1e-12:
        return 100.0
    rs = latest_gain / latest_loss
    return float(100.0 - 100.0 / (1.0 + rs))


def _is_st_name(name: Any) -> bool:
    text_value = str(name or "").strip().upper()
    return (
        text_value.startswith("ST")
        or text_value.startswith("*ST")
        or text_value.startswith("S*ST")
        or text_value.startswith("SST")
        or "退" in text_value
    )


def _limit_threshold_pct(ts_code: Any) -> float:
    code = str(ts_code or "").strip().upper()
    pure = code.split(".")[0]
    if code.endswith(".BJ") or pure.startswith(("8", "9")):
        return 29.7
    if pure.startswith(("300", "301", "688")):
        return 19.7
    return 9.7


def _robust_zscore(series: pd.Series, clip: float = 3.0) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    median = values.median()
    mad = (values - median).abs().median()
    if pd.isna(median):
        return pd.Series(0.0, index=series.index)

    if pd.notna(mad) and mad > 1e-12:
        scaled = (values - median) / (1.4826 * mad)
    else:
        std = values.std(ddof=0)
        if pd.isna(std) or std <= 1e-12:
            return pd.Series(0.0, index=series.index)
        scaled = (values - values.mean()) / std
    return scaled.clip(-clip, clip).fillna(0.0)


def _industry_neutral_zscore(
    frame: pd.DataFrame,
    column: str,
    *,
    industry_weight: float,
    min_group_size: int = 8,
) -> pd.Series:
    global_z = _robust_zscore(frame[column])
    if "industry" not in frame.columns or industry_weight <= 0:
        return global_z

    def _group_z(group: pd.Series) -> pd.Series:
        if group.notna().sum() < min_group_size:
            return pd.Series(np.nan, index=group.index)
        return _robust_zscore(group)

    industry_z = frame.groupby("industry", dropna=False)[column].transform(_group_z)
    industry_z = industry_z.fillna(global_z)
    global_weight = 1.0 - float(industry_weight)
    return (global_z * global_weight + industry_z * float(industry_weight)).fillna(0.0)


def _format_pct(value: float) -> str:
    if pd.isna(value):
        return "-"
    return f"{value:.1%}"


def _format_x(value: float) -> str:
    if pd.isna(value):
        return "-"
    return f"{value:.2f}x"


def _build_reason(row: pd.Series, mode: str) -> str:
    reasons: list[str] = []
    mom20 = _to_float(row.get("mom20"))
    mom60 = _to_float(row.get("mom60"))
    ma_alignment = _to_float(row.get("ma_alignment"))
    slope_r2_60 = _to_float(row.get("slope_r2_60"))
    amount_ratio = _to_float(row.get("amount_ma5_ratio"))
    volatility20 = _to_float(row.get("volatility20"))
    drawdown60 = _to_float(row.get("max_drawdown60"))
    rsi14 = _to_float(row.get("rsi14"))

    if mode == "up":
        if pd.notna(mom20) and mom20 > 0:
            reasons.append(f"20日动量{_format_pct(mom20)}")
        if pd.notna(mom60) and mom60 > 0:
            reasons.append(f"60日动量{_format_pct(mom60)}")
        if pd.notna(ma_alignment) and ma_alignment > 0:
            reasons.append("均线结构偏多")
        if pd.notna(slope_r2_60) and slope_r2_60 >= 0.45:
            reasons.append(f"60日趋势拟合度{_format_pct(slope_r2_60)}")
        if pd.notna(amount_ratio) and amount_ratio >= 1.15:
            reasons.append(f"成交额确认{_format_x(amount_ratio)}")
        if pd.notna(rsi14) and rsi14 >= 78:
            reasons.append(f"RSI {rsi14:.0f} 偏热，需控制追高")
        if pd.notna(volatility20):
            reasons.append(f"20日波动{_format_pct(volatility20)}")
    else:
        if pd.notna(mom20) and mom20 < 0:
            reasons.append(f"20日动量{_format_pct(mom20)}")
        if pd.notna(mom60) and mom60 < 0:
            reasons.append(f"60日动量{_format_pct(mom60)}")
        if pd.notna(ma_alignment) and ma_alignment < 0:
            reasons.append("均线结构偏空")
        if pd.notna(drawdown60):
            reasons.append(f"60日最大回撤{_format_pct(drawdown60)}")
        if pd.notna(volatility20):
            reasons.append(f"20日波动{_format_pct(volatility20)}")
        if pd.notna(amount_ratio) and amount_ratio < 0.75:
            reasons.append(f"成交额萎缩至{_format_x(amount_ratio)}")

    if not reasons:
        return "多因子趋势-风险模型综合排序"
    return "；".join(reasons[:5])


def _downcast_float_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce", downcast="float")
    return frame


def _compute_symbol_features(
    group: pd.DataFrame,
    effective_trade_date: pd.Timestamp,
    config: RecoConfig,
    latest_meta: dict[str, Any] | None = None,
) -> dict | None:
    ordered = group.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="last").reset_index(drop=True)
    ordered = ordered[ordered["trade_date"] <= effective_trade_date].copy()
    if len(ordered) < int(config.min_rows_per_symbol):
        return None

    latest = ordered.iloc[-1]
    latest_meta = latest_meta or {}
    latest_trade_date = pd.to_datetime(latest.get("trade_date"), errors="coerce")
    if pd.isna(latest_trade_date) or latest_trade_date.normalize() != effective_trade_date.normalize():
        return None

    name = str(latest_meta.get("name") or latest.get("name") or "").strip()
    list_status = str(latest_meta.get("list_status") or latest.get("list_status") or "").strip().upper()
    if _is_st_name(name) or (list_status and list_status != "L"):
        return None

    list_date = pd.to_datetime(latest_meta.get("list_date") or latest.get("list_date"), errors="coerce")
    if pd.notna(list_date):
        listing_days = int((effective_trade_date.normalize() - list_date.normalize()).days)
        if listing_days < int(config.min_listing_days):
            return None
    else:
        listing_days = None

    close = pd.to_numeric(ordered.get("close"), errors="coerce")
    high = pd.to_numeric(ordered.get("high"), errors="coerce")
    low = pd.to_numeric(ordered.get("low"), errors="coerce")
    amount = pd.to_numeric(ordered.get("amount"), errors="coerce")
    vol = pd.to_numeric(ordered.get("vol"), errors="coerce")
    ret = close.pct_change(fill_method=None)

    latest_close = _last_value(close)
    if pd.isna(latest_close) or latest_close < float(config.min_close):
        return None

    amount_ma20 = _last_value(amount.rolling(20, min_periods=10).mean())
    if float(config.min_avg_amount) > 0 and (pd.isna(amount_ma20) or amount_ma20 < float(config.min_avg_amount)):
        return None

    ma5 = _last_value(close.rolling(5, min_periods=5).mean())
    ma20 = _last_value(close.rolling(20, min_periods=20).mean())
    ma60 = _last_value(close.rolling(60, min_periods=60).mean())
    ma120 = _last_value(close.rolling(120, min_periods=80).mean())
    high20 = _last_value(high.rolling(20, min_periods=20).max())
    high60 = _last_value(high.rolling(60, min_periods=60).max())
    low60 = _last_value(low.rolling(60, min_periods=60).min())
    amount_ma5 = _last_value(amount.rolling(5, min_periods=5).mean())
    vol_ma20 = _last_value(vol.rolling(20, min_periods=10).mean())

    slope20, slope_r2_20 = _trend_slope_quality(close, 20)
    slope60, slope_r2_60 = _trend_slope_quality(close, 60)
    volatility20 = _last_value(ret.rolling(20, min_periods=15).std(ddof=0))
    volatility60 = _last_value(ret.rolling(60, min_periods=40).std(ddof=0))
    downside20 = _last_value(ret.clip(upper=0).rolling(20, min_periods=15).std(ddof=0))
    rsi14 = _rsi(close, 14)

    close_over_ma20 = _safe_ratio(latest_close, ma20) - 1.0
    close_over_ma60 = _safe_ratio(latest_close, ma60) - 1.0
    ma5_over_ma20 = _safe_ratio(ma5, ma20) - 1.0
    ma20_over_ma60 = _safe_ratio(ma20, ma60) - 1.0
    ma60_over_ma120 = _safe_ratio(ma60, ma120) - 1.0
    ma_alignment = np.nanmean([close_over_ma20, ma5_over_ma20, ma20_over_ma60, 0.5 * ma60_over_ma120])

    mom5 = _window_return(close, 5)
    mom20 = _window_return(close, 20)
    mom60 = _window_return(close, 60)
    mom120 = _window_return(close, 120)
    amount_ma5_ratio = _safe_ratio(amount_ma5, amount_ma20)
    vol_ma20_ratio = _safe_ratio(_last_value(vol), vol_ma20)
    distance_to_20d_high = _safe_ratio(latest_close, high20) - 1.0
    distance_to_60d_high = _safe_ratio(latest_close, high60) - 1.0
    distance_from_60d_low = _safe_ratio(latest_close, low60) - 1.0

    overheat = 0.0
    if pd.notna(close_over_ma20):
        overheat += max(0.0, close_over_ma20 - 0.12) * 3.0
    if pd.notna(mom5):
        overheat += max(0.0, mom5 - 0.10) * 2.0
    if pd.notna(rsi14):
        overheat += max(0.0, rsi14 - 76.0) / 40.0

    turnover_rate = _to_float(latest_meta.get("turnover_rate", latest.get("turnover_rate")))
    volume_ratio = _to_float(latest_meta.get("volume_ratio", latest.get("volume_ratio")))
    circ_mv = _to_float(latest_meta.get("circ_mv", latest.get("circ_mv")))
    total_mv = _to_float(latest_meta.get("total_mv", latest.get("total_mv")))
    pct_chg = _to_float(latest.get("pct_chg"))
    limit_threshold = _limit_threshold_pct(latest.get("ts_code"))
    is_limit_up_like = bool(pd.notna(pct_chg) and pct_chg >= limit_threshold)
    is_limit_down_like = bool(pd.notna(pct_chg) and pct_chg <= -limit_threshold)
    tradability_penalty = 0.0
    if is_limit_up_like:
        tradability_penalty += 8.0
    if is_limit_down_like:
        tradability_penalty += 12.0

    return {
        "trade_date": effective_trade_date.strftime("%Y-%m-%d"),
        "ts_code": str(latest.get("ts_code") or "").strip(),
        "name": name,
        "industry": str(latest_meta.get("industry") or latest.get("industry") or "").strip() or "-",
        "close": latest_close,
        "listing_days": listing_days,
        "mom5": mom5,
        "mom20": mom20,
        "mom60": mom60,
        "mom120": mom120,
        "ma_alignment": float(ma_alignment) if pd.notna(ma_alignment) else np.nan,
        "close_over_ma20": close_over_ma20,
        "close_over_ma60": close_over_ma60,
        "slope20": slope20,
        "slope60": slope60,
        "slope_r2_20": slope_r2_20,
        "slope_r2_60": slope_r2_60,
        "distance_to_20d_high": distance_to_20d_high,
        "distance_to_60d_high": distance_to_60d_high,
        "distance_from_60d_low": distance_from_60d_low,
        "amount_ma5_ratio": amount_ma5_ratio,
        "vol_ma20_ratio": vol_ma20_ratio,
        "turnover_rate": turnover_rate,
        "volume_ratio": volume_ratio,
        "avg_amount20": amount_ma20,
        "circ_mv": circ_mv,
        "total_mv": total_mv,
        "volatility20": volatility20,
        "volatility60": volatility60,
        "downside20": downside20,
        "max_drawdown60": _max_drawdown(close, 60),
        "rsi14": rsi14,
        "overheat": overheat,
        "pct_chg": pct_chg,
        "limit_threshold_pct": limit_threshold,
        "is_limit_up_like": is_limit_up_like,
        "is_limit_down_like": is_limit_down_like,
        "tradability_penalty": tradability_penalty,
    }


def score_trend_candidates(
    history_df: pd.DataFrame,
    config: RecoConfig | None = None,
    latest_meta_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    config = config or RecoConfig()
    if history_df is None or history_df.empty:
        return pd.DataFrame()

    df = history_df.copy()
    if "trade_date" not in df.columns or "ts_code" not in df.columns:
        raise ValueError("history_df must contain trade_date and ts_code columns")

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df = df.dropna(subset=["trade_date", "ts_code"]).copy()
    if df.empty:
        return pd.DataFrame()

    if config.trade_date:
        effective_trade_date = pd.to_datetime(str(config.trade_date), errors="coerce")
        if pd.isna(effective_trade_date):
            raise ValueError(f"invalid trade_date: {config.trade_date}")
        effective_trade_date = effective_trade_date.normalize()
    else:
        effective_trade_date = df["trade_date"].max().normalize()

    latest_meta_map: dict[str, dict[str, Any]] = {}
    if latest_meta_df is not None and not latest_meta_df.empty and "ts_code" in latest_meta_df.columns:
        meta_frame = latest_meta_df.copy()
        meta_frame["ts_code"] = meta_frame["ts_code"].astype(str).str.strip()
        latest_meta_map = meta_frame.set_index("ts_code").to_dict("index")

    rows = [
        row
        for ts_code, group in df.groupby("ts_code", sort=False, observed=False)
        if (row := _compute_symbol_features(group, effective_trade_date, config, latest_meta_map.get(str(ts_code), {}))) is not None
    ]
    if not rows:
        return pd.DataFrame()

    scored = pd.DataFrame(rows)
    industry_weight = _clip(float(config.industry_neutral_weight), 0.0, 1.0)
    positive_features = {
        "mom20": 0.18,
        "mom60": 0.18,
        "mom120": 0.10,
        "ma_alignment": 0.16,
        "slope20": 0.08,
        "slope60": 0.12,
        "distance_to_20d_high": 0.06,
        "distance_to_60d_high": 0.05,
        "amount_ma5_ratio": 0.04,
        "volume_ratio": 0.03,
    }
    risk_features = {
        "volatility20": 0.24,
        "volatility60": 0.14,
        "downside20": 0.18,
        "max_drawdown60": 0.24,
        "overheat": 0.14,
        "turnover_rate": 0.06,
    }

    for column in set(positive_features) | set(risk_features) | {"avg_amount20", "circ_mv"}:
        if column in scored.columns:
            scored[f"z_{column}"] = _industry_neutral_zscore(
                scored,
                column,
                industry_weight=industry_weight,
            )

    scored["liquidity_raw"] = np.log1p(pd.to_numeric(scored.get("avg_amount20"), errors="coerce").fillna(0.0))
    scored["z_liquidity"] = _robust_zscore(scored["liquidity_raw"])

    trend_alpha = pd.Series(0.0, index=scored.index)
    for column, weight in positive_features.items():
        trend_alpha += float(weight) * scored.get(f"z_{column}", 0.0)
    trend_alpha += 0.04 * scored["z_liquidity"]

    risk_pressure = pd.Series(0.0, index=scored.index)
    for column, weight in risk_features.items():
        risk_pressure += float(weight) * scored.get(f"z_{column}", 0.0)
    risk_pressure -= 0.04 * scored["z_liquidity"]

    scored["factor_alpha"] = trend_alpha
    scored["risk_pressure"] = risk_pressure
    scored["trend_raw"] = trend_alpha - 0.32 * risk_pressure
    scored["trend_score"] = (50.0 + 15.0 * scored["trend_raw"]).clip(0.0, 100.0)
    scored["risk_score"] = (50.0 + 16.0 * scored["risk_pressure"]).clip(0.0, 100.0)

    prob5_raw = (
        0.08
        + 0.78 * scored["trend_raw"]
        + 0.12 * scored.get("z_mom20", 0.0)
        + 0.08 * scored.get("z_slope20", 0.0)
        - 0.32 * scored["risk_pressure"]
        - 0.08 * scored.get("z_overheat", 0.0)
    )
    prob20_raw = (
        0.05
        + 0.88 * scored["trend_raw"]
        + 0.16 * scored.get("z_mom60", 0.0)
        + 0.10 * scored.get("z_slope60", 0.0)
        - 0.24 * scored["risk_pressure"]
    )
    scored["prob_up_5d"] = prob5_raw.map(lambda x: _clip(_sigmoid(_to_float(x, 0.0)), 0.08, 0.92))
    scored["prob_up_20d"] = prob20_raw.map(lambda x: _clip(_sigmoid(_to_float(x, 0.0)), 0.08, 0.92))

    scored["recommendation_score"] = (
        scored["trend_score"] * 0.58
        + scored["prob_up_5d"] * 100.0 * 0.24
        + scored["prob_up_20d"] * 100.0 * 0.10
        - scored["risk_score"] * 0.18
        - pd.to_numeric(scored.get("tradability_penalty"), errors="coerce").fillna(0.0)
    ).clip(0.0, 100.0)
    scored["avoid_score"] = (
        (100.0 - scored["trend_score"]) * 0.48
        + scored["risk_score"] * 0.32
        + (1.0 - scored["prob_up_5d"]) * 100.0 * 0.16
        + (-scored.get("z_mom20", 0.0)).clip(lower=0.0) * 4.0
        + pd.to_numeric(scored.get("tradability_penalty"), errors="coerce").fillna(0.0) * 0.25
    ).clip(0.0, 100.0)

    return scored.sort_values(["recommendation_score", "trend_score"], ascending=False).reset_index(drop=True)


def _future_returns_by_bars(history_df: pd.DataFrame, horizon_bars: int) -> pd.DataFrame:
    if history_df is None or history_df.empty:
        return pd.DataFrame(columns=["trade_date", "ts_code", "ret_fwd_5d", "y_up_5d"])

    df = history_df[["trade_date", "ts_code", "close"]].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["trade_date", "ts_code", "close"]).sort_values(["ts_code", "trade_date"])
    if df.empty:
        return pd.DataFrame(columns=["trade_date", "ts_code", "ret_fwd_5d", "y_up_5d"])

    future_close = df.groupby("ts_code", sort=False)["close"].shift(-int(horizon_bars))
    ret = future_close / df["close"] - 1.0
    out = df[["trade_date", "ts_code"]].copy()
    out["ret_fwd_5d"] = ret
    out["y_up_5d"] = (ret > 0).astype(float)
    return out.dropna(subset=["ret_fwd_5d"]).reset_index(drop=True)


def _select_calibration_anchor_dates(
    history_df: pd.DataFrame,
    config: RecoConfig,
) -> list[pd.Timestamp]:
    if history_df is None or history_df.empty or int(config.probability_calibration_anchors) <= 0:
        return []

    dates = pd.to_datetime(history_df["trade_date"], errors="coerce").dropna().drop_duplicates().sort_values().reset_index(drop=True)
    if dates.empty:
        return []

    horizon = max(1, int(config.probability_calibration_horizon_bars))
    spacing = max(1, int(config.probability_calibration_spacing_bars))
    anchor_count = max(0, int(config.probability_calibration_anchors))
    latest_usable_idx = len(dates) - horizon - 1
    min_idx = max(int(config.min_rows_per_symbol), 120)
    if latest_usable_idx < min_idx:
        return []

    anchors: list[pd.Timestamp] = []
    idx = latest_usable_idx
    while idx >= min_idx and len(anchors) < anchor_count:
        anchors.append(pd.Timestamp(dates.iloc[idx]).normalize())
        idx -= spacing
    anchors.reverse()
    return anchors


def build_probability_calibration_frame(
    history_df: pd.DataFrame,
    config: RecoConfig | None = None,
) -> pd.DataFrame:
    config = config or RecoConfig()
    if history_df is None or history_df.empty:
        return pd.DataFrame()

    work = history_df.copy()
    work["trade_date"] = pd.to_datetime(work["trade_date"], errors="coerce")
    work = work.dropna(subset=["trade_date", "ts_code"])
    anchors = _select_calibration_anchor_dates(work, config)
    if not anchors:
        return pd.DataFrame()

    meta_columns = [
        "ts_code",
        "name",
        "industry",
        "list_date",
        "list_status",
        "turnover_rate",
        "volume_ratio",
        "total_mv",
        "circ_mv",
    ]
    available_meta_columns = [column for column in meta_columns if column in work.columns]
    latest_meta_base = pd.DataFrame()
    if len(available_meta_columns) > 1:
        latest_meta_base = (
            work.sort_values(["ts_code", "trade_date"])
            .drop_duplicates(subset=["ts_code"], keep="last")[available_meta_columns]
            .copy()
        )

    future_df = _future_returns_by_bars(work, int(config.probability_calibration_horizon_bars))
    if future_df.empty:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for anchor in anchors:
        anchor_text = anchor.strftime("%Y-%m-%d")
        anchor_config = RecoConfig(
            lookback_days=config.lookback_days,
            min_rows_per_symbol=config.min_rows_per_symbol,
            topn=config.topn,
            trade_date=anchor_text,
            min_close=config.min_close,
            min_avg_amount=config.min_avg_amount,
            min_listing_days=config.min_listing_days,
            industry_neutral_weight=config.industry_neutral_weight,
            probability_calibration_anchors=0,
            probability_calibration_spacing_bars=config.probability_calibration_spacing_bars,
            probability_calibration_horizon_bars=config.probability_calibration_horizon_bars,
            probability_calibration_bins=config.probability_calibration_bins,
            probability_calibration_min_samples=config.probability_calibration_min_samples,
            probability_calibration_blend=config.probability_calibration_blend,
            exclude_limit_up_from_uptrend=config.exclude_limit_up_from_uptrend,
            exclude_limit_down_from_uptrend=config.exclude_limit_down_from_uptrend,
            ensure_source_views=False,
        )
        scored = score_trend_candidates(
            work[work["trade_date"] <= anchor],
            anchor_config,
            latest_meta_df=work[work["trade_date"] == anchor][["ts_code"]].merge(
                latest_meta_base,
                on="ts_code",
                how="left",
            ) if not latest_meta_base.empty else pd.DataFrame(),
        )
        if scored.empty:
            continue
        scored = scored[[
            "trade_date",
            "ts_code",
            "trend_score",
            "risk_score",
            "prob_up_5d",
            "prob_up_20d",
            "recommendation_score",
            "avoid_score",
        ]].copy()
        scored["anchor_trade_date"] = anchor
        labels = future_df[future_df["trade_date"] == anchor][["ts_code", "ret_fwd_5d", "y_up_5d"]]
        merged = scored.merge(labels, on="ts_code", how="inner")
        if not merged.empty:
            frames.append(merged)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def apply_probability_calibration(
    scored_df: pd.DataFrame,
    calibration_df: pd.DataFrame,
    config: RecoConfig | None = None,
) -> tuple[pd.DataFrame, dict]:
    config = config or RecoConfig()
    if scored_df is None or scored_df.empty:
        return scored_df if scored_df is not None else pd.DataFrame(), {"enabled": False, "reason": "empty scored frame"}

    out = scored_df.copy()
    out["prob_up_5d_raw"] = pd.to_numeric(out.get("prob_up_5d"), errors="coerce")

    min_samples = max(1, int(config.probability_calibration_min_samples))
    if calibration_df is None or calibration_df.empty or len(calibration_df) < min_samples:
        out["prob_up_5d_calibrated"] = out["prob_up_5d_raw"]
        return out, {
            "enabled": False,
            "reason": "insufficient calibration samples",
            "sample_count": int(0 if calibration_df is None else len(calibration_df)),
            "min_samples": int(min_samples),
        }

    cal = calibration_df.copy()
    cal["recommendation_score"] = pd.to_numeric(cal.get("recommendation_score"), errors="coerce")
    cal["y_up_5d"] = pd.to_numeric(cal.get("y_up_5d"), errors="coerce")
    cal = cal.dropna(subset=["recommendation_score", "y_up_5d"])
    if len(cal) < min_samples:
        out["prob_up_5d_calibrated"] = out["prob_up_5d_raw"]
        return out, {
            "enabled": False,
            "reason": "insufficient clean calibration samples",
            "sample_count": int(len(cal)),
            "min_samples": int(min_samples),
        }

    bins = max(3, int(config.probability_calibration_bins))
    edges = np.linspace(0.0, 1.0, bins + 1)
    cal["_score_pct"] = cal["recommendation_score"].rank(pct=True, method="average")
    cal["_bin"] = pd.cut(cal["_score_pct"], bins=edges, labels=False, include_lowest=True)
    global_hit = float(cal["y_up_5d"].mean())
    bin_stats = cal.groupby("_bin", dropna=True)["y_up_5d"].agg(["count", "mean"]).reset_index()
    bin_hit = {int(row["_bin"]): float(row["mean"]) for _, row in bin_stats.iterrows() if pd.notna(row["_bin"])}
    bin_count = {int(row["_bin"]): int(row["count"]) for _, row in bin_stats.iterrows() if pd.notna(row["_bin"])}

    out["_score_pct"] = pd.to_numeric(out.get("recommendation_score"), errors="coerce").rank(pct=True, method="average")
    out["_calibration_bin"] = pd.cut(out["_score_pct"], bins=edges, labels=False, include_lowest=True)
    empirical = out["_calibration_bin"].map(lambda x: bin_hit.get(int(x), global_hit) if pd.notna(x) else global_hit)
    sample_counts = out["_calibration_bin"].map(lambda x: bin_count.get(int(x), 0) if pd.notna(x) else 0)

    blend = _clip(float(config.probability_calibration_blend), 0.0, 1.0)
    sample_weight = (pd.to_numeric(sample_counts, errors="coerce").fillna(0.0) / float(min_samples)).clip(0.0, 1.0)
    effective_blend = blend * sample_weight
    out["prob_up_5d_calibrated"] = (
        out["prob_up_5d_raw"].fillna(global_hit) * (1.0 - effective_blend)
        + pd.to_numeric(empirical, errors="coerce").fillna(global_hit) * effective_blend
    ).clip(0.05, 0.95)
    out["prob_up_5d"] = out["prob_up_5d_calibrated"]
    out["recommendation_score"] = (
        out["trend_score"] * 0.58
        + out["prob_up_5d"] * 100.0 * 0.24
        + out["prob_up_20d"] * 100.0 * 0.10
        - out["risk_score"] * 0.18
        - pd.to_numeric(out.get("tradability_penalty"), errors="coerce").fillna(0.0)
    ).clip(0.0, 100.0)
    out["avoid_score"] = (
        (100.0 - out["trend_score"]) * 0.48
        + out["risk_score"] * 0.32
        + (1.0 - out["prob_up_5d"]) * 100.0 * 0.16
        + (-out.get("z_mom20", 0.0)).clip(lower=0.0) * 4.0
        + pd.to_numeric(out.get("tradability_penalty"), errors="coerce").fillna(0.0) * 0.25
    ).clip(0.0, 100.0)
    out = out.drop(columns=[c for c in ["_score_pct"] if c in out.columns])

    anchors = sorted(pd.to_datetime(calibration_df.get("anchor_trade_date"), errors="coerce").dropna().dt.strftime("%Y-%m-%d").unique().tolist()) if "anchor_trade_date" in calibration_df.columns else []
    return out.sort_values(["recommendation_score", "trend_score"], ascending=False).reset_index(drop=True), {
        "enabled": True,
        "method": "recent-anchor empirical hit-rate calibration",
        "sample_count": int(len(cal)),
        "anchors": anchors,
        "horizon_bars": int(config.probability_calibration_horizon_bars),
        "bins": int(bins),
        "global_hit_rate": float(global_hit),
        "bin_hit_rates": {str(k): bin_hit[k] for k in sorted(bin_hit)},
        "bin_counts": {str(k): bin_count[k] for k in sorted(bin_count)},
        "blend": float(blend),
    }


def _public_record(row: pd.Series, rank: int, mode: str) -> dict:
    return {
        "rank": int(rank),
        "ts_code": str(row.get("ts_code") or ""),
        "name": str(row.get("name") or ""),
        "industry": str(row.get("industry") or "-"),
        "close": round(_to_float(row.get("close"), 0.0), 4),
        "trend_score": round(_to_float(row.get("trend_score"), 0.0), 2),
        "risk_score": round(_to_float(row.get("risk_score"), 0.0), 2),
        "prob_up_5d": round(_to_float(row.get("prob_up_5d"), 0.0), 6),
        "prob_up_20d": round(_to_float(row.get("prob_up_20d"), 0.0), 6),
        "prob_up_5d_raw": round(_to_float(row.get("prob_up_5d_raw", row.get("prob_up_5d")), 0.0), 6),
        "recommendation_score": round(_to_float(row.get("recommendation_score"), 0.0), 2),
        "avoid_score": round(_to_float(row.get("avoid_score"), 0.0), 2),
        "pct_chg": round(_to_float(row.get("pct_chg"), 0.0), 4),
        "is_limit_up_like": bool(row.get("is_limit_up_like", False)),
        "is_limit_down_like": bool(row.get("is_limit_down_like", False)),
        "reason": _build_reason(row, mode),
    }


def build_recommendation_payload(
    scored_df: pd.DataFrame,
    *,
    trade_date: str,
    config: RecoConfig | None = None,
    calibration_meta: dict | None = None,
) -> dict:
    config = config or RecoConfig()
    if scored_df is None:
        scored_df = pd.DataFrame()

    topn = max(1, int(config.topn))
    up_pool = scored_df.copy()
    avoid_pool = scored_df.copy()

    if not up_pool.empty:
        up_pool = up_pool[
            (pd.to_numeric(up_pool["prob_up_5d"], errors="coerce") >= 0.50)
            & (pd.to_numeric(up_pool["risk_score"], errors="coerce") <= 78.0)
        ].copy()
        if config.exclude_limit_up_from_uptrend and "is_limit_up_like" in up_pool.columns:
            up_pool = up_pool[~up_pool["is_limit_up_like"].astype(bool)].copy()
        if config.exclude_limit_down_from_uptrend and "is_limit_down_like" in up_pool.columns:
            up_pool = up_pool[~up_pool["is_limit_down_like"].astype(bool)].copy()
        if up_pool.empty:
            up_pool = scored_df.copy()
        up_pool = up_pool.sort_values(["recommendation_score", "trend_score"], ascending=False)

    if not avoid_pool.empty:
        avoid_pool = avoid_pool.sort_values(["avoid_score", "risk_score"], ascending=False)

    top_uptrend = [
        _public_record(row, idx, "up")
        for idx, (_, row) in enumerate(up_pool.head(topn).iterrows(), start=1)
    ]
    top_avoid = [
        _public_record(row, idx, "avoid")
        for idx, (_, row) in enumerate(avoid_pool.head(topn).iterrows(), start=1)
    ]

    return {
        "trade_date": str(trade_date),
        "generated_at": _utcnow_iso(),
        "universe_size": int(len(scored_df)),
        "algorithm": {
            "name": ALGORITHM_NAME,
            "version": ALGORITHM_VERSION,
            "description": "多因子趋势-风险调整排序：动量、均线结构、趋势斜率质量、突破位置、成交额确认、流动性、可成交性过滤与回撤/波动/过热风险，并做行业内标准化和近期历史胜率校准。",
            "positive_factors": ["20/60/120日动量", "均线多头结构", "20/60日趋势斜率与拟合度", "接近阶段新高", "成交额/量比确认", "流动性"],
            "risk_factors": ["20/60日波动", "下行波动", "60日最大回撤", "短线过热", "异常换手", "涨停/跌停类可成交性惩罚"],
            "ranking": "recommendation_score = trend_score/calibrated_probability/risk/tradability 的风险调整综合分；avoid_score = 弱趋势、低概率、高风险与可成交性压力的综合分。",
            "calibration": calibration_meta or {"enabled": False},
        },
        "config": {
            "lookback_days": int(config.lookback_days),
            "min_rows_per_symbol": int(config.min_rows_per_symbol),
            "topn": int(config.topn),
            "min_close": float(config.min_close),
            "min_avg_amount": float(config.min_avg_amount),
            "min_listing_days": int(config.min_listing_days),
            "industry_neutral_weight": float(config.industry_neutral_weight),
            "probability_calibration_anchors": int(config.probability_calibration_anchors),
            "probability_calibration_spacing_bars": int(config.probability_calibration_spacing_bars),
            "probability_calibration_horizon_bars": int(config.probability_calibration_horizon_bars),
            "probability_calibration_bins": int(config.probability_calibration_bins),
            "probability_calibration_min_samples": int(config.probability_calibration_min_samples),
            "probability_calibration_blend": float(config.probability_calibration_blend),
            "exclude_limit_up_from_uptrend": bool(config.exclude_limit_up_from_uptrend),
            "exclude_limit_down_from_uptrend": bool(config.exclude_limit_down_from_uptrend),
        },
        "top_uptrend": top_uptrend,
        "top_avoid": top_avoid,
    }


def _get_engine() -> Engine:
    from src.trend_reco_store import get_engine

    return get_engine()


def _ensure_source_views(engine: Engine) -> None:
    try:
        from src.sync_tushare_security_data import ensure_storage_objects

        ensure_storage_objects(engine)
    except Exception as exc:
        logger.warning("ensure source views failed, continue with existing objects: %s", exc)


def _read_sql_df(engine: Engine, sql: str, params: dict[str, object] | None = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def _resolve_effective_trade_date(engine: Engine, requested_trade_date: str = "") -> str:
    requested = pd.to_datetime(str(requested_trade_date or ""), errors="coerce")
    if pd.notna(requested):
        sql = """
        SELECT MAX(trade_date) AS trade_date
        FROM vw_ts_stock_daily
        WHERE trade_date <= :trade_date
          AND close IS NOT NULL
        """
        params = {"trade_date": requested.strftime("%Y-%m-%d")}
    else:
        sql = """
        SELECT MAX(trade_date) AS trade_date
        FROM vw_ts_stock_daily
        WHERE close IS NOT NULL
        """
        params = {}

    df = _read_sql_df(engine, sql, params)
    if df.empty:
        return ""
    value = pd.to_datetime(df.iloc[0].get("trade_date"), errors="coerce")
    if pd.isna(value):
        return ""
    return value.strftime("%Y-%m-%d")


def load_history_frame(engine: Engine, trade_date: str, config: RecoConfig | None = None) -> pd.DataFrame:
    config = config or RecoConfig()
    trade_ts = pd.to_datetime(trade_date, errors="coerce")
    if pd.isna(trade_ts):
        return pd.DataFrame()

    calendar_buffer_days = max(int(config.lookback_days) * 3, 260)
    start_date = (trade_ts - pd.Timedelta(days=calendar_buffer_days)).strftime("%Y-%m-%d")
    sql = """
    SELECT
        d.trade_date,
        d.ts_code,
        d.high,
        d.low,
        d.close,
        d.pct_chg,
        d.vol,
        d.amount
    FROM vw_ts_stock_daily d
    WHERE d.trade_date >= :start_date
      AND d.trade_date <= :trade_date
      AND d.close IS NOT NULL
    ORDER BY d.ts_code, d.trade_date
    """
    frame = _read_sql_df(
        engine,
        sql,
        {
            "start_date": start_date,
            "trade_date": trade_ts.strftime("%Y-%m-%d"),
        },
    )
    if frame.empty:
        return frame
    frame["ts_code"] = frame["ts_code"].astype("category")
    return _downcast_float_columns(frame, ["high", "low", "close", "pct_chg", "vol", "amount"])


def load_latest_meta_frame(engine: Engine, trade_date: str) -> pd.DataFrame:
    trade_ts = pd.to_datetime(trade_date, errors="coerce")
    if pd.isna(trade_ts):
        return pd.DataFrame()

    sql = """
    SELECT
        d.ts_code,
        b.name,
        b.industry,
        b.list_date,
        b.list_status,
        db.turnover_rate,
        db.volume_ratio,
        db.total_mv,
        db.circ_mv
    FROM vw_ts_stock_daily d
    JOIN vw_ts_stock_basic b
      ON b.ts_code = d.ts_code
    LEFT JOIN vw_ts_stock_daily_basic db
      ON db.ts_code = d.ts_code
     AND db.trade_date = d.trade_date
    WHERE d.trade_date = :trade_date
      AND d.close IS NOT NULL
    ORDER BY d.ts_code
    """
    frame = _read_sql_df(
        engine,
        sql,
        {"trade_date": trade_ts.strftime("%Y-%m-%d")},
    )
    if frame.empty:
        return frame
    return _downcast_float_columns(frame, ["turnover_rate", "volume_ratio", "total_mv", "circ_mv"])


def generate_daily_trend_recommendations(config: RecoConfig | None = None, engine: Engine | None = None) -> dict:
    config = config or RecoConfig()
    engine = engine or _get_engine()
    if config.ensure_source_views:
        _ensure_source_views(engine)

    trade_date = _resolve_effective_trade_date(engine, config.trade_date)
    if not trade_date:
        raise RuntimeError("无法从 vw_ts_stock_daily 解析有效交易日")

    history_df = load_history_frame(engine, trade_date, config)
    latest_meta_df = load_latest_meta_frame(engine, trade_date)
    scored_df = score_trend_candidates(history_df, config, latest_meta_df=latest_meta_df)
    if scored_df.empty:
        raise RuntimeError(f"{trade_date} 没有满足条件的趋势推荐样本")

    calibration_meta: dict = {"enabled": False}
    if int(config.probability_calibration_anchors) > 0:
        calibration_df = build_probability_calibration_frame(history_df, config)
        scored_df, calibration_meta = apply_probability_calibration(scored_df, calibration_df, config)

    return build_recommendation_payload(
        scored_df,
        trade_date=trade_date,
        config=config,
        calibration_meta=calibration_meta,
    )


def save_recommendations(payload: dict, output_dir: str | os.PathLike = "data/recommendations") -> tuple[Path, Path]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    trade_date = str(payload.get("trade_date") or datetime.now().strftime("%Y-%m-%d"))
    daily_file = out_dir / f"{trade_date}_trend_recommendations.json"
    latest_file = out_dir / "latest_trend_recommendations.json"

    for path in (daily_file, latest_file):
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
    return daily_file, latest_file
