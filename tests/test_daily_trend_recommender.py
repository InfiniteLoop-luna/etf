from __future__ import annotations

import numpy as np
import pandas as pd

from src.daily_trend_recommender import (
    ALGORITHM_NAME,
    RecoConfig,
    apply_probability_calibration,
    build_recommendation_payload,
    score_trend_candidates,
)


def _symbol_history(
    ts_code: str,
    name: str,
    industry: str,
    close_values: np.ndarray,
    *,
    amount: float = 100_000.0,
) -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-01", periods=len(close_values))
    close = pd.Series(close_values, index=dates)
    return pd.DataFrame(
        {
            "trade_date": dates,
            "ts_code": ts_code,
            "open": close * 0.995,
            "high": close * 1.015,
            "low": close * 0.985,
            "close": close,
            "pre_close": close.shift(1),
            "pct_chg": close.pct_change(fill_method=None) * 100.0,
            "vol": 1_000_000.0,
            "amount": amount,
            "name": name,
            "industry": industry,
            "market": "主板",
            "exchange": "SSE",
            "list_date": "2020-01-01",
            "list_status": "L",
            "turnover_rate": 2.0,
            "turnover_rate_f": 2.5,
            "volume_ratio": 1.2,
            "total_mv": 10_000_000.0,
            "circ_mv": 8_000_000.0,
        }
    )


def test_professional_trend_ranker_orders_strong_and_avoid_candidates():
    bars = 140
    x = np.arange(bars)
    strong = 18.0 * np.exp(0.0045 * x) * (1.0 + 0.01 * np.sin(x / 5.0))
    weak = 32.0 * np.exp(-0.0042 * x) * (1.0 + 0.035 * np.sin(x / 4.0))
    flat = 20.0 * (1.0 + 0.01 * np.sin(x / 6.0))
    recovering = 14.0 * np.exp(0.0018 * x) * (1.0 + 0.02 * np.sin(x / 7.0))

    history = pd.concat(
        [
            _symbol_history("600001.SH", "强趋势", "科技", strong),
            _symbol_history("600002.SH", "弱趋势", "地产", weak),
            _symbol_history("600003.SH", "横盘", "消费", flat),
            _symbol_history("600004.SH", "修复", "制造", recovering),
        ],
        ignore_index=True,
    )
    config = RecoConfig(
        min_rows_per_symbol=60,
        min_avg_amount=0.0,
        min_close=1.0,
        min_listing_days=0,
        topn=2,
    )

    scored = score_trend_candidates(history, config)
    payload = build_recommendation_payload(
        scored,
        trade_date=str(history["trade_date"].max().date()),
        config=config,
    )

    assert payload["algorithm"]["name"] == ALGORITHM_NAME
    assert payload["universe_size"] == 4
    assert payload["top_uptrend"][0]["ts_code"] == "600001.SH"
    assert payload["top_avoid"][0]["ts_code"] == "600002.SH"
    assert payload["top_uptrend"][0]["trend_score"] > payload["top_avoid"][0]["trend_score"]
    assert payload["top_avoid"][0]["risk_score"] >= payload["top_uptrend"][0]["risk_score"]


def test_recommender_filters_st_and_low_history_symbols():
    bars = 80
    x = np.arange(bars)
    clean = 10.0 * np.exp(0.003 * x)
    st_stock = 11.0 * np.exp(0.006 * x)
    too_short = 9.0 * np.exp(0.006 * np.arange(30))

    history = pd.concat(
        [
            _symbol_history("600010.SH", "正常股票", "科技", clean),
            _symbol_history("600011.SH", "ST风险", "科技", st_stock),
            _symbol_history("600012.SH", "样本不足", "科技", too_short),
        ],
        ignore_index=True,
    )
    config = RecoConfig(
        min_rows_per_symbol=60,
        min_avg_amount=0.0,
        min_close=1.0,
        min_listing_days=0,
    )

    scored = score_trend_candidates(history, config)

    assert scored["ts_code"].tolist() == ["600010.SH"]


def test_probability_calibration_blends_recent_empirical_hit_rates():
    scored = pd.DataFrame(
        {
            "ts_code": ["A", "B", "C"],
            "name": ["A", "B", "C"],
            "industry": ["I", "I", "I"],
            "close": [10.0, 10.0, 10.0],
            "trend_score": [80.0, 50.0, 20.0],
            "risk_score": [30.0, 50.0, 70.0],
            "prob_up_5d": [0.50, 0.50, 0.50],
            "prob_up_20d": [0.50, 0.50, 0.50],
            "recommendation_score": [90.0, 50.0, 10.0],
            "avoid_score": [10.0, 50.0, 90.0],
            "tradability_penalty": [0.0, 0.0, 0.0],
            "z_mom20": [1.0, 0.0, -1.0],
        }
    )
    calibration = pd.DataFrame(
        {
            "recommendation_score": [95.0, 90.0, 55.0, 45.0, 15.0, 5.0],
            "y_up_5d": [1.0, 1.0, 1.0, 0.0, 0.0, 0.0],
            "anchor_trade_date": pd.to_datetime(["2025-01-01"] * 6),
        }
    )
    config = RecoConfig(
        probability_calibration_min_samples=2,
        probability_calibration_bins=3,
        probability_calibration_blend=0.50,
    )

    calibrated, meta = apply_probability_calibration(scored, calibration, config)

    assert meta["enabled"]
    assert calibrated.loc[calibrated["ts_code"] == "A", "prob_up_5d"].iloc[0] > 0.50
    assert calibrated.loc[calibrated["ts_code"] == "C", "prob_up_5d"].iloc[0] < 0.50
    assert "prob_up_5d_raw" in calibrated.columns


def test_uptrend_payload_excludes_limit_up_like_candidates():
    scored = pd.DataFrame(
        {
            "ts_code": ["600001.SH", "600002.SH"],
            "name": ["limit", "tradable"],
            "industry": ["I", "I"],
            "close": [10.0, 10.0],
            "trend_score": [95.0, 80.0],
            "risk_score": [20.0, 25.0],
            "prob_up_5d": [0.90, 0.70],
            "prob_up_20d": [0.80, 0.70],
            "recommendation_score": [95.0, 75.0],
            "avoid_score": [5.0, 15.0],
            "is_limit_up_like": [True, False],
            "is_limit_down_like": [False, False],
        }
    )
    config = RecoConfig(topn=1, exclude_limit_up_from_uptrend=True)

    payload = build_recommendation_payload(scored, trade_date="2026-01-01", config=config)

    assert payload["top_uptrend"][0]["ts_code"] == "600002.SH"


def test_score_trend_candidates_supports_separate_latest_meta_frame():
    bars = 140
    x = np.arange(bars)
    strong = 18.0 * np.exp(0.0045 * x) * (1.0 + 0.01 * np.sin(x / 5.0))
    history = _symbol_history("600001.SH", "强趋势", "科技", strong)
    history = history[["trade_date", "ts_code", "high", "low", "close", "pct_chg", "vol", "amount"]].copy()
    latest_meta = pd.DataFrame(
        {
            "ts_code": ["600001.SH"],
            "name": ["强趋势"],
            "industry": ["科技"],
            "list_date": ["2020-01-01"],
            "list_status": ["L"],
            "turnover_rate": [2.0],
            "volume_ratio": [1.2],
            "total_mv": [10_000_000.0],
            "circ_mv": [8_000_000.0],
        }
    )
    config = RecoConfig(
        min_rows_per_symbol=60,
        min_avg_amount=0.0,
        min_close=1.0,
        min_listing_days=0,
        topn=1,
    )

    scored = score_trend_candidates(history, config, latest_meta_df=latest_meta)

    assert len(scored) == 1
    assert scored.iloc[0]["ts_code"] == "600001.SH"
    assert scored.iloc[0]["name"] == "强趋势"
    assert scored.iloc[0]["industry"] == "科技"
