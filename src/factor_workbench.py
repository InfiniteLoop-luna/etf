from __future__ import annotations

from copy import deepcopy
from datetime import timedelta

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.alpha_191 import add_alpha095_family
from src.ml_stock_dataset import get_engine as get_dataset_engine


FACTOR_WORKBENCH_PAGE_LABEL = "🧠 因子选股工作台"


_FACTOR_CATALOG = [
    {"key": "alpha095_cv", "label": "Alpha095 CV", "group": "Alpha", "higher_better": True, "source": "vw_ts_stock_daily.amount", "description": "成交额20日波动率的去量纲版本。"},
    {"key": "alpha095_logstd", "label": "Alpha095 LogSTD", "group": "Alpha", "higher_better": True, "source": "vw_ts_stock_daily.amount", "description": "对数成交额20日波动率。"},
    {"key": "alpha095_pctstd", "label": "Alpha095 PctSTD", "group": "Alpha", "higher_better": True, "source": "vw_ts_stock_daily.amount", "description": "成交额变化率20日波动率。"},
    {"key": "amount_ma5_ratio", "label": "成交额/5日均额", "group": "流动性", "higher_better": True, "source": "ml_stock_feature_daily", "description": "衡量放量强度。"},
    {"key": "vol_ma5_ratio", "label": "成交量/5日均量", "group": "流动性", "higher_better": True, "source": "ml_stock_feature_daily", "description": "衡量量能偏离。"},
    {"key": "turnover_rate", "label": "换手率", "group": "流动性", "higher_better": True, "source": "ml_stock_feature_daily", "description": "日换手率。"},
    {"key": "close_over_ma20", "label": "收盘/20日均线", "group": "技术", "higher_better": True, "source": "ml_stock_feature_daily", "description": "价格是否站上中期均线。"},
    {"key": "ma5_over_ma20", "label": "5日均线/20日均线", "group": "技术", "higher_better": True, "source": "ml_stock_feature_daily", "description": "短期趋势相对中期趋势。"},
    {"key": "w_ema5_over_30", "label": "周EMA5/EMA30", "group": "技术", "higher_better": True, "source": "ml_stock_feature_daily", "description": "周线趋势结构。"},
    {"key": "m_ema5_over_30", "label": "月EMA5/EMA30", "group": "技术", "higher_better": True, "source": "ml_stock_feature_daily", "description": "月线趋势结构。"},
    {"key": "volatility_20d", "label": "20日波动率", "group": "技术", "higher_better": False, "source": "ml_stock_feature_daily", "description": "短期波动率，越低越稳。"},
    {"key": "net_mf_amount", "label": "主力净流入额", "group": "资金流", "higher_better": True, "source": "vw_moneyflow", "description": "标准口径主力净流入金额。"},
    {"key": "net_mf_amount_rate", "label": "主力净流入/成交额", "group": "资金流", "higher_better": True, "source": "vw_moneyflow + ml_stock_feature_daily", "description": "按成交额归一化后的主力流入强度。"},
    {"key": "ths_net_amount", "label": "THS净流入", "group": "资金流", "higher_better": True, "source": "vw_moneyflow_ths", "description": "同花顺口径净流入。"},
    {"key": "ths_net_d5_amount", "label": "THS近5日净流入", "group": "资金流", "higher_better": True, "source": "vw_moneyflow_ths", "description": "近5日资金持续性。"},
    {"key": "dc_net_amount", "label": "DC净流入", "group": "资金流", "higher_better": True, "source": "vw_moneyflow_dc", "description": "东方财富口径净流入。"},
    {"key": "dc_net_amount_rate", "label": "DC净流入占比", "group": "资金流", "higher_better": True, "source": "vw_moneyflow_dc", "description": "东方财富口径净流入占比。"},
    {"key": "pe_ttm", "label": "PE(TTM)", "group": "估值", "higher_better": False, "source": "ml_stock_feature_daily", "description": "滚动市盈率，越低越便宜。"},
    {"key": "pb", "label": "PB", "group": "估值", "higher_better": False, "source": "ml_stock_feature_daily", "description": "市净率，越低越便宜。"},
    {"key": "ps_ttm", "label": "PS(TTM)", "group": "估值", "higher_better": False, "source": "ml_stock_feature_daily", "description": "滚动市销率，越低越便宜。"},
    {"key": "dv_ratio", "label": "股息率", "group": "估值", "higher_better": True, "source": "ml_stock_feature_daily", "description": "股息率，越高越有防守性。"},
    {"key": "roe", "label": "ROE", "group": "财务", "higher_better": True, "source": "vw_ts_stock_fina_indicator", "description": "净资产收益率。"},
    {"key": "roa", "label": "ROA", "group": "财务", "higher_better": True, "source": "vw_ts_stock_fina_indicator", "description": "总资产收益率。"},
    {"key": "grossprofit_margin", "label": "毛利率", "group": "财务", "higher_better": True, "source": "vw_ts_stock_fina_indicator", "description": "毛利率越高，盈利质量通常越好。"},
    {"key": "current_ratio", "label": "流动比率", "group": "财务", "higher_better": True, "source": "vw_ts_stock_fina_indicator", "description": "短期偿债能力。"},
    {"key": "ocfps", "label": "每股经营现金流", "group": "财务", "higher_better": True, "source": "vw_ts_stock_fina_indicator", "description": "经营现金质量。"},
    {"key": "debt_to_assets", "label": "资产负债率", "group": "财务", "higher_better": False, "source": "vw_ts_stock_fina_indicator", "description": "杠杆水平，越低越稳健。"},
]


_FACTOR_MAP = {item["key"]: item for item in _FACTOR_CATALOG}


_SCORE_PRESETS = {
    "均衡打分": {
        "description": "质量、趋势、估值、资金流均衡配置。",
        "factor_weights": {
            "alpha095_cv": 0.8,
            "amount_ma5_ratio": 0.8,
            "close_over_ma20": 1.0,
            "w_ema5_over_30": 1.0,
            "net_mf_amount_rate": 1.0,
            "dc_net_amount_rate": 0.8,
            "pe_ttm": 0.7,
            "pb": 0.7,
            "roe": 1.0,
            "grossprofit_margin": 0.9,
        },
    },
    "趋势动量": {
        "description": "偏趋势和量价共振。",
        "factor_weights": {
            "alpha095_cv": 0.8,
            "amount_ma5_ratio": 1.0,
            "close_over_ma20": 1.1,
            "ma5_over_ma20": 1.1,
            "w_ema5_over_30": 1.0,
            "m_ema5_over_30": 0.8,
            "net_mf_amount_rate": 0.8,
            "ths_net_d5_amount": 0.8,
        },
    },
    "质量价值": {
        "description": "偏估值和财务质量。",
        "factor_weights": {
            "pe_ttm": 1.0,
            "pb": 1.0,
            "ps_ttm": 0.8,
            "dv_ratio": 0.6,
            "roe": 1.1,
            "roa": 0.8,
            "grossprofit_margin": 1.0,
            "current_ratio": 0.6,
            "debt_to_assets": 0.8,
        },
    },
    "资金驱动": {
        "description": "偏主力和强资金流。",
        "factor_weights": {
            "net_mf_amount": 0.8,
            "net_mf_amount_rate": 1.2,
            "ths_net_amount": 1.0,
            "ths_net_d5_amount": 1.0,
            "dc_net_amount": 0.8,
            "dc_net_amount_rate": 1.2,
            "amount_ma5_ratio": 0.6,
            "close_over_ma20": 0.6,
        },
    },
    "自定义": {
        "description": "手动调整因子权重。",
        "factor_weights": {
            "alpha095_cv": 1.0,
            "amount_ma5_ratio": 1.0,
            "close_over_ma20": 1.0,
            "w_ema5_over_30": 1.0,
            "net_mf_amount_rate": 1.0,
            "dc_net_amount_rate": 1.0,
            "pe_ttm": 1.0,
            "pb": 1.0,
            "roe": 1.0,
            "grossprofit_margin": 1.0,
        },
    },
}


def _get_engine(engine: Engine | None = None) -> Engine:
    return engine or get_dataset_engine()


def get_factor_catalog() -> list[dict]:
    return [dict(item) for item in _FACTOR_CATALOG]


def get_score_preset(name: str) -> dict:
    preset = _SCORE_PRESETS.get(name) or _SCORE_PRESETS["均衡打分"]
    return deepcopy({"name": name if name in _SCORE_PRESETS else "均衡打分", **preset})


def _coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _normalized_percentile(series: pd.Series, higher_better: bool) -> pd.Series:
    numeric = _coerce_numeric(series)
    valid = numeric.notna()
    result = pd.Series(np.full(len(numeric), 0.5, dtype=float), index=series.index, dtype=float)
    if not valid.any():
        return result

    ranked = numeric[valid].rank(method="average", pct=True, ascending=higher_better)
    result.loc[valid] = ranked.astype(float)
    return result


def apply_factor_filters(df: pd.DataFrame, filters: dict | None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=[] if df is None else df.columns)

    filters = filters or {}
    filtered = df.copy()
    mask = pd.Series(True, index=filtered.index, dtype=bool)

    markets = [item for item in (filters.get("markets") or []) if str(item).strip()]
    if markets:
        mask &= filtered.get("market", pd.Series(index=filtered.index, dtype=object)).isin(markets)

    industries = [item for item in (filters.get("industries") or []) if str(item).strip()]
    if industries:
        industry_series = filtered.get("industry", pd.Series(index=filtered.index, dtype=object)).fillna("").astype(str)
        mask &= industry_series.isin(industries)

    if bool(filters.get("exclude_historical_st")) and "has_ever_st" in filtered.columns:
        mask &= ~filtered["has_ever_st"].fillna(False).astype(bool)

    if bool(filters.get("require_is_hs")) and "is_hs" in filtered.columns:
        is_hs = filtered["is_hs"].fillna("").astype(str).str.strip()
        mask &= is_hs.ne("")

    numeric_rules = [
        ("min_turnover_rate_enabled", "turnover_rate", ">=", "min_turnover_rate"),
        ("min_amount_enabled", "amount", ">=", "min_amount"),
        ("min_total_mv_enabled", "total_mv", ">=", "min_total_mv"),
        ("max_total_mv_enabled", "total_mv", "<=", "max_total_mv"),
        ("min_close_over_ma20_enabled", "close_over_ma20", ">=", "min_close_over_ma20"),
        ("min_ma5_over_ma20_enabled", "ma5_over_ma20", ">=", "min_ma5_over_ma20"),
        ("min_w_ema5_over_30_enabled", "w_ema5_over_30", ">=", "min_w_ema5_over_30"),
        ("min_net_mf_amount_enabled", "net_mf_amount", ">=", "min_net_mf_amount"),
        ("min_net_mf_amount_rate_enabled", "net_mf_amount_rate", ">=", "min_net_mf_amount_rate"),
        ("min_dc_net_amount_rate_enabled", "dc_net_amount_rate", ">=", "min_dc_net_amount_rate"),
        ("min_pe_ttm_enabled", "pe_ttm", ">=", "min_pe_ttm"),
        ("max_pe_ttm_enabled", "pe_ttm", "<=", "max_pe_ttm"),
        ("max_pb_enabled", "pb", "<=", "max_pb"),
        ("min_roe_enabled", "roe", ">=", "min_roe"),
        ("min_grossprofit_margin_enabled", "grossprofit_margin", ">=", "min_grossprofit_margin"),
        ("max_debt_to_assets_enabled", "debt_to_assets", "<=", "max_debt_to_assets"),
        ("min_current_ratio_enabled", "current_ratio", ">=", "min_current_ratio"),
    ]

    for enabled_key, column, operator, value_key in numeric_rules:
        if not bool(filters.get(enabled_key)):
            continue
        if column not in filtered.columns:
            continue
        threshold = filters.get(value_key)
        if threshold is None:
            continue
        values = _coerce_numeric(filtered[column])
        if operator == ">=":
            mask &= values.ge(float(threshold))
        else:
            mask &= values.le(float(threshold))

    return filtered.loc[mask].copy()


def compute_factor_scores(
    df: pd.DataFrame,
    factor_weights: dict[str, float],
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=[] if df is None else list(df.columns) + ["final_score", "score_missing_count"])

    selected_weights = {
        key: float(weight)
        for key, weight in (factor_weights or {}).items()
        if float(weight) > 0 and key in df.columns
    }
    if not selected_weights:
        raise ValueError("At least one positive factor weight is required.")

    scored = df.copy()
    weighted_sum = pd.Series(0.0, index=scored.index, dtype=float)
    weight_sum = 0.0
    missing_count = pd.Series(0, index=scored.index, dtype=int)

    group_weighted_sum: dict[str, pd.Series] = {}
    group_weight_sum: dict[str, float] = {}

    for factor_key, weight in selected_weights.items():
        meta = _FACTOR_MAP.get(factor_key, {"higher_better": True, "group": "其他"})
        raw_values = scored[factor_key]
        percentiles = _normalized_percentile(raw_values, higher_better=bool(meta.get("higher_better", True)))
        score_col = f"score_{factor_key}"
        scored[score_col] = percentiles

        weighted_sum = weighted_sum.add(percentiles * weight, fill_value=0.0)
        weight_sum += weight
        missing_count = missing_count.add(raw_values.isna().astype(int), fill_value=0).astype(int)

        group = str(meta.get("group") or "其他")
        group_weighted_sum[group] = group_weighted_sum.get(group, pd.Series(0.0, index=scored.index, dtype=float)).add(
            percentiles * weight,
            fill_value=0.0,
        )
        group_weight_sum[group] = group_weight_sum.get(group, 0.0) + weight

    scored["score_missing_count"] = missing_count
    scored["final_score"] = weighted_sum / weight_sum * 100.0

    for group, group_sum in group_weighted_sum.items():
        safe_group_name = group.lower().replace(" ", "_")
        scored[f"score_group_{safe_group_name}"] = group_sum / group_weight_sum[group] * 100.0

    sort_columns = ["final_score"]
    ascending = [False]
    if "ts_code" in scored.columns:
        sort_columns.append("ts_code")
        ascending.append(True)

    return scored.sort_values(sort_columns, ascending=ascending, kind="mergesort").reset_index(drop=True)


def get_factor_workbench_trade_dates(engine: Engine | None = None, limit: int = 60) -> list[pd.Timestamp]:
    sql = text(
        """
        SELECT DISTINCT trade_date
        FROM ml_stock_feature_daily
        WHERE trade_date IS NOT NULL
        ORDER BY trade_date DESC
        LIMIT :limit
        """
    )
    with _get_engine(engine).connect() as conn:
        df = pd.read_sql(sql, conn, params={"limit": int(limit)})
    if df.empty:
        return []
    return pd.to_datetime(df["trade_date"], errors="coerce").dropna().tolist()


def get_factor_workbench_data_freshness(engine: Engine | None = None) -> dict[str, str | None]:
    sql = text(
        """
        SELECT
            (SELECT MAX(trade_date)::text FROM ml_stock_feature_daily) AS feature_date,
            (SELECT MAX(trade_date)::text FROM vw_moneyflow) AS moneyflow_date,
            (SELECT MAX(trade_date)::text FROM vw_moneyflow_ths) AS moneyflow_ths_date,
            (SELECT MAX(trade_date)::text FROM vw_moneyflow_dc) AS moneyflow_dc_date,
            (SELECT MAX(trade_date)::text FROM ts_stock_technical_signals) AS tech_signal_date,
            (SELECT MAX(end_date)::text FROM vw_ts_stock_fina_indicator) AS fina_end_date,
            (SELECT MAX(ann_date)::text FROM vw_ts_stock_fina_indicator) AS fina_ann_date
        """
    )
    with _get_engine(engine).connect() as conn:
        row = conn.execute(sql).mappings().one()
    return dict(row)


def load_factor_workbench_base_df(trade_date, engine: Engine | None = None) -> pd.DataFrame:
    trade_ts = pd.Timestamp(trade_date)
    sql = text(
        """
        WITH latest_fina AS (
            SELECT DISTINCT ON (ts_code)
                ts_code,
                ann_date,
                end_date,
                roe,
                roa,
                grossprofit_margin,
                current_ratio,
                ocfps,
                debt_to_assets
            FROM vw_ts_stock_fina_indicator
            WHERE ts_code IS NOT NULL
            ORDER BY ts_code, end_date DESC NULLS LAST, ann_date DESC NULLS LAST
        )
        SELECT
            f.trade_date,
            f.ts_code,
            b.name,
            b.industry,
            b.market,
            COALESCE(c.exchange, b.exchange) AS exchange,
            b.is_hs,
            u.list_date,
            u.listing_days,
            u.has_ever_st,
            u.is_current_st,
            f.close,
            f.amount,
            f.turnover_rate,
            f.volume_ratio,
            f.total_mv,
            f.circ_mv,
            f.amount_ma5_ratio,
            f.vol_ma5_ratio,
            f.close_over_ma20,
            f.ma5_over_ma20,
            f.w_ema5_over_30,
            f.m_ema5_over_30,
            f.distance_to_20d_high,
            f.distance_to_20d_low,
            f.volatility_20d,
            f.pe_ttm,
            f.pb,
            f.ps_ttm,
            f.dv_ratio,
            mf.net_mf_amount,
            ths.net_amount AS ths_net_amount,
            ths.net_d5_amount AS ths_net_d5_amount,
            dc.net_amount AS dc_net_amount,
            dc.net_amount_rate AS dc_net_amount_rate,
            fina.ann_date AS fina_ann_date,
            fina.end_date AS fina_end_date,
            fina.roe,
            fina.roa,
            fina.grossprofit_margin,
            fina.current_ratio,
            fina.ocfps,
            fina.debt_to_assets
        FROM ml_stock_feature_daily f
        JOIN ml_stock_universe_daily u
          ON u.trade_date = f.trade_date
         AND u.ts_code = f.ts_code
        JOIN vw_ts_stock_basic b
          ON b.ts_code = f.ts_code
        LEFT JOIN vw_ts_stock_company c
          ON c.ts_code = f.ts_code
        LEFT JOIN vw_moneyflow mf
          ON mf.ts_code = f.ts_code
         AND mf.trade_date = f.trade_date
        LEFT JOIN vw_moneyflow_ths ths
          ON ths.ts_code = f.ts_code
         AND ths.trade_date = f.trade_date
        LEFT JOIN vw_moneyflow_dc dc
          ON dc.ts_code = f.ts_code
         AND dc.trade_date = f.trade_date
        LEFT JOIN latest_fina fina
          ON fina.ts_code = f.ts_code
        WHERE f.trade_date = :trade_date
        ORDER BY b.industry, f.ts_code
        """
    )
    with _get_engine(engine).connect() as conn:
        df = pd.read_sql(sql, conn, params={"trade_date": trade_ts})

    if df.empty:
        return df

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    if "list_date" in df.columns:
        df["list_date"] = pd.to_datetime(df["list_date"], errors="coerce")
    for col in ["fina_ann_date", "fina_end_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    numeric_cols = [
        "close",
        "amount",
        "turnover_rate",
        "volume_ratio",
        "total_mv",
        "circ_mv",
        "amount_ma5_ratio",
        "vol_ma5_ratio",
        "close_over_ma20",
        "ma5_over_ma20",
        "w_ema5_over_30",
        "m_ema5_over_30",
        "distance_to_20d_high",
        "distance_to_20d_low",
        "volatility_20d",
        "pe_ttm",
        "pb",
        "ps_ttm",
        "dv_ratio",
        "net_mf_amount",
        "ths_net_amount",
        "ths_net_d5_amount",
        "dc_net_amount",
        "dc_net_amount_rate",
        "roe",
        "roa",
        "grossprofit_margin",
        "current_ratio",
        "ocfps",
        "debt_to_assets",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "net_mf_amount" in df.columns and "amount" in df.columns:
        denominator = df["amount"].replace(0, np.nan)
        df["net_mf_amount_rate"] = df["net_mf_amount"] / denominator

    return df


def load_alpha095_snapshot(trade_date, engine: Engine | None = None) -> pd.DataFrame:
    trade_ts = pd.Timestamp(trade_date)
    start_ts = trade_ts - timedelta(days=60)
    sql = text(
        """
        WITH target_codes AS (
            SELECT ts_code
            FROM ml_stock_feature_daily
            WHERE trade_date = :trade_date
        )
        SELECT
            d.trade_date AS date,
            d.ts_code AS code,
            d.amount
        FROM vw_ts_stock_daily d
        JOIN target_codes tc
          ON tc.ts_code = d.ts_code
        WHERE d.trade_date >= :start_date
          AND d.trade_date <= :trade_date
        ORDER BY d.ts_code, d.trade_date
        """
    )
    with _get_engine(engine).connect() as conn:
        history_df = pd.read_sql(
            sql,
            conn,
            params={"trade_date": trade_ts, "start_date": start_ts},
        )
    if history_df.empty:
        return pd.DataFrame(columns=["ts_code", "alpha095", "alpha095_cv", "alpha095_logstd", "alpha095_pctstd"])

    history_df["date"] = pd.to_datetime(history_df["date"], errors="coerce")
    history_df["amount"] = pd.to_numeric(history_df["amount"], errors="coerce")
    scored = add_alpha095_family(history_df, window=20, ddof=1)
    snapshot = scored.loc[scored["date"].eq(trade_ts), ["code", "alpha095", "alpha095_cv", "alpha095_logstd", "alpha095_pctstd"]].copy()
    snapshot = snapshot.rename(columns={"code": "ts_code"}).drop_duplicates(subset=["ts_code"])
    return snapshot.reset_index(drop=True)


def load_factor_workbench_frame(trade_date, engine: Engine | None = None) -> pd.DataFrame:
    base_df = load_factor_workbench_base_df(trade_date, engine=engine)
    if base_df.empty:
        return base_df

    alpha_df = load_alpha095_snapshot(trade_date, engine=engine)
    if alpha_df.empty:
        return base_df

    merged = base_df.merge(alpha_df, on="ts_code", how="left")
    return merged
