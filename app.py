# -*- coding: utf-8 -*-
"""ETF份额变动可视化 - Streamlit Web应用"""

# Version: 2.0 - Fixed data_only issue for formula cells
import os
import json
from hmac import compare_digest
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import logging
from typing import Optional, List, Union
from src.data_loader import load_etf_data
from src.volume_fetcher import load_volume_dataframe
from src.etf_classifier import fetch_etf_data, process_etf_classification, export_etfs_to_excel
from src.etf_stats import (
    get_available_dates, get_category_daily_summary,
    get_category_tree, get_category_timeseries, get_agg_summary,
    get_wide_index_available_dates, get_wide_index_timeseries,
    get_macro_date_bounds, get_macro_dataset_timeseries,
    search_security, get_security_profile, get_security_timeseries,
    get_security_financial_timeseries, get_stock_basic_summary,
    export_stock_basic_summary_excel, search_companies, update_stock_custom_info
)

try:
    from src.security_trend_model import (
        score_security_timeseries_model,
        get_security_model_meta,
    )
except Exception:
    score_security_timeseries_model = None
    get_security_model_meta = None

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 页面配置
st.set_page_config(
    page_title="WealthSpark 决策看板",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Plotly 统一主题：匹配浅灰蓝数据看板
pio.templates["wealthspark_balanced"] = go.layout.Template(
    layout=go.Layout(
        paper_bgcolor="rgba(248, 250, 252, 0.92)",
        plot_bgcolor="rgba(241, 245, 249, 0.58)",
        font=dict(color="#0F172A"),
        hoverlabel=dict(bgcolor="rgba(15, 23, 42, 0.96)", font=dict(color="#F8FAFC")),
        title=dict(font=dict(color="#0F172A")),
        legend=dict(bgcolor="rgba(255,255,255,0.52)", bordercolor="rgba(148,163,184,0.10)", borderwidth=1),
        xaxis=dict(
            showline=True,
            linewidth=1,
            ticks="outside",
            tickcolor="rgba(148, 163, 184, 0.28)",
            gridcolor="rgba(148, 163, 184, 0.12)",
            linecolor="rgba(148, 163, 184, 0.24)",
            zerolinecolor="rgba(148, 163, 184, 0.10)"
        ),
        yaxis=dict(
            showline=True,
            linewidth=1,
            ticks="outside",
            tickcolor="rgba(148, 163, 184, 0.28)",
            gridcolor="rgba(148, 163, 184, 0.12)",
            linecolor="rgba(148, 163, 184, 0.24)",
            zerolinecolor="rgba(148, 163, 184, 0.10)"
        ),
        colorway=["#2563EB", "#10B981", "#F59E0B", "#EF4444", "#8B5CF6", "#06B6D4"]
    )
)
pio.templates.default = "wealthspark_balanced"

# 自定义CSS样式 - 金融专业风格
st.markdown("""
<style>
    /* 导入专业字体 */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    /* 全局字体设置 */
    html, body, [class*="css"] {
        font-family: 'Inter', 'PingFang SC', -apple-system, BlinkMacSystemFont, sans-serif;
    }


    /* 整体页面背景：浅灰蓝数据看板 */
    html, body, .stApp, [data-testid="stAppViewContainer"] {
        background:
            radial-gradient(circle at top left, rgba(59, 130, 246, 0.10), transparent 28%),
            radial-gradient(circle at top right, rgba(16, 185, 129, 0.06), transparent 24%),
            linear-gradient(180deg, #F8FAFC 0%, #EEF4FF 48%, #E2E8F0 100%) !important;
        color: #0F172A !important;
    }

    [data-testid="stAppViewContainer"] > .main {
        background: transparent !important;
    }

    .main .block-container {
        background: transparent !important;
    }

    .main p,
    .main li,
    .main label,
    .main span,
    .main .stMarkdown,
    .main [data-testid="stCaptionContainer"] {
        color: #334155 !important;
    }

    /* 隐藏Streamlit默认元素 */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* 平衡看板侧边栏（浅色高可读） */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #F8FAFC 0%, #EEF4FF 100%);
        border-right: 1px solid rgba(148, 163, 184, 0.25);
        padding: 2rem 1rem;
        min-width: 300px !important;
    }

    /* 普通模式下强制保持侧边栏可见，避免折叠后刷新无法恢复 */
    [data-testid="stSidebar"][aria-expanded="false"] {
        min-width: 300px !important;
        width: 300px !important;
        transform: none !important;
        margin-left: 0 !important;
    }
    [data-testid="stSidebar"] > div:first-child {
        width: 300px !important;
    }

    [data-testid="collapsedControl"],
    button[aria-label="Open sidebar"],
    button[aria-label="Close sidebar"] {
        position: fixed !important;
        top: 0.75rem !important;
        left: 0.75rem !important;
        width: 2.75rem !important;
        height: 2.75rem !important;
        border-radius: 9999px !important;
        border: 1px solid rgba(59, 130, 246, 0.35) !important;
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.96) 0%, rgba(37, 99, 235, 0.96) 100%) !important;
        color: #0F172A !important;
        box-shadow: 0 10px 30px rgba(15, 23, 42, 0.28) !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        opacity: 1 !important;
        z-index: 1000 !important;
        transition: transform 0.2s ease, box-shadow 0.2s ease, background 0.2s ease !important;
    }

    [data-testid="collapsedControl"]:hover,
    button[aria-label="Open sidebar"]:hover,
    button[aria-label="Close sidebar"]:hover {
        transform: translateY(-1px) scale(1.02) !important;
        box-shadow: 0 14px 36px rgba(37, 99, 235, 0.28) !important;
        background: linear-gradient(135deg, rgba(30, 41, 59, 1) 0%, rgba(59, 130, 246, 1) 100%) !important;
    }

    [data-testid="collapsedControl"] svg,
    button[aria-label="Open sidebar"] svg,
    button[aria-label="Close sidebar"] svg {
        width: 1.2rem !important;
        height: 1.2rem !important;
        fill: currentColor !important;
    }

    [data-testid="stSidebar"] * {
        color: #1E293B !important;
    }

    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 {
        color: #F8FAFC !important;
        font-weight: 600;
        letter-spacing: -0.02em;
    }

    /* 侧边栏标签样式 */
    [data-testid="stSidebar"] label {
        color: #475569 !important;
        font-weight: 500;
        font-size: 0.875rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }

    /* Multiselect标签美化 */
    [data-testid="stSidebar"] [data-baseweb="tag"] {
        background: linear-gradient(135deg, #3B82F6 0%, #2563EB 100%) !important;
        border-radius: 6px !important;
        padding: 4px 10px !important;
        margin: 2px !important;
        border: none !important;
        box-shadow: 0 2px 4px rgba(59, 130, 246, 0.2);
    }

    [data-testid="stSidebar"] [data-baseweb="tag"] span {
        color: #FFFFFF !important;
        font-weight: 500;
    }

    /* 主内容区域 */
    .main .block-container {
        padding: 2rem 3rem;
        max-width: 1400px;
    }

    /* 卡片式容器 */
    .stPlotlyChart {
        background: linear-gradient(180deg, rgba(255, 255, 255, 0.96) 0%, rgba(248, 250, 252, 0.96) 100%);
        border: 1px solid rgba(148, 163, 184, 0.22);
        border-radius: 16px;
        padding: 1.25rem;
        box-shadow: 0 12px 28px rgba(15, 23, 42, 0.10);
        margin: 1rem 0;
        transition: box-shadow 0.3s ease, transform 0.3s ease;
    }

    .stPlotlyChart:hover {
        box-shadow: 0 16px 32px rgba(15, 23, 42, 0.14);
        transform: translateY(-1px);
    }

    /* 数据表格样式 */
    [data-testid="stDataFrame"] {
        background: linear-gradient(180deg, rgba(255, 255, 255, 0.98) 0%, rgba(248, 250, 252, 0.98) 100%);
        border: 1px solid rgba(148, 163, 184, 0.22);
        border-radius: 16px;
        padding: 1rem;
        box-shadow: 0 12px 28px rgba(15, 23, 42, 0.08);
    }

    /* 标题样式 */
    h1 {
        font-weight: 700;
        font-size: 2.5rem;
        background: linear-gradient(135deg, #1E293B 0%, #3B82F6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin-bottom: 0.5rem;
    }

    h2, h3 {
        font-weight: 600;
        color: #1E293B;
        letter-spacing: -0.02em;
    }

    /* 按钮美化 */
    .stButton > button {
        background: linear-gradient(135deg, #3B82F6 0%, #2563EB 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 1.5rem;
        font-weight: 500;
        transition: all 0.3s ease;
        box-shadow: 0 2px 4px rgba(59, 130, 246, 0.2);
    }

    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(59, 130, 246, 0.3);
    }

    /* 信息框样式 */
    .stAlert {
        border-radius: 8px;
        border-left: 4px solid #3B82F6;
    }

    /* 滑块样式 */
    [data-testid="stSidebar"] .stSlider {
        padding: 1rem 0;
    }

    /* 响应式设计移动端适配 */
    @media (max-width: 768px) {
        .main .block-container {
            padding: 1rem;
        }

        h1 {
            font-size: 1.8rem;
        }

        h2 {
            font-size: 1.5rem;
        }

        h3 {
            font-size: 1.2rem;
        }

        .stPlotlyChart {
            padding: 0.5rem;
        }

        [data-testid="stSidebar"] {
            padding: 1rem 0.5rem;
        }
        
        .stMetric, div[style*="background: white; border-radius: 12px;"] {
            padding: 1rem !important;
        }

        div[style*="font-size: 2rem;"] {
            font-size: 1.5rem !important; 
        }
    }
</style>
""", unsafe_allow_html=True)

# 数据文件路径
DATA_FILE = "主要ETF基金份额变动情况.xlsx"
TREND_RECO_FILE = "data/recommendations/latest_trend_recommendations.json"


# ===== 商业化MVP辅助函数 =====
def get_pro_access_password() -> str:
    secret_password = ""
    try:
        secret_password = st.secrets.get("pro_access_password", "")
        if not secret_password:
            secret_password = st.secrets.get("app", {}).get("pro_access_password", "")
    except Exception:
        secret_password = ""

    return str(
        secret_password
        or os.getenv("ETF_PRO_ACCESS_PASSWORD")
        or os.getenv("ETF_PRO_PASSWORD")
        or ""
    ).strip()


def has_pro_access() -> bool:
    return bool(st.session_state.get("is_pro_user", False))


def grant_pro_access(password: str) -> bool:
    expected_password = get_pro_access_password()
    if not expected_password:
        st.session_state["is_pro_user"] = False
        return False

    authorized = compare_digest(password or "", expected_password)
    st.session_state["is_pro_user"] = authorized
    return authorized


def clear_pro_access() -> None:
    st.session_state["is_pro_user"] = False


def parse_watchlist_input(raw: str) -> list[str]:
    if not raw:
        return []
    normalized = (
        str(raw)
        .replace("，", ",")
        .replace("、", ",")
        .replace(";", ",")
        .replace("；", ",")
        .replace("\n", ",")
    )
    tokens: list[str] = []
    for part in normalized.split(","):
        token = part.strip()
        if token:
            tokens.append(token)
    return list(dict.fromkeys(tokens))


def _safe_float(value, default=0.0) -> float:
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


@st.cache_data(ttl=900)
def load_security_model_score(ts_code: str, security_type: str = "stock") -> dict:
    if not callable(score_security_timeseries_model):
        return {}
    code = str(ts_code or "").strip()
    if not code:
        return {}
    try:
        ts_df = load_security_timeseries(code, security_type)
        if ts_df is None or ts_df.empty:
            return {}
        result = score_security_timeseries_model(ts_df, security_type=security_type, topk=60)
        return result or {}
    except Exception as exc:
        logger.warning(f"load_security_model_score failed for {code}: {exc}")
        return {}


def build_opportunity_snapshot(top_up_df: pd.DataFrame, top_avoid_df: pd.DataFrame, moneyflow_df: pd.DataFrame, emotion_stage: str = "") -> pd.DataFrame:
    if top_up_df is None or top_up_df.empty:
        return pd.DataFrame()

    base = top_up_df.copy()
    for col in ["ts_code", "name", "industry", "reason"]:
        if col not in base.columns:
            base[col] = ""

    base["trend_score"] = pd.to_numeric(base.get("trend_score"), errors="coerce")
    base["risk_score"] = pd.to_numeric(base.get("risk_score"), errors="coerce")
    base["prob_up_5d"] = pd.to_numeric(base.get("prob_up_5d"), errors="coerce")
    base["prob_up_20d"] = pd.to_numeric(base.get("prob_up_20d"), errors="coerce")
    base["close"] = pd.to_numeric(base.get("close"), errors="coerce")

    if moneyflow_df is None or moneyflow_df.empty:
        base["net_mf_amount"] = 0.0
    else:
        mf = moneyflow_df.copy()
        if "ts_code" in mf.columns:
            mf["ts_code"] = mf["ts_code"].astype(str)
        mf["net_mf_amount"] = pd.to_numeric(mf.get("net_mf_amount"), errors="coerce").fillna(0.0)
        keep_cols = [c for c in ["ts_code", "net_mf_amount"] if c in mf.columns]
        mf = mf[keep_cols].drop_duplicates(subset=["ts_code"], keep="first")
        base["ts_code"] = base["ts_code"].astype(str)
        base = base.merge(mf, on="ts_code", how="left")
        base["net_mf_amount"] = pd.to_numeric(base["net_mf_amount"], errors="coerce").fillna(0.0)

    # 轻量机器学习增强：使用现有 KNN 趋势模型做概率融合
    model_prob_5d = []
    model_prob_20d = []
    model_names = []
    for code in base["ts_code"].astype(str).tolist():
        model_res = load_security_model_score(code, "stock")
        model_prob_5d.append(pd.to_numeric(model_res.get("prob_up_5d"), errors="coerce") if model_res else np.nan)
        model_prob_20d.append(pd.to_numeric(model_res.get("prob_up_20d"), errors="coerce") if model_res else np.nan)
        model_names.append(str(model_res.get("model_name") or "-") if model_res else "-")

    base["model_prob_up_5d"] = model_prob_5d
    base["model_prob_up_20d"] = model_prob_20d
    base["model_name"] = model_names

    blend_profile = load_reco_blend_profile(max_files=20)
    rule_blend = float(blend_profile.get("rule_weight") or 0.65)
    model_blend = float(blend_profile.get("model_weight") or 0.35)
    base["blend_rule_weight"] = rule_blend
    base["blend_model_weight"] = model_blend
    base["prob_up_5d_final"] = np.where(
        pd.notna(base["model_prob_up_5d"]),
        base["prob_up_5d"].fillna(0.0) * rule_blend + base["model_prob_up_5d"].fillna(0.0) * model_blend,
        base["prob_up_5d"].fillna(0.0),
    )
    base["prob_up_20d_final"] = np.where(
        pd.notna(base["model_prob_up_20d"]),
        base["prob_up_20d"].fillna(0.0) * rule_blend + base["model_prob_up_20d"].fillna(0.0) * model_blend,
        base["prob_up_20d"].fillna(0.0),
    )
    base["model_agreement"] = np.where(
        pd.notna(base["model_prob_up_5d"]),
        1.0 - (base["prob_up_5d"].fillna(0.0) - base["model_prob_up_5d"].fillna(0.0)).abs(),
        np.nan,
    )

    stage = str(emotion_stage or "").strip()
    trend_weight = 0.50
    prob_weight = 0.18
    ml_weight = 0.14
    moneyflow_weight = 0.08
    risk_penalty_weight = 0.16

    if any(k in stage for k in ["退潮", "冰点", "分歧"]):
        trend_weight = 0.46
        prob_weight = 0.16
        ml_weight = 0.12
        moneyflow_weight = 0.06
        risk_penalty_weight = 0.22
    elif any(k in stage for k in ["主升", "高潮", "亢奋", "强修复"]):
        trend_weight = 0.52
        prob_weight = 0.18
        ml_weight = 0.16
        moneyflow_weight = 0.10
        risk_penalty_weight = 0.14

    base["prob5_pct"] = base["prob_up_5d"].fillna(0.0) * 100.0
    base["hybrid_prob5_pct"] = base["prob_up_5d_final"].fillna(0.0) * 100.0
    base["ml_prob5_pct"] = base["model_prob_up_5d"].fillna(0.0) * 100.0
    base["mf_norm"] = base["net_mf_amount"].clip(lower=-100000, upper=100000) / 10000.0
    base["risk_penalty"] = base["risk_score"].fillna(50.0) * risk_penalty_weight

    base["overheat_penalty"] = 0.0
    base.loc[base["risk_score"].fillna(0) >= 70, "overheat_penalty"] += 6.0
    base.loc[base["prob_up_5d_final"].fillna(0) >= 0.85, "overheat_penalty"] += 3.0
    base.loc[base["net_mf_amount"].fillna(0) >= 80000, "overheat_penalty"] += 2.0

    base["model_bonus"] = 0.0
    base.loc[base["model_agreement"].fillna(0) >= 0.85, "model_bonus"] += 3.0
    base.loc[(base["model_prob_up_5d"].fillna(0) >= 0.60) & (base["prob_up_5d"].fillna(0) >= 0.60), "model_bonus"] += 2.0
    base.loc[pd.notna(base["model_agreement"]) & (base["model_agreement"] <= 0.65), "model_bonus"] -= 3.0

    base["opportunity_score"] = (
        base["trend_score"].fillna(0.0) * trend_weight
        + base["hybrid_prob5_pct"] * prob_weight
        + base["ml_prob5_pct"] * ml_weight
        + base["mf_norm"] * moneyflow_weight
        - base["risk_penalty"]
        - base["overheat_penalty"]
        + base["model_bonus"]
    )
    base["opportunity_score"] = base["opportunity_score"].clip(lower=0, upper=100)

    def confidence_label(row: pd.Series) -> str:
        score = _safe_float(row.get("opportunity_score"), 0.0)
        risk = _safe_float(row.get("risk_score"), 100.0)
        prob5 = _safe_float(row.get("prob_up_5d_final"), 0.0)
        agreement = _safe_float(row.get("model_agreement"), 0.70)
        if score >= 68 and risk <= 45 and prob5 >= 0.60 and agreement >= 0.75:
            return "高"
        if score >= 52 and risk <= 60 and prob5 >= 0.50:
            return "中"
        return "低"

    avoid_codes = set()
    if top_avoid_df is not None and not top_avoid_df.empty and "ts_code" in top_avoid_df.columns:
        avoid_codes = set(top_avoid_df["ts_code"].astype(str).tolist())

    def action_label(row: pd.Series) -> str:
        code = str(row.get("ts_code") or "")
        if code in avoid_codes:
            return "⚠️ 回避"
        score = _safe_float(row.get("opportunity_score"), 0.0)
        risk = _safe_float(row.get("risk_score"), 0.0)
        prob5 = _safe_float(row.get("prob_up_5d_final"), 0.0)
        if score >= 65 and risk <= 45 and prob5 >= 0.60:
            return "✅ 重点关注"
        if score >= 50 and risk <= 60:
            return "👀 观察"
        return "🟡 等待"

    base["confidence"] = base.apply(confidence_label, axis=1)
    base["action"] = base.apply(action_label, axis=1)
    base = base.sort_values(["opportunity_score", "trend_score"], ascending=False).reset_index(drop=True)
    base["rank_commercial"] = np.arange(1, len(base) + 1)
    return base

MACRO_DATASET_META = {
    "cn_gdp": {"label": "GDP", "card_label": "GDP同比", "card_col": "gdp_yoy", "card_unit": "%"},
    "cn_cpi": {"label": "CPI", "card_label": "CPI同比", "card_col": "nt_yoy", "card_unit": "%"},
    "cn_ppi": {"label": "PPI", "card_label": "PPI同比", "card_col": "ppi_yoy", "card_unit": "%"},
    "cn_m": {"label": "M2", "card_label": "M2同比", "card_col": "m2_yoy", "card_unit": "%"},
    "shibor": {"label": "Shibor", "card_label": "Shibor 1Y", "card_col": "rate_1y", "card_unit": "%"},
    "shibor_lpr": {"label": "LPR", "card_label": "LPR 5Y", "card_col": "lpr_5y", "card_unit": "%"},
}


@st.cache_data(ttl=300)
def load_data(file_path: str) -> pd.DataFrame:
    """
    加载ETF数据，缓存5分钟

    Args:
        file_path: Excel文件路径

    Returns:
        DataFrame with columns: code, name, date, metric_type, value, is_aggregate
    """
    try:
        logger.info(f"Loading data from {file_path}")
        df = load_etf_data(file_path)
        logger.info(f"Data loaded successfully: {len(df)} rows")
        return df
    except FileNotFoundError:
        st.error(f"❌ 文件未找到: {file_path}")
        st.stop()
    except Exception as e:
        st.error(f"❌ 加载数据时出错: {str(e)}")
        logger.error(f"Error loading data: {e}", exc_info=True)
        st.stop()


@st.cache_data(ttl=300)
def load_security_search(keyword: str, security_type: str, limit: int = 20) -> pd.DataFrame:
    return search_security(keyword=keyword, security_type=security_type, limit=limit)


@st.cache_data(ttl=300)
def load_security_profile(ts_code: str, security_type: str) -> pd.DataFrame:
    return get_security_profile(ts_code=ts_code, security_type=security_type)


@st.cache_data(ttl=300)
def load_security_timeseries(ts_code: str, security_type: str) -> pd.DataFrame:
    return get_security_timeseries(ts_code=ts_code, security_type=security_type)


@st.cache_data(ttl=300)
def load_security_financial_timeseries(ts_code: str, security_type: str) -> pd.DataFrame:
    return get_security_financial_timeseries(ts_code=ts_code, security_type=security_type)


@st.cache_data(ttl=300)
def load_stock_basic_summary_export() -> pd.DataFrame:
    return get_stock_basic_summary()


@st.cache_data(ttl=300)
def load_macro_date_bounds() -> tuple[str | None, str | None]:
    return get_macro_date_bounds()


@st.cache_data(ttl=300)
def load_macro_dataset(dataset_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    return get_macro_dataset_timeseries(dataset_name=dataset_name, start_date=start_date, end_date=end_date)


@st.cache_data(ttl=300)
def load_fund_hot_stock_periods() -> List[str]:
    try:
        from src.fund_hot_stocks import get_engine as get_fund_hot_engine

        engine = get_fund_hot_engine()
        df = pd.read_sql(
            """
            SELECT DISTINCT end_date
            FROM agg_fund_holding_stock_quarterly
            ORDER BY end_date DESC
            LIMIT 12
            """,
            engine,
        )
        if df is None or df.empty:
            return []

        periods: List[str] = []
        for raw in df["end_date"].tolist():
            if pd.isna(raw):
                continue
            periods.append(pd.to_datetime(raw).strftime("%Y-%m-%d"))
        return periods
    except Exception as exc:
        logger.warning(f"load_fund_hot_stock_periods failed: {exc}")
        return []


@st.cache_data(ttl=300)
def load_fund_hot_stock_meta() -> dict:
    try:
        from src.fund_hot_stocks import get_engine as get_fund_hot_engine

        engine = get_fund_hot_engine()
        df = pd.read_sql(
            """
            SELECT
                COUNT(*) AS row_count,
                COUNT(DISTINCT end_date) AS period_count,
                MAX(end_date) AS latest_period,
                MIN(end_date) AS earliest_period,
                MAX(updated_at) AS latest_updated_at
            FROM agg_fund_holding_stock_quarterly
            """,
            engine,
        )
        if df is None or df.empty:
            return {
                "row_count": 0,
                "period_count": 0,
                "latest_period": None,
                "earliest_period": None,
                "latest_updated_at": None,
            }

        row = df.iloc[0].to_dict()
        return {
            "row_count": int(row.get("row_count") or 0),
            "period_count": int(row.get("period_count") or 0),
            "latest_period": row.get("latest_period"),
            "earliest_period": row.get("earliest_period"),
            "latest_updated_at": row.get("latest_updated_at"),
        }
    except Exception as exc:
        logger.warning(f"load_fund_hot_stock_meta failed: {exc}")
        return {
            "row_count": 0,
            "period_count": 0,
            "latest_period": None,
            "earliest_period": None,
            "latest_updated_at": None,
        }


@st.cache_data(ttl=300, show_spinner=False)
def load_security_top10_shareholders(symbol: str, period: str) -> dict:
    from src.fund_hot_stocks import query_stock_top10_shareholders

    ts_code = str(symbol or "").strip().upper()
    period_norm = str(period or "").replace("-", "").strip()
    if not ts_code:
        return {"top10_holders": pd.DataFrame(), "top10_floatholders": pd.DataFrame(), "errors": {}}

    top10_pack = query_stock_top10_shareholders(
        symbol=ts_code,
        period=period_norm or None,
    )
    return {
        "top10_holders": top10_pack.get("top10_holders", pd.DataFrame()),
        "top10_floatholders": top10_pack.get("top10_floatholders", pd.DataFrame()),
        "errors": top10_pack.get("errors", {}) or {},
    }


@st.cache_data(ttl=300)
def load_trend_recommendations() -> dict:
    try:
        if not os.path.exists(TREND_RECO_FILE):
            return {}
        with open(TREND_RECO_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as exc:
        logger.warning(f"load_trend_recommendations failed: {exc}")
        return {}




def _parse_reco_date_from_filename(filename: str):
    try:
        prefix = str(filename).split("_")[0]
        return datetime.strptime(prefix, "%Y-%m-%d").date()
    except Exception:
        return None


def _compute_forward_returns_by_bars(ts_df: pd.DataFrame, trade_date: str, entry_close) -> dict:
    result = {"ret_1d": np.nan, "ret_3d": np.nan, "ret_5d": np.nan}
    if ts_df is None or ts_df.empty:
        return result
    if "trade_date" not in ts_df.columns or "close" not in ts_df.columns:
        return result

    df = ts_df[["trade_date", "close"]].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["trade_date", "close"]).sort_values("trade_date").reset_index(drop=True)
    if df.empty:
        return result

    td = pd.to_datetime(trade_date, errors="coerce")
    if pd.isna(td):
        return result

    same_day = df[df["trade_date"] == td]
    if same_day.empty:
        same_day = df[df["trade_date"] <= td].tail(1)
    if same_day.empty:
        return result

    entry_idx = int(same_day.index[0])
    entry_px = pd.to_numeric(pd.Series([entry_close]), errors="coerce").iloc[0]
    if pd.isna(entry_px) or float(entry_px) <= 0:
        entry_px = float(same_day.iloc[0]["close"])

    for h in [1, 3, 5]:
        idx = entry_idx + h
        if idx < len(df):
            future_px = float(df.iloc[idx]["close"])
            result[f"ret_{h}d"] = future_px / float(entry_px) - 1.0

    return result


def _summarize_group_returns(returns: list[dict], mode: str) -> dict:
    out = {}
    for h in [1, 3, 5]:
        col = f"ret_{h}d"
        values = [float(r[col]) for r in returns if col in r and pd.notna(r[col])]
        sample = len(values)
        out[f"sample_{h}d"] = sample
        if sample == 0:
            out[f"avg_ret_{h}d"] = np.nan
            out[f"hit_count_{h}d"] = 0
            out[f"hit_rate_{h}d"] = np.nan
            continue

        avg_ret = float(np.mean(values))
        if mode == "up":
            hit_count = sum(1 for v in values if v > 0)
        else:
            hit_count = sum(1 for v in values if v <= 0)

        out[f"avg_ret_{h}d"] = avg_ret
        out[f"hit_count_{h}d"] = int(hit_count)
        out[f"hit_rate_{h}d"] = float(hit_count / sample)
    return out


def _evaluate_reco_payload(payload: dict, symbol_cache: dict, topn_limit: int = 10) -> dict:
    trade_date = str(payload.get("trade_date") or "")
    top_up = (payload.get("top_uptrend") or [])[:topn_limit]
    top_avoid = (payload.get("top_avoid") or [])[:topn_limit]

    up_returns = []
    avoid_returns = []

    def _get_ts(ts_code: str) -> pd.DataFrame:
        code = str(ts_code or "").strip()
        if not code:
            return pd.DataFrame()
        if code not in symbol_cache:
            try:
                symbol_cache[code] = load_security_timeseries(code, "stock")
            except Exception:
                symbol_cache[code] = pd.DataFrame()
        return symbol_cache[code]

    for row in top_up:
        code = str(row.get("ts_code") or "").strip()
        if not code:
            continue
        ts_df = _get_ts(code)
        ret = _compute_forward_returns_by_bars(ts_df, trade_date, row.get("close"))
        up_returns.append(ret)

    for row in top_avoid:
        code = str(row.get("ts_code") or "").strip()
        if not code:
            continue
        ts_df = _get_ts(code)
        ret = _compute_forward_returns_by_bars(ts_df, trade_date, row.get("close"))
        avoid_returns.append(ret)

    up_stats = _summarize_group_returns(up_returns, mode="up")
    avoid_stats = _summarize_group_returns(avoid_returns, mode="avoid")

    out = {
        "trade_date": trade_date,
        "up_candidates": len(top_up),
        "avoid_candidates": len(top_avoid),
    }
    for k, v in up_stats.items():
        out[f"up_{k}"] = v
    for k, v in avoid_stats.items():
        out[f"avoid_{k}"] = v
    return out


@st.cache_data(ttl=900)
def load_reco_effectiveness_history(max_files: int = 30, topn_limit: int = 10) -> pd.DataFrame:
    base_dir = os.path.dirname(TREND_RECO_FILE) or "."
    if not os.path.isdir(base_dir):
        return pd.DataFrame()

    entries = []
    try:
        for name in os.listdir(base_dir):
            if not str(name).endswith("_trend_recommendations.json"):
                continue
            d = _parse_reco_date_from_filename(name)
            if d is None:
                continue
            entries.append((d, os.path.join(base_dir, name)))
    except Exception:
        return pd.DataFrame()

    if not entries:
        return pd.DataFrame()

    entries.sort(key=lambda x: x[0])
    if max_files and len(entries) > int(max_files):
        entries = entries[-int(max_files):]

    rows = []
    symbol_cache = {}
    for _, fp in entries:
        try:
            with open(fp, "r", encoding="utf-8") as f:
                payload = json.load(f)
            rows.append(_evaluate_reco_payload(payload, symbol_cache, topn_limit=topn_limit))
        except Exception as exc:
            logger.warning(f"load_reco_effectiveness_history skip {fp}: {exc}")

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if "trade_date" in df.columns:
        df = df.sort_values("trade_date").reset_index(drop=True)
    return df


@st.cache_data(ttl=900)
def load_reco_blend_profile(max_files: int = 20) -> dict:
    hist_df = load_reco_effectiveness_history(max_files=max_files, topn_limit=10)
    if hist_df is None or hist_df.empty:
        return {
            "rule_weight": 0.65,
            "model_weight": 0.35,
            "source": "default",
            "up_hit_rate_5d": np.nan,
            "avoid_hit_rate_5d": np.nan,
        }

    up_col = "up_hit_rate_5d"
    av_col = "avoid_hit_rate_5d"
    up_rate = pd.to_numeric(hist_df.get(up_col), errors="coerce") if up_col in hist_df.columns else pd.Series(dtype=float)
    av_rate = pd.to_numeric(hist_df.get(av_col), errors="coerce") if av_col in hist_df.columns else pd.Series(dtype=float)

    up_mean = float(up_rate.dropna().mean()) if not up_rate.dropna().empty else np.nan
    av_mean = float(av_rate.dropna().mean()) if not av_rate.dropna().empty else np.nan

    rule_weight = 0.65
    model_weight = 0.35

    # 简单的历史效果驱动调权：
    # - 强势命中率高且避雷有效率高，略提高模型权重
    # - 若统计不稳定或历史偏弱，则更保守，偏向规则
    if pd.notna(up_mean) and pd.notna(av_mean):
        combined = 0.55 * up_mean + 0.45 * av_mean
        if combined >= 0.62:
            model_weight = 0.45
            rule_weight = 0.55
        elif combined >= 0.56:
            model_weight = 0.40
            rule_weight = 0.60
        elif combined <= 0.48:
            model_weight = 0.25
            rule_weight = 0.75

    return {
        "rule_weight": float(rule_weight),
        "model_weight": float(model_weight),
        "source": "history-adaptive",
        "up_hit_rate_5d": up_mean,
        "avoid_hit_rate_5d": av_mean,
    }




def _score_rule_candidate(row: dict) -> float:
    trend = _safe_float(row.get("trend_score"), 0.0)
    prob5 = _safe_float(row.get("prob_up_5d"), 0.0) * 100.0
    risk = _safe_float(row.get("risk_score"), 50.0)
    return trend * 0.65 + prob5 * 0.25 - risk * 0.15


def _slice_ts_to_trade_date(ts_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    if ts_df is None or ts_df.empty or "trade_date" not in ts_df.columns:
        return pd.DataFrame()
    df = ts_df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    td = pd.to_datetime(trade_date, errors="coerce")
    if pd.isna(td):
        return pd.DataFrame()
    return df[df["trade_date"] <= td].copy()


def _evaluate_strategy_compare(payload: dict, symbol_cache: dict, topn_limit: int = 10) -> dict:
    trade_date = str(payload.get("trade_date") or "")
    candidates = (payload.get("top_uptrend") or [])[:topn_limit]
    if not candidates:
        return {"trade_date": trade_date, "rule_ret_5d": np.nan, "model_ret_5d": np.nan, "hybrid_ret_5d": np.nan}

    blend = load_reco_blend_profile(max_files=20)
    rule_w = float(blend.get("rule_weight") or 0.65)
    model_w = float(blend.get("model_weight") or 0.35)

    rows = []
    for row in candidates:
        code = str(row.get("ts_code") or "").strip()
        if not code:
            continue
        if code not in symbol_cache:
            try:
                symbol_cache[code] = load_security_timeseries(code, "stock")
            except Exception:
                symbol_cache[code] = pd.DataFrame()
        ts_full = symbol_cache.get(code)
        ts_cut = _slice_ts_to_trade_date(ts_full, trade_date)
        model_res = {}
        try:
            if callable(score_security_timeseries_model) and ts_cut is not None and not ts_cut.empty:
                model_res = score_security_timeseries_model(ts_cut, security_type="stock", topk=60) or {}
        except Exception:
            model_res = {}

        model_prob = _safe_float(model_res.get("prob_up_5d"), np.nan)
        rule_score = _score_rule_candidate(row)
        model_score = (model_prob * 100.0 - _safe_float(row.get("risk_score"), 50.0) * 0.10) if pd.notna(model_prob) else np.nan
        hybrid_score = rule_score if pd.isna(model_score) else rule_w * rule_score + model_w * model_score

        fwd = _compute_forward_returns_by_bars(ts_full, trade_date, row.get("close"))
        rows.append({
            "ts_code": code,
            "name": str(row.get("name") or code),
            "rule_score": rule_score,
            "model_score": model_score,
            "hybrid_score": hybrid_score,
            "ret_5d": fwd.get("ret_5d"),
        })

    if not rows:
        return {"trade_date": trade_date, "rule_ret_5d": np.nan, "model_ret_5d": np.nan, "hybrid_ret_5d": np.nan}

    df = pd.DataFrame(rows)
    rule_pick = df.sort_values("rule_score", ascending=False).head(1)
    model_pick = df.sort_values("model_score", ascending=False, na_position="last").head(1)
    hybrid_pick = df.sort_values("hybrid_score", ascending=False).head(1)

    def _pick_ret(frame: pd.DataFrame):
        if frame is None or frame.empty:
            return np.nan
        return pd.to_numeric(frame.iloc[0].get("ret_5d"), errors="coerce")

    return {
        "trade_date": trade_date,
        "rule_ret_5d": _pick_ret(rule_pick),
        "model_ret_5d": _pick_ret(model_pick),
        "hybrid_ret_5d": _pick_ret(hybrid_pick),
        "rule_pick": str(rule_pick.iloc[0].get("name")) if not rule_pick.empty else "-",
        "model_pick": str(model_pick.iloc[0].get("name")) if not model_pick.empty else "-",
        "hybrid_pick": str(hybrid_pick.iloc[0].get("name")) if not hybrid_pick.empty else "-",
    }


@st.cache_data(ttl=900)
def load_reco_strategy_comparison(max_files: int = 20, topn_limit: int = 10) -> pd.DataFrame:
    base_dir = os.path.dirname(TREND_RECO_FILE) or "."
    if not os.path.isdir(base_dir):
        return pd.DataFrame()

    entries = []
    for name in os.listdir(base_dir):
        if not str(name).endswith("_trend_recommendations.json"):
            continue
        d = _parse_reco_date_from_filename(name)
        if d is None:
            continue
        entries.append((d, os.path.join(base_dir, name)))

    if not entries:
        return pd.DataFrame()
    entries.sort(key=lambda x: x[0])
    entries = entries[-int(max_files):]

    rows = []
    symbol_cache = {}
    for _, fp in entries:
        try:
            with open(fp, "r", encoding="utf-8") as f:
                payload = json.load(f)
            rows.append(_evaluate_strategy_compare(payload, symbol_cache, topn_limit=topn_limit))
        except Exception as exc:
            logger.warning(f"load_reco_strategy_comparison skip {fp}: {exc}")

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "trade_date" in df.columns:
        df = df.sort_values("trade_date").reset_index(drop=True)
    return df


def render_strategy_comparison_panel():
    st.markdown("#### 🧪 策略对比评估（Top1实验）")
    st.caption("比较口径：对每个交易日的强势候选池，分别用纯规则、纯模型、混合模型选出 Top1，再比较其后续 5 日收益。")

    ctl_cols = st.columns([1.1, 1.2, 3])
    with ctl_cols[0]:
        max_files = st.selectbox("对比样本", [20, 30, 60, 120], index=0, key="strategy_compare_max_files")
    with ctl_cols[1]:
        chart_mode = st.selectbox("图表模式", ["近5日收益曲线", "累计收益曲线", "双图对照"], index=2, key="strategy_compare_chart_mode")

    comp_df = load_reco_strategy_comparison(max_files=int(max_files), topn_limit=10)
    if comp_df is None or comp_df.empty:
        st.info("暂无足够样本用于做规则 / 模型 / 混合 的 Top1 对比。")
        return

    comp_df = comp_df.copy()
    comp_df["trade_date"] = pd.to_datetime(comp_df.get("trade_date"), errors="coerce")
    comp_df = comp_df.sort_values("trade_date").reset_index(drop=True)

    strategy_meta = [
        ("rule_ret_5d", "纯规则", "#2563EB"),
        ("model_ret_5d", "纯模型", "#F59E0B"),
        ("hybrid_ret_5d", "混合模型", "#10B981"),
    ]

    metrics = []
    plot_df = pd.DataFrame({"trade_date": comp_df["trade_date"]})
    for key, label, _color in strategy_meta:
        vals = pd.to_numeric(comp_df.get(key), errors="coerce")
        clean = vals.dropna()
        sample = len(clean)
        avg_ret = float(clean.mean()) if sample > 0 else np.nan
        hit_rate = float((clean > 0).mean()) if sample > 0 else np.nan
        cum_ret = float((1 + clean).prod() - 1) if sample > 0 else np.nan
        plot_df[label] = vals
        plot_df[f"{label}_cum"] = (1 + vals.fillna(0.0)).cumprod() - 1
        metrics.append({
            "策略": label,
            "样本数": sample,
            "5日命中率": f"{hit_rate:.1%}" if pd.notna(hit_rate) else "-",
            "5日平均收益": f"{avg_ret:+.2%}" if pd.notna(avg_ret) else "-",
            "累计收益": f"{cum_ret:+.2%}" if pd.notna(cum_ret) else "-",
        })

    st.dataframe(pd.DataFrame(metrics), use_container_width=True, hide_index=True)

    if plot_df["trade_date"].notna().any():
        date_x = plot_df["trade_date"]

        daily_fig = go.Figure()
        for _key, label, color in strategy_meta:
            daily_fig.add_trace(go.Scatter(
                x=date_x,
                y=plot_df[label],
                mode="lines+markers",
                name=label,
                line=dict(width=2.5, color=color),
                marker=dict(size=6),
                hovertemplate=f"{label}<br>%{{x|%Y-%m-%d}}<br>5日收益: %{{y:.2%}}<extra></extra>",
            ))
        daily_fig.update_layout(
            title="策略 Top1 单期收益趋势",
            height=360,
            template="wealthspark_balanced",
            hovermode="x unified",
            margin=dict(l=20, r=20, t=60, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        daily_fig.update_yaxes(tickformat=".0%", title_text="5日收益")
        daily_fig.update_xaxes(title_text="交易日")

        cum_fig = go.Figure()
        for _key, label, color in strategy_meta:
            cum_fig.add_trace(go.Scatter(
                x=date_x,
                y=plot_df[f"{label}_cum"],
                mode="lines",
                name=label,
                line=dict(width=3, color=color),
                hovertemplate=f"{label}<br>%{{x|%Y-%m-%d}}<br>累计收益: %{{y:.2%}}<extra></extra>",
            ))
        cum_fig.update_layout(
            title="策略 Top1 累计收益曲线",
            height=360,
            template="wealthspark_balanced",
            hovermode="x unified",
            margin=dict(l=20, r=20, t=60, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        cum_fig.update_yaxes(tickformat=".0%", title_text="累计收益")
        cum_fig.update_xaxes(title_text="交易日")

        if chart_mode == "近5日收益曲线":
            st.plotly_chart(daily_fig, use_container_width=True)
        elif chart_mode == "累计收益曲线":
            st.plotly_chart(cum_fig, use_container_width=True)
        else:
            chart_left, chart_right = st.columns(2)
            with chart_left:
                st.plotly_chart(daily_fig, use_container_width=True)
            with chart_right:
                st.plotly_chart(cum_fig, use_container_width=True)

    show_df = comp_df[[c for c in ["trade_date", "rule_pick", "rule_ret_5d", "model_pick", "model_ret_5d", "hybrid_pick", "hybrid_ret_5d"] if c in comp_df.columns]].copy()
    if "trade_date" in show_df.columns:
        show_df["trade_date"] = pd.to_datetime(show_df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    rename_map = {
        "trade_date": "交易日",
        "rule_pick": "纯规则Top1",
        "rule_ret_5d": "纯规则5日收益",
        "model_pick": "纯模型Top1",
        "model_ret_5d": "纯模型5日收益",
        "hybrid_pick": "混合模型Top1",
        "hybrid_ret_5d": "混合模型5日收益",
    }
    show_df = show_df.rename(columns=rename_map)
    for c in ["纯规则5日收益", "纯模型5日收益", "混合模型5日收益"]:
        if c in show_df.columns:
            show_df[c] = pd.to_numeric(show_df[c], errors="coerce").map(lambda v: f"{v:+.2%}" if pd.notna(v) else "-")
    st.dataframe(show_df, use_container_width=True, hide_index=True, height=320)


def render_reco_effectiveness_tracking_panel():
    st.markdown("#### 📊 推荐有效性追踪（实验版）")
    blend_profile = load_reco_blend_profile(max_files=20)
    if blend_profile.get("source") == "history-adaptive":
        st.caption(
            f"当前融合策略：规则 {float(blend_profile.get('rule_weight') or 0.65):.0%} / 模型 {float(blend_profile.get('model_weight') or 0.35):.0%}（基于最近历史效果自动调节）"
        )
    st.caption("统计口径：以推荐当日收盘价为起点，按后续第 1 / 3 / 5 个交易日收盘价计算收益。强势榜命中 = 未来收益 > 0；避雷榜有效 = 未来收益 ≤ 0。")

    ctl_cols = st.columns([1.2, 1, 3])
    with ctl_cols[0]:
        max_files = st.selectbox("样本文件", [10, 20, 30, 60, 120], index=2, key="reco_eval_max_files")
    with ctl_cols[1]:
        horizon_label = st.selectbox("核心周期", ["1日", "3日", "5日"], index=2, key="reco_eval_horizon")
    horizon = {"1日": "1d", "3日": "3d", "5日": "5d"}[horizon_label]

    hist_df = load_reco_effectiveness_history(max_files=int(max_files), topn_limit=10)
    if hist_df is None or hist_df.empty:
        st.info("暂无足够的历史推荐文件用于统计（至少需要生成并保留多日推荐结果）。")
        return

    up_sample_col = f"up_sample_{horizon}"
    up_hit_col = f"up_hit_count_{horizon}"
    up_avg_col = f"up_avg_ret_{horizon}"
    av_sample_col = f"avoid_sample_{horizon}"
    av_hit_col = f"avoid_hit_count_{horizon}"
    av_avg_col = f"avoid_avg_ret_{horizon}"

    for col in [up_sample_col, up_hit_col, up_avg_col, av_sample_col, av_hit_col, av_avg_col]:
        if col not in hist_df.columns:
            hist_df[col] = np.nan

    up_samples = int(pd.to_numeric(hist_df[up_sample_col], errors="coerce").fillna(0).sum())
    up_hits = int(pd.to_numeric(hist_df[up_hit_col], errors="coerce").fillna(0).sum())
    av_samples = int(pd.to_numeric(hist_df[av_sample_col], errors="coerce").fillna(0).sum())
    av_hits = int(pd.to_numeric(hist_df[av_hit_col], errors="coerce").fillna(0).sum())

    up_hit_rate = (up_hits / up_samples) if up_samples > 0 else np.nan
    av_hit_rate = (av_hits / av_samples) if av_samples > 0 else np.nan

    up_weighted_ret = np.nan
    if up_samples > 0:
        up_weighted_ret = (
            pd.to_numeric(hist_df[up_avg_col], errors="coerce").fillna(0)
            * pd.to_numeric(hist_df[up_sample_col], errors="coerce").fillna(0)
        ).sum() / up_samples

    av_weighted_ret = np.nan
    if av_samples > 0:
        av_weighted_ret = (
            pd.to_numeric(hist_df[av_avg_col], errors="coerce").fillna(0)
            * pd.to_numeric(hist_df[av_sample_col], errors="coerce").fillna(0)
        ).sum() / av_samples

    metric_cols = st.columns(4)
    metric_cols[0].metric(f"强势命中率（{horizon_label}）", f"{up_hit_rate:.1%}" if pd.notna(up_hit_rate) else "-")
    metric_cols[1].metric(f"强势平均收益（{horizon_label}）", f"{up_weighted_ret:+.2%}" if pd.notna(up_weighted_ret) else "-")
    metric_cols[2].metric(f"避雷有效率（{horizon_label}）", f"{av_hit_rate:.1%}" if pd.notna(av_hit_rate) else "-")
    metric_cols[3].metric(f"避雷平均收益（{horizon_label}）", f"{av_weighted_ret:+.2%}" if pd.notna(av_weighted_ret) else "-")

    st.caption(f"当前统计样本：强势 {up_samples} 条，避雷 {av_samples} 条（来自最近 {int(max_files)} 份推荐文件）。")

    show_cols = [
        "trade_date",
        "up_candidates",
        "up_hit_rate_1d", "up_hit_rate_3d", "up_hit_rate_5d",
        "up_avg_ret_1d", "up_avg_ret_3d", "up_avg_ret_5d",
        "avoid_candidates",
        "avoid_hit_rate_1d", "avoid_hit_rate_3d", "avoid_hit_rate_5d",
        "avoid_avg_ret_1d", "avoid_avg_ret_3d", "avoid_avg_ret_5d",
    ]
    avail_cols = [c for c in show_cols if c in hist_df.columns]
    show_df = hist_df[avail_cols].copy()

    for c in [
        "up_hit_rate_1d", "up_hit_rate_3d", "up_hit_rate_5d",
        "avoid_hit_rate_1d", "avoid_hit_rate_3d", "avoid_hit_rate_5d",
    ]:
        if c in show_df.columns:
            show_df[c] = pd.to_numeric(show_df[c], errors="coerce").map(lambda v: f"{v:.1%}" if pd.notna(v) else "-")

    for c in [
        "up_avg_ret_1d", "up_avg_ret_3d", "up_avg_ret_5d",
        "avoid_avg_ret_1d", "avoid_avg_ret_3d", "avoid_avg_ret_5d",
    ]:
        if c in show_df.columns:
            show_df[c] = pd.to_numeric(show_df[c], errors="coerce").map(lambda v: f"{v:+.2%}" if pd.notna(v) else "-")

    rename_map = {
        "trade_date": "交易日",
        "up_candidates": "强势候选数",
        "up_hit_rate_1d": "强势命中率1日",
        "up_hit_rate_3d": "强势命中率3日",
        "up_hit_rate_5d": "强势命中率5日",
        "up_avg_ret_1d": "强势均收1日",
        "up_avg_ret_3d": "强势均收3日",
        "up_avg_ret_5d": "强势均收5日",
        "avoid_candidates": "避雷候选数",
        "avoid_hit_rate_1d": "避雷有效率1日",
        "avoid_hit_rate_3d": "避雷有效率3日",
        "avoid_hit_rate_5d": "避雷有效率5日",
        "avoid_avg_ret_1d": "避雷均收1日",
        "avoid_avg_ret_3d": "避雷均收3日",
        "avoid_avg_ret_5d": "避雷均收5日",
    }
    show_df = show_df.rename(columns={k: v for k, v in rename_map.items() if k in show_df.columns})
    st.dataframe(show_df, use_container_width=True, hide_index=True, height=320)

    render_strategy_comparison_panel()


def get_stock_info_edit_password() -> str:
    secret_password = ""
    try:
        secret_password = st.secrets.get("stock_info_edit_password", "")
        if not secret_password:
            secret_password = st.secrets.get("app", {}).get("stock_info_edit_password", "")
    except Exception:
        secret_password = ""

    return str(
        secret_password
        or os.getenv("ETF_STOCK_INFO_EDIT_PASSWORD")
        or os.getenv("ETF_EDIT_PASSWORD")
        or ""
    ).strip()


def has_stock_info_edit_permission() -> bool:
    return bool(get_stock_info_edit_password()) and bool(
        st.session_state.get("stock_info_edit_authorized", False)
    )


def grant_stock_info_edit_permission(password: str) -> bool:
    expected_password = get_stock_info_edit_password()
    if not expected_password:
        st.session_state["stock_info_edit_authorized"] = False
        return False

    is_authorized = compare_digest(password or "", expected_password)
    st.session_state["stock_info_edit_authorized"] = is_authorized
    return is_authorized


def get_query_param_value(name: str) -> str:
    try:
        value = st.query_params.get(name, "")
        if isinstance(value, list):
            return str(value[0]) if value else ""
        return str(value or "")
    except Exception:
        try:
            params = st.experimental_get_query_params()
            value = params.get(name, [""])
            if isinstance(value, list):
                return str(value[0]) if value else ""
            return str(value or "")
        except Exception:
            return ""


def hydrate_security_jump_from_query_params() -> None:
    security_query = get_query_param_value("security_query").strip()
    if not security_query:
        return

    jump_nonce = get_query_param_value("jump_nonce").strip()
    open_tab = get_query_param_value("open_tab").strip().lower()
    security_type = get_query_param_value("security_type").strip().lower()

    if jump_nonce and jump_nonce == st.session_state.get("last_consumed_jump_nonce"):
        return

    st.session_state["security_search_keyword"] = security_query
    if security_type == "stock":
        st.session_state["security_search_type"] = "股票"
    elif security_type == "index":
        st.session_state["security_search_type"] = "指数"

    if open_tab == "security":
        # 方案B：通过 sidebar 一级导航 + 个股子导航完成跳转
        st.session_state["sidebar_nav_group"] = "个股"
        st.session_state["stock_subpage"] = "🔎 个股/指数查询"
        st.session_state["jump_to_security_tab"] = True

    if jump_nonce:
        st.session_state["last_consumed_jump_nonce"] = jump_nonce


def trigger_security_tab_jump_if_needed() -> None:
    """若存在跳转请求，切到 sidebar 的“个股 -> 个股/指数查询”。"""
    if not st.session_state.get("jump_to_security_tab", False):
        return

    st.session_state["sidebar_nav_group"] = "个股"
    st.session_state["stock_subpage"] = "🔎 个股/指数查询"
    st.session_state["jump_to_security_tab"] = False

def render_tech_picker_jump_table(df: pd.DataFrame) -> None:
    from urllib.parse import quote

    if df is None or df.empty:
        return

    display_df = df.rename(columns={
        'ts_code': '代码', 'name': '简称', 'industry': '行业',
        'trade_date': '满足日期',
        'w_ema5': '周线EMA5', 'w_ema30': '周线EMA30',
        'm_ema5': '月线EMA5', 'm_ema30': '月线EMA30',
        'main_business': '主要业务'
    }).copy()

    for col in ['周线EMA5', '周线EMA30', '月线EMA5', '月线EMA30']:
        if col in display_df.columns:
            display_df[col] = pd.to_numeric(display_df[col], errors='coerce').map(
                lambda x: '-' if pd.isna(x) else f"{x:,.2f}"
            )

    if '满足日期' in display_df.columns:
        display_df['满足日期'] = pd.to_datetime(display_df['满足日期'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('-')

    display_df = display_df.fillna('-')

    render_nonce = st.session_state.get('tech_picker_render_nonce', 0) + 1
    st.session_state['tech_picker_render_nonce'] = render_nonce

    query_links = []
    for _, row in display_df.iterrows():
        query = str(row.get('代码') or row.get('简称') or '').strip()
        if not query:
            query_links.append('#')
            continue
        query_links.append(
            f"?security_query={quote(query)}&security_type=stock&open_tab=security&jump_nonce={render_nonce}_{quote(query)}"
        )

    render_df = display_df.copy()
    render_df.insert(0, '查询', query_links)

    st.info("💡 直接点击每行最左侧“🔎 查询”即可跳到“个股/指数查询”，并自动带入该股票代码。")

    st.dataframe(
        render_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            '查询': st.column_config.LinkColumn(
                '查询',
                help='点击后跳转到个股/指数查询',
                display_text='🔎 查询'
            )
        }
    )

def format_security_option(row: pd.Series) -> str:
    security_type_label = "股票" if row.get('security_type') == 'stock' else "指数"
    name = row.get('name') or row.get('ts_code') or '-'
    ts_code = row.get('ts_code') or '-'
    symbol = row.get('symbol')
    industry = row.get('industry')
    market = row.get('market')
    extras = [item for item in [symbol, industry, market] if item and str(item).strip() and str(item) != ts_code]
    extra_text = f" | {' / '.join(extras)}" if extras else ""
    return f"{security_type_label} | {name} | {ts_code}{extra_text}"


def format_optional_number(value, digits: int = 2, scale: float = 1.0, suffix: str = "") -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value) / scale:,.{digits}f}{suffix}"


def format_optional_date(value) -> str:
    if value is None or pd.isna(value):
        return "-"
    return pd.to_datetime(value).strftime('%Y-%m-%d')


def clamp_value(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def compute_rsi(series: pd.Series, window: int = 14) -> pd.Series:
    series = pd.to_numeric(series, errors='coerce')
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
    avg_loss = loss.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)


def build_security_trend_analysis(ts_df: pd.DataFrame, security_type: str = 'stock') -> dict | None:
    if ts_df is None or ts_df.empty or 'close' not in ts_df.columns:
        return None

    df = ts_df.copy()
    if 'trade_date' not in df.columns:
        return None

    df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    for col in ['turnover_rate', 'volume_ratio', 'pe', 'pe_ttm', 'pb', 'ps_ttm', 'total_mv', 'circ_mv', 'float_mv']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=['trade_date', 'close']).sort_values('trade_date').reset_index(drop=True)
    if len(df) < 10:
        return None

    close = df['close']
    df['ma5'] = close.rolling(5, min_periods=3).mean()
    df['ma10'] = close.rolling(10, min_periods=5).mean()
    df['ma20'] = close.rolling(20, min_periods=10).mean()
    df['ma60'] = close.rolling(60, min_periods=20).mean()
    df['ret_5'] = close.pct_change(5)
    df['ret_20'] = close.pct_change(20)
    df['rsi14'] = compute_rsi(close, 14)

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df['macd_dif'] = ema12 - ema26
    df['macd_dea'] = df['macd_dif'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = df['macd_dif'] - df['macd_dea']

    df['boll_mid'] = close.rolling(20, min_periods=10).mean()
    boll_std = close.rolling(20, min_periods=10).std()
    df['boll_up'] = df['boll_mid'] + 2 * boll_std
    df['boll_down'] = df['boll_mid'] - 2 * boll_std
    boll_span = (df['boll_up'] - df['boll_down']).replace(0, np.nan)
    df['boll_pos'] = (close - df['boll_down']) / boll_span

    df['volatility20'] = close.pct_change().rolling(20, min_periods=10).std() * np.sqrt(20) * 100
    df['support_20'] = close.rolling(20, min_periods=5).min()
    df['resistance_20'] = close.rolling(20, min_periods=5).max()
    df['support_60'] = close.rolling(60, min_periods=20).min()
    df['resistance_60'] = close.rolling(60, min_periods=20).max()

    last = df.iloc[-1]
    latest_close = float(last['close'])
    ma5 = float(last['ma5']) if pd.notna(last['ma5']) else latest_close
    ma20 = float(last['ma20']) if pd.notna(last['ma20']) else latest_close
    ma60 = float(last['ma60']) if pd.notna(last['ma60']) else ma20
    ret_5 = float(last['ret_5']) if pd.notna(last['ret_5']) else 0.0
    ret_20 = float(last['ret_20']) if pd.notna(last['ret_20']) else 0.0
    rsi14 = float(last['rsi14']) if pd.notna(last['rsi14']) else 50.0
    macd_hist = float(last['macd_hist']) if pd.notna(last.get('macd_hist')) else 0.0
    boll_pos = float(last['boll_pos']) if pd.notna(last.get('boll_pos')) else np.nan
    volatility20 = float(last['volatility20']) if pd.notna(last['volatility20']) else 0.0
    volume_ratio = float(last['volume_ratio']) if 'volume_ratio' in df.columns and pd.notna(last.get('volume_ratio')) else np.nan
    turnover_rate = float(last['turnover_rate']) if 'turnover_rate' in df.columns and pd.notna(last.get('turnover_rate')) else np.nan

    trend_score = 50.0
    reasons: List[str] = []
    risk_flags: List[str] = []

    if latest_close >= ma20:
        trend_score += 12
        reasons.append('收盘价位于20日均线上方，短中期结构偏强')
    else:
        trend_score -= 12
        risk_flags.append('收盘价跌破20日均线，短线结构偏弱')

    if latest_close >= ma60:
        trend_score += 10
        reasons.append('收盘价站上60日均线，中期趋势仍有支撑')
    else:
        trend_score -= 10
        risk_flags.append('收盘价位于60日均线下方，中期承压')

    if ma5 >= ma20:
        trend_score += 8
        reasons.append('5日均线位于20日均线上方，短线动能占优')
    else:
        trend_score -= 8
        risk_flags.append('5日均线跌破20日均线，短线动能转弱')

    if ret_20 > 0:
        trend_score += 10
        reasons.append(f'近20日累计收益为 {ret_20:+.1%}，中期惯性偏正')
    else:
        trend_score -= 10
        risk_flags.append(f'近20日累计收益为 {ret_20:+.1%}，中期惯性偏弱')

    if ret_5 > 0:
        trend_score += 6
        reasons.append(f'近5日累计收益为 {ret_5:+.1%}，短线保持修复/上行动能')
    else:
        trend_score -= 6
        risk_flags.append(f'近5日累计收益为 {ret_5:+.1%}，短线仍在消化压力')

    if 45 <= rsi14 <= 70:
        trend_score += 7
        reasons.append(f'RSI14 为 {rsi14:.1f}，处于相对健康区间')
    elif rsi14 < 30:
        trend_score += 2
        reasons.append(f'RSI14 为 {rsi14:.1f}，处于超跌观察区')
    elif rsi14 > 80:
        trend_score -= 7
        risk_flags.append(f'RSI14 为 {rsi14:.1f}，短线偏热')

    if macd_hist >= 0:
        trend_score += 6
        reasons.append(f'MACD柱值 {macd_hist:.4f} 为正，短线动能偏强')
    else:
        trend_score -= 6
        risk_flags.append(f'MACD柱值 {macd_hist:.4f} 为负，动能仍偏弱')

    if pd.notna(boll_pos):
        if 0.30 <= boll_pos <= 0.80:
            trend_score += 3
            reasons.append(f'布林带位置 {boll_pos:.2f}，价格运行在相对健康区间')
        elif boll_pos > 0.95:
            trend_score -= 4
            risk_flags.append(f'布林带位置 {boll_pos:.2f}，短线接近上轨，追高风险上升')
        elif boll_pos < 0.05:
            trend_score += 1
            reasons.append(f'布林带位置 {boll_pos:.2f}，处于下轨附近，关注超跌修复')

    if pd.notna(volume_ratio):
        if 1.0 <= volume_ratio <= 2.2:
            trend_score += 4
            reasons.append(f'量比 {volume_ratio:.2f}，量能配合相对正常')
        elif volume_ratio < 0.7:
            trend_score -= 2
            risk_flags.append(f'量比 {volume_ratio:.2f}，量能偏弱')
        elif volume_ratio > 3.0:
            trend_score -= 3
            risk_flags.append(f'量比 {volume_ratio:.2f}，波动可能放大')

    if pd.notna(turnover_rate) and turnover_rate > 8:
        risk_flags.append(f'换手率 {turnover_rate:.2f}% 较高，短线博弈较强')

    trend_score = clamp_value(trend_score, 0, 100)

    if trend_score >= 72:
        trend_label = '上涨趋势'
    elif trend_score >= 58:
        trend_label = '震荡偏强'
    elif trend_score >= 45:
        trend_label = '震荡整理'
    elif trend_score >= 30:
        trend_label = '震荡偏弱'
    else:
        trend_label = '下跌趋势'

    prob_up_5d = 0.50
    prob_up_20d = 0.50
    prob_up_5d += 0.08 if latest_close >= ma20 else -0.08
    prob_up_5d += 0.06 if ma5 >= ma20 else -0.06
    prob_up_5d += 0.05 if ret_5 > 0 else -0.05
    prob_up_5d += 0.03 if ret_20 > 0 else -0.03
    if 45 <= rsi14 <= 70:
        prob_up_5d += 0.04
    elif rsi14 > 80:
        prob_up_5d -= 0.06
    elif rsi14 < 30:
        prob_up_5d += 0.02
    if pd.notna(volume_ratio):
        if 1.0 <= volume_ratio <= 2.2:
            prob_up_5d += 0.03
        elif volume_ratio > 3.0:
            prob_up_5d -= 0.03
        elif volume_ratio < 0.7:
            prob_up_5d -= 0.02
    if latest_close < ma60:
        prob_up_5d -= 0.04
    if macd_hist > 0:
        prob_up_5d += 0.04
    else:
        prob_up_5d -= 0.04
    if pd.notna(boll_pos):
        if boll_pos > 0.95:
            prob_up_5d -= 0.04
        elif boll_pos < 0.10:
            prob_up_5d += 0.03

    prob_up_20d += 0.10 if latest_close >= ma60 else -0.10
    prob_up_20d += 0.08 if ma20 >= ma60 else -0.08
    prob_up_20d += 0.08 if ret_20 > 0 else -0.08
    prob_up_20d += 0.04 if latest_close >= ma20 else -0.04
    if 45 <= rsi14 <= 68:
        prob_up_20d += 0.03
    elif rsi14 > 75:
        prob_up_20d -= 0.04
    if pd.notna(volume_ratio) and 0.9 <= volume_ratio <= 2.0:
        prob_up_20d += 0.02
    prob_up_20d += 0.03 if macd_hist > 0 else -0.03

    prob_up_5d = clamp_value(prob_up_5d, 0.08, 0.92)
    prob_up_20d = clamp_value(prob_up_20d, 0.08, 0.92)
    rule_prob_up_5d = float(prob_up_5d)
    rule_prob_up_20d = float(prob_up_20d)

    model_v3 = None
    model_prob_up_5d = None
    model_prob_up_20d = None
    blend_weight_model = 0.0

    if callable(score_security_timeseries_model):
        try:
            score_cols = [c for c in ['trade_date', 'close', 'turnover_rate', 'volume_ratio'] if c in df.columns]
            score_df = df[score_cols].copy()
            model_v3 = score_security_timeseries_model(score_df, security_type=security_type, topk=60)
        except Exception:
            model_v3 = None

    if model_v3:
        model_prob_up_5d = float(model_v3.get('prob_up_5d', rule_prob_up_5d))
        model_prob_up_20d = float(model_v3.get('prob_up_20d', rule_prob_up_20d))
        neighbor_count = int(model_v3.get('neighbor_count', 0))
        median_dist = float(model_v3.get('median_distance', 9.9))

        base_weight = 0.35 if neighbor_count >= 50 else 0.25
        if median_dist <= 1.2:
            base_weight += 0.10
        elif median_dist >= 2.0:
            base_weight -= 0.10
        blend_weight_model = clamp_value(base_weight, 0.15, 0.55)

        prob_up_5d = clamp_value((1 - blend_weight_model) * rule_prob_up_5d + blend_weight_model * model_prob_up_5d, 0.08, 0.92)
        prob_up_20d = clamp_value((1 - blend_weight_model) * rule_prob_up_20d + blend_weight_model * model_prob_up_20d, 0.08, 0.92)

        avg_ret_5d = float(model_v3.get('avg_future_ret_5d', 0.0))
        avg_ret_20d = float(model_v3.get('avg_future_ret_20d', 0.0))
        reasons.append(
            f"模型V3相似样本{neighbor_count}条，历史平均未来5日收益 {avg_ret_5d:+.2%} / 20日收益 {avg_ret_20d:+.2%}"
        )

    risk_score = 28.0
    risk_score += min(25.0, volatility20 * 0.8)
    if latest_close < ma20:
        risk_score += 12
    if latest_close < ma60:
        risk_score += 12
    if ret_20 < -0.10:
        risk_score += 8
    if rsi14 > 80:
        risk_score += 6
    if pd.notna(volume_ratio) and volume_ratio > 3.0:
        risk_score += 6
    if pd.notna(boll_pos) and boll_pos > 0.95:
        risk_score += 5
    if volatility20 > 45:
        risk_score += 6

    risk_score = clamp_value(risk_score, 0, 100)

    if risk_score >= 70:
        risk_level = '高风险'
    elif risk_score >= 45:
        risk_level = '中等风险'
    else:
        risk_level = '低到中等风险'

    support = last['support_20'] if pd.notna(last['support_20']) else last['support_60']
    resistance = last['resistance_20'] if pd.notna(last['resistance_20']) else last['resistance_60']
    support = float(support) if pd.notna(support) else latest_close * 0.95
    resistance = float(resistance) if pd.notna(resistance) else latest_close * 1.05

    distance_to_support = (latest_close / support - 1) if support > 0 else np.nan
    distance_to_resistance = (resistance / latest_close - 1) if latest_close > 0 else np.nan
    if pd.notna(distance_to_support) and distance_to_support < 0.03:
        reasons.append('当前价格靠近近20日支撑区，若能企稳有利于反弹/续涨')
    if pd.notna(distance_to_resistance) and distance_to_resistance < 0.03:
        risk_flags.append('当前价格接近近20日压力区，需关注突破有效性')

    bull_trigger = f"站稳 {ma20:.2f} 并放量突破 {resistance:.2f}"
    bear_trigger = f"跌破 {support:.2f} 且 MACD继续走弱"

    security_label = '个股' if security_type == 'stock' else '指数'
    summary = (
        f"该{security_label}当前处于“{trend_label}”状态，未来5日上涨概率约 {prob_up_5d:.0%}，"
        f"未来20日上涨概率约 {prob_up_20d:.0%}。"
        f"短线关注 {support:.2f} 一带支撑与 {resistance:.2f} 一带压力。"
    )

    return {
        'trend': trend_label,
        'trend_score': float(trend_score),
        'prob_up_5d': float(prob_up_5d),
        'prob_up_20d': float(prob_up_20d),
        'risk_score': float(risk_score),
        'risk_level': risk_level,
        'support': float(support),
        'resistance': float(resistance),
        'latest_close': latest_close,
        'ret_5': float(ret_5),
        'ret_20': float(ret_20),
        'rsi14': float(rsi14),
        'macd_hist': float(macd_hist),
        'boll_pos': None if pd.isna(boll_pos) else float(boll_pos),
        'volume_ratio': None if pd.isna(volume_ratio) else float(volume_ratio),
        'turnover_rate': None if pd.isna(turnover_rate) else float(turnover_rate),
        'volatility20': float(volatility20),
        'summary': summary,
        'bull_trigger': bull_trigger,
        'bear_trigger': bear_trigger,
        'prob_up_5d_rule': rule_prob_up_5d,
        'prob_up_20d_rule': rule_prob_up_20d,
        'prob_up_5d_model': model_prob_up_5d,
        'prob_up_20d_model': model_prob_up_20d,
        'blend_weight_model': float(blend_weight_model),
        'model_v3': model_v3,
        'reasons': reasons[:6],
        'risk_flags': risk_flags[:6],
    }


def render_security_trend_analysis(analysis: dict | None, security_type: str) -> None:
    st.markdown("##### 🔮 未来走势分析（V3）")
    st.caption("基于规则信号 + 历史相似样本模型的融合判断，用于辅助分析，不构成投资建议。")

    if not analysis:
        st.info('该标的数据长度不足，暂时无法生成走势分析。')
        return

    top_cols = st.columns(5)
    top_cols[0].metric('趋势状态', analysis['trend'])
    top_cols[1].metric('趋势分', f"{analysis['trend_score']:.1f}")
    top_cols[2].metric('5日上涨概率', f"{analysis['prob_up_5d']:.0%}")
    top_cols[3].metric('20日上涨概率', f"{analysis['prob_up_20d']:.0%}")
    top_cols[4].metric(analysis['risk_level'], f"{analysis['risk_score']:.1f}")

    model_v3 = analysis.get('model_v3')
    model_cols = st.columns(5)
    model_cols[0].metric('规则5日概率', f"{analysis.get('prob_up_5d_rule', 0):.0%}")
    model_cols[1].metric('规则20日概率', f"{analysis.get('prob_up_20d_rule', 0):.0%}")
    model_cols[2].metric('模型5日概率', f"{analysis.get('prob_up_5d_model'):.0%}" if analysis.get('prob_up_5d_model') is not None else '-')
    model_cols[3].metric('模型20日概率', f"{analysis.get('prob_up_20d_model'):.0%}" if analysis.get('prob_up_20d_model') is not None else '-')

    if model_v3:
        model_cols[4].metric('融合模式', '规则+模型')
        st.caption(
            f"模型权重 {analysis.get('blend_weight_model', 0):.0%} / 规则权重 {1-analysis.get('blend_weight_model', 0):.0%}"
            f"｜相似样本 {model_v3.get('neighbor_count', 0)} 条｜中位距离 {model_v3.get('median_distance', 0):.3f}"
        )
    else:
        model_cols[4].metric('融合模式', '仅规则')
        st.caption("模型样本未命中或该品种暂未覆盖，因此当前仅展示规则概率。")

    extra_cols = st.columns(5)
    extra_cols[0].metric('支撑位', f"{analysis['support']:.2f}")
    extra_cols[1].metric('压力位', f"{analysis['resistance']:.2f}")
    extra_cols[2].metric('近5日收益', f"{analysis['ret_5']:+.2%}")
    extra_cols[3].metric('近20日收益', f"{analysis['ret_20']:+.2%}")
    extra_cols[4].metric('MACD柱值', f"{analysis['macd_hist']:.4f}")

    st.info(analysis['summary'])

    trigger_cols = st.columns(2)
    trigger_cols[0].success(f"偏强触发条件：{analysis.get('bull_trigger', '-') }")
    trigger_cols[1].warning(f"转弱触发条件：{analysis.get('bear_trigger', '-') }")

    reason_cols = st.columns(2)
    with reason_cols[0]:
        st.markdown('**偏强依据**')
        if analysis.get('reasons'):
            for item in analysis['reasons']:
                st.markdown(f'- {item}')
        else:
            st.markdown('- 暂无明显偏强依据')
    with reason_cols[1]:
        st.markdown('**风险提示**')
        if analysis.get('risk_flags'):
            for item in analysis['risk_flags']:
                st.markdown(f'- {item}')
        else:
            st.markdown('- 当前未出现明显额外风险信号')


def get_security_metric_config(security_type: str) -> dict[str, dict[str, Union[str, float, int]]]:
    if security_type == 'stock':
        return {
            '收盘价(元)': {'column': 'close', 'scale': 1.0, 'digits': 2},
            '滚动市盈率PE_TTM': {'column': 'pe_ttm', 'scale': 1.0, 'digits': 2},
            '市净率PB': {'column': 'pb', 'scale': 1.0, 'digits': 2},
            '滚动市销率PS_TTM': {'column': 'ps_ttm', 'scale': 1.0, 'digits': 2},
            '换手率(%)': {'column': 'turnover_rate', 'scale': 1.0, 'digits': 2},
            '量比': {'column': 'volume_ratio', 'scale': 1.0, 'digits': 2},
            '总市值(亿元)': {'column': 'total_mv', 'scale': 10000.0, 'digits': 2},
            '流通市值(亿元)': {'column': 'circ_mv', 'scale': 10000.0, 'digits': 2},
            '总股本(亿股)': {'column': 'total_share', 'scale': 10000.0, 'digits': 2},
            '流通股本(亿股)': {'column': 'float_share', 'scale': 10000.0, 'digits': 2},
            '自由流通股本(亿股)': {'column': 'free_share', 'scale': 10000.0, 'digits': 2},
        }

    return {
        '收盘点位': {'column': 'close', 'scale': 1.0, 'digits': 2},
        '市盈率PE': {'column': 'pe', 'scale': 1.0, 'digits': 2},
        '滚动市盈率PE_TTM': {'column': 'pe_ttm', 'scale': 1.0, 'digits': 2},
        '市净率PB': {'column': 'pb', 'scale': 1.0, 'digits': 2},
        '换手率(%)': {'column': 'turnover_rate', 'scale': 1.0, 'digits': 2},
        '总市值(亿元)': {'column': 'total_mv', 'scale': 10000.0, 'digits': 2},
        '流通市值(亿元)': {'column': 'float_mv', 'scale': 10000.0, 'digits': 2},
        '总股本(亿股)': {'column': 'total_share', 'scale': 10000.0, 'digits': 2},
        '流通股本(亿股)': {'column': 'float_share', 'scale': 10000.0, 'digits': 2},
        '自由流通股本(亿股)': {'column': 'free_share', 'scale': 10000.0, 'digits': 2},
    }


def create_metric_line_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    yaxis_title: str,
    scale: float = 1.0,
    digits: int = 2,
    color: str = '#2563EB'
):
    if df is None or df.empty or y_col not in df.columns:
        return None

    chart_df = df[[x_col, y_col]].copy()
    chart_df[y_col] = pd.to_numeric(chart_df[y_col], errors='coerce') / scale
    chart_df = chart_df.dropna(subset=[y_col]).sort_values(x_col)
    if chart_df.empty:
        return None

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=chart_df[x_col],
        y=chart_df[y_col],
        mode='lines',
        name=title,
        line=dict(width=2.5, shape='spline', color=color),
        hovertemplate=f"%{{x|%Y-%m-%d}}<br>{yaxis_title}: %{{y:,.{digits}f}}<extra></extra>"
    ))
    fig.update_layout(
        title=dict(text=title, x=0.02, font=dict(size=18, color='#1E293B')),
        xaxis_title='日期',
        yaxis_title=yaxis_title,
        hovermode='x unified',
        height=360,
        template='plotly_white',
        plot_bgcolor='rgba(248, 250, 252, 0.5)',
        paper_bgcolor='rgba(248, 250, 252, 0.92)',
        font=dict(family='Inter, PingFang SC, sans-serif'),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(226,232,240,0.5)')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(226,232,240,0.5)')
    return fig


def create_financial_bar_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    yaxis_title: str,
    scale: float = 1.0,
    digits: int = 2,
    positive_color: str = '#2563EB',
    negative_color: str = '#10B981'
):
    if df is None or df.empty or y_col not in df.columns:
        return None

    chart_df = df[[x_col, y_col]].copy()
    chart_df[y_col] = pd.to_numeric(chart_df[y_col], errors='coerce') / scale
    chart_df = chart_df.dropna(subset=[y_col]).sort_values(x_col)
    if chart_df.empty:
        return None

    chart_df['label'] = pd.to_datetime(chart_df[x_col]).dt.strftime('%Y-%m-%d')
    chart_df['color'] = chart_df[y_col].apply(lambda value: positive_color if value >= 0 else negative_color)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=chart_df['label'],
        y=chart_df[y_col],
        marker_color=chart_df['color'],
        hovertemplate=f"%{{x}}<br>{yaxis_title}: %{{y:,.{digits}f}}<extra></extra>"
    ))
    fig.update_layout(
        title=dict(text=title, x=0.02, font=dict(size=18, color='#1E293B')),
        xaxis_title='报告期',
        yaxis_title=yaxis_title,
        height=360,
        template='plotly_white',
        plot_bgcolor='rgba(248, 250, 252, 0.5)',
        paper_bgcolor='rgba(248, 250, 252, 0.92)',
        font=dict(family='Inter, PingFang SC, sans-serif'),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(226,232,240,0.5)')
    return fig


def draw_metric_card(title: str, value: str, delta: str, delta_pct: str = None) -> str:
    """
    创建美观的指标卡片HTML

    Args:
        title: 卡片标题
        value: 当前数值
        delta: 变动值
        delta_pct: 变动百分比（可选）

    Returns:
        HTML字符串
    """
    # 判断涨跌（中国股市标准：红涨绿跌）
    is_positive = delta.startswith('+') if delta != '-' else None

    if is_positive is None:
        arrow = ""
        color = "#64748B"
    elif is_positive:
        arrow = "↑"
        color = "#EF4444"  # 红色表示上涨
    else:
        arrow = "↓"
        color = "#10B981"  # 绿色表示下跌

    delta_display = f"{arrow} {delta}" if delta != '-' else '-'
    if delta_pct and delta_pct != '-':
        delta_display += f" ({delta_pct})"

    card_html = f"""
    <div style="
        background: white;
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08), 0 1px 2px rgba(0, 0, 0, 0.06);
        transition: all 0.3s ease;
        border-left: 4px solid {color};
        height: 100%;
    " onmouseover="this.style.transform='scale(1.02)'; this.style.boxShadow='0 4px 6px rgba(0, 0, 0, 0.1)'"
       onmouseout="this.style.transform='scale(1)'; this.style.boxShadow='0 1px 3px rgba(0, 0, 0, 0.08)'">
        <div style="
            font-size: 0.875rem;
            font-weight: 600;
            color: #64748B;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.5rem;
        ">{title}</div>
        <div style="
            font-size: 2rem;
            font-weight: 700;
            color: #1E293B;
            margin-bottom: 0.5rem;
        ">{value}</div>
        <div style="
            font-size: 0.875rem;
            font-weight: 600;
            color: {color};
        ">{delta_display}</div>
    </div>
    """
    return card_html


def create_line_chart(filtered_df: pd.DataFrame, metric_name: str, is_aggregate: bool, selected_etfs: list = None, chart_type: str = 'line') -> go.Figure:
    """
    创建Plotly折线图

    Args:
        filtered_df: 筛选后的DataFrame
        metric_name: 指标名称
        is_aggregate: 是否显示汇总数据
        selected_etfs: 选中的ETF列表（非汇总模式）
        chart_type: 图表类型 ('line', 'area', 'scatter')

    Returns:
        Plotly Figure对象
    """
    # 专业金融调色盘
    color_palette = [
        '#2E5BFF', '#8E54E9', '#FF9966', '#00D4AA', '#FF6B9D',
        '#FFC233', '#00C9FF', '#FF5757', '#A0D911', '#9254DE'
    ]

    fig = go.Figure()

    if is_aggregate:
        # 单条线显示汇总数据
        agg_data = filtered_df[filtered_df['is_aggregate'] == True].sort_values('date')
        if len(agg_data) > 0:
            if chart_type == 'area':
                fig.add_trace(go.Scatter(
                    x=agg_data['date'],
                    y=agg_data['value'],
                    mode='lines',
                    name='所有ETF总和',
                    fill='tozeroy',
                    line=dict(width=3, shape='spline', color=color_palette[0]),
                    fillcolor='rgba(46, 91, 255, 0.1)',
                    hovertemplate='<b>%{x|%Y-%m-%d}</b><br>%{y:.2f}<extra></extra>'
                ))
            else:
                fig.add_trace(go.Scatter(
                    x=agg_data['date'],
                    y=agg_data['value'],
                    mode='lines',
                    name='所有ETF总和',
                    line=dict(width=3, shape='spline', color=color_palette[0]),
                    hovertemplate='<b>%{x|%Y-%m-%d}</b><br>%{y:.2f}<extra></extra>'
                ))
    else:
        # 多条线显示各个ETF
        if selected_etfs:
            # 前3个ETF高亮显示，其余半透明
            for idx, etf_name in enumerate(selected_etfs):
                etf_data = filtered_df[filtered_df['name'] == etf_name].sort_values('date')
                if len(etf_data) > 0:
                    color = color_palette[idx % len(color_palette)]
                    opacity = 1.0 if idx < 3 else 0.3
                    line_width = 2.5 if idx < 3 else 1.5

                    if chart_type == 'area':
                        fig.add_trace(go.Scatter(
                            x=etf_data['date'],
                            y=etf_data['value'],
                            mode='lines',
                            name=etf_name,
                            fill='tonexty',
                            line=dict(width=line_width, shape='spline', color=color),
                            opacity=opacity,
                            hovertemplate=f'<b>{etf_name}</b><br>%{{x|%Y-%m-%d}}<br>%{{y:.4f}}<extra></extra>'
                        ))
                    elif chart_type == 'scatter':
                        fig.add_trace(go.Scatter(
                            x=etf_data['date'],
                            y=etf_data['value'],
                            mode='markers',
                            name=etf_name,
                            marker=dict(size=8, opacity=opacity, color=color),
                            hovertemplate=f'<b>{etf_name}</b><br>%{{x|%Y-%m-%d}}<br>%{{y:.4f}}<extra></extra>'
                        ))
                    else:  # line
                        fig.add_trace(go.Scatter(
                            x=etf_data['date'],
                            y=etf_data['value'],
                            mode='lines',
                            name=etf_name,
                            line=dict(width=line_width, shape='spline', color=color),
                            opacity=opacity,
                            hovertemplate=f'<b>{etf_name}</b><br>%{{x|%Y-%m-%d}}<br>%{{y:.4f}}<extra></extra>'
                        ))

    # 布局配置 - 响应式设计
    fig.update_layout(
        title=dict(
            text=f'{metric_name} 变动趋势',
            font=dict(size=20, weight=700, color='#1E293B'),
            x=0.02
        ),
        xaxis_title='日期',
        yaxis_title=metric_name,
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.25,
            xanchor="center",
            x=0.5,
            bgcolor="rgba(255, 255, 255, 0)",
            font=dict(size=11)
        ),
        height=500,
        template='plotly_white',
        plot_bgcolor='rgba(248, 250, 252, 0.5)',
        paper_bgcolor='rgba(248, 250, 252, 0.92)',
        font=dict(family='Inter, PingFang SC, sans-serif'),
        margin=dict(l=20, r=20, t=60, b=20)
    )

    # 网格线样式
    fig.update_xaxes(
        rangeslider_visible=False,
        showgrid=True,
        gridwidth=1,
        gridcolor='rgba(226, 232, 240, 0.5)',
        showline=True,
        linewidth=1,
        linecolor='#E2E8F0'
    )

    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='rgba(226, 232, 240, 0.5)',
        showline=True,
        linewidth=1,
        linecolor='#E2E8F0',
        fixedrange=True
    )

    return fig


def calculate_statistics(filtered_df: pd.DataFrame, is_aggregate: bool, selected_etfs: list = None) -> pd.DataFrame:
    """
    计算统计信息 - 显示最新日期和前一天的数据对比

    Args:
        filtered_df: 筛选后的DataFrame
        is_aggregate: 是否为汇总数据
        selected_etfs: 选中的ETF列表（非汇总模式）

    Returns:
        包含统计信息的DataFrame
    """
    stats_list = []

    if is_aggregate:
        # 计算汇总数据的统计信息
        agg_data = filtered_df[filtered_df['is_aggregate'] == True].sort_values('date')
        if len(agg_data) >= 2:
            # 获取最新日期和前一天的数据
            latest_date = agg_data.iloc[-1]['date']
            latest_value = agg_data.iloc[-1]['value']
            prev_value = agg_data.iloc[-2]['value']

            change = latest_value - prev_value
            change_pct = (change / prev_value * 100) if prev_value != 0 else 0

            stats_list.append({
                'ETF名称': '所有ETF总和',
                '最新日期': latest_date.strftime('%Y-%m-%d'),
                '当日数据': f'{latest_value:.2f}',
                '前日数据': f'{prev_value:.2f}',
                '变动': f'{change:+.2f}',
                '变动幅度': f'{change_pct:+.2f}%'
            })
        elif len(agg_data) == 1:
            # 只有一天的数据
            latest_date = agg_data.iloc[-1]['date']
            latest_value = agg_data.iloc[-1]['value']

            stats_list.append({
                'ETF名称': '所有ETF总和',
                '最新日期': latest_date.strftime('%Y-%m-%d'),
                '当日数据': f'{latest_value:.2f}',
                '前日数据': '-',
                '变动': '-',
                '变动幅度': '-'
            })
    else:
        # 计算各个ETF的统计信息
        if selected_etfs:
            for etf_name in selected_etfs:
                etf_data = filtered_df[filtered_df['name'] == etf_name].sort_values('date')

                if len(etf_data) == 0:
                    continue

                # 根据数值大小确定小数位数
                sample_value = etf_data.iloc[-1]['value']
                decimals = 2 if sample_value > 100 else 4

                if len(etf_data) >= 2:
                    # 获取最新日期和前一天的数据
                    latest_date = etf_data.iloc[-1]['date']
                    latest_value = etf_data.iloc[-1]['value']
                    prev_value = etf_data.iloc[-2]['value']

                    change = latest_value - prev_value
                    change_pct = (change / prev_value * 100) if prev_value != 0 else 0

                    stats_list.append({
                        'ETF名称': etf_name,
                        '最新日期': latest_date.strftime('%Y-%m-%d'),
                        '当日数据': f'{latest_value:.{decimals}f}',
                        '前日数据': f'{prev_value:.{decimals}f}',
                        '变动': f'{change:+.{decimals}f}',
                        '变动幅度': f'{change_pct:+.2f}%'
                    })
                else:
                    # 只有一天的数据
                    latest_date = etf_data.iloc[-1]['date']
                    latest_value = etf_data.iloc[-1]['value']

                    stats_list.append({
                        'ETF名称': etf_name,
                        '最新日期': latest_date.strftime('%Y-%m-%d'),
                        '当日数据': f'{latest_value:.{decimals}f}',
                        '前日数据': '-',
                        '变动': '-',
                        '变动幅度': '-'
                    })

    return pd.DataFrame(stats_list)


def create_volume_stacked_bar(df: pd.DataFrame) -> go.Figure:
    """
    创建板块成交额堆叠柱状图

    Args:
        df: 成交量DataFrame

    Returns:
        Plotly Figure
    """
    # 板块颜色映射
    sector_colors = {
        '沪市主板': '#2E5BFF',
        '深市主板': '#00D4AA',
        '创业板': '#FF9966',
        '科创板': '#8E54E9',
    }

    fig = go.Figure()

    for sector_name in ['沪市主板', '深市主板', '创业板', '科创板']:
        sector_data = df[df['ts_name'] == sector_name].sort_values('trade_date')
        if len(sector_data) > 0:
            fig.add_trace(go.Bar(
                x=sector_data['trade_date'],
                y=sector_data['amount'],
                name=sector_name,
                marker_color=sector_colors.get(sector_name, '#999'),
                hovertemplate=f'<b>{sector_name}</b><br>%{{x|%Y-%m-%d}}<br>成交额: %{{y:.2f}} 亿元<extra></extra>'
            ))

    fig.update_layout(
        barmode='stack',
        title=dict(
            text='各板块每日成交额（亿元）',
            font=dict(size=20, weight=700, color='#1E293B'),
            x=0.02
        ),
        xaxis_title='日期',
        yaxis_title='成交额（亿元）',
        hovermode='x unified',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=-0.25,
            xanchor='center',
            x=0.5,
            bgcolor='rgba(255, 255, 255, 0)',
            font=dict(size=11)
        ),
        height=500,
        template='plotly_white',
        plot_bgcolor='rgba(248, 250, 252, 0.5)',
        paper_bgcolor='rgba(248, 250, 252, 0.92)',
        font=dict(family='Inter, PingFang SC, sans-serif'),
        margin=dict(l=20, r=20, t=60, b=20)
    )

    fig.update_xaxes(
        showgrid=True, gridwidth=1, gridcolor='rgba(226, 232, 240, 0.5)',
        showline=True, linewidth=1, linecolor='#E2E8F0'
    )
    fig.update_yaxes(
        showgrid=True, gridwidth=1, gridcolor='rgba(226, 232, 240, 0.5)',
        showline=True, linewidth=1, linecolor='#E2E8F0',
        fixedrange=True
    )

    return fig


def create_volume_total_line(df: pd.DataFrame) -> go.Figure:
    """
    创建总成交额趋势折线图

    Args:
        df: 成交量DataFrame

    Returns:
        Plotly Figure
    """
    # 按日期汇总
    daily_total = df.groupby('trade_date').agg({'amount': 'sum', 'vol': 'sum'}).reset_index()
    daily_total = daily_total.sort_values('trade_date')

    # 计算5日均线和20日均线
    daily_total['ma5'] = daily_total['amount'].rolling(window=5).mean()
    daily_total['ma20'] = daily_total['amount'].rolling(window=20).mean()

    fig = go.Figure()

    # 成交额柱状图（半透明背景）
    fig.add_trace(go.Bar(
        x=daily_total['trade_date'],
        y=daily_total['amount'],
        name='每日总成交额',
        marker_color='rgba(46, 91, 255, 0.25)',
        hovertemplate='<b>%{x|%Y-%m-%d}</b><br>成交额: %{y:.2f} 亿元<extra></extra>'
    ))

    # 5日均线
    fig.add_trace(go.Scatter(
        x=daily_total['trade_date'],
        y=daily_total['ma5'],
        mode='lines',
        name='5日均线',
        line=dict(width=2, color='#FF9966', shape='spline'),
        hovertemplate='<b>%{x|%Y-%m-%d}</b><br>5日均线: %{y:.2f} 亿元<extra></extra>'
    ))

    # 20日均线
    fig.add_trace(go.Scatter(
        x=daily_total['trade_date'],
        y=daily_total['ma20'],
        mode='lines',
        name='20日均线',
        line=dict(width=2.5, color='#EF4444', shape='spline'),
        hovertemplate='<b>%{x|%Y-%m-%d}</b><br>20日均线: %{y:.2f} 亿元<extra></extra>'
    ))

    fig.update_layout(
        title=dict(
            text='A股每日总成交额趋势',
            font=dict(size=20, weight=700, color='#1E293B'),
            x=0.02
        ),
        xaxis_title='日期',
        yaxis_title='成交额（亿元）',
        hovermode='x unified',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=-0.25,
            xanchor='center',
            x=0.5,
            bgcolor='rgba(255, 255, 255, 0)',
            font=dict(size=11)
        ),
        height=500,
        template='plotly_white',
        plot_bgcolor='rgba(248, 250, 252, 0.5)',
        paper_bgcolor='rgba(248, 250, 252, 0.92)',
        font=dict(family='Inter, PingFang SC, sans-serif'),
        margin=dict(l=20, r=20, t=60, b=20)
    )

    fig.update_xaxes(
        showgrid=True, gridwidth=1, gridcolor='rgba(226, 232, 240, 0.5)',
        showline=True, linewidth=1, linecolor='#E2E8F0'
    )
    fig.update_yaxes(
        showgrid=True, gridwidth=1, gridcolor='rgba(226, 232, 240, 0.5)',
        showline=True, linewidth=1, linecolor='#E2E8F0',
        fixedrange=True
    )

    return fig


def render_volume_tab():
    """渲染每日成交量Tab页内容"""
    st.subheader("📊 A股每日成交量")
    st.caption("数据来源: Tushare | 展示2024年以来各板块每日成交额")

    # 加载成交量数据
    vol_df = load_volume_dataframe()

    if vol_df is None or len(vol_df) == 0:
        st.warning("⚠️ 暂无成交量数据。请先运行 `python update_volume.py --full` 获取数据。")
        return

    # 侧边栏日期范围筛选
    vol_min_date = vol_df['trade_date'].min().date()
    vol_max_date = vol_df['trade_date'].max().date()

    iphone_mode = get_query_param_value("iphone_mode").strip() == "1"

    if iphone_mode:
        with st.expander("📅 成交量筛选", expanded=True):
            if vol_min_date == vol_max_date:
                st.info(f"📅 当前数据日期: {vol_min_date}")
                vol_date_range = (vol_min_date, vol_max_date)
            else:
                vol_date_range = st.slider(
                    "选择日期范围（成交量）",
                    min_value=vol_min_date,
                    max_value=vol_max_date,
                    value=(vol_min_date, vol_max_date),
                    format="YYYY-MM-DD",
                    key="iphone_vol_date_range"
                )

            all_sectors = sorted(vol_df['ts_name'].unique())
            selected_sectors = st.multiselect(
                "选择板块",
                options=all_sectors,
                default=all_sectors,
                key="iphone_vol_sectors"
            )
    else:
        st.sidebar.header("📅 成交量日期筛选")

        if vol_min_date == vol_max_date:
            st.sidebar.info(f"📅 当前数据日期: {vol_min_date}")
            vol_date_range = (vol_min_date, vol_max_date)
        else:
            vol_date_range = st.sidebar.slider(
                "选择日期范围（成交量）",
                min_value=vol_min_date,
                max_value=vol_max_date,
                value=(vol_min_date, vol_max_date),
                format="YYYY-MM-DD",
                key="vol_date_range"
            )

        # 板块筛选
        all_sectors = sorted(vol_df['ts_name'].unique())
        selected_sectors = st.sidebar.multiselect(
            "选择板块",
            options=all_sectors,
            default=all_sectors,
            key="vol_sectors"
        )

    # 筛选数据
    filtered_vol = vol_df[
        (vol_df['trade_date'].dt.date >= vol_date_range[0]) &
        (vol_df['trade_date'].dt.date <= vol_date_range[1]) &
        (vol_df['ts_name'].isin(selected_sectors))
    ].copy()

    if len(filtered_vol) == 0:
        st.warning("⚠️ 所选条件下没有数据，请调整筛选条件")
        return

    # 关键指标卡片
    daily_total = filtered_vol.groupby('trade_date')['amount'].sum().reset_index().sort_values('trade_date')

    if len(daily_total) >= 1:
        latest_date = daily_total.iloc[-1]['trade_date']
        latest_total = daily_total.iloc[-1]['amount']

        card_cols = st.columns(5)

        # 最新总成交额
        with card_cols[0]:
            if len(daily_total) >= 2:
                prev_total = daily_total.iloc[-2]['amount']
                change = latest_total - prev_total
                change_pct = f"{change / prev_total * 100:+.2f}%" if prev_total else '-'
                delta_str = f"{change:+.2f}"
            else:
                delta_str = '-'
                change_pct = '-'
            st.markdown(
                draw_metric_card('总成交额（亿元）', f'{latest_total:,.2f}', delta_str, change_pct),
                unsafe_allow_html=True
            )

        # 各板块最新成交额
        latest_by_sector = filtered_vol[filtered_vol['trade_date'] == latest_date]
        
        # 获取前一日数据备用
        if len(daily_total) >= 2:
            prev_date = daily_total.iloc[-2]['trade_date']
            prev_by_sector = filtered_vol[filtered_vol['trade_date'] == prev_date]
        else:
            prev_by_sector = pd.DataFrame()

        sector_order = ['沪市主板', '深市主板', '创业板', '科创板']
        displayed = 0
        for sector in sector_order:
            if displayed >= 4:
                break
            sector_row = latest_by_sector[latest_by_sector['ts_name'] == sector]
            if len(sector_row) > 0:
                val = sector_row.iloc[0]['amount']
                
                # 计算变动情况
                sec_delta_str = '-'
                sec_change_pct = '-'
                if not prev_by_sector.empty:
                    prev_sector_row = prev_by_sector[prev_by_sector['ts_name'] == sector]
                    if len(prev_sector_row) > 0:
                        prev_val = prev_sector_row.iloc[0]['amount']
                        sec_change = val - prev_val
                        sec_change_pct = f"{sec_change / prev_val * 100:+.2f}%" if prev_val else '-'
                        sec_delta_str = f"{sec_change:+.2f}"

                with card_cols[displayed + 1]:
                    st.markdown(
                        draw_metric_card(sector, f'{val:,.2f}', sec_delta_str, sec_change_pct),
                        unsafe_allow_html=True
                    )
                displayed += 1

    st.markdown("<br>", unsafe_allow_html=True)

    # 堆叠柱状图 - 各板块成交额
    fig_stacked = create_volume_stacked_bar(filtered_vol)
    st.plotly_chart(fig_stacked, use_container_width=True)

    # 总量趋势线
    fig_total = create_volume_total_line(filtered_vol)
    st.plotly_chart(fig_total, use_container_width=True)

    # 数据明细表格
    st.subheader("📋 成交量数据明细")

    # 透视表格：日期x板块
    pivot_df = filtered_vol.pivot_table(
        index='trade_date',
        columns='ts_name',
        values='amount',
        aggfunc='sum'
    ).reset_index()
    pivot_df['trade_date'] = pivot_df['trade_date'].dt.strftime('%Y-%m-%d')
    pivot_df = pivot_df.rename(columns={'trade_date': '日期'})

    # 添加总计列
    numeric_cols = [c for c in pivot_df.columns if c != '日期']
    pivot_df['总计'] = pivot_df[numeric_cols].sum(axis=1)

    # 按日期降序排列（最新在前）
    pivot_df = pivot_df.sort_values('日期', ascending=False)

    st.dataframe(
        pivot_df,
        use_container_width=True,
        hide_index=True,
        height=400
    )


# 主应用
def main():
    """主应用逻辑"""
    hydrate_security_jump_from_query_params()

    # ===== iPhone only mode (no sidebar dependency) =====
    iphone_mode = get_query_param_value("iphone_mode").strip() == "1"

    if iphone_mode:
        st.title("WealthSpark")
        st.caption("iPhone 模式")
    else:
        st.title("WealthSpark 决策看板")
        st.caption("趋势 × 资金流 × 情绪，一页看懂今日机会")
        st.caption("📌 Version 4.5 - 新增策略收益趋势图与累计收益曲线（规则/模型/混合）(2026-04-20)")

    # 显示最后更新时间
    try:
        import json
        import os
        if os.path.exists('last_update.json'):
            with open('last_update.json', 'r') as f:
                update_info = json.load(f)
                update_date = update_info.get('update_date', '未知')
                last_update = update_info.get('last_update', '未知')
                if iphone_mode:
                    st.caption(f"📅 更新: {update_date}")
                else:
                    st.info(f"📅 数据最后更新: {update_date} (GitHub Action 执行时间: {last_update})")
    except Exception as e:
        pass  # 如果文件不存在或读取失败，不显示更新时间

    # 处理外部跳转请求（例如从榜单点击跳到个股查询）
    trigger_security_tab_jump_if_needed()

    if not iphone_mode:
        st.markdown(
            '<div style="margin:0.25rem 0 0.75rem 0;"><a href="?iphone_mode=1" '
            'style="display:inline-block;padding:0.45rem 0.8rem;border-radius:999px;'
            'background:#2563eb;color:#fff;text-decoration:none;font-weight:700;">📱 iPhone模式</a></div>',
            unsafe_allow_html=True,
        )

    if iphone_mode:
        st.markdown(
            """
            <style>
            [data-testid="stSidebar"],
            [data-testid="collapsedControl"],
            button[aria-label="Open sidebar"],
            button[aria-label="Close sidebar"] {
                display: none !important;
            }
            .main .block-container {
                max-width: 100% !important;
                padding: 1rem 1rem 3rem 1rem !important;
            }
            div[data-testid="stExpander"] {
                border: 1px solid #dbeafe !important;
                border-radius: 14px !important;
                background: linear-gradient(180deg, #f8fbff 0%, #eef4ff 100%) !important;
                overflow: hidden !important;
                box-shadow: 0 8px 24px rgba(37, 99, 235, 0.08) !important;
                margin-bottom: 1rem !important;
            }
            div[data-testid="stExpander"] details summary {
                padding: 0.85rem 1rem !important;
                font-weight: 700 !important;
                color: #1e3a8a !important;
            }
            div[data-testid="stExpanderDetails"] {
                padding: 0.2rem 1rem 1rem 1rem !important;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        st.markdown(
            '<div style="margin-bottom:0.6rem;color:#475569;">'
            '已启用 iPhone 专用模式（不依赖 sidebar）。 '
            '<a href="?" style="color:#64748b;text-decoration:none;">退出 iPhone 模式</a>'
            '</div>',
            unsafe_allow_html=True,
        )

        mobile_group = st.radio(
            "模块",
            ["决策", "ETF", "个股", "资金", "宏观"],
            horizontal=True,
            key="iphone_group_radio",
        )

        if mobile_group == "决策":
            mobile_page = st.selectbox(
                "页面",
                ["💼 今日机会清单", "⭐ 每日趋势推荐", "🧪 推荐评估"],
                key="iphone_page_decision",
            )
            st.caption(f"当前位置：决策 / {mobile_page}")
            if mobile_page == "💼 今日机会清单":
                render_commercial_mvp_tab()
            elif mobile_page == "⭐ 每日趋势推荐":
                render_daily_trend_reco_tab()
            else:
                render_reco_effectiveness_tracking_panel()

        elif mobile_group == "ETF":
            mobile_page = st.selectbox(
                "页面",
                ["📈 ETF份额变动", "📊 每日成交量", "🥧 ETF分类占比", "📈 ETF分类趋势", "📊 宽基指数ETF"],
                key="iphone_page_etf",
            )
            st.caption(f"当前位置：ETF / {mobile_page}")
            if mobile_page == "📈 ETF份额变动":
                render_etf_tab()
            elif mobile_page == "📊 每日成交量":
                render_volume_tab()
            elif mobile_page == "🥧 ETF分类占比":
                render_etf_category_ratio_tab()
            elif mobile_page == "📈 ETF分类趋势":
                render_etf_trend_tab()
            else:
                render_wide_index_tab()

        elif mobile_group == "个股":
            mobile_page = st.selectbox(
                "页面",
                ["🔎 个股/指数查询", "🏢 公司筛选", "🎯 技术选股"],
                key="iphone_page_stock",
            )
            st.caption(f"当前位置：个股 / {mobile_page}")
            if mobile_page == "🔎 个股/指数查询":
                render_security_search_tab()
            elif mobile_page == "🏢 公司筛选":
                render_company_screener_tab()
            else:
                render_tech_picker_tab()

        elif mobile_group == "资金":
            mobile_page = st.selectbox(
                "页面",
                ["💹 资金流向", "🏦 公募持仓热股", "🔥 打板情绪", "🧨 游资名录"],
                key="iphone_page_money",
            )
            st.caption(f"当前位置：资金 / {mobile_page}")
            if mobile_page == "💹 资金流向":
                render_moneyflow_tab()
            elif mobile_page == "🏦 公募持仓热股":
                render_fund_hot_stocks_tab()
            elif mobile_page == "🔥 打板情绪":
                render_limitup_monitor_tab()
            else:
                render_hotmoney_tab()

        else:
            st.caption("当前位置：宏观 / 🌏 宏观经济")
            render_macro_tab()

        st.stop()

    # ===== 方案B进阶版：sidebar 顶部一级导航 + 二级页面 + 下方动态筛选 =====
    st.sidebar.markdown("---")
    st.sidebar.header("🧭 页面导航")

    nav_group = st.sidebar.radio(
        "选择模块",
        ["决策", "ETF", "个股", "资金", "宏观"],
        key="sidebar_nav_group"
    )

    st.sidebar.markdown("**当前页面**")

    if nav_group == "决策":
        decision_subpage = st.sidebar.radio(
            "决策模块",
            ["💼 今日机会清单", "⭐ 每日趋势推荐", "🧪 推荐评估"],
            key="decision_subpage"
        )
        st.caption(f"当前位置：决策 / {decision_subpage}")
        if decision_subpage == "💼 今日机会清单":
            render_commercial_mvp_tab()
        elif decision_subpage == "⭐ 每日趋势推荐":
            render_daily_trend_reco_tab()
        else:
            render_reco_effectiveness_tracking_panel()

    elif nav_group == "ETF":
        etf_subpage = st.sidebar.radio(
            "ETF模块",
            ["📈 ETF份额变动", "📊 每日成交量", "🥧 ETF分类占比", "📈 ETF分类趋势", "📊 宽基指数ETF"],
            key="etf_subpage"
        )
        st.caption(f"当前位置：ETF / {etf_subpage}")
        if etf_subpage == "📈 ETF份额变动":
            render_etf_tab()
        elif etf_subpage == "📊 每日成交量":
            render_volume_tab()
        elif etf_subpage == "🥧 ETF分类占比":
            render_etf_category_ratio_tab()
        elif etf_subpage == "📈 ETF分类趋势":
            render_etf_trend_tab()
        else:
            render_wide_index_tab()

    elif nav_group == "个股":
        stock_subpage = st.sidebar.radio(
            "个股模块",
            ["🔎 个股/指数查询", "🏢 公司筛选", "🎯 技术选股"],
            key="stock_subpage"
        )
        st.caption(f"当前位置：个股 / {stock_subpage}")
        if stock_subpage == "🔎 个股/指数查询":
            render_security_search_tab()
        elif stock_subpage == "🏢 公司筛选":
            render_company_screener_tab()
        else:
            render_tech_picker_tab()

    elif nav_group == "资金":
        money_subpage = st.sidebar.radio(
            "资金模块",
            ["💹 资金流向", "🏦 公募持仓热股", "🔥 打板情绪", "🧨 游资名录"],
            key="money_subpage"
        )
        st.caption(f"当前位置：资金 / {money_subpage}")
        if money_subpage == "💹 资金流向":
            render_moneyflow_tab()
        elif money_subpage == "🏦 公募持仓热股":
            render_fund_hot_stocks_tab()
        elif money_subpage == "🔥 打板情绪":
            render_limitup_monitor_tab()
        else:
            render_hotmoney_tab()

    else:
        st.caption("当前位置：宏观 / 🌏 宏观经济")
        render_macro_tab()




# ===== 商业化MVP Tab =====
def render_commercial_mvp_tab():
    st.subheader("💼 今日机会清单（商业化MVP）")
    st.caption("整合趋势推荐 + 资金流 + 情绪快照，形成更接近产品化的每日机会视图。免费版看精简结果，Pro 版看完整候选与详细理由。")

    with st.expander("🔐 Pro 会员入口", expanded=False):
        pro_pwd = get_pro_access_password()
        if not pro_pwd:
            st.info("当前未配置 Pro 口令（可通过 secrets.pro_access_password 或 ETF_PRO_ACCESS_PASSWORD 配置）。")
        else:
            if has_pro_access():
                col_a, col_b = st.columns([2, 1])
                with col_a:
                    st.success("已解锁 Pro 视图")
                with col_b:
                    if st.button("退出 Pro", key="btn_pro_logout"):
                        clear_pro_access()
                        st.rerun()
            else:
                col_a, col_b = st.columns([2, 1])
                with col_a:
                    input_pwd = st.text_input("输入 Pro 口令", type="password", key="pro_pwd_input")
                with col_b:
                    if st.button("解锁 Pro", type="primary", key="btn_pro_login"):
                        if grant_pro_access(input_pwd):
                            st.success("Pro 解锁成功")
                            st.rerun()
                        else:
                            st.error("口令错误，请重试")

    payload = load_trend_recommendations()
    if not payload:
        st.info("暂无趋势推荐数据，请先完成当日更新。")
        return

    top_up = pd.DataFrame(payload.get("top_uptrend", []) or [])
    top_avoid = pd.DataFrame(payload.get("top_avoid", []) or [])
    trade_date = str(payload.get("trade_date") or "").replace("-", "")

    mf_top = pd.DataFrame()
    try:
        from src.moneyflow_fetcher import _get_engine_cached, get_moneyflow_latest_date, query_moneyflow_daily_top
        _mf_engine = _get_engine_cached()
        if not trade_date:
            trade_date = str(get_moneyflow_latest_date(_mf_engine) or "")
        if trade_date:
            mf_top = query_moneyflow_daily_top(trade_date, top_n=80, engine=_mf_engine)
    except Exception as exc:
        logger.warning(f"render_commercial_mvp_tab moneyflow load failed: {exc}")

    emotion_stage = "-"
    limitup_cnt = None
    try:
        from src.limitup_monitor import get_limitup_latest_date, query_limitup_emotion_daily
        from src.moneyflow_fetcher import _get_engine_cached as _get_mf_engine_for_limitup
        _lu_engine = _get_mf_engine_for_limitup()
        lu_latest = get_limitup_latest_date(_lu_engine)
        if lu_latest:
            em_df = query_limitup_emotion_daily(lu_latest, lu_latest, engine=_lu_engine)
            if em_df is not None and not em_df.empty:
                row = em_df.iloc[-1]
                emotion_stage = str(row.get("emotion_stage") or "-")
                try:
                    limitup_cnt = int(row.get("up_cnt") or 0)
                except Exception:
                    limitup_cnt = None
    except Exception as exc:
        logger.warning(f"render_commercial_mvp_tab limitup load failed: {exc}")

    opp_df = build_opportunity_snapshot(top_up, top_avoid, mf_top, emotion_stage=emotion_stage)
    if opp_df.empty:
        st.info("当前可用样本不足，稍后再试。")
        return

    hot_stock_name = "-"
    try:
        from src.fund_hot_stocks import get_engine as get_fund_hot_engine, get_latest_agg_period, query_hot_stocks_leaderboard
        _fh_engine = get_fund_hot_engine()
        latest_period = get_latest_agg_period(_fh_engine)
        if latest_period:
            top_hot_df = query_hot_stocks_leaderboard(
                period=str(latest_period).replace("-", ""),
                top_n=1,
                order_by="heat_score",
                min_holding_funds=3,
                fund_type_filter="全部",
                engine=_fh_engine,
            )
            if top_hot_df is not None and not top_hot_df.empty:
                hot_stock_name = str(top_hot_df.iloc[0].get("ts_name") or "-")
    except Exception as exc:
        logger.warning(f"render_commercial_mvp_tab fund hot load failed: {exc}")

    best_row = opp_df.iloc[0]
    avg_prob5 = opp_df["prob_up_5d_final"].astype(float).mean() if "prob_up_5d_final" in opp_df.columns else (opp_df["prob_up_5d"].astype(float).mean() if "prob_up_5d" in opp_df.columns else np.nan)
    best_name = str(best_row.get("name") or "-")
    best_action = str(best_row.get("action") or "观察")
    best_reason = str(best_row.get("reason") or "").strip()

    pulse_cols = st.columns(5)
    pulse_cols[0].metric("交易日", payload.get("trade_date") or "-")
    pulse_cols[1].metric("机会样本", str(len(opp_df)))
    pulse_cols[2].metric("榜首机会", best_name)
    pulse_cols[3].metric("平均5日概率", f"{avg_prob5:.0%}" if pd.notna(avg_prob5) else "-")
    pulse_cols[4].metric("情绪阶段", emotion_stage)

    summary_bits = []
    if emotion_stage and emotion_stage != "-":
        summary_bits.append(f"当前市场情绪处于【{emotion_stage}】阶段")
    if limitup_cnt is not None:
        summary_bits.append(f"当日涨停数为 {limitup_cnt} 家")
    if pd.notna(avg_prob5):
        summary_bits.append(f"机会池平均 5 日上涨概率约为 {avg_prob5:.0%}")

    summary_line_1 = "，".join(summary_bits[:2]) + "。" if summary_bits else "今日机会页已根据最新数据完成刷新。"
    summary_line_2 = f"当前榜首候选是【{best_name}】，模型动作建议为「{best_action}」。"
    if best_reason:
        summary_line_2 += f" 主要原因：{best_reason}"
    summary_line_3 = f"公募热股代表当前显示为【{hot_stock_name}】；更适合结合趋势、资金与自选池一起看，而不是单看单一信号。"

    st.success(f"📌 今日摘要：{summary_line_1}")
    st.markdown(f"- {summary_line_2}\n- {summary_line_3}")

    st.markdown("#### 🔍 免费版 vs Pro 版")
    diff_left, diff_right = st.columns(2)
    with diff_left:
        st.info("""**免费版**

- 查看 Top3 精简机会
- 快速了解今日最值得先看的标的
- 适合先判断当天是否有研究价值""")
    with diff_right:
        st.success("""**Pro 版**

- 查看 Top20 完整候选池
- 解锁详细理由、风险分与更多字段
- 更适合做盘前筛选和自选池管理""")

    is_pro = has_pro_access()

    display_df = opp_df.copy()
    display_df["收盘价"] = pd.to_numeric(display_df.get("close"), errors="coerce").map(lambda x: f"{x:.2f}" if pd.notna(x) else "-")
    display_df["趋势分"] = pd.to_numeric(display_df.get("trend_score"), errors="coerce").map(lambda x: f"{x:.1f}" if pd.notna(x) else "-")
    display_df["风险分"] = pd.to_numeric(display_df.get("risk_score"), errors="coerce").map(lambda x: f"{x:.1f}" if pd.notna(x) else "-")
    display_df["5日概率"] = pd.to_numeric(display_df.get("prob_up_5d_final", display_df.get("prob_up_5d")), errors="coerce").map(lambda x: f"{x:.0%}" if pd.notna(x) else "-")
    display_df["20日概率"] = pd.to_numeric(display_df.get("prob_up_20d_final", display_df.get("prob_up_20d")), errors="coerce").map(lambda x: f"{x:.0%}" if pd.notna(x) else "-")
    display_df["主力净流入(万元)"] = pd.to_numeric(display_df.get("net_mf_amount"), errors="coerce").map(lambda x: f"{x:,.0f}" if pd.notna(x) else "-")
    display_df["机会分"] = pd.to_numeric(display_df.get("opportunity_score"), errors="coerce").map(lambda x: f"{x:.1f}" if pd.notna(x) else "-")
    display_df["置信度"] = display_df.get("confidence", "-")
    display_df["模型5日概率"] = pd.to_numeric(display_df.get("model_prob_up_5d"), errors="coerce").map(lambda x: f"{x:.0%}" if pd.notna(x) else "-")
    display_df["模型一致性"] = pd.to_numeric(display_df.get("model_agreement"), errors="coerce").map(lambda x: f"{x:.0%}" if pd.notna(x) else "-")
    display_df["融合权重"] = display_df.apply(lambda r: f"规则{_safe_float(r.get('blend_rule_weight'), 0.65):.0%} / 模型{_safe_float(r.get('blend_model_weight'), 0.35):.0%}", axis=1)
    display_df = display_df.rename(columns={"rank_commercial": "机会排名", "ts_code": "代码", "name": "名称", "industry": "行业", "reason": "原因", "action": "建议动作"})

    st.markdown("#### 🎯 今日机会清单")
    if is_pro:
        st.success("你正在查看 Pro 完整版（Top20 + 详细字段）。")
        cols = ["机会排名", "代码", "名称", "行业", "收盘价", "趋势分", "风险分", "5日概率", "20日概率", "模型5日概率", "模型一致性", "融合权重", "主力净流入(万元)", "机会分", "置信度", "建议动作", "原因"]
        st.dataframe(display_df[cols].head(20), use_container_width=True, hide_index=True, height=470)
    else:
        st.info("免费版仅展示 Top3 精简字段，适合先快速判断今天值不值得深入研究。")
        cols = ["机会排名", "代码", "名称", "行业", "趋势分", "5日概率", "置信度", "建议动作"]
        st.dataframe(display_df[cols].head(3), use_container_width=True, hide_index=True, height=220)
        st.warning("解锁 Pro 可查看 Top20、详细理由、风险分以及更多候选标的。")

    st.markdown("#### ⭐ 自选池体验版")
    st.caption("把你常看的标的放进来，可以快速判断它今天是落在强势榜、避雷榜，还是只是资金流短期关注。")

    if "mvp_watchlist" not in st.session_state:
        st.session_state["mvp_watchlist"] = []

    col_in, col_add, col_clear = st.columns([4, 1, 1])
    with col_in:
        watch_raw = st.text_input("添加自选（例：600519.SH, 宁德时代, 000001.SZ）", key="mvp_watch_input")
    with col_add:
        if st.button("加入", key="btn_mvp_watch_add"):
            new_tokens = parse_watchlist_input(watch_raw)
            merged = list(dict.fromkeys((st.session_state.get("mvp_watchlist", []) + new_tokens)))
            st.session_state["mvp_watchlist"] = merged
            st.success(f"已加入 {len(new_tokens)} 个标的")
    with col_clear:
        if st.button("清空", key="btn_mvp_watch_clear"):
            st.session_state["mvp_watchlist"] = []
            st.success("自选池已清空")

    watchlist = st.session_state.get("mvp_watchlist", [])
    if not watchlist:
        st.info("自选池为空，先添加几个你常看的标的吧～")
        return

    search_frames = []
    up_df = top_up.copy() if top_up is not None else pd.DataFrame()
    if not up_df.empty:
        up_df["list_tag"] = "趋势强势榜"
        search_frames.append(up_df)

    avoid_df = top_avoid.copy() if top_avoid is not None else pd.DataFrame()
    if not avoid_df.empty:
        avoid_df["list_tag"] = "趋势避雷榜"
        search_frames.append(avoid_df)

    mf_df = mf_top.copy() if mf_top is not None else pd.DataFrame()
    if not mf_df.empty:
        if "rank" not in mf_df.columns:
            mf_df = mf_df.reset_index(drop=True)
            mf_df["rank"] = mf_df.index + 1
        if "name" not in mf_df.columns:
            mf_df["name"] = mf_df.get("ts_code", "")
        if "industry" not in mf_df.columns:
            mf_df["industry"] = "-"
        if "prob_up_5d" not in mf_df.columns:
            mf_df["prob_up_5d"] = np.nan
        if "risk_score" not in mf_df.columns:
            mf_df["risk_score"] = np.nan
        if "trend_score" not in mf_df.columns:
            mf_df["trend_score"] = np.nan
        if "reason" not in mf_df.columns:
            mf_df["reason"] = "当日主力资金净流入关注"
        mf_df["list_tag"] = "资金流关注榜"
        search_frames.append(mf_df)

    if not search_frames:
        st.info("暂无可用于自选匹配的数据。")
        return

    pool = pd.concat(search_frames, ignore_index=True, sort=False)
    if "ts_code" not in pool.columns:
        pool["ts_code"] = ""
    if "name" not in pool.columns:
        pool["name"] = ""
    pool["ts_code"] = pool["ts_code"].astype(str)
    pool["name"] = pool["name"].astype(str)

    records = []
    for token in watchlist:
        key = str(token).strip()
        if not key:
            continue

        key_upper = key.upper()
        key_compact = key_upper.replace(" ", "")
        key_code = key_compact.split(".")[0]
        is_code_like = key_code.isdigit() and len(key_code) in (6, 8)

        hit_mask = (
            (pool["ts_code"].str.upper() == key_upper)
            | (pool["name"].str.contains(key, case=False, na=False))
        )
        if is_code_like:
            hit_mask = hit_mask | pool["ts_code"].str.upper().str.startswith(f"{key_code}.")

        hit = pool[hit_mask]
        if hit.empty:
            resolved_code = key if is_code_like else "-"
            resolved_name = key if not is_code_like else "-"

            try:
                fallback_df = load_security_search(key, "stock", limit=10)
                if fallback_df is not None and not fallback_df.empty:
                    fallback = fallback_df.iloc[0]
                    resolved_code = str(
                        fallback.get("ts_code")
                        or fallback.get("code")
                        or fallback.get("symbol")
                        or resolved_code
                    )
                    resolved_name = str(
                        fallback.get("name")
                        or fallback.get("ts_name")
                        or fallback.get("security_name")
                        or resolved_name
                    )
            except Exception:
                pass

            records.append({
                "输入": key,
                "代码": resolved_code,
                "名称": resolved_name,
                "来源": "未进入今日机会池",
                "5日概率": "-",
                "风险分": "-",
                "建议": "继续观察",
            })
            continue

        first = hit.iloc[0]
        prob5 = pd.to_numeric(first.get("prob_up_5d"), errors="coerce")
        risk = pd.to_numeric(first.get("risk_score"), errors="coerce")
        source = str(first.get("list_tag") or "模型池")
        advice = "观察"
        if "避雷" in source:
            advice = "谨慎回避"
        elif "强势" in source and (pd.isna(risk) or risk <= 55):
            advice = "重点跟踪"
        elif "资金流" in source:
            advice = "关注资金持续性"

        records.append({
            "输入": key,
            "代码": str(first.get("ts_code") or "-"),
            "名称": str(first.get("name") or "-"),
            "来源": source,
            "5日概率": f"{prob5:.0%}" if pd.notna(prob5) else "-",
            "风险分": f"{risk:.1f}" if pd.notna(risk) else "-",
            "建议": advice,
        })

    watch_df = pd.DataFrame(records)
    st.dataframe(watch_df, use_container_width=True, hide_index=True, height=320)

def render_daily_trend_reco_tab():
    st.subheader("⭐ 每日趋势推荐")
    st.caption("基于日更后的最新日线数据，自动筛选 Top10 上涨趋势最强股票 与 Top10 避雷股票。仅供辅助分析，不构成投资建议。")

    payload = load_trend_recommendations()
    if not payload:
        st.info("暂无每日趋势推荐结果，请先完成当日数据更新。")
        return

    meta_cols = st.columns(4)
    meta_cols[0].metric("交易日", payload.get('trade_date') or '-')
    meta_cols[1].metric("样本池", f"{int(payload.get('universe_size') or 0):,}")
    meta_cols[2].metric("强势股数量", str(len(payload.get('top_uptrend', []) or [])))
    meta_cols[3].metric("避雷股数量", str(len(payload.get('top_avoid', []) or [])))

    generated_at = payload.get('generated_at') or '-'
    st.caption(f"生成时间：{generated_at}")

    left, right = st.columns(2)

    from urllib.parse import quote

    render_nonce = st.session_state.get('daily_trend_reco_render_nonce', 0) + 1
    st.session_state['daily_trend_reco_render_nonce'] = render_nonce

    def _format_frame(rows: list[dict], mode: str) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=["查询", "排名", "代码", "名称", "行业", "收盘价", "趋势分", "风险分", "5日概率", "20日概率", "原因"])
        df = pd.DataFrame(rows).copy()
        df = df[["rank", "ts_code", "name", "industry", "close", "trend_score", "risk_score", "prob_up_5d", "prob_up_20d", "reason"]]
        df.columns = ["排名", "代码", "名称", "行业", "收盘价", "趋势分", "风险分", "5日概率", "20日概率", "原因"]
        query_links = []
        for _, row in df.iterrows():
            query = str(row.get('代码') or row.get('名称') or '').strip()
            if not query:
                query_links.append('#')
            else:
                query_links.append(
                    f"?security_query={quote(query)}&security_type=stock&open_tab=security&jump_nonce={render_nonce}_{quote(query)}"
                )
        df.insert(0, "查询", query_links)
        df["收盘价"] = pd.to_numeric(df["收盘价"], errors='coerce').map(lambda x: f"{x:.2f}" if pd.notna(x) else '-')
        df["趋势分"] = pd.to_numeric(df["趋势分"], errors='coerce').map(lambda x: f"{x:.1f}" if pd.notna(x) else '-')
        df["风险分"] = pd.to_numeric(df["风险分"], errors='coerce').map(lambda x: f"{x:.1f}" if pd.notna(x) else '-')
        df["5日概率"] = pd.to_numeric(df["5日概率"], errors='coerce').map(lambda x: f"{x:.0%}" if pd.notna(x) else '-')
        df["20日概率"] = pd.to_numeric(df["20日概率"], errors='coerce').map(lambda x: f"{x:.0%}" if pd.notna(x) else '-')
        return df

    st.info("💡 点击表格最左侧“🔎 查看”可直接跳到“个股/指数查询”，查看该股票的详细走势分析。")

    with left:
        st.markdown("#### 📈 十大上涨趋势最强")
        top_up = payload.get('top_uptrend', []) or []
        if top_up:
            best = top_up[0]
            st.success(
                f"No.1 {best.get('name', '-') }（{best.get('ts_code', '-') }）｜趋势分 {best.get('trend_score', 0):.1f}｜5日概率 {best.get('prob_up_5d', 0):.0%}"
            )
        st.dataframe(
            _format_frame(top_up, 'up'),
            use_container_width=True,
            hide_index=True,
            height=420,
            column_config={
                '查询': st.column_config.LinkColumn(
                    '查询',
                    help='点击后跳转到个股/指数查询',
                    display_text='🔎 查看'
                )
            }
        )

    with right:
        st.markdown("#### ⚠️ 十大避雷股票")
        top_avoid = payload.get('top_avoid', []) or []
        if top_avoid:
            worst = top_avoid[0]
            st.warning(
                f"No.1 {worst.get('name', '-') }（{worst.get('ts_code', '-') }）｜风险分 {worst.get('risk_score', 0):.1f}｜5日概率 {worst.get('prob_up_5d', 0):.0%}"
            )
        st.dataframe(
            _format_frame(top_avoid, 'avoid'),
            use_container_width=True,
            hide_index=True,
            height=420,
            column_config={
                '查询': st.column_config.LinkColumn(
                    '查询',
                    help='点击后跳转到个股/指数查询',
                    display_text='🔎 查看'
                )
            }
        )


def render_hotmoney_tab():
    st.subheader("🧨 游资名录与博弈明细")
    st.caption("基于 Tushare 游资名录（hm_list）与游资每日明细（hm_detail），观察活跃游资、偏好个股与净买卖。")

    from src.hotmoney_monitor import (
        get_hotmoney_latest_detail_date,
        get_hotmoney_sync_meta,
        query_hotmoney_list,
        query_hotmoney_detail,
        query_hotmoney_top_active,
        query_hotmoney_top_stocks,
    )
    from src.moneyflow_fetcher import _get_engine_cached

    try:
        _hm_engine = _get_engine_cached()
        sync_meta = get_hotmoney_sync_meta(_hm_engine)
        latest_date = get_hotmoney_latest_detail_date(_hm_engine)
    except Exception as e:
        st.error(f"游资数据初始化失败：{e}")
        return

    latest_trade_label = latest_date if latest_date else "-"
    if latest_date:
        latest_trade_label = pd.to_datetime(latest_date, format="%Y%m%d").strftime("%Y-%m-%d")
    latest_sync_val = sync_meta.get("latest_ingested_at")
    latest_sync_label = "-"
    if latest_sync_val is not None and not pd.isna(latest_sync_val):
        latest_sync_label = pd.to_datetime(latest_sync_val).strftime("%Y-%m-%d %H:%M")

    meta_cols = st.columns(4)
    meta_cols[0].metric("游资名录数", f"{int(sync_meta.get('hm_list_count') or 0):,}")
    meta_cols[1].metric("游资明细行数", f"{int(sync_meta.get('hm_detail_count') or 0):,}")
    meta_cols[2].metric("最新明细交易日", latest_trade_label)
    meta_cols[3].metric("最近同步时间", latest_sync_label)

    ctl1, ctl2, ctl3, ctl4 = st.columns([1.1, 1, 1, 1.1])
    with ctl1:
        hm_keyword = st.text_input("搜索游资名称", value="", key="hm_keyword")
    with ctl2:
        detail_window = st.selectbox("明细窗口", ["最近1日", "最近5日", "最近20日", "全部已入库"], index=1, key="hm_detail_window")
    with ctl3:
        top_n = st.selectbox("TopN", [10, 20, 30, 50], index=1, key="hm_topn")
    with ctl4:
        stock_rank_mode = st.selectbox("个股排序", ["按上榜次数", "按游资数", "按净买卖绝对值"], index=0, key="hm_stock_rank_mode")

    hm_list_df = query_hotmoney_list(name=hm_keyword or None, limit=300, engine=_hm_engine)
    if hm_list_df is not None and not hm_list_df.empty:
        list_cols = st.columns([1.15, 1.85])
        with list_cols[0]:
            show = hm_list_df.copy()
            show["org_count"] = show["hm_orgs"].astype(str).apply(lambda x: len([i for i in x.split('、') if i.strip()]))
            show = show.sort_values(["org_count", "hm_name"], ascending=[False, True]).head(20)
            fig_org = go.Figure(go.Bar(
                x=show["org_count"],
                y=show["hm_name"],
                orientation="h",
                marker=dict(color=show["org_count"], colorscale="Blues", showscale=False),
                text=show["org_count"],
                textposition="outside",
            ))
            fig_org.update_layout(
                title=dict(text="游资关联机构数 Top20", x=0.02, font=dict(size=16, color="#1E293B")),
                template="wealthspark_balanced",
                paper_bgcolor="rgba(248, 250, 252, 0.92)",
                plot_bgcolor="rgba(241, 245, 249, 0.58)",
                font=dict(family="Inter, PingFang SC, sans-serif"),
                height=max(360, len(show) * 24),
                margin=dict(l=120, r=30, t=55, b=20),
                yaxis=dict(autorange="reversed"),
                xaxis_title="关联机构数",
            )
            st.plotly_chart(fig_org, use_container_width=True)
        with list_cols[1]:
            out = hm_list_df[["hm_name", "hm_desc", "hm_orgs"]].copy()
            out.columns = ["游资名称", "说明", "关联机构"]
            st.dataframe(out, use_container_width=True, hide_index=True, height=420)
    else:
        st.info("暂无游资名录数据。")

    if latest_date:
        latest_dt = pd.to_datetime(latest_date, format="%Y%m%d").date()
        if detail_window == "最近1日":
            start_dt = latest_dt
        elif detail_window == "最近5日":
            start_dt = latest_dt - timedelta(days=7)
        elif detail_window == "最近20日":
            start_dt = latest_dt - timedelta(days=30)
        else:
            start_dt = pd.to_datetime("2024-01-01").date()

        stock_order_by = "hit_count"
        if stock_rank_mode == "按游资数":
            stock_order_by = "hm_count"
        elif stock_rank_mode == "按净买卖绝对值":
            stock_order_by = "net_amount_abs"

        try:
            df_active = query_hotmoney_top_active(start_dt.strftime("%Y%m%d"), latest_date, top_n=int(top_n), engine=_hm_engine)
            df_stocks = query_hotmoney_top_stocks(start_dt.strftime("%Y%m%d"), latest_date, top_n=int(top_n), order_by=stock_order_by, engine=_hm_engine)
            df_detail = query_hotmoney_detail(start_dt.strftime("%Y%m%d"), latest_date, hm_name=hm_keyword or None, limit=500, engine=_hm_engine)
        except Exception as e:
            st.error(f"游资明细查询失败：{e}")
            return

        row1 = st.columns(2)
        with row1[0]:
            st.markdown("#### 🔥 活跃游资榜")
            if df_active is not None and not df_active.empty:
                show = df_active.copy()
                show["total_net_amount_yi"] = pd.to_numeric(show["total_net_amount"], errors="coerce").fillna(0) / 1e8
                fig_active = go.Figure(go.Bar(
                    x=show["hit_count"],
                    y=show["hm_name"],
                    orientation="h",
                    marker=dict(color=show["total_net_amount_yi"], colorscale="Tealgrn", showscale=False),
                    text=show["hit_count"],
                    textposition="outside",
                ))
                fig_active.update_layout(
                    title=dict(text="活跃游资 TopN", x=0.02, font=dict(size=16, color="#1E293B")),
                    template="wealthspark_balanced",
                    paper_bgcolor="rgba(248, 250, 252, 0.92)",
                    plot_bgcolor="rgba(241, 245, 249, 0.58)",
                    font=dict(family="Inter, PingFang SC, sans-serif"),
                    height=max(320, len(show) * 24),
                    margin=dict(l=120, r=30, t=55, b=20),
                    yaxis=dict(autorange="reversed"),
                    xaxis_title="上榜次数",
                )
                st.plotly_chart(fig_active, use_container_width=True)
                out = show[["hm_name", "hit_count", "stock_count", "total_net_amount_yi"]].copy()
                out.columns = ["游资", "上榜次数", "涉及股票数", "净买卖(亿)"]
                out["净买卖(亿)"] = out["净买卖(亿)"].map(lambda v: f"{v:,.2f}")
                st.dataframe(out, use_container_width=True, hide_index=True)
            else:
                st.info("当前窗口暂无活跃游资数据。")

        with row1[1]:
            rank_mode_label = stock_rank_mode.replace("按", "")
            st.markdown(f"#### 🎯 游资偏好个股（{rank_mode_label}）")
            if df_stocks is not None and not df_stocks.empty:
                show = df_stocks.copy()
                show["total_net_amount_yi"] = pd.to_numeric(show["total_net_amount"], errors="coerce").fillna(0) / 1e8
                stock_x_col = "hit_count"
                stock_x_title = "上榜次数"
                stock_text_col = "hit_count"
                if stock_order_by == "hm_count":
                    stock_x_col = "hm_count"
                    stock_x_title = "游资数"
                    stock_text_col = "hm_count"
                elif stock_order_by == "net_amount_abs":
                    stock_x_col = "total_net_amount_yi"
                    stock_x_title = "净买卖绝对值(亿)"
                    stock_text_col = "total_net_amount_yi"

                fig_stocks = go.Figure(go.Bar(
                    x=show[stock_x_col],
                    y=show["ts_name"],
                    orientation="h",
                    marker=dict(color=show["hm_count"], colorscale="Oranges", showscale=False),
                    text=show[stock_text_col],
                    textposition="outside",
                ))
                fig_stocks.update_layout(
                    title=dict(text="游资关注个股 TopN", x=0.02, font=dict(size=16, color="#1E293B")),
                    template="wealthspark_balanced",
                    paper_bgcolor="rgba(248, 250, 252, 0.92)",
                    plot_bgcolor="rgba(241, 245, 249, 0.58)",
                    font=dict(family="Inter, PingFang SC, sans-serif"),
                    height=max(320, len(show) * 24),
                    margin=dict(l=120, r=30, t=55, b=20),
                    yaxis=dict(autorange="reversed"),
                    xaxis_title=stock_x_title,
                )
                st.plotly_chart(fig_stocks, use_container_width=True)
                out = show[["ts_name", "ts_code", "hit_count", "hm_count", "total_net_amount_yi"]].copy()
                out.columns = ["股票", "代码", "上榜次数", "游资数", "净买卖(亿)"]
                out["净买卖(亿)"] = out["净买卖(亿)"].map(lambda v: f"{v:,.2f}")
                st.dataframe(out, use_container_width=True, hide_index=True)
            else:
                st.info("当前窗口暂无游资个股数据。")

        st.markdown("#### 🧾 游资博弈每日明细")
        if df_detail is not None and not df_detail.empty:
            show = df_detail.copy()
            show["trade_date"] = pd.to_datetime(show["trade_date"]).dt.strftime("%Y-%m-%d")
            for col in ["buy_amount", "sell_amount", "net_amount"]:
                show[col] = pd.to_numeric(show[col], errors="coerce").fillna(0) / 1e8
            out = show[["trade_date", "hm_name", "ts_name", "ts_code", "tag", "buy_amount", "sell_amount", "net_amount", "hm_orgs"]].copy()
            out.columns = ["日期", "游资", "股票", "代码", "标签", "买入(亿)", "卖出(亿)", "净买卖(亿)", "关联机构"]
            for col in ["买入(亿)", "卖出(亿)", "净买卖(亿)"]:
                out[col] = pd.to_numeric(out[col], errors="coerce").map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
            st.dataframe(out, use_container_width=True, hide_index=True, height=420)
        else:
            st.info("当前窗口暂无游资明细数据。")
    else:
        st.warning("游资每日明细目前仅成功拉到 2024-01-02；该接口限频很低，后续需要按低频增量策略继续补数。")


def render_limitup_monitor_tab():
    st.subheader("🔥 打板情绪与接力监控")
    st.caption("基于 Tushare 打板专题数据，观察情绪周期、板块接力和龙头健康度。")

    from src.limitup_monitor import (
        get_limitup_latest_date,
        get_limitup_sync_meta,
        query_limitup_emotion_daily,
        query_limitup_sector_relay_daily,
        query_limitup_leader_daily,
        query_limitup_ths_tag_daily,
        query_limitup_ths_reason_daily,
    )

    try:
        from src.moneyflow_fetcher import _get_engine_cached

        _lu_engine = _get_engine_cached()
        latest_date = get_limitup_latest_date(_lu_engine)
        sync_meta = get_limitup_sync_meta(_lu_engine)
        if not latest_date:
            st.info("暂无打板情绪数据。")
            return
        latest_dt = pd.to_datetime(latest_date, format="%Y%m%d").date()
    except Exception as e:
        st.error(f"打板情绪数据初始化失败：{e}")
        return

    latest_trade_label = latest_dt.strftime("%Y-%m-%d")
    latest_sync_val = sync_meta.get("latest_ingested_at")
    latest_sync_label = "-"
    if latest_sync_val is not None and not pd.isna(latest_sync_val):
        latest_sync_label = pd.to_datetime(latest_sync_val).strftime("%Y-%m-%d %H:%M")
    total_rows = int(sync_meta.get("total_rows") or 0)

    meta_cols = st.columns(3)
    meta_cols[0].metric("最新交易日", latest_trade_label)
    meta_cols[1].metric("最近同步时间", latest_sync_label)
    meta_cols[2].metric("累计入库行数", f"{total_rows:,}")

    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        window_mode = st.selectbox("观察窗口", ["快览(120日)", "标准(2024至今)", "研究(2023至今)"], index=1, key="lu_window_mode")
    with col2:
        relay_topn = st.selectbox("板块榜 TopN", [10, 15, 20, 30], index=1, key="lu_relay_topn")
    with col3:
        leader_topn = st.selectbox("龙头榜 TopN", [10, 20, 30, 50], index=1, key="lu_leader_topn")

    if window_mode == "快览(120日)":
        start_dt = latest_dt - timedelta(days=180)
    elif window_mode == "研究(2023至今)":
        start_dt = pd.to_datetime("2023-01-01").date()
    else:
        start_dt = pd.to_datetime("2024-01-01").date()

    try:
        df_emotion = query_limitup_emotion_daily(start_dt.strftime("%Y%m%d"), latest_date, engine=_lu_engine)
    except Exception as e:
        st.error(f"情绪序列查询失败：{e}")
        return

    if df_emotion is None or df_emotion.empty:
        st.info("当前时间窗口暂无情绪数据。")
        return

    latest_row = df_emotion.iloc[-1]
    met1, met2, met3, met4, met5 = st.columns(5)
    met1.metric("情绪阶段", str(latest_row.get("emotion_stage") or "-"))
    met2.metric("涨停数", f"{int(latest_row.get('up_cnt') or 0)}")
    met3.metric("炸板数", f"{int(latest_row.get('zha_cnt') or 0)}")
    met4.metric("连板高度", f"{int(latest_row.get('high_days') or 0)}")
    met5.metric("强势概念数", f"{int(latest_row.get('strong_cpt_cnt') or 0)}")

    fig_emotion = go.Figure()
    fig_emotion.add_trace(go.Scatter(
        x=df_emotion["trade_date"],
        y=df_emotion["emotion_score"],
        mode="lines+markers",
        name="情绪分",
        line=dict(color="#EF4444", width=2.5),
    ))
    fig_emotion.update_layout(
        title=dict(text="情绪趋势图", x=0.02, font=dict(size=17, color="#1E293B")),
        template="wealthspark_balanced",
        paper_bgcolor="rgba(248, 250, 252, 0.92)",
        plot_bgcolor="rgba(241, 245, 249, 0.58)",
        font=dict(family="Inter, PingFang SC, sans-serif"),
        height=360,
        margin=dict(l=30, r=30, t=55, b=30),
        xaxis_title="日期",
        yaxis_title="情绪分",
    )
    st.plotly_chart(fig_emotion, use_container_width=True)

    low1, low2 = st.columns(2)
    with low1:
        st.markdown("#### 🚀 板块接力榜")
        try:
            df_relay = query_limitup_sector_relay_daily(latest_date, top_n=int(relay_topn), engine=_lu_engine)
            if df_relay is not None and not df_relay.empty:
                show = df_relay.copy()
                show["relay_score"] = pd.to_numeric(show["up_cnt"], errors="coerce").fillna(0) * 1.0 + pd.to_numeric(show["max_height"], errors="coerce").fillna(0) * 1.5 - pd.to_numeric(show["zha_cnt"], errors="coerce").fillna(0) * 0.8
                show = show.sort_values(["relay_score", "up_cnt"], ascending=[False, False])
                out = show[["concept_name", "up_cnt", "zha_cnt", "lead_cnt", "max_height", "relay_score"]].copy()
                out.columns = ["概念", "涨停数", "炸板数", "龙头数", "连板高度", "接力分"]
                st.dataframe(out, use_container_width=True, hide_index=True)
            else:
                st.info("暂无板块接力数据。")
        except Exception as e:
            st.warning(f"板块接力榜查询失败：{e}")

    with low2:
        st.markdown("#### 👑 龙头健康度")
        try:
            df_leader = query_limitup_leader_daily(latest_date, top_n=int(leader_topn), engine=_lu_engine)
            if df_leader is not None and not df_leader.empty:
                show = df_leader.copy()
                show["health_score"] = pd.to_numeric(show["high_days"], errors="coerce").fillna(0) * 1.5 + pd.to_numeric(show["fd_amount"], errors="coerce").fillna(0) * 0.01 - pd.to_numeric(show["open_num"], errors="coerce").fillna(0) * 0.8
                out = show[["name", "ts_code", "high_days", "status", "open_num", "fd_amount", "health_score"]].copy()
                out.columns = ["名称", "代码", "连板高度", "状态", "开板次数", "封单额", "健康分"]
                st.dataframe(out.sort_values("健康分", ascending=False), use_container_width=True, hide_index=True)
            else:
                st.info("暂无龙头健康度数据。")
        except Exception as e:
            st.warning(f"龙头健康度查询失败：{e}")

    st.markdown("#### 🏷️ 同花顺标签 / 题材视角")
    ths1, ths2 = st.columns(2)
    with ths1:
        try:
            df_ths_tag = query_limitup_ths_tag_daily(latest_date, top_n=15, engine=_lu_engine)
            if df_ths_tag is not None and not df_ths_tag.empty:
                show = df_ths_tag.copy()
                show["stock_count"] = pd.to_numeric(show["stock_count"], errors="coerce").fillna(0)
                show["lb_count"] = pd.to_numeric(show["lb_count"], errors="coerce").fillna(0)
                show["avg_open_num"] = pd.to_numeric(show["avg_open_num"], errors="coerce").round(2)
                show = show.sort_values(["stock_count", "lb_count"], ascending=[False, False])

                fig_tag = go.Figure()
                fig_tag.add_trace(go.Bar(
                    x=show["tag"],
                    y=show["stock_count"],
                    name="个股数",
                    marker_color="#3B82F6",
                ))
                fig_tag.add_trace(go.Bar(
                    x=show["tag"],
                    y=show["lb_count"],
                    name="连板股数",
                    marker_color="#EF4444",
                ))
                fig_tag.update_layout(
                    barmode="group",
                    title=dict(text="同花顺标签分布", x=0.02, font=dict(size=15, color="#1E293B")),
                    template="wealthspark_balanced",
                    paper_bgcolor="rgba(248, 250, 252, 0.92)",
                    plot_bgcolor="rgba(241, 245, 249, 0.58)",
                    font=dict(family="Inter, PingFang SC, sans-serif"),
                    height=300,
                    margin=dict(l=25, r=25, t=50, b=25),
                    xaxis_title="标签",
                    yaxis_title="数量",
                )
                st.plotly_chart(fig_tag, use_container_width=True)

                out = show[["tag", "stock_count", "lb_count", "avg_open_num", "sample_reason"]].copy()
                out.columns = ["标签", "个股数", "连板股数", "平均开板次数", "样例题材"]
                st.dataframe(out, use_container_width=True, hide_index=True)
            else:
                st.info("暂无同花顺标签数据。")
        except Exception as e:
            st.warning(f"同花顺标签榜查询失败：{e}")

    with ths2:
        try:
            df_ths_reason = query_limitup_ths_reason_daily(latest_date, top_n=15, engine=_lu_engine)
            if df_ths_reason is not None and not df_ths_reason.empty:
                show = df_ths_reason.copy()
                show["stock_count"] = pd.to_numeric(show["stock_count"], errors="coerce").fillna(0)
                show["uniq_stock_count"] = pd.to_numeric(show["uniq_stock_count"], errors="coerce").fillna(0)
                show = show.sort_values(["stock_count", "uniq_stock_count"], ascending=[False, False])

                chart_df = show.head(10).copy()
                chart_df["reason_short"] = chart_df["reason"].astype(str).apply(lambda x: x if len(x) <= 20 else x[:20] + "…")

                fig_reason = go.Figure()
                fig_reason.add_trace(go.Bar(
                    x=chart_df["stock_count"],
                    y=chart_df["reason_short"],
                    orientation="h",
                    marker_color="#10B981",
                    name="出现次数",
                ))
                fig_reason.update_layout(
                    title=dict(text="涨停原因 Top10", x=0.02, font=dict(size=15, color="#1E293B")),
                    template="wealthspark_balanced",
                    paper_bgcolor="rgba(248, 250, 252, 0.92)",
                    plot_bgcolor="rgba(241, 245, 249, 0.58)",
                    font=dict(family="Inter, PingFang SC, sans-serif"),
                    height=300,
                    margin=dict(l=25, r=25, t=50, b=25),
                    xaxis_title="出现次数",
                    yaxis_title="涨停原因",
                    yaxis=dict(autorange="reversed"),
                )
                st.plotly_chart(fig_reason, use_container_width=True)

                out = show[["reason", "stock_count", "uniq_stock_count"]].copy()
                out.columns = ["涨停原因", "出现次数", "股票数"]
                st.dataframe(out, use_container_width=True, hide_index=True)
            else:
                st.info("暂无同花顺题材数据。")
        except Exception as e:
            st.warning(f"同花顺题材榜查询失败：{e}")

def render_tech_picker_tab():
    st.subheader("🎯 技术指标选股")
    st.caption("基于最近交易日的 Tushare 复权行情预计算，筛选组合符合周线或月线 EMA5 < EMA30 的标的。")
    
    st.info("💡 提示：为保证极速检索体验，底层采用预计算结果，因此每日盘后方会刷新最新一日技术形态。")
    
    col1, col2 = st.columns(2)
    with col1:
        use_weekly = st.checkbox("☑️ 满足条件：周线 EMA5 < EMA30", value=True)
    with col2:
        use_monthly = st.checkbox("☑️ 满足条件：月线 EMA5 < EMA30", value=False)
        
    if not use_weekly and not use_monthly:
        st.warning("请至少勾选一个筛选条件！")
        return

    if st.button("开始精准筛选", type="primary", key="btn_tech_picker"):
        with st.spinner("正在检索分布..."):
            try:
                from src.etf_stats import search_stocks_by_technical_signals
                df = search_stocks_by_technical_signals(use_weekly, use_monthly)
                st.session_state["tech_picker_results"] = df if df is not None else pd.DataFrame()
                st.session_state["tech_picker_last_filters"] = {
                    "use_weekly": use_weekly,
                    "use_monthly": use_monthly,
                }
                st.session_state.pop("tech_picker_last_jump_marker", None)
                st.session_state["tech_picker_industry_filter"] = "全部行业"
            except Exception as e:
                st.session_state["tech_picker_results"] = pd.DataFrame()
                st.error(f"技术面检索失败，确保增量脚本及因子脚本已运行: {e}")

    result_df = st.session_state.get("tech_picker_results")
    if result_df is None:
        return
    if result_df.empty:
        st.warning("最新交易日，没有找到符合上述技术面条件的股票。")
        return

    filtered_df = result_df.copy()
    if 'industry' in result_df.columns:
        raw_industries = result_df['industry'].dropna().astype(str).str.strip()
        industries = sorted([item for item in raw_industries.unique().tolist() if item])
        if industries:
            industry_options = ['全部行业'] + industries
            current_industry = st.session_state.get("tech_picker_industry_filter", '全部行业')
            selected_industry = st.selectbox(
                "行业",
                options=industry_options,
                index=industry_options.index(current_industry) if current_industry in industry_options else 0,
                key="tech_picker_industry_filter",
                help="选择后，仅显示该行业对应的符合条件股票"
            )
            if selected_industry == '全部行业':
                filtered_df = result_df.copy()
            else:
                filtered_df = result_df[
                    result_df['industry'].fillna('').astype(str).str.strip().eq(selected_industry)
                ].copy()

    if len(filtered_df) == len(result_df):
        st.success(f"共筛选出 {len(result_df)} 家企业")
    else:
        st.success(f"共筛选出 {len(result_df)} 家企业，当前行业“{st.session_state.get('tech_picker_industry_filter', '全部行业')}”下显示 {len(filtered_df)} 家")

    if filtered_df.empty:
        st.warning("当前行业筛选条件下暂无符合条件的股票。")
        return

    render_tech_picker_jump_table(filtered_df)

def render_company_screener_tab():
    st.subheader("🏢 公司主营与产品筛选")
    st.caption("按照行业、产品和主营业务服务筛选公司")
    
    col1, col2, col3 = st.columns([1.5, 1.5, 1.5])
    with col1:
        raw_industries = get_stock_basic_summary()['所属行业'].dropna().unique().tolist()
        industries = [i for i in raw_industries if str(i).strip()]
        selected_industries = st.multiselect("所属行业", options=['全部'] + sorted(industries), default=['全部'])
    with col2:
        product_kw = st.text_input("核心产品关键字", placeholder="例如: 芯片, 新能源...")
    with col3:
        business_kw = st.text_input("服务/主营业务关键字", placeholder="例如: 研发, 制造...")
        
    if st.button("开始筛选", type="primary"):
        with st.spinner("正在检索符合条件的公司..."):
            df = search_companies(industries=selected_industries, product_kw=product_kw, business_kw=business_kw)
            if df.empty:
                st.warning("没有检索到符合条件的公司，请尝试更改关键词")
            else:
                st.success(f"共为您检索到 {len(df)} 家企业")
                st.dataframe(
                    df.rename(columns={
                        'ts_code': '代码', 'name': '简称', 'industry': '行业',
                        'main_business': '主要业务', 'product': '产品及服务'
                    }),
                    use_container_width=True, hide_index=True
                )

def render_etf_tab():
    """渲染ETF份额变动Tab页内容"""
    # 加载数据
    df = load_data(DATA_FILE)

    # 验证数据
    if df is None or len(df) == 0:
        st.error("❌ 未能加载任何数据，请检查Excel文件")
        st.stop()

    # 根据GitHub Action更新日期过滤数据
    try:
        import json
        import os
        from datetime import datetime
        if os.path.exists('last_update.json'):
            with open('last_update.json', 'r') as f:
                update_info = json.load(f)
                update_date_str = update_info.get('update_date')
                if update_date_str:
                    update_date = datetime.strptime(update_date_str, '%Y-%m-%d')
                    # 只保留更新日期及之前的数据
                    df = df[df['date'] <= update_date]
                    logger.info(f"数据已过滤至GitHub Action更新日期: {update_date_str}")
    except Exception as e:
        logger.warning(f"无法读取last_update.json，使用所有数据: {e}")

    iphone_mode = get_query_param_value("iphone_mode").strip() == "1"

    # 显示数据加载信息
    if iphone_mode:
        st.success(f"✅ 已加载 {len(df)} 条数据记录")
    else:
        st.sidebar.success(f"✅ 已加载 {len(df)} 条数据记录")
        st.sidebar.header("🔍 数据筛选")

    # 1. 指标选择器
    metric_types = sorted(df['metric_type'].unique())

    if len(metric_types) == 0:
        st.error("❌ 未检测到任何指标数据，请检查Excel文件格式")
        st.info("Excel文件应包含section标题行，标题中应包含关键词：市值、份额、变动、申赎、比例、涨跌幅")
        st.stop()

    metric_categories = {
        "市值类": [m for m in metric_types if "市值" in m],
        "份额类": [m for m in metric_types if "份额" in m],
        "变动类": [m for m in metric_types if "变动" in m or "申赎" in m],
        "比例类": [m for m in metric_types if "比例" in m],
        "涨跌类": [m for m in metric_types if "涨跌" in m],
        "其他": [m for m in metric_types if not any(keyword in m for keyword in ["市值", "份额", "变动", "申赎", "比例", "涨跌"])]
    }
    metric_categories = {k: v for k, v in metric_categories.items() if v}

    if iphone_mode:
        with st.expander("🔍 ETF筛选条件", expanded=True):
            if len(metric_categories) > 1:
                selected_category = st.radio(
                    "指标分类",
                    options=list(metric_categories.keys()),
                    horizontal=True,
                    key="iphone_etf_metric_category"
                )
                available_metrics = metric_categories[selected_category]
            else:
                available_metrics = metric_types

            selected_metric = st.selectbox(
                "选择具体指标",
                options=available_metrics,
                index=0,
                key="iphone_etf_selected_metric"
            )

            quick_metrics = {
                "总市值": [m for m in metric_types if "总市值" in m],
                "份额": [m for m in metric_types if "份额" in m and "总市值" not in m],
                "涨跌幅": [m for m in metric_types if "涨跌" in m]
            }
            q1, q2, q3 = st.columns(3)
            if quick_metrics["总市值"] and q1.button("总市值", use_container_width=True, key="iphone_quick_market"):
                selected_metric = quick_metrics["总市值"][0]
            if quick_metrics["份额"] and q2.button("份额", use_container_width=True, key="iphone_quick_share"):
                selected_metric = quick_metrics["份额"][0]
            if quick_metrics["涨跌幅"] and q3.button("涨跌幅", use_container_width=True, key="iphone_quick_change"):
                selected_metric = quick_metrics["涨跌幅"][0]

            metric_df = df[df['metric_type'] == selected_metric].copy()
            has_aggregate = metric_df['is_aggregate'].any()
            contains_total_market_value = '总市值' in selected_metric if selected_metric else False

            if has_aggregate and contains_total_market_value:
                st.info("📊 当前显示所有ETF的总和")
                selected_etfs = None
            else:
                etf_names = sorted(metric_df[metric_df['is_aggregate'] == False]['name'].unique())
                selected_etfs = st.multiselect(
                    "选择ETF",
                    options=etf_names,
                    default=etf_names,
                    key="iphone_etf_selected_etfs"
                )

            min_date = metric_df['date'].min().date()
            max_date = metric_df['date'].max().date()
            if min_date == max_date:
                st.info(f"📅 当前数据日期: {min_date}")
                date_range = (min_date, max_date)
            else:
                date_range = st.slider(
                    "选择日期范围",
                    min_value=min_date,
                    max_value=max_date,
                    value=(min_date, max_date),
                    format="YYYY-MM-DD",
                    key="iphone_etf_date_range"
                )

            chart_type = st.selectbox(
                "图表类型",
                options=['line', 'area', 'scatter'],
                format_func=lambda x: {'line': '📈 平滑曲线', 'area': '📊 面积图', 'scatter': '⚫ 散点图'}[x],
                index=0,
                key="iphone_etf_chart_type"
            )
    else:
        if len(metric_categories) > 1:
            st.sidebar.markdown("**指标分类**")
            selected_category = st.sidebar.radio(
                "选择指标类别",
                options=list(metric_categories.keys()),
                label_visibility="collapsed"
            )
            available_metrics = metric_categories[selected_category]
        else:
            available_metrics = metric_types

        selected_metric = st.sidebar.selectbox(
            "选择具体指标",
            options=available_metrics,
            index=0
        )

        metric_df = df[df['metric_type'] == selected_metric].copy()
        has_aggregate = metric_df['is_aggregate'].any()
        contains_total_market_value = '总市值' in selected_metric if selected_metric else False

        if has_aggregate and contains_total_market_value:
            st.sidebar.info("📊 当前显示所有ETF的总和")
            selected_etfs = None
        else:
            etf_names = sorted(metric_df[metric_df['is_aggregate'] == False]['name'].unique())
            selected_etfs = st.sidebar.multiselect(
                "选择ETF",
                options=etf_names,
                default=etf_names
            )

        min_date = metric_df['date'].min().date()
        max_date = metric_df['date'].max().date()

        if min_date == max_date:
            st.sidebar.info(f"📅 当前数据日期: {min_date}")
            date_range = (min_date, max_date)
        else:
            date_range = st.sidebar.slider(
                "选择日期范围",
                min_value=min_date,
                max_value=max_date,
                value=(min_date, max_date),
                format="YYYY-MM-DD"
            )

        st.sidebar.header("📊 图表设置")
        st.sidebar.markdown("---")
        st.sidebar.markdown("**快速切换**")

        quick_metrics = {
            "总市值": [m for m in metric_types if "总市值" in m],
            "份额": [m for m in metric_types if "份额" in m and "总市值" not in m],
            "涨跌幅": [m for m in metric_types if "涨跌" in m]
        }

        quick_cols = st.sidebar.columns(3)
        for idx, (label, metrics) in enumerate(quick_metrics.items()):
            if metrics and quick_cols[idx].button(label, use_container_width=True):
                selected_metric = metrics[0]
                st.rerun()

        chart_type = st.sidebar.radio(
            "图表类型",
            options=['line', 'area', 'scatter'],
            format_func=lambda x: {'line': '📈 平滑曲线', 'area': '📊 面积图', 'scatter': '⚫ 散点图'}[x],
            index=0,
            help="平滑曲线：清晰的线条，适合查看趋势\n面积图：填充区域，适合对比数量\n散点图：仅显示数据点，适合查看离散数据"
        )

    # 主区域 - 图表和统计信息
    # 筛选数据
    filtered_df = metric_df[
        (metric_df['date'].dt.date >= date_range[0]) &
        (metric_df['date'].dt.date <= date_range[1])
    ].copy()

    # 检查是否有数据
    if len(filtered_df) == 0:
        st.warning("⚠️ 所选条件下没有数据，请调整筛选条件")
        return

    # 确定是否为汇总模式
    is_aggregate = has_aggregate and contains_total_market_value

    # 验证ETF选择（非汇总模式）
    if not is_aggregate and (selected_etfs is None or len(selected_etfs) == 0):
        st.info("ℹ️ 请至少选择一个ETF")
        return

    # 创建并显示图表
    fig = create_line_chart(filtered_df, selected_metric, is_aggregate, selected_etfs, chart_type)

    # 在图表之前显示关键指标卡片
    st.subheader("📊 关键指标")

    # 计算关键指标
    stats_df = calculate_statistics(filtered_df, is_aggregate, selected_etfs)

    if len(stats_df) > 0:
        # 显示前4个最重要的指标卡片
        num_cards = min(4, len(stats_df))
        cols = st.columns(num_cards)

        for idx in range(num_cards):
            with cols[idx]:
                row = stats_df.iloc[idx]
                st.markdown(
                    draw_metric_card(
                        title=row['ETF名称'],
                        value=row['当日数据'],
                        delta=row['变动'],
                        delta_pct=row['变动幅度']
                    ),
                    unsafe_allow_html=True
                )

    st.markdown("<br>", unsafe_allow_html=True)

    # 显示图表
    st.plotly_chart(fig, use_container_width=True)

    # 显示统计信息
    st.subheader("📊 最新数据对比")
    st.caption("显示选定日期范围内最新一天与前一天的数据对比")
    stats_df = calculate_statistics(filtered_df, is_aggregate, selected_etfs)

    if len(stats_df) > 0:
        st.dataframe(
            stats_df,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("ℹ️ 没有可显示的统计信息")


def render_etf_classification_tab():
    """渲染ETF分类统计Tab页内容"""
    st.subheader("📊 ETF数据自动分类提取")
    st.caption("基于 Tushare 获取全市场ETF基本信息，进行清洗和预定义分类拆表。")

    st.info("点击下方按钮将实时从 Tushare `etf_basic` 接口获取 ETF 基础信息（这可能需要几秒钟），处理完成后可下载多 Sheet Excel。")
    
    # 因为操作耗时，我们用按钮触发
    if st.button("🚀 从 Tushare 获取并生成分类 Excel", type="primary"):
        with st.spinner("正在从 Tushare 拉取全市场 ETF 基础数据..."):
            try:
                raw_df = fetch_etf_data()
            except Exception as e:
                st.error(f"获取 Tushare 数据失败: {str(e)}")
                return
            
        with st.spinner("成功获取数据，正在进行洗表与自动分类处理..."):
            try:
                results_dict = process_etf_classification(raw_df)
                excel_bytes = export_etfs_to_excel(results_dict)
                st.success(f"✅ 处理完成！原始数据 {len(raw_df)} 条，清理退市后主表剩余 {len(results_dict.get('ETF汇总表', []))} 条。")
                
                # 在页面中提示各表行数预览
                st.write("**数据行数概览:**")
                col1, col2, col3, col4 = st.columns(4)
                summary_keys = list(results_dict.keys())
                for i, k in enumerate(summary_keys[:8]):
                    with (col1 if i % 4 == 0 else col2 if i % 4 == 1 else col3 if i % 4 == 2 else col4):
                        st.metric(k, f"{len(results_dict[k])} 行")
                
                # 提供下载按钮
                st.download_button(
                    label="📥 下载 ETF 分类汇总 Excel",
                    data=excel_bytes,
                    file_name=f"ETF分类汇总_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary"
                )
            except Exception as e:
                st.error(f"处理分类数据时发生异常: {str(e)}")
                logger.error("ETF classification error", exc_info=True)

    st.divider()
    st.subheader("📄 股票信息表导出")
    st.caption("从数据库读取股票列表与上市公司基本信息，生成“股票基本信息汇总表”Excel。")
    st.info("导出规则：仅导出未退市股票，合并股票列表与上市公司基本信息，按所属行业排序，并将 main_business 提炼为一列“主要业务”和一列“产品”，同时保留原始主营业务内容。")

    if st.button("📥 导出股票信息表 Excel"):
        with st.spinner("正在生成股票基本信息汇总表..."):
            try:
                stock_export_df = load_stock_basic_summary_export()
                stock_excel_bytes = export_stock_basic_summary_excel(stock_export_df)
            except Exception as e:
                st.error(f"生成股票信息表失败: {str(e)}")
                logger.error("Stock summary export error", exc_info=True)
                return

        industry_count = int(stock_export_df['所属行业'].replace('', pd.NA).dropna().nunique()) if '所属行业' in stock_export_df.columns else 0

        metric_cols = st.columns(3)
        metric_cols[0].metric("股票数量", f"{len(stock_export_df):,}")
        metric_cols[1].metric("行业数量", f"{industry_count:,}")
        metric_cols[2].metric("导出字段数", str(len(stock_export_df.columns)))

        preview_columns = [
            column for column in ['股票代码', '股票简称', '所属行业', '主营业务原文', '主要业务', '产品']
            if column in stock_export_df.columns
        ]
        if preview_columns:
            st.write("**导出预览：**")
            st.dataframe(
                stock_export_df[preview_columns].head(20),
                use_container_width=True,
                hide_index=True
            )

        st.download_button(
            label="📥 下载 股票基本信息汇总表 Excel",
            data=stock_excel_bytes,
            file_name=f"股票基本信息汇总表_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary"
        )


def render_etf_category_ratio_tab():
    st.subheader("🥧 ETF分类占比")
    st.caption("按交易日统计指数ETF、QDII-ETF、商品ETF、货币ETF、债券ETF的总份额/总规模占比")

    try:
        available_dates = get_available_dates(limit=250)
    except Exception as e:
        st.error(f"读取可用交易日失败: {str(e)}")
        return

    if not available_dates:
        st.warning("暂无可用交易日数据")
        return

    selected_date = st.selectbox("选择交易日期", options=available_dates, index=0)

    try:
        summary_df = get_category_daily_summary(selected_date)
    except Exception as e:
        st.error(f"读取分类汇总失败: {str(e)}")
        return

    if summary_df is None or len(summary_df) == 0:
        st.warning("该日期没有ETF分类汇总数据")
        return

    target_categories = ["指数ETF", "QDII-ETF", "商品ETF", "货币ETF", "债券ETF"]

    def normalize_category(value: str) -> str:
        text = str(value).strip()
        upper_text = text.upper()
        if "QDII" in upper_text:
            return "QDII-ETF"
        if "指数" in text:
            return "指数ETF"
        if "商品" in text:
            return "商品ETF"
        if "货币" in text:
            return "货币ETF"
        if "债券" in text:
            return "债券ETF"
        return ""

    category_df = summary_df[summary_df["category"] != "全部"].copy()
    category_df["category_name"] = category_df["category"].map(normalize_category)
    category_df = category_df[category_df["category_name"] != ""].copy()

    if len(category_df) == 0:
        st.warning("该日期未匹配到目标分类（指数/QDII/商品/货币/债券）")
        return

    category_df = category_df.groupby("category_name", as_index=False).agg({
        "etf_count": "sum",
        "total_share_yi": "sum",
        "total_size_yi": "sum"
    })

    category_df = (
        category_df
        .set_index("category_name")
        .reindex(target_categories, fill_value=0)
        .reset_index()
    )

    category_df["total_share_yi"] = pd.to_numeric(category_df["total_share_yi"], errors="coerce").fillna(0.0)
    category_df["total_size_yi"] = pd.to_numeric(category_df["total_size_yi"], errors="coerce").fillna(0.0)

    total_share = float(category_df["total_share_yi"].sum())
    total_size = float(category_df["total_size_yi"].sum())

    if total_share <= 0 and total_size <= 0:
        st.warning("该日期份额与规模数据均不可用，无法计算占比")
        return

    category_df["share_ratio"] = category_df["total_share_yi"] / total_share if total_share > 0 else 0.0
    category_df["size_ratio"] = category_df["total_size_yi"] / total_size if total_size > 0 else 0.0
    category_df["share_ratio_pct"] = (category_df["share_ratio"] * 100).round(2)
    category_df["size_ratio_pct"] = (category_df["size_ratio"] * 100).round(2)

    kpi_col1, kpi_col2 = st.columns(2)
    with kpi_col1:
        st.metric("ETF总份额（亿份）", f"{total_share:,.2f}" if total_share > 0 else "-")
    with kpi_col2:
        st.metric("ETF总规模（亿元）", f"{total_size:,.2f}" if total_size > 0 else "-")

    col1, col2 = st.columns(2)
    with col1:
        if total_share > 0:
            share_fig = go.Figure(
                data=[go.Pie(
                    labels=category_df["category_name"],
                    values=category_df["total_share_yi"],
                    hole=0.45,
                    textinfo="label+percent",
                    hovertemplate="%{label}<br>总份额: %{value:.2f} 亿份<br>占比: %{percent}<extra></extra>"
                )]
            )
            share_fig.update_layout(
                title=f"{selected_date} ETF总份额分类占比",
                template="wealthspark_balanced",
                height=500
            )
            st.plotly_chart(share_fig, use_container_width=True)
        else:
            st.info("该日期缺少份额数据，无法绘制份额占比饼图")

    with col2:
        if total_size > 0:
            size_fig = go.Figure(
                data=[go.Pie(
                    labels=category_df["category_name"],
                    values=category_df["total_size_yi"],
                    hole=0.45,
                    textinfo="label+percent",
                    hovertemplate="%{label}<br>总规模: %{value:.2f} 亿元<br>占比: %{percent}<extra></extra>"
                )]
            )
            size_fig.update_layout(
                title=f"{selected_date} ETF总规模分类占比",
                template="wealthspark_balanced",
                height=500
            )
            st.plotly_chart(size_fig, use_container_width=True)
        else:
            st.info("该日期缺少规模数据，无法绘制规模占比饼图")

    display_df = category_df.rename(columns={
        "category_name": "ETF分类",
        "etf_count": "ETF只数",
        "total_share_yi": "总份额（亿份）",
        "share_ratio_pct": "份额占比（%）",
        "total_size_yi": "总规模（亿元）",
        "size_ratio_pct": "规模占比（%）"
    })[["ETF分类", "ETF只数", "总份额（亿份）", "份额占比（%）", "总规模（亿元）", "规模占比（%）"]]

    st.subheader("📋 分类汇总明细")
    st.dataframe(display_df, use_container_width=True, hide_index=True)


def get_latest_metric_date(df: pd.DataFrame, metric_col: str):
    valid_df = df.dropna(subset=[metric_col]).copy()
    if valid_df.empty:
        return None, valid_df
    latest_date = valid_df['trade_date'].max()
    return latest_date, valid_df[valid_df['trade_date'] == latest_date].copy()


def format_metric_delta(value, pct):
    if value is None or pd.isna(value):
        return "-"
    text = f"{float(value):+,.2f}"
    if pct is None or pd.isna(pct):
        return text
    return f"{text} ({float(pct):+,.2f}%)"


def create_change_curve_chart(
    df: pd.DataFrame,
    value_col: str,
    title: str,
    yaxis_title: str,
    pct_col: "Optional[str]" = None,
    series_col: "Optional[str]" = None,
    series_names: "Optional[List[str]]" = None,
    color_palette: "Optional[List[str]]" = None,
    value_suffix: str = "",
    extra_col: "Optional[str]" = None,
    extra_label: "Optional[str]" = None,
    extra_suffix: str = ""
) -> go.Figure:
    fig = go.Figure()
    chart_df = df.dropna(subset=[value_col]).copy()
    positive_max = chart_df.loc[chart_df[value_col] > 0, value_col].max() if not chart_df.empty else None
    negative_min = chart_df.loc[chart_df[value_col] < 0, value_col].min() if not chart_df.empty else None
    if pd.notna(positive_max):
        fig.add_hrect(y0=0, y1=float(positive_max), fillcolor='rgba(239, 68, 68, 0.05)', line_width=0)
    if pd.notna(negative_min):
        fig.add_hrect(y0=float(negative_min), y1=0, fillcolor='rgba(16, 185, 129, 0.05)', line_width=0)

    if series_col is None:
        custom_cols = [col for col in [pct_col, extra_col] if col and col in chart_df.columns]
        custom_data = chart_df[custom_cols].to_numpy() if custom_cols else None
        hover_template = f"<b>%{{x|%Y-%m-%d}}</b><br>{yaxis_title}: %{{y:+,.2f}}{value_suffix}"
        custom_idx = 0
        if pct_col and pct_col in custom_cols:
            hover_template += f"<br>变动比例: %{{customdata[{custom_idx}]:+,.2f}}%"
            custom_idx += 1
        if extra_col and extra_label and extra_col in custom_cols:
            hover_template += f"<br>{extra_label}: %{{customdata[{custom_idx}]:+,.2f}}{extra_suffix}"
        hover_template += "<extra></extra>"
        fig.add_trace(go.Scatter(
            x=chart_df['trade_date'],
            y=chart_df[value_col],
            mode='lines+markers',
            name=yaxis_title,
            line=dict(width=2.4, color='#F59E0B', shape='spline'),
            marker=dict(size=5, color='#F59E0B'),
            fill='tozeroy',
            fillcolor='rgba(245, 158, 11, 0.10)',
            customdata=custom_data,
            hovertemplate=hover_template
        ))
    else:
        palette = color_palette or ['#2E5BFF', '#8E54E9', '#FF9966', '#00D4AA', '#FF6B9D']
        ordered_names = series_names or chart_df[series_col].dropna().unique().tolist()
        for idx, name in enumerate(ordered_names):
            line_df = chart_df[chart_df[series_col] == name]
            if line_df.empty:
                continue
            custom_cols = [col for col in [pct_col, extra_col] if col and col in line_df.columns]
            custom_data = line_df[custom_cols].to_numpy() if custom_cols else None
            hover_template = f"<b>{name}</b><br>%{{x|%Y-%m-%d}}<br>{yaxis_title}: %{{y:+,.2f}}{value_suffix}"
            custom_idx = 0
            if pct_col and pct_col in custom_cols:
                hover_template += f"<br>变动比例: %{{customdata[{custom_idx}]:+,.2f}}%"
                custom_idx += 1
            if extra_col and extra_label and extra_col in custom_cols:
                hover_template += f"<br>{extra_label}: %{{customdata[{custom_idx}]:+,.2f}}{extra_suffix}"
            hover_template += "<extra></extra>"
            fig.add_trace(go.Scatter(
                x=line_df['trade_date'],
                y=line_df[value_col],
                mode='lines+markers',
                name=name,
                line=dict(width=2.2 if idx < 4 else 1.8, color=palette[idx % len(palette)], shape='spline'),
                marker=dict(size=4, color=palette[idx % len(palette)]),
                customdata=custom_data,
                hovertemplate=hover_template
            ))

    fig.add_hline(y=0, line_width=1, line_dash='dash', line_color='#94A3B8')
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(size=20, weight=700, color='#1E293B'),
            x=0.02
        ),
        xaxis_title='日期',
        yaxis_title=yaxis_title,
        hovermode='x unified',
        height=420,
        template='plotly_white',
        plot_bgcolor='rgba(248, 250, 252, 0.5)',
        paper_bgcolor='rgba(248, 250, 252, 0.92)',
        font=dict(family='Inter, PingFang SC, sans-serif'),
        legend=dict(
            orientation='h', yanchor='bottom', y=-0.25,
            xanchor='center', x=0.5,
            bgcolor='rgba(255,255,255,0)', font=dict(size=11)
        ),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    fig.update_xaxes(
        showgrid=True, gridwidth=1, gridcolor='rgba(226,232,240,0.5)',
        showline=True, linewidth=1, linecolor='#E2E8F0'
    )
    fig.update_yaxes(
        showgrid=True, gridwidth=1, gridcolor='rgba(226,232,240,0.5)',
        showline=True, linewidth=1, linecolor='#E2E8F0',
        zeroline=False,
        fixedrange=True
    )
    return fig


def create_change_bar_chart(
    df: pd.DataFrame,
    value_col: str,
    title: str,
    yaxis_title: str,
    pct_col: "Optional[str]" = None,
    series_col: "Optional[str]" = None,
    series_names: "Optional[List[str]]" = None,
    value_suffix: str = "",
    extra_col: "Optional[str]" = None,
    extra_label: "Optional[str]" = None,
    extra_suffix: str = ""
) -> go.Figure:
    fig = go.Figure()
    chart_df = df.dropna(subset=[value_col]).copy()
    positive_color = '#EF4444'
    negative_color = '#10B981'
    positive_max = chart_df.loc[chart_df[value_col] > 0, value_col].max() if not chart_df.empty else None
    negative_min = chart_df.loc[chart_df[value_col] < 0, value_col].min() if not chart_df.empty else None
    if pd.notna(positive_max):
        fig.add_hrect(y0=0, y1=float(positive_max), fillcolor='rgba(239, 68, 68, 0.05)', line_width=0)
    if pd.notna(negative_min):
        fig.add_hrect(y0=float(negative_min), y1=0, fillcolor='rgba(16, 185, 129, 0.05)', line_width=0)

    if series_col is None:
        colors = [
            positive_color if value >= 0 else negative_color
            for value in chart_df[value_col]
        ]
        custom_cols = [col for col in [pct_col, extra_col] if col and col in chart_df.columns]
        custom_data = chart_df[custom_cols].to_numpy() if custom_cols else None
        hover_template = f"<b>%{{x|%Y-%m-%d}}</b><br>{yaxis_title}: %{{y:+,.2f}}{value_suffix}"
        custom_idx = 0
        if pct_col and pct_col in custom_cols:
            hover_template += f"<br>变动比例: %{{customdata[{custom_idx}]:+,.2f}}%"
            custom_idx += 1
        if extra_col and extra_label and extra_col in custom_cols:
            hover_template += f"<br>{extra_label}: %{{customdata[{custom_idx}]:+,.2f}}{extra_suffix}"
        hover_template += "<extra></extra>"
        fig.add_trace(go.Bar(
            x=chart_df['trade_date'],
            y=chart_df[value_col],
            name=yaxis_title,
            marker=dict(color=colors, line=dict(width=0)),
            opacity=0.88,
            customdata=custom_data,
            hovertemplate=hover_template
        ))
    else:
        ordered_names = series_names or chart_df[series_col].dropna().unique().tolist()
        for name in ordered_names:
            bar_df = chart_df[chart_df[series_col] == name]
            if bar_df.empty:
                continue
            colors = [
                positive_color if value >= 0 else negative_color
                for value in bar_df[value_col]
            ]
            custom_cols = [col for col in [pct_col, extra_col] if col and col in bar_df.columns]
            custom_data = bar_df[custom_cols].to_numpy() if custom_cols else None
            hover_template = f"<b>{name}</b><br>%{{x|%Y-%m-%d}}<br>{yaxis_title}: %{{y:+,.2f}}{value_suffix}"
            custom_idx = 0
            if pct_col and pct_col in custom_cols:
                hover_template += f"<br>变动比例: %{{customdata[{custom_idx}]:+,.2f}}%"
                custom_idx += 1
            if extra_col and extra_label and extra_col in custom_cols:
                hover_template += f"<br>{extra_label}: %{{customdata[{custom_idx}]:+,.2f}}{extra_suffix}"
            hover_template += "<extra></extra>"
            fig.add_trace(go.Bar(
                x=bar_df['trade_date'],
                y=bar_df[value_col],
                name=name,
                marker=dict(color=colors, line=dict(width=0)),
                opacity=0.82,
                customdata=custom_data,
                hovertemplate=hover_template
            ))

    fig.add_hline(y=0, line_width=1, line_dash='dash', line_color='#94A3B8')
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(size=20, weight=700, color='#1E293B'),
            x=0.02
        ),
        xaxis_title='日期',
        yaxis_title=yaxis_title,
        hovermode='x unified',
        height=420,
        template='plotly_white',
        plot_bgcolor='rgba(248, 250, 252, 0.5)',
        paper_bgcolor='rgba(248, 250, 252, 0.92)',
        font=dict(family='Inter, PingFang SC, sans-serif'),
        legend=dict(
            orientation='h', yanchor='bottom', y=-0.25,
            xanchor='center', x=0.5,
            bgcolor='rgba(255,255,255,0)', font=dict(size=11)
        ),
        margin=dict(l=20, r=20, t=60, b=20),
        bargap=0.18,
        barmode='group'
    )
    fig.update_xaxes(
        showgrid=True, gridwidth=1, gridcolor='rgba(226,232,240,0.5)',
        showline=True, linewidth=1, linecolor='#E2E8F0'
    )
    fig.update_yaxes(
        showgrid=True, gridwidth=1, gridcolor='rgba(226,232,240,0.5)',
        showline=True, linewidth=1, linecolor='#E2E8F0',
        zeroline=False,
        fixedrange=True
    )
    return fig


def format_macro_value(value, unit: str = "") -> str:
    if value is None or pd.isna(value):
        return "-"
    unit = unit or ""
    return f"{float(value):,.2f}{unit}"


def build_macro_metric_snapshot(df: pd.DataFrame, metric_col: str):
    if df is None or df.empty or metric_col not in df.columns:
        return None, None, None, None

    working = df.dropna(subset=[metric_col]).sort_values("trade_date").copy()
    if working.empty:
        return None, None, None, None

    latest_row = working.iloc[-1]
    latest_value = latest_row[metric_col]
    latest_date = pd.to_datetime(latest_row["trade_date"])
    prev_value = working.iloc[-2][metric_col] if len(working) >= 2 else None
    delta = float(latest_value) - float(prev_value) if prev_value is not None and pd.notna(prev_value) else None
    return latest_date, latest_value, prev_value, delta


def create_macro_line_chart(
    df: pd.DataFrame,
    series: list[tuple[str, str]],
    title: str,
    yaxis_title: str
) -> go.Figure:
    fig = go.Figure()
    palette = ['#2E5BFF', '#EF4444', '#00A76F', '#F59E0B', '#8E54E9']
    chart_df = df.sort_values("trade_date").copy()

    for idx, (column, label) in enumerate(series):
        if column not in chart_df.columns:
            continue
        line_df = chart_df.dropna(subset=[column])
        if line_df.empty:
            continue
        fig.add_trace(go.Scatter(
            x=line_df["trade_date"],
            y=line_df[column],
            mode="lines+markers",
            name=label,
            line=dict(width=2.2, color=palette[idx % len(palette)], shape="spline"),
            marker=dict(size=4, color=palette[idx % len(palette)]),
            hovertemplate=f"<b>{label}</b><br>%{{x|%Y-%m-%d}}<br>{yaxis_title}: %{{y:,.2f}}<extra></extra>"
        ))

    fig.update_layout(
        title=dict(text=title, font=dict(size=20, weight=700, color="#1E293B"), x=0.02),
        xaxis_title="日期",
        yaxis_title=yaxis_title,
        hovermode="x unified",
        height=420,
        template="wealthspark_balanced",
        plot_bgcolor="rgba(248, 250, 252, 0.5)",
        paper_bgcolor="rgba(248, 250, 252, 0.92)",
        font=dict(family="Inter, PingFang SC, sans-serif"),
        legend=dict(
            orientation="h", yanchor="bottom", y=-0.25,
            xanchor="center", x=0.5,
            bgcolor="rgba(255,255,255,0)", font=dict(size=11)
        ),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    fig.update_xaxes(
        showgrid=True, gridwidth=1, gridcolor="rgba(226,232,240,0.5)",
        showline=True, linewidth=1, linecolor="#E2E8F0"
    )
    fig.update_yaxes(
        showgrid=True, gridwidth=1, gridcolor="rgba(226,232,240,0.5)",
        showline=True, linewidth=1, linecolor="#E2E8F0",
        zeroline=False,
        fixedrange=True
    )
    return fig


def render_etf_trend_tab():
    """渲染 ETF 分类趋势 Tab 页"""
    st.subheader("📈 ETF分类份额/规模趋势")
    st.caption("按分类查看 ETF 总份额、总规模的时间序列曲线")

    # 加载分类树
    try:
        category_tree = get_category_tree()
    except Exception as e:
        st.error(f"加载分类信息失败: {e}")
        return

    if not category_tree:
        st.warning("暂无分类数据，请先运行聚合脚本")
        return

    iphone_mode = get_query_param_value("iphone_mode").strip() == "1"

    primary_options = ['全部'] + sorted(category_tree.keys())

    if iphone_mode:
        with st.expander("📂 分类趋势筛选", expanded=True):
            selected_primary = st.selectbox(
                "一级分类",
                options=primary_options,
                index=0,
                key="iphone_trend_primary"
            )

            category_key = selected_primary
            if selected_primary != '全部' and category_tree.get(selected_primary):
                secondary_list = category_tree[selected_primary]
                secondary_options = ['全部(小计)'] + secondary_list
                selected_secondary = st.selectbox(
                    "二级分类",
                    options=secondary_options,
                    index=0,
                    key="iphone_trend_secondary"
                )
                if selected_secondary == '全部(小计)':
                    category_key = selected_primary
                else:
                    category_key = f"{selected_primary}-{selected_secondary}"
            elif selected_primary == '全部':
                category_key = '全部'

            metric = st.radio(
                "查看指标",
                options=['总份额(亿份)', '总规模(亿元)'],
                index=0,
                horizontal=True,
                key="iphone_trend_metric"
            )
            metric_col = 'total_share_yi' if '份额' in metric else 'total_size_yi'

            try:
                available = get_available_dates(limit=1000)
            except Exception as e:
                st.error(f"获取可用日期失败: {e}")
                return

            if not available:
                st.warning("暂无可用交易日数据")
                return

            from datetime import datetime as dt
            all_dates = sorted([dt.strptime(d, '%Y-%m-%d').date() for d in available])
            min_d, max_d = all_dates[0], all_dates[-1]

            date_range = st.slider(
                "时间范围",
                min_value=min_d,
                max_value=max_d,
                value=(min_d, max_d),
                format="YYYY-MM-DD",
                key="iphone_trend_date_range"
            )
    else:
        # 侧边栏筛选器
        st.sidebar.header("📂 分类趋势筛选")

        selected_primary = st.sidebar.selectbox(
            "一级分类",
            options=primary_options,
            index=0,
            key="trend_primary"
        )

        # 二级分类联动
        category_key = selected_primary
        if selected_primary != '全部' and category_tree.get(selected_primary):
            secondary_list = category_tree[selected_primary]
            secondary_options = ['全部(小计)'] + secondary_list
            selected_secondary = st.sidebar.selectbox(
                "二级分类",
                options=secondary_options,
                index=0,
                key="trend_secondary"
            )
            if selected_secondary == '全部(小计)':
                category_key = selected_primary
            else:
                category_key = f"{selected_primary}-{selected_secondary}"
        elif selected_primary == '全部':
            category_key = '全部'

        # 指标选择
        metric = st.sidebar.radio(
            "查看指标",
            options=['总份额(亿份)', '总规模(亿元)'],
            index=0,
            key="trend_metric"
        )
        metric_col = 'total_share_yi' if '份额' in metric else 'total_size_yi'

        # 日期范围
        try:
            available = get_available_dates(limit=1000)
        except Exception as e:
            st.error(f"获取可用日期失败: {e}")
            return

        if not available:
            st.warning("暂无可用交易日数据")
            return

        from datetime import datetime as dt
        all_dates = sorted([dt.strptime(d, '%Y-%m-%d').date() for d in available])
        min_d, max_d = all_dates[0], all_dates[-1]

        date_range = st.sidebar.slider(
            "时间范围",
            min_value=min_d,
            max_value=max_d,
            value=(min_d, max_d),
            format="YYYY-MM-DD",
            key="trend_date_range"
        )

    # 查询时序数据
    try:
        ts_df = get_category_timeseries(
            category_key=category_key,
            start_date=str(date_range[0]),
            end_date=str(date_range[1])
        )
    except Exception as e:
        st.error(f"查询时序数据失败: {e}")
        return

    if ts_df is None or len(ts_df) == 0:
        st.warning(f"分类 [{category_key}] 在所选时间范围内无数据")
        return

    ts_df['trade_date'] = pd.to_datetime(ts_df['trade_date'])
    ts_df[metric_col] = pd.to_numeric(ts_df[metric_col], errors='coerce')
    for col in ['share_change_yi', 'share_change_pct', 'size_change_yi', 'size_change_pct']:
        if col in ts_df.columns:
            ts_df[col] = pd.to_numeric(ts_df[col], errors='coerce')

    # 顶部指标卡片
    latest_metric_date, latest_metric_rows = get_latest_metric_date(ts_df, metric_col)
    valid_ts = ts_df.dropna(subset=[metric_col])
    if len(valid_ts) > 0:
        latest = valid_ts.iloc[-1]
        latest_val = float(latest[metric_col])
        latest_count = int(latest['etf_count'])
        latest_date_str = latest['trade_date'].strftime('%Y-%m-%d')

        if metric_col == 'total_share_yi':
            change_col = 'share_change_yi'
            change_pct_col = 'share_change_pct'
        else:
            change_col = 'size_change_yi'
            change_pct_col = 'size_change_pct'

        if len(valid_ts) >= 2:
            prev_val = float(valid_ts.iloc[-2][metric_col])
            fallback_change = latest_val - prev_val
            fallback_change_pct = (fallback_change / prev_val * 100) if prev_val != 0 else 0
        else:
            fallback_change = 0
            fallback_change_pct = 0

        change = (
            float(latest[change_col])
            if change_col in latest and pd.notna(latest[change_col])
            else fallback_change
        )
        change_pct = (
            float(latest[change_pct_col])
            if change_pct_col in latest and pd.notna(latest[change_pct_col])
            else fallback_change_pct
        )

        kpi_cols = st.columns(3)
        with kpi_cols[0]:
            st.metric(f"{category_key} - ETF只数", f"{latest_count}")
        with kpi_cols[1]:
            st.metric(
                f"{metric} ({latest_date_str})",
                f"{latest_val:,.2f}",
                f"{change:+,.2f} ({change_pct:+.2f}%)"
            )
        with kpi_cols[2]:
            first_val = float(valid_ts.iloc[0][metric_col])
            period_change = latest_val - first_val
            period_pct = (period_change / first_val * 100) if first_val != 0 else 0
            st.metric(
                "区间变动",
                f"{period_change:+,.2f}",
                f"{period_pct:+.2f}%"
            )

    # 时序曲线图
    fig = go.Figure()
    chart_data = ts_df.dropna(subset=[metric_col]).copy()

    fig.add_trace(go.Scatter(
        x=chart_data['trade_date'],
        y=chart_data[metric_col],
        mode='lines',
        name=category_key,
        line=dict(width=2.5, shape='spline', color='#2E5BFF'),
        fill='tozeroy',
        fillcolor='rgba(46, 91, 255, 0.08)',
        hovertemplate='<b>%{x|%Y-%m-%d}</b><br>%{y:,.2f}<extra></extra>'
    ))

    if len(chart_data) >= 20:
        chart_data['ma20'] = chart_data[metric_col].rolling(window=20).mean()
        fig.add_trace(go.Scatter(
            x=chart_data['trade_date'],
            y=chart_data['ma20'],
            mode='lines',
            name='20日均线',
            line=dict(width=1.5, color='#EF4444', dash='dot'),
            hovertemplate='<b>%{x|%Y-%m-%d}</b><br>20MA: %{y:,.2f}<extra></extra>'
        ))

    fig.update_layout(
        title=dict(
            text=f'{category_key} \u2014 {metric} 趋势',
            font=dict(size=20, weight=700, color='#1E293B'),
            x=0.02
        ),
        xaxis_title='日期',
        yaxis_title=metric,
        hovermode='x unified',
        legend=dict(
            orientation='h', yanchor='bottom', y=-0.2,
            xanchor='center', x=0.5,
            bgcolor='rgba(255,255,255,0)', font=dict(size=11)
        ),
        height=500,
        template='plotly_white',
        plot_bgcolor='rgba(248, 250, 252, 0.5)',
        paper_bgcolor='rgba(248, 250, 252, 0.92)',
        font=dict(family='Inter, PingFang SC, sans-serif'),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    fig.update_xaxes(
        showgrid=True, gridwidth=1, gridcolor='rgba(226,232,240,0.5)',
        showline=True, linewidth=1, linecolor='#E2E8F0'
    )
    fig.update_yaxes(
        showgrid=True, gridwidth=1, gridcolor='rgba(226,232,240,0.5)',
        showline=True, linewidth=1, linecolor='#E2E8F0',
        fixedrange=True
    )


    st.plotly_chart(fig, use_container_width=True)

    size_change_chart_data = ts_df.dropna(subset=['size_change_yi']).copy()
    if not size_change_chart_data.empty:
        st.subheader("📉 规模变动曲线")
        st.caption("纵轴展示按当日收盘价 × 份额变化数计算的规模变动金额，hover 可查看变动比例")
        trend_change_view = st.radio(
            "展示方式",
            options=["曲线", "红绿柱状"],
            key="trend_size_change_view",
            horizontal=True
        )
        size_change_fig = create_change_curve_chart(
            df=size_change_chart_data,
            value_col='size_change_yi',
            title=f'{category_key} — 规模变动(亿元)趋势',
            yaxis_title='规模变动(亿元)',
            pct_col='size_change_pct'
        )
        size_change_bar_fig = create_change_bar_chart(
            df=size_change_chart_data,
            value_col='size_change_yi',
            title=f'{category_key} — 规模变动(亿元)红绿柱状图',
            yaxis_title='规模变动(亿元)',
            pct_col='size_change_pct'
        )
        if trend_change_view == "曲线":
            st.plotly_chart(size_change_fig, use_container_width=True)
        else:
            st.plotly_chart(size_change_bar_fig, use_container_width=True)

    # 汇总表格
    try:
        if latest_metric_date is None:
            st.info("当前指标暂无可用于汇总展示的数据")
            return
        summary_date = latest_metric_date.strftime('%Y-%m-%d')
        sum_df = get_agg_summary(summary_date)

        if sum_df is not None and len(sum_df) > 0:
            display_rows = []
            for _, row in sum_df.iterrows():
                level = int(row['level'])
                if level == 1:
                    name = f"  {row['primary_category']}-{row['secondary_category']}"
                elif level == 2:
                    if row['primary_category'] != '全部':
                        name = f"{row['primary_category']}(小计)"
                    else:
                        name = row['primary_category']
                else:
                    name = '全部'

                display_rows.append({
                    '分类': name,
                    'ETF只数': int(row['etf_count']) if pd.notna(row['etf_count']) else 0,
                    '总份额(亿份)': float(row['total_share_yi']) if pd.notna(row['total_share_yi']) else None,
                    '份额变动(亿份)': float(row['share_change_yi']) if pd.notna(row['share_change_yi']) else None,
                    '份额变动比例(%)': float(row['share_change_pct']) if pd.notna(row['share_change_pct']) else None,
                    '总规模(亿元)': float(row['total_size_yi']) if pd.notna(row['total_size_yi']) else None,
                    '规模变动(亿元)': float(row['size_change_yi']) if pd.notna(row['size_change_yi']) else None,
                    '规模变动比例(%)': float(row['size_change_pct']) if pd.notna(row['size_change_pct']) else None,
                })

            disp_df = pd.DataFrame(display_rows)
            st.caption(f"数据日期: {summary_date}")
            st.dataframe(
                disp_df.style.format({
                    '总份额(亿份)': '{:,.2f}',
                    '份额变动(亿份)': '{:,.2f}',
                    '份额变动比例(%)': '{:,.2f}',
                    '总规模(亿元)': '{:,.2f}',
                    '规模变动(亿元)': '{:,.2f}',
                    '规模变动比例(%)': '{:,.2f}'
                }, na_rep='-'),
                use_container_width=True,
                hide_index=True,
                height=600
            )
        else:
            st.info(f"{summary_date} 暂无汇总数据")
    except Exception as e:
        st.warning(f"加载汇总数据失败: {e}")


def render_security_search_tab():
    st.subheader("🔎 个股 / 指数查询")
    st.caption("支持按代码、简称、拼音检索个股或指数，查看最新快照与历史趋势")

    pending_keyword = st.session_state.pop("pending_security_search_keyword", None)
    pending_type = st.session_state.pop("pending_security_search_type", None)
    if pending_type in {"全部", "股票", "指数"}:
        st.session_state["security_search_type"] = pending_type
    if pending_keyword is not None:
        st.session_state["security_search_keyword"] = pending_keyword
        st.session_state.pop("security_search_option", None)

    if "security_search_type" not in st.session_state:
        st.session_state["security_search_type"] = "全部"
    if "security_search_keyword" not in st.session_state:
        st.session_state["security_search_keyword"] = ""

    control_cols = st.columns([1, 1.4, 2.6])
    with control_cols[0]:
        security_type_label = st.radio(
            "检索类型",
            options=["全部", "股票", "指数"],
            horizontal=True,
            key="security_search_type"
        )
    with control_cols[1]:
        keyword = st.text_input(
            "关键字",
            placeholder="输入代码、简称或拼音",
            key="security_search_keyword"
        ).strip()
    type_mapping = {"全部": "all", "股票": "stock", "指数": "index"}

    if not keyword:
        st.info("请输入代码、简称或拼音开始检索，例如 600519、贵州茅台、000001.SH")
        return

    try:
        candidate_df = load_security_search(keyword, type_mapping[security_type_label], limit=30)
    except Exception as e:
        st.error(f"检索证券失败: {e}")
        return

    if candidate_df is None or len(candidate_df) == 0:
        st.warning("未检索到匹配的个股或指数，请尝试更换关键字")
        return

    option_labels = [format_security_option(row) for _, row in candidate_df.iterrows()]
    with control_cols[2]:
        selected_label = st.selectbox(
            "匹配结果",
            options=option_labels,
            index=0,
            key="security_search_option"
        )

    selected_idx = option_labels.index(selected_label)
    selected_row = candidate_df.iloc[selected_idx]
    selected_type = selected_row['security_type']
    selected_code = selected_row['ts_code']
    financial_df = None

    try:
        profile_df = load_security_profile(selected_code, selected_type)
        ts_df = load_security_timeseries(selected_code, selected_type)
        if selected_type == 'stock':
            financial_df = load_security_financial_timeseries(selected_code, selected_type)
    except Exception as e:
        st.error(f"加载证券详情失败: {e}")
        return

    if profile_df is None or len(profile_df) == 0:
        st.warning("未查询到该证券的详情数据")
        return
    if ts_df is None or len(ts_df) == 0:
        st.warning("未查询到该证券的历史时序数据")
        return

    profile = profile_df.iloc[0]
    ts_df = ts_df.copy()
    ts_df['trade_date'] = pd.to_datetime(ts_df['trade_date'])
    ts_df = ts_df.sort_values('trade_date')
    trend_analysis = build_security_trend_analysis(ts_df, selected_type)

    min_date = ts_df['trade_date'].min().date()
    max_date = ts_df['trade_date'].max().date()
    default_start = min_date if selected_type == 'stock' else max(min_date, max_date - timedelta(days=365))
    metric_config = get_security_metric_config(selected_type)

    filter_cols = st.columns([1.5, 1.3, 1.2])
    with filter_cols[0]:
        date_range = st.slider(
            "时间范围",
            min_value=min_date,
            max_value=max_date,
            value=(default_start, max_date),
            format="YYYY-MM-DD",
            key=f"security_date_range_{selected_type}_{selected_code}"
        )
    with filter_cols[1]:
        metric_label = st.selectbox(
            "趋势指标",
            options=list(metric_config.keys()),
            index=0,
            key=f"security_metric_{selected_type}_{selected_code}"
        )
    with filter_cols[2]:
        st.metric("数据区间", f"{len(ts_df):,} 条")

    filtered_df = ts_df[
        (ts_df['trade_date'].dt.date >= date_range[0]) &
        (ts_df['trade_date'].dt.date <= date_range[1])
    ].copy()
    if filtered_df.empty:
        st.warning("当前时间范围内没有数据")
        return

    filtered_financial_df = None
    if financial_df is not None and len(financial_df) > 0:
        financial_df = financial_df.copy()
        financial_df['end_date'] = pd.to_datetime(financial_df['end_date'])
        if 'ann_date' in financial_df.columns:
            financial_df['ann_date'] = pd.to_datetime(financial_df['ann_date'], errors='coerce')
        filtered_financial_df = financial_df[
            (financial_df['end_date'].dt.date >= date_range[0]) &
            (financial_df['end_date'].dt.date <= date_range[1])
        ].copy()

    title_name = profile.get('name') or selected_row.get('name') or selected_code
    subtitle_parts = [selected_code]
    if selected_type == 'stock':
        subtitle_parts.extend([value for value in [profile.get('industry'), profile.get('market')] if value and not pd.isna(value)])
    st.markdown(f"### {title_name}")
    st.caption(" | ".join(subtitle_parts))

    latest_trade_date = format_optional_date(profile.get('latest_trade_date'))
    if selected_type == 'stock':
        metric_cols_top = st.columns(5)
        metric_cols_top[0].metric("最新交易日", latest_trade_date)
        metric_cols_top[1].metric("收盘价(元)", format_optional_number(profile.get('close')))
        metric_cols_top[2].metric("PE_TTM", format_optional_number(profile.get('pe_ttm')))
        metric_cols_top[3].metric("PB", format_optional_number(profile.get('pb')))
        metric_cols_top[4].metric("总市值(亿元)", format_optional_number(profile.get('total_mv'), scale=10000.0))

        metric_cols_bottom = st.columns(5)
        metric_cols_bottom[0].metric("ROE(%)", format_optional_number(profile.get('roe')))
        metric_cols_bottom[1].metric("ROA(%)", format_optional_number(profile.get('roa')))
        metric_cols_bottom[2].metric("毛利率(%)", format_optional_number(profile.get('gross_margin')))
        metric_cols_bottom[3].metric("净利润(亿元)", format_optional_number(profile.get('n_income'), scale=100000000.0))
        metric_cols_bottom[4].metric("经营现金流(亿元)", format_optional_number(profile.get('n_cashflow_act'), scale=100000000.0))

        render_security_trend_analysis(trend_analysis, selected_type)

        info_cols = st.columns(2)
        with info_cols[0]:
            st.dataframe(
                pd.DataFrame([
                    {"字段": "上市日期", "值": format_optional_date(profile.get('list_date'))},
                    {"字段": "所属行业", "值": profile.get('industry') or "-"},
                    {"字段": "市场板块", "值": profile.get('market') or "-"},
                    {"字段": "上市状态", "值": profile.get('list_status') or "-"},
                    {"字段": "法人代表", "值": profile.get('act_name') or "-"},
                ]),
                use_container_width=True,
                hide_index=True
            )
        with info_cols[1]:
            st.dataframe(
                pd.DataFrame([
                    {"字段": "最近财报期", "值": format_optional_date(profile.get('fina_end_date'))},
                    {"字段": "最近利润期", "值": format_optional_date(profile.get('income_end_date'))},
                    {"字段": "最近资产负债表期", "值": format_optional_date(profile.get('balance_end_date'))},
                    {"字段": "总资产(亿元)", "值": format_optional_number(profile.get('total_assets'), scale=100000000.0)},
                    {"字段": "总负债(亿元)", "值": format_optional_number(profile.get('total_liab'), scale=100000000.0)},
                ]),
                use_container_width=True,
                hide_index=True
            )
        
        st.markdown("##### 📜 主营与产品")
        st.info(f"**主要业务**：{profile.get('main_business') or '-'}")
        st.info(f"**产品及业务范围**：{profile.get('business_scope') or '-'}")
        
        with st.expander("📝 订正主营与产品信息"):
            configured_password = get_stock_info_edit_password()
            if not configured_password:
                st.warning("当前未配置编辑权限密码，修改功能已禁用。请设置 ETF_STOCK_INFO_EDIT_PASSWORD 或 ETF_EDIT_PASSWORD 后重启应用。")
            else:
                status_cols = st.columns([4, 1.2])
                if has_stock_info_edit_permission():
                    status_cols[0].success("当前会话已获得个股信息修改权限。")
                    if status_cols[1].button("退出权限", key=f"revoke_stock_edit_permission_{selected_code}"):
                        st.session_state["stock_info_edit_authorized"] = False
                        st.rerun()
                else:
                    access_password = status_cols[0].text_input(
                        "编辑权限密码",
                        type="password",
                        key=f"stock_edit_password_{selected_code}"
                    )
                    if status_cols[1].button("权限验证", key=f"grant_stock_edit_permission_{selected_code}"):
                        if grant_stock_info_edit_permission(access_password):
                            st.success("权限验证成功，请继续提交修订内容。")
                            st.rerun()
                        st.error("权限验证失败，请检查密码。")
                    st.info("仅通过权限验证的会话可以修改个股主营与产品信息。")

                if has_stock_info_edit_permission():
                    with st.form(key=f"edit_custom_info_{selected_code}"):
                        custom_mb = st.text_area("新的主要业务", value=profile.get('main_business') or '')
                        custom_pd = st.text_area("新的产品及业务范围", value=profile.get('business_scope') or '')
                        if st.form_submit_button("保存修订，优先应用新数据"):
                            mb_stripped = custom_mb.strip()
                            pd_stripped = custom_pd.strip()

                            if not has_stock_info_edit_permission():
                                st.error("当前会话没有修改权限，请重新完成权限验证。")
                            elif not mb_stripped and not pd_stripped:
                                update_stock_custom_info(selected_code, '', '')
                                st.success("修订已保存 (已清空自定义信息)！请重新检索刷新结果。")
                            elif len(mb_stripped) < 2 and len(pd_stripped) < 2:
                                st.error("保存失败：修订内容过短。若要清空请完全留白，否则请填写有效的业务信息。")
                            elif mb_stripped == (profile.get('main_business') or '').strip() and \
                                 pd_stripped == (profile.get('business_scope') or '').strip():
                                st.warning("您未做任何实质性修改。")
                            else:
                                update_stock_custom_info(selected_code, mb_stripped, pd_stripped)
                                st.success("更新成功！请重新点击关键字刷新搜索结果。")

        with st.expander("🧱 前十大股东 / 前十大流通股东", expanded=False):
            st.caption("入口已迁移到个股查询页，可按报告期直接查询股东结构（结果缓存 5 分钟）。")

            top10_period_options = load_fund_hot_stock_periods()
            if not top10_period_options:
                top10_period_options = [datetime.now().strftime("%Y-%m-%d")]

            top10_period_key = f"security_top10_period_{selected_code}"
            if top10_period_key not in st.session_state:
                st.session_state[top10_period_key] = top10_period_options[0]
            elif st.session_state[top10_period_key] not in top10_period_options:
                st.session_state[top10_period_key] = top10_period_options[0]

            if st.session_state.get("security_top10_last_code") != selected_code:
                st.session_state["security_top10_holders"] = pd.DataFrame()
                st.session_state["security_top10_floatholders"] = pd.DataFrame()
                st.session_state["security_top10_errors"] = {}
                st.session_state["security_top10_status"] = "待查询"
                st.session_state["security_top10_last_code"] = selected_code

            top10_ctl_period, top10_ctl_btn = st.columns([1.6, 1.0])
            with top10_ctl_period:
                top10_period = st.selectbox(
                    "股东结构报告期",
                    options=top10_period_options,
                    key=top10_period_key,
                )
            with top10_ctl_btn:
                st.caption(" ")
                query_top10_clicked = st.button(
                    "查询/刷新前十大股东",
                    type="primary",
                    key=f"btn_security_top10_{selected_code}",
                )

            top10_query_signature = f"{selected_code}|{top10_period}"
            auto_query_needed = st.session_state.get("security_top10_last_signature") != top10_query_signature

            if query_top10_clicked or auto_query_needed:
                st.session_state["security_top10_holders"] = pd.DataFrame()
                st.session_state["security_top10_floatholders"] = pd.DataFrame()
                st.session_state["security_top10_errors"] = {}
                st.session_state["security_top10_status"] = "查询中"
                try:
                    top10_pack = load_security_top10_shareholders(
                        symbol=selected_code,
                        period=str(top10_period).replace("-", ""),
                    )
                    st.session_state["security_top10_holders"] = top10_pack.get("top10_holders", pd.DataFrame())
                    st.session_state["security_top10_floatholders"] = top10_pack.get("top10_floatholders", pd.DataFrame())
                    st.session_state["security_top10_errors"] = top10_pack.get("errors", {}) or {}

                    has_holder = isinstance(st.session_state["security_top10_holders"], pd.DataFrame) and not st.session_state["security_top10_holders"].empty
                    has_float = isinstance(st.session_state["security_top10_floatholders"], pd.DataFrame) and not st.session_state["security_top10_floatholders"].empty
                    st.session_state["security_top10_status"] = "查询成功" if (has_holder or has_float) else "该报告期暂无数据"
                except Exception as top10_exc:
                    logger.warning(f"security_search query_stock_top10_shareholders failed: {top10_exc}", exc_info=True)
                    st.session_state["security_top10_holders"] = pd.DataFrame()
                    st.session_state["security_top10_floatholders"] = pd.DataFrame()
                    st.session_state["security_top10_errors"] = {"query": str(top10_exc)}
                    st.session_state["security_top10_status"] = "查询异常"
                finally:
                    st.session_state["security_top10_last_signature"] = top10_query_signature

            top10_holders = st.session_state.get("security_top10_holders")
            top10_floatholders = st.session_state.get("security_top10_floatholders")
            top10_errors = st.session_state.get("security_top10_errors", {}) or {}
            top10_status = st.session_state.get("security_top10_status", "待查询")

            has_top10_holders = isinstance(top10_holders, pd.DataFrame) and not top10_holders.empty
            has_top10_float = isinstance(top10_floatholders, pd.DataFrame) and not top10_floatholders.empty

            st.info(f"📌 当前个股：{title_name}（{selected_code}）｜报告期：{top10_period}｜状态：{top10_status}")

            if has_top10_holders or has_top10_float:
                def _fmt_pct(v):
                    return f"{float(v):.2f}%" if pd.notna(v) else "-"

                def _fmt_shares(v):
                    if pd.isna(v):
                        return "-"
                    val = float(v)
                    return f"{val / 1e8:,.2f} 亿股" if abs(val) >= 1e8 else f"{val:,.0f} 股"

                holder_total_ratio = pd.to_numeric(top10_holders.get("hold_ratio"), errors="coerce").fillna(0).sum() if has_top10_holders else 0
                holder_top3_ratio = pd.to_numeric(top10_holders.get("hold_ratio"), errors="coerce").fillna(0).head(3).sum() if has_top10_holders else 0
                float_total_ratio = pd.to_numeric(top10_floatholders.get("hold_float_ratio"), errors="coerce").fillna(0).sum() if has_top10_float else 0
                float_change_total = pd.to_numeric(top10_floatholders.get("hold_change"), errors="coerce").fillna(0).sum() if has_top10_float else 0

                top10_metrics = st.columns(4)
                top10_metrics[0].metric("前十股东合计持股", _fmt_pct(holder_total_ratio))
                top10_metrics[1].metric("前三股东集中度", _fmt_pct(holder_top3_ratio))
                top10_metrics[2].metric("前十流通股东锁仓", _fmt_pct(float_total_ratio))
                top10_metrics[3].metric("流通股东净变动", _fmt_shares(float_change_total))

                tab_holder, tab_float = st.tabs(["🏛 前十大股东", "🔓 前十大流通股东"])
                with tab_holder:
                    if has_top10_holders:
                        holder_show = top10_holders.copy()
                        for col in ["hold_amount", "hold_ratio", "hold_float_ratio", "hold_change"]:
                            if col in holder_show.columns:
                                holder_show[col] = pd.to_numeric(holder_show[col], errors="coerce")
                        holder_show = holder_show.rename(columns={
                            "holder_name": "股东名称",
                            "hold_amount": "持股数量",
                            "hold_ratio": "占总股本比(%)",
                            "hold_float_ratio": "占流通股比(%)",
                            "hold_change": "持股变动",
                            "holder_type": "股东类型",
                        })
                        for col in ["占总股本比(%)", "占流通股比(%)"]:
                            if col in holder_show.columns:
                                holder_show[col] = holder_show[col].map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
                        for col in ["持股数量", "持股变动"]:
                            if col in holder_show.columns:
                                holder_show[col] = holder_show[col].map(lambda v: f"{v:,.0f}" if pd.notna(v) else "-")
                        show_cols = [c for c in ["股东名称", "持股数量", "占总股本比(%)", "占流通股比(%)", "持股变动", "股东类型"] if c in holder_show.columns]
                        st.dataframe(holder_show[show_cols], use_container_width=True, hide_index=True)
                    else:
                        st.info("当前报告期暂无前十大股东数据。")

                with tab_float:
                    if has_top10_float:
                        float_show = top10_floatholders.copy()
                        for col in ["hold_amount", "hold_ratio", "hold_float_ratio", "hold_change"]:
                            if col in float_show.columns:
                                float_show[col] = pd.to_numeric(float_show[col], errors="coerce")
                        float_show = float_show.rename(columns={
                            "holder_name": "股东名称",
                            "hold_amount": "持股数量",
                            "hold_ratio": "占总股本比(%)",
                            "hold_float_ratio": "占流通股比(%)",
                            "hold_change": "持股变动",
                            "holder_type": "股东类型",
                        })
                        for col in ["占总股本比(%)", "占流通股比(%)"]:
                            if col in float_show.columns:
                                float_show[col] = float_show[col].map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
                        for col in ["持股数量", "持股变动"]:
                            if col in float_show.columns:
                                float_show[col] = float_show[col].map(lambda v: f"{v:,.0f}" if pd.notna(v) else "-")
                        show_cols = [c for c in ["股东名称", "持股数量", "占总股本比(%)", "占流通股比(%)", "持股变动", "股东类型"] if c in float_show.columns]
                        st.dataframe(float_show[show_cols], use_container_width=True, hide_index=True)
                    else:
                        st.info("当前报告期暂无前十大流通股东数据。")
            elif top10_errors:
                err_text = "；".join([str(v) for v in top10_errors.values() if str(v).strip()])
                st.info(f"前十大股东数据暂不可用：{err_text or '接口暂无返回'}")

    else:
        metric_cols_top = st.columns(5)
        metric_cols_top[0].metric("最新交易日", latest_trade_date)
        metric_cols_top[1].metric("收盘点位", format_optional_number(profile.get('close')))
        metric_cols_top[2].metric("PE", format_optional_number(profile.get('pe')))
        metric_cols_top[3].metric("PB", format_optional_number(profile.get('pb')))
        metric_cols_top[4].metric("总市值(亿元)", format_optional_number(profile.get('total_mv'), scale=10000.0))

        metric_cols_bottom = st.columns(4)
        metric_cols_bottom[0].metric("流通市值(亿元)", format_optional_number(profile.get('float_mv'), scale=10000.0))
        metric_cols_bottom[1].metric("换手率(%)", format_optional_number(profile.get('turnover_rate')))
        metric_cols_bottom[2].metric("总股本(亿股)", format_optional_number(profile.get('total_share'), scale=10000.0))
        metric_cols_bottom[3].metric("流通股本(亿股)", format_optional_number(profile.get('float_share'), scale=10000.0))

        render_security_trend_analysis(trend_analysis, selected_type)

    metric_meta = metric_config[metric_label]
    metric_col = metric_meta['column']
    metric_scale = float(metric_meta['scale'])
    metric_digits = int(metric_meta['digits'])
    chart_df = filtered_df.dropna(subset=[metric_col]).copy()

    if chart_df.empty:
        st.warning("所选指标在当前时间范围内没有可展示的数据")
        return

    chart_df['metric_value'] = pd.to_numeric(chart_df[metric_col], errors='coerce') / metric_scale
    chart_df = chart_df.dropna(subset=['metric_value'])
    if chart_df.empty:
        st.warning("所选指标无法转换为可绘制的数值")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=chart_df['trade_date'],
        y=chart_df['metric_value'],
        mode='lines',
        name=metric_label,
        line=dict(width=2.6, shape='spline', color='#2563EB'),
        fill='tozeroy',
        fillcolor='rgba(37, 99, 235, 0.08)',
        hovertemplate=f"<b>{title_name}</b><br>%{{x|%Y-%m-%d}}<br>{metric_label}: %{{y:,.{metric_digits}f}}<extra></extra>"
    ))
    fig.update_layout(
        title=dict(text=f'{title_name} — {metric_label}趋势', x=0.02, font=dict(size=20, color='#1E293B')),
        xaxis_title='日期',
        yaxis_title=metric_label,
        hovermode='x unified',
        height=500,
        template='plotly_white',
        plot_bgcolor='rgba(248, 250, 252, 0.5)',
        paper_bgcolor='rgba(248, 250, 252, 0.92)',
        font=dict(family='Inter, PingFang SC, sans-serif'),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(226,232,240,0.5)')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(226,232,240,0.5)', fixedrange=True)
    st.plotly_chart(fig, use_container_width=True)

    if selected_type == 'stock':
        tab_valuation, tab_financial, tab_capital = st.tabs(["📈 估值", "🧾 财务", "🏦 市值股本"])

        with tab_valuation:
            st.caption("展示静态市盈率、动态市盈率与股息率曲线")
            valuation_metrics = [
                ('静态市盈率曲线', 'pe', '静态市盈率PE', 1.0, 2, '#2563EB'),
                ('动态市盈率曲线', 'pe_ttm', '动态市盈率PE_TTM', 1.0, 2, '#7C3AED'),
                ('股息率曲线', 'dv_ratio', '股息率(%)', 1.0, 2, '#F59E0B'),
            ]
            valuation_cols = st.columns(2)
            for index, (title, column, yaxis_title, scale, digits, color) in enumerate(valuation_metrics):
                chart = create_metric_line_chart(
                    filtered_df,
                    x_col='trade_date',
                    y_col=column,
                    title=title,
                    yaxis_title=yaxis_title,
                    scale=scale,
                    digits=digits,
                    color=color
                )
                with valuation_cols[index % 2]:
                    if chart is not None:
                        st.plotly_chart(chart, use_container_width=True)
                    else:
                        st.info(f"{title} 暂无可展示数据")

        with tab_financial:
            st.caption("财务柱状图按报告期展示，默认与上方时间范围联动")
            financial_metrics = [
                ('营业总收入柱状图', 'total_revenue', '营业总收入(亿元)', 100000000.0, 2, '#2563EB', '#1D4ED8'),
                ('净利润柱状图', 'net_profit', '净利润(亿元)', 100000000.0, 2, '#7C3AED', '#059669'),
                ('扣非净利润柱状图', 'profit_dedt', '扣非净利润(亿元)', 100000000.0, 2, '#F59E0B', '#059669'),
            ]
            financial_cols = st.columns(3)
            for index, (title, column, yaxis_title, scale, digits, positive_color, negative_color) in enumerate(financial_metrics):
                chart = create_financial_bar_chart(
                    filtered_financial_df,
                    x_col='end_date',
                    y_col=column,
                    title=title,
                    yaxis_title=yaxis_title,
                    scale=scale,
                    digits=digits,
                    positive_color=positive_color,
                    negative_color=negative_color
                )
                with financial_cols[index]:
                    if chart is not None:
                        st.plotly_chart(chart, use_container_width=True)
                    else:
                        st.info(f"{title} 暂无可展示数据")

        with tab_capital:
            st.caption("展示总市值、流通市值、总股本与流通股本曲线")
            cap_metrics = [
                ('总市值曲线', 'total_mv', '总市值(亿元)', 10000.0, 2, '#DC2626'),
                ('流通市值曲线', 'circ_mv', '流通市值(亿元)', 10000.0, 2, '#EA580C'),
                ('总股本曲线', 'total_share', '总股本(亿股)', 10000.0, 2, '#0891B2'),
                ('流通股本曲线', 'float_share', '流通股本(亿股)', 10000.0, 2, '#059669'),
            ]
            cap_cols = st.columns(2)
            for index, (title, column, yaxis_title, scale, digits, color) in enumerate(cap_metrics):
                chart = create_metric_line_chart(
                    filtered_df,
                    x_col='trade_date',
                    y_col=column,
                    title=title,
                    yaxis_title=yaxis_title,
                    scale=scale,
                    digits=digits,
                    color=color
                )
                with cap_cols[index % 2]:
                    if chart is not None:
                        st.plotly_chart(chart, use_container_width=True)
                    else:
                        st.info(f"{title} 暂无可展示数据")
    else:
        tab_index_valuation, tab_index_capital, tab_index_turnover = st.tabs(["📈 估值", "🏦 市值股本", "🔁 换手率"])

        with tab_index_valuation:
            st.caption("展示指数静态市盈率与动态市盈率曲线")
            valuation_metrics = [
                ('静态市盈率曲线', 'pe', '静态市盈率PE', 1.0, 2, '#2563EB'),
                ('动态市盈率曲线', 'pe_ttm', '动态市盈率PE_TTM', 1.0, 2, '#7C3AED'),
            ]
            valuation_cols = st.columns(2)
            for index, (title, column, yaxis_title, scale, digits, color) in enumerate(valuation_metrics):
                chart = create_metric_line_chart(
                    filtered_df,
                    x_col='trade_date',
                    y_col=column,
                    title=title,
                    yaxis_title=yaxis_title,
                    scale=scale,
                    digits=digits,
                    color=color
                )
                with valuation_cols[index % 2]:
                    if chart is not None:
                        st.plotly_chart(chart, use_container_width=True)
                    else:
                        st.info(f"{title} 暂无可展示数据")

        with tab_index_capital:
            st.caption("展示指数总市值、流通市值、总股本与流通股本曲线")
            cap_metrics = [
                ('当日总市值曲线', 'total_mv', '总市值(亿元)', 10000.0, 2, '#DC2626'),
                ('当日流通市值曲线', 'float_mv', '流通市值(亿元)', 10000.0, 2, '#EA580C'),
                ('当日总股本曲线', 'total_share', '总股本(亿股)', 10000.0, 2, '#0891B2'),
                ('当日流通股本曲线', 'float_share', '流通股本(亿股)', 10000.0, 2, '#059669'),
            ]
            cap_cols = st.columns(2)
            for index, (title, column, yaxis_title, scale, digits, color) in enumerate(cap_metrics):
                chart = create_metric_line_chart(
                    filtered_df,
                    x_col='trade_date',
                    y_col=column,
                    title=title,
                    yaxis_title=yaxis_title,
                    scale=scale,
                    digits=digits,
                    color=color
                )
                with cap_cols[index % 2]:
                    if chart is not None:
                        st.plotly_chart(chart, use_container_width=True)
                    else:
                        st.info(f"{title} 暂无可展示数据")

        with tab_index_turnover:
            st.caption("展示指数换手率曲线")
            turnover_chart = create_metric_line_chart(
                filtered_df,
                x_col='trade_date',
                y_col='turnover_rate',
                title='换手率曲线',
                yaxis_title='换手率(%)',
                scale=1.0,
                digits=2,
                color='#0F766E'
            )
            if turnover_chart is not None:
                st.plotly_chart(turnover_chart, use_container_width=True)
            else:
                st.info("换手率曲线 暂无可展示数据")

    display_df = filtered_df.sort_values('trade_date', ascending=False).copy()
    display_df['日期'] = display_df['trade_date'].dt.strftime('%Y-%m-%d')
    ordered_cols = ['日期'] + [config['column'] for config in metric_config.values()]
    ordered_cols = [column for column in ordered_cols if column in display_df.columns]

    rename_map = {
        'close': '收盘价/点位',
        'turnover_rate': '换手率(%)',
        'turnover_rate_f': '自由流通换手率(%)',
        'volume_ratio': '量比',
        'pe': 'PE',
        'pe_ttm': 'PE_TTM',
        'pb': 'PB',
        'ps': 'PS',
        'ps_ttm': 'PS_TTM',
        'dv_ratio': '股息率(%)',
        'dv_ttm': '股息率TTM(%)',
        'total_share': '总股本',
        'float_share': '流通股本',
        'free_share': '自由流通股本',
        'total_mv': '总市值',
        'circ_mv': '流通市值',
        'float_mv': '流通市值',
    }
    st.subheader("📋 历史数据")
    st.dataframe(
        display_df[ordered_cols].rename(columns=rename_map),
        use_container_width=True,
        hide_index=True,
        height=460
    )


def render_wide_index_tab():
    st.subheader("📊 宽基指数ETF总览")
    st.caption("展示 ETF 基准指数代码相同且二级分类为宽基的基金日度总份额、总规模及较前一日变动")

    try:
        available = get_wide_index_available_dates(limit=2000)
    except Exception as e:
        st.error(f"获取宽基指数聚合日期失败: {e}")
        return

    if not available:
        st.warning("暂无宽基指数聚合数据，请先运行聚合脚本")
        return

    all_dates = sorted(list(pd.to_datetime(available).date))
    min_d, max_d = all_dates[0], all_dates[-1]
    default_start = max(min_d, max_d - timedelta(days=180))

    control_cols = st.columns([1.4, 1.6, 1.2])
    with control_cols[0]:
        date_range = st.slider(
            "时间范围",
            min_value=min_d,
            max_value=max_d,
            value=(default_start, max_d),
            format="YYYY-MM-DD",
            key="wide_index_date_range"
        )
    try:
        base_df = get_wide_index_timeseries(
            start_date=str(date_range[0]),
            end_date=str(date_range[1])
        )
    except Exception as e:
        st.error(f"查询宽基指数聚合数据失败: {e}")
        return

    if base_df is None or len(base_df) == 0:
        st.warning("所选时间范围内暂无宽基指数数据")
        return

    code_name_df = (
        base_df[['benchmark_index_code', 'benchmark_index_name']]
        .drop_duplicates()
        .sort_values(['benchmark_index_code'])
    )
    code_to_name = dict(zip(code_name_df['benchmark_index_code'], code_name_df['benchmark_index_name']))
    name_options = [code_to_name[code] for code in code_name_df['benchmark_index_code'].tolist()]
    default_names = name_options[:4] if len(name_options) > 4 else name_options

    with control_cols[1]:
        selected_names = st.multiselect(
            "宽基指数",
            options=name_options,
            default=default_names,
            key="wide_index_names"
        )
    with control_cols[2]:
        metric = st.radio(
            "查看指标",
            options=["总份额(亿份)", "总规模(亿元)"],
            index=0,
            key="wide_index_metric"
        )

    if not selected_names:
        st.info("请至少选择一个宽基指数")
        return

    selected_codes = [code for code, name in code_to_name.items() if name in selected_names]
    ts_df = base_df[base_df['benchmark_index_code'].isin(selected_codes)].copy()
    ts_df['trade_date'] = pd.to_datetime(ts_df['trade_date'])

    if ts_df.empty:
        st.warning("当前筛选条件下暂无数据")
        return

    metric_col = 'total_share_yi' if '??' in metric else 'total_size_yi'
    metric_title = "???(??)" if '??' in metric else "???(??)"

    latest_date, latest_df = get_latest_metric_date(ts_df, metric_col)
    if latest_date is None:
        st.warning(f"当前筛选条件下暂无{metric}数据")
        return

    share_total = latest_df['total_share_yi'].sum(min_count=1)
    share_delta = latest_df['share_change_yi'].sum(min_count=1)
    share_base = (
        float(share_total) - float(share_delta)
        if pd.notna(share_total) and pd.notna(share_delta)
        else None
    )
    share_delta_pct = (
        float(share_delta) / share_base * 100
        if share_base not in (None, 0)
        else None
    )
    size_total = latest_df['total_size_yi'].sum(min_count=1)
    size_delta = latest_df['size_change_yi'].sum(min_count=1)
    size_base = (
        float(size_total) - float(size_delta)
        if pd.notna(size_total) and pd.notna(size_delta)
        else None
    )
    size_delta_pct = (
        float(size_delta) / size_base * 100
        if size_base not in (None, 0)
        else None
    )

    kpi_cols = st.columns(4)
    with kpi_cols[0]:
        st.metric("最新交易日", latest_date.strftime('%Y-%m-%d'))
    with kpi_cols[1]:
        st.metric(
            "选中指数数",
            f"{latest_df['benchmark_index_code'].nunique()}"
        )
    with kpi_cols[2]:
        st.metric(
            "总份额(亿份)",
            f"{float(share_total):,.2f}" if pd.notna(share_total) else "-",
            format_metric_delta(share_delta, share_delta_pct)
        )
    with kpi_cols[3]:
        st.metric(
            "总规模(亿元)",
            f"{float(size_total):,.2f}" if pd.notna(size_total) else "-",
            format_metric_delta(size_delta, size_delta_pct)
        )

    metric_col = 'total_share_yi' if '份额' in metric else 'total_size_yi'
    metric_title = "总份额(亿份)" if '份额' in metric else "总规模(亿元)"
    chart_df = ts_df.sort_values(['benchmark_index_name', 'trade_date']).copy()

    fig = go.Figure()
    color_palette = [
        '#2E5BFF', '#8E54E9', '#FF9966', '#00D4AA', '#FF6B9D',
        '#FFC233', '#00C9FF', '#FF5757', '#A0D911', '#9254DE',
        '#1D4ED8', '#059669', '#F97316'
    ]
    for idx, name in enumerate(selected_names):
        line_df = chart_df[chart_df['benchmark_index_name'] == name]
        if line_df.empty:
            continue
        fig.add_trace(go.Scatter(
            x=line_df['trade_date'],
            y=line_df[metric_col],
            mode='lines',
            name=name,
            line=dict(width=2.4 if idx < 4 else 1.8, color=color_palette[idx % len(color_palette)], shape='spline'),
            hovertemplate=f"<b>{name}</b><br>%{{x|%Y-%m-%d}}<br>{metric_title}: %{{y:,.2f}}<extra></extra>"
        ))

    fig.update_layout(
        title=dict(
            text=f'宽基指数ETF {metric_title} 趋势',
            font=dict(size=20, weight=700, color='#1E293B'),
            x=0.02
        ),
        xaxis_title='日期',
        yaxis_title=metric_title,
        hovermode='x unified',
        height=520,
        template='plotly_white',
        plot_bgcolor='rgba(248, 250, 252, 0.5)',
        paper_bgcolor='rgba(248, 250, 252, 0.92)',
        font=dict(family='Inter, PingFang SC, sans-serif'),
        legend=dict(
            orientation='h', yanchor='bottom', y=-0.28,
            xanchor='center', x=0.5,
            bgcolor='rgba(255,255,255,0)', font=dict(size=11)
        ),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    fig.update_xaxes(
        showgrid=True, gridwidth=1, gridcolor='rgba(226,232,240,0.5)',
        showline=True, linewidth=1, linecolor='#E2E8F0'
    )
    fig.update_yaxes(
        showgrid=True, gridwidth=1, gridcolor='rgba(226,232,240,0.5)',
        showline=True, linewidth=1, linecolor='#E2E8F0',
        fixedrange=True
    )
    st.plotly_chart(fig, use_container_width=True)

    size_change_chart_df = chart_df.dropna(subset=['size_change_yi']).copy()
    if not size_change_chart_df.empty:
        st.caption("纵轴展示按当日收盘价 × 份额变化数计算的规模变动金额，hover 可查看变动比例")
        wide_index_change_view = st.radio(
            "规模变动展示方式",
            options=["曲线", "红绿柱状"],
            key="wide_index_size_change_view",
            horizontal=True
        )
        size_change_fig = create_change_curve_chart(
            df=size_change_chart_df,
            value_col='size_change_yi',
            title='宽基指数ETF 规模变动(亿元)趋势',
            yaxis_title='规模变动(亿元)',
            pct_col='size_change_pct',
            series_col='benchmark_index_name',
            series_names=selected_names,
            color_palette=color_palette
        )
        size_change_bar_fig = create_change_bar_chart(
            df=size_change_chart_df,
            value_col='size_change_yi',
            title='宽基指数ETF 规模变动(亿元)红绿柱状图',
            yaxis_title='规模变动(亿元)',
            pct_col='size_change_pct',
            series_col='benchmark_index_name',
            series_names=selected_names
        )
        if wide_index_change_view == "曲线":
            st.plotly_chart(size_change_fig, use_container_width=True)
        else:
            st.plotly_chart(size_change_bar_fig, use_container_width=True)

    st.subheader("📋 每日聚合明细")
    display_df = ts_df.sort_values(['trade_date', 'benchmark_index_code'], ascending=[False, True]).copy()
    display_df['日期'] = display_df['trade_date'].dt.strftime('%Y-%m-%d')
    display_df['宽基指数'] = display_df['benchmark_index_name']
    display_df['ETF只数'] = display_df['etf_count'].fillna(0).astype(int)
    display_df['总份额(亿份)'] = pd.to_numeric(display_df['total_share_yi'], errors='coerce')
    display_df['份额变动(亿份)'] = pd.to_numeric(display_df['share_change_yi'], errors='coerce')
    display_df['份额变动比例(%)'] = pd.to_numeric(display_df['share_change_pct'], errors='coerce')
    display_df['总规模(亿元)'] = pd.to_numeric(display_df['total_size_yi'], errors='coerce')
    display_df['规模变动(亿元)'] = pd.to_numeric(display_df['size_change_yi'], errors='coerce')
    display_df['规模变动比例(%)'] = pd.to_numeric(display_df['size_change_pct'], errors='coerce')

    st.dataframe(
        display_df[
            ['日期', '宽基指数', 'benchmark_index_code', 'ETF只数',
             '总份额(亿份)', '份额变动(亿份)', '份额变动比例(%)',
             '总规模(亿元)', '规模变动(亿元)', '规模变动比例(%)']
        ].rename(columns={'benchmark_index_code': '基准指数代码'}).style.format({
            '总份额(亿份)': '{:,.2f}',
            '份额变动(亿份)': '{:,.2f}',
            '份额变动比例(%)': '{:,.2f}',
            '总规模(亿元)': '{:,.2f}',
            '规模变动(亿元)': '{:,.2f}',
            '规模变动比例(%)': '{:,.2f}'
        }, na_rep='-'),
        use_container_width=True,
        hide_index=True,
        height=560
    )


def render_macro_tab():
    st.subheader("🌏 宏观经济总览")
    st.caption("展示 GDP、CPI、PPI、M2、Shibor、LPR 的最新读数与历史趋势")

    try:
        min_trade_date, max_trade_date = load_macro_date_bounds()
    except Exception as e:
        st.error(f"获取宏观数据日期范围失败: {e}")
        st.info("请先运行宏观数据同步脚本并确保标准化视图已创建。")
        return

    if not min_trade_date or not max_trade_date:
        st.warning("暂无宏观数据，请先执行同步脚本。")
        return

    min_d = pd.to_datetime(min_trade_date).date()
    max_d = pd.to_datetime(max_trade_date).date()
    default_start = max(min_d, max_d - timedelta(days=365 * 5))

    date_range = st.slider(
        "时间范围",
        min_value=min_d,
        max_value=max_d,
        value=(default_start, max_d),
        format="YYYY-MM-DD",
        key="macro_date_range"
    )

    datasets = {}
    for dataset_name in MACRO_DATASET_META:
        try:
            dataset_df = load_macro_dataset(dataset_name, str(date_range[0]), str(date_range[1]))
            if dataset_df is not None and not dataset_df.empty:
                dataset_df = dataset_df.copy()
                dataset_df["trade_date"] = pd.to_datetime(dataset_df["trade_date"])
            datasets[dataset_name] = dataset_df
        except Exception as e:
            st.warning(f"{MACRO_DATASET_META[dataset_name]['label']} 数据加载失败: {e}")
            datasets[dataset_name] = pd.DataFrame()

    if all(df is None or df.empty for df in datasets.values()):
        st.warning("当前时间范围内暂无宏观数据。")
        return

    st.markdown("### 总览")
    card_cols = st.columns(6)
    for idx, (dataset_name, meta) in enumerate(MACRO_DATASET_META.items()):
        latest_date, latest_value, _, delta = build_macro_metric_snapshot(datasets.get(dataset_name), meta["card_col"])
        with card_cols[idx]:
            st.metric(
                meta["card_label"],
                format_macro_value(latest_value, meta["card_unit"]),
                format_macro_value(delta, meta["card_unit"]) if delta is not None else "-"
            )
            if latest_date is not None:
                st.caption(f"最新日期: {latest_date.strftime('%Y-%m-%d')}")

    tab_overview, tab_growth, tab_liquidity = st.tabs(
        ["📌 总览图表", "📈 增长与通胀", "💧 流动性与利率"]
    )

    with tab_overview:
        overview_left, overview_right = st.columns(2)
        with overview_left:
            gdp_df = datasets.get("cn_gdp", pd.DataFrame())
            if not gdp_df.empty:
                st.plotly_chart(
                    create_macro_line_chart(gdp_df, [("gdp_yoy", "GDP同比")], "GDP同比趋势", "GDP同比(%)"),
                    use_container_width=True
                )
            else:
                st.info("GDP 数据为空")
        with overview_right:
            cpi_df = datasets.get("cn_cpi", pd.DataFrame())
            ppi_df = datasets.get("cn_ppi", pd.DataFrame())
            merged_df = pd.DataFrame()
            if not cpi_df.empty:
                merged_df = cpi_df[["trade_date", "nt_yoy"]].rename(columns={"nt_yoy": "CPI同比"})
            if not ppi_df.empty:
                ppi_view = ppi_df[["trade_date", "ppi_yoy"]].rename(columns={"ppi_yoy": "PPI同比"})
                merged_df = ppi_view if merged_df.empty else merged_df.merge(ppi_view, on="trade_date", how="outer")
            if not merged_df.empty:
                st.plotly_chart(
                    create_macro_line_chart(
                        merged_df,
                        [("CPI同比", "CPI同比"), ("PPI同比", "PPI同比")],
                        "CPI / PPI 同比对比",
                        "同比(%)"
                    ),
                    use_container_width=True
                )
            else:
                st.info("CPI/PPI 数据为空")

    with tab_growth:
        growth_left, growth_right = st.columns(2)
        with growth_left:
            gdp_df = datasets.get("cn_gdp", pd.DataFrame())
            if not gdp_df.empty:
                st.plotly_chart(
                    create_macro_line_chart(
                        gdp_df,
                        [("gdp", "GDP累计值"), ("gdp_yoy", "GDP同比")],
                        "GDP 总量与同比",
                        "数值"
                    ),
                    use_container_width=True
                )
        with growth_right:
            cpi_df = datasets.get("cn_cpi", pd.DataFrame())
            if not cpi_df.empty:
                st.plotly_chart(
                    create_macro_line_chart(
                        cpi_df,
                        [("nt_yoy", "全国同比"), ("nt_mom", "全国环比")],
                        "CPI 同比与环比",
                        "CPI(%)"
                    ),
                    use_container_width=True
                )
        ppi_df = datasets.get("cn_ppi", pd.DataFrame())
        if not ppi_df.empty:
            st.plotly_chart(
                create_macro_line_chart(
                    ppi_df,
                    [("ppi_yoy", "PPI同比"), ("ppi_mom", "PPI环比"), ("ppi_accu", "PPI累计同比")],
                    "PPI 走势",
                    "PPI(%)"
                ),
                use_container_width=True
            )

    with tab_liquidity:
        liquidity_left, liquidity_right = st.columns(2)
        with liquidity_left:
            m2_df = datasets.get("cn_m", pd.DataFrame())
            if not m2_df.empty:
                st.plotly_chart(
                    create_macro_line_chart(
                        m2_df,
                        [("m2", "M2余额"), ("m2_yoy", "M2同比")],
                        "M2 余额与同比",
                        "M2"
                    ),
                    use_container_width=True
                )
        with liquidity_right:
            shibor_df = datasets.get("shibor", pd.DataFrame())
            if not shibor_df.empty:
                st.plotly_chart(
                    create_macro_line_chart(
                        shibor_df,
                        [("rate_1w", "Shibor 1W"), ("rate_1m", "Shibor 1M"), ("rate_3m", "Shibor 3M"), ("rate_1y", "Shibor 1Y")],
                        "Shibor 多期限走势",
                        "利率(%)"
                    ),
                    use_container_width=True
                )
        lpr_df = datasets.get("shibor_lpr", pd.DataFrame())
        if not lpr_df.empty:
            st.plotly_chart(
                create_macro_line_chart(
                    lpr_df,
                    [("lpr_1y", "LPR 1Y"), ("lpr_5y", "LPR 5Y")],
                    "LPR 走势",
                    "利率(%)"
                ),
                use_container_width=True
            )

def render_fund_hot_stocks_tab():
    """渲染公募基金持仓热股 Tab 页"""
    from urllib.parse import quote
    from src.fund_hot_stocks import (
        get_engine as get_fund_hot_engine,
        get_latest_agg_period,
        query_hot_stocks_leaderboard,
        query_stock_fund_holding_detail,
        query_stock_holding_trend,
        query_fund_preference_snapshot,
        search_funds,
    )
    from src.limitup_monitor import (
        get_limitup_latest_date,
        get_limitup_sync_meta,
        query_limitup_emotion_daily,
        query_limitup_sector_relay_daily,
        query_limitup_leader_daily,
        query_limitup_ths_tag_daily,
        query_limitup_ths_reason_daily,
    )

    st.subheader("🏦 公募基金持仓热股")
    st.caption("数据来源：Tushare 公募基金季度持仓 | 季度披露数据，适合中期结构观察")

    try:
        _fh_engine = get_fund_hot_engine()
        latest_period = get_latest_agg_period(_fh_engine)
    except Exception as exc:
        st.error(f"❌ 无法连接公募持仓数据库：{exc}")
        return

    if not latest_period:
        st.warning("⚠️ 暂无公募基金持仓热股数据，请先运行初始化脚本：\n```\n/opt/etf-app/.venv/bin/python update_fund_hot_stocks.py\n```")
        return

    latest_period_label = pd.to_datetime(latest_period).strftime("%Y-%m-%d")
    meta = load_fund_hot_stock_meta()
    period_count = int(meta.get("period_count") or 0)
    row_count = int(meta.get("row_count") or 0)
    latest_updated_at = meta.get("latest_updated_at")
    latest_updated_label = "-"
    if latest_updated_at is not None and not pd.isna(latest_updated_at):
        latest_updated_label = pd.to_datetime(latest_updated_at).strftime("%Y-%m-%d %H:%M")

    freshness_cols = st.columns(3)
    freshness_cols[0].metric("最新报告期", latest_period_label)
    freshness_cols[1].metric("已覆盖报告期", f"{period_count} 个")
    freshness_cols[2].metric("聚合记录数", f"{row_count:,}")
    st.caption(f"最近聚合更新时间：{latest_updated_label}")

    latest_period_dt = pd.to_datetime(latest_period)
    stale_cutoff = pd.Timestamp(datetime.now().date()) - pd.Timedelta(days=160)
    if latest_period_dt < stale_cutoff:
        st.warning("⚠️ 当前公募持仓数据可能偏旧（季度披露数据更新较慢）。如需最新结果，请先同步并重建聚合表。")

    periods = load_fund_hot_stock_periods() or [latest_period_label]
    periods = list(dict.fromkeys(periods))

    fund_type_options = ["全部", "混合型", "股票型", "债券型", "ETF", "QDII", "LOF", "货币型"]
    selected_fund_type = st.selectbox("基金类型筛选", fund_type_options, index=0, key="fh_fund_type_filter")

    sub_top, sub_stock, sub_fund = st.tabs(["🔥 热股榜", "🔎 个股持仓透视", "🏦 基金偏好分析"])

    with sub_top:
        sort_options = {
            "综合热度": "heat_score",
            "持有基金数": "holding_fund_count",
            "持仓总市值": "total_mkv",
            "持有基金数变化": "delta_holding_fund_count",
            "持仓市值变化": "delta_total_mkv",
        }

        col_period, col_sort, col_topn, col_min = st.columns([1.4, 1.6, 1, 1])
        with col_period:
            selected_period = st.selectbox("报告期", periods, index=0, key="fh_top_period")
        with col_sort:
            sort_label = st.selectbox("排序方式", list(sort_options.keys()), index=0, key="fh_top_sort_label")
        with col_topn:
            top_n = st.selectbox("显示数量", [10, 20, 30, 50, 100], index=2, key="fh_top_n")
        with col_min:
            min_holding_funds = st.number_input("最少持有基金数", min_value=1, max_value=100, value=3, step=1, key="fh_min_holding_funds")

        if st.button("查询热股榜", type="primary", key="btn_fh_top_query") or "fh_top_result" not in st.session_state:
            try:
                df_top = query_hot_stocks_leaderboard(
                    period=selected_period.replace("-", ""),
                    top_n=int(top_n),
                    order_by=sort_options[sort_label],
                    min_holding_funds=int(min_holding_funds),
                    fund_type_filter=selected_fund_type,
                    engine=_fh_engine,
                )
                st.session_state["fh_top_result"] = df_top
                st.session_state["fh_top_render_sort"] = sort_label
            except Exception as exc:
                st.error(f"热股榜查询失败：{exc}")
                st.session_state["fh_top_result"] = pd.DataFrame()

        df_top = st.session_state.get("fh_top_result", pd.DataFrame())
        if df_top is not None and not df_top.empty:
            df_top = df_top.copy()
            if "stock_name" not in df_top.columns:
                df_top["stock_name"] = df_top["symbol"]
            else:
                df_top["stock_name"] = df_top["stock_name"].fillna(df_top["symbol"])
            df_top["display_name"] = df_top["stock_name"].astype(str) + "（" + df_top["symbol"].astype(str) + "）"

            selected_sort = st.session_state.get("fh_top_render_sort", sort_label)
            metric_key = sort_options.get(selected_sort, "heat_score")
            plot_df = df_top.head(min(len(df_top), 20)).copy()

            if metric_key in {"total_mkv", "delta_total_mkv"}:
                plot_df["plot_value"] = pd.to_numeric(plot_df[metric_key], errors="coerce").fillna(0) / 1e8
                xaxis_title = "金额（亿元）"
            elif metric_key == "heat_score":
                plot_df["plot_value"] = pd.to_numeric(plot_df[metric_key], errors="coerce").fillna(0) * 100
                xaxis_title = "热度分"
            else:
                plot_df["plot_value"] = pd.to_numeric(plot_df[metric_key], errors="coerce").fillna(0)
                xaxis_title = "数量"

            fig_top = go.Figure(go.Bar(
                x=plot_df["plot_value"],
                y=plot_df["display_name"],
                orientation="h",
                marker=dict(color=plot_df["plot_value"], colorscale="Blues", showscale=False),
                text=plot_df["plot_value"].map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-"),
                textposition="outside",
                hovertemplate="%{y}<br>%{x:,.2f}<extra></extra>",
            ))
            fig_top.update_layout(
                title=dict(text=f"{selected_sort} Top{min(len(plot_df), int(top_n))}", x=0.02, font=dict(size=18, color="#1E293B")),
                xaxis_title=xaxis_title,
                height=max(420, len(plot_df) * 24),
                template="wealthspark_balanced",
                paper_bgcolor="rgba(248, 250, 252, 0.92)",
                plot_bgcolor="rgba(241, 245, 249, 0.58)",
                font=dict(family="Inter, PingFang SC, sans-serif"),
                margin=dict(l=120, r=40, t=60, b=20),
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig_top, use_container_width=True)

            render_nonce = st.session_state.get("fh_top_render_nonce", 0) + 1
            st.session_state["fh_top_render_nonce"] = render_nonce
            df_top["jump_link"] = df_top["symbol"].astype(str).map(
                lambda code: f"?security_query={quote(code)}&security_type=stock&open_tab=security&jump_nonce={render_nonce}_{quote(code)}"
            )

            show_df = pd.DataFrame({
                "跳转": df_top["jump_link"],
                "名称": df_top["stock_name"],
                "代码": df_top["symbol"],
                "持有基金数": pd.to_numeric(df_top["holding_fund_count"], errors="coerce").fillna(0).astype(int),
                "持仓总市值(亿)": pd.to_numeric(df_top["total_mkv"], errors="coerce").fillna(0) / 1e8,
                "环比基金数变化": pd.to_numeric(df_top["delta_holding_fund_count"], errors="coerce").fillna(0).astype(int),
                "环比市值变化(亿)": pd.to_numeric(df_top["delta_total_mkv"], errors="coerce").fillna(0) / 1e8,
                "新进基金数": pd.to_numeric(df_top["new_fund_count"], errors="coerce").fillna(0).astype(int),
                "退出基金数": pd.to_numeric(df_top["exited_fund_count"], errors="coerce").fillna(0).astype(int),
                "平均持仓占比(%)": pd.to_numeric(df_top["avg_stk_mkv_ratio"], errors="coerce"),
                "热度分": pd.to_numeric(df_top["heat_score"], errors="coerce").fillna(0) * 100,
            })

            for col in ["持仓总市值(亿)", "环比市值变化(亿)", "平均持仓占比(%)", "热度分"]:
                show_df[col] = pd.to_numeric(show_df[col], errors="coerce").map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")

            st.info("💡 点击“跳转”列里的“🔎 查询”可直接跳到“个股/指数查询”，自动带入股票代码。")
            st.dataframe(
                show_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "跳转": st.column_config.LinkColumn("跳转", display_text="🔎 查询")
                },
            )

            st.markdown("#### 📊 季度对比 / 新晋抱团股")
            prev_candidates = [p for p in periods if p != selected_period]
            prev_period_label = prev_candidates[0] if prev_candidates else None
            if prev_period_label:
                try:
                    prev_df = query_hot_stocks_leaderboard(
                        period=prev_period_label.replace("-", ""),
                        top_n=int(top_n),
                        order_by=sort_options[sort_label],
                        min_holding_funds=int(min_holding_funds),
                        fund_type_filter=selected_fund_type,
                        engine=_fh_engine,
                    )
                except Exception as exc:
                    logger.error(f"query previous leaderboard failed: {exc}", exc_info=True)
                    prev_df = pd.DataFrame()

                if prev_df is not None and not prev_df.empty:
                    cur_codes = set(df_top["symbol"].astype(str))
                    prev_codes = set(prev_df["symbol"].astype(str))
                    new_entries = df_top[df_top["symbol"].astype(str).isin(cur_codes - prev_codes)].copy()
                    dropped_entries = prev_df[prev_df["symbol"].astype(str).isin(prev_codes - cur_codes)].copy()
                    strongest_up = df_top.sort_values(["delta_holding_fund_count", "delta_total_mkv"], ascending=[False, False]).head(10).copy()

                    compare_metrics = st.columns(3)
                    compare_metrics[0].metric("新晋上榜数", f"{len(new_entries):,}")
                    compare_metrics[1].metric("退出上榜数", f"{len(dropped_entries):,}")
                    compare_metrics[2].metric("对比基准季度", prev_period_label)

                    cmp_a, cmp_b, cmp_c = st.tabs(["🆕 新晋上榜", "📉 退出上榜", "🚀 增幅最强"])

                    with cmp_a:
                        if not new_entries.empty:
                            new_show = new_entries[["stock_name", "symbol", "holding_fund_count", "total_mkv", "heat_score"]].copy()
                            new_show.columns = ["名称", "代码", "持有基金数", "总持仓市值", "热度分"]
                            st.dataframe(new_show, use_container_width=True, hide_index=True)
                        else:
                            st.info("本季度 Top 榜中没有新晋上榜股票。")

                    with cmp_b:
                        if not dropped_entries.empty:
                            drop_show = dropped_entries[["stock_name", "symbol", "holding_fund_count", "total_mkv", "heat_score"]].copy()
                            drop_show.columns = ["名称", "代码", "持有基金数", "总持仓市值", "热度分"]
                            st.dataframe(drop_show, use_container_width=True, hide_index=True)
                        else:
                            st.info("上季度 Top 榜股票本季度仍基本延续，没有明显掉队。")

                    with cmp_c:
                        if not strongest_up.empty:
                            up_show = strongest_up[["stock_name", "symbol", "delta_holding_fund_count", "delta_total_mkv", "holding_fund_count", "total_mkv"]].copy()
                            up_show.columns = ["名称", "代码", "基金数环比+", "市值环比+", "当前持有基金数", "当前总持仓市值"]
                            st.dataframe(up_show, use_container_width=True, hide_index=True)
                        else:
                            st.info("暂无可用的增幅对比数据。")
                else:
                    st.info("上一季度缺少可比榜单数据，暂时无法生成季度对比。")
            else:
                st.info("当前仅有一个报告期，暂时无法做季度对比。")
        else:
            st.info("当前筛选条件下暂无热股榜数据。")

    with sub_stock:
        st.markdown("#### 🔎 个股持仓透视")
        st.caption("输入股票代码、名称或拼音，查看当前季度有哪些基金在持有，以及相较上季度是新进/加仓/减仓。")

        col_period, col_keyword, col_match = st.columns([1.2, 1.6, 2.2])
        with col_period:
            detail_period = st.selectbox("报告期", periods, index=0, key="fh_detail_period")
        with col_keyword:
            stock_keyword = st.text_input(
                "股票代码 / 名称 / 拼音",
                value=st.session_state.get("fh_stock_keyword", ""),
                placeholder="如 603083.SH、剑桥科技、jqkj",
                key="fh_stock_keyword_input",
            ).strip()

        candidate_df = pd.DataFrame()
        option_labels = []
        selected_row = None
        if stock_keyword:
            try:
                candidate_df = load_security_search(stock_keyword, 'stock', limit=30)
            except Exception as exc:
                st.warning(f"匹配股票失败：{exc}")
                candidate_df = pd.DataFrame()

        with col_match:
            if stock_keyword and (candidate_df is None or len(candidate_df) == 0):
                st.warning("未找到匹配股票，请换个代码、名称或拼音再试。")
                st.text_input("匹配结果", value="没有匹配结果", disabled=True, key="fh_stock_match_placeholder")
            elif candidate_df is not None and len(candidate_df) > 0:
                option_labels = [format_security_option(row) for _, row in candidate_df.iterrows()]
                selected_label = st.selectbox("匹配结果", options=option_labels, key="fh_stock_match_option")
                selected_idx = option_labels.index(selected_label)
                selected_row = candidate_df.iloc[selected_idx]
            else:
                st.text_input("匹配结果", value="请输入关键词后自动匹配", disabled=True, key="fh_stock_match_placeholder")

        if st.button("查询持仓透视", type="primary", key="btn_fh_detail_query"):
            st.session_state["fh_detail_error"] = ""
            st.session_state["fh_detail_result"] = pd.DataFrame()
            st.session_state["fh_last_query_period"] = detail_period
            st.session_state["fh_last_query_status"] = ""
            if selected_row is None:
                st.session_state["fh_last_query_code"] = ""
                st.session_state["fh_last_query_name"] = ""
                if stock_keyword:
                    st.session_state["fh_detail_error"] = "没有匹配到股票，暂时无法查询持仓透视。"
                    st.session_state["fh_last_query_status"] = "未匹配到股票"
                else:
                    st.session_state["fh_detail_error"] = "请先输入股票代码/名称，并从匹配结果中选择股票。"
                    st.session_state["fh_last_query_status"] = "缺少查询对象"
            else:
                code = str(selected_row.get("ts_code") or "").strip().upper()
                name = str(selected_row.get("name") or code).strip()
                st.session_state["fh_last_query_code"] = code
                st.session_state["fh_last_query_name"] = name
                try:
                    df_detail = query_stock_fund_holding_detail(
                        symbol=code,
                        period=detail_period.replace("-", ""),
                        top_n=200,
                        fund_type_filter=selected_fund_type,
                        engine=_fh_engine,
                    )
                    st.session_state["fh_detail_result"] = df_detail
                    st.session_state["fh_stock_code"] = code
                    st.session_state["fh_stock_name"] = name
                    st.session_state["fh_stock_keyword"] = stock_keyword
                    if df_detail is None or df_detail.empty:
                        st.session_state["fh_last_query_status"] = "该季度暂无基金持仓明细"
                    else:
                        st.session_state["fh_last_query_status"] = "查询成功"
                except Exception as exc:
                    logger.error(f"query_stock_fund_holding_detail failed: {exc}", exc_info=True)
                    st.session_state["fh_detail_error"] = "持仓透视查询失败，请稍后重试；若持续失败，说明该股票当前报告期数据可能缺失。"
                    st.session_state["fh_detail_result"] = pd.DataFrame()
                    st.session_state["fh_last_query_status"] = "查询异常"

        detail_error = st.session_state.get("fh_detail_error", "")
        last_query_code = st.session_state.get("fh_last_query_code", "")
        last_query_name = st.session_state.get("fh_last_query_name", "")
        last_query_period = st.session_state.get("fh_last_query_period", detail_period)
        last_query_status = st.session_state.get("fh_last_query_status", "")

        if last_query_code or last_query_name or last_query_status:
            query_title = last_query_name or last_query_code or "未选择股票"
            if last_query_code and last_query_name:
                query_title = f"{last_query_name}（{last_query_code}）"
            st.info(f"📌 当前查询对象：{query_title}｜报告期：{last_query_period}｜状态：{last_query_status or '待查询'}")

        if detail_error:
            st.error(detail_error)

        st.info("前十大股东入口已迁移到「个股 / 指数查询」页面。这里仅展示基金持仓透视。")

        df_detail = st.session_state.get("fh_detail_result")
        if df_detail is not None and not df_detail.empty:
            df_detail = df_detail.copy()
            if "fund_name" not in df_detail.columns:
                df_detail["fund_name"] = df_detail["fund_code"]
            else:
                df_detail["fund_name"] = df_detail["fund_name"].fillna(df_detail["fund_code"])
            df_detail["holding_change_flag"] = df_detail["holding_change_flag"].replace({
                "new": "新进",
                "increase": "加仓",
                "decrease": "减仓",
                "stable": "持平",
            })

            stock_title = st.session_state.get("fh_stock_name") or st.session_state.get("fh_stock_code", "")
            if st.session_state.get("fh_stock_code") and st.session_state.get("fh_stock_name"):
                stock_title = f"{st.session_state.get('fh_stock_name')}（{st.session_state.get('fh_stock_code')}）"

            metric_cols = st.columns(4)
            metric_cols[0].metric("持有基金数", f"{len(df_detail):,}")
            metric_cols[1].metric("持仓总市值", f"{pd.to_numeric(df_detail['mkv'], errors='coerce').fillna(0).sum() / 1e8:,.2f} 亿")
            metric_cols[2].metric("新进基金", f"{int((df_detail['holding_change_flag'] == '新进').sum())}")
            metric_cols[3].metric("加仓基金", f"{int((df_detail['holding_change_flag'] == '加仓').sum())}")

            plot_df = df_detail.head(min(len(df_detail), 20)).copy()
            plot_df["plot_value"] = pd.to_numeric(plot_df["mkv"], errors="coerce").fillna(0) / 1e8
            plot_df["display_name"] = plot_df["fund_name"].astype(str)

            fig_detail = go.Figure(go.Bar(
                x=plot_df["plot_value"],
                y=plot_df["display_name"],
                orientation="h",
                marker=dict(color=plot_df["plot_value"], colorscale="Viridis", showscale=False),
                text=plot_df["plot_value"].map(lambda v: f"{v:,.2f}亿" if pd.notna(v) else "-"),
                textposition="outside",
                hovertemplate="%{y}<br>持仓市值：%{x:,.2f} 亿<extra></extra>",
            ))
            fig_detail.update_layout(
                title=dict(text=f"{stock_title} 持仓基金 Top20", x=0.02, font=dict(size=18, color="#1E293B")),
                xaxis_title="持仓市值（亿元）",
                height=max(420, len(plot_df) * 24),
                template="wealthspark_balanced",
                paper_bgcolor="rgba(248, 250, 252, 0.92)",
                plot_bgcolor="rgba(241, 245, 249, 0.58)",
                font=dict(family="Inter, PingFang SC, sans-serif"),
                margin=dict(l=120, r=40, t=60, b=20),
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig_detail, use_container_width=True)

            management_series = df_detail["management"] if "management" in df_detail.columns else pd.Series([""] * len(df_detail))
            show_df = pd.DataFrame({
                "基金代码": df_detail["fund_code"],
                "基金名称": df_detail["fund_name"],
                "管理人": management_series,
                "持仓市值(亿)": pd.to_numeric(df_detail["mkv"], errors="coerce").fillna(0) / 1e8,
                "持仓数量": pd.to_numeric(df_detail["amount"], errors="coerce") if "amount" in df_detail.columns else pd.Series([None] * len(df_detail)),
                "占基金股票市值比(%)": pd.to_numeric(df_detail["stk_mkv_ratio"], errors="coerce") if "stk_mkv_ratio" in df_detail.columns else pd.Series([None] * len(df_detail)),
                "上季持仓市值(亿)": pd.to_numeric(df_detail["prev_mkv"], errors="coerce").fillna(0) / 1e8 if "prev_mkv" in df_detail.columns else pd.Series([0] * len(df_detail)),
                "市值变化(亿)": pd.to_numeric(df_detail["delta_mkv"], errors="coerce").fillna(0) / 1e8 if "delta_mkv" in df_detail.columns else pd.Series([0] * len(df_detail)),
                "变动类型": df_detail["holding_change_flag"],
            })

            for col in ["持仓市值(亿)", "占基金股票市值比(%)", "上季持仓市值(亿)", "市值变化(亿)"]:
                show_df[col] = pd.to_numeric(show_df[col], errors="coerce").map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
            show_df["持仓数量"] = pd.to_numeric(show_df["持仓数量"], errors="coerce").map(lambda v: f"{v:,.0f}" if pd.notna(v) else "-")

            st.dataframe(show_df, use_container_width=True, hide_index=True, height=520)

            st.markdown("#### 🏢 管理人维度")
            mgmt_df = df_detail.copy()
            mgmt_df["management"] = management_series.fillna("未知管理人").replace("", "未知管理人")
            mgmt_df["mkv_yi"] = pd.to_numeric(mgmt_df["mkv"], errors="coerce").fillna(0) / 1e8
            mgmt_df["delta_mkv_yi"] = pd.to_numeric(mgmt_df["delta_mkv"], errors="coerce").fillna(0) / 1e8 if "delta_mkv" in mgmt_df.columns else 0

            mgmt_group = mgmt_df.groupby("management", dropna=False).agg(
                持有基金数=("fund_code", "nunique"),
                总持仓市值亿=("mkv_yi", "sum"),
                市值变化亿=("delta_mkv_yi", "sum"),
            ).reset_index().sort_values(["总持仓市值亿", "持有基金数"], ascending=[False, False])

            mgmt_metrics = st.columns(3)
            mgmt_metrics[0].metric("参与管理人数", f"{len(mgmt_group):,}")
            mgmt_metrics[1].metric("抱团最强管理人", mgmt_group.iloc[0]["management"] if not mgmt_group.empty else "-")
            mgmt_metrics[2].metric("Top管理人持仓市值", f"{mgmt_group.iloc[0]['总持仓市值亿']:,.2f} 亿" if not mgmt_group.empty else "-")

            if not mgmt_group.empty:
                mgmt_plot = mgmt_group.head(15).copy()
                fig_mgmt = go.Figure(go.Bar(
                    x=mgmt_plot["总持仓市值亿"],
                    y=mgmt_plot["management"],
                    orientation="h",
                    marker=dict(color=mgmt_plot["总持仓市值亿"], colorscale="Tealgrn", showscale=False),
                    text=mgmt_plot["总持仓市值亿"].map(lambda v: f"{v:,.2f}亿" if pd.notna(v) else "-"),
                    textposition="outside",
                    hovertemplate="%{y}<br>总持仓市值：%{x:,.2f} 亿<extra></extra>",
                ))
                fig_mgmt.update_layout(
                    title=dict(text="管理人抱团分布 Top15", x=0.02, font=dict(size=17, color="#1E293B")),
                    xaxis_title="总持仓市值（亿元）",
                    height=max(360, len(mgmt_plot) * 26),
                    template="wealthspark_balanced",
                    paper_bgcolor="rgba(248, 250, 252, 0.92)",
                    plot_bgcolor="rgba(241, 245, 249, 0.58)",
                    font=dict(family="Inter, PingFang SC, sans-serif"),
                    margin=dict(l=120, r=40, t=55, b=20),
                    yaxis=dict(autorange="reversed"),
                )
                st.plotly_chart(fig_mgmt, use_container_width=True)

                mgmt_show = mgmt_group.copy()
                mgmt_show["总持仓市值(亿)"] = pd.to_numeric(mgmt_show["总持仓市值亿"], errors="coerce").map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
                mgmt_show["市值变化(亿)"] = pd.to_numeric(mgmt_show["市值变化亿"], errors="coerce").map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
                mgmt_show = mgmt_show.rename(columns={"management": "管理人"})[["管理人", "持有基金数", "总持仓市值(亿)", "市值变化(亿)"]]
                st.dataframe(mgmt_show, use_container_width=True, hide_index=True)
            else:
                st.info("当前结果暂无可用的管理人维度数据。")

            st.markdown("#### 📈 个股季度趋势")
            trend_periods = st.selectbox("趋势季度数", [4, 6, 8, 12], index=2, key="fh_trend_periods")
            try:
                trend_df = query_stock_holding_trend(
                    symbol=st.session_state.get("fh_stock_code", ""),
                    periods=int(trend_periods),
                    fund_type_filter=selected_fund_type,
                    engine=_fh_engine,
                )
            except Exception as exc:
                logger.error(f"query_stock_holding_trend failed: {exc}", exc_info=True)
                trend_df = pd.DataFrame()

            if trend_df is not None and not trend_df.empty:
                trend_df = trend_df.copy()
                trend_df["end_date"] = pd.to_datetime(trend_df["end_date"])
                trend_df["end_date_label"] = trend_df["end_date"].dt.strftime("%Y-%m-%d")
                trend_df["total_mkv_yi"] = pd.to_numeric(trend_df["total_mkv"], errors="coerce").fillna(0) / 1e8
                trend_df["holding_fund_count"] = pd.to_numeric(trend_df["holding_fund_count"], errors="coerce").fillna(0)

                fig_trend = make_subplots(specs=[[{"secondary_y": True}]])
                fig_trend.add_trace(
                    go.Bar(
                        x=trend_df["end_date_label"],
                        y=trend_df["holding_fund_count"],
                        name="持有基金数",
                        marker_color="#3B82F6",
                        opacity=0.75,
                    ),
                    secondary_y=False,
                )
                fig_trend.add_trace(
                    go.Scatter(
                        x=trend_df["end_date_label"],
                        y=trend_df["total_mkv_yi"],
                        name="总持仓市值(亿)",
                        mode="lines+markers",
                        line=dict(color="#F59E0B", width=3),
                        marker=dict(size=7),
                    ),
                    secondary_y=True,
                )
                fig_trend.update_layout(
                    title=dict(text="近季度持仓趋势", x=0.02, font=dict(size=17, color="#1E293B")),
                    template="wealthspark_balanced",
                    paper_bgcolor="rgba(248, 250, 252, 0.92)",
                    plot_bgcolor="rgba(241, 245, 249, 0.58)",
                    font=dict(family="Inter, PingFang SC, sans-serif"),
                    height=420,
                    margin=dict(l=50, r=50, t=55, b=30),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                )
                fig_trend.update_yaxes(title_text="持有基金数", secondary_y=False)
                fig_trend.update_yaxes(title_text="总持仓市值（亿元）", secondary_y=True)
                st.plotly_chart(fig_trend, use_container_width=True)

                trend_show = trend_df[[
                    "end_date_label",
                    "holding_fund_count",
                    "total_mkv_yi",
                    "delta_holding_fund_count",
                    "delta_total_mkv",
                    "new_fund_count",
                    "exited_fund_count",
                    "heat_score",
                ]].copy()
                trend_show.columns = [
                    "报告期",
                    "持有基金数",
                    "总持仓市值(亿)",
                    "基金数环比变化",
                    "市值环比变化",
                    "新进基金数",
                    "退出基金数",
                    "热度分",
                ]
                trend_show["市值环比变化"] = pd.to_numeric(trend_show["市值环比变化"], errors="coerce").fillna(0) / 1e8
                trend_show["热度分"] = pd.to_numeric(trend_show["热度分"], errors="coerce").fillna(0) * 100
                for col in ["总持仓市值(亿)", "市值环比变化", "热度分"]:
                    trend_show[col] = pd.to_numeric(trend_show[col], errors="coerce").map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
                st.dataframe(trend_show, use_container_width=True, hide_index=True)
            else:
                st.info("该股票暂无可用的季度趋势数据。")

        elif df_detail is not None and df_detail.empty and not detail_error and st.session_state.get("fh_last_query_code"):
            st.info("该股票在所选报告期暂无基金持仓明细。可能原因：当前季度未被基金持有，或该股票尚未纳入本期聚合数据。")

    with sub_fund:
        st.markdown("#### 🏦 基金偏好分析")
        st.caption("输入基金代码 / 基金名称，支持模糊匹配后选择基金，再查看该基金在当前报告期的偏好持仓。")

        fund_col1, fund_col2, fund_col3 = st.columns([1.5, 1.6, 1])
        with fund_col1:
            fund_keyword = st.text_input(
                "基金代码 / 基金名称",
                value=st.session_state.get("fh_fund_keyword", st.session_state.get("fh_fund_code", "")),
                placeholder="如 000001.OF、招商中证白酒、白酒",
                key="fh_fund_keyword_input",
            ).strip()
        with fund_col2:
            fund_candidates = pd.DataFrame()
            fund_option_labels = []
            fund_selected_row = None
            if fund_keyword:
                try:
                    fund_candidates = search_funds(fund_keyword, limit=30, engine=_fh_engine)
                except Exception as exc:
                    st.warning(f"匹配基金失败：{exc}")
                    fund_candidates = pd.DataFrame()

            if fund_keyword and (fund_candidates is None or len(fund_candidates) == 0):
                st.warning("未找到匹配基金，请换个代码、名称或关键词再试。")
                st.text_input("匹配基金", value="没有匹配结果", disabled=True, key="fh_fund_match_placeholder")
            elif fund_candidates is not None and len(fund_candidates) > 0:
                fund_option_labels = [
                    f"{str(row.get('name') or row.get('fund_code') or '')}（{str(row.get('fund_code') or '')}｜{str(row.get('management') or '未知管理人')}｜{str(row.get('fund_type') or '未知类型')}）"
                    for _, row in fund_candidates.iterrows()
                ]
                fund_selected_label = st.selectbox("匹配基金", options=fund_option_labels, key="fh_fund_match_option")
                fund_selected_idx = fund_option_labels.index(fund_selected_label)
                fund_selected_row = fund_candidates.iloc[fund_selected_idx]
            else:
                st.text_input("匹配基金", value="请输入关键词后自动匹配", disabled=True, key="fh_fund_match_placeholder")
        with fund_col3:
            fund_period = st.selectbox("报告期", periods, index=0, key="fh_fund_period")

        if st.button("查询基金偏好", type="primary", key="btn_fh_fund_query"):
            st.session_state["fh_fund_error"] = ""
            st.session_state["fh_fund_result"] = pd.DataFrame()
            st.session_state["fh_fund_keyword"] = fund_keyword
            if fund_selected_row is None:
                st.session_state["fh_fund_code"] = ""
                if fund_keyword:
                    st.session_state["fh_fund_error"] = "没有匹配到基金，请先从候选结果中选择基金。"
                else:
                    st.session_state["fh_fund_error"] = "请先输入基金代码/名称，并从匹配结果中选择基金。"
            else:
                fund_code = str(fund_selected_row.get("fund_code") or "").strip().upper()
                st.session_state["fh_fund_code"] = fund_code
                st.session_state["fh_fund_name"] = str(fund_selected_row.get("name") or fund_code).strip()
                try:
                    fund_df = query_fund_preference_snapshot(
                        fund_code=fund_code,
                        period=fund_period.replace("-", ""),
                        top_n=30,
                        engine=_fh_engine,
                    )
                    st.session_state["fh_fund_result"] = fund_df
                except Exception as exc:
                    logger.error(f"query_fund_preference_snapshot failed: {exc}", exc_info=True)
                    st.session_state["fh_fund_error"] = "基金偏好分析查询失败，请检查基金选择结果或稍后重试。"

        fund_error = st.session_state.get("fh_fund_error", "")
        if fund_error:
            st.error(fund_error)

        fund_df = st.session_state.get("fh_fund_result")
        if fund_df is not None and not fund_df.empty:
            fund_df = fund_df.copy()
            fund_df["mkv_yi"] = pd.to_numeric(fund_df["mkv"], errors="coerce").fillna(0) / 1e8
            fund_df["delta_mkv_yi"] = pd.to_numeric(fund_df["delta_mkv"], errors="coerce").fillna(0) / 1e8
            fund_df["holding_change_flag"] = fund_df["holding_change_flag"].replace({
                "new": "新进",
                "increase": "加仓",
                "decrease": "减仓",
                "stable": "持平",
            })

            fund_name = str(fund_df.iloc[0].get("fund_name") or st.session_state.get("fh_fund_name") or st.session_state.get("fh_fund_code", ""))
            management = str(fund_df.iloc[0].get("management") or "-")
            st.info(f"📌 当前基金：{fund_name}｜管理人：{management}｜报告期：{fund_period}")

            fund_metrics = st.columns(4)
            fund_metrics[0].metric("偏好持仓数", f"{len(fund_df):,}")
            fund_metrics[1].metric("持仓总市值", f"{fund_df['mkv_yi'].sum():,.2f} 亿")
            fund_metrics[2].metric("新进持仓", f"{int((fund_df['holding_change_flag'] == '新进').sum())}")
            fund_metrics[3].metric("加仓持仓", f"{int((fund_df['holding_change_flag'] == '加仓').sum())}")

            pref_plot = fund_df.head(15).copy()
            fig_pref = go.Figure(go.Bar(
                x=pref_plot["mkv_yi"],
                y=pref_plot["stock_name"],
                orientation="h",
                marker=dict(color=pref_plot["mkv_yi"], colorscale="Bluered", showscale=False),
                text=pref_plot["mkv_yi"].map(lambda v: f"{v:,.2f}亿" if pd.notna(v) else "-"),
                textposition="outside",
                hovertemplate="%{y}<br>持仓市值：%{x:,.2f} 亿<extra></extra>",
            ))
            fig_pref.update_layout(
                title=dict(text="基金偏好持仓 Top15", x=0.02, font=dict(size=17, color="#1E293B")),
                xaxis_title="持仓市值（亿元）",
                height=max(380, len(pref_plot) * 26),
                template="wealthspark_balanced",
                paper_bgcolor="rgba(248, 250, 252, 0.92)",
                plot_bgcolor="rgba(241, 245, 249, 0.58)",
                font=dict(family="Inter, PingFang SC, sans-serif"),
                margin=dict(l=120, r=40, t=55, b=20),
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig_pref, use_container_width=True)

            pref_show = fund_df[["stock_name", "symbol", "mkv_yi", "delta_mkv_yi", "stk_mkv_ratio", "holding_change_flag"]].copy()
            pref_show.columns = ["股票", "代码", "持仓市值(亿)", "市值变化(亿)", "占基金股票市值比(%)", "变动类型"]
            for col in ["持仓市值(亿)", "市值变化(亿)", "占基金股票市值比(%)"]:
                pref_show[col] = pd.to_numeric(pref_show[col], errors="coerce").map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
            st.dataframe(pref_show, use_container_width=True, hide_index=True)
        elif fund_df is not None and fund_df.empty and not fund_error and st.session_state.get("fh_fund_code"):
            st.info("该基金在所选报告期暂无可用偏好持仓数据。")


def render_moneyflow_tab():
    """渲染资金流向 Tab 页"""
    from src.moneyflow_fetcher import (
        query_moneyflow_daily_top,
        query_moneyflow_stock_history,
        query_moneyflow_stock_history_ths,
        query_moneyflow_stock_history_dc,
        backfill_moneyflow_stock_sources,
        query_moneyflow_consecutive_inflow,
        query_moneyflow_hsgt_history,
        query_moneyflow_ind_ths_daily,
        query_moneyflow_dc_ind_daily,
        query_moneyflow_ind_ths_range,
        query_moneyflow_dc_ind_range,
        get_moneyflow_latest_date,
        get_moneyflow_sector_min_date,
        get_engine,
        MONEYFLOW_TABLES,
        get_max_trade_date,
    )

    st.subheader("💹 资金流向分析")
    st.caption("数据来源：Tushare | 2025-01-01 起（个股THS/DC按需回补） | 包含个股主力、行业板块（THS+DC）、北向资金")

    # 尝试连接数据库
    try:
        _mf_engine = get_engine()
        latest_date = get_moneyflow_latest_date(_mf_engine)
    except Exception as exc:
        st.error(f"❌ 无法连接数据库：{exc}")
        return

    if not latest_date:
        st.warning("⚠️ 暂无资金流向数据，请先运行初始化脚本：\n```\npython -m src.moneyflow_fetcher --full\n```")
        return

    latest_dt = pd.to_datetime(latest_date, format="%Y%m%d").date()
    sector_min_raw = get_moneyflow_sector_min_date(_mf_engine)
    sector_min_dt = pd.to_datetime(sector_min_raw, format="%Y%m%d").date() if sector_min_raw else latest_dt
    st.info(f"📅 数据最新日期：**{latest_dt.strftime('%Y-%m-%d')}**")

    # ---- 子标签页 ----
    sub_top, sub_stock, sub_screen, sub_sector, sub_hsgt = st.tabs([
        "🏆 主力净流入排行",
        "📊 个股资金走势",
        "🔍 连续净流入选股",
        "🏭 行业板块流向",
        "🌐 北向资金",
    ])

    # ============================================================
    # 子标签1：每日 Top20 主力净流入排行
    # ============================================================
    with sub_top:
        st.markdown("#### 🏆 当日主力净流入 Top 个股")

        col_date, col_n = st.columns([2, 1])
        with col_date:
            query_date = st.date_input(
                "查询日期",
                value=latest_dt,
                key="mf_top_date"
            )
        with col_n:
            top_n = st.selectbox("显示数量", [10, 20, 30, 50], index=1, key="mf_top_n")

        if st.button("查询排行", type="primary", key="btn_mf_top"):
            with st.spinner("查询中..."):
                try:
                    df_top = query_moneyflow_daily_top(
                        str(query_date).replace("-", ""),
                        top_n=top_n,
                        engine=_mf_engine
                    )
                    st.session_state["mf_top_result"] = df_top
                except Exception as e:
                    st.error(f"查询失败：{e}")
                    st.session_state["mf_top_result"] = pd.DataFrame()
        else:
            # 首次进入自动查最新日期
            if "mf_top_result" not in st.session_state:
                try:
                    df_top = query_moneyflow_daily_top(latest_date, top_n=top_n, engine=_mf_engine)
                    st.session_state["mf_top_result"] = df_top
                except Exception:
                    st.session_state["mf_top_result"] = pd.DataFrame()

        df_top = st.session_state.get("mf_top_result", pd.DataFrame())

        if df_top is not None and not df_top.empty:
            # 计算派生指标
            df_disp = df_top.copy()
            df_disp["超大单净额(万)"] = (
                df_disp.get("buy_elg_amount", 0).fillna(0)
                - df_disp.get("sell_elg_amount", 0).fillna(0)
            )
            df_disp["大单净额(万)"] = (
                df_disp.get("buy_lg_amount", 0).fillna(0)
                - df_disp.get("sell_lg_amount", 0).fillna(0)
            )
            df_disp["小单净额(万)"] = (
                df_disp.get("buy_sm_amount", 0).fillna(0)
                - df_disp.get("sell_sm_amount", 0).fillna(0)
            )

            # 主力净流入条形图（显示名称，便于识别）
            df_disp["name"] = df_disp.get("name", df_disp.get("ts_code", "")).fillna(df_disp.get("ts_code", ""))
            df_disp["display_name"] = df_disp["name"].astype(str) + "（" + df_disp["ts_code"].astype(str) + "）"

            fig_bar = go.Figure(go.Bar(
                x=df_disp["net_mf_amount"].astype(float),
                y=df_disp["display_name"],
                orientation="h",
                marker=dict(
                    color=df_disp["net_mf_amount"].astype(float),
                    colorscale=[[0, "#10B981"], [0.5, "#F59E0B"], [1, "#EF4444"]],
                    showscale=False,
                ),
                text=df_disp["net_mf_amount"].apply(lambda v: f"{float(v):,.0f}万"),
                textposition="outside",
                hovertemplate="%{y}<br>主力净流入: %{x:,.0f} 万元<extra></extra>",
            ))
            fig_bar.update_layout(
                title=dict(text="主力净流入（万元）", x=0.02, font=dict(size=18, color="#1E293B")),
                xaxis_title="净流入额（万元）",
                height=max(400, top_n * 22),
                template="wealthspark_balanced",
                paper_bgcolor="rgba(248, 250, 252, 0.92)",
                plot_bgcolor="rgba(241, 245, 249, 0.58)",
                font=dict(family="Inter, PingFang SC, sans-serif"),
                margin=dict(l=100, r=60, t=60, b=20),
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig_bar, use_container_width=True)

            # 详细数据表格（名称可点击跳转到“个股/指数查询”）
            from urllib.parse import quote

            render_nonce = st.session_state.get('mf_top_render_nonce', 0) + 1
            st.session_state['mf_top_render_nonce'] = render_nonce

            df_disp["jump_link"] = df_disp["ts_code"].astype(str).map(
                lambda code: f"?security_query={quote(code)}&security_type=stock&open_tab=security&jump_nonce={render_nonce}_{quote(code)}"
            )
            df_disp["name_link"] = df_disp["name"].astype(str)

            display_cols = {
                "name_link": "名称",
                "ts_code": "代码",
                "net_mf_amount": "主力净流入(万)",
                "超大单净额(万)": "超大单净额(万)",
                "大单净额(万)": "大单净额(万)",
                "小单净额(万)": "小散净额(万)",
            }
            show_df = df_disp[[c for c in display_cols if c in df_disp.columns]].rename(columns=display_cols)
            show_df.insert(0, "跳转", df_disp["jump_link"])
            for col in show_df.columns:
                if col not in {"代码", "名称", "跳转"}:
                    show_df[col] = pd.to_numeric(show_df[col], errors="coerce").map(
                        lambda v: f"{v:,.0f}" if pd.notna(v) else "-"
                    )

            st.info("💡 点击“名称”列最左侧的“🔎 查询”即可跳转到“个股/指数查询”，并自动带入该股票代码。")
            st.dataframe(
                show_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "跳转": st.column_config.LinkColumn(
                        "跳转",
                        help='点击后跳转到个股/指数查询',
                        display_text='🔎 查询'
                    )
                }
            )
        else:
            st.info("该日期暂无数据（可能非交易日，或数据尚未入库）")

    # ============================================================
    # 子标签2：个股历史资金走势
    # ============================================================
    with sub_stock:
        st.markdown("#### 📊 个股历史资金流向趋势")
        st.caption("支持输入股票代码、名称或拼音缩写，自动模糊匹配后查询资金走势")

        col_code, col_match, col_range = st.columns([1.6, 2.4, 1.6])
        with col_code:
            stock_keyword = st.text_input(
                "股票代码 / 名称 / 拼音",
                value=st.session_state.get("mf_stock_keyword", st.session_state.get("mf_stock_code", "")),
                placeholder="如 000001.SZ、平安银行、payh",
                key="mf_stock_code_input"
            ).strip()

        candidate_df = pd.DataFrame()
        option_labels = []
        selected_row = None

        if stock_keyword:
            try:
                candidate_df = load_security_search(stock_keyword, 'stock', limit=30)
            except Exception as e:
                st.warning(f"匹配股票失败：{e}")
                candidate_df = pd.DataFrame()

        with col_match:
            if candidate_df is not None and len(candidate_df) > 0:
                option_labels = [format_security_option(row) for _, row in candidate_df.iterrows()]
                default_index = 0
                existing_code = st.session_state.get("mf_stock_code", "")
                if existing_code:
                    for idx, (_, row) in enumerate(candidate_df.iterrows()):
                        if str(row.get('ts_code', '')).upper() == str(existing_code).upper():
                            default_index = idx
                            break
                selected_label = st.selectbox(
                    "匹配结果",
                    options=option_labels,
                    index=default_index,
                    key="mf_stock_match_option"
                )
                selected_idx = option_labels.index(selected_label)
                selected_row = candidate_df.iloc[selected_idx]
            else:
                st.text_input("匹配结果", value="请输入关键词后自动匹配", disabled=True, key="mf_stock_match_placeholder")

        with col_range:
            date_range_opts = {"近1月": 30, "近3月": 90, "近半年": 180, "全部": 0}
            range_choice = st.selectbox("时间范围", list(date_range_opts.keys()), key="mf_stock_range")

        if st.button("查询个股走势", type="primary", key="btn_mf_stock"):
            if selected_row is None:
                st.warning("请先输入股票代码/名称，并从匹配结果中选择股票")
            else:
                code = str(selected_row['ts_code']).strip().upper()
                name = str(selected_row.get('name') or code).strip()
                days = date_range_opts[range_choice]
                start_d = None
                if days > 0:
                    start_d = (pd.Timestamp.today() - pd.Timedelta(days=days)).strftime("%Y%m%d")

                with st.spinner(f"正在拉取 {name}（{code}）资金流向..."):
                    try:
                        # 用户要求：THS/DC 从 2025 年开始拉取并分别展示
                        base_start = "20250101"
                        if start_d:
                            base_start = max(base_start, str(start_d).replace("-", ""))

                        backfill_stats = backfill_moneyflow_stock_sources(
                            ts_code=code,
                            start_date=base_start,
                            end_date=None,
                            engine=_mf_engine,
                        )

                        df_hist = query_moneyflow_stock_history(code, start_date=base_start, engine=_mf_engine)
                        df_hist_ths = query_moneyflow_stock_history_ths(code, start_date=base_start, engine=_mf_engine)
                        df_hist_dc = query_moneyflow_stock_history_dc(code, start_date=base_start, engine=_mf_engine)

                        st.session_state["mf_stock_result"] = df_hist
                        st.session_state["mf_stock_result_ths"] = df_hist_ths
                        st.session_state["mf_stock_result_dc"] = df_hist_dc
                        st.session_state["mf_stock_code"] = code
                        st.session_state["mf_stock_name"] = name
                        st.session_state["mf_stock_keyword"] = stock_keyword
                        st.session_state["mf_stock_backfill_stats"] = backfill_stats
                    except Exception as e:
                        st.error(f"查询失败：{e}")
                        st.session_state["mf_stock_result"] = pd.DataFrame()
                        st.session_state["mf_stock_result_ths"] = pd.DataFrame()
                        st.session_state["mf_stock_result_dc"] = pd.DataFrame()

        df_hist = st.session_state.get("mf_stock_result")
        df_hist_ths = st.session_state.get("mf_stock_result_ths")
        df_hist_dc = st.session_state.get("mf_stock_result_dc")

        backfill_stats = st.session_state.get("mf_stock_backfill_stats", {})
        if backfill_stats:
            st.caption(
                f"THS新增/更新：{int(backfill_stats.get('moneyflow_ths', 0))} 条 ｜ "
                f"DC新增/更新：{int(backfill_stats.get('moneyflow_dc', 0))} 条"
            )

        if df_hist is not None and not df_hist.empty:
            df_hist = df_hist.copy()
            df_hist["trade_date"] = pd.to_datetime(df_hist["trade_date"])
            df_hist = df_hist.sort_values("trade_date")

            # 主力净流入趋势（面积图）
            fig_hist = go.Figure()
            net = df_hist["net_mf_amount"].astype(float)
            colors = ["#EF4444" if v >= 0 else "#10B981" for v in net]

            fig_hist.add_trace(go.Bar(
                x=df_hist["trade_date"],
                y=df_hist["net_mf_amount"].astype(float),
                name="主力净流入",
                marker_color=colors,
                hovertemplate="%{x|%Y-%m-%d}<br>主力净流入: %{y:,.0f} 万元<extra></extra>",
            ))

            # 叠加超大单
            if "buy_elg_amount" in df_hist.columns and "sell_elg_amount" in df_hist.columns:
                elg_net = df_hist["buy_elg_amount"].astype(float) - df_hist["sell_elg_amount"].astype(float)
                fig_hist.add_trace(go.Scatter(
                    x=df_hist["trade_date"],
                    y=elg_net,
                    mode="lines",
                    name="超大单净额",
                    line=dict(color="#8E54E9", width=2),
                    hovertemplate="%{x|%Y-%m-%d}<br>超大单净额: %{y:,.0f} 万元<extra></extra>",
                ))

            selected_stock_title = st.session_state.get('mf_stock_name') or st.session_state.get('mf_stock_code', '')
            if st.session_state.get('mf_stock_code') and st.session_state.get('mf_stock_name'):
                selected_stock_title = f"{st.session_state.get('mf_stock_name')}（{st.session_state.get('mf_stock_code')}）"

            fig_hist.update_layout(
                title=dict(
                    text=f"{selected_stock_title} 主力资金流入趋势",
                    x=0.02, font=dict(size=18, color="#1E293B")
                ),
                xaxis_title="日期",
                yaxis_title="净流入额（万元）",
                hovermode="x unified",
                height=420,
                template="wealthspark_balanced",
                paper_bgcolor="rgba(248, 250, 252, 0.92)",
                plot_bgcolor="rgba(241, 245, 249, 0.58)",
                font=dict(family="Inter, PingFang SC, sans-serif"),
                margin=dict(l=20, r=20, t=60, b=20),
                legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5),
            )
            fig_hist.update_xaxes(showgrid=True, gridcolor="rgba(226,232,240,0.5)")
            fig_hist.update_yaxes(showgrid=True, gridcolor="rgba(226,232,240,0.5)", zeroline=True, zerolinecolor="#CBD5E1", zerolinewidth=1.5)
            st.plotly_chart(fig_hist, use_container_width=True)

            # 买卖力量对比（最近20个交易日）
            recent = df_hist.tail(20).copy()
            if len(recent) > 0:
                fig_force = go.Figure()
                fig_force.add_trace(go.Bar(
                    name="超大单买入",
                    x=recent["trade_date"], y=recent.get("buy_elg_amount", pd.Series(dtype=float)).astype(float),
                    marker_color="#EF4444", opacity=0.85,
                ))
                fig_force.add_trace(go.Bar(
                    name="超大单卖出",
                    x=recent["trade_date"], y=-recent.get("sell_elg_amount", pd.Series(dtype=float)).astype(float),
                    marker_color="#10B981", opacity=0.85,
                ))
                fig_force.add_trace(go.Bar(
                    name="大单买入",
                    x=recent["trade_date"], y=recent.get("buy_lg_amount", pd.Series(dtype=float)).astype(float),
                    marker_color="#F97316", opacity=0.7,
                ))
                fig_force.add_trace(go.Bar(
                    name="大单卖出",
                    x=recent["trade_date"], y=-recent.get("sell_lg_amount", pd.Series(dtype=float)).astype(float),
                    marker_color="#34D399", opacity=0.7,
                ))
                fig_force.update_layout(
                    barmode="relative",
                    title=dict(text="近20日买卖力量博弈（万元）", x=0.02, font=dict(size=16, color="#1E293B")),
                    height=360,
                    template="wealthspark_balanced",
                    paper_bgcolor="rgba(248, 250, 252, 0.92)",
                    plot_bgcolor="rgba(241, 245, 249, 0.58)",
                    font=dict(family="Inter, PingFang SC, sans-serif"),
                    margin=dict(l=20, r=20, t=60, b=20),
                    hovermode="x unified",
                    legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5),
                )
                fig_force.update_xaxes(showgrid=True, gridcolor="rgba(226,232,240,0.5)")
                fig_force.update_yaxes(showgrid=True, gridcolor="rgba(226,232,240,0.5)", zeroline=True, zerolinecolor="#CBD5E1", zerolinewidth=1.5)
                st.plotly_chart(fig_force, use_container_width=True)

            # ===== 新增：THS 与 DC 分别展示 =====
            st.markdown("##### 🧭 THS / DC 个股资金流向（分口径展示）")
            src_tab_ths, src_tab_dc = st.tabs(["THS（moneyflow_ths）", "DC（moneyflow_dc）"])

            with src_tab_ths:
                if df_hist_ths is not None and not df_hist_ths.empty:
                    _ths = df_hist_ths.copy()
                    _ths["trade_date"] = pd.to_datetime(_ths["trade_date"])
                    _ths = _ths.sort_values("trade_date")

                    fig_ths = go.Figure()
                    fig_ths.add_trace(go.Bar(
                        x=_ths["trade_date"],
                        y=_ths["net_amount"].astype(float),
                        name="THS主力净流入",
                        marker_color=["#EF4444" if v >= 0 else "#10B981" for v in _ths["net_amount"].astype(float)],
                        hovertemplate="%{x|%Y-%m-%d}<br>THS净流入: %{y:,.0f} 万元<extra></extra>",
                    ))
                    if "net_d5_amount" in _ths.columns:
                        fig_ths.add_trace(go.Scatter(
                            x=_ths["trade_date"],
                            y=_ths["net_d5_amount"].astype(float),
                            mode="lines",
                            name="THS 5日主力净额",
                            line=dict(color="#2563EB", width=2),
                            hovertemplate="%{x|%Y-%m-%d}<br>THS 5日净额: %{y:,.0f} 万元<extra></extra>",
                        ))

                    fig_ths.update_layout(
                        title=dict(text="THS 个股资金流向趋势", x=0.02, font=dict(size=16, color="#1E293B")),
                        xaxis_title="日期",
                        yaxis_title="净流入额（万元）",
                        hovermode="x unified",
                        height=360,
                        template="wealthspark_balanced",
                        paper_bgcolor="rgba(248, 250, 252, 0.92)",
                        plot_bgcolor="rgba(241, 245, 249, 0.58)",
                        font=dict(family="Inter, PingFang SC, sans-serif"),
                        margin=dict(l=20, r=20, t=60, b=20),
                    )
                    st.plotly_chart(fig_ths, use_container_width=True)
                else:
                    st.info("THS口径暂无数据（会在查询时按需拉取，拉取起点 2025-01-01）。")

            with src_tab_dc:
                if df_hist_dc is not None and not df_hist_dc.empty:
                    _dc = df_hist_dc.copy()
                    _dc["trade_date"] = pd.to_datetime(_dc["trade_date"])
                    _dc = _dc.sort_values("trade_date")

                    fig_dc = go.Figure()
                    fig_dc.add_trace(go.Bar(
                        x=_dc["trade_date"],
                        y=_dc["net_amount"].astype(float),
                        name="DC主力净流入",
                        marker_color=["#EF4444" if v >= 0 else "#10B981" for v in _dc["net_amount"].astype(float)],
                        hovertemplate="%{x|%Y-%m-%d}<br>DC净流入: %{y:,.0f} 万元<extra></extra>",
                    ))
                    if "net_amount_rate" in _dc.columns:
                        fig_dc.add_trace(go.Scatter(
                            x=_dc["trade_date"],
                            y=_dc["net_amount_rate"].astype(float),
                            mode="lines",
                            name="DC净占比(%)",
                            yaxis="y2",
                            line=dict(color="#F59E0B", width=2),
                            hovertemplate="%{x|%Y-%m-%d}<br>DC净占比: %{y:.2f}%<extra></extra>",
                        ))

                    fig_dc.update_layout(
                        title=dict(text="DC 个股资金流向趋势", x=0.02, font=dict(size=16, color="#1E293B")),
                        xaxis_title="日期",
                        yaxis_title="净流入额（万元）",
                        yaxis2=dict(title="净占比(%)", overlaying="y", side="right", showgrid=False),
                        hovermode="x unified",
                        height=360,
                        template="wealthspark_balanced",
                        paper_bgcolor="rgba(248, 250, 252, 0.92)",
                        plot_bgcolor="rgba(241, 245, 249, 0.58)",
                        font=dict(family="Inter, PingFang SC, sans-serif"),
                        margin=dict(l=20, r=20, t=60, b=20),
                    )
                    st.plotly_chart(fig_dc, use_container_width=True)
                else:
                    st.info("DC口径暂无数据（会在查询时按需拉取，拉取起点 2025-01-01）。")

        elif df_hist is not None and df_hist.empty:
            st.info("该股票暂无资金流向数据，可能尚未入库。")

    # ============================================================
    # 子标签3：连续净流入选股
    # ============================================================
    with sub_screen:
        st.markdown("#### 🔍 连续主力净流入选股策略")
        st.info("筛选截至最新交易日，连续至少 N 天主力净流入的个股。")

        col_days, col_dt = st.columns(2)
        with col_days:
            min_days = st.slider("最少连续天数", min_value=2, max_value=20, value=3, key="mf_screen_days")
        with col_dt:
            screen_date = st.date_input("截止日期", value=latest_dt, key="mf_screen_date")

        if st.button("开始筛选", type="primary", key="btn_mf_screen"):
            with st.spinner(f"正在筛选连续 {min_days} 天净流入个股..."):
                try:
                    df_screen = query_moneyflow_consecutive_inflow(
                        min_days=min_days,
                        end_date=str(screen_date).replace("-", ""),
                        engine=_mf_engine
                    )
                    st.session_state["mf_screen_result"] = df_screen
                except Exception as e:
                    st.error(f"筛选失败：{e}")
                    st.session_state["mf_screen_result"] = pd.DataFrame()

        df_screen = st.session_state.get("mf_screen_result")
        if df_screen is not None and not df_screen.empty:
            st.success(f"✅ 共筛选出 **{len(df_screen)}** 只符合条件的个股")

            df_screen = df_screen.copy()
            df_screen["name"] = df_screen.get("name", df_screen.get("ts_code", "")).fillna(df_screen.get("ts_code", ""))
            df_screen["display_name"] = df_screen["name"].astype(str) + "（" + df_screen["ts_code"].astype(str) + "）"

            # 散点图：连续天数 vs 累计净流入
            fig_scatter = go.Figure(go.Scatter(
                x=df_screen["consecutive_days"].astype(float),
                y=df_screen["total_net_amount"].astype(float),
                mode="markers+text",
                text=df_screen["display_name"],
                textposition="top center",
                marker=dict(
                    size=df_screen["consecutive_days"].astype(float) * 4,
                    color=df_screen["total_net_amount"].astype(float),
                    colorscale="RdYlGn",
                    showscale=True,
                    colorbar=dict(title="累计净流入(万)"),
                    line=dict(color="white", width=1),
                ),
                hovertemplate="<b>%{text}</b><br>连续天数: %{x}<br>累计净流入: %{y:,.0f} 万<extra></extra>",
            ))
            fig_scatter.update_layout(
                title=dict(text="连续净流入天数 vs 累计净流入额", x=0.02, font=dict(size=18, color="#1E293B")),
                xaxis_title="连续净流入天数",
                yaxis_title="累计主力净流入（万元）",
                height=480,
                template="wealthspark_balanced",
                paper_bgcolor="rgba(248, 250, 252, 0.92)",
                plot_bgcolor="rgba(241, 245, 249, 0.58)",
                font=dict(family="Inter, PingFang SC, sans-serif"),
                margin=dict(l=20, r=20, t=60, b=20),
            )
            st.plotly_chart(fig_scatter, use_container_width=True)

            # 表格
            disp_cols = {
                "name": "名称",
                "ts_code": "代码",
                "consecutive_days": "连续天数",
                "total_net_amount": "累计净流入(万)",
                "avg_net_amount": "日均净流入(万)",
                "last_date": "最后日期",
            }
            from urllib.parse import quote

            render_nonce = st.session_state.get('mf_screen_render_nonce', 0) + 1
            st.session_state['mf_screen_render_nonce'] = render_nonce
            df_screen["jump_link"] = df_screen["ts_code"].astype(str).map(
                lambda code: f"?security_query={quote(code)}&security_type=stock&open_tab=security&jump_nonce={render_nonce}_{quote(code)}"
            )

            show_df = df_screen[[c for c in disp_cols if c in df_screen.columns]].rename(columns=disp_cols)
            show_df.insert(0, "跳转", df_screen["jump_link"])
            for col in ["累计净流入(万)", "日均净流入(万)"]:
                if col in show_df.columns:
                    show_df[col] = pd.to_numeric(show_df[col], errors="coerce").map(
                        lambda v: f"{v:,.0f}" if pd.notna(v) else "-"
                    )

            st.info("💡 点击“跳转”列的“🔎 查询”即可跳到“个股/指数查询”，并自动带入该股票代码。")
            st.dataframe(
                show_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "跳转": st.column_config.LinkColumn(
                        "跳转",
                        help='点击后跳转到个股/指数查询',
                        display_text='🔎 查询'
                    )
                }
            )
        elif df_screen is not None and df_screen.empty:
            st.info(f"未发现连续 {min_days} 天净流入个股（或该日非交易日）")

    # ============================================================
    # 子标签4：行业板块资金流向（THS + DC 两口径）
    # ============================================================
    with sub_sector:
        st.markdown("#### 🏭 行业/板块资金流向")

        st.markdown("##### 🎬 行业板块动画能力")
        anim_col0, anim_col1, anim_col2, anim_col3, anim_col4, anim_col5, anim_col6 = st.columns([1.0, 0.95, 0.95, 0.75, 0.75, 0.85, 0.9])
        with anim_col0:
            sector_anim_mode = st.selectbox("动画类型", ["条形轮动", "资金曲线"], index=0, key="mf_sector_anim_mode")
        with anim_col1:
            sector_anim_source = st.selectbox("动画口径", ["THS行业", "DC板块"], index=1, key="mf_sector_anim_source")
        with anim_col2:
            sector_anim_start = st.date_input("起始日期", value=max(latest_dt - timedelta(days=30), sector_min_dt), min_value=sector_min_dt, max_value=latest_dt, key="mf_sector_anim_start")
        with anim_col3:
            sector_anim_topn = st.selectbox("TopN", [5, 8, 10, 12, 15], index=2, key="mf_sector_anim_topn")
        with anim_col4:
            sector_anim_speed = st.selectbox("速度", [300, 500, 800], index=1, format_func=lambda x: {300: "快", 500: "中", 800: "慢"}[x], key="mf_sector_anim_speed")
        with anim_col5:
            sector_anim_sampling = st.selectbox("采样", ["每日", "每2日", "每5日"], index=0, key="mf_sector_anim_sampling")
        with anim_col6:
            sector_label_count = st.selectbox("标签数", [3, 5, 8], index=1, key="mf_sector_label_count")

        try:
            start_str = str(sector_anim_start).replace("-", "")
            end_str = latest_date
            if sector_anim_source == "THS行业":
                anim_df = query_moneyflow_ind_ths_range(start_str, end_str, engine=_mf_engine)
            else:
                anim_df = query_moneyflow_dc_ind_range(start_str, end_str, engine=_mf_engine)

            if anim_df is not None and not anim_df.empty:
                anim_df = anim_df.copy()
                anim_df["trade_date"] = pd.to_datetime(anim_df["trade_date"])
                anim_df["date_dt"] = pd.to_datetime(anim_df["trade_date"]).dt.normalize()
                anim_df["date_label"] = anim_df["trade_date"].dt.strftime("%Y-%m-%d")
                anim_df["sector_name"] = anim_df["sector_name"].fillna("未知板块")
                anim_df["net_amount"] = pd.to_numeric(anim_df["net_amount"], errors="coerce").fillna(0)
                anim_df["net_amount_yi"] = anim_df["net_amount"]

                sampling_step = {"每日": 1, "每2日": 2, "每5日": 5}.get(sector_anim_sampling, 1)
                date_list = sorted(anim_df["date_dt"].dropna().unique().tolist())
                kept_dates = set(date_list[::sampling_step] or date_list)
                if date_list and date_list[-1] not in kept_dates:
                    kept_dates.add(date_list[-1])
                anim_df = anim_df[anim_df["date_dt"].isin(kept_dates)].copy()

                anim_df["abs_rank"] = anim_df.groupby("date_label")["net_amount_yi"].transform(lambda s: s.abs().rank(method="first", ascending=False))
                anim_top = anim_df[anim_df["abs_rank"] <= int(sector_anim_topn)].copy()
                max_abs = anim_top["net_amount_yi"].abs().max()
                if pd.isna(max_abs) or max_abs <= 0:
                    max_abs = 1
                anim_top["color_group"] = anim_top["net_amount_yi"].apply(lambda v: "净流入" if v >= 0 else "净流出")

                if sector_anim_mode == "条形轮动":
                    fig_anim = px.bar(
                        anim_top,
                        x="net_amount_yi",
                        y="sector_name",
                        animation_frame="date_label",
                        animation_group="sector_name",
                        color="color_group",
                        color_discrete_map={"净流入": "#EF4444", "净流出": "#10B981"},
                        orientation="h",
                        range_x=[-max_abs * 1.15, max_abs * 1.15],
                        hover_data={"net_amount_yi": ':.2f', "sector_name": True, "date_label": True},
                    )
                    fig_anim.update_layout(
                        title=dict(text=f"{sector_anim_source} 资金流向轮动（从 {str(sector_anim_start)} 到 {latest_date}）", x=0.02, font=dict(size=17, color="#1E293B")),
                        template="wealthspark_balanced",
                        paper_bgcolor="rgba(248, 250, 252, 0.92)",
                        plot_bgcolor="rgba(241, 245, 249, 0.58)",
                        font=dict(family="Inter, PingFang SC, sans-serif"),
                        height=560,
                        margin=dict(l=30, r=30, t=60, b=30),
                        xaxis_title="净流入（亿元）",
                        yaxis_title="",
                        legend_title_text="方向",
                    )
                    fig_anim.update_yaxes(categoryorder="total ascending")
                    if fig_anim.layout.updatemenus:
                        fig_anim.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = int(sector_anim_speed)
                        fig_anim.layout.updatemenus[0].buttons[0].args[1]["transition"]["duration"] = max(120, int(sector_anim_speed * 0.6))
                    st.plotly_chart(fig_anim, use_container_width=True)
                    st.caption("说明：条形轮动动画展示每个交易日 TopN 净流入/净流出最显著板块，用于观察行业轮动路径。")
                else:
                    anim_top_sorted = anim_top.sort_values(["trade_date", "net_amount_yi"], ascending=[True, False]).copy()
                    curve_dates = sorted(anim_top_sorted["date_dt"].dropna().unique().tolist())
                    if not curve_dates:
                        st.info("当前筛选下无可播放的资金曲线数据。")
                    else:
                        ymin = float(anim_top_sorted["net_amount_yi"].min())
                        ymax = float(anim_top_sorted["net_amount_yi"].max())
                        if ymin == ymax:
                            pad = max(1.5, abs(ymin) * 0.35)
                        else:
                            pad = (ymax - ymin) * 0.32
                        y_range = [ymin - pad, ymax + pad]

                        def _build_frame_payload(frame_df: pd.DataFrame, current_dt=None):
                            traces = []
                            ann = []
                            last_points = frame_df.sort_values(["sector_name", "date_dt"]).groupby("sector_name", as_index=False).tail(1).copy()
                            last_points["abs_val"] = pd.to_numeric(last_points["net_amount_yi"], errors="coerce").fillna(0).abs()
                            last_points = last_points.sort_values("abs_val", ascending=False).head(int(sector_label_count)).copy()
                            last_points["label_text"] = last_points.apply(lambda r: f"{r['sector_name']} {r['net_amount_yi']:.2f}亿", axis=1)
                            y_span = float(frame_df["net_amount_yi"].max() - frame_df["net_amount_yi"].min()) if not frame_df.empty else 0.0
                            y_offset = max(0.12, y_span * 0.02)
                            if current_dt is not None:
                                current_dt = pd.to_datetime(current_dt)
                                label_x = current_dt + pd.Timedelta(days=2)
                            else:
                                label_x = last_points["date_dt"].max() + pd.Timedelta(days=2)

                            for sector_name, g in frame_df.groupby("sector_name", sort=False):
                                traces.append(go.Scatter(
                                    x=g["date_dt"],
                                    y=g["net_amount_yi"],
                                    mode="lines+markers",
                                    name=sector_name,
                                    line=dict(width=3, shape="spline", smoothing=0.9),
                                    marker=dict(size=6),
                                    hovertemplate="%{x|%Y-%m-%d}<br>%{fullData.name}: %{y:.2f}亿<extra></extra>",
                                    showlegend=True,
                                ))

                            for _, r in last_points.iterrows():
                                ann.append(dict(
                                    x=label_x,
                                    y=float(r.get("net_amount_yi") or 0) + y_offset,
                                    xref="x",
                                    yref="y",
                                    text=str(r.get("label_text") or ""),
                                    showarrow=False,
                                    xanchor="left",
                                    yanchor="middle",
                                    align="left",
                                    font=dict(size=12, color="#0F172A"),
                                    bgcolor="rgba(255,255,255,0.0)",
                                    borderwidth=0,
                                ))
                            return traces, ann
                        frames = []
                        for i, d in enumerate(curve_dates, start=1):
                            frame_df = anim_top_sorted[anim_top_sorted["date_dt"].isin(curve_dates[:i])].copy()
                            if frame_df.empty:
                                continue
                            frame_name = pd.to_datetime(d).strftime("%Y-%m-%d")
                            frame_traces, frame_ann = _build_frame_payload(frame_df, current_dt=d)
                            frames.append(go.Frame(data=frame_traces, layout=go.Layout(annotations=frame_ann), name=frame_name))

                        first_df = anim_top_sorted[anim_top_sorted["date_dt"].isin([curve_dates[0]])].copy()
                        first_traces, first_ann = _build_frame_payload(first_df, current_dt=curve_dates[0])
                        fig_anim = go.Figure(data=first_traces)
                        fig_anim.frames = frames
                        fig_anim.update_layout(
                            title=dict(text=f"{sector_anim_source} 资金曲线动画（从 {str(sector_anim_start)} 到 {latest_date}）", x=0.02, font=dict(size=17, color="#1E293B")),
                            template="wealthspark_balanced",
                            paper_bgcolor="rgba(248, 250, 252, 0.92)",
                            plot_bgcolor="rgba(241, 245, 249, 0.58)",
                            font=dict(family="Inter, PingFang SC, sans-serif"),
                            height=720,
                            margin=dict(l=30, r=260, t=80, b=30),
                            xaxis_title="日期",
                            yaxis_title="净流入（亿元）",
                            legend_title_text="板块",
                            hovermode="x unified",
                            xaxis=dict(type="date", tickformat="%Y-%m-%d", tickangle=-35, showgrid=True, range=[curve_dates[0], pd.to_datetime(curve_dates[-1]) + pd.Timedelta(days=12)]),
                            yaxis=dict(range=y_range, autorange=False, showgrid=True),
                            annotations=first_ann,
                            updatemenus=[{
                                "type": "buttons",
                                "showactive": False,
                                "buttons": [
                                    {
                                        "label": "播放",
                                        "method": "animate",
                                        "args": [None, {"frame": {"duration": int(sector_anim_speed), "redraw": True}, "transition": {"duration": max(180, int(sector_anim_speed * 0.8))}, "fromcurrent": True}]
                                    },
                                    {
                                        "label": "暂停",
                                        "method": "animate",
                                        "args": [[None], {"frame": {"duration": 0, "redraw": False}, "mode": "immediate", "transition": {"duration": 0}}]
                                    }
                                ]
                            }],
                        )
                        st.plotly_chart(fig_anim, use_container_width=True)
                        st.caption("说明：为避免重叠，曲线动画默认只显示部分末端标签（可调 3/5/8），标签会跟随线尾运动。")
                latest_frame = anim_top[anim_top["date_label"] == anim_top["date_label"].max()].copy()
                latest_frame = latest_frame.sort_values("net_amount_yi", ascending=False)
                latest_show = latest_frame[["sector_name", "net_amount_yi", "pct_change"]].copy()
                latest_show.columns = ["板块", "最新净流入(亿)", "涨跌幅(%)"]
                for c in ["最新净流入(亿)", "涨跌幅(%)"]:
                    latest_show[c] = pd.to_numeric(latest_show[c], errors="coerce").map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
                st.dataframe(latest_show, use_container_width=True, hide_index=True)
            else:
                st.info("所选时间区间暂无可用行业/板块流向动画数据。")
        except Exception as e:
            st.warning(f"行业板块轮动动画生成失败：{e}")

        st.divider()
        sector_date = st.date_input("查询日期", value=latest_dt, key="mf_sector_date")
        sector_query_date = str(sector_date).replace("-", "")

        col_ths, col_dc = st.columns(2)

        with col_ths:
            st.markdown("**📗 同花顺口径（THS）**")
            try:
                df_ths = query_moneyflow_ind_ths_daily(sector_query_date, engine=_mf_engine)
                if df_ths is not None and not df_ths.empty:
                    df_ths["net_amount"] = pd.to_numeric(df_ths["net_amount"], errors="coerce")
                    df_ths = df_ths.dropna(subset=["net_amount"]).sort_values("net_amount", ascending=False)

                    colors_ths = ["#EF4444" if v >= 0 else "#10B981" for v in df_ths["net_amount"]]
                    fig_ths = go.Figure(go.Bar(
                        x=df_ths["net_amount"],
                        y=df_ths["industry"].fillna("未知"),
                        orientation="h",
                        marker_color=colors_ths,
                        hovertemplate="%{y}<br>净流入: %{x:,.2f} 亿元<extra></extra>",
                    ))
                    fig_ths.update_layout(
                        title=dict(text="THS 行业净流入（亿元）", x=0.02, font=dict(size=15, color="#1E293B")),
                        height=max(380, len(df_ths) * 20),
                        template="wealthspark_balanced",
                        paper_bgcolor="rgba(248, 250, 252, 0.92)",
                        plot_bgcolor="rgba(241, 245, 249, 0.58)",
                        font=dict(family="Inter, PingFang SC, sans-serif"),
                        margin=dict(l=20, r=20, t=50, b=20),
                        yaxis=dict(autorange="reversed"),
                    )
                    st.plotly_chart(fig_ths, use_container_width=True)

                    show_ths = df_ths[["industry", "net_amount", "pct_change", "lead_stock"]].rename(columns={
                        "industry": "行业", "net_amount": "净流入(亿)",
                        "pct_change": "涨跌幅(%)", "lead_stock": "领涨股",
                    }).copy()
                    for c in ["净流入(亿)", "涨跌幅(%)"]:
                        if c in show_ths.columns:
                            show_ths[c] = pd.to_numeric(show_ths[c], errors="coerce").map(
                                lambda v: f"{v:,.2f}" if pd.notna(v) else "-"
                            )
                    st.dataframe(show_ths, use_container_width=True, hide_index=True)
                else:
                    st.info("THS 行业数据暂无（需要5000+积分，或当日非交易日）")
            except Exception as e:
                st.warning(f"THS行业数据查询失败：{e}")

        with col_dc:
            st.markdown("**📘 东方财富口径（DC）**")
            try:
                df_dc = query_moneyflow_dc_ind_daily(sector_query_date, engine=_mf_engine)
                if df_dc is not None and not df_dc.empty:
                    df_dc["net_amount"] = pd.to_numeric(df_dc["net_amount"], errors="coerce")
                    df_dc = df_dc.dropna(subset=["net_amount"]).sort_values("net_amount", ascending=False)

                    colors_dc = ["#EF4444" if v >= 0 else "#10B981" for v in df_dc["net_amount"]]
                    fig_dc = go.Figure(go.Bar(
                        x=df_dc["net_amount"],
                        y=df_dc["name"].fillna("未知"),
                        orientation="h",
                        marker_color=colors_dc,
                        hovertemplate="%{y}<br>净流入: %{x:,.2f} 亿元<extra></extra>",
                    ))
                    fig_dc.update_layout(
                        title=dict(text="DC 板块净流入（亿元）", x=0.02, font=dict(size=15, color="#1E293B")),
                        height=max(380, len(df_dc) * 20),
                        template="wealthspark_balanced",
                        paper_bgcolor="rgba(248, 250, 252, 0.92)",
                        plot_bgcolor="rgba(241, 245, 249, 0.58)",
                        font=dict(family="Inter, PingFang SC, sans-serif"),
                        margin=dict(l=20, r=20, t=50, b=20),
                        yaxis=dict(autorange="reversed"),
                    )
                    st.plotly_chart(fig_dc, use_container_width=True)

                    show_dc = df_dc[["name", "net_amount", "pct_change", "net_amount_rate"]].rename(columns={
                        "name": "板块", "net_amount": "净流入(亿)",
                        "pct_change": "涨跌幅(%)", "net_amount_rate": "净流入占比(%)",
                    }).copy()
                    for c in ["净流入(亿)", "涨跌幅(%)", "净流入占比(%)"]:
                        if c in show_dc.columns:
                            show_dc[c] = pd.to_numeric(show_dc[c], errors="coerce").map(
                                lambda v: f"{v:,.2f}" if pd.notna(v) else "-"
                            )
                    st.dataframe(show_dc, use_container_width=True, hide_index=True)
                else:
                    st.info("DC 板块数据暂无（需要2000+积分，或当日非交易日）")
            except Exception as e:
                st.warning(f"DC板块数据查询失败：{e}")

    # ============================================================
    # 子标签5：北向资金（沪深港通）
    # ============================================================
    with sub_hsgt:
        st.markdown("#### 🌐 沪深港通资金流向（北向 / 南向）")

        hsgt_days_map = {"近1月": 30, "近3月": 90, "近半年": 180, "近1年": 365, "全部": 0}
        hsgt_range = st.selectbox("时间范围", list(hsgt_days_map.keys()), index=1, key="mf_hsgt_range")
        hsgt_days = hsgt_days_map[hsgt_range]

        try:
            hsgt_start = None
            if hsgt_days > 0:
                hsgt_start = (pd.Timestamp.today() - pd.Timedelta(days=hsgt_days)).strftime("%Y%m%d")
            df_hsgt = query_moneyflow_hsgt_history(start_date=hsgt_start, engine=_mf_engine)
            if df_hsgt is not None and not df_hsgt.empty:
                df_hsgt["trade_date"] = pd.to_datetime(df_hsgt["trade_date"])
                df_hsgt = df_hsgt.sort_values("trade_date")

                # 摘要指标卡片
                latest_hsgt = df_hsgt.iloc[-1]
                prev_hsgt = df_hsgt.iloc[-2] if len(df_hsgt) >= 2 else None

                def safe_float(val):
                    try:
                        return float(val)
                    except Exception:
                        return None

                north_val = safe_float(latest_hsgt.get("north_money"))
                south_val = safe_float(latest_hsgt.get("south_money"))
                north_prev = safe_float(prev_hsgt.get("north_money")) if prev_hsgt is not None else None

                hsgt_cols = st.columns(3)
                with hsgt_cols[0]:
                    n_str = f"{north_val:,.2f} 亿" if north_val is not None else "-"
                    nd = f"{north_val - north_prev:+.2f}" if north_val is not None and north_prev is not None else "-"
                    st.markdown(draw_metric_card("北向净流入（亿元）", n_str, nd), unsafe_allow_html=True)
                with hsgt_cols[1]:
                    s_str = f"{south_val:,.2f} 亿" if south_val is not None else "-"
                    st.markdown(draw_metric_card("南向净流入（亿元）", s_str, "-"), unsafe_allow_html=True)
                with hsgt_cols[2]:
                    cumul_north = df_hsgt["north_money"].astype(float).sum()
                    st.markdown(draw_metric_card("区间北向累计（亿元）", f"{cumul_north:,.2f}", "-"), unsafe_allow_html=True)

                st.markdown("<br>", unsafe_allow_html=True)

                # 北向/南向趋势图
                fig_hsgt = go.Figure()

                north_vals = df_hsgt["north_money"].astype(float)
                colors_n = ["#EF4444" if v >= 0 else "#10B981" for v in north_vals]
                fig_hsgt.add_trace(go.Bar(
                    x=df_hsgt["trade_date"],
                    y=north_vals,
                    name="北向净流入",
                    marker_color=colors_n,
                    opacity=0.85,
                    hovertemplate="%{x|%Y-%m-%d}<br>北向净流入: %{y:,.2f} 亿元<extra></extra>",
                ))

                # 北向5日累计均线
                north_ma5 = north_vals.rolling(5).mean()
                fig_hsgt.add_trace(go.Scatter(
                    x=df_hsgt["trade_date"],
                    y=north_ma5,
                    mode="lines",
                    name="5日均线",
                    line=dict(color="#8E54E9", width=2.5),
                    hovertemplate="%{x|%Y-%m-%d}<br>5日均线: %{y:,.2f} 亿<extra></extra>",
                ))

                fig_hsgt.update_layout(
                    title=dict(text="北向资金每日净流入（亿元）", x=0.02, font=dict(size=18, color="#1E293B")),
                    xaxis_title="日期",
                    yaxis_title="净流入额（亿元）",
                    hovermode="x unified",
                    height=420,
                    template="wealthspark_balanced",
                    paper_bgcolor="rgba(248, 250, 252, 0.92)",
                    plot_bgcolor="rgba(241, 245, 249, 0.58)",
                    font=dict(family="Inter, PingFang SC, sans-serif"),
                    margin=dict(l=20, r=20, t=60, b=20),
                    legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5),
                )
                fig_hsgt.update_xaxes(showgrid=True, gridcolor="rgba(226,232,240,0.5)")
                fig_hsgt.update_yaxes(
                    showgrid=True, gridcolor="rgba(226,232,240,0.5)",
                    zeroline=True, zerolinecolor="#CBD5E1", zerolinewidth=1.5
                )
                st.plotly_chart(fig_hsgt, use_container_width=True)

                # 沪股通 + 深股通分项
                if "hgt" in df_hsgt.columns and "sgt" in df_hsgt.columns:
                    fig_detail = go.Figure()
                    fig_detail.add_trace(go.Scatter(
                        x=df_hsgt["trade_date"],
                        y=df_hsgt["hgt"].astype(float),
                        mode="lines",
                        name="沪股通",
                        line=dict(color="#3B82F6", width=2),
                        hovertemplate="%{x|%Y-%m-%d}<br>沪股通: %{y:,.2f} 亿<extra></extra>",
                    ))
                    fig_detail.add_trace(go.Scatter(
                        x=df_hsgt["trade_date"],
                        y=df_hsgt["sgt"].astype(float),
                        mode="lines",
                        name="深股通",
                        line=dict(color="#F59E0B", width=2),
                        hovertemplate="%{x|%Y-%m-%d}<br>深股通: %{y:,.2f} 亿<extra></extra>",
                    ))
                    fig_detail.update_layout(
                        title=dict(text="沪股通 / 深股通 净流入分项", x=0.02, font=dict(size=16, color="#1E293B")),
                        height=360,
                        template="wealthspark_balanced",
                        paper_bgcolor="rgba(248, 250, 252, 0.92)",
                        plot_bgcolor="rgba(241, 245, 249, 0.58)",
                        font=dict(family="Inter, PingFang SC, sans-serif"),
                        margin=dict(l=20, r=20, t=60, b=20),
                        hovermode="x unified",
                        legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5),
                    )
                    fig_detail.update_xaxes(showgrid=True, gridcolor="rgba(226,232,240,0.5)")
                    fig_detail.update_yaxes(showgrid=True, gridcolor="rgba(226,232,240,0.5)")
                    st.plotly_chart(fig_detail, use_container_width=True)

                # 明细表格
                show_hsgt = df_hsgt[["trade_date", "north_money", "south_money", "hgt", "sgt"]].copy()
                show_hsgt["trade_date"] = show_hsgt["trade_date"].dt.strftime("%Y-%m-%d")
                show_hsgt = show_hsgt.rename(columns={
                    "trade_date": "日期", "north_money": "北向(亿)",
                    "south_money": "南向(亿)", "hgt": "沪股通(亿)", "sgt": "深股通(亿)"
                }).sort_values("日期", ascending=False)
                for col in ["北向(亿)", "南向(亿)", "沪股通(亿)", "深股通(亿)"]:
                    if col in show_hsgt.columns:
                        show_hsgt[col] = pd.to_numeric(show_hsgt[col], errors="coerce").map(
                            lambda v: f"{v:,.2f}" if pd.notna(v) else "-"
                        )
                st.dataframe(show_hsgt, use_container_width=True, hide_index=True, height=380)
            else:
                st.info("暂无北向资金数据（moneyflow_hsgt 尚未入库，或时间范围内无交易日）")
        except Exception as e:
            st.warning(f"北向资金查询失败：{e}")


if __name__ == "__main__":
    main()
