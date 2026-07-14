# -*- coding: utf-8 -*-
"""ETF份额变动可视化 - Streamlit Web应用"""

# Version: 2.0 - Fixed data_only issue for formula cells
import os
import json
import time
from html import escape
from hmac import compare_digest
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio
try:
    from streamlit_plotly_events import plotly_events
except Exception:
    plotly_events = None
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import logging
from typing import Optional, List, Union
from urllib.parse import quote
from src.data_loader import load_etf_data
from src.volume_fetcher import load_volume_dataframe
from src.etf_classifier import fetch_etf_data, process_etf_classification, export_etfs_to_excel
from src.etf_stats import (
    get_available_dates, get_category_daily_summary,
    get_category_tree, get_category_timeseries, get_agg_summary,
    get_wide_index_available_dates, get_wide_index_timeseries,
    get_macro_date_bounds, get_macro_dataset_timeseries,
    search_security, get_security_profile, get_security_timeseries,
    get_security_financial_timeseries, get_security_kline_timeseries, get_stock_basic_summary,
    export_stock_basic_summary_excel, search_companies, update_stock_custom_info,
    validate_stock_custom_info_inputs,
    get_stock_holder_number_timeseries
)
from src.security_intraday_store import (
    get_engine as get_security_intraday_engine,
    load_or_fetch_stock_intraday_timeseries,
)
from src.trend_reco_store import (
    fetch_trend_reco_payload,
    get_engine as get_trend_reco_engine,
    list_trend_reco_payloads,
    list_trend_reco_runs,
)
from src.etf_deposit_store import (
    build_balance_trend_df,
    build_change_trend_df,
    build_deposit_summary,
    build_upsert_rows,
    classify_import_rows,
    delete_deposit_months,
    get_engine as get_deposit_engine,
    load_deposit_monthly_df,
    to_deposit_display_df,
    upsert_deposit_rows,
)
from src.etf_deposit_importer import parse_deposit_workbook
from src.fund_monitor_importer import parse_fund_monitor_workbook
from src.fund_monitor_store import (
    CHANGE_TREND_FIELD_LABELS as FUND_CHANGE_TREND_FIELD_LABELS,
    build_fund_monitor_change_trend_df,
    build_fund_monitor_summary,
    build_fund_monitor_trend_df,
    build_fund_monitor_upsert_rows,
    classify_fund_monitor_import_rows,
    delete_fund_monitor_months,
    get_engine as get_fund_monitor_engine,
    load_fund_monitor_df,
    to_fund_monitor_display_df,
    upsert_fund_monitor_rows,
)

from src.index_monitor_importer import parse_index_monitor_workbook
from src.index_monitor_store import (
    CHANGE_TREND_FIELD_LABELS,
    build_index_change_trend_df,
    build_index_monitor_summary,
    build_index_upsert_rows,
    build_price_trend_df,
    build_valuation_trend_df,
    classify_index_import_rows,
    get_engine as get_index_monitor_engine,
    load_index_monitor_df,
    to_index_monitor_display_df,
    upsert_index_monitor_rows,
)
from src.navigation_config import (
    DECISION_DAILY_RECO_PAGE_LABEL,
    DECISION_ML_PAGE_LABEL,
    DECISION_PAGE_OPTIONS,
    DECISION_RECO_EVAL_PAGE_LABEL,
    DECISION_TODAY_PAGE_LABEL,
    ETF_FUND_MONITOR_PAGE_LABEL,
    ETF_FUND_WATCHLIST_PAGE_LABEL,
    ETF_MAIN_PAGE_LABEL,
    ETF_PAGE_OPTIONS,
    ETF_RATIO_PAGE_LABEL,
    ETF_TREND_PAGE_LABEL,
    ETF_WIDE_INDEX_PAGE_LABEL,
    MACRO_DEPOSIT_PAGE_LABEL,
    MACRO_INDEX_MONITOR_PAGE_LABEL,
    MACRO_MAIN_PAGE_LABEL,
    MACRO_PAGE_OPTIONS,
    MONEY_FLOW_PAGE_LABEL,
    MONEY_FUND_HOT_PAGE_LABEL,
    MONEY_HOTMONEY_PAGE_LABEL,
    MONEY_LIMITUP_PAGE_LABEL,
    MONEY_PAGE_OPTIONS,
    MONEY_VOLUME_PAGE_LABEL,
    STOCK_COMPANY_SCREENER_LABEL,
    STOCK_LHB_PAGE_LABEL,
    STOCK_PAGE_OPTIONS,
    STOCK_POOL_PAGE_LABEL,
    STOCK_SECURITY_SEARCH_LABEL,
    STOCK_TECH_PICKER_LABEL,
    STOCK_USER_WATCHLIST_LABEL,
)
from src.sidebar_navigation import (
    SIDEBAR_MODULES,
    get_module_by_id,
    get_module_by_label,
    get_module_label_for_page,
    get_module_labels,
    get_page_by_id,
    get_page_labels,
    get_recent_visits,
    record_recent_visit,
    resolve_expanded_module_id,
    search_sidebar_pages,
)
from src.factor_workbench import (
    FACTOR_WORKBENCH_PAGE_LABEL,
    apply_factor_filters,
    compute_factor_scores,
    get_factor_catalog,
    get_factor_workbench_data_freshness,
    get_factor_workbench_trade_dates,
    get_score_preset,
    load_factor_workbench_frame,
)
from src.fund_watchlist_dashboard import (
    build_fund_watchlist_item,
    build_fund_watchlist_summary,
    build_fund_watchlist_table,
    sort_fund_watchlist_items,
)
from src.page_filter_utils import (
    build_metric_categories,
    build_quick_metric_groups,
    build_secondary_category_options,
    resolve_trend_category_key,
)
from src.ml_reco_candidate_scores import (
    load_candidate_scores_from_snapshot,
    compute_candidate_scores as compute_ml_reco_candidate_scores,
)
from src.apple_theme import (
    APPLE_THEME_TOKENS,
    build_apple_plotly_template,
    build_author_tracker_apple_css,
    build_global_apple_theme_css,
    get_apple_theme_tokens,
)

from src.ml_stock_train_v1 import (
    DEFAULT_CLASSIFICATION_TARGET,
    DEFAULT_REGRESSION_TARGET,
    SUPPORTED_CLASSIFIERS,
    SUPPORTED_MODEL_KINDS,
    SUPPORTED_REGRESSORS,
    prepare_training_data,
    run_walk_forward_evaluation,
)
from src.eastmoney_author_tracker.ui import TRACKING_PAGE_LABEL, render_author_tracking_tab
from src.user_watchlist_store import (
    add_watchlist_item,
    is_in_watchlist,
    list_watchlist_items,
    normalize_username,
    remove_watchlist_item,
    remove_watchlist_items_batch,
)
from src.distribution_alert_store import get_latest_alerts_for_stocks
from src.distribution_report_store import get_daily_report, get_report_status, get_report_statuses
from src.stock_research_report_store import (
    get_daily_report_record as get_stock_research_daily_report_record,
    get_report_status as get_stock_research_report_status,
    get_report_statuses as get_stock_research_report_statuses,
)
from src.stock_analysis_template_report import generate_stock_analysis_template_report_bundle
from src.watchlist_distribution_refresh import refresh_watchlist_distribution_reports
from src.watchlist_stock_research_refresh import refresh_watchlist_stock_research_reports
from src.watchlist_excel_importer import (
    import_watchlist_rows,
    parse_watchlist_import_workbook,
)
from src.stock_pool_excel_importer import parse_stock_pool_import_workbook
from src.user_stock_pool_store import (
    format_tags as format_stock_pool_tags,
    import_stock_pool_rows,
    list_stock_pool_items,
    remove_stock_pool_items_batch,
    split_tags as split_stock_pool_tags,
    update_stock_pool_item_metadata,
    upsert_stock_pool_item,
)

try:
    from src.security_trend_model import (
        score_security_timeseries_model,
        get_security_model_meta,
    )
except Exception:
    score_security_timeseries_model = None
    get_security_model_meta = None

_LHB_TODAY_BOARD_COMPONENT = components.declare_component(
    "lhb_today_board",
    path=os.path.join(os.path.dirname(os.path.abspath(__file__)), "src", "lhb_board_component"),
)

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

apple_plotly_template = build_apple_plotly_template()
pio.templates["wealthspark_apple"] = apple_plotly_template
pio.templates["wealthspark_balanced"] = apple_plotly_template
pio.templates["plotly_white"] = apple_plotly_template
pio.templates.default = "wealthspark_apple"

THEME = get_apple_theme_tokens(APPLE_THEME_TOKENS)
THEME_PRIMARY = THEME["primary"]
THEME_PRIMARY_HOVER = THEME["primary_hover"]
THEME_PRIMARY_STRONG = THEME["primary_strong"]
THEME_NAVY = THEME["bg_dark"]
THEME_SURFACE = THEME["bg_surface"]
THEME_TEXT = THEME["text_main"]
THEME_MUTED = THEME["text_muted"]
THEME_BORDER_SOFT = THEME["border_soft"]
THEME_SHADOW = THEME["shadow"]
THEME_UP = THEME["color_up"]
THEME_DOWN = THEME["color_down"]
THEME_WARN = THEME["color_warn"]
THEME_NEUTRAL = THEME["color_neutral"]
THEME_PURPLE = THEME["color_purple"]
CHART_BG = THEME_SURFACE
CHART_PAPER_BG = THEME["bg_base"]
CHART_GRID_COLOR = "rgba(27, 38, 59, 0.08)"
CHART_AXIS_COLOR = "rgba(27, 38, 59, 0.12)"
CHART_ZERO_LINE_COLOR = "rgba(27, 38, 59, 0.18)"
CHART_UP_FILL = "rgba(230, 57, 70, 0.20)"
CHART_DOWN_FILL = "rgba(42, 157, 143, 0.20)"
CHART_NAVY_SOFT_FILL = "rgba(27, 38, 59, 0.10)"
CHART_GOLD_SOFT_FILL = "rgba(212, 175, 55, 0.12)"
CHART_SERIES = [THEME_NAVY, THEME_PRIMARY, "#4F6785", "#5B8E7D", "#C28C4E", THEME_PURPLE]
TIME_SERIES_HOVER_RIGHT_MARGIN = 160
TIME_SERIES_HOVER_DISTANCE = 60
TIME_SERIES_HOVER_TARGET_WIDTH = 42
TIME_SERIES_XAXIS_RIGHT_PAD_RATIO = 0.05
TIME_SERIES_DAILY_MIN_RIGHT_PAD = pd.Timedelta(days=30)
TIME_SERIES_INTRADAY_MIN_RIGHT_PAD = pd.Timedelta(minutes=20)
VOLUME_STACKED_HOVER_RIGHT_MARGIN = TIME_SERIES_HOVER_RIGHT_MARGIN
VOLUME_STACKED_HOVER_DISTANCE = TIME_SERIES_HOVER_DISTANCE
VOLUME_STACKED_XAXIS_RIGHT_PAD_DAYS = 75


def _coerce_datetime_series(values) -> pd.Series:
    if values is None:
        return pd.Series(dtype="datetime64[ns]")
    try:
        series = pd.Series(values)
    except Exception:
        series = pd.Series([values])
    return pd.to_datetime(series, errors="coerce").dropna()


def _coerce_numeric_series(values) -> pd.Series:
    if values is None:
        return pd.Series(dtype="float64")
    if isinstance(values, pd.DataFrame):
        return pd.to_numeric(values.stack(), errors="coerce").dropna()
    if isinstance(values, (list, tuple)) and values and all(isinstance(item, pd.Series) for item in values):
        return pd.to_numeric(pd.concat(values, ignore_index=True), errors="coerce").dropna()
    try:
        series = pd.Series(values)
    except Exception:
        series = pd.Series([values])
    return pd.to_numeric(series, errors="coerce").dropna()


def _max_timedelta(left: pd.Timedelta, right: pd.Timedelta) -> pd.Timedelta:
    return left if left >= right else right


def apply_time_series_hover_affordance(
    fig: go.Figure,
    x_values,
    y_values=None,
    *,
    min_right_pad: pd.Timedelta = TIME_SERIES_DAILY_MIN_RIGHT_PAD,
    latest_label: str = "最新",
    add_latest_marker: bool = True,
    add_hover_target: bool = True,
) -> go.Figure:
    dates = _coerce_datetime_series(x_values)
    if dates.empty:
        return fig

    earliest = dates.min()
    latest = dates.max()
    span = latest - earliest
    proportional_pad = span * TIME_SERIES_XAXIS_RIGHT_PAD_RATIO if span > pd.Timedelta(0) else pd.Timedelta(0)
    right_pad = _max_timedelta(proportional_pad, min_right_pad)

    fig.update_layout(
        hoverdistance=TIME_SERIES_HOVER_DISTANCE,
        margin=dict(r=TIME_SERIES_HOVER_RIGHT_MARGIN),
    )
    fig.update_xaxes(range=[earliest, latest + right_pad])

    if add_hover_target:
        y_series = _coerce_numeric_series(y_values)
        if y_series.empty:
            y0, y1 = 0.0, 1.0
        else:
            y0 = float(y_series.min())
            y1 = float(y_series.max())
            if y0 == y1:
                padding = max(abs(y0) * 0.01, 1.0)
                y0 -= padding
                y1 += padding
        fig.add_trace(go.Scatter(
            x=[latest, latest],
            y=[y0, y1],
            mode="lines",
            name="latest-day-hover-target",
            line=dict(color="rgba(212, 175, 55, 0.01)", width=TIME_SERIES_HOVER_TARGET_WIDTH),
            hovertemplate="<extra></extra>",
            showlegend=False,
        ))

    if add_latest_marker:
        date_format = "%H:%M" if (latest - earliest) <= pd.Timedelta(days=1) else "%Y-%m-%d"
        fig.add_shape(
            type="line",
            xref="x",
            yref="paper",
            x0=latest,
            x1=latest,
            y0=0,
            y1=1,
            line=dict(color=THEME_PRIMARY, width=1.5, dash="dot"),
            layer="above",
        )
        fig.add_annotation(
            x=latest,
            y=1.02,
            xref="x",
            yref="paper",
            text=f"{latest_label} {latest:{date_format}}",
            showarrow=False,
            xanchor="left",
            yanchor="bottom",
            xshift=6,
            font=dict(size=11, color=THEME_TEXT),
            bgcolor="rgba(255, 255, 255, 0.72)",
            bordercolor=THEME_BORDER_SOFT,
            borderwidth=1,
            borderpad=3,
        )
    return fig

# Legacy inline CSS retired; shared Professional Gold theme is injected below.

# 数据文件路径
DATA_FILE = "主要ETF基金份额变动情况.xlsx"
st.markdown(
    f"<style>{build_global_apple_theme_css()}{build_author_tracker_apple_css()}</style>",
    unsafe_allow_html=True,
)


HOTMONEY_SECTION_WRAPPER_CSS = """
<style>
.ws-hotmoney-section {
    margin: 1.05rem 0 1.35rem 0;
    padding: 0.2rem 0 0.8rem 0;
    border-bottom: 1px solid var(--ws-border-soft);
}
.ws-hotmoney-section:last-of-type {
    border-bottom: none;
    margin-bottom: 0.35rem;
}
.ws-hotmoney-section .ws-hotmoney-kicker {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    padding: 0.22rem 0.72rem;
    border-radius: 999px;
    background: var(--ws-color-primary-soft);
    color: var(--ws-color-primary);
    font-size: 0.76rem;
    font-weight: 700;
    letter-spacing: 0.02em;
    margin-bottom: 0.45rem;
}
.ws-hotmoney-section .ws-hotmoney-kicker::before {
    content: "";
    width: 0.38rem;
    height: 0.38rem;
    border-radius: 999px;
    background: currentColor;
    opacity: 0.85;
}
.ws-hotmoney-section .ws-hotmoney-note {
    color: var(--ws-text-soft);
    font-size: 0.83rem;
    margin-top: -0.05rem;
    margin-bottom: 0.15rem;
    line-height: 1.6;
}
</style>
"""

st.markdown(HOTMONEY_SECTION_WRAPPER_CSS, unsafe_allow_html=True)

WATCHLIST_CYBER_DASHBOARD_CSS = """
<style>
.ws-watchboard-shell {
    --wb-bg: #030816;
    --wb-panel: rgba(5, 17, 39, 0.92);
    --wb-panel-2: rgba(3, 12, 30, 0.95);
    --wb-line: rgba(70, 126, 255, 0.54);
    --wb-line-soft: rgba(70, 126, 255, 0.22);
    --wb-cyan: #22d7ff;
    --wb-blue: #2f7bff;
    --wb-red: #ff3f55;
    --wb-green: #20dfb8;
    --wb-text: #f5f9ff;
    --wb-muted: #93a9ca;
    margin: 0.75rem 0 1.2rem 0;
    padding: 1rem;
    color: var(--wb-text);
    border: 1px solid var(--wb-line);
    border-radius: 8px;
    background:
        radial-gradient(circle at 10% 0%, rgba(47, 123, 255, 0.30), transparent 30%),
        radial-gradient(circle at 88% 14%, rgba(34, 215, 255, 0.17), transparent 24%),
        linear-gradient(180deg, #061128 0%, #020615 100%);
    box-shadow: 0 18px 48px rgba(4, 11, 30, 0.34), inset 0 0 36px rgba(47, 123, 255, 0.10);
    overflow: hidden;
    position: relative;
}
.ws-watchboard-shell::before {
    content: "";
    position: absolute;
    inset: 0;
    pointer-events: none;
    background-image:
        linear-gradient(rgba(64, 125, 255, 0.055) 1px, transparent 1px),
        linear-gradient(90deg, rgba(64, 125, 255, 0.055) 1px, transparent 1px);
    background-size: 54px 54px;
    mask-image: linear-gradient(180deg, rgba(0,0,0,0.65), rgba(0,0,0,0.16));
}
.ws-watchboard-shell * {
    box-sizing: border-box;
}
.ws-watchboard-topbar,
.ws-watchboard-hero,
.ws-watchboard-main,
.ws-watchboard-strip {
    position: relative;
    z-index: 1;
}
.ws-watchboard-topbar {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto minmax(210px, auto);
    align-items: center;
    gap: 0.8rem;
    margin-bottom: 0.9rem;
}
.ws-watchboard-title {
    display: flex;
    align-items: baseline;
    gap: 0.7rem;
    min-width: 0;
}
.ws-watchboard-title strong {
    font-size: clamp(1.2rem, 2.2vw, 1.75rem);
    line-height: 1.1;
    color: var(--wb-text);
    letter-spacing: 0;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.ws-watchboard-title span {
    color: var(--wb-muted);
    font-size: 0.95rem;
    white-space: nowrap;
}
.ws-watchboard-chip,
.ws-watchboard-clock,
.ws-watchboard-status {
    min-height: 42px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 0.45rem;
    padding: 0.5rem 0.82rem;
    border: 1px solid var(--wb-line-soft);
    border-radius: 8px;
    background: rgba(3, 13, 33, 0.78);
    box-shadow: inset 0 0 18px rgba(47, 123, 255, 0.10);
    color: #c9dcff;
    font-weight: 650;
    white-space: nowrap;
}
.ws-watchboard-chip {
    color: #a8c5ff;
}
.ws-watchboard-hero {
    display: grid;
    grid-template-columns: minmax(280px, 0.9fr) minmax(460px, 1.1fr);
    gap: 0.75rem;
    margin-bottom: 0.75rem;
}
.ws-watchboard-panel {
    border: 1px solid var(--wb-line);
    border-radius: 8px;
    background: linear-gradient(180deg, var(--wb-panel) 0%, var(--wb-panel-2) 100%);
    box-shadow: inset 0 0 20px rgba(47, 123, 255, 0.11), 0 0 0 1px rgba(34, 215, 255, 0.03);
    position: relative;
    overflow: hidden;
}
.ws-watchboard-panel::after {
    content: "";
    position: absolute;
    inset: 0;
    pointer-events: none;
    border-radius: 8px;
    box-shadow: inset 0 0 0 1px rgba(255,255,255,0.025);
}
.ws-watchboard-price {
    min-height: 116px;
    padding: 1.05rem 1.05rem 0.95rem 1.05rem;
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
    align-items: center;
    gap: 0.65rem;
}
.ws-watchboard-symbol {
    color: var(--wb-muted);
    font-size: 0.88rem;
    margin-bottom: 0.2rem;
}
.ws-watchboard-bigprice {
    display: flex;
    align-items: center;
    gap: 0.45rem;
    color: var(--wb-red);
    font-size: clamp(2.1rem, 5.4vw, 4.2rem);
    line-height: 1;
    font-weight: 800;
    letter-spacing: 0;
}
.ws-watchboard-bigprice.ws-negative {
    color: var(--wb-green);
}
.ws-watchboard-arrow {
    font-size: 0.64em;
    line-height: 1;
}
.ws-watchboard-delta {
    color: var(--wb-red);
    font-size: clamp(1rem, 2vw, 1.45rem);
    font-weight: 750;
    line-height: 1.35;
    text-align: right;
}
.ws-watchboard-delta.ws-negative {
    color: var(--wb-green);
}
.ws-watchboard-status {
    min-height: 34px;
    color: #ff5368;
    border-color: rgba(255, 63, 85, 0.35);
    background: rgba(255, 63, 85, 0.08);
}
.ws-watchboard-stats {
    min-height: 116px;
    padding: 0.72rem;
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 0.55rem;
}
.ws-watchboard-stat {
    display: grid;
    grid-template-columns: 34px minmax(0, 1fr);
    gap: 0.55rem;
    align-items: center;
    min-width: 0;
    padding: 0.56rem 0.58rem;
    border: 1px solid rgba(70, 126, 255, 0.18);
    border-radius: 8px;
    background: rgba(2, 9, 24, 0.45);
}
.ws-watchboard-stat-icon {
    width: 34px;
    height: 34px;
    display: grid;
    place-items: center;
    color: #83aaff;
    border-radius: 8px;
    background: linear-gradient(180deg, rgba(47, 123, 255, 0.26), rgba(34, 215, 255, 0.10));
    font-weight: 800;
}
.ws-watchboard-stat label {
    display: block;
    color: var(--wb-muted);
    font-size: 0.78rem;
    margin-bottom: 0.12rem;
    white-space: nowrap;
}
.ws-watchboard-stat strong {
    display: block;
    color: var(--wb-text);
    font-size: clamp(0.9rem, 1.18vw, 1.08rem);
    line-height: 1.15;
    white-space: normal;
    overflow-wrap: anywhere;
}
.ws-watchboard-main {
    display: grid;
    grid-template-columns: minmax(0, 1.45fr) minmax(300px, 0.92fr);
    gap: 0.75rem;
}
.ws-watchboard-chart {
    padding: 0;
}
.ws-watchboard-tabs {
    display: grid;
    grid-template-columns: repeat(4, 1fr) auto;
    border-bottom: 1px solid rgba(70, 126, 255, 0.22);
    min-height: 48px;
}
.ws-watchboard-tab,
.ws-watchboard-fullscreen {
    display: flex;
    align-items: center;
    justify-content: center;
    color: #c5d8fa;
    border-right: 1px solid rgba(70, 126, 255, 0.15);
    font-weight: 650;
}
.ws-watchboard-tab.is-active {
    color: #ff5469;
    background: linear-gradient(180deg, rgba(255, 63, 85, 0.12), rgba(255, 63, 85, 0.02));
    box-shadow: inset 0 -2px 0 #ff4055;
}
.ws-watchboard-fullscreen {
    min-width: 88px;
    color: #88adff;
    border-right: 0;
}
.ws-watchboard-chart-body {
    padding: 0.9rem 1rem 0.7rem 1rem;
}
.ws-watchboard-chart-title {
    display: flex;
    justify-content: space-between;
    gap: 0.8rem;
    color: var(--wb-text);
    font-weight: 750;
    margin-bottom: 0.45rem;
}
.ws-watchboard-chart-title span:last-child {
    color: #ff5469;
}
.ws-watchboard-svg {
    display: block;
    width: 100%;
    height: auto;
}
.ws-watchboard-linechart {
    position: relative;
    width: 100%;
    aspect-ratio: 820 / 320;
    min-height: 260px;
    overflow: hidden;
    border-radius: 8px;
}
.ws-watchboard-grid-h,
.ws-watchboard-grid-v {
    position: absolute;
    pointer-events: none;
    background: rgba(70, 126, 255, 0.14);
}
.ws-watchboard-grid-h {
    left: 6.4%;
    right: 2.2%;
    top: var(--grid-top);
    height: 1px;
}
.ws-watchboard-grid-v {
    top: 5.6%;
    bottom: 18%;
    left: var(--grid-left);
    width: 1px;
    opacity: 0.72;
}
.ws-watchboard-axis-label {
    position: absolute;
    left: 0;
    top: var(--label-top);
    transform: translateY(-50%);
    color: #8fa8ce;
    font-size: 0.78rem;
    line-height: 1;
}
.ws-watchboard-area {
    position: absolute;
    inset: 0;
    background: linear-gradient(180deg, color-mix(in srgb, var(--line-color), transparent 72%), transparent 74%);
    clip-path: var(--area-path);
    opacity: 0.9;
    pointer-events: none;
}
.ws-watchboard-segment {
    position: absolute;
    height: 3px;
    left: var(--x);
    top: var(--y);
    width: var(--len);
    transform: translateY(-50%) rotate(var(--angle));
    transform-origin: left center;
    border-radius: 999px;
    background: var(--line-color);
    box-shadow: 0 0 14px color-mix(in srgb, var(--line-color), transparent 30%);
}
.ws-watchboard-point {
    position: absolute;
    left: var(--x);
    top: var(--y);
    width: 9px;
    height: 9px;
    transform: translate(-50%, -50%);
    border-radius: 999px;
    background: var(--line-color);
    border: 1px solid #f8fbff;
    box-shadow: 0 0 16px color-mix(in srgb, var(--line-color), transparent 24%);
}
.ws-watchboard-volume {
    position: absolute;
    left: var(--x);
    bottom: 3.5%;
    width: var(--bar-width);
    height: var(--bar-height);
    transform: translateX(-50%);
    border-radius: 2px 2px 0 0;
    background: var(--bar-color);
    opacity: 0.55;
}
.ws-watchboard-x-label {
    position: absolute;
    bottom: 0.2rem;
    color: #d6e5ff;
    font-size: 0.82rem;
}
.ws-watchboard-x-label.is-start {
    left: 6.4%;
}
.ws-watchboard-x-label.is-end {
    right: 2.2%;
}
.ws-watchboard-footer-metrics {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    border-top: 1px solid rgba(70, 126, 255, 0.20);
}
.ws-watchboard-foot {
    padding: 0.7rem 0.75rem;
    border-right: 1px solid rgba(70, 126, 255, 0.14);
}
.ws-watchboard-foot:last-child {
    border-right: 0;
}
.ws-watchboard-foot label {
    display: block;
    color: var(--wb-muted);
    font-size: 0.78rem;
}
.ws-watchboard-foot strong {
    display: block;
    color: var(--wb-text);
    font-size: 0.96rem;
    margin-top: 0.12rem;
    white-space: normal;
    overflow-wrap: anywhere;
}
.ws-watchboard-side {
    display: grid;
    gap: 0.75rem;
}
.ws-watchboard-score {
    padding: 0.9rem 1rem;
    display: grid;
    grid-template-columns: 148px minmax(0, 1fr);
    gap: 0.9rem;
    align-items: center;
}
.ws-score-donut {
    width: 132px;
    aspect-ratio: 1;
    border-radius: 50%;
    display: grid;
    place-items: center;
    background:
        radial-gradient(circle at center, #051128 0 54%, transparent 55%),
        conic-gradient(var(--score-color) calc(var(--score) * 1%), #1d376b 0);
    box-shadow: 0 0 24px rgba(47, 123, 255, 0.24), inset 0 0 12px rgba(0,0,0,0.30);
    margin: 0 auto;
}
.ws-score-donut strong {
    color: var(--score-color);
    font-size: 2.75rem;
    line-height: 0.95;
    letter-spacing: 0;
}
.ws-score-donut span {
    display: block;
    color: #d8e6ff;
    font-size: 0.78rem;
    font-weight: 650;
    text-align: center;
}
.ws-watchboard-score-bars {
    display: grid;
    gap: 0.5rem;
}
.ws-watchboard-score-row {
    display: grid;
    grid-template-columns: 4.5em minmax(0, 1fr) 2.2em;
    align-items: center;
    gap: 0.45rem;
    color: #d9e7ff;
    font-size: 0.88rem;
}
.ws-score-track {
    height: 7px;
    border-radius: 999px;
    background: #122954;
    overflow: hidden;
}
.ws-score-fill {
    height: 100%;
    border-radius: inherit;
    background: linear-gradient(90deg, var(--bar-color), rgba(255,255,255,0.78));
    box-shadow: 0 0 12px color-mix(in srgb, var(--bar-color), transparent 38%);
}
.ws-watchboard-summary {
    padding: 1rem;
}
.ws-watchboard-summary h4 {
    display: flex;
    gap: 0.65rem;
    align-items: baseline;
    margin: 0 0 0.55rem 0;
    color: var(--wb-text);
    font-size: 1.12rem;
    line-height: 1.2;
}
.ws-watchboard-summary h4 strong {
    color: var(--wb-red);
    font-size: 1.45rem;
}
.ws-watchboard-summary .ws-beat {
    font-size: clamp(1.25rem, 2.4vw, 1.85rem);
    line-height: 1.25;
    color: var(--wb-text);
    font-weight: 780;
    margin: 0.3rem 0 0.45rem 0;
}
.ws-watchboard-summary .ws-beat strong {
    color: var(--wb-red);
    font-size: 1.15em;
}
.ws-watchboard-summary p {
    margin: 0.38rem 0 0 0;
    color: #c3d2ec;
    line-height: 1.62;
    font-size: 0.95rem;
}
.ws-watchboard-strip {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 0.65rem;
    margin-top: 0.75rem;
}
.ws-watchboard-mini {
    padding: 0.75rem 0.8rem;
    min-height: 102px;
}
.ws-watchboard-mini.is-active {
    border-color: rgba(255, 63, 85, 0.58);
    box-shadow: inset 0 0 22px rgba(255, 63, 85, 0.10);
}
.ws-watchboard-mini-top,
.ws-watchboard-mini-bottom {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.55rem;
}
.ws-watchboard-mini-name {
    color: var(--wb-text);
    font-weight: 760;
    min-width: 0;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.ws-watchboard-mini-code {
    color: var(--wb-muted);
    font-size: 0.76rem;
    white-space: nowrap;
}
.ws-watchboard-mini-price {
    color: var(--wb-text);
    font-size: 1.32rem;
    font-weight: 800;
    margin-top: 0.5rem;
}
.ws-watchboard-mini-delta {
    color: var(--wb-red);
    font-weight: 760;
    white-space: nowrap;
}
.ws-watchboard-mini-delta.ws-negative {
    color: var(--wb-green);
}
.ws-watchboard-mini-score {
    color: #98b5ed;
    font-size: 0.82rem;
}
.ws-watchboard-shell.is-compact {
    padding: 0.52rem;
    margin-top: 0.28rem;
}
.ws-watchboard-shell.is-compact::before {
    background-size: 38px 38px;
}
.ws-watchboard-compact-topbar {
    position: relative;
    z-index: 1;
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
    gap: 0.48rem;
    align-items: center;
    margin-bottom: 0.42rem;
}
.ws-watchboard-compact-title {
    min-width: 0;
}
.ws-watchboard-compact-title strong {
    display: block;
    color: var(--wb-text);
    font-size: clamp(0.95rem, 1.5vw, 1.16rem);
    line-height: 1.1;
}
.ws-watchboard-compact-title span {
    display: block;
    color: var(--wb-muted);
    font-size: 0.72rem;
    margin-top: 0.12rem;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.ws-watchboard-compact-meta {
    min-height: 30px;
    display: inline-flex;
    align-items: center;
    gap: 0.42rem;
    padding: 0.34rem 0.52rem;
    border-radius: 8px;
    border: 1px solid var(--wb-line-soft);
    background: rgba(3, 13, 33, 0.78);
    color: #c9dcff;
    font-size: 0.76rem;
    font-weight: 700;
    white-space: nowrap;
}
.ws-watchboard-summary-row {
    position: relative;
    z-index: 1;
    display: grid;
    grid-template-columns: repeat(5, minmax(0, 1fr));
    gap: 0.34rem;
    margin-bottom: 0.4rem;
}
.ws-watchboard-summary-pill {
    min-width: 0;
    padding: 0.36rem 0.44rem;
    border: 1px solid rgba(70, 126, 255, 0.22);
    border-radius: 8px;
    background: rgba(2, 9, 24, 0.52);
}
.ws-watchboard-summary-pill label {
    display: block;
    color: var(--wb-muted);
    font-size: 0.64rem;
    line-height: 1.1;
}
.ws-watchboard-summary-pill strong {
    display: block;
    margin-top: 0.1rem;
    color: var(--wb-text);
    font-size: 0.84rem;
    line-height: 1.18;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.ws-watchboard-all-grid {
    position: relative;
    z-index: 1;
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(124px, 1fr));
    gap: 0.34rem;
}
.ws-watchboard-stock-link {
    display: block;
    min-width: 0;
    color: inherit;
    text-decoration: none;
}
.ws-watchboard-stock-link:focus-visible {
    outline: 2px solid var(--wb-cyan);
    outline-offset: 2px;
    border-radius: 8px;
}
.ws-watchboard-stock-card {
    min-height: 104px;
    height: 100%;
    padding: 0.42rem 0.46rem;
    border: 1px solid rgba(70, 126, 255, 0.35);
    border-radius: 8px;
    background:
        linear-gradient(180deg, rgba(5, 17, 39, 0.93), rgba(2, 9, 24, 0.96)),
        radial-gradient(circle at 90% 0%, color-mix(in srgb, var(--accent), transparent 70%), transparent 36%);
    box-shadow: inset 0 0 16px rgba(47, 123, 255, 0.10);
    overflow: hidden;
    cursor: pointer;
    transition: border-color 140ms ease, transform 140ms ease, box-shadow 140ms ease;
}
.ws-watchboard-stock-link:hover .ws-watchboard-stock-card {
    border-color: color-mix(in srgb, var(--accent), white 22%);
    transform: translateY(-1px);
    box-shadow: inset 0 0 18px color-mix(in srgb, var(--accent), transparent 76%), 0 8px 18px rgba(2, 8, 24, 0.24);
}
.ws-watchboard-stock-card.is-active {
    border-color: color-mix(in srgb, var(--accent), white 12%);
    box-shadow: inset 0 0 20px color-mix(in srgb, var(--accent), transparent 74%), 0 0 0 1px rgba(255,255,255,0.03);
}
#watchlist-detail-card {
    scroll-margin-top: 16px;
}
.ws-watchboard-shell.is-detail {
    margin-top: 0.75rem;
}
.st-key-watchlist_card_grid {
    margin-top: 0.34rem;
    padding: 0.52rem;
    border: 1px solid rgba(70, 126, 255, 0.54);
    border-radius: 8px;
    background:
        radial-gradient(circle at 10% 0%, rgba(47, 123, 255, 0.26), transparent 28%),
        linear-gradient(180deg, #061128 0%, #020615 100%);
    box-shadow: 0 18px 48px rgba(4, 11, 30, 0.20), inset 0 0 28px rgba(47, 123, 255, 0.08);
}
.st-key-watchlist_card_grid div[class*="st-key-watchlist_card_wrap_"] {
    position: relative;
    min-height: 104px;
}
.st-key-watchlist_card_grid div[class*="st-key-watchlist_card_wrap_"] [data-testid="stHtml"] {
    pointer-events: none;
    margin-bottom: -104px;
    position: relative;
    z-index: 1;
}
.st-key-watchlist_card_grid div[class*="st-key-watchlist_card_wrap_"] [data-testid="stButton"] {
    position: relative;
    z-index: 5;
    margin: 0;
}
.st-key-watchlist_card_grid div[class*="st-key-watchlist_card_wrap_"] [data-testid="stButton"] > button {
    width: 100%;
    height: 104px;
    min-height: 104px;
    padding: 0;
    border: 0 !important;
    background: transparent !important;
    color: transparent !important;
    box-shadow: none !important;
    opacity: 0;
    cursor: pointer;
    font-size: 0 !important;
    line-height: 0 !important;
}
.st-key-watchlist_card_grid div[class*="st-key-watchlist_card_wrap_"] [data-testid="stButton"] > button p,
.st-key-watchlist_card_grid div[class*="st-key-watchlist_card_wrap_"] [data-testid="stButton"] > button span,
.st-key-watchlist_card_grid div[class*="st-key-watchlist_card_wrap_"] [data-testid="stButton"] > button div[data-testid="stMarkdownContainer"] {
    display: none !important;
}
.st-key-watchlist_card_grid div[class*="st-key-watchlist_card_wrap_"] [data-testid="stButton"] > button:hover,
.st-key-watchlist_card_grid div[class*="st-key-watchlist_card_wrap_"] [data-testid="stButton"] > button:focus {
    background: transparent !important;
    border: 0 !important;
    color: transparent !important;
    box-shadow: none !important;
    opacity: 0;
}
.st-key-watchlist_card_grid div[class*="st-key-watchlist_card_wrap_"] [data-testid="stCheckbox"] {
    position: relative;
    z-index: 6;
    margin-top: 0.35rem;
    padding: 0 0.15rem;
}
.st-key-watchlist_card_grid div[class*="st-key-watchlist_card_wrap_"] [data-testid="stCheckbox"] label {
    display: flex;
    align-items: center;
    gap: 0.35rem;
    color: #dce8ff;
    font-size: 0.78rem;
    font-weight: 700;
}
.st-key-watchlist_card_grid div[class*="st-key-watchlist_card_wrap_"] [data-testid="stCheckbox"] input {
    accent-color: #4f8cff;
}
.st-key-watchlist_card_grid div[class*="st-key-watchlist_card_wrap_"] [data-testid="stCheckbox"] > label {
    cursor: pointer;
}
.ws-watchboard-stock-head,
.ws-watchboard-stock-price-row,
.ws-watchboard-stock-foot {
    display: flex;
    justify-content: space-between;
    gap: 0.28rem;
    align-items: center;
}
.ws-watchboard-stock-name {
    min-width: 0;
    color: #f6fbff;
    font-size: 0.82rem;
    font-weight: 900;
    text-shadow: 0 1px 2px rgba(0, 0, 0, 0.68), 0 0 10px rgba(118, 198, 255, 0.24);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.ws-watchboard-stock-code {
    flex: 0 0 auto;
    color: #8ea8d2;
    font-size: 0.6rem;
}
.ws-watchboard-stock-price {
    color: var(--accent);
    font-size: clamp(1.04rem, 1.55vw, 1.34rem);
    line-height: 1;
    font-weight: 900;
    letter-spacing: 0;
    margin-top: 0.34rem;
}
.ws-watchboard-stock-ret {
    color: var(--accent);
    font-size: 0.72rem;
    font-weight: 800;
    white-space: nowrap;
}
.ws-watchboard-stock-metrics {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 0.2rem;
    margin-top: 0.36rem;
}
.ws-watchboard-stock-metric {
    min-width: 0;
}
.ws-watchboard-stock-metric label {
    display: block;
    color: var(--wb-muted);
    font-size: 0.56rem;
    line-height: 1;
}
.ws-watchboard-stock-metric strong {
    display: block;
    color: #edf5ff;
    font-size: 0.66rem;
    line-height: 1.15;
    margin-top: 0.08rem;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.ws-watchboard-stock-score {
    margin-top: 0.34rem;
    height: 4px;
    border-radius: 999px;
    background: #122954;
    overflow: hidden;
}
.ws-watchboard-stock-score span {
    display: block;
    height: 100%;
    width: var(--score);
    border-radius: inherit;
    background: linear-gradient(90deg, var(--accent), rgba(255,255,255,0.78));
}
.ws-watchboard-stock-foot {
    margin-top: 0.28rem;
    color: #9db8e6;
    font-size: 0.6rem;
    line-height: 1.1;
}
.ws-watchboard-stock-signal {
    min-width: 0;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
@media (max-width: 1180px) {
    .ws-watchboard-hero,
    .ws-watchboard-main {
        grid-template-columns: 1fr;
    }
    .ws-watchboard-strip {
        grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .ws-watchboard-summary-row {
        grid-template-columns: repeat(3, minmax(0, 1fr));
    }
}
@media (max-width: 760px) {
    .ws-watchboard-shell {
        padding: 0.75rem;
    }
    .ws-watchboard-topbar,
    .ws-watchboard-stats,
    .ws-watchboard-score,
    .ws-watchboard-footer-metrics,
    .ws-watchboard-strip {
        grid-template-columns: 1fr;
    }
    .ws-watchboard-clock,
    .ws-watchboard-chip {
        justify-content: flex-start;
    }
    .ws-watchboard-tabs {
        grid-template-columns: repeat(2, 1fr);
    }
    .ws-watchboard-fullscreen {
        min-width: 0;
        grid-column: span 2;
        border-top: 1px solid rgba(70, 126, 255, 0.15);
    }
    .ws-watchboard-price {
        grid-template-columns: 1fr;
    }
    .ws-watchboard-delta {
        text-align: left;
    }
    .ws-watchboard-compact-topbar,
    .ws-watchboard-summary-row {
        grid-template-columns: 1fr;
    }
    .ws-watchboard-compact-meta {
        justify-content: flex-start;
    }
    .ws-watchboard-all-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
    }
}
</style>
"""

FUND_WATCHLIST_DASHBOARD_CSS = """
<style>
.ws-fund-watchboard {
    --fw-bg:#030816;
    --fw-panel:rgba(5,17,39,.92);
    --fw-panel-strong:rgba(3,12,30,.97);
    --fw-line:rgba(70,126,255,.54);
    --fw-line-soft:rgba(70,126,255,.22);
    --fw-cyan:#22d7ff;
    --fw-blue:#2f7bff;
    --fw-red:#ff3f55;
    --fw-green:#20dfb8;
    --fw-text:#f5f9ff;
    --fw-muted:#93a9ca;
    box-sizing:border-box;
    position:relative;
    overflow:hidden;
    margin:.65rem 0 1rem;
    padding:1rem;
    color:var(--fw-text);
    border:1px solid var(--fw-line);
    border-radius:12px;
    background:
        radial-gradient(circle at 8% 0%,rgba(47,123,255,.28),transparent 32%),
        radial-gradient(circle at 90% 8%,rgba(34,215,255,.14),transparent 26%),
        linear-gradient(180deg,#061128 0%,var(--fw-bg) 100%);
    box-shadow:0 18px 48px rgba(4,11,30,.26),inset 0 0 34px rgba(47,123,255,.08);
}
.ws-fund-watchboard::before {
    content:"";
    position:absolute;
    inset:0;
    pointer-events:none;
    background-image:
        linear-gradient(rgba(64,125,255,.045) 1px,transparent 1px),
        linear-gradient(90deg,rgba(64,125,255,.045) 1px,transparent 1px);
    background-size:48px 48px;
    mask-image:linear-gradient(180deg,rgba(0,0,0,.72),rgba(0,0,0,.1));
}
.ws-fund-watchboard * { box-sizing:border-box; }
.ws-fund-watchboard__eyebrow {
    position:relative;
    z-index:1;
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:.8rem;
    margin-bottom:.8rem;
    color:var(--fw-muted);
    font-size:.78rem;
    letter-spacing:.04em;
}
.ws-fund-watchboard__eyebrow strong {
    color:var(--fw-cyan);
    font-size:.82rem;
    letter-spacing:.1em;
}
.ws-fund-watchboard__summary {
    position:relative;
    z-index:1;
    display:grid;
    grid-template-columns:repeat(4,minmax(0,1fr));
    gap:.72rem;
}
.ws-fund-watchboard__metric {
    min-width:0;
    padding:.86rem .9rem;
    border:1px solid var(--fw-line-soft);
    border-radius:10px;
    background:linear-gradient(145deg,rgba(9,28,61,.92),rgba(4,14,34,.9));
    box-shadow:inset 0 0 18px rgba(47,123,255,.07);
}
.ws-fund-watchboard__metric label {
    display:block;
    margin-bottom:.28rem;
    color:var(--fw-muted);
    font-size:.72rem;
}
.ws-fund-watchboard__metric strong {
    display:block;
    overflow:hidden;
    color:var(--fw-text);
    font-size:clamp(1.05rem,2vw,1.48rem);
    line-height:1.2;
    text-overflow:ellipsis;
    white-space:nowrap;
}
.ws-fund-watchboard__metric span {
    display:block;
    margin-top:.24rem;
    color:#6f8fbd;
    font-size:.66rem;
}
.ws-fund-watchboard__metric.is-accent strong { color:var(--fw-cyan); }
.ws-fund-watchboard__metric.is-change strong { color:var(--fw-green); }
.ws-fund-watchboard__cards {
    display:grid;
    grid-template-columns:repeat(3,minmax(0,1fr));
    gap:.72rem;
}
.ws-fund-watchboard__card {
    min-height:230px;
    padding:.92rem;
    border:1px solid var(--fw-line-soft);
    border-radius:10px;
    background:
        linear-gradient(145deg,rgba(9,29,64,.96),rgba(3,13,32,.98));
    box-shadow:inset 0 0 20px rgba(47,123,255,.08);
    transition:border-color 150ms ease,transform 150ms ease,box-shadow 150ms ease;
}
.ws-fund-watchboard__card:hover {
    transform:translateY(-2px);
    border-color:rgba(34,215,255,.7);
    box-shadow:inset 0 0 24px rgba(34,215,255,.08),0 12px 28px rgba(2,8,24,.3);
}
.ws-fund-watchboard__card.is-active {
    border-color:var(--fw-cyan);
    box-shadow:inset 0 0 26px rgba(34,215,255,.12),0 0 0 1px rgba(34,215,255,.16);
}
.ws-fund-watchboard__card-head {
    display:flex;
    align-items:flex-start;
    justify-content:space-between;
    gap:.7rem;
}
.ws-fund-watchboard__card-title {
    min-width:0;
}
.ws-fund-watchboard__card-title strong {
    display:block;
    overflow:hidden;
    color:var(--fw-text);
    font-size:.96rem;
    line-height:1.35;
    text-overflow:ellipsis;
    white-space:nowrap;
}
.ws-fund-watchboard__card-title span {
    color:var(--fw-muted);
    font-size:.69rem;
    letter-spacing:.04em;
}
.ws-fund-watchboard__badge {
    flex:none;
    max-width:42%;
    overflow:hidden;
    padding:.18rem .48rem;
    color:#cfe0ff;
    border:1px solid rgba(70,126,255,.3);
    border-radius:999px;
    background:rgba(47,123,255,.12);
    font-size:.64rem;
    text-overflow:ellipsis;
    white-space:nowrap;
}
.ws-fund-watchboard__ratio {
    margin:.82rem 0 .7rem;
    color:var(--fw-cyan);
    font-size:1.52rem;
    font-weight:900;
    line-height:1;
}
.ws-fund-watchboard__ratio small {
    display:block;
    margin-bottom:.28rem;
    color:var(--fw-muted);
    font-size:.66rem;
    font-weight:600;
}
.ws-fund-watchboard__ratio.is-low { color:var(--fw-green); }
.ws-fund-watchboard__ratio.is-high { color:var(--fw-red); }
.ws-fund-watchboard__card-metrics {
    display:grid;
    grid-template-columns:repeat(2,minmax(0,1fr));
    gap:.45rem;
}
.ws-fund-watchboard__card-metrics div {
    min-width:0;
    padding:.48rem .55rem;
    border:1px solid rgba(70,126,255,.15);
    border-radius:7px;
    background:rgba(3,12,30,.58);
}
.ws-fund-watchboard__card-metrics label,
.ws-fund-watchboard__changes label {
    display:block;
    color:var(--fw-muted);
    font-size:.61rem;
}
.ws-fund-watchboard__card-metrics strong {
    display:block;
    overflow:hidden;
    margin-top:.1rem;
    color:#e9f2ff;
    font-size:.77rem;
    text-overflow:ellipsis;
    white-space:nowrap;
}
.ws-fund-watchboard__changes {
    display:grid;
    grid-template-columns:repeat(3,minmax(0,1fr));
    gap:.38rem;
    margin-top:.55rem;
}
.ws-fund-watchboard__changes strong {
    display:block;
    margin-top:.06rem;
    color:#dce8ff;
    font-size:.76rem;
}
.ws-fund-watchboard__changes .is-positive strong { color:var(--fw-green); }
.ws-fund-watchboard__changes .is-negative strong { color:var(--fw-red); }
.ws-fund-watchboard__date {
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:.5rem;
    margin-top:.62rem;
    padding-top:.52rem;
    color:#7895bf;
    border-top:1px solid rgba(70,126,255,.13);
    font-size:.63rem;
}
.ws-fund-watchboard__date .is-error { color:#ff8392; }
.ws-fund-watchboard__focus {
    display:grid;
    grid-template-columns:minmax(240px,.85fr) minmax(0,1.7fr);
    gap:1rem;
    margin-top:1.1rem;
    padding:1rem;
    color:var(--fw-text);
    border:1px solid var(--fw-line);
    border-radius:12px;
    background:
        radial-gradient(circle at 0% 0%,rgba(47,123,255,.2),transparent 34%),
        linear-gradient(160deg,#061128,#020615);
    box-shadow:inset 0 0 30px rgba(47,123,255,.08);
}
.ws-fund-watchboard__focus-overview {
    padding:.25rem .35rem .25rem .15rem;
}
.ws-fund-watchboard__focus-kicker {
    color:var(--fw-cyan);
    font-size:.7rem;
    font-weight:800;
    letter-spacing:.12em;
}
.ws-fund-watchboard__focus h3 {
    margin:.35rem 0 .05rem;
    color:var(--fw-text);
    font-size:1.25rem;
}
.ws-fund-watchboard__focus-code {
    color:var(--fw-muted);
    font-size:.74rem;
}
.ws-fund-watchboard__focus-main {
    display:grid;
    grid-template-columns:116px minmax(0,1fr);
    align-items:center;
    gap:.9rem;
    margin-top:.85rem;
}
.ws-fund-watchboard__ring {
    display:grid;
    width:110px;
    height:110px;
    place-items:center;
    border-radius:50%;
    background:conic-gradient(var(--fw-cyan) var(--ratio),rgba(70,126,255,.16) 0);
    box-shadow:0 0 24px rgba(34,215,255,.14);
}
.ws-fund-watchboard__ring::before {
    content:"";
    grid-area:1/1;
    width:82px;
    height:82px;
    border-radius:50%;
    background:#061229;
    box-shadow:inset 0 0 16px rgba(47,123,255,.18);
}
.ws-fund-watchboard__ring span {
    z-index:1;
    grid-area:1/1;
    text-align:center;
    color:var(--fw-muted);
    font-size:.61rem;
}
.ws-fund-watchboard__ring strong {
    display:block;
    color:var(--fw-text);
    font-size:1.08rem;
}
.ws-fund-watchboard__facts {
    display:grid;
    gap:.38rem;
}
.ws-fund-watchboard__fact {
    display:flex;
    justify-content:space-between;
    gap:.8rem;
    padding-bottom:.32rem;
    border-bottom:1px solid rgba(70,126,255,.12);
    color:var(--fw-muted);
    font-size:.68rem;
}
.ws-fund-watchboard__fact strong {
    color:#e7f0ff;
    font-size:.7rem;
    text-align:right;
}
.ws-fund-watchboard__holdings {
    min-width:0;
    padding:.2rem 0 .1rem;
}
.ws-fund-watchboard__holdings-head {
    display:flex;
    align-items:flex-end;
    justify-content:space-between;
    gap:.8rem;
    margin-bottom:.65rem;
}
.ws-fund-watchboard__holdings-head strong {
    color:var(--fw-text);
    font-size:.92rem;
}
.ws-fund-watchboard__holdings-head span {
    color:var(--fw-muted);
    font-size:.65rem;
}
.ws-fund-watchboard__table-wrap {
    overflow-x:auto;
    border:1px solid rgba(70,126,255,.18);
    border-radius:8px;
}
.ws-fund-watchboard__holdings table {
    width:100%;
    min-width:560px;
    border-collapse:collapse;
    font-size:.68rem;
}
.ws-fund-watchboard__holdings th {
    padding:.56rem .6rem;
    color:#8fa9d0;
    background:rgba(47,123,255,.1);
    font-weight:700;
    text-align:left;
    white-space:nowrap;
}
.ws-fund-watchboard__holdings td {
    padding:.5rem .6rem;
    color:#dce8ff;
    border-top:1px solid rgba(70,126,255,.1);
    white-space:nowrap;
}
.ws-fund-watchboard__holdings td.is-positive { color:var(--fw-green); }
.ws-fund-watchboard__holdings td.is-negative { color:var(--fw-red); }
.ws-fund-watchboard__empty {
    display:grid;
    min-height:180px;
    place-items:center;
    color:var(--fw-muted);
    border:1px dashed rgba(70,126,255,.28);
    border-radius:8px;
    background:rgba(3,12,30,.5);
    text-align:center;
}
.ws-fund-watchboard__error {
    margin-top:.72rem;
    padding:.58rem .68rem;
    color:#ffb5be;
    border:1px solid rgba(255,63,85,.3);
    border-radius:7px;
    background:rgba(255,63,85,.08);
    font-size:.68rem;
}
.st-key-fund_watchlist_card_grid {
    margin-top:.35rem;
    padding:.72rem;
    border:1px solid rgba(70,126,255,.42);
    border-radius:12px;
    background:linear-gradient(180deg,rgba(6,17,40,.94),rgba(2,6,21,.96));
    box-shadow:inset 0 0 30px rgba(47,123,255,.07);
}
.st-key-fund_watchlist_card_grid div[class*="st-key-fund_watchlist_card_wrap_"] {
    position:relative;
    min-height:230px;
}
.st-key-fund_watchlist_card_grid div[class*="st-key-fund_watchlist_card_wrap_"] [data-testid="stHtml"] {
    position:relative;
    z-index:1;
    margin-bottom:-230px;
    pointer-events:none;
}
.st-key-fund_watchlist_card_grid div[class*="st-key-fund_watchlist_card_wrap_"] [data-testid="stButton"] {
    position:relative;
    z-index:5;
    margin:0;
}
.st-key-fund_watchlist_card_grid div[class*="st-key-fund_watchlist_card_wrap_"] [data-testid="stButton"] > button {
    width:100%;
    height:230px;
    min-height:230px;
    padding:0;
    border:0 !important;
    background:transparent !important;
    color:transparent !important;
    box-shadow:none !important;
    opacity:0;
    cursor:pointer;
    font-size:0 !important;
}
.st-key-fund_watchlist_card_grid div[class*="st-key-fund_watchlist_card_wrap_"] [data-testid="stCheckbox"] {
    position:relative;
    z-index:6;
    margin-top:.3rem;
}
.st-key-fund_watchlist_card_grid div[class*="st-key-fund_watchlist_card_wrap_"] [data-testid="stCheckbox"] label {
    color:#dce8ff;
    font-size:.76rem;
    font-weight:700;
}
.st-key-fund_watchlist_table_wrap {
    padding:.72rem;
    border:1px solid rgba(70,126,255,.35);
    border-radius:12px;
    background:linear-gradient(180deg,rgba(6,17,40,.94),rgba(2,6,21,.96));
}
.st-key-fund_watchlist_add_panel {
    margin:.75rem 0 1rem;
    padding:.85rem 1rem 1rem;
    border:1px solid rgba(70,126,255,.34);
    border-radius:12px;
    background:
        radial-gradient(circle at 96% 0%,rgba(34,215,255,.12),transparent 30%),
        linear-gradient(145deg,rgba(8,26,57,.94),rgba(3,12,30,.96));
    box-shadow:inset 0 0 24px rgba(47,123,255,.06);
}
.st-key-fund_watchlist_add_panel h4 {
    margin:.1rem 0 .15rem;
    color:#f5f9ff;
}
.st-key-fund_watchlist_add_panel p {
    color:#93a9ca;
}
@media (max-width:900px) {
    .ws-fund-watchboard__summary { grid-template-columns:repeat(2,minmax(0,1fr)); }
    .ws-fund-watchboard__cards { grid-template-columns:repeat(2,minmax(0,1fr)); }
    .ws-fund-watchboard__focus { grid-template-columns:1fr; }
}
@media (max-width:620px) {
    .ws-fund-watchboard { padding:.72rem; }
    .ws-fund-watchboard__cards { grid-template-columns:1fr; }
    .ws-fund-watchboard__focus { padding:.75rem; }
    .ws-fund-watchboard__focus-main { grid-template-columns:1fr; }
    .ws-fund-watchboard__ring { margin:0 auto; }
}
</style>
"""

TREND_RECO_FILE = "data/recommendations/latest_trend_recommendations.json"
LIVE_ML_RECO_SCORING_ENABLED = os.environ.get("ETF_ENABLE_LIVE_RECO_SCORING", "").strip().lower() in {"1", "true", "yes", "on"}


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


def get_logged_in_username() -> str:
    session_username = normalize_username(st.session_state.get("logged_in_username", ""))
    if session_username:
        return session_username
    return normalize_username(get_query_param_value("app_user").strip())


def is_user_logged_in() -> bool:
    return bool(get_logged_in_username())


def login_app_user(username: str) -> bool:
    normalized_username = normalize_username(username)
    st.session_state["logged_in_username"] = normalized_username
    try:
        if normalized_username:
            st.query_params["app_user"] = normalized_username
        elif "app_user" in st.query_params:
            del st.query_params["app_user"]
    except Exception:
        pass
    
    if normalized_username:
        try:
            engine = get_security_intraday_engine_cached()
            if engine is not None:
                preload_watchlist_reports_bg(normalized_username, engine)
        except Exception as e:
            logger.warning(f"Failed to start preload background task: {e}")
            
    return bool(normalized_username)


def logout_app_user() -> None:
    st.session_state["logged_in_username"] = ""
    try:
        if "app_user" in st.query_params:
            del st.query_params["app_user"]
    except Exception:
        pass


def render_user_login_status() -> None:
    current_username = get_logged_in_username()
    with st.expander("👤 用户登录", expanded=not bool(current_username)):
        if current_username:
            status_cols = st.columns([3, 1])
            status_cols[0].success(f"当前登录用户：{current_username}")
            if status_cols[1].button("退出登录", key="btn_user_logout"):
                logout_app_user()
                st.rerun()
            st.caption("当前版本为轻量登录：只需要用户名，不校验密码。")
        else:
            with st.form("app_user_login_form", clear_on_submit=False):
                username_input = st.text_input(
                    "用户名",
                    placeholder="输入用户名后登录",
                    key="app_login_username_input",
                )
                submitted = st.form_submit_button("登录", type="primary")
            if submitted:
                login_value = st.session_state.get("app_login_username_input", username_input)
                if login_app_user(login_value):
                    st.success(f"登录成功，欢迎你：{get_logged_in_username()}")
                    st.rerun()
                else:
                    st.error("用户名不能为空")
            st.caption("登录后可使用自选管理，并在个股查询页把股票加入自选。")


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


def build_opportunity_snapshot(
    top_up_df: pd.DataFrame,
    top_avoid_df: pd.DataFrame,
    moneyflow_df: pd.DataFrame,
    emotion_stage: str = "",
    trade_date_hint: str = "",
) -> pd.DataFrame:
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

    base_trade_date = ""
    for candidate in (base.get("trade_date"), base.get("date")):
        if candidate is None:
            continue
        values = pd.to_datetime(candidate, errors="coerce").dropna()
        if len(values) > 0:
            base_trade_date = values.max().strftime("%Y-%m-%d")
            break

    if not base_trade_date:
        base_trade_date = str(trade_date_hint or "").strip()

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

    candidate_codes = tuple(base["ts_code"].astype(str).tolist()) if "ts_code" in base.columns else ()
    for col_name, default_value in {
        "ml_new_prob_up_5d": np.nan,
        "ml_new_pred_ret_5d": np.nan,
        "ml_new_classifier": "-",
        "ml_new_regressor": "-",
    }.items():
        if col_name not in base.columns:
            base[col_name] = default_value

    ml_new_df = load_ml_prediction_candidate_scores(
        base_trade_date,
        candidate_codes=candidate_codes,
    ) if base_trade_date else pd.DataFrame()
    if ml_new_df is not None and not ml_new_df.empty and "ts_code" in ml_new_df.columns:
        merged_ml = ml_new_df[[c for c in ["ts_code", "ml_new_prob_up_5d", "ml_new_pred_ret_5d", "ml_new_classifier", "ml_new_regressor"] if c in ml_new_df.columns]].copy()
        merged_ml["ts_code"] = merged_ml["ts_code"].astype(str)
        merged_ml = merged_ml.rename(columns={
            "ml_new_prob_up_5d": "ml_new_prob_up_5d_snapshot",
            "ml_new_pred_ret_5d": "ml_new_pred_ret_5d_snapshot",
            "ml_new_classifier": "ml_new_classifier_snapshot",
            "ml_new_regressor": "ml_new_regressor_snapshot",
        })
        base = base.merge(merged_ml.drop_duplicates(subset=["ts_code"], keep="first"), on="ts_code", how="left")

        for live_col, snap_col in {
            "ml_new_prob_up_5d": "ml_new_prob_up_5d_snapshot",
            "ml_new_pred_ret_5d": "ml_new_pred_ret_5d_snapshot",
        }.items():
            if snap_col in base.columns:
                base[live_col] = pd.to_numeric(base.get(snap_col), errors="coerce").combine_first(
                    pd.to_numeric(base.get(live_col), errors="coerce")
                )
                base.drop(columns=[snap_col], inplace=True)

        for live_col, snap_col in {
            "ml_new_classifier": "ml_new_classifier_snapshot",
            "ml_new_regressor": "ml_new_regressor_snapshot",
        }.items():
            if snap_col in base.columns:
                snapshot_series = base.get(snap_col)
                current_series = base.get(live_col)
                if snapshot_series is not None:
                    snapshot_series = snapshot_series.astype(object)
                    snapshot_series = snapshot_series.where(snapshot_series.notna() & (snapshot_series.astype(str).str.strip() != ""), None)
                    if current_series is None:
                        base[live_col] = snapshot_series
                    else:
                        base[live_col] = snapshot_series.combine_first(current_series)
                base.drop(columns=[snap_col], inplace=True)

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
    base["ml_new_prob5_pct"] = pd.to_numeric(base.get("ml_new_prob_up_5d"), errors="coerce").fillna(0.0) * 100.0
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
    base.loc[pd.to_numeric(base.get("ml_new_prob_up_5d"), errors="coerce").fillna(0) >= 0.60, "model_bonus"] += 2.0
    base.loc[pd.to_numeric(base.get("ml_new_pred_ret_5d"), errors="coerce").fillna(0) >= 0.03, "model_bonus"] += 1.0

    base["opportunity_score"] = (
        base["trend_score"].fillna(0.0) * trend_weight
        + base["hybrid_prob5_pct"] * prob_weight
        + base["ml_prob5_pct"] * ml_weight
        + base["ml_new_prob5_pct"] * 0.10
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
def load_security_kline_timeseries(ts_code: str, security_type: str) -> pd.DataFrame:
    return get_security_kline_timeseries(ts_code=ts_code, security_type=security_type)


@st.cache_data(ttl=300)
def load_stock_holder_number_timeseries(ts_code: str) -> pd.DataFrame:
    return get_stock_holder_number_timeseries(ts_code=ts_code)


@st.cache_data(ttl=300)
def load_stock_basic_summary_export() -> pd.DataFrame:
    return get_stock_basic_summary()


@st.cache_data(ttl=600, show_spinner=False)
def load_factor_workbench_trade_dates_cached() -> list[pd.Timestamp]:
    return get_factor_workbench_trade_dates()


@st.cache_data(ttl=600, show_spinner=False)
def load_factor_workbench_frame_cached(trade_date_text: str) -> pd.DataFrame:
    return load_factor_workbench_frame(trade_date_text)


@st.cache_data(ttl=600, show_spinner=False)
def load_factor_workbench_data_freshness_cached() -> dict[str, str | None]:
    return get_factor_workbench_data_freshness()


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


@st.cache_data(ttl=300, show_spinner=False)
def load_security_fund_holding_detail(symbol: str, period: str, fund_type_filter: str = "全部") -> pd.DataFrame:
    from src.fund_hot_stocks import query_stock_fund_holding_detail

    ts_code = str(symbol or "").strip().upper()
    period_norm = str(period or "").replace("-", "").strip()
    normalized_fund_type = None if str(fund_type_filter or "").strip() in {"", "全部"} else fund_type_filter
    if not ts_code:
        return pd.DataFrame()

    return query_stock_fund_holding_detail(
        symbol=ts_code,
        period=period_norm or None,
        top_n=300,
        fund_type_filter=normalized_fund_type,
    )


@st.cache_resource
def get_trend_reco_engine_cached():
    try:
        return get_trend_reco_engine()
    except Exception as exc:
        logger.warning(f"get_trend_reco_engine_cached failed: {exc}")
        return None


@st.cache_resource
def get_security_intraday_engine_cached():
    try:
        return get_security_intraday_engine()
    except Exception as exc:
        logger.warning(f"get_security_intraday_engine_cached failed: {exc}")
        return None


def _event_payload_get(obj, key: str, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    try:
        value = getattr(obj, key)
        return default if value is None else value
    except Exception:
        pass
    try:
        return obj[key]
    except Exception:
        return default



def extract_trade_date_from_plotly_event(event, fallback_dates: list[str] | None = None) -> str:
    if event is None:
        return ""

    fallback_dates = [str(item).strip() for item in (fallback_dates or []) if str(item).strip()]

    if isinstance(event, list):
        points = event or []
        point_indices = []
    else:
        selection = _event_payload_get(event, "selection")
        payload = selection if selection is not None else event
        points = _event_payload_get(payload, "points", []) or []
        point_indices = _event_payload_get(payload, "point_indices", []) or []

    def _parse_candidate(candidate) -> str:
        if candidate is None:
            return ""
        parsed = pd.to_datetime(candidate, errors="coerce")
        if pd.notna(parsed):
            return parsed.strftime("%Y-%m-%d")
        return ""

    for point in reversed(list(points)):
        trace_name = str(_event_payload_get(point, "dataName") or _event_payload_get(point, "fullDataName") or _event_payload_get(point, "curveName") or "").strip()
        if trace_name and trace_name not in {"K线", "点击查看分时", "当前选中日K", "当前选中日K影线"}:
            continue

        customdata = _event_payload_get(point, "customdata")
        if customdata is not None and not isinstance(customdata, (list, tuple, np.ndarray, pd.Series)):
            customdata = [customdata]
        if customdata is not None:
            for candidate in list(customdata):
                parsed = _parse_candidate(candidate)
                if parsed:
                    return parsed

        for key in ("x", "label", "value", "text"):
            parsed = _parse_candidate(_event_payload_get(point, key))
            if parsed:
                return parsed

        for key in ("point_index", "point_number", "pointIndex", "pointNumber"):
            point_idx = _event_payload_get(point, key)
            try:
                point_idx = int(point_idx)
            except Exception:
                continue
            if 0 <= point_idx < len(fallback_dates):
                parsed = _parse_candidate(fallback_dates[point_idx])
                if parsed:
                    return parsed

    for point_idx in reversed(list(point_indices)):
        try:
            point_idx = int(point_idx)
        except Exception:
            continue
        if 0 <= point_idx < len(fallback_dates):
            parsed = _parse_candidate(fallback_dates[point_idx])
            if parsed:
                return parsed

    return ""


def load_security_intraday_timeseries(ts_code: str, trade_date: str, freq: str = "1min") -> tuple[pd.DataFrame, str, str]:
    empty_df = pd.DataFrame(columns=["trade_time", "open", "high", "low", "close", "vol", "amount"])
    try:
        engine = get_security_intraday_engine_cached()
        if engine is None:
            return empty_df, "error", "数据库引擎不可用"
        df, source = load_or_fetch_stock_intraday_timeseries(
            ts_code=ts_code,
            trade_date=trade_date,
            freq=freq,
            engine=engine,
        )
        return df, source, ""
    except Exception as exc:
        logger.warning(f"load_security_intraday_timeseries failed for {ts_code} {trade_date}: {exc}")
        return empty_df, "error", str(exc)


@st.cache_data(ttl=1800, show_spinner=False)
def load_ml_prediction_candidate_scores(
    trade_date: str = "",
    *,
    candidate_codes: tuple[str, ...] = (),
    lookback_days: int = 120,
    min_train_rows: int = 5000,
    max_candidates: int = 200,
    recent_train_rows: int = 12000,
    classification_model_kind: str = "sklearn",
    regression_model_kind: str = "sklearn",
    classifier: str = "logistic",
    regressor: str = "ridge",
) -> pd.DataFrame:
    trade_date_text = str(trade_date or "").strip()
    normalized_candidate_codes = tuple(
        str(code).strip().upper()
        for code in (candidate_codes or ())
        if str(code).strip()
    )
    if not trade_date_text:
        return pd.DataFrame()

    snapshot_df = load_candidate_scores_from_snapshot(
        trade_date_text,
        candidate_codes=normalized_candidate_codes,
    )
    if snapshot_df is not None and not snapshot_df.empty:
        logger.info(
            "load_ml_prediction_candidate_scores loaded snapshot rows=%s trade_date=%s",
            len(snapshot_df),
            trade_date_text,
        )
        return snapshot_df

    if not LIVE_ML_RECO_SCORING_ENABLED:
        logger.info("load_ml_prediction_candidate_scores skipped: live scoring disabled and no snapshot matched")
        return pd.DataFrame()

    try:
        return compute_ml_reco_candidate_scores(
            trade_date_text,
            candidate_codes=normalized_candidate_codes,
            lookback_days=lookback_days,
            min_train_rows=min_train_rows,
            max_candidates=max_candidates,
            recent_train_rows=recent_train_rows,
            classification_model_kind=classification_model_kind,
            regression_model_kind=regression_model_kind,
            classifier=classifier,
            regressor=regressor,
        )
    except Exception as exc:
        logger.warning(f"load_ml_prediction_candidate_scores failed for {trade_date_text}: {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_trend_recommendations_from_path(file_path: str) -> dict:
    try:
        if not file_path or not os.path.exists(file_path):
            return {}
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as exc:
        logger.warning(f"load_trend_recommendations_from_path failed for {file_path}: {exc}")
        return {}


@st.cache_data(ttl=300)
def load_trend_recommendations_from_db(trade_date: str = "") -> dict:
    try:
        engine = get_trend_reco_engine_cached()
        if engine is None:
            return {}
        return fetch_trend_reco_payload(engine, trade_date=trade_date or None) or {}
    except Exception as exc:
        logger.warning(f"load_trend_recommendations_from_db failed for {trade_date or 'latest'}: {exc}")
        return {}


@st.cache_data(ttl=300)
def load_trend_recommendations_snapshot(snapshot: dict | None) -> dict:
    snapshot = snapshot or {}
    trade_date = str(snapshot.get("trade_date") or "").strip()
    source = str(snapshot.get("source") or "").strip().lower()
    path = str(snapshot.get("path") or "").strip()

    if source in {"db", "latest"} or trade_date:
        payload = load_trend_recommendations_from_db(trade_date)
        if payload:
            return payload

    if path:
        return load_trend_recommendations_from_path(path)
    return {}


@st.cache_data(ttl=300)
def load_trend_recommendations() -> dict:
    payload = load_trend_recommendations_from_db("")
    if payload:
        return payload
    return load_trend_recommendations_from_path(TREND_RECO_FILE)


@st.cache_data(ttl=300)
def list_trend_recommendation_snapshots() -> list[dict]:
    entries_by_date = {}

    try:
        engine = get_trend_reco_engine_cached()
        if engine is not None:
            db_entries = list_trend_reco_runs(engine, limit=180)
            for idx, item in enumerate(db_entries):
                trade_date = str(item.get("trade_date") or "").strip()
                if not trade_date:
                    continue
                entries_by_date[trade_date] = {
                    "trade_date": trade_date,
                    "path": str(item.get("source_file") or "").strip(),
                    "source": "latest" if idx == 0 else "db",
                    "generated_at": str(item.get("generated_at") or "").strip(),
                    "universe_size": int(item.get("universe_size") or 0),
                }
    except Exception as exc:
        logger.warning(f"list_trend_recommendation_snapshots db load failed: {exc}")

    latest_payload = load_trend_recommendations()
    latest_trade_date = str(latest_payload.get("trade_date") or "").strip()
    if latest_payload:
        latest_key = latest_trade_date or "latest"
        existing = entries_by_date.get(latest_key, {})
        entries_by_date[latest_key] = {
            "trade_date": latest_trade_date or existing.get("trade_date") or "最新",
            "path": existing.get("path") or TREND_RECO_FILE,
            "source": "latest",
            "generated_at": str(latest_payload.get("generated_at") or existing.get("generated_at") or "").strip(),
            "universe_size": int(latest_payload.get("universe_size") or existing.get("universe_size") or 0),
        }

    base_dir = os.path.dirname(TREND_RECO_FILE) or "."
    if os.path.isdir(base_dir):
        try:
            for name in os.listdir(base_dir):
                if not str(name).endswith("_trend_recommendations.json"):
                    continue
                file_path = os.path.join(base_dir, name)
                parsed_date = _parse_reco_date_from_filename(name)
                payload = load_trend_recommendations_from_path(file_path)
                trade_date = str(payload.get("trade_date") or "").strip()
                if not trade_date and parsed_date is not None:
                    trade_date = parsed_date.strftime("%Y-%m-%d")
                if not trade_date:
                    continue
                if trade_date not in entries_by_date:
                    entries_by_date[trade_date] = {
                        "trade_date": trade_date,
                        "path": file_path,
                        "source": "archive",
                        "generated_at": str(payload.get("generated_at") or "").strip(),
                        "universe_size": int(payload.get("universe_size") or 0),
                    }
                elif not entries_by_date[trade_date].get("path"):
                    entries_by_date[trade_date]["path"] = file_path
        except Exception as exc:
            logger.warning(f"list_trend_recommendation_snapshots failed: {exc}")

    entries = list(entries_by_date.values())

    def _sort_key(item: dict):
        td = pd.to_datetime(item.get("trade_date"), errors="coerce")
        if pd.notna(td):
            return td
        return pd.Timestamp.min

    entries.sort(key=_sort_key, reverse=True)
    return entries




@st.cache_data(ttl=900)
def load_trend_reco_history_payloads(max_files: int = 30) -> list[dict]:
    payloads: list[dict] = []
    limit = int(max_files) if max_files else None

    try:
        engine = get_trend_reco_engine_cached()
        if engine is not None:
            payloads = list_trend_reco_payloads(engine, limit=limit)
    except Exception as exc:
        logger.warning(f"load_trend_reco_history_payloads db load failed: {exc}")
        payloads = []

    if not payloads:
        base_dir = os.path.dirname(TREND_RECO_FILE) or "."
        if not os.path.isdir(base_dir):
            return []

        entries = []
        try:
            for name in os.listdir(base_dir):
                if not str(name).endswith("_trend_recommendations.json"):
                    continue
                d = _parse_reco_date_from_filename(name)
                if d is None:
                    continue
                entries.append((d, os.path.join(base_dir, name)))
        except Exception as exc:
            logger.warning(f"load_trend_reco_history_payloads file scan failed: {exc}")
            return []

        if not entries:
            return []

        entries.sort(key=lambda x: x[0])
        if limit and len(entries) > limit:
            entries = entries[-limit:]

        for _, fp in entries:
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                if payload:
                    payloads.append(payload)
            except Exception as exc:
                logger.warning(f"load_trend_reco_history_payloads skip {fp}: {exc}")

    payloads = [p for p in payloads if isinstance(p, dict) and str(p.get("trade_date") or "").strip()]
    payloads.sort(key=lambda p: pd.to_datetime(p.get("trade_date"), errors="coerce"))
    return payloads



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
    payloads = load_trend_reco_history_payloads(max_files=max_files)
    if not payloads:
        return pd.DataFrame()

    rows = []
    symbol_cache = {}
    for payload in payloads:
        try:
            rows.append(_evaluate_reco_payload(payload, symbol_cache, topn_limit=topn_limit))
        except Exception as exc:
            logger.warning(f"load_reco_effectiveness_history skip {payload.get('trade_date')}: {exc}")

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
    payloads = load_trend_reco_history_payloads(max_files=max_files)
    if not payloads:
        return pd.DataFrame()

    rows = []
    symbol_cache = {}
    for payload in payloads:
        try:
            rows.append(_evaluate_strategy_compare(payload, symbol_cache, topn_limit=topn_limit))
        except Exception as exc:
            logger.warning(f"load_reco_strategy_comparison skip {payload.get('trade_date')}: {exc}")

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
        ("rule_ret_5d", "纯规则", THEME_NAVY),
        ("model_ret_5d", "纯模型", THEME_WARN),
        ("hybrid_ret_5d", "混合模型", THEME_DOWN),
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
        apply_time_series_hover_affordance(
            daily_fig,
            date_x,
            [plot_df[label] for _key, label, _color in strategy_meta],
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
        apply_time_series_hover_affordance(
            cum_fig,
            date_x,
            [plot_df[f"{label}_cum"] for _key, label, _color in strategy_meta],
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



def get_deposit_edit_password() -> str:
    secret_password = ""
    try:
        secret_password = st.secrets.get("deposit_edit_password", "")
        if not secret_password:
            secret_password = st.secrets.get("app", {}).get("deposit_edit_password", "")
    except Exception:
        secret_password = ""

    return str(
        secret_password
        or os.getenv("ETF_DEPOSIT_EDIT_PASSWORD")
        or os.getenv("ETF_EDIT_PASSWORD")
        or ""
    ).strip()



def has_deposit_edit_permission() -> bool:
    return bool(get_deposit_edit_password()) and bool(
        st.session_state.get("deposit_edit_authorized", False)
    )



def grant_deposit_edit_permission(password: str) -> bool:
    expected_password = get_deposit_edit_password()
    if not expected_password:
        st.session_state["deposit_edit_authorized"] = False
        return False

    is_authorized = compare_digest(password or "", expected_password)
    st.session_state["deposit_edit_authorized"] = is_authorized
    return is_authorized



def clear_deposit_edit_permission() -> None:
    st.session_state["deposit_edit_authorized"] = False
    st.session_state["deposit_manual_open"] = False
    st.session_state["deposit_import_open"] = False
    st.session_state["deposit_edit_month"] = ""



def get_index_monitor_edit_password() -> str:
    secret_password = ""
    try:
        secret_password = st.secrets.get("index_monitor_edit_password", "")
        if not secret_password:
            secret_password = st.secrets.get("app", {}).get("index_monitor_edit_password", "")
    except Exception:
        secret_password = ""

    return str(
        secret_password
        or os.getenv("ETF_INDEX_MONITOR_EDIT_PASSWORD")
        or os.getenv("ETF_EDIT_PASSWORD")
        or ""
    ).strip()



def has_index_monitor_edit_permission() -> bool:
    return bool(get_index_monitor_edit_password()) and bool(
        st.session_state.get("index_monitor_edit_authorized", False)
    )



def grant_index_monitor_edit_permission(password: str) -> bool:
    expected_password = get_index_monitor_edit_password()
    if not expected_password:
        st.session_state["index_monitor_edit_authorized"] = False
        return False

    is_authorized = compare_digest(password or "", expected_password)
    st.session_state["index_monitor_edit_authorized"] = is_authorized
    return is_authorized



def clear_index_monitor_edit_permission() -> None:
    st.session_state["index_monitor_edit_authorized"] = False
    st.session_state["index_manual_month_open"] = False
    st.session_state["index_single_edit_open"] = False
    st.session_state["index_import_open"] = False



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
        # 方案B：通过 sidebar 一级导航 + 股票子导航完成跳转
        st.session_state["sidebar_nav_group"] = "股票"
        st.session_state["sidebar_expanded_module_id"] = "stock"
        st.session_state["stock_subpage"] = STOCK_SECURITY_SEARCH_LABEL
        st.session_state["jump_to_security_tab"] = True

    if jump_nonce:
        st.session_state["last_consumed_jump_nonce"] = jump_nonce


def trigger_security_tab_jump_if_needed() -> None:
    """若存在跳转请求，切到 sidebar 的“股票 -> 个股/指数查询”。"""
    if not st.session_state.get("jump_to_security_tab", False):
        return

    st.session_state["sidebar_nav_group"] = "股票"
    st.session_state["sidebar_expanded_module_id"] = "stock"
    st.session_state["stock_subpage"] = STOCK_SECURITY_SEARCH_LABEL
    st.session_state["jump_to_security_tab"] = False


def _build_sidebar_element_key(base_key: str, *suffixes: str) -> str:
    key = base_key
    for suffix in suffixes:
        if suffix:
            key = f"{key}-{suffix}"
    return key


def _consume_pending_sidebar_search_reset() -> None:
    if st.session_state.pop("sidebar_search_query_pending_reset", False):
        st.session_state["sidebar_search_query"] = ""


def _resolve_desktop_sidebar_selection():
    module_labels = get_module_labels()
    selected_module_label = st.session_state.get("sidebar_nav_group")
    if selected_module_label not in module_labels:
        selected_module_label = module_labels[0]
        st.session_state["sidebar_nav_group"] = selected_module_label

    selected_module = get_module_by_label(selected_module_label)
    page_labels = get_page_labels(selected_module.label)
    selected_page_label = st.session_state.get(selected_module.session_key)
    if selected_page_label not in page_labels:
        selected_page_label = page_labels[0]
        st.session_state[selected_module.session_key] = selected_page_label

    selected_page = next(
        page for page in selected_module.pages if page.label == selected_page_label
    )
    return selected_module, selected_page


def _navigate_desktop_sidebar_to(
    module_id: str,
    page_id: str,
    *,
    clear_search: bool = False,
) -> None:
    module = get_module_by_id(module_id)
    page = get_page_by_id(page_id)
    if page not in module.pages:
        raise KeyError(f"Unknown page {page_id!r} for module {module_id!r}")

    st.session_state["sidebar_nav_group"] = module.label
    st.session_state[module.session_key] = page.label
    st.session_state["sidebar_expanded_module_id"] = module.id
    if clear_search:
        st.session_state["sidebar_search_query_pending_reset"] = True


def queue_fund_watchlist_navigation() -> None:
    """Queue a safe cross-module jump before navigation widgets are created."""
    st.session_state["pending_fund_watchlist_navigation"] = True


def consume_pending_fund_watchlist_navigation() -> None:
    if not st.session_state.pop("pending_fund_watchlist_navigation", False):
        return

    _navigate_desktop_sidebar_to("fund", "fund_watchlist")
    st.session_state["iphone_group_radio"] = "基金"
    st.session_state["iphone_page_etf"] = ETF_FUND_WATCHLIST_PAGE_LABEL


def render_desktop_sidebar_navigation() -> tuple[str, str]:
    selected_module, selected_page = _resolve_desktop_sidebar_selection()
    expanded_module_id = resolve_expanded_module_id(
        selected_page.id,
        st.session_state.get("sidebar_expanded_module_id"),
    )
    st.session_state["sidebar_expanded_module_id"] = expanded_module_id
    record_recent_visit(st.session_state, selected_module.id, selected_page.id)
    recent_visits = get_recent_visits(st.session_state)

    st.sidebar.markdown(
        """
        <div class="ws-sidebar-brand">
            <span class="ws-sidebar-brand-kicker">WealthSpark</span>
            <h2>桌面导航</h2>
            <p>通过搜索、模块树和最近访问在桌面端快速切换页面。</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar:
        _consume_pending_sidebar_search_reset()
        st.markdown(
            """
            <div class="ws-sidebar-block">
                <div class="ws-sidebar-block-title">搜索与导航</div>
                <p class="ws-sidebar-block-copy">搜索页面，或在模块树中展开当前工作区。</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        search_query = st.text_input(
            "搜索页面",
            key="sidebar_search_query",
            placeholder="搜索模块、页面或描述…",
            label_visibility="collapsed",
        ).strip()

        with st.container(key="ws-sidebar-tree"):
            if search_query:
                search_results = search_sidebar_pages(search_query)
                if search_results:
                    for result_index, result in enumerate(search_results):
                        if st.button(
                            f"{result.module_label} / {result.page_label}",
                            key=f"ws-sidebar-search-result-{result.page_id}-{result_index}",
                            use_container_width=True,
                        ):
                            _navigate_desktop_sidebar_to(
                                result.module_id,
                                result.page_id,
                                clear_search=True,
                            )
                            st.rerun()
                        st.markdown(
                            (
                                '<span class="ws-sidebar-search-result-meta">'
                                f'{escape(result.module_label)} · {escape(result.description)}'
                                "</span>"
                            ),
                            unsafe_allow_html=True,
                        )
                else:
                    st.markdown(
                        '<span class="ws-sidebar-empty">未找到匹配页面，请尝试模块名、页面名或描述。</span>',
                        unsafe_allow_html=True,
                    )
            else:
                for module in SIDEBAR_MODULES:
                    is_current_module = module.id == selected_module.id
                    is_expanded_module = module.id == expanded_module_id
                    module_key = _build_sidebar_element_key(
                        f"ws-sidebar-module-{module.id}",
                        "current" if is_current_module else "",
                        "expanded" if is_expanded_module else "",
                    )
                    if st.button(
                        f'{"▾" if is_expanded_module else "▸"} {module.label}',
                        key=module_key,
                        use_container_width=True,
                    ):
                        st.session_state["sidebar_expanded_module_id"] = module.id
                        st.rerun()

                    if not is_expanded_module:
                        continue

                    for page in module.pages:
                        is_active_page = (
                            module.id == selected_module.id and page.id == selected_page.id
                        )
                        page_key = _build_sidebar_element_key(
                            f"ws-sidebar-page-{page.id}",
                            "active" if is_active_page else "",
                            "current" if is_active_page else "",
                        )
                        if st.button(
                            page.label,
                            key=page_key,
                            use_container_width=True,
                        ):
                            _navigate_desktop_sidebar_to(module.id, page.id)
                            st.rerun()
                        if is_active_page:
                            st.markdown(
                                (
                                    '<span class="ws-sidebar-page-description">'
                                    f"{escape(page.description)}"
                                    "</span>"
                                ),
                                unsafe_allow_html=True,
                            )

        st.markdown(
            """
            <div class="ws-sidebar-block">
                <div class="ws-sidebar-block-title">最近访问</div>
                <p class="ws-sidebar-block-copy">保留最近浏览页面，作为次级快捷入口。</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if recent_visits:
            for recent_index, recent_item in enumerate(recent_visits):
                if st.button(
                    f'{recent_item["module_label"]} / {recent_item["page_label"]}',
                    key=f'ws-sidebar-recent-link-{recent_item["page_id"]}-{recent_index}',
                    use_container_width=True,
                ):
                    _navigate_desktop_sidebar_to(
                        recent_item["module_id"],
                        recent_item["page_id"],
                        clear_search=True,
                    )
                    st.rerun()
        else:
            st.markdown(
                '<span class="ws-sidebar-empty">最近访问会显示在这里。</span>',
                unsafe_allow_html=True,
            )

    return selected_module.label, selected_page.label

def render_tech_picker_jump_table(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return

    display_df = df.rename(columns={
        'ts_code': '代码', 'name': '简称', 'industry': '行业',
        'trade_date': '满足日期',
        'w_ema5': '周线EMA5', 'w_ema30': '周线EMA30',
        'm_ema5': '月线EMA5', 'm_ema30': '月线EMA30',
        'main_business': '主要业务'
    }).copy()

    if 'has_ever_st' in display_df.columns:
        display_df['标签'] = display_df['has_ever_st'].map(lambda x: '曾经ST' if bool(x) else '')
        display_df = display_df.drop(columns=['has_ever_st'])

    for col in ['周线EMA5', '周线EMA30', '月线EMA5', '月线EMA30']:
        if col in display_df.columns:
            display_df[col] = pd.to_numeric(display_df[col], errors='coerce').map(
                lambda x: '-' if pd.isna(x) else f"{x:,.2f}"
            )

    if '满足日期' in display_df.columns:
        display_df['满足日期'] = pd.to_datetime(display_df['满足日期'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('-')

    display_df = display_df.fillna('-')

    render_security_jump_table(
        display_df,
        help_text="💡 直接点击每行最左侧“🔎 查询”即可跳到“个股/指数查询”，并自动带入该股票代码。",
        code_col='代码',
        fallback_col='简称',
        nonce_key='tech_picker_render_nonce',
    )


def build_security_jump_links(df: pd.DataFrame, code_col: str = '代码', fallback_col: str = '简称', nonce_key: str = 'security_jump_render_nonce') -> list[str]:
    from urllib.parse import quote

    if df is None or df.empty:
        return []

    render_nonce = st.session_state.get(nonce_key, 0) + 1
    st.session_state[nonce_key] = render_nonce

    query_links: list[str] = []
    for _, row in df.iterrows():
        query = str(row.get(code_col) or row.get(fallback_col) or '').strip()
        if not query:
            query_links.append('#')
            continue
        query_links.append(
            f"?security_query={quote(query)}&security_type=stock&open_tab=security&jump_nonce={render_nonce}_{quote(query)}"
        )
    return query_links


def build_security_name_jump_links(
    df: pd.DataFrame,
    code_col: str = '代码',
    label_col: str = '名称',
    fallback_col: str | None = None,
    label_prefix: str = '',
    nonce_key: str = 'security_name_jump_render_nonce',
) -> list[str]:
    from urllib.parse import quote

    if df is None or df.empty:
        return []

    render_nonce = st.session_state.get(nonce_key, 0) + 1
    st.session_state[nonce_key] = render_nonce

    query_links: list[str] = []
    for _, row in df.iterrows():
        fallback_value = row.get(fallback_col) if fallback_col else ''
        query = str(row.get(code_col) or fallback_value or '').strip()
        if not query:
            query_links.append('#')
            continue

        label = str(row.get(label_col) or query).strip() or query
        if label_prefix:
            label = f"{label_prefix}{label}"
        query_links.append(
            f"?security_query={quote(query)}&security_type=stock&open_tab=security&jump_nonce={render_nonce}_{quote(query)}#{label}"
        )
    return query_links


def build_hotmoney_stock_preference_display_df(df_stocks: pd.DataFrame) -> pd.DataFrame:
    if df_stocks is None or df_stocks.empty:
        return pd.DataFrame(columns=["股票名称", "代码", "上榜次数", "游资数", "净买卖(亿)"])

    out = df_stocks[["ts_name", "ts_code", "hit_count", "hm_count", "total_net_amount_yi"]].copy()
    out.columns = ["股票名称", "代码", "上榜次数", "游资数", "净买卖(亿)"]
    out["股票名称"] = build_security_name_jump_links(
        out,
        code_col="代码",
        label_col="股票名称",
        fallback_col="股票名称",
        label_prefix="🔎 ",
        nonce_key="hm_stock_preference_render_nonce",
    )
    out["净买卖(亿)"] = out["净买卖(亿)"].map(lambda v: f"{v:,.2f}")
    return out


def _format_hotmoney_yi(value, signed: bool = False) -> str:
    if value is None or pd.isna(value):
        return "-"
    try:
        num = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{num:+,.2f}" if signed else f"{num:,.2f}"


def _join_hotmoney_names(values, max_items: int = 4) -> str:
    names: list[str] = []
    for value in values:
        name = str(value or "").strip()
        if not name:
            continue
        if name not in names:
            names.append(name)

    if not names:
        return "-"
    label = "、".join(names[:max_items])
    if len(names) > max_items:
        label = f"{label} 等{len(names)}路"
    return label


def prepare_hotmoney_detail_frame(df_detail: pd.DataFrame) -> pd.DataFrame:
    if df_detail is None or df_detail.empty:
        return pd.DataFrame()

    work = df_detail.copy()
    work["trade_date"] = pd.to_datetime(work["trade_date"], errors="coerce")
    for col in ["buy_amount", "sell_amount", "net_amount"]:
        work[col] = pd.to_numeric(work[col], errors="coerce").fillna(0)
        work[f"{col}_yi"] = work[col] / 1e8

    for col in ["ts_code", "ts_name", "hm_name", "hm_orgs", "tag"]:
        if col in work.columns:
            work[col] = work[col].fillna("").astype(str)

    work["ts_name"] = work.apply(
        lambda row: row["ts_name"].strip() or row["ts_code"].strip() or "-",
        axis=1,
    )
    work["hm_name"] = work["hm_name"].replace("", "未知游资")
    work["abs_net_amount_yi"] = work["net_amount_yi"].abs()
    work["direction"] = np.where(
        work["net_amount_yi"] > 0,
        "净买入",
        np.where(work["net_amount_yi"] < 0, "净卖出", "均衡"),
    )
    return work


def build_hotmoney_stock_battle_summary(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df is None or detail_df.empty:
        return pd.DataFrame(
            columns=[
                "ts_code",
                "ts_name",
                "hit_count",
                "trade_days",
                "hm_count",
                "total_buy_yi",
                "total_sell_yi",
                "total_net_yi",
                "battle_amount_yi",
                "latest_date",
                "main_hotmoney",
                "latest_hotmoney",
            ]
        )

    work = detail_df.copy()
    grouped = work.groupby("ts_code", dropna=False)
    summary = grouped.agg(
        ts_name=("ts_name", lambda values: next((str(v).strip() for v in values if str(v).strip()), "-")),
        hit_count=("hm_name", "size"),
        trade_days=("trade_date", "nunique"),
        hm_count=("hm_name", "nunique"),
        total_buy_yi=("buy_amount_yi", "sum"),
        total_sell_yi=("sell_amount_yi", "sum"),
        total_net_yi=("net_amount_yi", "sum"),
        battle_amount_yi=("abs_net_amount_yi", "sum"),
        latest_date=("trade_date", "max"),
    ).reset_index()

    main_hotmoney_map: dict[str, str] = {}
    latest_hotmoney_map: dict[str, str] = {}
    for ts_code, stock_rows in work.groupby("ts_code"):
        hm_rank = (
            stock_rows.groupby("hm_name")["abs_net_amount_yi"]
            .sum()
            .sort_values(ascending=False)
        )
        main_hotmoney_map[ts_code] = _join_hotmoney_names(hm_rank.index.tolist(), max_items=4)

        latest_date = stock_rows["trade_date"].max()
        latest_rows = stock_rows[stock_rows["trade_date"] == latest_date].sort_values(
            "abs_net_amount_yi",
            ascending=False,
        )
        latest_hotmoney_map[ts_code] = _join_hotmoney_names(latest_rows["hm_name"], max_items=4)

    summary["main_hotmoney"] = summary["ts_code"].map(main_hotmoney_map).fillna("-")
    summary["latest_hotmoney"] = summary["ts_code"].map(latest_hotmoney_map).fillna("-")
    summary["net_direction"] = np.where(
        summary["total_net_yi"] > 0,
        "净买入",
        np.where(summary["total_net_yi"] < 0, "净卖出", "均衡"),
    )
    summary["latest_date_label"] = pd.to_datetime(summary["latest_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    summary["stock_label"] = summary["ts_name"] + "（" + summary["ts_code"] + "）"
    return summary


def build_hotmoney_stock_battle_display_df(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df is None or summary_df.empty:
        return pd.DataFrame(
            columns=[
                "股票",
                "代码",
                "上榜次数",
                "交易日数",
                "游资数",
                "净买卖(亿)",
                "博弈强度(亿)",
                "主导游资",
                "最近动作",
                "最近日期",
            ]
        )

    out = summary_df[
        [
            "ts_name",
            "ts_code",
            "hit_count",
            "trade_days",
            "hm_count",
            "total_net_yi",
            "battle_amount_yi",
            "main_hotmoney",
            "latest_hotmoney",
            "latest_date_label",
        ]
    ].copy()
    out.columns = [
        "股票",
        "代码",
        "上榜次数",
        "交易日数",
        "游资数",
        "净买卖(亿)",
        "博弈强度(亿)",
        "主导游资",
        "最近动作",
        "最近日期",
    ]
    out["股票"] = build_security_name_jump_links(
        out,
        code_col="代码",
        label_col="股票",
        fallback_col="股票",
        label_prefix="🔎 ",
        nonce_key="hm_stock_battle_render_nonce",
    )
    out["净买卖(亿)"] = out["净买卖(亿)"].map(lambda v: _format_hotmoney_yi(v, signed=True))
    out["博弈强度(亿)"] = out["博弈强度(亿)"].map(_format_hotmoney_yi)
    return out


def build_hotmoney_daily_digest_df(stock_detail_df: pd.DataFrame) -> pd.DataFrame:
    if stock_detail_df is None or stock_detail_df.empty:
        return pd.DataFrame(columns=["日期", "上榜次数", "游资数", "净买卖(亿)", "主买游资", "主卖游资"])

    work = stock_detail_df.copy()
    rows = []
    for trade_date, date_rows in work.groupby("trade_date"):
        buy_rows = date_rows[date_rows["net_amount_yi"] > 0].sort_values("net_amount_yi", ascending=False)
        sell_rows = date_rows[date_rows["net_amount_yi"] < 0].sort_values("net_amount_yi")
        rows.append(
            {
                "日期": pd.to_datetime(trade_date).strftime("%Y-%m-%d"),
                "上榜次数": int(len(date_rows)),
                "游资数": int(date_rows["hm_name"].nunique()),
                "净买卖(亿)": date_rows["net_amount_yi"].sum(),
                "主买游资": _join_hotmoney_names(buy_rows["hm_name"], max_items=4),
                "主卖游资": _join_hotmoney_names(sell_rows["hm_name"], max_items=4),
            }
        )

    out = pd.DataFrame(rows).sort_values("日期", ascending=False)
    out["净买卖(亿)"] = out["净买卖(亿)"].map(lambda v: _format_hotmoney_yi(v, signed=True))
    return out


def build_hotmoney_detail_display_df(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df is None or detail_df.empty:
        return pd.DataFrame(columns=["日期", "游资", "股票", "代码", "方向", "标签", "买入(亿)", "卖出(亿)", "净买卖(亿)", "关联机构"])

    out = detail_df.sort_values(["trade_date", "abs_net_amount_yi"], ascending=[False, False])[
        ["trade_date", "hm_name", "ts_name", "ts_code", "direction", "tag", "buy_amount_yi", "sell_amount_yi", "net_amount_yi", "hm_orgs"]
    ].copy()
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out.columns = ["日期", "游资", "股票", "代码", "方向", "标签", "买入(亿)", "卖出(亿)", "净买卖(亿)", "关联机构"]
    for col in ["买入(亿)", "卖出(亿)"]:
        out[col] = out[col].map(_format_hotmoney_yi)
    out["净买卖(亿)"] = out["净买卖(亿)"].map(lambda v: _format_hotmoney_yi(v, signed=True))
    return out


@st.cache_data(ttl=300, show_spinner=False)
def load_single_stock_hotmoney_model_cached(
    start_date: str,
    end_date: str,
    stock_query: str,
    refresh_nonce: int,
) -> dict:
    from src.hotmoney_stock_tracker import load_single_stock_hotmoney_model
    from src.moneyflow_fetcher import _get_engine_cached

    engine = _get_engine_cached()
    return load_single_stock_hotmoney_model(
        start_date=start_date,
        end_date=end_date,
        stock_query=stock_query,
        engine=engine,
    )


def _hotmoney_tracker_confidence_text(confidence_label: str) -> str:
    mapping = {
        "direct+seat": "游资明细 + 龙虎榜席位",
        "direct": "游资明细",
        "seat": "龙虎榜席位",
        "no_data": "暂无证据",
    }
    return mapping.get(str(confidence_label or ""), str(confidence_label or "-"))


def _hotmoney_tracker_actor_display_df(actor_summary: pd.DataFrame) -> pd.DataFrame:
    if actor_summary is None or actor_summary.empty:
        return pd.DataFrame(columns=["参与方", "证据类型", "置信度", "出现次数", "交易日数", "关联席位数", "净买卖(亿)", "博弈强度(亿)", "最近日期", "证据摘要"])

    out = actor_summary.copy()
    out["证据类型"] = out["evidence_type"].map({"direct_hotmoney": "游资明细", "lhb_seat": "龙虎榜席位"}).fillna(out["evidence_type"])
    out["置信度"] = out["confidence"].map({"direct": "直接", "seat_evidence": "席位证据"}).fillna(out["confidence"])
    out["最近日期"] = pd.to_datetime(out["latest_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out[["actor_name", "证据类型", "置信度", "hit_count", "trade_days", "seat_count", "net_amount_yi", "abs_net_amount_yi", "最近日期", "reasons"]].copy()
    out.columns = ["参与方", "证据类型", "置信度", "出现次数", "交易日数", "关联席位数", "净买卖(亿)", "博弈强度(亿)", "最近日期", "证据摘要"]
    out["净买卖(亿)"] = out["净买卖(亿)"].map(lambda value: _format_hotmoney_yi(value, signed=True))
    out["博弈强度(亿)"] = out["博弈强度(亿)"].map(_format_hotmoney_yi)
    return out


def _hotmoney_tracker_evidence_display_df(evidence_detail: pd.DataFrame) -> pd.DataFrame:
    if evidence_detail is None or evidence_detail.empty:
        return pd.DataFrame(columns=["日期", "来源", "证据类型", "参与方/席位", "关联席位", "买入(亿)", "卖出(亿)", "净买卖(亿)", "原因/标签"])

    out = evidence_detail.copy()
    out["日期"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out["来源"] = out["source"].map({"hm_detail": "游资明细", "lhb_top_inst": "龙虎榜席位"}).fillna(out["source"])
    out["证据类型"] = out["evidence_type"].map({"direct_hotmoney": "直接游资", "lhb_seat": "席位证据"}).fillna(out["evidence_type"])
    out = out[["日期", "来源", "证据类型", "actor_name", "seat_name", "buy_amount_yi", "sell_amount_yi", "net_amount_yi", "reason"]].copy()
    out.columns = ["日期", "来源", "证据类型", "参与方/席位", "关联席位", "买入(亿)", "卖出(亿)", "净买卖(亿)", "原因/标签"]
    for col in ["买入(亿)", "卖出(亿)"]:
        out[col] = out[col].map(_format_hotmoney_yi)
    out["净买卖(亿)"] = out["净买卖(亿)"].map(lambda value: _format_hotmoney_yi(value, signed=True))
    return out


def _hotmoney_tracker_daily_display_df(daily_summary: pd.DataFrame) -> pd.DataFrame:
    if daily_summary is None or daily_summary.empty:
        return pd.DataFrame(columns=["日期", "记录数", "直接游资数", "席位数", "净买卖(亿)", "博弈强度(亿)"])

    out = daily_summary.copy()
    out["日期"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out[["日期", "hit_count", "direct_actor_count", "lhb_seat_count", "net_amount_yi", "abs_net_amount_yi"]].copy()
    out.columns = ["日期", "记录数", "直接游资数", "席位数", "净买卖(亿)", "博弈强度(亿)"]
    out["净买卖(亿)"] = out["净买卖(亿)"].map(lambda value: _format_hotmoney_yi(value, signed=True))
    out["博弈强度(亿)"] = out["博弈强度(亿)"].map(_format_hotmoney_yi)
    return out.sort_values("日期", ascending=False)


def _hotmoney_tracker_lhb_reason_display_df(reason_summary: pd.DataFrame) -> pd.DataFrame:
    if reason_summary is None or reason_summary.empty:
        return pd.DataFrame(columns=["日期", "上榜原因数", "龙虎榜净额(亿)", "龙虎榜成交(亿)", "原因"])

    out = reason_summary.copy()
    out["日期"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out[["日期", "reason_count", "net_amount_yi", "lhb_amount_yi", "reasons"]].copy()
    out.columns = ["日期", "上榜原因数", "龙虎榜净额(亿)", "龙虎榜成交(亿)", "原因"]
    out["龙虎榜净额(亿)"] = out["龙虎榜净额(亿)"].map(lambda value: _format_hotmoney_yi(value, signed=True))
    out["龙虎榜成交(亿)"] = out["龙虎榜成交(亿)"].map(_format_hotmoney_yi)
    return out


def _hotmoney_tracker_process_display_df(process_summary: pd.DataFrame) -> pd.DataFrame:
    if process_summary is None or process_summary.empty:
        return pd.DataFrame(columns=["日期", "净买卖(亿)", "累计净买卖(亿)", "主买", "主卖", "新出现", "活跃参与方"])

    out = process_summary.copy()
    out["日期"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out["主买"] = out.apply(lambda row: "-" if row["leading_buy_actor"] == "-" else f"{row['leading_buy_actor']} {_format_hotmoney_yi(row['leading_buy_yi'], signed=True)}", axis=1)
    out["主卖"] = out.apply(lambda row: "-" if row["leading_sell_actor"] == "-" else f"{row['leading_sell_actor']} {_format_hotmoney_yi(row['leading_sell_yi'], signed=True)}", axis=1)
    out = out[["日期", "net_amount_yi", "cumulative_net_yi", "主买", "主卖", "new_actors", "active_actors"]].copy()
    out.columns = ["日期", "净买卖(亿)", "累计净买卖(亿)", "主买", "主卖", "新出现", "活跃参与方"]
    out["净买卖(亿)"] = out["净买卖(亿)"].map(lambda value: _format_hotmoney_yi(value, signed=True))
    out["累计净买卖(亿)"] = out["累计净买卖(亿)"].map(lambda value: _format_hotmoney_yi(value, signed=True))
    return out.sort_values("日期")


def render_single_stock_hotmoney_tracker(latest_dt):
    from src.hotmoney_stock_tracker import resolve_tracker_default_window

    tracker_start_default, tracker_end_default = resolve_tracker_default_window(latest_dt or datetime.today().date())
    legacy_start_default = tracker_end_default - timedelta(days=31)
    if st.session_state.get("hm_single_stock_window_version") != "2m":
        current_start = st.session_state.get("hm_single_stock_start")
        if current_start is None or pd.to_datetime(current_start).date() == legacy_start_default:
            st.session_state["hm_single_stock_start"] = tracker_start_default
        current_end = st.session_state.get("hm_single_stock_end")
        if current_end is None or pd.to_datetime(current_end).date() == tracker_end_default:
            st.session_state["hm_single_stock_end"] = tracker_end_default
        st.session_state["hm_single_stock_window_version"] = "2m"

    st.markdown(
        """
        <div class="ws-hotmoney-section">
          <div class="ws-hotmoney-kicker">单股追踪</div>
          <div class="ws-hotmoney-note">输入股票后，按“直接游资明细”和“龙虎榜席位证据”两层口径拆解近2个月操作过程。</div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("#### 单股游资操作追踪")

    ctl1, ctl2, ctl3, ctl4 = st.columns([1.15, 0.95, 0.95, 0.65])
    with ctl1:
        stock_query = st.text_input("股票代码/名称", value="", placeholder="000001.SZ / 000001 / 股票简称", key="hm_single_stock_query")
    with ctl2:
        tracker_start_dt = st.date_input(
            "开始日期",
            value=tracker_start_default,
            max_value=tracker_end_default,
            key="hm_single_stock_start",
        )
    with ctl3:
        tracker_end_dt = st.date_input(
            "结束日期",
            value=tracker_end_default,
            max_value=tracker_end_default,
            key="hm_single_stock_end",
        )
    with ctl4:
        st.caption("操作")
        analyze_clicked = st.button("分析/刷新", type="primary", key="hm_single_stock_analyze", use_container_width=True)

    if analyze_clicked:
        st.session_state["hm_single_stock_refresh_nonce"] = int(st.session_state.get("hm_single_stock_refresh_nonce", 0)) + 1

    if tracker_start_dt > tracker_end_dt:
        st.warning("开始日期不能晚于结束日期。")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    if not str(stock_query or "").strip():
        st.info("输入一只股票后，这里会展示游资排名、每日净买卖、龙虎榜上榜原因和原始证据表。")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    start_key = pd.to_datetime(tracker_start_dt).strftime("%Y%m%d")
    end_key = pd.to_datetime(tracker_end_dt).strftime("%Y%m%d")
    refresh_nonce = int(st.session_state.get("hm_single_stock_refresh_nonce", 0))

    try:
        with st.spinner("正在汇总单股游资与龙虎榜证据..."):
            model = load_single_stock_hotmoney_model_cached(start_key, end_key, stock_query, refresh_nonce)
    except Exception as exc:
        st.error(f"单股游资追踪查询失败：{exc}")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    evidence_detail = model.get("evidence_detail", pd.DataFrame())
    actor_summary = model.get("actor_summary", pd.DataFrame())
    daily_summary = model.get("daily_summary", pd.DataFrame())
    actor_timeline = model.get("actor_timeline", pd.DataFrame())
    process_summary = model.get("process_summary", pd.DataFrame())
    reason_summary = model.get("lhb_reason_summary", pd.DataFrame())

    if (evidence_detail is None or evidence_detail.empty) and (reason_summary is None or reason_summary.empty):
        st.info("当前区间没有查到该股票的游资明细或龙虎榜席位证据。")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    title_name = str(model.get("stock_name") or stock_query)
    title_code = str(model.get("stock_code") or "")
    st.caption(
        f"分析对象：{title_name}{f'（{title_code}）' if title_code else ''}；"
        f"证据区间：{model.get('date_label', '-')}；"
        f"口径：{_hotmoney_tracker_confidence_text(model.get('confidence_label', ''))}"
    )

    metric_cols = st.columns(5)
    metric_cols[0].metric("直接游资数", f"{int(model.get('direct_hotmoney_count') or 0):,}")
    metric_cols[1].metric("龙虎榜席位数", f"{int(model.get('lhb_seat_count') or 0):,}")
    metric_cols[2].metric("游资明细净额(亿)", _format_hotmoney_yi(model.get("direct_net_yi"), signed=True))
    metric_cols[3].metric("席位证据净额(亿)", _format_hotmoney_yi(model.get("lhb_seat_net_yi"), signed=True))
    metric_cols[4].metric("合计净额(亿)", _format_hotmoney_yi(model.get("total_net_yi"), signed=True))

    if process_summary is not None and not process_summary.empty:
        st.markdown("##### 观测日到最新日的变化过程")
        process_plot = process_summary.copy().sort_values("trade_date")
        process_plot["date_label"] = pd.to_datetime(process_plot["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")

        fig_process = make_subplots(specs=[[{"secondary_y": True}]])
        fig_process.add_trace(
            go.Bar(
                x=process_plot["date_label"],
                y=process_plot["net_amount_yi"],
                name="当日净买卖",
                marker_color=[THEME_UP if value >= 0 else THEME_DOWN for value in process_plot["net_amount_yi"]],
                text=process_plot["net_amount_yi"].map(lambda value: _format_hotmoney_yi(value, signed=True)),
                textposition="outside",
                customdata=np.stack(
                    [
                        process_plot["leading_buy_actor"],
                        process_plot["leading_sell_actor"],
                        process_plot["new_actors"],
                    ],
                    axis=-1,
                ),
                hovertemplate=(
                    "日期：%{x}<br>"
                    "当日净买卖：%{y:+.2f} 亿<br>"
                    "主买：%{customdata[0]}<br>"
                    "主卖：%{customdata[1]}<br>"
                    "新出现：%{customdata[2]}<extra></extra>"
                ),
            ),
            secondary_y=False,
        )
        fig_process.add_trace(
            go.Scatter(
                x=process_plot["date_label"],
                y=process_plot["cumulative_net_yi"],
                name="累计净买卖",
                mode="lines+markers+text",
                line=dict(color=THEME_PRIMARY, width=2.8),
                marker=dict(size=7, color=THEME_PRIMARY),
                text=process_plot["cumulative_net_yi"].map(lambda value: _format_hotmoney_yi(value, signed=True)),
                textposition="top center",
                hovertemplate="日期：%{x}<br>累计净买卖：%{y:+.2f} 亿<extra></extra>",
            ),
            secondary_y=True,
        )
        fig_process.add_hline(y=0, line_width=1, line_dash="dash", line_color=THEME_NEUTRAL, secondary_y=False)
        fig_process.update_layout(
            title=dict(text="区间资金过程：当日动作与累计方向", x=0.02, font=dict(size=16, color=THEME_TEXT)),
            template="wealthspark_balanced",
            paper_bgcolor=CHART_PAPER_BG,
            plot_bgcolor=CHART_BG,
            font=dict(family="Inter, PingFang SC, sans-serif"),
            height=360,
            margin=dict(l=45, r=45, t=55, b=45),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        fig_process.update_yaxes(title_text="当日净买卖(亿)", secondary_y=False)
        fig_process.update_yaxes(title_text="累计净买卖(亿)", secondary_y=True)
        st.plotly_chart(fig_process, use_container_width=True)

    if actor_timeline is not None and not actor_timeline.empty:
        timeline_rank = (
            actor_timeline.groupby("actor_label", dropna=False)
            .agg(
                total_abs_yi=("abs_net_amount_yi", "sum"),
                first_date=("first_date", "min"),
            )
            .sort_values(["first_date", "total_abs_yi"], ascending=[True, False])
        )
        top_actor_labels = timeline_rank.head(12).index.tolist()
        timeline_show = actor_timeline[actor_timeline["actor_label"].isin(top_actor_labels)].copy()
        timeline_show["date_label"] = pd.to_datetime(timeline_show["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")

        process_left, process_right = st.columns([1, 1])
        with process_left:
            line_rank = (
                actor_timeline.groupby("actor_label", dropna=False)["abs_net_amount_yi"]
                .sum()
                .sort_values(ascending=False)
                .head(6)
                .index
                .tolist()
            )
            fig_actor_flow = go.Figure()
            for actor_label in line_rank:
                actor_rows = actor_timeline[actor_timeline["actor_label"] == actor_label].sort_values("trade_date").copy()
                actor_rows["date_label"] = pd.to_datetime(actor_rows["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
                evidence_type = str(actor_rows["evidence_type"].iloc[0]) if not actor_rows.empty else ""
                fig_actor_flow.add_trace(
                    go.Scatter(
                        x=actor_rows["date_label"],
                        y=actor_rows["cumulative_net_yi"],
                        mode="lines+markers",
                        name=actor_label,
                        line=dict(width=2.2, dash="solid" if evidence_type == "direct_hotmoney" else "dash"),
                        hovertemplate="%{fullData.name}<br>日期：%{x}<br>累计净买卖：%{y:+.2f} 亿<extra></extra>",
                    )
                )
            fig_actor_flow.add_hline(y=0, line_width=1, line_dash="dash", line_color=THEME_NEUTRAL)
            fig_actor_flow.update_layout(
                title=dict(text="主力参与方累计轨迹", x=0.02, font=dict(size=15, color=THEME_TEXT)),
                template="wealthspark_balanced",
                paper_bgcolor=CHART_PAPER_BG,
                plot_bgcolor=CHART_BG,
                font=dict(family="Inter, PingFang SC, sans-serif"),
                height=360,
                margin=dict(l=45, r=25, t=55, b=45),
                xaxis_title="",
                yaxis_title="累计净买卖(亿)",
                legend=dict(orientation="h", yanchor="bottom", y=-0.28, xanchor="left", x=0),
            )
            st.plotly_chart(fig_actor_flow, use_container_width=True)

        with process_right:
            pivot = (
                timeline_show.pivot_table(
                    index="actor_label",
                    columns="date_label",
                    values="net_amount_yi",
                    aggfunc="sum",
                    fill_value=0.0,
                )
                .reindex(top_actor_labels)
            )
            heat_text = [
                [_format_hotmoney_yi(value, signed=True) if abs(float(value)) >= 0.005 else "" for value in row]
                for row in pivot.to_numpy()
            ]
            fig_actor_matrix = go.Figure(go.Heatmap(
                z=pivot.to_numpy(),
                x=pivot.columns,
                y=pivot.index,
                text=heat_text,
                texttemplate="%{text}",
                colorscale=[[0, THEME_DOWN], [0.5, "#F2EFE7"], [1, THEME_UP]],
                zmid=0,
                colorbar=dict(title="亿"),
                hovertemplate="参与方：%{y}<br>日期：%{x}<br>净买卖：%{z:+.2f} 亿<extra></extra>",
            ))
            fig_actor_matrix.update_layout(
                title=dict(text="日期 × 游资动作矩阵", x=0.02, font=dict(size=15, color=THEME_TEXT)),
                template="wealthspark_balanced",
                paper_bgcolor=CHART_PAPER_BG,
                plot_bgcolor=CHART_BG,
                font=dict(family="Inter, PingFang SC, sans-serif"),
                height=max(360, len(pivot) * 28),
                margin=dict(l=150, r=25, t=55, b=55),
                xaxis_title="",
                yaxis_title="",
            )
            st.plotly_chart(fig_actor_matrix, use_container_width=True)

    if process_summary is not None and not process_summary.empty:
        st.markdown("##### 阶段摘要")
        st.dataframe(_hotmoney_tracker_process_display_df(process_summary), use_container_width=True, hide_index=True, height=260)

    if actor_summary is not None and not actor_summary.empty:
        plot_actor = actor_summary.head(14).copy()
        plot_actor["plot_label"] = plot_actor["actor_name"] + " · " + plot_actor["evidence_type"].map({"direct_hotmoney": "直接", "lhb_seat": "席位"}).fillna("")
        plot_actor = plot_actor.sort_values("abs_net_amount_yi", ascending=True)
        fig_actor = go.Figure(go.Bar(
            x=plot_actor["net_amount_yi"],
            y=plot_actor["plot_label"],
            orientation="h",
            marker_color=[THEME_UP if value >= 0 else THEME_DOWN for value in plot_actor["net_amount_yi"]],
            text=plot_actor["net_amount_yi"].map(lambda value: _format_hotmoney_yi(value, signed=True)),
            textposition="outside",
            hovertemplate="%{y}<br>净买卖：%{x:+.2f} 亿<extra></extra>",
        ))
        fig_actor.add_vline(x=0, line_width=1, line_dash="dash", line_color=THEME_NEUTRAL)
        fig_actor.update_layout(
            title=dict(text="参与游资/席位净买卖排行", x=0.02, font=dict(size=16, color=THEME_TEXT)),
            template="wealthspark_balanced",
            paper_bgcolor=CHART_PAPER_BG,
            plot_bgcolor=CHART_BG,
            font=dict(family="Inter, PingFang SC, sans-serif"),
            height=max(330, len(plot_actor) * 28),
            margin=dict(l=145, r=55, t=55, b=30),
            xaxis_title="净买卖(亿)",
            yaxis_title="",
        )
        st.plotly_chart(fig_actor, use_container_width=True)

    if daily_summary is not None and not daily_summary.empty:
        daily_plot = daily_summary.copy().sort_values("trade_date")
        daily_plot["date_label"] = pd.to_datetime(daily_plot["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        fig_daily = go.Figure(go.Bar(
            x=daily_plot["date_label"],
            y=daily_plot["net_amount_yi"],
            marker_color=[THEME_UP if value >= 0 else THEME_DOWN for value in daily_plot["net_amount_yi"]],
            text=daily_plot["net_amount_yi"].map(lambda value: _format_hotmoney_yi(value, signed=True)),
            textposition="outside",
            hovertemplate="日期：%{x}<br>净买卖：%{y:+.2f} 亿<extra></extra>",
        ))
        fig_daily.add_hline(y=0, line_width=1, line_dash="dash", line_color=THEME_NEUTRAL)
        fig_daily.update_layout(
            title=dict(text="每日证据净买卖时间线", x=0.02, font=dict(size=16, color=THEME_TEXT)),
            template="wealthspark_balanced",
            paper_bgcolor=CHART_PAPER_BG,
            plot_bgcolor=CHART_BG,
            font=dict(family="Inter, PingFang SC, sans-serif"),
            height=320,
            margin=dict(l=45, r=35, t=55, b=45),
            xaxis_title="",
            yaxis_title="净买卖(亿)",
        )
        st.plotly_chart(fig_daily, use_container_width=True)

    table_left, table_right = st.columns([1.15, 1])
    with table_left:
        st.markdown("##### 参与方汇总")
        st.dataframe(_hotmoney_tracker_actor_display_df(actor_summary), use_container_width=True, hide_index=True, height=320)
    with table_right:
        st.markdown("##### 每日摘要")
        st.dataframe(_hotmoney_tracker_daily_display_df(daily_summary), use_container_width=True, hide_index=True, height=320)

    if reason_summary is not None and not reason_summary.empty:
        st.markdown("##### 龙虎榜上榜原因")
        st.dataframe(_hotmoney_tracker_lhb_reason_display_df(reason_summary), use_container_width=True, hide_index=True, height=220)

    with st.expander("原始证据明细", expanded=False):
        st.caption("游资明细是直接口径；龙虎榜席位只代表公开席位证据，映射游资时应标为疑似。")
        st.dataframe(_hotmoney_tracker_evidence_display_df(evidence_detail), use_container_width=True, hide_index=True, height=420)

    st.markdown("</div>", unsafe_allow_html=True)


def _format_lhb_yi(value, signed: bool = False) -> str:
    if value is None or pd.isna(value):
        return "-"
    try:
        num = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{num:+,.2f}" if signed else f"{num:,.2f}"


def _format_lhb_percent(value) -> str:
    if value is None or pd.isna(value):
        return "-"
    try:
        return f"{float(value):+,.2f}%"
    except (TypeError, ValueError):
        return "-"


def _normalize_lhb_ts_code_input(raw_value: str) -> str:
    value = str(raw_value or "").strip().upper()
    if not value:
        return ""
    if "." in value:
        return value
    if value.isdigit() and len(value) == 6:
        if value.startswith("6"):
            return f"{value}.SH"
        if value.startswith(("4", "8", "9")):
            return f"{value}.BJ"
        return f"{value}.SZ"
    return value


def build_lhb_stock_summary_display_df(summary_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "股票",
        "代码",
        "上榜次数",
        "交易日数",
        "龙虎榜净买(亿)",
        "机构净买(亿)",
        "合计净买(亿)",
        "龙虎榜成交(亿)",
        "平均涨跌幅",
        "最大换手率",
        "最近日期",
        "主要上榜理由",
    ]
    if summary_df is None or summary_df.empty:
        return pd.DataFrame(columns=columns)

    out = summary_df[
        [
            "name",
            "ts_code",
            "hit_count",
            "trade_days",
            "net_amount_yi",
            "inst_net_yi",
            "combined_net_yi",
            "lhb_amount_yi",
            "avg_pct_change",
            "max_turnover_rate",
            "latest_date_label",
            "reasons",
        ]
    ].copy()
    out.columns = columns
    out["股票"] = build_security_name_jump_links(
        out,
        code_col="代码",
        label_col="股票",
        fallback_col="股票",
        label_prefix="🔎 ",
        nonce_key="lhb_stock_summary_render_nonce",
    )
    for column in ["龙虎榜净买(亿)", "机构净买(亿)", "合计净买(亿)"]:
        out[column] = out[column].map(lambda value: _format_lhb_yi(value, signed=True))
    out["龙虎榜成交(亿)"] = out["龙虎榜成交(亿)"].map(_format_lhb_yi)
    out["平均涨跌幅"] = out["平均涨跌幅"].map(_format_lhb_percent)
    out["最大换手率"] = out["最大换手率"].map(lambda value: "-" if pd.isna(value) else f"{float(value):,.2f}%")
    return out


def build_lhb_top_list_display_df(top_list_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "日期",
        "股票",
        "代码",
        "收盘价",
        "涨跌幅",
        "换手率",
        "榜单买入(亿)",
        "榜单卖出(亿)",
        "净买入(亿)",
        "成交占比",
        "上榜理由",
    ]
    if top_list_df is None or top_list_df.empty:
        return pd.DataFrame(columns=columns)

    work = top_list_df.copy().sort_values(["trade_date", "net_amount_yi"], ascending=[False, False])
    out = work[
        [
            "trade_date",
            "name",
            "ts_code",
            "close",
            "pct_change",
            "turnover_rate",
            "l_buy_yi",
            "l_sell_yi",
            "net_amount_yi",
            "amount_rate",
            "reason",
        ]
    ].copy()
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out.columns = columns
    out["股票"] = build_security_name_jump_links(
        out,
        code_col="代码",
        label_col="股票",
        fallback_col="股票",
        label_prefix="🔎 ",
        nonce_key="lhb_top_list_render_nonce",
    )
    out["收盘价"] = out["收盘价"].map(lambda value: "-" if pd.isna(value) else f"{float(value):,.2f}")
    out["涨跌幅"] = out["涨跌幅"].map(_format_lhb_percent)
    out["换手率"] = out["换手率"].map(lambda value: "-" if pd.isna(value) else f"{float(value):,.2f}%")
    for column in ["榜单买入(亿)", "榜单卖出(亿)"]:
        out[column] = out[column].map(_format_lhb_yi)
    out["净买入(亿)"] = out["净买入(亿)"].map(lambda value: _format_lhb_yi(value, signed=True))
    out["成交占比"] = out["成交占比"].map(lambda value: "-" if pd.isna(value) else f"{float(value):,.2f}%")
    return out


def build_lhb_inst_display_df(inst_df: pd.DataFrame, stock_name_map: dict[str, str]) -> pd.DataFrame:
    columns = ["日期", "股票", "代码", "席位", "方向", "买入(亿)", "卖出(亿)", "净买入(亿)", "上榜理由"]
    if inst_df is None or inst_df.empty:
        return pd.DataFrame(columns=columns)

    work = inst_df.copy().sort_values(["trade_date", "net_buy_yi"], ascending=[False, False])
    work["name"] = work["ts_code"].map(stock_name_map).fillna(work["ts_code"])
    out = work[["trade_date", "name", "ts_code", "exalter", "side_label", "buy_yi", "sell_yi", "net_buy_yi", "reason"]].copy()
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out.columns = columns
    out["股票"] = build_security_name_jump_links(
        out,
        code_col="代码",
        label_col="股票",
        fallback_col="股票",
        label_prefix="🔎 ",
        nonce_key="lhb_inst_render_nonce",
    )
    for column in ["买入(亿)", "卖出(亿)"]:
        out[column] = out[column].map(_format_lhb_yi)
    out["净买入(亿)"] = out["净买入(亿)"].map(lambda value: _format_lhb_yi(value, signed=True))
    return out


@st.cache_data(ttl=3600, show_spinner=False)
def load_lhb_data_cached(
    start_date: str,
    end_date: str,
    ts_code: str,
    include_inst: bool,
    refresh_nonce: int,
) -> dict:
    from src.lhb_monitor import fetch_lhb_data, load_lhb_data_from_db
    from src.moneyflow_fetcher import _get_engine_cached
    from src.volume_fetcher import _init_tushare

    db_error = None
    try:
        engine = _get_engine_cached()
        db_data = load_lhb_data_from_db(
            start_date=start_date,
            end_date=end_date,
            ts_code=ts_code or None,
            include_inst=include_inst,
            engine=engine,
        )
        if not db_data.get("top_list", pd.DataFrame()).empty or (
            include_inst and not db_data.get("top_inst", pd.DataFrame()).empty
        ):
            return db_data
    except Exception as exc:
        db_error = str(exc)

    pro = _init_tushare()
    live_data = fetch_lhb_data(
        pro=pro,
        start_date=start_date,
        end_date=end_date,
        ts_code=ts_code or None,
        include_inst=include_inst,
        request_sleep_seconds=0.25,
    )
    live_data["source"] = "tushare"
    if db_error:
        live_data.setdefault("errors", []).append({"api": "db_cache", "trade_date": "", "error": db_error})
    return live_data


@st.cache_data(ttl=1800, show_spinner=False)
def load_lhb_industry_map_cached(ts_codes: tuple[str, ...]) -> dict[str, str]:
    from src.lhb_board import load_lhb_industry_map

    return load_lhb_industry_map(ts_codes)


def render_lhb_monitor_tab():
    st.subheader("🐉 龙虎榜")
    st.caption("基于 Tushare 龙虎榜每日明细（top_list）与机构成交明细（top_inst），仅拉取今年以来的数据。")

    from src.lhb_board import (
        build_lhb_today_board_model,
    )
    from src.lhb_monitor import (
        build_lhb_daily_overview,
        build_lhb_reason_summary,
        build_lhb_stock_summary,
    )

    today = datetime.now().date()
    year_start = datetime(today.year, 1, 1).date()

    ctl1, ctl2, ctl3, ctl4, ctl5 = st.columns([1.0, 1.0, 1.0, 0.75, 0.9])
    with ctl1:
        start_dt = st.date_input(
            "起始日期",
            value=year_start,
            min_value=year_start,
            max_value=today,
            key="lhb_start_date",
        )
    with ctl2:
        end_dt = st.date_input(
            "结束日期",
            value=today,
            min_value=year_start,
            max_value=today,
            key="lhb_end_date",
        )
    with ctl3:
        stock_code_input = st.text_input("股票代码", value="", placeholder="000001.SZ", key="lhb_stock_code")
    with ctl4:
        top_n = st.selectbox("TopN", [10, 20, 30, 50], index=1, key="lhb_topn")
    with ctl5:
        order_label = st.selectbox(
            "排序",
            ["上榜次数", "合计净买", "机构净买", "榜单成交"],
            index=0,
            key="lhb_order_label",
        )

    action_cols = st.columns([1.0, 1.0, 3.2])
    with action_cols[0]:
        include_inst = st.checkbox("机构明细", value=True, key="lhb_include_inst")
    with action_cols[1]:
        load_clicked = st.button("拉取/刷新", type="primary", key="lhb_load_button", use_container_width=True)

    if start_dt > end_dt:
        st.warning("起始日期不能晚于结束日期。")
        return

    if load_clicked:
        st.session_state["lhb_loaded_once"] = True
        st.session_state["lhb_refresh_nonce"] = int(st.session_state.get("lhb_refresh_nonce", 0)) + 1

    if not st.session_state.get("lhb_loaded_once", False):
        st.info("当前页面尚未拉取龙虎榜数据。")
        return

    normalized_code = _normalize_lhb_ts_code_input(stock_code_input)
    start_key = pd.to_datetime(start_dt).strftime("%Y%m%d")
    end_key = pd.to_datetime(end_dt).strftime("%Y%m%d")
    refresh_nonce = int(st.session_state.get("lhb_refresh_nonce", 0))

    try:
        with st.spinner(f"正在拉取 {start_dt:%Y-%m-%d} ~ {end_dt:%Y-%m-%d} 的龙虎榜数据..."):
            lhb_data = load_lhb_data_cached(
                start_key,
                end_key,
                normalized_code,
                bool(include_inst),
                refresh_nonce,
            )
    except Exception as exc:
        st.error(f"龙虎榜数据拉取失败：{exc}")
        return

    top_list_df = lhb_data.get("top_list", pd.DataFrame())
    inst_df = lhb_data.get("top_inst", pd.DataFrame())
    order_key_map = {
        "上榜次数": "hit_count",
        "合计净买": "combined_net",
        "机构净买": "inst_net",
        "榜单成交": "lhb_amount",
    }
    summary_df = build_lhb_stock_summary(top_list_df, inst_df, order_by=order_key_map.get(order_label, "hit_count"))
    daily_df = build_lhb_daily_overview(top_list_df, inst_df)
    reason_df = build_lhb_reason_summary(top_list_df, top_n=12)
    stock_name_map = {}
    if top_list_df is not None and not top_list_df.empty:
        stock_name_map = (
            top_list_df.sort_values("trade_date")
            .dropna(subset=["ts_code"])
            .groupby("ts_code")["name"]
            .last()
            .to_dict()
        )

    trade_dates = lhb_data.get("trade_dates", []) or []
    errors = lhb_data.get("errors", []) or []
    metric_cols = st.columns(5)
    metric_cols[0].metric("交易日", f"{len(trade_dates):,}")
    metric_cols[1].metric("上榜股票", f"{int(summary_df['ts_code'].nunique()) if not summary_df.empty else 0:,}")
    metric_cols[2].metric("上榜记录", f"{len(top_list_df):,}")
    metric_cols[3].metric("龙虎榜净买(亿)", _format_lhb_yi(summary_df["net_amount_yi"].sum() if not summary_df.empty else 0, signed=True))
    metric_cols[4].metric("机构净买(亿)", _format_lhb_yi(summary_df["inst_net_yi"].sum() if not summary_df.empty else 0, signed=True))
    data_source = str(lhb_data.get("source") or "tushare")
    source_label = "数据库定时缓存" if data_source == "db" else "Tushare 实时接口"
    st.caption(f"数据来源：{source_label} · 数据区间：{lhb_data.get('start_date', start_key)} ~ {lhb_data.get('end_date', end_key)}")

    if errors:
        with st.expander(f"接口异常 {len(errors)} 条", expanded=False):
            st.dataframe(pd.DataFrame(errors), use_container_width=True, hide_index=True, height=220)

    if top_list_df is None or top_list_df.empty:
        st.warning("当前条件下没有返回龙虎榜每日明细。")
        return

    board_codes = tuple(sorted({str(code).strip().upper() for code in top_list_df.get("ts_code", pd.Series(dtype=str)).dropna() if str(code).strip()}))
    industry_map = load_lhb_industry_map_cached(board_codes)
    today_board = build_lhb_today_board_model(top_list_df, inst_df, industry_map=industry_map)
    if today_board.get("stock_count", 0) > 0:
        st.markdown("#### 当日龙虎榜板块视图")
        board_metric_cols = st.columns(5)
        board_metric_cols[0].metric("当日日期", today_board.get("trade_date_label", "-"))
        board_metric_cols[1].metric("上榜股票", f"{int(today_board.get('stock_count', 0)):,}")
        board_metric_cols[2].metric("上榜板块", f"{int(today_board.get('sector_count', 0)):,}")
        board_metric_cols[3].metric("合计净买(亿)", _format_lhb_yi(today_board.get("combined_net_yi", 0), signed=True))
        board_metric_cols[4].metric("机构净买(亿)", _format_lhb_yi(today_board.get("inst_net_yi", 0), signed=True))

        board_stock_codes = list(today_board.get("stock_codes", []))
        selected_board_key = "lhb_today_selected_stock"
        selected_board_stock = str(st.session_state.get(selected_board_key, "") or "").strip().upper()
        if selected_board_stock not in board_stock_codes and board_stock_codes:
            selected_board_stock = board_stock_codes[0]
            st.session_state[selected_board_key] = selected_board_stock

        board_component_model = json.loads(json.dumps(today_board, ensure_ascii=False, default=str))
        board_component_value = _LHB_TODAY_BOARD_COMPONENT(
            model=board_component_model,
            selectedTsCode=selected_board_stock,
            height=720,
            default=selected_board_stock,
            key=f"lhb_today_board_component_{today_board.get('trade_date_label', 'latest')}",
        )
        if isinstance(board_component_value, str):
            component_stock = board_component_value.strip().upper()
        elif isinstance(board_component_value, dict):
            component_stock = str(board_component_value.get("ts_code") or "").strip().upper()
        else:
            component_stock = ""
        if component_stock in board_stock_codes:
            selected_board_stock = component_stock

        pill_options = board_stock_codes[:24]
        if selected_board_stock and selected_board_stock not in pill_options:
            pill_options = [selected_board_stock] + pill_options[:23]
        picked_stock = st.pills(
            "焦点个股",
            options=pill_options,
            default=selected_board_stock if selected_board_stock in pill_options else None,
            format_func=lambda code: f"{stock_name_map.get(code, code)}（{code}）",
            key="lhb_today_board_pick",
            selection_mode="single",
        )
        if picked_stock:
            selected_board_stock = picked_stock

        st.session_state[selected_board_key] = selected_board_stock
        if selected_board_stock in summary_df["ts_code"].tolist():
            st.session_state["lhb_focus_stock"] = selected_board_stock

        selected_board_meta = {}
        for sector in today_board.get("sectors", []):
            for stock in sector.get("stocks", []):
                if stock.get("ts_code") == selected_board_stock:
                    selected_board_meta = stock
                    break
            if selected_board_meta:
                break

        if selected_board_stock:
            st.markdown('<div id="lhb-today-detail"></div>', unsafe_allow_html=True)
            board_trade_date = today_board.get("trade_date")
            top_detail = top_list_df.copy()
            top_detail["_trade_date_norm"] = pd.to_datetime(top_detail["trade_date"], errors="coerce").dt.normalize()
            selected_top_detail = top_detail[
                (top_detail["_trade_date_norm"] == pd.Timestamp(board_trade_date).normalize())
                & (top_detail["ts_code"].astype(str).str.upper() == selected_board_stock)
            ].drop(columns=["_trade_date_norm"])

            if inst_df is not None and not inst_df.empty:
                inst_detail = inst_df.copy()
                inst_detail["_trade_date_norm"] = pd.to_datetime(inst_detail["trade_date"], errors="coerce").dt.normalize()
                selected_inst_detail = inst_detail[
                    (inst_detail["_trade_date_norm"] == pd.Timestamp(board_trade_date).normalize())
                    & (inst_detail["ts_code"].astype(str).str.upper() == selected_board_stock)
                ].drop(columns=["_trade_date_norm"])
            else:
                selected_inst_detail = pd.DataFrame()

            selected_label = f"{selected_board_meta.get('name') or stock_name_map.get(selected_board_stock, selected_board_stock)}（{selected_board_stock}）"
            st.markdown(f"##### {selected_label} 当日明细")
            focus_metric_cols = st.columns(5)
            focus_metric_cols[0].metric("板块", selected_board_meta.get("sector", "-"))
            focus_metric_cols[1].metric("涨跌幅", selected_board_meta.get("pct_label", "-"))
            focus_metric_cols[2].metric("榜单净买", _format_lhb_yi(selected_board_meta.get("net_amount_yi", 0), signed=True))
            focus_metric_cols[3].metric("机构净买", _format_lhb_yi(selected_board_meta.get("inst_net_yi", 0), signed=True))
            focus_metric_cols[4].metric("成交占比", f"{float(selected_board_meta.get('amount_rate', 0)):,.2f}%")

            detail_left, detail_right = st.columns([1.08, 0.92])
            with detail_left:
                st.dataframe(
                    build_lhb_top_list_display_df(selected_top_detail),
                    use_container_width=True,
                    hide_index=True,
                    height=220,
                    column_config={
                        "股票": st.column_config.LinkColumn(
                            "股票",
                            help="点击后跳转到个股/指数查询",
                            display_text=r".*#(.*)$",
                        )
                    },
                )
            with detail_right:
                if selected_inst_detail is not None and not selected_inst_detail.empty:
                    st.dataframe(
                        build_lhb_inst_display_df(selected_inst_detail, stock_name_map),
                        use_container_width=True,
                        hide_index=True,
                        height=220,
                        column_config={
                            "股票": st.column_config.LinkColumn(
                                "股票",
                                help="点击后跳转到个股/指数查询",
                                display_text=r".*#(.*)$",
                            )
                        },
                    )
                else:
                    st.info("当日暂无机构席位明细。")

    tab_overview, tab_stock, tab_inst, tab_detail = st.tabs(["📊 总览", "🎯 个股榜", "🏛 机构席位", "🧾 每日明细"])

    with tab_overview:
        chart_left, chart_right = st.columns([1.2, 0.9])
        with chart_left:
            fig_daily = go.Figure()
            fig_daily.add_trace(
                go.Bar(
                    x=daily_df["trade_date_label"],
                    y=daily_df["net_amount_yi"],
                    marker_color=[THEME_UP if value >= 0 else THEME_DOWN for value in daily_df["net_amount_yi"]],
                    name="龙虎榜净买",
                    hovertemplate="日期：%{x}<br>龙虎榜净买：%{y:+.2f} 亿<extra></extra>",
                )
            )
            if include_inst and "inst_net_yi" in daily_df.columns:
                fig_daily.add_trace(
                    go.Scatter(
                        x=daily_df["trade_date_label"],
                        y=daily_df["inst_net_yi"],
                        mode="lines+markers",
                        line=dict(color=THEME_NAVY, width=2.2),
                        name="机构净买",
                        hovertemplate="日期：%{x}<br>机构净买：%{y:+.2f} 亿<extra></extra>",
                    )
                )
            fig_daily.add_hline(y=0, line_width=1, line_dash="dash", line_color=THEME_NEUTRAL)
            fig_daily.update_layout(
                title=dict(text="每日龙虎榜净买入", x=0.02, font=dict(size=16, color=THEME_TEXT)),
                template="wealthspark_balanced",
                paper_bgcolor=CHART_PAPER_BG,
                plot_bgcolor=CHART_BG,
                font=dict(family="Inter, PingFang SC, sans-serif"),
                height=390,
                margin=dict(l=45, r=25, t=55, b=45),
                xaxis_title="",
                yaxis_title="亿元",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_daily, use_container_width=True)

        with chart_right:
            if reason_df is not None and not reason_df.empty:
                reason_plot = reason_df.sort_values("hit_count", ascending=True)
                fig_reason = go.Figure(
                    go.Bar(
                        x=reason_plot["hit_count"],
                        y=reason_plot["reason"],
                        orientation="h",
                        marker_color=THEME_PRIMARY,
                        text=reason_plot["hit_count"],
                        textposition="outside",
                        hovertemplate="理由：%{y}<br>上榜次数：%{x}<extra></extra>",
                    )
                )
                fig_reason.update_layout(
                    title=dict(text="上榜理由分布", x=0.02, font=dict(size=16, color=THEME_TEXT)),
                    template="wealthspark_balanced",
                    paper_bgcolor=CHART_PAPER_BG,
                    plot_bgcolor=CHART_BG,
                    font=dict(family="Inter, PingFang SC, sans-serif"),
                    height=max(390, len(reason_plot) * 28),
                    margin=dict(l=150, r=35, t=55, b=30),
                    xaxis_title="次数",
                    yaxis=dict(autorange=False),
                )
                st.plotly_chart(fig_reason, use_container_width=True)
            else:
                st.info("暂无上榜理由分布。")

        if not daily_df.empty:
            daily_show = daily_df.sort_values("trade_date", ascending=False)[
                [
                    "trade_date_label",
                    "stock_count",
                    "record_count",
                    "total_buy_yi",
                    "total_sell_yi",
                    "net_amount_yi",
                    "inst_net_yi",
                    "inst_hit_count",
                ]
            ].copy()
            daily_show.columns = ["日期", "股票数", "记录数", "买入(亿)", "卖出(亿)", "净买入(亿)", "机构净买(亿)", "机构记录"]
            for column in ["买入(亿)", "卖出(亿)"]:
                daily_show[column] = daily_show[column].map(_format_lhb_yi)
            for column in ["净买入(亿)", "机构净买(亿)"]:
                daily_show[column] = daily_show[column].map(lambda value: _format_lhb_yi(value, signed=True))
            st.dataframe(daily_show, use_container_width=True, hide_index=True, height=300)

    with tab_stock:
        if summary_df.empty:
            st.info("当前范围暂无可汇总的个股龙虎榜数据。")
        else:
            plot_df = summary_df.head(int(top_n)).sort_values("combined_net_yi", ascending=True)
            fig_stock = go.Figure(
                go.Bar(
                    x=plot_df["combined_net_yi"],
                    y=plot_df["stock_label"],
                    orientation="h",
                    marker_color=[THEME_UP if value >= 0 else THEME_DOWN for value in plot_df["combined_net_yi"]],
                    text=plot_df["hit_count"].map(lambda value: f"{int(value)}次"),
                    textposition="outside",
                    customdata=np.stack(
                        [
                            plot_df["net_amount_yi"],
                            plot_df["inst_net_yi"],
                            plot_df["trade_days"],
                        ],
                        axis=-1,
                    ),
                    hovertemplate=(
                        "%{y}<br>合计净买：%{x:+.2f} 亿<br>"
                        "龙虎榜净买：%{customdata[0]:+.2f} 亿<br>"
                        "机构净买：%{customdata[1]:+.2f} 亿<br>"
                        "交易日：%{customdata[2]}<extra></extra>"
                    ),
                )
            )
            fig_stock.add_vline(x=0, line_width=1, line_dash="dash", line_color=THEME_NEUTRAL)
            fig_stock.update_layout(
                title=dict(text=f"个股龙虎榜强度 Top{int(top_n)}", x=0.02, font=dict(size=16, color=THEME_TEXT)),
                template="wealthspark_balanced",
                paper_bgcolor=CHART_PAPER_BG,
                plot_bgcolor=CHART_BG,
                font=dict(family="Inter, PingFang SC, sans-serif"),
                height=max(380, len(plot_df) * 28),
                margin=dict(l=150, r=55, t=55, b=35),
                xaxis_title="合计净买入(亿)",
                yaxis=dict(autorange=False),
            )
            st.plotly_chart(fig_stock, use_container_width=True)
            st.dataframe(
                build_lhb_stock_summary_display_df(summary_df.head(max(int(top_n), 30))),
                use_container_width=True,
                hide_index=True,
                height=410,
                column_config={
                    "股票": st.column_config.LinkColumn(
                        "股票",
                        help="点击后跳转到个股/指数查询",
                        display_text=r".*#(.*)$",
                    )
                },
            )

            selected_stock = st.selectbox(
                "单股追踪",
                options=summary_df["ts_code"].tolist(),
                format_func=lambda code: f"{stock_name_map.get(code, code)}（{code}）",
                key="lhb_focus_stock",
            )
            focus_top = top_list_df[top_list_df["ts_code"] == selected_stock].copy()
            focus_inst = inst_df[inst_df["ts_code"] == selected_stock].copy() if inst_df is not None and not inst_df.empty else pd.DataFrame()
            if not focus_top.empty:
                focus_summary = summary_df[summary_df["ts_code"] == selected_stock].iloc[0]
                focus_cols = st.columns(4)
                focus_cols[0].metric("上榜次数", int(focus_summary["hit_count"]))
                focus_cols[1].metric("龙虎榜净买(亿)", _format_lhb_yi(focus_summary["net_amount_yi"], signed=True))
                focus_cols[2].metric("机构净买(亿)", _format_lhb_yi(focus_summary["inst_net_yi"], signed=True))
                focus_cols[3].metric("合计净买(亿)", _format_lhb_yi(focus_summary["combined_net_yi"], signed=True))

                focus_daily = (
                    focus_top.groupby("trade_date", dropna=False)
                    .agg(
                        net_amount_yi=("net_amount_yi", "sum"),
                        lhb_amount_yi=("l_amount_yi", "sum"),
                        hit_count=("ts_code", "size"),
                    )
                    .reset_index()
                    .sort_values("trade_date")
                )
                if not focus_inst.empty:
                    focus_inst_daily = (
                        focus_inst.groupby("trade_date", dropna=False)["net_buy_yi"]
                        .sum()
                        .reset_index(name="inst_net_yi")
                    )
                    focus_daily = focus_daily.merge(focus_inst_daily, on="trade_date", how="left")
                else:
                    focus_daily["inst_net_yi"] = 0.0
                focus_daily["inst_net_yi"] = pd.to_numeric(focus_daily["inst_net_yi"], errors="coerce").fillna(0.0)
                focus_daily["date_label"] = pd.to_datetime(focus_daily["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")

                fig_focus = go.Figure()
                fig_focus.add_trace(
                    go.Bar(
                        x=focus_daily["date_label"],
                        y=focus_daily["net_amount_yi"],
                        marker_color=[THEME_UP if value >= 0 else THEME_DOWN for value in focus_daily["net_amount_yi"]],
                        name="龙虎榜净买",
                    )
                )
                fig_focus.add_trace(
                    go.Scatter(
                        x=focus_daily["date_label"],
                        y=focus_daily["inst_net_yi"],
                        mode="lines+markers",
                        line=dict(color=THEME_NAVY, width=2),
                        name="机构净买",
                    )
                )
                fig_focus.add_hline(y=0, line_width=1, line_dash="dash", line_color=THEME_NEUTRAL)
                fig_focus.update_layout(
                    title=dict(text=f"{stock_name_map.get(selected_stock, selected_stock)} 每日净买入", x=0.02, font=dict(size=15, color=THEME_TEXT)),
                    template="wealthspark_balanced",
                    paper_bgcolor=CHART_PAPER_BG,
                    plot_bgcolor=CHART_BG,
                    font=dict(family="Inter, PingFang SC, sans-serif"),
                    height=330,
                    margin=dict(l=45, r=25, t=55, b=45),
                    yaxis_title="亿元",
                    xaxis_title="",
                )
                st.plotly_chart(fig_focus, use_container_width=True)
                st.dataframe(
                    build_lhb_top_list_display_df(focus_top),
                    use_container_width=True,
                    hide_index=True,
                    height=280,
                    column_config={
                        "股票": st.column_config.LinkColumn(
                            "股票",
                            help="点击后跳转到个股/指数查询",
                            display_text=r".*#(.*)$",
                        )
                    },
                )

    with tab_inst:
        if inst_df is None or inst_df.empty:
            st.info("当前范围暂无机构成交明细，或未勾选机构明细。")
        else:
            inst_rank = (
                inst_df.groupby("exalter", dropna=False)
                .agg(
                    hit_count=("ts_code", "size"),
                    stock_count=("ts_code", "nunique"),
                    buy_yi=("buy_yi", "sum"),
                    sell_yi=("sell_yi", "sum"),
                    net_buy_yi=("net_buy_yi", "sum"),
                )
                .reset_index()
                .sort_values(["net_buy_yi", "hit_count"], ascending=False)
            )
            plot_inst = inst_rank.head(int(top_n)).sort_values("net_buy_yi", ascending=True)
            fig_inst = go.Figure(
                go.Bar(
                    x=plot_inst["net_buy_yi"],
                    y=plot_inst["exalter"],
                    orientation="h",
                    marker_color=[THEME_UP if value >= 0 else THEME_DOWN for value in plot_inst["net_buy_yi"]],
                    text=plot_inst["stock_count"].map(lambda value: f"{int(value)}股"),
                    textposition="outside",
                    hovertemplate="席位：%{y}<br>净买入：%{x:+.2f} 亿<extra></extra>",
                )
            )
            fig_inst.add_vline(x=0, line_width=1, line_dash="dash", line_color=THEME_NEUTRAL)
            fig_inst.update_layout(
                title=dict(text=f"机构席位净买入 Top{int(top_n)}", x=0.02, font=dict(size=16, color=THEME_TEXT)),
                template="wealthspark_balanced",
                paper_bgcolor=CHART_PAPER_BG,
                plot_bgcolor=CHART_BG,
                font=dict(family="Inter, PingFang SC, sans-serif"),
                height=max(360, len(plot_inst) * 28),
                margin=dict(l=150, r=55, t=55, b=35),
                xaxis_title="净买入(亿)",
                yaxis=dict(autorange=False),
            )
            st.plotly_chart(fig_inst, use_container_width=True)

            inst_rank_show = inst_rank.head(max(int(top_n), 30)).copy()
            inst_rank_show.columns = ["席位", "记录数", "股票数", "买入(亿)", "卖出(亿)", "净买入(亿)"]
            for column in ["买入(亿)", "卖出(亿)"]:
                inst_rank_show[column] = inst_rank_show[column].map(_format_lhb_yi)
            inst_rank_show["净买入(亿)"] = inst_rank_show["净买入(亿)"].map(lambda value: _format_lhb_yi(value, signed=True))
            st.dataframe(inst_rank_show, use_container_width=True, hide_index=True, height=300)
            st.dataframe(
                build_lhb_inst_display_df(inst_df.head(1000), stock_name_map),
                use_container_width=True,
                hide_index=True,
                height=420,
                column_config={
                    "股票": st.column_config.LinkColumn(
                        "股票",
                        help="点击后跳转到个股/指数查询",
                        display_text=r".*#(.*)$",
                    )
                },
            )

    with tab_detail:
        st.dataframe(
            build_lhb_top_list_display_df(top_list_df.head(1200)),
            use_container_width=True,
            hide_index=True,
            height=520,
            column_config={
                "股票": st.column_config.LinkColumn(
                    "股票",
                    help="点击后跳转到个股/指数查询",
                    display_text=r".*#(.*)$",
                )
            },
        )


HISTORICAL_ST_BADGE_TEXT = '曾经ST'


def format_historical_st_badge(value) -> str:
    return HISTORICAL_ST_BADGE_TEXT if bool(value) else ''


def style_historical_st_badge_column(column: pd.Series) -> list[str]:
    badge_style = (
        'background-color: #F6E7B8; '
        'color: #1B263B; '
        'font-weight: 700; '
        f'border: 1px solid {THEME_PRIMARY}; '
        'border-radius: 999px; '
        'text-align: center; '
        'white-space: nowrap;'
    )
    return [badge_style if str(value or '').strip() == HISTORICAL_ST_BADGE_TEXT else '' for value in column]


def build_security_jump_table_styler(render_df: pd.DataFrame):
    styler = render_df.style
    if '标签' in render_df.columns:
        styler = styler.apply(style_historical_st_badge_column, subset=['标签'])
    return styler


def render_security_jump_table(display_df: pd.DataFrame, help_text: str, code_col: str = '代码', fallback_col: str = '简称', nonce_key: str = 'security_jump_render_nonce') -> None:
    if display_df is None or display_df.empty:
        return

    render_df = display_df.copy()
    query_links = build_security_jump_links(render_df, code_col=code_col, fallback_col=fallback_col, nonce_key=nonce_key)
    render_df.insert(0, '查询', query_links)
    styled_df = build_security_jump_table_styler(render_df)

    st.info(help_text)
    st.dataframe(
        styled_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            '查询': st.column_config.LinkColumn(
                '查询',
                help='点击后跳转到个股/指数查询',
                display_text='🔎 查询'
            ),
            '标签': st.column_config.TextColumn(
                '标签',
                width='small',
                help='黄色 badge 表示该股票历史上曾被 ST 处理，但当前不一定处于 ST 状态。'
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
    if bool(row.get('has_ever_st')):
        extras.append('曾经ST')
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


def format_holder_number_metric(value, end_date) -> tuple[str, str | None]:
    value_text = format_optional_number(value, digits=0)
    end_date_text = format_optional_date(end_date)
    if end_date_text == "-":
        return value_text, None
    return value_text, f"截止 {end_date_text}"


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


def _series_to_plotly_list(series: pd.Series) -> list:
    return pd.Series(series).astype(object).where(pd.notna(series), None).tolist()


def create_security_kline_chart(
    df: pd.DataFrame,
    prefix: str,
    title: str,
    ma_windows: list[int] | None = None,
    volume_ma_windows: list[int] | None = None,
    show_macd: bool = False,
    enable_select_points: bool = False,
    selected_trade_date: str = "",
) -> go.Figure | None:
    if prefix == "d":
        open_col, high_col, low_col, close_col = "open", "high", "low", "close"
        amount_col, vol_col = "amount", "vol"
    else:
        open_col = f"{prefix}_open"
        high_col = f"{prefix}_high"
        low_col = f"{prefix}_low"
        close_col = f"{prefix}_close"
        amount_col = f"{prefix}_amount"
        vol_col = f"{prefix}_vol"

    required = ["trade_date", open_col, high_col, low_col, close_col]
    if df is None or df.empty or any(col not in df.columns for col in required):
        return None

    chart_df = df[required + [c for c in [amount_col, vol_col] if c in df.columns]].copy()
    chart_df["trade_date"] = pd.to_datetime(chart_df["trade_date"], errors="coerce")
    for col in [open_col, high_col, low_col, close_col, amount_col, vol_col]:
        if col in chart_df.columns:
            chart_df[col] = pd.to_numeric(chart_df[col], errors="coerce")

    chart_df = chart_df.dropna(subset=["trade_date", open_col, high_col, low_col, close_col]).sort_values("trade_date")
    if chart_df.empty:
        return None

    trade_dates = chart_df["trade_date"].dt.strftime("%Y-%m-%d").tolist()
    selected_trade_date = str(selected_trade_date or "").strip()
    selected_trade_idx = trade_dates.index(selected_trade_date) if selected_trade_date in trade_dates else -1
    open_values = _series_to_plotly_list(chart_df[open_col])
    high_values = _series_to_plotly_list(chart_df[high_col])
    low_values = _series_to_plotly_list(chart_df[low_col])
    close_values = _series_to_plotly_list(chart_df[close_col])

    rangebreaks = []
    normalized_dates = chart_df["trade_date"].dt.normalize().drop_duplicates().sort_values()
    if len(normalized_dates) >= 2:
        full_dates = pd.date_range(normalized_dates.iloc[0], normalized_dates.iloc[-1], freq="D")
        missing_dates = full_dates.difference(pd.DatetimeIndex(normalized_dates))
        if len(missing_dates) > 0:
            rangebreaks = [dict(values=missing_dates.strftime("%Y-%m-%d").tolist())]

    ma_windows = ma_windows or [5, 10]
    ma_windows = sorted({int(w) for w in ma_windows if isinstance(w, (int, float)) and int(w) > 1})
    if not ma_windows:
        ma_windows = [5, 10]
    for w in ma_windows:
        chart_df[f"ma{w}"] = chart_df[close_col].rolling(window=w).mean()

    row_heights = [0.62, 0.2, 0.18] if show_macd else [0.74, 0.26]
    subplot_titles = (title, "成交额/成交量", "MACD") if show_macd else (title, "成交额/成交量")
    fig = make_subplots(
        rows=3 if show_macd else 2, cols=1, shared_xaxes=True, vertical_spacing=0.03,
        row_heights=row_heights,
        subplot_titles=subplot_titles
    )

    up_mask = chart_df[close_col] >= chart_df[open_col]
    bar_colors = np.where(up_mask, THEME_UP, THEME_DOWN)

    fig.add_trace(
        go.Candlestick(
            x=trade_dates,
            open=open_values,
            high=high_values,
            low=low_values,
            close=close_values,
            increasing_line_color=THEME_UP,
            decreasing_line_color=THEME_DOWN,
            increasing_fillcolor=CHART_UP_FILL,
            decreasing_fillcolor=CHART_DOWN_FILL,
            name="K线",
        ),
        row=1, col=1
    )

    if selected_trade_idx >= 0:
        selected_low = float(low_values[selected_trade_idx])
        selected_high = float(high_values[selected_trade_idx])
        selected_open = float(open_values[selected_trade_idx])
        selected_close = float(close_values[selected_trade_idx])
        selected_body_bottom = min(selected_open, selected_close)
        selected_body_top = max(selected_open, selected_close)
        selected_range = max(selected_high - selected_low, 0.01)
        min_body_height = max(selected_range * 0.08, 0.01)
        if (selected_body_top - selected_body_bottom) < min_body_height:
            body_mid = (selected_open + selected_close) / 2
            selected_body_bottom = body_mid - min_body_height / 2
            selected_body_top = body_mid + min_body_height / 2
        highlight_body_height = max(selected_body_top - selected_body_bottom, 0.01)
        selected_body_width_ms = 0.68 * 24 * 60 * 60 * 1000
        selected_date = trade_dates[selected_trade_idx]
        fig.add_trace(
            go.Bar(
                x=[selected_date],
                y=[highlight_body_height],
                base=[selected_body_bottom],
                width=[selected_body_width_ms],
                marker=dict(
                    color=CHART_GOLD_SOFT_FILL,
                    line=dict(color=THEME_PRIMARY, width=2.4),
                ),
                hoverinfo="skip",
                showlegend=False,
                name="当前选中日K",
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=[selected_date, selected_date],
                y=[selected_low, selected_high],
                mode="lines",
                line=dict(color=THEME_PRIMARY, width=1.8),
                hoverinfo="skip",
                showlegend=False,
                name="当前选中日K影线",
            ),
            row=1,
            col=1,
        )

    ma_colors = [THEME_NAVY, THEME_PURPLE, THEME_WARN, THEME_DOWN, THEME_UP, "#6FA3B8"]
    for idx, w in enumerate(ma_windows):
        ma_col = f"ma{w}"
        if ma_col not in chart_df.columns:
            continue
        fig.add_trace(
            go.Scatter(
                x=trade_dates,
                y=_series_to_plotly_list(chart_df[ma_col]),
                mode="lines",
                name=f"MA{w}",
                line=dict(color=ma_colors[idx % len(ma_colors)], width=1.6),
                hovertemplate=f"%{{x|%Y-%m-%d}}<br>MA{w}: %{{y:,.2f}}<extra></extra>",
            ),
            row=1, col=1
        )

    volume_used = None
    if amount_col in chart_df.columns and chart_df[amount_col].notna().any():
        volume_used = amount_col
        y_title = "成交额"
    elif vol_col in chart_df.columns and chart_df[vol_col].notna().any():
        volume_used = vol_col
        y_title = "成交量"
    else:
        y_title = "成交额/成交量"

    if volume_used:
        fig.add_trace(
            go.Bar(
                x=trade_dates,
                y=_series_to_plotly_list(chart_df[volume_used]),
                name=y_title,
                marker_color=bar_colors,
                opacity=0.45,
                hovertemplate="%{x|%Y-%m-%d}<br>值: %{y:,.2f}<extra></extra>",
            ),
            row=2, col=1
        )

        volume_ma_windows = volume_ma_windows or [5, 10]
        volume_ma_windows = sorted({int(w) for w in volume_ma_windows if isinstance(w, (int, float)) and int(w) > 1})
        if not volume_ma_windows:
            volume_ma_windows = [5, 10]

        vol_ma_colors = ["#4F6785", THEME_PURPLE, "#C28C4E", "#5B8E7D"]
        for idx, w in enumerate(volume_ma_windows):
            vol_ma_col = f"vol_ma{w}"
            chart_df[vol_ma_col] = pd.to_numeric(chart_df[volume_used], errors="coerce").rolling(window=w).mean()
            fig.add_trace(
                go.Scatter(
                    x=trade_dates,
                    y=_series_to_plotly_list(chart_df[vol_ma_col]),
                    mode="lines",
                    name=f"VOL_MA{w}",
                    line=dict(color=vol_ma_colors[idx % len(vol_ma_colors)], width=1.4),
                    hovertemplate=f"%{{x|%Y-%m-%d}}<br>VOL_MA{w}: %{{y:,.2f}}<extra></extra>",
                ),
                row=2, col=1
            )

    if show_macd:
        close_series = pd.to_numeric(chart_df[close_col], errors="coerce")
        ema12 = close_series.ewm(span=12, adjust=False).mean()
        ema26 = close_series.ewm(span=26, adjust=False).mean()
        chart_df["dif"] = ema12 - ema26
        chart_df["dea"] = chart_df["dif"].ewm(span=9, adjust=False).mean()
        chart_df["macd_hist"] = (chart_df["dif"] - chart_df["dea"]) * 2

        macd_colors = np.where(chart_df["macd_hist"] >= 0, THEME_UP, THEME_DOWN)
        fig.add_trace(
            go.Bar(
                x=trade_dates,
                y=_series_to_plotly_list(chart_df["macd_hist"]),
                name="MACD",
                marker_color=macd_colors,
                opacity=0.55,
                hovertemplate="%{x|%Y-%m-%d}<br>MACD: %{y:,.3f}<extra></extra>",
            ),
            row=3, col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=trade_dates,
                y=_series_to_plotly_list(chart_df["dif"]),
                mode="lines",
                name="DIF",
                line=dict(color=THEME_NAVY, width=1.5),
                hovertemplate="%{x|%Y-%m-%d}<br>DIF: %{y:,.3f}<extra></extra>",
            ),
            row=3, col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=trade_dates,
                y=_series_to_plotly_list(chart_df["dea"]),
                mode="lines",
                name="DEA",
                line=dict(color=THEME_PURPLE, width=1.5),
                hovertemplate="%{x|%Y-%m-%d}<br>DEA: %{y:,.3f}<extra></extra>",
            ),
            row=3, col=1,
        )

    fig.update_layout(
        template="wealthspark_balanced",
        height=620,
        hovermode="x unified",
        clickmode="event+select",
        dragmode=False,
        showlegend=True,
        xaxis_rangeslider_visible=False,
        margin=dict(l=20, r=20, t=60, b=20),
    )
    apply_time_series_hover_affordance(fig, chart_df["trade_date"], chart_df[close_col])
    fig.update_yaxes(title_text="价格", row=1, col=1, fixedrange=True)
    fig.update_yaxes(title_text=y_title, row=2, col=1, fixedrange=True)
    if show_macd:
        fig.update_yaxes(title_text="MACD", row=3, col=1, fixedrange=True)
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor=CHART_GRID_COLOR,
        type="date",
        rangebreaks=rangebreaks,
        fixedrange=True,
    )

    return fig


def create_security_intraday_chart(
    df: pd.DataFrame,
    title: str,
    reference_close: float | None = None,
) -> go.Figure | None:
    required = ["trade_time", "open", "high", "low", "close"]
    if df is None or df.empty or any(col not in df.columns for col in required):
        return None

    chart_df = df.copy()
    chart_df["trade_time"] = pd.to_datetime(chart_df["trade_time"], errors="coerce")
    for col in ["open", "high", "low", "close", "vol", "amount"]:
        if col in chart_df.columns:
            chart_df[col] = pd.to_numeric(chart_df[col], errors="coerce")
    chart_df = chart_df.dropna(subset=["trade_time", "open", "high", "low", "close"]).sort_values("trade_time")
    if chart_df.empty:
        return None

    reference_price = pd.to_numeric(pd.Series([reference_close]), errors="coerce").iloc[0]
    if pd.isna(reference_price) or float(reference_price) == 0:
        fallback_open = pd.to_numeric(chart_df["open"], errors="coerce").dropna()
        reference_price = float(fallback_open.iloc[0]) if not fallback_open.empty else np.nan

    if pd.notna(reference_price) and float(reference_price) != 0:
        chart_df["hover_pct_text"] = ((chart_df["close"] - float(reference_price)) / float(reference_price) * 100.0).map(
            lambda value: f"{value:+.2f}%"
        )
    else:
        chart_df["hover_pct_text"] = "-"

    # A股午休（11:30-13:00）不应在分时图里显示为空白时间段。
    intraday_rangebreaks = [dict(bounds=[11.5, 13], pattern="hour")]

    row_heights = [0.72, 0.28]
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=row_heights,
        subplot_titles=(title, "成交额/成交量"),
    )

    up_mask = chart_df["close"] >= chart_df["open"]
    bar_colors = np.where(up_mask, THEME_UP, THEME_DOWN)

    fig.add_trace(
        go.Candlestick(
            x=chart_df["trade_time"],
            open=chart_df["open"],
            high=chart_df["high"],
            low=chart_df["low"],
            close=chart_df["close"],
            customdata=chart_df[["hover_pct_text"]],
            hovertemplate=(
                "%{x|%H:%M}"
                "<br>开: %{open:.2f}"
                "<br>高: %{high:.2f}"
                "<br>低: %{low:.2f}"
                "<br>收: %{close:.2f}"
                "<br>涨幅: %{customdata[0]}"
                "<extra></extra>"
            ),
            increasing_line_color=THEME_UP,
            decreasing_line_color=THEME_DOWN,
            increasing_fillcolor=CHART_UP_FILL,
            decreasing_fillcolor=CHART_DOWN_FILL,
            name="1分钟K线",
        ),
        row=1,
        col=1,
    )

    volume_used = None
    y_title = "成交额/成交量"
    if "amount" in chart_df.columns and chart_df["amount"].notna().any():
        volume_used = "amount"
        y_title = "成交额"
    elif "vol" in chart_df.columns and chart_df["vol"].notna().any():
        volume_used = "vol"
        y_title = "成交量"

    if volume_used:
        fig.add_trace(
            go.Bar(
                x=chart_df["trade_time"],
                y=chart_df[volume_used],
                name=y_title,
                marker_color=bar_colors,
                opacity=0.45,
                hovertemplate="%{x|%H:%M}<br>值: %{y:,.2f}<extra></extra>",
            ),
            row=2,
            col=1,
        )

    fig.update_layout(
        template="wealthspark_balanced",
        height=520,
        hovermode="x unified",
        showlegend=True,
        xaxis_rangeslider_visible=False,
        margin=dict(l=20, r=20, t=60, b=20),
    )
    apply_time_series_hover_affordance(
        fig,
        chart_df["trade_time"],
        chart_df["close"],
        min_right_pad=TIME_SERIES_INTRADAY_MIN_RIGHT_PAD,
    )
    fig.update_yaxes(title_text="价格", row=1, col=1, fixedrange=True)
    fig.update_yaxes(title_text=y_title, row=2, col=1, fixedrange=True)
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor=CHART_GRID_COLOR,
        type="date",
        tickformat="%H:%M",
        rangebreaks=intraday_rangebreaks,
    )
    return fig


def create_metric_line_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    yaxis_title: str,
    scale: float = 1.0,
    digits: int = 2,
    color: str = THEME_NAVY
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
        title=dict(text=title, x=0.02, font=dict(size=18, color=THEME_TEXT)),
        xaxis_title='日期',
        yaxis_title=yaxis_title,
        hovermode='x unified',
        height=360,
        template='plotly_white',
        plot_bgcolor=CHART_BG,
        paper_bgcolor=CHART_PAPER_BG,
        font=dict(family='Inter, PingFang SC, sans-serif'),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    apply_time_series_hover_affordance(fig, chart_df[x_col], chart_df[y_col])
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR)
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR)
    return fig


def create_financial_bar_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    yaxis_title: str,
    scale: float = 1.0,
    digits: int = 2,
    positive_color: str = THEME_NAVY,
    negative_color: str = THEME_DOWN
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
        title=dict(text=title, x=0.02, font=dict(size=18, color=THEME_TEXT)),
        xaxis_title='报告期',
        yaxis_title=yaxis_title,
        height=360,
        template='plotly_white',
        plot_bgcolor=CHART_BG,
        paper_bgcolor=CHART_PAPER_BG,
        font=dict(family='Inter, PingFang SC, sans-serif'),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR)
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
        color = THEME_MUTED
    elif is_positive:
        arrow = "↑"
        color = THEME_UP  # 红色表示上涨
    else:
        arrow = "↓"
        color = THEME_DOWN  # 绿色表示下跌

    delta_display = f"{arrow} {delta}" if delta != '-' else '-'
    if delta_pct and delta_pct != '-':
        delta_display += f" ({delta_pct})"

    card_html = f"""
    <div style="
        background: {THEME_SURFACE};
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
            color: {THEME_MUTED};
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.5rem;
        ">{title}</div>
        <div style="
            font-size: 2rem;
            font-weight: 700;
            color: {THEME_TEXT};
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
        THEME_NAVY, THEME_PURPLE, "#C28C4E", "#5B8E7D", "#B86A84",
        THEME_PRIMARY, "#6FA3B8", THEME_UP, "#8AA05A", THEME_PURPLE
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
                    fillcolor=CHART_NAVY_SOFT_FILL,
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
            font=dict(size=20, weight=700, color=THEME_TEXT),
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
        plot_bgcolor=CHART_BG,
        paper_bgcolor=CHART_PAPER_BG,
        font=dict(family='Inter, PingFang SC, sans-serif'),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    if fig.data:
        apply_time_series_hover_affordance(fig, filtered_df['date'], filtered_df['value'])

    # 网格线样式
    fig.update_xaxes(
        rangeslider_visible=False,
        showgrid=True,
        gridwidth=1,
        gridcolor=CHART_GRID_COLOR,
        showline=True,
        linewidth=1,
        linecolor=CHART_AXIS_COLOR
    )

    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor=CHART_GRID_COLOR,
        showline=True,
        linewidth=1,
        linecolor=CHART_AXIS_COLOR,
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
        '沪市主板': THEME_NAVY,
        '深市主板': "#5B8E7D",
        '创业板': "#C28C4E",
        '科创板': THEME_PURPLE,
    }

    trade_dates = (
        pd.to_datetime(df['trade_date'], errors='coerce').dropna()
        if 'trade_date' in df.columns
        else pd.Series(dtype='datetime64[ns]')
    )
    latest_trade_date = None
    latest_total_amount = None
    xaxis_range = None
    if not trade_dates.empty:
        earliest_trade_date = trade_dates.min()
        latest_trade_date = trade_dates.max()
        xaxis_range = [
            earliest_trade_date,
            latest_trade_date + pd.Timedelta(days=VOLUME_STACKED_XAXIS_RIGHT_PAD_DAYS),
        ]
        if 'amount' in df.columns:
            latest_trade_mask = pd.to_datetime(df['trade_date'], errors='coerce') == latest_trade_date
            latest_amounts = pd.to_numeric(df.loc[latest_trade_mask, 'amount'], errors='coerce').dropna()
            if not latest_amounts.empty:
                latest_total_amount = float(latest_amounts.sum())

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

    if latest_trade_date is not None and latest_total_amount and latest_total_amount > 0:
        fig.add_trace(go.Scatter(
            x=[latest_trade_date, latest_trade_date],
            y=[0, latest_total_amount],
            mode="lines",
            name="latest-day-hover-target",
            line=dict(color="rgba(212, 175, 55, 0.01)", width=42),
            hovertemplate="<extra></extra>",
            showlegend=False,
        ))

    fig.update_layout(
        barmode='stack',
        title=dict(
            text='各板块每日成交额（亿元）',
            font=dict(size=20, weight=700, color=THEME_TEXT),
            x=0.02
        ),
        xaxis_title='日期',
        yaxis_title='成交额（亿元）',
        hovermode='x unified',
        hoverdistance=VOLUME_STACKED_HOVER_DISTANCE,
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
        plot_bgcolor=CHART_BG,
        paper_bgcolor=CHART_PAPER_BG,
        font=dict(family='Inter, PingFang SC, sans-serif'),
        margin=dict(l=20, r=VOLUME_STACKED_HOVER_RIGHT_MARGIN, t=60, b=20)
    )

    if latest_trade_date is not None:
        fig.add_shape(
            type="line",
            xref="x",
            yref="paper",
            x0=latest_trade_date,
            x1=latest_trade_date,
            y0=0,
            y1=1,
            line=dict(color=THEME_PRIMARY, width=1.5, dash="dot"),
            layer="above",
        )
        fig.add_annotation(
            x=latest_trade_date,
            y=1.02,
            xref="x",
            yref="paper",
            text=f"最新 {latest_trade_date:%Y-%m-%d}",
            showarrow=False,
            xanchor="left",
            yanchor="bottom",
            xshift=6,
            font=dict(size=11, color=THEME_TEXT),
            bgcolor="rgba(255, 255, 255, 0.72)",
            bordercolor=THEME_BORDER_SOFT,
            borderwidth=1,
            borderpad=3,
        )

    fig.update_xaxes(
        showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR,
        showline=True, linewidth=1, linecolor=CHART_AXIS_COLOR,
        range=xaxis_range
    )
    fig.update_yaxes(
        showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR,
        showline=True, linewidth=1, linecolor=CHART_AXIS_COLOR,
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
        marker_color=CHART_NAVY_SOFT_FILL,
        hovertemplate='<b>%{x|%Y-%m-%d}</b><br>成交额: %{y:.2f} 亿元<extra></extra>'
    ))

    # 5日均线
    fig.add_trace(go.Scatter(
        x=daily_total['trade_date'],
        y=daily_total['ma5'],
        mode='lines',
        name='5日均线',
        line=dict(width=2, color="#C28C4E", shape='spline'),
        hovertemplate='<b>%{x|%Y-%m-%d}</b><br>5日均线: %{y:.2f} 亿元<extra></extra>'
    ))

    # 20日均线
    fig.add_trace(go.Scatter(
        x=daily_total['trade_date'],
        y=daily_total['ma20'],
        mode='lines',
        name='20日均线',
        line=dict(width=2.5, color=THEME_UP, shape='spline'),
        hovertemplate='<b>%{x|%Y-%m-%d}</b><br>20日均线: %{y:.2f} 亿元<extra></extra>'
    ))

    fig.update_layout(
        title=dict(
            text='A股每日总成交额趋势',
            font=dict(size=20, weight=700, color=THEME_TEXT),
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
        plot_bgcolor=CHART_BG,
        paper_bgcolor=CHART_PAPER_BG,
        font=dict(family='Inter, PingFang SC, sans-serif'),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    apply_time_series_hover_affordance(fig, daily_total['trade_date'], daily_total['amount'])

    fig.update_xaxes(
        showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR,
        showline=True, linewidth=1, linecolor=CHART_AXIS_COLOR
    )
    fig.update_yaxes(
        showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR,
        showline=True, linewidth=1, linecolor=CHART_AXIS_COLOR,
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
        with st.container(key="ws-page-toolbar-volume"):
            toolbar_date_col, toolbar_sector_col = st.columns([1.0, 1.4])
            with toolbar_date_col:
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
                        key="vol_date_range"
                    )
            with toolbar_sector_col:
                all_sectors = sorted(vol_df['ts_name'].unique())
                selected_sectors = st.multiselect(
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
    consume_pending_fund_watchlist_navigation()

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
            f'background:linear-gradient(135deg,{THEME_PRIMARY} 0%, {THEME_PRIMARY_STRONG} 100%);'
            f'color:{THEME_NAVY};text-decoration:none;font-weight:700;box-shadow:{THEME_SHADOW};">📱 iPhone模式</a></div>',
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
                border: 1px solid rgba(27, 38, 59, 0.08) !important;
                border-radius: 14px !important;
                background: linear-gradient(180deg, rgba(255,255,255,0.98) 0%, rgba(248, 250, 248, 0.98) 100%) !important;
                overflow: hidden !important;
                box-shadow: 0 8px 24px rgba(37, 99, 235, 0.08) !important;
                margin-bottom: 1rem !important;
            }
            div[data-testid="stExpander"] details summary {
                padding: 0.85rem 1rem !important;
                font-weight: 700 !important;
                color: #1B263B !important;
            }
            div[data-testid="stExpanderDetails"] {
                padding: 0.2rem 1rem 1rem 1rem !important;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        st.markdown(
            f'<div style="margin-bottom:0.6rem;color:{THEME_MUTED};">'
            '已启用 iPhone 专用模式（不依赖 sidebar）。 '
            f'<a href="?" style="color:{THEME_PRIMARY};text-decoration:none;">退出 iPhone 模式</a>'
            '</div>',
            unsafe_allow_html=True,
        )

        render_user_login_status()

        mobile_group = st.radio(
            "模块",
            ["决策", "基金", "股票", "资金", "宏观"],
            horizontal=True,
            key="iphone_group_radio",
        )

        if mobile_group == "决策":
            mobile_page = st.selectbox(
                "页面",
                DECISION_PAGE_OPTIONS,
                key="iphone_page_decision",
            )
            st.caption(f"当前位置：决策 / {mobile_page}")
            if mobile_page == "💼 今日机会清单":
                render_commercial_mvp_tab()
            elif mobile_page == "⭐ 每日趋势推荐":
                render_daily_trend_reco_tab()
            elif mobile_page == "🧪 推荐评估":
                render_reco_effectiveness_tracking_panel()
            else:
                render_ml_prediction_upgrade_tab()

        elif mobile_group == "基金":
            mobile_page = st.selectbox(
                "页面",
                ETF_PAGE_OPTIONS,
                key="iphone_page_etf",
            )
            st.caption(f"当前位置：基金 / {mobile_page}")
            if mobile_page == ETF_MAIN_PAGE_LABEL:
                render_etf_tab()
            elif mobile_page == ETF_RATIO_PAGE_LABEL:
                render_etf_category_ratio_tab()
            elif mobile_page == ETF_TREND_PAGE_LABEL:
                render_etf_trend_tab()
            elif mobile_page == ETF_WIDE_INDEX_PAGE_LABEL:
                render_wide_index_tab()
            elif mobile_page == ETF_FUND_MONITOR_PAGE_LABEL:
                render_fund_monitor_tab()
            elif mobile_page == ETF_FUND_WATCHLIST_PAGE_LABEL:
                render_fund_watchlist_tab()
            else:
                render_etf_tab()
        elif mobile_group == "股票":
            mobile_page = st.selectbox(
                "页面",
                STOCK_PAGE_OPTIONS,
                key="iphone_page_stock",
            )
            st.caption(f"当前位置：股票 / {mobile_page}")
            if mobile_page == STOCK_SECURITY_SEARCH_LABEL:
                render_security_search_tab()
            elif mobile_page == STOCK_LHB_PAGE_LABEL:
                render_lhb_monitor_tab()
            elif mobile_page == STOCK_USER_WATCHLIST_LABEL:
                render_user_watchlist_tab()
            elif mobile_page == STOCK_POOL_PAGE_LABEL:
                render_user_stock_pool_tab()
            elif mobile_page == STOCK_COMPANY_SCREENER_LABEL:
                render_company_screener_tab()
            elif mobile_page == FACTOR_WORKBENCH_PAGE_LABEL:
                render_factor_workbench_tab()
            elif mobile_page == TRACKING_PAGE_LABEL:
                render_author_tracking_tab()
            elif mobile_page == STOCK_TECH_PICKER_LABEL:
                render_tech_picker_tab()
            else:
                render_security_search_tab()

        elif mobile_group == "资金":
            mobile_page = st.selectbox(
                "页面",
                MONEY_PAGE_OPTIONS,
                key="iphone_page_money",
            )
            st.caption(f"当前位置：资金 / {mobile_page}")
            if mobile_page == "💹 资金流向":
                render_moneyflow_tab()
            elif mobile_page == "📊 每日成交量":
                render_volume_tab()
            elif mobile_page == "🏦 公募持仓热股":
                render_fund_hot_stocks_tab()
            elif mobile_page == "🔥 打板情绪":
                render_limitup_monitor_tab()
            else:
                render_hotmoney_tab()

        else:
            mobile_page = st.selectbox(
                "页面",
                MACRO_PAGE_OPTIONS,
                key="iphone_page_macro",
            )
            st.caption(f"当前位置：宏观 / {mobile_page}")
            if mobile_page == "🌏 宏观经济":
                render_macro_tab()
            elif mobile_page == "🏦 本外币存款":
                render_etf_deposit_tab()
            elif mobile_page == "📊 指数监测":
                render_index_monitor_tab()
            else:
                render_fund_monitor_tab()

        st.stop()

    # ===== 方案B进阶版：desktop sidebar 导航壳层 =====
    selected_module, selected_page = render_desktop_sidebar_navigation()
    render_user_login_status()
    st.caption(f"当前位置：{selected_module} / {selected_page}")

    decision_module_label = get_module_label_for_page(DECISION_TODAY_PAGE_LABEL)
    fund_module_label = get_module_label_for_page(ETF_MAIN_PAGE_LABEL)
    stock_module_label = get_module_label_for_page(STOCK_SECURITY_SEARCH_LABEL)
    money_module_label = get_module_label_for_page(MONEY_FLOW_PAGE_LABEL)
    macro_module_label = get_module_label_for_page(MACRO_MAIN_PAGE_LABEL)

    if selected_module == decision_module_label:
        if selected_page == DECISION_TODAY_PAGE_LABEL:
            render_commercial_mvp_tab()
        elif selected_page == DECISION_DAILY_RECO_PAGE_LABEL:
            render_daily_trend_reco_tab()
        elif selected_page == DECISION_RECO_EVAL_PAGE_LABEL:
            render_reco_effectiveness_tracking_panel()
        elif selected_page == DECISION_ML_PAGE_LABEL:
            render_ml_prediction_upgrade_tab()
        else:
            render_commercial_mvp_tab()

    elif selected_module == fund_module_label:
        if selected_page == ETF_MAIN_PAGE_LABEL:
            render_etf_tab()
        elif selected_page == ETF_RATIO_PAGE_LABEL:
            render_etf_category_ratio_tab()
        elif selected_page == ETF_TREND_PAGE_LABEL:
            render_etf_trend_tab()
        elif selected_page == ETF_FUND_MONITOR_PAGE_LABEL:
            render_fund_monitor_tab()
        elif selected_page == ETF_WIDE_INDEX_PAGE_LABEL:
            render_wide_index_tab()
        elif selected_page == ETF_FUND_WATCHLIST_PAGE_LABEL:
            render_fund_watchlist_tab()
        else:
            render_etf_tab()

    elif selected_module == stock_module_label:
        if selected_page == STOCK_SECURITY_SEARCH_LABEL:
            render_security_search_tab()
        elif selected_page == STOCK_LHB_PAGE_LABEL:
            render_lhb_monitor_tab()
        elif selected_page == STOCK_USER_WATCHLIST_LABEL:
            render_user_watchlist_tab()
        elif selected_page == STOCK_POOL_PAGE_LABEL:
            render_user_stock_pool_tab()
        elif selected_page == STOCK_COMPANY_SCREENER_LABEL:
            render_company_screener_tab()
        elif selected_page == FACTOR_WORKBENCH_PAGE_LABEL:
            render_factor_workbench_tab()
        elif selected_page == TRACKING_PAGE_LABEL:
            render_author_tracking_tab()
        elif selected_page == STOCK_TECH_PICKER_LABEL:
            render_tech_picker_tab()
        else:
            render_security_search_tab()

    elif selected_module == money_module_label:
        if selected_page == MONEY_FLOW_PAGE_LABEL:
            render_moneyflow_tab()
        elif selected_page == MONEY_VOLUME_PAGE_LABEL:
            render_volume_tab()
        elif selected_page == MONEY_FUND_HOT_PAGE_LABEL:
            render_fund_hot_stocks_tab()
        elif selected_page == MONEY_LIMITUP_PAGE_LABEL:
            render_limitup_monitor_tab()
        elif selected_page == MONEY_HOTMONEY_PAGE_LABEL:
            render_hotmoney_tab()
        else:
            render_moneyflow_tab()

    elif selected_module == macro_module_label:
        if selected_page == MACRO_MAIN_PAGE_LABEL:
            render_macro_tab()
        elif selected_page == MACRO_DEPOSIT_PAGE_LABEL:
            render_etf_deposit_tab()
        elif selected_page == MACRO_INDEX_MONITOR_PAGE_LABEL:
            render_index_monitor_tab()
        else:
            render_macro_tab()




# ===== ML预测升级 Tab =====
ML_PREDICTION_RUNTIME_SNAPSHOT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "tasks",
    "etf-prediction-upgrade",
    "outputs",
    "runtime",
    "ml_prediction_upgrade_walk_forward_snapshot.json",
)
ML_PREDICTION_SNAPSHOT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "tasks",
    "etf-prediction-upgrade",
    "outputs",
    "ml_prediction_upgrade_walk_forward_snapshot.json",
)


def _format_ratio_pct(value, digits=1) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "-"
    return f"{float(numeric):.{digits}%}"


def _format_decimal_metric(value, digits=4) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "-"
    return f"{float(numeric):.{digits}f}"


def _build_ml_prediction_upgrade_demo_sample_df() -> pd.DataFrame:
    rows = []
    for day_idx, trade_date in enumerate(pd.date_range("2026-01-05", periods=8, freq="D")):
        for stock_idx in range(4):
            signal = day_idx * 0.55 + stock_idx * 0.35
            forward_return = (
                -0.8
                + day_idx * 0.7
                + stock_idx * 0.4
                - (1.8 if (day_idx + stock_idx) % 4 == 0 else 0.0)
            ) / 100.0
            rows.append(
                {
                    "trade_date": trade_date,
                    "ts_code": f"{600000 + day_idx * 10 + stock_idx}.SH",
                    "listing_days": 120 + day_idx * 5 + stock_idx,
                    "is_current_st": 0,
                    "has_ever_st": int(stock_idx == 3 and day_idx % 3 == 0),
                    "close": 10 + signal * 2.5,
                    "ret_1d": -0.3 + signal * 0.12,
                    "ret_3d": -0.1 + signal * 0.18,
                    "ret_5d": signal * 0.22,
                    "ret_10d": signal * 0.27,
                    "ret_20d": signal * 0.33,
                    "close_over_ma5": 0.97 + signal * 0.03,
                    "close_over_ma20": 0.95 + signal * 0.035,
                    "ma5_over_ma20": 0.93 + signal * 0.04,
                    "w_ema5_over_30": 0.94 + signal * 0.035,
                    "feature_complete_ratio": 0.99 - stock_idx * 0.01,
                    "y_up_5d": int(forward_return > 0.003),
                    "ret_fwd_5d": forward_return,
                }
            )
    return pd.DataFrame(rows)


def _load_ml_prediction_upgrade_snapshot_results(snapshot_path: str | None = None) -> dict:
    candidate_paths = []
    if snapshot_path:
        candidate_paths.append(snapshot_path)
    candidate_paths.extend([
        ML_PREDICTION_RUNTIME_SNAPSHOT_PATH,
        ML_PREDICTION_SNAPSHOT_PATH,
    ])

    seen = set()
    for path in candidate_paths:
        normalized_path = os.path.abspath(path)
        if normalized_path in seen:
            continue
        seen.add(normalized_path)
        try:
            if not normalized_path or not os.path.exists(normalized_path):
                continue
            with open(normalized_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            snapshot_type = str(payload.get("snapshot_type") or "").strip()
            if snapshot_type != "ml_stock_walk_forward":
                continue

            normalized = {
                "snapshot_type": snapshot_type,
                "generated_at": payload.get("generated_at"),
                "data_source": payload.get("data_source") or "snapshot",
                "snapshot_path": normalized_path,
                "sample_overview": payload.get("sample_overview") or {},
                "classification": payload.get("classification") or {},
                "regression": payload.get("regression") or {},
            }

            task_type = str(payload.get("task_type") or "").strip()
            if task_type in {"classification", "regression"} and not normalized.get(task_type):
                normalized[task_type] = {
                    "task_type": task_type,
                    "model_kind": payload.get("model_kind"),
                    "target_column": payload.get("target_column"),
                    "fill_method": payload.get("fill_method"),
                    "classifier": payload.get("classifier"),
                    "regressor": payload.get("regressor"),
                    "rows_loaded": payload.get("rows_loaded"),
                    "sample_overview": payload.get("sample_overview") or {},
                    "prepared": payload.get("prepared") or {},
                    "aggregate": payload.get("aggregate") or {},
                    "window_results": payload.get("window_results") or [],
                    "skipped_windows": payload.get("skipped_windows") or [],
                }
            return normalized
        except Exception as exc:
            logger.warning(f"load_ml_prediction_upgrade_snapshot_results failed for {normalized_path}: {exc}")
            continue
    return {}


@st.cache_data(show_spinner=False)
def _load_ml_prediction_upgrade_demo_results() -> dict:
    sample_df = _build_ml_prediction_upgrade_demo_sample_df()
    common_eval_kwargs = {
        "fill_method": "median",
        "min_train_rows": 8,
        "min_test_rows": 8,
        "max_windows": 3,
    }

    classification_prepared = prepare_training_data(
        sample_df,
        task_type="classification",
        fill_method="median",
    )
    classification_result = run_walk_forward_evaluation(
        classification_prepared,
        model_kind="baseline",
        **common_eval_kwargs,
    )

    regression_prepared = prepare_training_data(
        sample_df,
        task_type="regression",
        target_column=DEFAULT_REGRESSION_TARGET,
        fill_method="median",
    )
    regression_result = run_walk_forward_evaluation(
        regression_prepared,
        model_kind="sklearn",
        regressor="linear",
        **common_eval_kwargs,
    )

    trade_dates = pd.to_datetime(sample_df["trade_date"], errors="coerce")
    return {
        "snapshot_type": "ml_stock_walk_forward_demo",
        "generated_at": None,
        "data_source": "demo",
        "sample_overview": {
            "row_count": int(len(sample_df)),
            "day_count": int(trade_dates.nunique()),
            "symbol_count": int(sample_df["ts_code"].nunique()),
            "date_start": trade_dates.min().strftime("%Y-%m-%d"),
            "date_end": trade_dates.max().strftime("%Y-%m-%d"),
        },
        "classification": classification_result.to_summary(),
        "regression": regression_result.to_summary(),
    }


def _build_ml_strategy_overview_df(strategy_metrics: dict) -> pd.DataFrame:
    rows = []
    for top_n in strategy_metrics.get("topn_levels", []):
        top_key = f"top{top_n}"
        rows.append(
            {
                "TopN": f"Top{top_n}",
                "平均日收益": _format_ratio_pct(
                    strategy_metrics.get(f"average_daily_{top_key}_return"),
                    digits=2,
                ),
                "平均命中率": _format_ratio_pct(
                    strategy_metrics.get(f"average_daily_{top_key}_hit_rate"),
                    digits=1,
                ),
            }
        )
    return pd.DataFrame(rows)


def _build_ml_window_results_df(summary: dict) -> pd.DataFrame:
    rows = []
    task_type = str(summary.get("task_type") or "")
    for window in summary.get("window_results", []):
        test_metrics = window.get("test_metrics") or {}
        strategy_metrics = window.get("strategy_metrics") or {}
        row = {
            "cutoff_date": window.get("cutoff_date"),
            "train_rows": window.get("train_rows"),
            "test_rows": window.get("test_rows"),
            "Top1收益": _format_ratio_pct((strategy_metrics.get("top1") or {}).get("avg_return"), digits=2),
            "Top3收益": _format_ratio_pct((strategy_metrics.get("top3") or {}).get("avg_return"), digits=2),
            "Top5收益": _format_ratio_pct((strategy_metrics.get("top5") or {}).get("avg_return"), digits=2),
            "Top1命中率": _format_ratio_pct((strategy_metrics.get("top1") or {}).get("hit_rate"), digits=1),
        }
        if task_type == "classification":
            row["测试准确率"] = _format_ratio_pct(test_metrics.get("accuracy"), digits=1)
        else:
            row["测试RMSE"] = _format_decimal_metric(test_metrics.get("rmse"), digits=4)
            row["测试R²"] = _format_decimal_metric(test_metrics.get("r2"), digits=4)
        rows.append(row)
    return pd.DataFrame(rows)


def _build_ml_prediction_panel_title(summary: dict) -> str:
    task_type = str(summary.get("task_type") or "").strip() or "unknown"
    model_kind = str(summary.get("model_kind") or "").strip() or "unknown"
    selection = ""
    if task_type == "classification":
        selection = str(summary.get("classifier") or "").strip()
    elif task_type == "regression":
        selection = str(summary.get("regressor") or "").strip()
    return f"{task_type.capitalize()} / {model_kind}{('-' + selection) if selection else ''}"



def _render_ml_prediction_upgrade_demo_panel(title: str, summary: dict):
    aggregate = summary.get("aggregate") or {}
    strategy_metrics = aggregate.get("strategy_metrics") or {}
    cutoff_dates = " / ".join(aggregate.get("cutoff_dates") or []) or "-"
    score_sources = ", ".join(strategy_metrics.get("score_sources") or []) or "-"

    st.markdown(f"##### {title}")
    metric_cols = st.columns(5)
    metric_cols[0].metric("窗口数", str(aggregate.get("window_count") or 0))
    metric_cols[1].metric("评估行数", str(aggregate.get("rows_evaluated_total") or 0))
    metric_cols[2].metric("Top1日均收益", _format_ratio_pct(strategy_metrics.get("average_daily_top1_return"), digits=2))

    if summary.get("task_type") == "classification":
        metric_cols[3].metric("平均准确率", _format_ratio_pct(aggregate.get("average_test_accuracy"), digits=1))
        metric_cols[4].metric("Top3日均收益", _format_ratio_pct(strategy_metrics.get("average_daily_top3_return"), digits=2))
    else:
        metric_cols[3].metric("平均RMSE", _format_decimal_metric(aggregate.get("average_test_rmse"), digits=4))
        metric_cols[4].metric("平均R²", _format_decimal_metric(aggregate.get("average_test_r2"), digits=4))

    st.caption(
        f"cutoffs: {cutoff_dates} ｜ score source: {score_sources} ｜ return column: {strategy_metrics.get('return_column') or '-'}"
    )

    left_col, right_col = st.columns([1, 2])
    with left_col:
        st.markdown("**策略聚合摘要**")
        st.dataframe(
            _build_ml_strategy_overview_df(strategy_metrics),
            width="stretch",
            hide_index=True,
        )
    with right_col:
        st.markdown("**分窗口结果**")
        st.dataframe(
            _build_ml_window_results_df(summary),
            width="stretch",
            hide_index=True,
        )

    with st.expander(f"查看 {title} 原始 summary", expanded=False):
        st.json(summary)


def render_ml_prediction_upgrade_tab():
    st.subheader("🤖 ML 预测升级（最小结果页）")
    st.caption("先把 ETF 预测升级的训练/评估进展挂到页面里。当前版本保持只读，不触发数据库写入，也不依赖 live DB 凭据。")

    st.info("这一页现在会优先读取 runtime walk-forward snapshot；如果还没有 snapshot，就自动回退到零 DB 依赖的 demo 结果。")

    capability_cols = st.columns(4)
    capability_cols[0].metric("评估模式", "Single / Walk-forward")
    capability_cols[1].metric("模型后端", "Baseline / Sklearn")
    capability_cols[2].metric("策略指标", "Top1 / Top3 / Top5")
    capability_cols[3].metric("默认分类目标", DEFAULT_CLASSIFICATION_TARGET)

    with st.expander("✅ 当前已落地能力", expanded=True):
        st.markdown(
            "\n".join([
                "- 数据层：`ml_stock_universe_daily / feature / label / sample` 已有保守脚手架",
                "- 训练层：支持 `baseline` 与 `sklearn` 两条路径",
                "- sklearn：当前支持 `LogisticRegression`、`Ridge`、`LinearRegression`",
                "- 评估层：支持单次切分（single）与滚动评估（walk-forward）",
                "- 策略层：walk-forward 已输出按交易日聚合的 Top1 / Top3 / Top5 指标",
                "- 页面层：已能展示 demo / runtime aggregate + 分窗口结果结构，后续可替换或扩展成更完整真实样本视图",
            ])
        )

    with st.expander("🧪 walk-forward 策略指标口径", expanded=True):
        st.markdown(
            "\n".join([
                "- 训练集使用 `trade_date < cutoff`，测试集使用 `trade_date >= cutoff`",
                "- 每个窗口只用训练集拟合缺失值填充，再应用到测试集",
                "- 分类优先使用 `test_scores` 排序；baseline 分类回退到 `score_feature`；其余回退到 prediction",
                "- 当前策略指标保持 V1 简化版：按日分组后统计 Top1 / Top3 / Top5 的平均收益与命中率",
                "- 暂未引入完整回测、仓位管理、交易成本或调参流程",
            ])
        )

    st.markdown("#### 🖥️ CLI 用法示例")
    st.code(
        "python scripts/train_ml_stock_v1.py --eval-mode walk-forward --model-kind sklearn --task-type classification --classifier logistic --min-train-rows 200 --min-test-rows 50 --max-windows 5 --json\n"
        "python scripts/train_ml_stock_v1.py --eval-mode walk-forward --model-kind sklearn --task-type regression --regressor ridge --target-column ret_fwd_5d --min-train-rows 200 --min-test-rows 50 --max-windows 5 --json",
        language="bash",
    )

    st.markdown("#### 🧭 当前代码配置")
    config_cols = st.columns(3)
    config_cols[0].write({
        "model_kinds": list(SUPPORTED_MODEL_KINDS),
        "classifiers": list(SUPPORTED_CLASSIFIERS),
        "regressors": list(SUPPORTED_REGRESSORS),
    })
    config_cols[1].write({
        "classification_target": DEFAULT_CLASSIFICATION_TARGET,
        "regression_target": DEFAULT_REGRESSION_TARGET,
        "topn_levels": [1, 3, 5],
    })
    config_cols[2].write({
        "status": "runtime snapshot ready",
        "live_db_smoke": "classification runtime verified",
        "page_mode": "read-only + runtime/demo results",
    })

    st.markdown("#### 📊 walk-forward 结果")
    st.caption("页面会优先读取 runtime walk-forward snapshot；如果当前没有 runtime snapshot，就回退到仓库内 demo snapshot / 内置 demo 结果。整个流程保持只读，不触发数据库写入。")

    snapshot_results = _load_ml_prediction_upgrade_snapshot_results()
    result_payload = snapshot_results
    result_source = "snapshot"
    if not result_payload:
        try:
            result_payload = _load_ml_prediction_upgrade_demo_results()
            result_source = "demo"
        except Exception as exc:
            logger.warning(f"load_ml_prediction_upgrade_demo_results failed: {exc}")
            st.warning("结果页数据生成失败，当前先保留说明页内容。")
            result_payload = None

    if result_payload:
        sample_overview = result_payload.get("sample_overview") or {}
        generated_at = str(result_payload.get("generated_at") or "").strip()
        source_label = "真实 snapshot" if result_source == "snapshot" else "内置 demo"
        source_caption = f"当前数据源：{source_label}"
        data_source = str(result_payload.get("data_source") or "").strip()
        if data_source:
            source_caption += f" ｜ source_tag: {data_source}"
        snapshot_path = str(result_payload.get("snapshot_path") or "").strip()
        if snapshot_path:
            source_caption += f" ｜ file: {os.path.relpath(snapshot_path, os.path.dirname(os.path.abspath(__file__)))}"
        if generated_at:
            source_caption += f" ｜ generated_at: {generated_at}"
        st.caption(source_caption)

        overview_cols = st.columns(4)
        overview_cols[0].metric("样本行数", str(sample_overview.get("row_count") or 0))
        overview_cols[1].metric("交易日数", str(sample_overview.get("day_count") or 0))
        overview_cols[2].metric("样本股票数", str(sample_overview.get("symbol_count") or 0))
        overview_cols[3].metric(
            "样本区间",
            f"{sample_overview.get('date_start') or '-'} → {sample_overview.get('date_end') or '-'}",
        )

        panel_defs = []
        if result_payload.get("classification"):
            cls_summary = result_payload.get("classification") or {}
            panel_defs.append(("📈 Classification", _build_ml_prediction_panel_title(cls_summary), cls_summary))
        if result_payload.get("regression"):
            reg_summary = result_payload.get("regression") or {}
            panel_defs.append(("📉 Regression", _build_ml_prediction_panel_title(reg_summary), reg_summary))

        if len(panel_defs) >= 2:
            tabs = st.tabs([panel[0] for panel in panel_defs])
            for tab, (_, title, panel_summary) in zip(tabs, panel_defs):
                with tab:
                    _render_ml_prediction_upgrade_demo_panel(title, panel_summary)
        elif len(panel_defs) == 1:
            _, title, panel_summary = panel_defs[0]
            _render_ml_prediction_upgrade_demo_panel(title, panel_summary)
        else:
            st.warning("snapshot 文件已读取，但没有识别到可展示的 classification / regression 结果。")

    st.markdown("#### 📌 下一步")
    st.markdown(
        "\n".join([
            "1. 用真实样本执行 walk-forward，并优先把 CLI 输出落成 runtime snapshot 文件覆盖页面当前数据源",
            "2. 明确 classification / regression 目标与策略指标解释口径",
            "3. 后续如需要，再接入图表、结果历史归档或更完整回测展示",
        ])
    )


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

    opp_df = build_opportunity_snapshot(
        top_up,
        top_avoid,
        mf_top,
        emotion_stage=emotion_stage,
        trade_date_hint=str(payload.get("trade_date") or ""),
    )
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
    display_df["新模型5日概率"] = pd.to_numeric(display_df.get("ml_new_prob_up_5d"), errors="coerce").map(lambda x: f"{x:.0%}" if pd.notna(x) else "-")
    display_df["新模型5日收益预测"] = pd.to_numeric(display_df.get("ml_new_pred_ret_5d"), errors="coerce").map(lambda x: f"{x:.2%}" if pd.notna(x) else "-")
    display_df["模型一致性"] = pd.to_numeric(display_df.get("model_agreement"), errors="coerce").map(lambda x: f"{x:.0%}" if pd.notna(x) else "-")
    display_df["融合权重"] = display_df.apply(lambda r: f"规则{_safe_float(r.get('blend_rule_weight'), 0.65):.0%} / 模型{_safe_float(r.get('blend_model_weight'), 0.35):.0%}", axis=1)
    display_df = display_df.rename(columns={"rank_commercial": "机会排名", "ts_code": "代码", "name": "名称", "industry": "行业", "reason": "原因", "action": "建议动作"})

    st.markdown("#### 🎯 今日机会清单")
    if is_pro:
        st.success("你正在查看 Pro 完整版（Top20 + 详细字段）。")
        cols = ["机会排名", "代码", "名称", "行业", "收盘价", "趋势分", "风险分", "5日概率", "20日概率", "模型5日概率", "新模型5日概率", "新模型5日收益预测", "模型一致性", "融合权重", "主力净流入(万元)", "机会分", "置信度", "建议动作", "原因"]
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
    st.caption("基于专业多因子趋势-风险调整模型，自动筛选 Top10 上涨趋势最强股票 与 Top10 避雷股票。仅供辅助分析，不构成投资建议。")

    snapshots = list_trend_recommendation_snapshots()
    if not snapshots:
        st.info("暂无每日趋势推荐结果，请先完成当日数据更新。")
        return

    snapshot_labels = [f"{item.get('trade_date', '-')}{'（最新）' if item.get('source') == 'latest' else ''}" for item in snapshots]
    default_index = next((idx for idx, item in enumerate(snapshots) if item.get('source') == 'latest'), 0)

    selector_cols = st.columns([1.4, 3])
    with selector_cols[0]:
        selected_label = st.selectbox(
            "查看交易日",
            options=snapshot_labels,
            index=default_index,
            key="daily_trend_reco_selected_snapshot",
        )
    selected_snapshot = snapshots[snapshot_labels.index(selected_label)]
    payload = load_trend_recommendations_snapshot(selected_snapshot)
    if not payload:
        st.warning("所选交易日的推荐数据读取失败，请稍后重试。")
        return

    selected_trade_date = str(payload.get('trade_date') or selected_snapshot.get('trade_date') or '-')
    is_latest = selected_snapshot.get('source') == 'latest'

    meta_cols = st.columns(4)
    meta_cols[0].metric("交易日", selected_trade_date)
    meta_cols[1].metric("样本池", f"{int(payload.get('universe_size') or 0):,}")
    meta_cols[2].metric("强势股数量", str(len(payload.get('top_uptrend', []) or [])))
    meta_cols[3].metric("避雷股数量", str(len(payload.get('top_avoid', []) or [])))

    generated_at = payload.get('generated_at') or '-'
    latest_hint = "（当前最新）" if is_latest else "（历史快照）"
    st.caption(f"生成时间：{generated_at} {latest_hint}")
    algorithm_meta = payload.get("algorithm") or {}
    algorithm_name = str(algorithm_meta.get("name") or "").strip()
    algorithm_version = str(algorithm_meta.get("version") or "").strip()
    algorithm_description = str(algorithm_meta.get("description") or "").strip()
    if algorithm_name:
        st.caption(f"推荐算法：{algorithm_name} v{algorithm_version or '-'}｜{algorithm_description}")
    else:
        st.caption("推荐算法：ProfessionalTrendRanker v1.0 多因子趋势-风险调整模型；当前快照未包含算法元数据，重新生成后会写入完整说明。")

    left, right = st.columns(2)

    from urllib.parse import quote

    render_nonce = st.session_state.get('daily_trend_reco_render_nonce', 0) + 1
    st.session_state['daily_trend_reco_render_nonce'] = render_nonce

    candidate_codes = tuple(
        pd.unique(
            pd.Series(
                [
                    str(row.get("ts_code") or "").strip()
                    for row in ((payload.get('top_uptrend', []) or []) + (payload.get('top_avoid', []) or []))
                    if str(row.get("ts_code") or "").strip()
                ]
            )
        ).tolist()
    )
    ml_new_scores = load_ml_prediction_candidate_scores(
        selected_trade_date,
        candidate_codes=candidate_codes,
    ) if selected_trade_date and selected_trade_date != '-' else pd.DataFrame()

    def _format_frame(rows: list[dict], mode: str) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=["查询", "排名", "代码", "名称", "行业", "收盘价", "趋势分", "风险分", "5日概率", "20日概率", "新模型5日概率", "新模型5日收益预测", "原因"])
        df = pd.DataFrame(rows).copy()
        for col_name, default_value in {
            "ml_new_prob_up_5d": np.nan,
            "ml_new_pred_ret_5d": np.nan,
        }.items():
            if col_name not in df.columns:
                df[col_name] = default_value
        if ml_new_scores is not None and not ml_new_scores.empty and "ts_code" in df.columns:
            score_df = ml_new_scores.copy()
            score_df["ts_code"] = score_df["ts_code"].astype(str)
            df["ts_code"] = df["ts_code"].astype(str)
            score_df = score_df.rename(columns={
                "ml_new_prob_up_5d": "ml_new_prob_up_5d_snapshot",
                "ml_new_pred_ret_5d": "ml_new_pred_ret_5d_snapshot",
            })
            df = df.merge(
                score_df[[c for c in ["ts_code", "ml_new_prob_up_5d_snapshot", "ml_new_pred_ret_5d_snapshot"] if c in score_df.columns]],
                on="ts_code",
                how="left",
            )
            for live_col, snap_col in {
                "ml_new_prob_up_5d": "ml_new_prob_up_5d_snapshot",
                "ml_new_pred_ret_5d": "ml_new_pred_ret_5d_snapshot",
            }.items():
                if snap_col in df.columns:
                    df[live_col] = pd.to_numeric(df.get(snap_col), errors="coerce").combine_first(
                        pd.to_numeric(df.get(live_col), errors="coerce")
                    )
                    df.drop(columns=[snap_col], inplace=True)
        df = df[["rank", "ts_code", "name", "industry", "close", "trend_score", "risk_score", "prob_up_5d", "prob_up_20d", "ml_new_prob_up_5d", "ml_new_pred_ret_5d", "reason"]]
        df.columns = ["排名", "代码", "名称", "行业", "收盘价", "趋势分", "风险分", "5日概率", "20日概率", "新模型5日概率", "新模型5日收益预测", "原因"]
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
        df["新模型5日概率"] = pd.to_numeric(df["新模型5日概率"], errors='coerce').map(lambda x: f"{x:.0%}" if pd.notna(x) else '-')
        df["新模型5日收益预测"] = pd.to_numeric(df["新模型5日收益预测"], errors='coerce').map(lambda x: f"{x:.2%}" if pd.notna(x) else '-')
        return df

    st.info("💡 点击表格最左侧“🔎 查看”可直接跳到“个股/指数查询”，查看该股票的详细走势分析。当前已并联展示 ML预测升级 新模型分数，但暂未用它直接改写 Top10 原始入选名单。")

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
        query_hotmoney_detail_dates,
        query_hotmoney_detail,
        query_hotmoney_top_active,
        query_hotmoney_top_stocks,
    )
    from src.hotmoney_tree import render_hotmoney_tree_html
    from src.hotmoney_window import (
        DAILY_QUERY_LABEL,
        HOTMONEY_HISTORY_START,
        HOTMONEY_WINDOW_OPTIONS,
        resolve_hotmoney_detail_date_window,
    )
    from src.moneyflow_fetcher import _get_engine_cached

    try:
        _hm_engine = _get_engine_cached()
        sync_meta = get_hotmoney_sync_meta(_hm_engine)
        latest_date = get_hotmoney_latest_detail_date(_hm_engine)
        available_detail_dates = query_hotmoney_detail_dates(limit=520, engine=_hm_engine)
    except Exception as e:
        st.error(f"游资数据初始化失败：{e}")
        return

    latest_dt = None
    latest_trade_label = latest_date if latest_date else "-"
    if latest_date:
        latest_dt = pd.to_datetime(latest_date, format="%Y%m%d").date()
        latest_trade_label = latest_dt.strftime("%Y-%m-%d")
    latest_sync_val = sync_meta.get("latest_ingested_at")
    latest_sync_label = "-"
    if latest_sync_val is not None and not pd.isna(latest_sync_val):
        latest_sync_label = pd.to_datetime(latest_sync_val).strftime("%Y-%m-%d %H:%M")

    meta_cols = st.columns(4)
    meta_cols[0].metric("游资名录数", f"{int(sync_meta.get('hm_list_count') or 0):,}")
    meta_cols[1].metric("游资明细行数", f"{int(sync_meta.get('hm_detail_count') or 0):,}")
    meta_cols[2].metric("最新明细交易日", latest_trade_label)
    meta_cols[3].metric("最近同步时间", latest_sync_label)

    render_single_stock_hotmoney_tracker(latest_dt)

    if (
        st.session_state.get("hm_detail_window") == "最近5日"
        and not st.session_state.get("hm_detail_window_daily_default_migrated")
    ):
        st.session_state["hm_detail_window"] = DAILY_QUERY_LABEL
        st.session_state["hm_detail_window_daily_default_migrated"] = True

    ctl1, ctl2, ctl3, ctl4, ctl5, ctl6 = st.columns([1.0, 1.0, 0.85, 0.95, 0.65, 1.0])
    with ctl1:
        hm_keyword = st.text_input("搜索游资名称", value="", key="hm_keyword")
    with ctl2:
        stock_keyword = st.text_input("聚焦股票/代码", value="", key="hm_stock_keyword")
    with ctl3:
        detail_window = st.selectbox("查询方式", HOTMONEY_WINDOW_OPTIONS, index=0, key="hm_detail_window")
    with ctl4:
        selected_trade_date = None
        if detail_window == DAILY_QUERY_LABEL:
            if available_detail_dates:
                current_trade_date = st.session_state.get("hm_daily_trade_date")
                default_trade_index = available_detail_dates.index(current_trade_date) if current_trade_date in available_detail_dates else 0
                selected_trade_date = st.selectbox(
                    "交易日期",
                    available_detail_dates,
                    index=default_trade_index,
                    key="hm_daily_trade_date",
                )
            else:
                selected_trade_date = st.date_input(
                    "交易日期",
                    value=latest_dt or datetime.today().date(),
                    min_value=HOTMONEY_HISTORY_START,
                    max_value=latest_dt or datetime.today().date(),
                    key="hm_daily_trade_date_input",
                )
        else:
            st.caption("交易日期")
            st.write("随窗口")
    with ctl5:
        top_n = st.selectbox("TopN", [10, 20, 30, 50], index=1, key="hm_topn")
    with ctl6:
        stock_rank_mode = st.selectbox("个股排序", ["按上榜次数", "按游资数", "按净买卖绝对值"], index=0, key="hm_stock_rank_mode")

    hm_list_df = query_hotmoney_list(name=hm_keyword or None, limit=300, engine=_hm_engine)
    st.markdown(
        """
        <div class="ws-hotmoney-section">
          <div class="ws-hotmoney-kicker">游资名录</div>
          <div class="ws-hotmoney-note">先看名录与关联机构，再往下看活跃游资、偏好个股和每日博弈明细。</div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("#### 🗂️ 游资名录总览")
    if hm_list_df is not None and not hm_list_df.empty:
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
            title=dict(text="游资关联机构数 Top20", x=0.02, font=dict(size=16, color=THEME_TEXT)),
            template="wealthspark_balanced",
            paper_bgcolor=CHART_PAPER_BG,
            plot_bgcolor=CHART_BG,
            font=dict(family="Inter, PingFang SC, sans-serif"),
            height=max(340, len(show) * 20),
            margin=dict(l=120, r=30, t=55, b=20),
            yaxis=dict(autorange="reversed"),
            xaxis_title="关联机构数",
        )
        st.plotly_chart(fig_org, use_container_width=True)

        out = hm_list_df[["hm_name", "hm_desc", "hm_orgs"]].copy()
        out.columns = ["游资名称", "说明", "关联机构"]
        st.dataframe(out, use_container_width=True, hide_index=True, height=380)
    else:
        st.info("暂无游资名录数据。")

    st.markdown("</div>", unsafe_allow_html=True)

    if latest_date:
        detail_date_window = resolve_hotmoney_detail_date_window(
            latest_date=latest_date,
            detail_window=detail_window,
            selected_date=selected_trade_date,
        )
        start_dt = detail_date_window.start_date
        end_dt = detail_date_window.end_date
        query_start = start_dt.strftime("%Y%m%d")
        query_end = end_dt.strftime("%Y%m%d")

        stock_order_by = "hit_count"
        if stock_rank_mode == "按游资数":
            stock_order_by = "hm_count"
        elif stock_rank_mode == "按净买卖绝对值":
            stock_order_by = "net_amount_abs"

        try:
            detail_limit = max(3000, int(top_n) * 160)
            df_active = query_hotmoney_top_active(query_start, query_end, top_n=int(top_n), engine=_hm_engine)
            df_stocks = query_hotmoney_top_stocks(query_start, query_end, top_n=int(top_n), order_by=stock_order_by, engine=_hm_engine)
            df_detail = query_hotmoney_detail(
                query_start,
                query_end,
                hm_name=hm_keyword or None,
                stock_keyword=stock_keyword or None,
                limit=detail_limit,
                engine=_hm_engine,
            )
        except Exception as e:
            st.error(f"游资明细查询失败：{e}")
            return

        st.markdown(
            """
            <div class="ws-hotmoney-section">
              <div class="ws-hotmoney-kicker">活跃榜</div>
            """,
            unsafe_allow_html=True,
        )
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
                title=dict(text="活跃游资 TopN", x=0.02, font=dict(size=16, color=THEME_TEXT)),
                template="wealthspark_balanced",
                paper_bgcolor=CHART_PAPER_BG,
                plot_bgcolor=CHART_BG,
                font=dict(family="Inter, PingFang SC, sans-serif"),
                height=max(340, len(show) * 20),
                margin=dict(l=120, r=30, t=55, b=20),
                yaxis=dict(autorange="reversed"),
                xaxis_title="上榜次数",
            )
            st.plotly_chart(fig_active, use_container_width=True)
            out = show[["hm_name", "hit_count", "stock_count", "total_net_amount_yi"]].copy()
            out.columns = ["游资", "上榜次数", "涉及股票数", "净买卖(亿)"]
            out["净买卖(亿)"] = out["净买卖(亿)"].map(lambda v: f"{v:,.2f}")
            st.dataframe(out, use_container_width=True, hide_index=True, height=380)
        else:
            st.info("当前窗口暂无活跃游资数据。")

        st.markdown("</div>", unsafe_allow_html=True)

        rank_mode_label = stock_rank_mode.replace("按", "")
        st.markdown(
            f"""
            <div class="ws-hotmoney-section">
              <div class="ws-hotmoney-kicker">偏好股</div>
            """,
            unsafe_allow_html=True,
        )
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
                title=dict(text="游资关注个股 TopN", x=0.02, font=dict(size=16, color=THEME_TEXT)),
                template="wealthspark_balanced",
                paper_bgcolor=CHART_PAPER_BG,
                plot_bgcolor=CHART_BG,
                font=dict(family="Inter, PingFang SC, sans-serif"),
                height=max(340, len(show) * 20),
                margin=dict(l=120, r=30, t=55, b=20),
                yaxis=dict(autorange="reversed"),
                xaxis_title=stock_x_title,
            )
            st.plotly_chart(fig_stocks, use_container_width=True)
            out = build_hotmoney_stock_preference_display_df(show)
            st.dataframe(
                out,
                use_container_width=True,
                hide_index=True,
                height=380,
                column_config={
                    "股票名称": st.column_config.LinkColumn(
                        "股票名称",
                        help="点击后跳转到个股/指数查询",
                        display_text=r".*#(.*)$",
                    )
                },
            )
        else:
            st.info("当前窗口暂无游资个股数据。")

        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown(
            """
            <div class="ws-hotmoney-section">
              <div class="ws-hotmoney-kicker">每日明细</div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("#### 🧾 游资博弈每日明细")
        if df_detail is not None and not df_detail.empty:
            detail_frame = prepare_hotmoney_detail_frame(df_detail)
            battle_summary = build_hotmoney_stock_battle_summary(detail_frame)
            battle_summary["total_net_abs_yi"] = battle_summary["total_net_yi"].abs()

            if stock_order_by == "hm_count":
                battle_summary = battle_summary.sort_values(
                    ["hm_count", "hit_count", "battle_amount_yi"],
                    ascending=[False, False, False],
                )
            elif stock_order_by == "net_amount_abs":
                battle_summary = battle_summary.sort_values(
                    ["total_net_abs_yi", "hit_count", "hm_count"],
                    ascending=[False, False, False],
                )
            else:
                battle_summary = battle_summary.sort_values(
                    ["hit_count", "hm_count", "battle_amount_yi"],
                    ascending=[False, False, False],
                )

            summary_cols = st.columns(4)
            summary_cols[0].metric("涉及股票", f"{len(battle_summary):,}")
            summary_cols[1].metric("参与游资", f"{int(detail_frame['hm_name'].nunique()):,}")
            summary_cols[2].metric("上榜记录", f"{len(detail_frame):,}")
            summary_cols[3].metric("合计净买卖(亿)", _format_hotmoney_yi(detail_frame["net_amount_yi"].sum(), signed=True))

            tree_subtitle_parts = [f"交易日：{detail_date_window.label}" if detail_window == DAILY_QUERY_LABEL else detail_date_window.label, f"Top{int(top_n)}"]
            if hm_keyword:
                tree_subtitle_parts.append(f"游资：{hm_keyword}")
            if stock_keyword:
                tree_subtitle_parts.append(f"股票：{stock_keyword}")
            st.markdown("#### 🌳 游资关系图")
            st.markdown(
                render_hotmoney_tree_html(
                    detail_frame,
                    title="游资龙虎图谱",
                    subtitle=" · ".join(tree_subtitle_parts),
                    max_hotmoney=min(int(top_n), 10),
                    max_stocks_per_hotmoney=6,
                    max_orgs_per_stock=4,
                ),
                unsafe_allow_html=True,
            )

            plot_df = battle_summary.head(int(top_n)).sort_values("battle_amount_yi", ascending=True)
            fig_battle = go.Figure(go.Bar(
                x=plot_df["battle_amount_yi"],
                y=plot_df["stock_label"],
                orientation="h",
                marker_color=[THEME_UP if v >= 0 else THEME_DOWN for v in plot_df["total_net_yi"]],
                text=plot_df.apply(lambda row: f"{int(row['hm_count'])}路/{int(row['hit_count'])}次", axis=1),
                textposition="outside",
                customdata=np.stack(
                    [
                        plot_df["total_net_yi"],
                        plot_df["main_hotmoney"],
                        plot_df["latest_hotmoney"],
                    ],
                    axis=-1,
                ),
                hovertemplate=(
                    "%{y}<br>"
                    "博弈强度：%{x:.2f} 亿<br>"
                    "累计净买卖：%{customdata[0]:+.2f} 亿<br>"
                    "主导游资：%{customdata[1]}<br>"
                    "最近动作：%{customdata[2]}<extra></extra>"
                ),
            ))
            fig_battle.update_layout(
                title=dict(text="个股游资博弈强度 TopN", x=0.02, font=dict(size=16, color=THEME_TEXT)),
                template="wealthspark_balanced",
                paper_bgcolor=CHART_PAPER_BG,
                plot_bgcolor=CHART_BG,
                font=dict(family="Inter, PingFang SC, sans-serif"),
                height=max(340, len(plot_df) * 24),
                margin=dict(l=140, r=55, t=55, b=30),
                xaxis_title="净买卖绝对额合计(亿)",
                yaxis=dict(autorange=False),
            )
            st.plotly_chart(fig_battle, use_container_width=True)

            display_summary = build_hotmoney_stock_battle_display_df(battle_summary.head(max(int(top_n), 20)))
            st.dataframe(
                display_summary,
                use_container_width=True,
                hide_index=True,
                height=360,
                column_config={
                    "股票": st.column_config.LinkColumn(
                        "股票",
                        help="点击后跳转到个股/指数查询",
                        display_text=r".*#(.*)$",
                    )
                },
            )

            if not battle_summary.empty:
                stock_options = battle_summary["ts_code"].tolist()
                if st.session_state.get("hm_focus_stock_select") not in stock_options:
                    st.session_state["hm_focus_stock_select"] = stock_options[0]
                stock_label_map = dict(zip(battle_summary["ts_code"], battle_summary["stock_label"]))
                selected_code = st.selectbox(
                    "单股追踪",
                    options=stock_options,
                    format_func=lambda code: stock_label_map.get(code, code),
                    key="hm_focus_stock_select",
                )
                focus_summary = battle_summary[battle_summary["ts_code"] == selected_code].iloc[0]
                focus_detail = detail_frame[detail_frame["ts_code"] == selected_code].copy()
                focus_label = stock_label_map.get(selected_code, selected_code)

                focus_cols = st.columns(4)
                focus_cols[0].metric("上榜次数", int(focus_summary["hit_count"]))
                focus_cols[1].metric("参与游资", int(focus_summary["hm_count"]))
                focus_cols[2].metric("交易日数", int(focus_summary["trade_days"]))
                focus_cols[3].metric("累计净买卖(亿)", _format_hotmoney_yi(focus_summary["total_net_yi"], signed=True))

                daily_focus = (
                    focus_detail.groupby("trade_date")
                    .agg(
                        net_amount_yi=("net_amount_yi", "sum"),
                        buy_amount_yi=("buy_amount_yi", "sum"),
                        sell_amount_yi=("sell_amount_yi", "sum"),
                        hit_count=("hm_name", "size"),
                        hm_count=("hm_name", "nunique"),
                    )
                    .reset_index()
                    .sort_values("trade_date")
                )
                daily_focus["trade_date_label"] = pd.to_datetime(daily_focus["trade_date"]).dt.strftime("%Y-%m-%d")

                chart_left, chart_right = st.columns([1.05, 1])
                with chart_left:
                    fig_daily = go.Figure(go.Bar(
                        x=daily_focus["trade_date_label"],
                        y=daily_focus["net_amount_yi"],
                        marker_color=[THEME_UP if v >= 0 else THEME_DOWN for v in daily_focus["net_amount_yi"]],
                        text=daily_focus["net_amount_yi"].map(lambda v: _format_hotmoney_yi(v, signed=True)),
                        textposition="outside",
                        hovertemplate="日期：%{x}<br>净买卖：%{y:+.2f} 亿<extra></extra>",
                    ))
                    fig_daily.add_hline(y=0, line_width=1, line_dash="dash", line_color=THEME_NEUTRAL)
                    fig_daily.update_layout(
                        title=dict(text=f"{focus_label} 每日净买卖", x=0.02, font=dict(size=15, color=THEME_TEXT)),
                        template="wealthspark_balanced",
                        paper_bgcolor=CHART_PAPER_BG,
                        plot_bgcolor=CHART_BG,
                        font=dict(family="Inter, PingFang SC, sans-serif"),
                        height=330,
                        margin=dict(l=45, r=25, t=55, b=45),
                        yaxis_title="净买卖(亿)",
                        xaxis_title="",
                    )
                    st.plotly_chart(fig_daily, use_container_width=True)

                with chart_right:
                    matrix_source = (
                        focus_detail.groupby(["hm_name", "trade_date"])["net_amount_yi"]
                        .sum()
                        .reset_index()
                    )
                    top_hm_names = (
                        focus_detail.groupby("hm_name")["abs_net_amount_yi"]
                        .sum()
                        .sort_values(ascending=False)
                        .head(12)
                        .index
                        .tolist()
                    )
                    matrix_source = matrix_source[matrix_source["hm_name"].isin(top_hm_names)]
                    pivot = (
                        matrix_source.pivot(index="hm_name", columns="trade_date", values="net_amount_yi")
                        .fillna(0)
                        .reindex(top_hm_names)
                    )
                    pivot.columns = [pd.to_datetime(c).strftime("%Y-%m-%d") for c in pivot.columns]
                    heat_text = [
                        [_format_hotmoney_yi(value, signed=True) if abs(float(value)) >= 0.005 else "" for value in row]
                        for row in pivot.to_numpy()
                    ]
                    fig_heat = go.Figure(go.Heatmap(
                        z=pivot.to_numpy(),
                        x=pivot.columns,
                        y=pivot.index,
                        text=heat_text,
                        texttemplate="%{text}",
                        colorscale=[[0, THEME_DOWN], [0.5, "#F2EFE7"], [1, THEME_UP]],
                        zmid=0,
                        colorbar=dict(title="亿"),
                        hovertemplate="游资：%{y}<br>日期：%{x}<br>净买卖：%{z:+.2f} 亿<extra></extra>",
                    ))
                    fig_heat.update_layout(
                        title=dict(text="日期 × 游资净买卖矩阵", x=0.02, font=dict(size=15, color=THEME_TEXT)),
                        template="wealthspark_balanced",
                        paper_bgcolor=CHART_PAPER_BG,
                        plot_bgcolor=CHART_BG,
                        font=dict(family="Inter, PingFang SC, sans-serif"),
                        height=max(330, len(pivot) * 28),
                        margin=dict(l=100, r=25, t=55, b=45),
                        xaxis_title="",
                        yaxis_title="",
                    )
                    st.plotly_chart(fig_heat, use_container_width=True)

                digest_df = build_hotmoney_daily_digest_df(focus_detail)
                st.dataframe(digest_df, use_container_width=True, hide_index=True, height=260)

                with st.expander("原始游资流水", expanded=False):
                    st.dataframe(
                        build_hotmoney_detail_display_df(focus_detail),
                        use_container_width=True,
                        hide_index=True,
                        height=420,
                    )
        else:
            st.info("当前窗口暂无游资明细数据。")

        st.markdown("</div>", unsafe_allow_html=True)
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
        line=dict(color=THEME_UP, width=2.5),
    ))
    fig_emotion.update_layout(
        title=dict(text="情绪趋势图", x=0.02, font=dict(size=17, color=THEME_TEXT)),
        template="wealthspark_balanced",
        paper_bgcolor=CHART_PAPER_BG,
        plot_bgcolor=CHART_BG,
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
                    marker_color=THEME_NAVY,
                ))
                fig_tag.add_trace(go.Bar(
                    x=show["tag"],
                    y=show["lb_count"],
                    name="连板股数",
                    marker_color=THEME_UP,
                ))
                fig_tag.update_layout(
                    barmode="group",
                    title=dict(text="同花顺标签分布", x=0.02, font=dict(size=15, color=THEME_TEXT)),
                    template="wealthspark_balanced",
                    paper_bgcolor=CHART_PAPER_BG,
                    plot_bgcolor=CHART_BG,
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
                    marker_color=THEME_DOWN,
                    name="出现次数",
                ))
                fig_reason.update_layout(
                    title=dict(text="涨停原因 Top10", x=0.02, font=dict(size=15, color=THEME_TEXT)),
                    template="wealthspark_balanced",
                    paper_bgcolor=CHART_PAPER_BG,
                    plot_bgcolor=CHART_BG,
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

def _build_factor_workbench_result_display_df(result_df: pd.DataFrame) -> pd.DataFrame:
    if result_df is None or result_df.empty:
        return pd.DataFrame()

    display_df = result_df.copy()
    display_df["标签"] = display_df.get("has_ever_st", False).map(format_historical_st_badge)
    if "fina_end_date" in display_df.columns:
        display_df["财报期"] = pd.to_datetime(display_df["fina_end_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    else:
        display_df["财报期"] = "-"

    selected_cols = [
        "ts_code",
        "name",
        "industry",
        "market",
        "final_score",
        "alpha095_cv",
        "close_over_ma20",
        "w_ema5_over_30",
        "net_mf_amount_rate",
        "dc_net_amount_rate",
        "roe",
        "pb",
        "财报期",
        "标签",
    ]
    selected_cols = [col for col in selected_cols if col in display_df.columns]
    out = display_df[selected_cols].copy()
    out = out.rename(
        columns={
            "ts_code": "代码",
            "name": "简称",
            "industry": "行业",
            "market": "市场",
            "final_score": "总分",
            "alpha095_cv": "Alpha095 CV",
            "close_over_ma20": "收盘/MA20",
            "w_ema5_over_30": "周EMA5/30",
            "net_mf_amount_rate": "主力流入/成交额",
            "dc_net_amount_rate": "DC流入占比",
            "roe": "ROE",
            "pb": "PB",
        }
    )

    numeric_formatters = {
        "总分": "{:,.1f}",
        "Alpha095 CV": "{:,.4f}",
        "收盘/MA20": "{:,.3f}",
        "周EMA5/30": "{:,.3f}",
        "主力流入/成交额": "{:,.4f}",
        "DC流入占比": "{:,.4f}",
        "ROE": "{:,.2f}",
        "PB": "{:,.2f}",
    }
    for col, formatter in numeric_formatters.items():
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").map(
                lambda value, fmt=formatter: "-" if pd.isna(value) else fmt.format(value)
            )

    if "财报期" in out.columns:
        out["财报期"] = out["财报期"].fillna("-")

    return out.fillna("-")


def render_factor_workbench_tab():
    st.subheader(FACTOR_WORKBENCH_PAGE_LABEL)
    st.caption("以日度特征表为锚点，先做硬条件初筛，再做多因子加权打分，输出每日盘后候选池。")

    try:
        trade_dates = load_factor_workbench_trade_dates_cached()
        freshness = load_factor_workbench_data_freshness_cached()
    except Exception as exc:
        st.error(f"因子工作台初始化失败：{exc}")
        return

    if not trade_dates:
        st.warning("暂无可用的日度因子锚点数据，请先构建 ml_stock_feature_daily。")
        return

    page_tab_workbench, page_tab_catalog, page_tab_freshness = st.tabs(["🛠 工作台", "📚 因子字典", "🕒 数据新鲜度"])

    with page_tab_workbench:
        latest_feature_date = freshness.get("feature_date") or str(pd.Timestamp(trade_dates[0]).date())
        selected_trade_date = st.selectbox(
            "交易日",
            options=trade_dates,
            index=0,
            format_func=lambda value: pd.Timestamp(value).strftime("%Y-%m-%d"),
            key="factor_workbench_trade_date",
        )
        selected_trade_date_text = pd.Timestamp(selected_trade_date).strftime("%Y-%m-%d")

        st.info(
            "💡 说明：量价、技术、估值和标准资金流按选定交易日对齐；财务因子使用每只股票最近一期财报快照。"
        )
        st.caption(
            f"当前锚点交易日：{selected_trade_date_text} ｜ 特征表最新：{latest_feature_date} ｜ "
            f"标准资金流最新：{freshness.get('moneyflow_date') or '-'} ｜ 财报最新披露期：{freshness.get('fina_end_date') or '-'}"
        )

        try:
            base_df = load_factor_workbench_frame_cached(selected_trade_date_text)
        except Exception as exc:
            st.error(f"加载因子底表失败：{exc}")
            return

        if base_df.empty:
            st.warning("选定交易日没有可用的因子底表数据。")
            return

        market_options = sorted([item for item in base_df["market"].dropna().astype(str).str.strip().unique().tolist() if item])
        industry_options = sorted([item for item in base_df["industry"].dropna().astype(str).str.strip().unique().tolist() if item])
        preset_names = list(get_score_preset(name).get("name") for name in ["均衡打分", "趋势动量", "质量价值", "资金驱动", "自定义"])
        factor_catalog = get_factor_catalog()
        factor_catalog_map = {item["key"]: item for item in factor_catalog}

        with st.expander("1. 股票池与输出范围", expanded=True):
            col_scope_1, col_scope_2, col_scope_3 = st.columns(3)
            with col_scope_1:
                selected_markets = st.multiselect(
                    "市场",
                    options=market_options,
                    default=market_options,
                    key="factor_workbench_markets",
                )
            with col_scope_2:
                selected_industries = st.multiselect(
                    "行业",
                    options=industry_options,
                    default=[],
                    key="factor_workbench_industries",
                )
            with col_scope_3:
                top_n = st.selectbox("展示 TopN", [20, 50, 100, 200], index=1, key="factor_workbench_topn")
                min_score = st.slider("最低总分", min_value=0, max_value=100, value=0, step=5, key="factor_workbench_min_score")

            col_scope_4, col_scope_5, col_scope_6 = st.columns(3)
            with col_scope_4:
                exclude_historical_st = st.checkbox("剔除历史ST", value=True, key="factor_workbench_exclude_historical_st")
            with col_scope_5:
                require_is_hs = st.checkbox("仅保留沪深港通", value=False, key="factor_workbench_require_is_hs")
            with col_scope_6:
                st.metric("锚点股票数", len(base_df))

        with st.expander("2. 硬条件初筛", expanded=True):
            liq_col_1, liq_col_2, liq_col_3 = st.columns(3)
            min_turnover_enabled = liq_col_1.checkbox("启用最小换手率", value=False, key="factor_filter_min_turnover_enabled")
            min_turnover_value = liq_col_1.number_input("最小换手率(%)", min_value=0.0, value=3.0, step=0.1, disabled=not min_turnover_enabled, key="factor_filter_min_turnover")
            min_amount_enabled = liq_col_2.checkbox("启用最小成交额", value=False, key="factor_filter_min_amount_enabled")
            min_amount_value = liq_col_2.number_input("最小成交额", min_value=0.0, value=5e8, step=1e8, disabled=not min_amount_enabled, key="factor_filter_min_amount")
            min_total_mv_enabled = liq_col_3.checkbox("启用最小总市值", value=False, key="factor_filter_min_total_mv_enabled")
            min_total_mv_value = liq_col_3.number_input("最小总市值", min_value=0.0, value=1e10, step=1e9, disabled=not min_total_mv_enabled, key="factor_filter_min_total_mv")

            tech_col_1, tech_col_2, tech_col_3 = st.columns(3)
            min_close_ma20_enabled = tech_col_1.checkbox("启用收盘/MA20下限", value=True, key="factor_filter_min_close_ma20_enabled")
            min_close_ma20_value = tech_col_1.number_input("收盘/MA20 >=", min_value=0.0, value=1.0, step=0.01, disabled=not min_close_ma20_enabled, key="factor_filter_min_close_ma20")
            min_ma5_ma20_enabled = tech_col_2.checkbox("启用MA5/MA20下限", value=False, key="factor_filter_min_ma5_ma20_enabled")
            min_ma5_ma20_value = tech_col_2.number_input("MA5/MA20 >=", min_value=0.0, value=1.0, step=0.01, disabled=not min_ma5_ma20_enabled, key="factor_filter_min_ma5_ma20")
            min_w_ema_enabled = tech_col_3.checkbox("启用周EMA5/30下限", value=False, key="factor_filter_min_w_ema_enabled")
            min_w_ema_value = tech_col_3.number_input("周EMA5/30 >=", min_value=0.0, value=1.0, step=0.01, disabled=not min_w_ema_enabled, key="factor_filter_min_w_ema")

            flow_col_1, flow_col_2, flow_col_3 = st.columns(3)
            min_mf_enabled = flow_col_1.checkbox("启用主力净流入额下限", value=False, key="factor_filter_min_mf_enabled")
            min_mf_value = flow_col_1.number_input("主力净流入额 >=", value=0.0, step=1e7, disabled=not min_mf_enabled, key="factor_filter_min_mf")
            min_mf_rate_enabled = flow_col_2.checkbox("启用主力流入/成交额下限", value=False, key="factor_filter_min_mf_rate_enabled")
            min_mf_rate_value = flow_col_2.number_input("主力流入/成交额 >=", value=0.0, step=0.001, format="%.4f", disabled=not min_mf_rate_enabled, key="factor_filter_min_mf_rate")
            min_dc_rate_enabled = flow_col_3.checkbox("启用DC流入占比下限", value=False, key="factor_filter_min_dc_rate_enabled")
            min_dc_rate_value = flow_col_3.number_input("DC流入占比 >=", value=0.0, step=0.001, format="%.4f", disabled=not min_dc_rate_enabled, key="factor_filter_min_dc_rate")

            val_col_1, val_col_2, val_col_3 = st.columns(3)
            min_pe_enabled = val_col_1.checkbox("启用PE下限", value=False, key="factor_filter_min_pe_enabled")
            min_pe_value = val_col_1.number_input("PE下限", value=0.0, step=1.0, disabled=not min_pe_enabled, key="factor_filter_min_pe")
            max_pe_enabled = val_col_2.checkbox("启用PE上限", value=False, key="factor_filter_max_pe_enabled")
            max_pe_value = val_col_2.number_input("PE上限", value=60.0, step=1.0, disabled=not max_pe_enabled, key="factor_filter_max_pe")
            max_pb_enabled = val_col_3.checkbox("启用PB上限", value=False, key="factor_filter_max_pb_enabled")
            max_pb_value = val_col_3.number_input("PB <=", value=5.0, step=0.1, disabled=not max_pb_enabled, key="factor_filter_max_pb")

            fin_col_1, fin_col_2, fin_col_3, fin_col_4 = st.columns(4)
            min_roe_enabled = fin_col_1.checkbox("启用ROE下限", value=False, key="factor_filter_min_roe_enabled")
            min_roe_value = fin_col_1.number_input("ROE >=", value=10.0, step=0.5, disabled=not min_roe_enabled, key="factor_filter_min_roe")
            min_gpm_enabled = fin_col_2.checkbox("启用毛利率下限", value=False, key="factor_filter_min_gpm_enabled")
            min_gpm_value = fin_col_2.number_input("毛利率 >=", value=20.0, step=0.5, disabled=not min_gpm_enabled, key="factor_filter_min_gpm")
            max_debt_enabled = fin_col_3.checkbox("启用资产负债率上限", value=False, key="factor_filter_max_debt_enabled")
            max_debt_value = fin_col_3.number_input("资产负债率 <=", value=60.0, step=1.0, disabled=not max_debt_enabled, key="factor_filter_max_debt")
            min_current_ratio_enabled = fin_col_4.checkbox("启用流动比率下限", value=False, key="factor_filter_min_current_ratio_enabled")
            min_current_ratio_value = fin_col_4.number_input("流动比率 >=", value=1.0, step=0.1, disabled=not min_current_ratio_enabled, key="factor_filter_min_current_ratio")

        with st.expander("3. 打分模型", expanded=True):
            selected_preset_name = st.selectbox("打分模板", options=preset_names, index=0, key="factor_workbench_preset")
            preset = get_score_preset(selected_preset_name)
            st.caption(preset.get("description", ""))

            factor_weights = dict(preset["factor_weights"])
            if selected_preset_name == "自定义":
                weight_cols = st.columns(2)
                editable_keys = list(factor_weights.keys())
                for idx, factor_key in enumerate(editable_keys):
                    meta = factor_catalog_map.get(factor_key, {"label": factor_key})
                    factor_weights[factor_key] = weight_cols[idx % 2].number_input(
                        f"{meta['label']} 权重",
                        min_value=0.0,
                        value=float(factor_weights[factor_key]),
                        step=0.1,
                        key=f"factor_workbench_weight_{factor_key}",
                    )
            else:
                weight_df = pd.DataFrame(
                    [
                        {
                            "因子": factor_catalog_map.get(factor_key, {"label": factor_key})["label"],
                            "字段": factor_key,
                            "权重": weight,
                        }
                        for factor_key, weight in factor_weights.items()
                        if weight > 0
                    ]
                )
                st.dataframe(weight_df, use_container_width=True, hide_index=True)

        if st.button("开始筛选并打分", type="primary", key="factor_workbench_run"):
            filters = {
                "markets": selected_markets,
                "industries": selected_industries,
                "exclude_historical_st": exclude_historical_st,
                "require_is_hs": require_is_hs,
                "min_turnover_rate_enabled": min_turnover_enabled,
                "min_turnover_rate": min_turnover_value,
                "min_amount_enabled": min_amount_enabled,
                "min_amount": min_amount_value,
                "min_total_mv_enabled": min_total_mv_enabled,
                "min_total_mv": min_total_mv_value,
                "min_close_over_ma20_enabled": min_close_ma20_enabled,
                "min_close_over_ma20": min_close_ma20_value,
                "min_ma5_over_ma20_enabled": min_ma5_ma20_enabled,
                "min_ma5_over_ma20": min_ma5_ma20_value,
                "min_w_ema5_over_30_enabled": min_w_ema_enabled,
                "min_w_ema5_over_30": min_w_ema_value,
                "min_net_mf_amount_enabled": min_mf_enabled,
                "min_net_mf_amount": min_mf_value,
                "min_net_mf_amount_rate_enabled": min_mf_rate_enabled,
                "min_net_mf_amount_rate": min_mf_rate_value,
                "min_dc_net_amount_rate_enabled": min_dc_rate_enabled,
                "min_dc_net_amount_rate": min_dc_rate_value,
                "min_pe_ttm_enabled": min_pe_enabled,
                "min_pe_ttm": min_pe_value,
                "max_pe_ttm_enabled": max_pe_enabled,
                "max_pe_ttm": max_pe_value,
                "max_pb_enabled": max_pb_enabled,
                "max_pb": max_pb_value,
                "min_roe_enabled": min_roe_enabled,
                "min_roe": min_roe_value,
                "min_grossprofit_margin_enabled": min_gpm_enabled,
                "min_grossprofit_margin": min_gpm_value,
                "max_debt_to_assets_enabled": max_debt_enabled,
                "max_debt_to_assets": max_debt_value,
                "min_current_ratio_enabled": min_current_ratio_enabled,
                "min_current_ratio": min_current_ratio_value,
            }

            filtered_df = apply_factor_filters(base_df, filters)
            if filtered_df.empty:
                st.session_state["factor_workbench_result"] = {
                    "selected_trade_date_text": selected_trade_date_text,
                    "anchor_count": int(len(base_df)),
                    "filtered_count": 0,
                    "ranked_df": pd.DataFrame(),
                    "display_top_df": pd.DataFrame(),
                }
            else:
                scored_df = compute_factor_scores(filtered_df, factor_weights=factor_weights)
                if min_score > 0:
                    scored_df = scored_df.loc[scored_df["final_score"] >= float(min_score)].copy()

                st.session_state["factor_workbench_result"] = {
                    "selected_trade_date_text": selected_trade_date_text,
                    "anchor_count": int(len(base_df)),
                    "filtered_count": int(len(filtered_df)),
                    "ranked_df": scored_df.copy(),
                    "display_top_df": scored_df.head(int(top_n)).copy(),
                }

        result_payload = st.session_state.get("factor_workbench_result")
        if result_payload and result_payload.get("selected_trade_date_text") == selected_trade_date_text:
            ranked_df = result_payload.get("ranked_df")
            display_top_df = result_payload.get("display_top_df")
            anchor_count = int(result_payload.get("anchor_count", len(base_df)))
            filtered_count = int(result_payload.get("filtered_count", 0))
            ranked_count = 0 if ranked_df is None else int(len(ranked_df))

            stat_col_1, stat_col_2, stat_col_3, stat_col_4 = st.columns(4)
            stat_col_1.metric("锚点股票数", anchor_count)
            stat_col_2.metric("初筛后数量", filtered_count)
            stat_col_3.metric("达标候选数", ranked_count)
            top_score = 0.0
            if display_top_df is not None and not display_top_df.empty:
                top_score = float(pd.to_numeric(display_top_df["final_score"], errors="coerce").iloc[0] or 0.0)
            stat_col_4.metric("最高分", f"{top_score:,.1f}")

            if ranked_df is None or ranked_df.empty:
                st.warning("当前条件下没有产生候选股，请放宽筛选条件或降低最低总分。")
            else:
                chart_df = display_top_df.copy()
                chart_df["display_name"] = chart_df.get("name", chart_df.get("ts_code", "")).fillna(chart_df.get("ts_code", "")).astype(str)
                fig = px.bar(
                    chart_df.head(10),
                    x="display_name",
                    y="final_score",
                    color="final_score",
                    text="final_score",
                    color_continuous_scale="Tealgrn",
                    title="候选池 Top10 总分",
                )
                fig.update_layout(
                    template="wealthspark_balanced",
                    paper_bgcolor=CHART_PAPER_BG,
                    plot_bgcolor=CHART_BG,
                    font=dict(family="Inter, PingFang SC, sans-serif"),
                    height=360,
                    xaxis_title="股票",
                    yaxis_title="总分",
                    coloraxis_showscale=False,
                )
                fig.update_traces(texttemplate="%{text:.1f}", textposition="outside")
                st.plotly_chart(fig, use_container_width=True)

                render_security_jump_table(
                    _build_factor_workbench_result_display_df(display_top_df),
                    help_text="💡 点击最左侧“🔎 查询”可跳转到个股/指数查询，并自动带入该股票代码。",
                    code_col="代码",
                    fallback_col="简称",
                    nonce_key="factor_workbench_render_nonce",
                )

                export_df = ranked_df.copy()
                for col in ["trade_date", "list_date", "fina_ann_date", "fina_end_date"]:
                    if col in export_df.columns:
                        export_df[col] = pd.to_datetime(export_df[col], errors="coerce").dt.strftime("%Y-%m-%d")
                st.download_button(
                    "下载候选池 CSV",
                    data=export_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                    file_name=f"factor_workbench_{selected_trade_date_text}.csv",
                    mime="text/csv",
                    key="factor_workbench_download_csv",
                )

    with page_tab_catalog:
        catalog_df = pd.DataFrame(get_factor_catalog())
        if not catalog_df.empty:
            catalog_df = catalog_df.rename(
                columns={
                    "key": "字段",
                    "label": "名称",
                    "group": "分组",
                    "higher_better": "方向",
                    "source": "来源",
                    "description": "说明",
                }
            )
            catalog_df["方向"] = catalog_df["方向"].map(lambda value: "高值更优" if bool(value) else "低值更优")
            st.dataframe(catalog_df, use_container_width=True, hide_index=True)
        else:
            st.info("暂无因子字典。")

    with page_tab_freshness:
        freshness_df = pd.DataFrame(
            [
                {"数据源": "ml_stock_feature_daily", "最新日期": freshness.get("feature_date") or "-", "说明": "页面主锚点"},
                {"数据源": "vw_moneyflow", "最新日期": freshness.get("moneyflow_date") or "-", "说明": "标准资金流"},
                {"数据源": "vw_moneyflow_ths", "最新日期": freshness.get("moneyflow_ths_date") or "-", "说明": "THS资金流"},
                {"数据源": "vw_moneyflow_dc", "最新日期": freshness.get("moneyflow_dc_date") or "-", "说明": "DC资金流"},
                {"数据源": "ts_stock_technical_signals", "最新日期": freshness.get("tech_signal_date") or "-", "说明": "周月线EMA"},
                {"数据源": "vw_ts_stock_fina_indicator.end_date", "最新日期": freshness.get("fina_end_date") or "-", "说明": "最新财报期"},
                {"数据源": "vw_ts_stock_fina_indicator.ann_date", "最新日期": freshness.get("fina_ann_date") or "-", "说明": "最新披露日"},
            ]
        )
        st.dataframe(freshness_df, use_container_width=True, hide_index=True)
        st.info("财务因子使用最近一期财报快照，不与交易日严格同步；页面会同时展示对应财报期。")


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

def _format_company_screener_list_date(value) -> str:
    if value is None or pd.isna(value):
        return ""
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.notna(parsed):
        return parsed.strftime("%Y-%m-%d")
    return str(value).strip()


def build_company_screener_result_action_df(
    results_df: pd.DataFrame,
    existing_watchlist_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if results_df is None or results_df.empty:
        return pd.DataFrame(columns=["选择", "序号", "查询", "代码", "简称", "行业", "上市日期", "标签", "已在自选", "主要业务", "产品及服务"])

    existing_codes: set[str] = set()
    if isinstance(existing_watchlist_df, pd.DataFrame) and not existing_watchlist_df.empty and "ts_code" in existing_watchlist_df.columns:
        existing_codes = {
            str(code or "").strip().upper()
            for code in existing_watchlist_df["ts_code"].tolist()
            if str(code or "").strip()
        }

    base_df = pd.DataFrame({
        "代码": results_df["ts_code"].astype(str).str.strip().str.upper(),
        "简称": results_df["name"].fillna("").astype(str).str.strip(),
    })
    query_links = build_security_jump_links(
        base_df,
        code_col="代码",
        fallback_col="简称",
        nonce_key="company_screener_jump_render_nonce",
    )

    action_df = pd.DataFrame({
        "选择": [False] * len(results_df),
        "序号": range(1, len(results_df) + 1),
        "查询": query_links,
        "代码": base_df["代码"],
        "简称": base_df["简称"],
        "行业": results_df.get("industry", pd.Series([""] * len(results_df))).fillna("").astype(str).str.strip(),
        "上市日期": results_df.get("list_date", pd.Series([""] * len(results_df))).map(_format_company_screener_list_date),
        "主要业务": results_df.get("main_business", pd.Series([""] * len(results_df))).fillna("").astype(str),
        "产品及服务": results_df.get("product", pd.Series([""] * len(results_df))).fillna("").astype(str),
    })
    if "has_ever_st" in results_df.columns:
        action_df["标签"] = results_df["has_ever_st"].map(lambda value: "曾经ST" if bool(value) else "")
    else:
        action_df["标签"] = ""
    action_df["已在自选"] = action_df["代码"].map(lambda code: "✅ 已在自选" if code in existing_codes else "")
    return action_df


COMPANY_SCREENER_TIME_FILTER_OPTIONS = ("全部", "指定时间段", "最近1个月", "最近2个月", "最近3个月")
COMPANY_SCREENER_RELATIVE_MONTHS = {
    "最近1个月": 1,
    "最近2个月": 2,
    "最近3个月": 3,
}


def _coerce_company_screener_date(value, fallback=None):
    if value is None:
        return fallback
    if isinstance(value, datetime):
        return value.date()
    if hasattr(value, "date") and not isinstance(value, str):
        try:
            return value.date()
        except Exception:
            pass
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.notna(parsed):
        return parsed.date()
    return fallback


def _resolve_company_screener_list_date_filter(time_filter: str, start_date=None, end_date=None) -> tuple[str | None, str | None]:
    today = datetime.now().date()
    if time_filter == "指定时间段":
        normalized_start = _coerce_company_screener_date(start_date, today)
        normalized_end = _coerce_company_screener_date(end_date, normalized_start)
        return normalized_start.isoformat(), normalized_end.isoformat()

    months = COMPANY_SCREENER_RELATIVE_MONTHS.get(time_filter)
    if months:
        start_date = (pd.Timestamp(today) - pd.DateOffset(months=months)).date()
        return start_date.isoformat(), today.isoformat()

    return None, None



def add_company_screener_rows_to_watchlist(
    selected_rows: list[dict],
    username: str,
    *,
    existing_watchlist_df: pd.DataFrame | None = None,
    report_engine=None,
) -> dict:
    normalized_username = normalize_username(username)
    try:
        logger.info(
            "add_company_screener_rows_to_watchlist called | raw_user=%s | normalized_user=%s | row_count=%s | codes=%s",
            username,
            normalized_username,
            len(selected_rows or []),
            [str((row or {}).get("代码") or (row or {}).get("ts_code") or "") for row in (selected_rows or [])[:10]],
        )
    except Exception:
        pass
    if not normalized_username:
        raise ValueError("username 不能为空")

    existing_codes: set[str] = set()
    if isinstance(existing_watchlist_df, pd.DataFrame) and not existing_watchlist_df.empty and "ts_code" in existing_watchlist_df.columns:
        existing_codes = {
            str(code or "").strip().upper()
            for code in existing_watchlist_df["ts_code"].tolist()
            if str(code or "").strip()
        }

    added_codes: list[str] = []
    skipped_existing = 0
    skipped_invalid = 0
    failed_items: list[str] = []

    for row in selected_rows:
        ts_code = str(
            row.get("代码")
            or row.get("ts_code")
            or ""
        ).strip().upper()
        security_name = str(
            row.get("简称")
            or row.get("name")
            or row.get("security_name")
            or ts_code
        ).strip()
        if not ts_code:
            skipped_invalid += 1
            continue
        if ts_code in existing_codes:
            skipped_existing += 1
            continue

        try:
            add_watchlist_item(
                normalized_username,
                ts_code,
                security_name=security_name,
                security_type="stock",
            )
        except Exception as exc:
            failed_items.append(f"{ts_code}: {exc}")
            continue

        existing_codes.add(ts_code)
        added_codes.append(ts_code)

    if report_engine is not None:
        for ts_code in added_codes:
            try:
                trigger_single_distribution_refresh_bg(normalized_username, ts_code, report_engine)
                trigger_single_stock_research_refresh_bg(normalized_username, ts_code, report_engine)
            except Exception as trigger_exc:
                logger.warning(
                    "Failed to trigger watchlist refresh for %s / %s: %s",
                    normalized_username,
                    ts_code,
                    trigger_exc,
                )

    return {
        "added": len(added_codes),
        "added_codes": added_codes,
        "skipped_existing": skipped_existing,
        "skipped_invalid": skipped_invalid,
        "failed": len(failed_items),
        "failed_items": failed_items,
    }


def import_uploaded_watchlist_to_user(
    uploaded_file,
    username: str,
    *,
    existing_watchlist_df: pd.DataFrame | None = None,
    report_engine=None,
) -> dict:
    normalized_username = normalize_username(username)
    if not normalized_username:
        raise ValueError("Please login before importing watchlist items")

    parsed_rows = parse_watchlist_import_workbook(uploaded_file)
    actual_existing_df = (
        existing_watchlist_df
        if existing_watchlist_df is not None
        else list_watchlist_items(normalized_username)
    )
    summary = import_watchlist_rows(
        normalized_username,
        parsed_rows,
        existing_watchlist_df=actual_existing_df,
    )
    summary["parsed"] = len(parsed_rows)

    if report_engine is not None and summary.get("added", 0) > 0:
        trigger_watchlist_bulk_refresh_bg(normalized_username, report_engine)

    return summary


def trigger_watchlist_bulk_refresh_bg(username: str, engine) -> None:
    import threading

    normalized_username = normalize_username(username)
    if not normalized_username or engine is None:
        return

    def _worker():
        try:
            logger.info("Started watchlist distribution bulk refresh after Excel import for %s", normalized_username)
            dist_summary = refresh_watchlist_distribution_reports(engine, username=normalized_username)
            logger.info(
                "Finished watchlist distribution bulk refresh after Excel import for %s: %s",
                normalized_username,
                dist_summary,
            )
        except Exception as dist_exc:
            logger.error(
                "Watchlist distribution bulk refresh after Excel import crashed for %s: %s",
                normalized_username,
                dist_exc,
            )

        try:
            logger.info("Started stock research bulk refresh after Excel import for %s", normalized_username)
            research_summary = refresh_watchlist_stock_research_reports(
                engine,
                username=normalized_username,
                force=False,
            )
            logger.info(
                "Finished stock research bulk refresh after Excel import for %s: %s",
                normalized_username,
                research_summary,
            )
        except Exception as research_exc:
            logger.error(
                "Stock research bulk refresh after Excel import crashed for %s: %s",
                normalized_username,
                research_exc,
            )

    t = threading.Thread(target=_worker, daemon=True)
    t.start()



def render_company_screener_tab():
    st.subheader("🏢 公司主营与产品筛选")
    st.caption("按照行业、产品和主营业务关键词筛选公司，并可对当前筛选结果逐只订正主营与产品信息。")

    result_state_key = "company_screener_results"
    filter_state_key = "company_screener_filters"
    flash_state_key = "company_screener_flash"

    flash_message = st.session_state.pop(flash_state_key, None)
    if flash_message:
        level = flash_message.get("level", "info")
        message = flash_message.get("message", "")
        if level == "success":
            st.success(message)
        elif level == "warning":
            st.warning(message)
        elif level == "error":
            st.error(message)
        else:
            st.info(message)

    existing_filters = st.session_state.get(filter_state_key, {})
    default_industries = existing_filters.get("industries") or ["全部"]
    default_time_filter = existing_filters.get("time_filter") or "全部"
    if default_time_filter == "指定日期":
        default_time_filter = "指定时间段"
    if default_time_filter not in COMPANY_SCREENER_TIME_FILTER_OPTIONS:
        default_time_filter = "全部"
    today_for_company_screener = datetime.now().date()
    default_list_date_start = _coerce_company_screener_date(
        existing_filters.get("selected_list_start_date") or existing_filters.get("selected_list_date"),
        (pd.Timestamp(today_for_company_screener) - pd.DateOffset(months=1)).date(),
    )
    default_list_date_end = _coerce_company_screener_date(
        existing_filters.get("selected_list_end_date") or existing_filters.get("selected_list_date"),
        today_for_company_screener,
    )

    col1, col2, col3 = st.columns([1.5, 1.5, 1.5])
    with col1:
        raw_industries = get_stock_basic_summary()["所属行业"].dropna().unique().tolist()
        industries = [industry for industry in raw_industries if str(industry).strip()]
        selected_industries = st.multiselect(
            "所属行业",
            options=["全部"] + sorted(industries),
            default=default_industries,
            key="company_screener_industries",
        )
    with col2:
        product_kw = st.text_input(
            "核心产品关键词",
            value=existing_filters.get("product_kw", ""),
            placeholder="例如: 芯片, 新能源",
            key="company_screener_product_kw",
        )
    with col3:
        business_kw = st.text_input(
            "服务/主营业务关键词",
            value=existing_filters.get("business_kw", ""),
            placeholder="例如: 研发, 制造",
            key="company_screener_business_kw",
        )

    time_col1, time_col2, time_col3, time_col4 = st.columns([1.1, 1.2, 1.2, 1.8])
    with time_col1:
        time_filter = st.selectbox(
            "上市日期",
            options=list(COMPANY_SCREENER_TIME_FILTER_OPTIONS),
            index=COMPANY_SCREENER_TIME_FILTER_OPTIONS.index(default_time_filter),
            key="company_screener_time_filter",
        )
    if time_filter == "指定时间段":
        with time_col2:
            selected_list_start_date = st.date_input(
                "开始日期",
                value=default_list_date_start,
                key="company_screener_selected_list_start_date",
            )
        with time_col3:
            selected_list_end_date = st.date_input(
                "结束日期",
                value=default_list_date_end,
                key="company_screener_selected_list_end_date",
            )
    else:
        selected_list_start_date = default_list_date_start
        selected_list_end_date = default_list_date_end

    list_date_start, list_date_end = _resolve_company_screener_list_date_filter(
        time_filter,
        selected_list_start_date,
        selected_list_end_date,
    )
    is_invalid_list_date_range = bool(list_date_start and list_date_end and list_date_start > list_date_end)
    if list_date_start and list_date_end:
        if is_invalid_list_date_range:
            time_col4.warning("开始日期不能晚于结束日期。")
        else:
            time_col4.caption(f"筛选上市日期：{list_date_start} 至 {list_date_end}")
    else:
        time_col4.caption("不限制上市日期")

    if st.button(
        "开始筛选",
        type="primary",
        key="company_screener_submit",
        disabled=is_invalid_list_date_range,
    ):
        with st.spinner("正在检索符合条件的公司..."):
            df = search_companies(
                industries=selected_industries,
                product_kw=product_kw,
                business_kw=business_kw,
                list_date_start=list_date_start,
                list_date_end=list_date_end,
            )
            st.session_state[result_state_key] = df
            st.session_state.pop("company_screener_action_df_cache", None)
            st.session_state[filter_state_key] = {
                "industries": selected_industries,
                "product_kw": product_kw,
                "business_kw": business_kw,
                "time_filter": time_filter,
                "selected_list_start_date": _coerce_company_screener_date(selected_list_start_date, today_for_company_screener).isoformat(),
                "selected_list_end_date": _coerce_company_screener_date(selected_list_end_date, today_for_company_screener).isoformat(),
                "list_date_start": list_date_start,
                "list_date_end": list_date_end,
            }
            if df.empty:
                st.warning("没有检索到符合条件的公司，请尝试调整关键词。")

    results_df = st.session_state.get(result_state_key)
    if not isinstance(results_df, pd.DataFrame) or results_df.empty:
        return

    st.success(f"共为您检索到 {len(results_df)} 家企业。")
    current_username = get_logged_in_username()
    watchlist_df = pd.DataFrame()
    if current_username:
        try:
            watchlist_df = list_watchlist_items(current_username)
        except Exception as exc:
            st.warning(f"读取当前自选失败：{exc}")
            watchlist_df = pd.DataFrame()

    # --- Build or reuse cached action_df to keep data_editor input stable ---
    action_df_cache_key = "company_screener_action_df_cache"
    cached_results_len = st.session_state.get("company_screener_cached_results_len", -1)
    need_rebuild = (
        action_df_cache_key not in st.session_state
        or cached_results_len != len(results_df)
        or st.session_state.pop("company_screener_force_rebuild", False)
    )
    if need_rebuild:
        action_df = build_company_screener_result_action_df(results_df, watchlist_df)
        st.session_state[action_df_cache_key] = action_df
        st.session_state["company_screener_cached_results_len"] = len(results_df)
    else:
        action_df = st.session_state[action_df_cache_key]

    # --- Apply pending bulk-select / clear actions (set by buttons below) ---
    pending_screener_action = st.session_state.pop("company_screener_pending_action", None)
    if pending_screener_action == "select_all":
        action_df = action_df.copy()
        action_df["选择"] = action_df["已在自选"] != "✅ 已在自选"
        st.session_state[action_df_cache_key] = action_df
    elif pending_screener_action == "clear_all":
        action_df = action_df.copy()
        action_df["选择"] = False
        st.session_state[action_df_cache_key] = action_df
    
    st.info("💡 在这张结果表里直接勾选股票，可批量加入自选或自选池；点击“查询”即可跳到个股/指数查询。")
    edited_action_df = st.data_editor(
        action_df,
        use_container_width=True,
        hide_index=True,
        disabled=[col for col in action_df.columns if col != "选择"],
        column_config={
            "选择": st.column_config.CheckboxColumn("选择", help="勾选后可加入自选或自选池"),
            "查询": st.column_config.LinkColumn(
                "查询",
                help="点击后跳转到个股/指数查询",
                display_text="🔎 查询",
            ),
            "标签": st.column_config.TextColumn("标签", width="small"),
            "已在自选": st.column_config.TextColumn("已在自选", width="small"),
            "上市日期": st.column_config.TextColumn("上市日期", width="small"),
            "主要业务": st.column_config.TextColumn("主要业务", width="large"),
            "产品及服务": st.column_config.TextColumn("产品及服务", width="large"),
        },
        key="company_screener_result_editor",
    )
    
    selected_company_rows = edited_action_df[edited_action_df["选择"]].to_dict(orient="records") if not edited_action_df.empty else []
    selected_count = len(selected_company_rows)
    can_select_all = bool(action_df[action_df["已在自选"] != "✅ 已在自选"].shape[0])
    action_cols = st.columns([1.15, 1.15, 1.45, 1.55, 2.1])
    if action_cols[0].button("全选未入自选", key="company_screener_watchlist_select_all", disabled=not can_select_all):
        st.session_state["company_screener_pending_action"] = "select_all"
        st.rerun()
    if action_cols[1].button("清空勾选", key="company_screener_watchlist_clear_all", disabled=action_df.empty):
        st.session_state["company_screener_pending_action"] = "clear_all"
        st.rerun()
    if current_username:
        if action_cols[2].button("加入选中自选", key="company_screener_watchlist_add_selected", disabled=selected_count == 0):
            try:
                report_engine = get_security_intraday_engine_cached()
                summary = add_company_screener_rows_to_watchlist(
                    selected_company_rows,
                    current_username,
                    existing_watchlist_df=watchlist_df,
                    report_engine=report_engine,
                )
            except Exception as exc:
                logger.exception("company_screener add selected failed | user=%s", current_username)
                st.error(f"加入自选失败：{exc}")
            else:
                message_parts = [f"已加入 {summary['added']} 只"]
                if summary["skipped_existing"]:
                    message_parts.append(f"跳过已在自选 {summary['skipped_existing']} 只")
                if summary["skipped_invalid"]:
                    message_parts.append(f"跳过无效 {summary['skipped_invalid']} 只")
                if summary["failed"]:
                    message_parts.append(f"失败 {summary['failed']} 只")
                flash_level = "warning" if summary["failed"] and summary["added"] == 0 else "success"
                flash_message = "，".join(message_parts)
                if summary["failed_items"]:
                    flash_message = f"{flash_message}。失败明细：{'；'.join(summary['failed_items'][:3])}"
                st.session_state[flash_state_key] = {
                    "level": flash_level,
                    "message": flash_message,
                }
                st.session_state["company_screener_force_rebuild"] = True
                st.rerun()
        if action_cols[3].button("加入选中自选池", key="company_screener_stock_pool_add_selected", disabled=selected_count == 0):
            try:
                pool_summary = import_stock_pool_rows(
                    current_username,
                    selected_company_rows,
                    source_file="公司主营与产品筛选",
                )
            except Exception as exc:
                logger.exception("company_screener add selected to stock pool failed | user=%s", current_username)
                st.error(f"加入自选池失败：{exc}")
            else:
                message_parts = [
                    f"新增 {pool_summary.get('added', 0)} 只",
                    f"更新 {pool_summary.get('updated', 0)} 只",
                ]
                if pool_summary.get("skipped_invalid"):
                    message_parts.append(f"跳过无效 {pool_summary.get('skipped_invalid')} 只")
                if pool_summary.get("failed"):
                    message_parts.append(f"失败 {pool_summary.get('failed')} 只")
                has_pool_changes = bool(pool_summary.get("added") or pool_summary.get("updated"))
                flash_level = "success" if has_pool_changes else "warning"
                flash_message = "自选池：" + "，".join(message_parts)
                if pool_summary.get("failed_items"):
                    flash_message = f"{flash_message}。失败明细：{'；'.join(pool_summary.get('failed_items', [])[:3])}"
                st.session_state[flash_state_key] = {
                    "level": flash_level,
                    "message": flash_message,
                }
                st.rerun()
        action_cols[4].caption(f"当前登录用户：{current_username}｜已勾选 {selected_count} 只")
    else:
        action_cols[2].button("加入选中自选", key="company_screener_watchlist_add_selected_disabled", disabled=True)
        action_cols[3].button("加入选中自选池", key="company_screener_stock_pool_add_selected_disabled", disabled=True)
        action_cols[4].info("先登录用户名，才能把结果表里的股票加入个人自选或自选池。")

    st.markdown("#### ✏️ 逐只订正主营与产品信息")
    st.caption("筛选结果中的每只股票都可以单独展开编辑；每次只会保存当前这一只。")

    configured_password = get_stock_info_edit_password()
    if not configured_password:
        st.warning("当前未配置编辑权限密码，修改功能已禁用。请设置 ETF_STOCK_INFO_EDIT_PASSWORD 或 ETF_EDIT_PASSWORD 后重启应用。")
        return

    status_cols = st.columns([4, 1.2])
    if has_stock_info_edit_permission():
        status_cols[0].success("当前会话已获得个股信息修改权限。")
        if status_cols[1].button("退出权限", key="revoke_stock_edit_permission_company_screener"):
            st.session_state["stock_info_edit_authorized"] = False
            st.rerun()
    else:
        access_password = status_cols[0].text_input(
            "编辑权限密码",
            type="password",
            key="stock_edit_password_company_screener",
        )
        if status_cols[1].button("权限验证", key="grant_stock_edit_permission_company_screener"):
            if grant_stock_info_edit_permission(access_password):
                st.success("权限验证成功，请继续逐只提交修订内容。")
                st.rerun()
            st.error("权限验证失败，请检查密码。")
        st.info("仅通过权限验证的会话可以修改个股主营与产品信息。")

    if not has_stock_info_edit_permission():
        return

    st.caption(f"当前筛选结果共 {len(results_df)} 家公司，可逐只展开编辑。")

    for row_index, row in results_df.reset_index(drop=True).iterrows():
        ts_code = str(row.get("ts_code") or "").strip()
        if not ts_code:
            continue

        name = str(row.get("name") or ts_code).strip() or ts_code
        industry = str(row.get("industry") or "-").strip() or "-"
        current_mb = row.get("main_business") or ""
        current_pd = row.get("product") or ""
        form_key = f"company_screener_single_edit_form_{ts_code}"
        mb_key = f"company_screener_edit_mb_{ts_code}"
        pd_key = f"company_screener_edit_pd_{ts_code}"

        with st.expander(f"{row_index + 1}. {name}（{ts_code}）｜{industry}", expanded=False):
            info_cols = st.columns(2)
            with info_cols[0]:
                st.info(f"**当前主要业务**：{current_mb or '-'}")
            with info_cols[1]:
                st.info(f"**当前产品及业务范围**：{current_pd or '-'}")

            with st.form(key=form_key):
                custom_mb = st.text_area(
                    "新的主要业务",
                    value=current_mb,
                    key=mb_key,
                )
                custom_pd = st.text_area(
                    "新的产品及业务范围",
                    value=current_pd,
                    key=pd_key,
                )
                if st.form_submit_button("保存当前股票修订"):
                    if not has_stock_info_edit_permission():
                        st.error("当前会话没有修改权限，请重新完成权限验证。")
                    else:
                        validation = validate_stock_custom_info_inputs(
                            custom_mb,
                            custom_pd,
                            current_main_business=current_mb,
                            current_product=current_pd,
                            check_unchanged=True,
                        )
                        if validation["action"] == "invalid":
                            st.error("保存失败：修订内容过短。若要清空请完全留白，否则请填写有效的业务信息。")
                        elif validation["action"] == "unchanged":
                            st.warning("您未做任何实质性修改。")
                        else:
                            update_stock_custom_info(
                                ts_code,
                                validation["main_business"],
                                validation["product"],
                            )
                            latest_filters = st.session_state.get(filter_state_key, {})
                            refreshed_df = search_companies(
                                industries=latest_filters.get("industries"),
                                product_kw=latest_filters.get("product_kw"),
                                business_kw=latest_filters.get("business_kw"),
                                list_date_start=latest_filters.get("list_date_start"),
                                list_date_end=latest_filters.get("list_date_end"),
                            )
                            st.session_state[result_state_key] = refreshed_df
                            st.session_state.pop("company_screener_action_df_cache", None)
                            st.session_state.pop(mb_key, None)
                            st.session_state.pop(pd_key, None)
                            if validation["action"] == "clear":
                                message = f"已清空 {name}（{ts_code}） 的自定义主营与产品信息。"
                            else:
                                message = f"已更新 {name}（{ts_code}） 的主营与产品信息。"
                            st.session_state[flash_state_key] = {
                                "level": "success",
                                "message": message,
                            }
                            st.rerun()


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

    # 1. 指标选择器
    metric_types = sorted(df['metric_type'].unique())

    if len(metric_types) == 0:
        st.error("❌ 未检测到任何指标数据，请检查Excel文件格式")
        st.info("Excel文件应包含section标题行，标题中应包含关键词：市值、份额、变动、申赎、比例、涨跌幅")
        st.stop()

    metric_categories = build_metric_categories(metric_types)

    def resolve_metric_category(metric_name: str | None) -> str:
        for category_name, metrics in metric_categories.items():
            if metric_name in metrics:
                return category_name
        return next(iter(metric_categories))

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
        st.success(f"✅ 已加载 {len(df)} 条数据记录")

        quick_metric_groups = build_quick_metric_groups(metric_types)
        category_options = list(metric_categories.keys())
        chart_options = ['line', 'area', 'scatter']
        pending_metric = st.session_state.pop("etf_pending_metric", None)
        pending_metric_category = st.session_state.pop("etf_pending_metric_category", None)

        if pending_metric in metric_types:
            resolved_pending_category = resolve_metric_category(pending_metric)
            if (
                pending_metric_category in category_options and
                pending_metric in metric_categories.get(pending_metric_category, [])
            ):
                resolved_pending_category = pending_metric_category
            st.session_state["etf_selected_metric"] = pending_metric
            st.session_state["etf_metric_category"] = resolved_pending_category

        if st.session_state.get("etf_selected_metric") not in metric_types:
            st.session_state["etf_selected_metric"] = metric_types[0]

        if st.session_state.get("etf_metric_category") not in category_options:
            st.session_state["etf_metric_category"] = resolve_metric_category(
                st.session_state["etf_selected_metric"]
            )

        if st.session_state.get("etf_chart_type") not in chart_options:
            st.session_state["etf_chart_type"] = chart_options[0]

        with st.container(key="ws-page-toolbar-etf"):
            toolbar_category_col, toolbar_metric_col, toolbar_chart_col = st.columns([1.0, 1.2, 0.9])
            with toolbar_category_col:
                selected_category = st.selectbox(
                    "指标分类",
                    options=category_options,
                    key="etf_metric_category",
                    disabled=len(category_options) == 1
                )

            available_metrics = metric_categories.get(selected_category, metric_types)
            if st.session_state.get("etf_selected_metric") not in available_metrics:
                st.session_state["etf_selected_metric"] = available_metrics[0]

            with toolbar_metric_col:
                selected_metric = st.selectbox(
                    "选择具体指标",
                    options=available_metrics,
                    key="etf_selected_metric"
                )

            with toolbar_chart_col:
                chart_type = st.selectbox(
                    "图表类型",
                    options=chart_options,
                    format_func=lambda x: {'line': '📈 平滑曲线', 'area': '📊 面积图', 'scatter': '⚫ 散点图'}[x],
                    key="etf_chart_type"
                )

            st.markdown("**快速切换**")
            quick_cols = st.columns(len(quick_metric_groups))
            quick_button_keys = {
                "总市值": "etf_quick_market",
                "份额": "etf_quick_share",
                "涨跌幅": "etf_quick_change",
            }
            for idx, (label, metrics) in enumerate(quick_metric_groups.items()):
                if quick_cols[idx].button(
                    label,
                    use_container_width=True,
                    key=quick_button_keys[label],
                    disabled=not metrics
                ):
                    target_metric = metrics[0]
                    st.session_state["etf_pending_metric"] = target_metric
                    st.session_state["etf_pending_metric_category"] = resolve_metric_category(target_metric)
                    st.rerun()

            metric_df = df[df['metric_type'] == selected_metric].copy()
            has_aggregate = metric_df['is_aggregate'].any()
            contains_total_market_value = '总市值' in selected_metric if selected_metric else False

            with st.expander("更多筛选", expanded=False):
                if has_aggregate and contains_total_market_value:
                    st.info("📊 当前显示所有ETF的总和")
                    selected_etfs = None
                else:
                    etf_names = sorted(metric_df[metric_df['is_aggregate'] == False]['name'].unique())
                    current_selected_etfs = st.session_state.get("etf_selected_etfs")
                    if not isinstance(current_selected_etfs, list):
                        current_selected_etfs = etf_names
                    else:
                        current_selected_etfs = [etf for etf in current_selected_etfs if etf in etf_names]
                        if not current_selected_etfs:
                            current_selected_etfs = etf_names
                    st.session_state["etf_selected_etfs"] = current_selected_etfs
                    selected_etfs = st.multiselect(
                        "选择ETF",
                        options=etf_names,
                        key="etf_selected_etfs"
                    )

                min_date = metric_df['date'].min().date()
                max_date = metric_df['date'].max().date()

                if min_date == max_date:
                    st.info(f"📅 当前数据日期: {min_date}")
                    date_range = (min_date, max_date)
                    st.session_state["etf_date_range"] = date_range
                else:
                    current_date_range = st.session_state.get("etf_date_range")
                    valid_date_range = (
                        isinstance(current_date_range, (tuple, list)) and
                        len(current_date_range) == 2 and
                        min_date <= current_date_range[0] <= current_date_range[1] <= max_date
                    )
                    if not valid_date_range:
                        st.session_state["etf_date_range"] = (min_date, max_date)
                    date_range = st.slider(
                        "选择日期范围",
                        min_value=min_date,
                        max_value=max_date,
                        format="YYYY-MM-DD",
                        key="etf_date_range"
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
        fig.add_hrect(y0=0, y1=float(positive_max), fillcolor="rgba(230, 57, 70, 0.05)", line_width=0)
    if pd.notna(negative_min):
        fig.add_hrect(y0=float(negative_min), y1=0, fillcolor="rgba(42, 157, 143, 0.05)", line_width=0)

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
            line=dict(width=2.4, color=THEME_WARN, shape='spline'),
            marker=dict(size=5, color=THEME_WARN),
            fill='tozeroy',
            fillcolor=CHART_GOLD_SOFT_FILL,
            customdata=custom_data,
            hovertemplate=hover_template
        ))
    else:
        palette = color_palette or [THEME_NAVY, THEME_PURPLE, "#C28C4E", "#5B8E7D", "#B86A84"]
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

    fig.add_hline(y=0, line_width=1, line_dash='dash', line_color=THEME_NEUTRAL)
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(size=20, weight=700, color=THEME_TEXT),
            x=0.02
        ),
        xaxis_title='日期',
        yaxis_title=yaxis_title,
        hovermode='x unified',
        height=420,
        template='plotly_white',
        plot_bgcolor=CHART_BG,
        paper_bgcolor=CHART_PAPER_BG,
        font=dict(family='Inter, PingFang SC, sans-serif'),
        legend=dict(
            orientation='h', yanchor='bottom', y=-0.25,
            xanchor='center', x=0.5,
            bgcolor='rgba(255,255,255,0)', font=dict(size=11)
        ),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    apply_time_series_hover_affordance(fig, chart_df['trade_date'], chart_df[value_col])
    fig.update_xaxes(
        showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR,
        showline=True, linewidth=1, linecolor=CHART_AXIS_COLOR
    )
    fig.update_yaxes(
        showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR,
        showline=True, linewidth=1, linecolor=CHART_AXIS_COLOR,
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
    positive_color = THEME_UP
    negative_color = THEME_DOWN
    positive_max = chart_df.loc[chart_df[value_col] > 0, value_col].max() if not chart_df.empty else None
    negative_min = chart_df.loc[chart_df[value_col] < 0, value_col].min() if not chart_df.empty else None
    if pd.notna(positive_max):
        fig.add_hrect(y0=0, y1=float(positive_max), fillcolor="rgba(230, 57, 70, 0.05)", line_width=0)
    if pd.notna(negative_min):
        fig.add_hrect(y0=float(negative_min), y1=0, fillcolor="rgba(42, 157, 143, 0.05)", line_width=0)

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

    fig.add_hline(y=0, line_width=1, line_dash='dash', line_color=THEME_NEUTRAL)
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(size=20, weight=700, color=THEME_TEXT),
            x=0.02
        ),
        xaxis_title='日期',
        yaxis_title=yaxis_title,
        hovermode='x unified',
        height=420,
        template='plotly_white',
        plot_bgcolor=CHART_BG,
        paper_bgcolor=CHART_PAPER_BG,
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
    apply_time_series_hover_affordance(fig, chart_df['trade_date'], chart_df[value_col])
    fig.update_xaxes(
        showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR,
        showline=True, linewidth=1, linecolor=CHART_AXIS_COLOR
    )
    fig.update_yaxes(
        showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR,
        showline=True, linewidth=1, linecolor=CHART_AXIS_COLOR,
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
    palette = [THEME_NAVY, THEME_UP, "#5B8E7D", THEME_WARN, THEME_PURPLE]
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
        title=dict(text=title, font=dict(size=20, weight=700, color=THEME_TEXT), x=0.02),
        xaxis_title="日期",
        yaxis_title=yaxis_title,
        hovermode="x unified",
        height=420,
        template="wealthspark_balanced",
        plot_bgcolor=CHART_BG,
        paper_bgcolor=CHART_PAPER_BG,
        font=dict(family="Inter, PingFang SC, sans-serif"),
        legend=dict(
            orientation="h", yanchor="bottom", y=-0.25,
            xanchor="center", x=0.5,
            bgcolor="rgba(255,255,255,0)", font=dict(size=11)
        ),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    if fig.data:
        apply_time_series_hover_affordance(
            fig,
            chart_df["trade_date"],
            [chart_df[column] for column, _label in series if column in chart_df.columns],
        )
    fig.update_xaxes(
        showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR,
        showline=True, linewidth=1, linecolor=CHART_AXIS_COLOR
    )
    fig.update_yaxes(
        showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR,
        showline=True, linewidth=1, linecolor=CHART_AXIS_COLOR,
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

    try:
        available = get_available_dates(limit=1000)
    except Exception as e:
        st.error(f"获取可用日期失败: {e}")
        return

    if not available:
        st.warning("暂无可用交易日数据")
        return

    all_dates = sorted([datetime.strptime(d, '%Y-%m-%d').date() for d in available])
    min_d, max_d = all_dates[0], all_dates[-1]

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

            date_range = st.slider(
                "时间范围",
                min_value=min_d,
                max_value=max_d,
                value=(min_d, max_d),
                format="YYYY-MM-DD",
                key="iphone_trend_date_range"
            )
    else:
        with st.container(key="ws-page-toolbar-trend"):
            toolbar_primary_col, toolbar_secondary_col, toolbar_metric_col = st.columns([1.05, 1.05, 0.95])
            with toolbar_primary_col:
                selected_primary = st.selectbox(
                    "一级分类",
                    options=primary_options,
                    index=0,
                    key="trend_primary"
                )
            secondary_options = build_secondary_category_options(selected_primary, category_tree)
            with toolbar_secondary_col:
                if secondary_options:
                    selected_secondary = st.selectbox(
                        "二级分类",
                        options=secondary_options,
                        index=0,
                        key="trend_secondary"
                    )
                else:
                    selected_secondary = None
                    st.caption("当前一级分类没有二级分类可选")
            with toolbar_metric_col:
                metric = st.radio(
                    "查看指标",
                    options=['总份额(亿份)', '总规模(亿元)'],
                    index=0,
                    key="trend_metric"
                )
            metric_col = 'total_share_yi' if '份额' in metric else 'total_size_yi'
            category_key = resolve_trend_category_key(selected_primary, selected_secondary, category_tree)
            with st.expander("更多筛选", expanded=False):
                date_range = st.slider(
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
        line=dict(width=2.5, shape='spline', color=THEME_NAVY),
        fill='tozeroy',
        fillcolor=CHART_NAVY_SOFT_FILL,
        hovertemplate='<b>%{x|%Y-%m-%d}</b><br>%{y:,.2f}<extra></extra>'
    ))

    if len(chart_data) >= 20:
        chart_data['ma20'] = chart_data[metric_col].rolling(window=20).mean()
        fig.add_trace(go.Scatter(
            x=chart_data['trade_date'],
            y=chart_data['ma20'],
            mode='lines',
            name='20日均线',
            line=dict(width=1.5, color=THEME_UP, dash='dot'),
            hovertemplate='<b>%{x|%Y-%m-%d}</b><br>20MA: %{y:,.2f}<extra></extra>'
        ))

    fig.update_layout(
        title=dict(
            text=f'{category_key} \u2014 {metric} 趋势',
            font=dict(size=20, weight=700, color=THEME_TEXT),
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
        plot_bgcolor=CHART_BG,
        paper_bgcolor=CHART_PAPER_BG,
        font=dict(family='Inter, PingFang SC, sans-serif'),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    apply_time_series_hover_affordance(fig, chart_data['trade_date'], chart_data[metric_col])
    fig.update_xaxes(
        showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR,
        showline=True, linewidth=1, linecolor=CHART_AXIS_COLOR
    )
    fig.update_yaxes(
        showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR,
        showline=True, linewidth=1, linecolor=CHART_AXIS_COLOR,
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


def _render_top10_shareholder_panel(top10_holders, top10_floatholders, stock_title_for_top10="", top10_errors=None, *, expanded=False):
    top10_errors = top10_errors or {}
    has_top10_holders = isinstance(top10_holders, pd.DataFrame) and not top10_holders.empty
    has_top10_float = isinstance(top10_floatholders, pd.DataFrame) and not top10_floatholders.empty

    if not (has_top10_holders or has_top10_float):
        if top10_errors:
            err_text = "；".join([str(v) for v in top10_errors.values() if str(v).strip()])
            st.info(f"前十大股东数据暂不可用：{err_text or '接口暂无返回'}")
        return

    st.markdown("##### 🧱 股东情况")

    def _fmt_pct(v):
        return f"{float(v):.2f}%" if pd.notna(v) else "-"

    def _fmt_shares(v):
        if pd.isna(v):
            return "-"
        val = float(v)
        return f"{val / 1e8:,.2f} 亿股" if abs(val) >= 1e8 else f"{val:,.0f} 股"

    snapshot_end = "-"
    snapshot_ann = "-"
    for _df in [top10_holders, top10_floatholders]:
        if isinstance(_df, pd.DataFrame) and not _df.empty:
            if "end_date" in _df.columns and _df["end_date"].notna().any():
                snapshot_end = str(_df["end_date"].iloc[0])
            if "ann_date" in _df.columns and _df["ann_date"].notna().any():
                snapshot_ann = str(_df["ann_date"].iloc[0])
            if snapshot_end != "-" or snapshot_ann != "-":
                break
    st.caption(f"{stock_title_for_top10 or '当前股票'}｜报告期：{snapshot_end}｜公告日：{snapshot_ann}")

    holder_total_ratio = pd.to_numeric(top10_holders.get("hold_ratio"), errors="coerce").fillna(0).sum() if has_top10_holders else 0
    holder_top3_ratio = pd.to_numeric(top10_holders.get("hold_ratio"), errors="coerce").fillna(0).head(3).sum() if has_top10_holders else 0
    float_total_ratio = pd.to_numeric(top10_floatholders.get("hold_float_ratio"), errors="coerce").fillna(0).sum() if has_top10_float else 0
    float_change_total = pd.to_numeric(top10_floatholders.get("hold_change"), errors="coerce").fillna(0).sum() if has_top10_float else 0

    top10_metrics = st.columns(4)
    top10_metrics[0].metric("前十股东合计持股", _fmt_pct(holder_total_ratio))
    top10_metrics[1].metric("前三股东集中度", _fmt_pct(holder_top3_ratio))
    top10_metrics[2].metric("前十流通股东锁仓", _fmt_pct(float_total_ratio))
    top10_metrics[3].metric("流通股东净变动", _fmt_shares(float_change_total))

    with st.expander("展开股东情况", expanded=expanded):
        st.caption("展开后会同时显示前十大股东与前十大流通股东。")

        st.markdown("##### 🏛 前十大股东")
        if has_top10_holders:
            holder_plot = top10_holders.head(10).copy()
            holder_plot["plot_ratio"] = pd.to_numeric(holder_plot.get("hold_ratio"), errors="coerce").fillna(0)
            holder_plot["display_name"] = holder_plot["holder_name"].astype(str)
            fig_holder = go.Figure(go.Bar(
                x=holder_plot["plot_ratio"],
                y=holder_plot["display_name"],
                orientation="h",
                marker=dict(color=holder_plot["plot_ratio"], colorscale="Blues", showscale=False),
                text=holder_plot["plot_ratio"].map(lambda v: f"{v:,.2f}%"),
                textposition="outside",
                hovertemplate="%{y}<br>占总股本：%{x:,.2f}%<extra></extra>",
            ))
            fig_holder.update_layout(
                title=dict(text=f"{stock_title_for_top10} 前十大股东持股柱状图", x=0.02, font=dict(size=17, color=THEME_TEXT)),
                xaxis_title="占总股本比例（%）",
                height=max(360, len(holder_plot) * 26),
                template="wealthspark_balanced",
                paper_bgcolor=CHART_PAPER_BG,
                plot_bgcolor=CHART_BG,
                font=dict(family="Inter, PingFang SC, sans-serif"),
                margin=dict(l=140, r=40, t=55, b=20),
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig_holder, use_container_width=True)

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
            display_cols = [c for c in ["股东名称", "持股数量", "占总股本比(%)", "占流通股比(%)", "持股变动", "股东类型"] if c in holder_show.columns]
            st.dataframe(holder_show[display_cols], use_container_width=True, hide_index=True)
        else:
            st.info("当前报告期暂无前十大股东数据。")

        st.divider()
        st.markdown("##### 🔓 前十大流通股东")
        if has_top10_float:
            float_plot = top10_floatholders.head(10).copy()
            metric_col = "hold_float_ratio" if "hold_float_ratio" in float_plot.columns else "hold_ratio"
            float_plot["plot_ratio"] = pd.to_numeric(float_plot.get(metric_col), errors="coerce").fillna(0)
            float_plot["display_name"] = float_plot["holder_name"].astype(str)
            fig_float = go.Figure(go.Bar(
                x=float_plot["plot_ratio"],
                y=float_plot["display_name"],
                orientation="h",
                marker=dict(color=float_plot["plot_ratio"], colorscale="Tealgrn", showscale=False),
                text=float_plot["plot_ratio"].map(lambda v: f"{v:,.2f}%"),
                textposition="outside",
                hovertemplate="%{y}<br>占流通股比例：%{x:,.2f}%<extra></extra>",
            ))
            fig_float.update_layout(
                title=dict(text=f"{stock_title_for_top10} 前十大流通股东持股柱状图", x=0.02, font=dict(size=17, color=THEME_TEXT)),
                xaxis_title="占流通股比例（%）",
                height=max(360, len(float_plot) * 26),
                template="wealthspark_balanced",
                paper_bgcolor=CHART_PAPER_BG,
                plot_bgcolor=CHART_BG,
                font=dict(family="Inter, PingFang SC, sans-serif"),
                margin=dict(l=140, r=40, t=55, b=20),
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig_float, use_container_width=True)

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
            display_cols = [c for c in ["股东名称", "持股数量", "占总股本比(%)", "占流通股比(%)", "持股变动", "股东类型"] if c in float_show.columns]
            st.dataframe(float_show[display_cols], use_container_width=True, hide_index=True)
        else:
            st.info("当前报告期暂无前十大流通股东数据。")


def _render_security_fund_holding_panel(selected_code: str, title_name: str, period_options: list[str]) -> None:
    with st.expander("🏦 公募基金持仓", expanded=False):
        if not period_options:
            st.info("暂无公募基金持仓报告期数据。")
            return

        fund_type_options = ["全部", "混合型", "股票型", "债券型", "ETF", "QDII", "LOF", "货币型"]
        fund_col_period, fund_col_type, fund_col_count, fund_col_btn = st.columns([1.2, 1.2, 1.0, 1.0])
        with fund_col_period:
            fund_period_key = f"security_fund_period_{selected_code}"
            if fund_period_key not in st.session_state or st.session_state[fund_period_key] not in period_options:
                st.session_state[fund_period_key] = period_options[0]
            holding_period = st.selectbox("基金持仓报告期", options=period_options, key=fund_period_key)
        with fund_col_type:
            fund_type_filter = st.selectbox("基金类型", options=fund_type_options, index=0, key=f"security_fund_type_{selected_code}")
        with fund_col_count:
            holding_top_n = st.selectbox("显示基金数", [20, 50, 100, 300], index=1, key=f"security_fund_top_n_{selected_code}")
        with fund_col_btn:
            st.caption(" ")
            query_clicked = st.button("查询基金持仓", type="primary", key=f"btn_security_fund_holding_{selected_code}")

        if st.session_state.get("security_fund_last_code") != selected_code:
            st.session_state["security_fund_holding_df"] = pd.DataFrame()
            st.session_state["security_fund_holding_status"] = "待查询"
            st.session_state["security_fund_last_code"] = selected_code

        query_signature = f"{selected_code}|{holding_period}|{fund_type_filter}|{holding_top_n}"
        auto_query_needed = st.session_state.get("security_fund_last_signature") != query_signature
        if query_clicked or auto_query_needed:
            try:
                fund_holding_df = load_security_fund_holding_detail(
                    symbol=selected_code,
                    period=str(holding_period).replace("-", ""),
                    fund_type_filter=fund_type_filter,
                )
                if isinstance(fund_holding_df, pd.DataFrame) and not fund_holding_df.empty:
                    fund_holding_df = fund_holding_df.head(int(holding_top_n)).copy()
                st.session_state["security_fund_holding_df"] = fund_holding_df
                st.session_state["security_fund_holding_status"] = (
                    "查询成功"
                    if isinstance(fund_holding_df, pd.DataFrame) and not fund_holding_df.empty
                    else "该报告期暂无基金持仓"
                )
            except Exception as exc:
                logger.warning(f"load_security_fund_holding_detail failed: {exc}", exc_info=True)
                st.session_state["security_fund_holding_df"] = pd.DataFrame()
                st.session_state["security_fund_holding_status"] = "查询异常"
            finally:
                st.session_state["security_fund_last_signature"] = query_signature

        status = st.session_state.get("security_fund_holding_status", "待查询")
        st.info(f"📌 当前个股：{title_name}（{selected_code}）｜报告期：{holding_period}｜状态：{status}")

        fund_holding_df = st.session_state.get("security_fund_holding_df")
        if not isinstance(fund_holding_df, pd.DataFrame) or fund_holding_df.empty:
            return

        fund_holding_df = fund_holding_df.copy()
        fund_holding_df["fund_name"] = fund_holding_df.get("fund_name", fund_holding_df.get("fund_code", "")).fillna(fund_holding_df.get("fund_code", ""))
        fund_holding_df["mkv_yi"] = pd.to_numeric(fund_holding_df.get("mkv"), errors="coerce").fillna(0) / 1e8
        fund_holding_df["delta_mkv_yi"] = pd.to_numeric(fund_holding_df.get("delta_mkv"), errors="coerce").fillna(0) / 1e8
        fund_holding_df["holding_change_flag"] = fund_holding_df.get("holding_change_flag", "").replace({
            "new": "新进",
            "increase": "加仓",
            "decrease": "减仓",
            "stable": "持平",
        })

        holding_metrics = st.columns(4)
        holding_metrics[0].metric("持有基金数", f"{len(fund_holding_df):,}")
        holding_metrics[1].metric("持仓总市值", f"{fund_holding_df['mkv_yi'].sum():,.2f} 亿")
        holding_metrics[2].metric("新进基金", f"{int((fund_holding_df['holding_change_flag'] == '新进').sum())}")
        holding_metrics[3].metric("加仓基金", f"{int((fund_holding_df['holding_change_flag'] == '加仓').sum())}")

        plot_df = fund_holding_df.head(min(len(fund_holding_df), 20)).copy()
        fig_holding = go.Figure(go.Bar(
            x=plot_df["mkv_yi"],
            y=plot_df["fund_name"],
            orientation="h",
            marker=dict(color=plot_df["mkv_yi"], colorscale="Viridis", showscale=False),
            text=plot_df["mkv_yi"].map(lambda v: f"{v:,.2f}亿" if pd.notna(v) else "-"),
            textposition="outside",
            hovertemplate="%{y}<br>持仓市值：%{x:,.2f} 亿<extra></extra>",
        ))
        fig_holding.update_layout(
            title=dict(text=f"{title_name} 持仓基金 Top{len(plot_df)}", x=0.02, font=dict(size=17, color=THEME_TEXT)),
            xaxis_title="持仓市值（亿元）",
            height=max(380, len(plot_df) * 26),
            template="wealthspark_balanced",
            paper_bgcolor=CHART_PAPER_BG,
            plot_bgcolor=CHART_BG,
            font=dict(family="Inter, PingFang SC, sans-serif"),
            margin=dict(l=120, r=40, t=55, b=20),
            yaxis=dict(autorange="reversed"),
        )
        st.plotly_chart(fig_holding, use_container_width=True)

        management_series = fund_holding_df.get("management", pd.Series([""] * len(fund_holding_df)))
        show_df = pd.DataFrame({
            "基金代码": fund_holding_df["fund_code"],
            "基金名称": fund_holding_df["fund_name"],
            "管理人": management_series,
            "持仓市值(亿)": fund_holding_df["mkv_yi"],
            "持仓数量": pd.to_numeric(fund_holding_df.get("amount"), errors="coerce"),
            "占基金股票市值比(%)": pd.to_numeric(fund_holding_df.get("stk_mkv_ratio"), errors="coerce"),
            "占流通股本比例(%)": pd.to_numeric(fund_holding_df.get("stk_float_ratio"), errors="coerce"),
            "市值变化(亿)": fund_holding_df["delta_mkv_yi"],
            "变动类型": fund_holding_df["holding_change_flag"],
        })
        for col in ["持仓市值(亿)", "占基金股票市值比(%)", "占流通股本比例(%)", "市值变化(亿)"]:
            show_df[col] = pd.to_numeric(show_df[col], errors="coerce").map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
        show_df["持仓数量"] = pd.to_numeric(show_df["持仓数量"], errors="coerce").map(lambda v: f"{v:,.0f}" if pd.notna(v) else "-")
        st.dataframe(show_df, use_container_width=True, hide_index=True)


@st.cache_data(ttl=300)
def load_watchlist_enriched_data(ts_codes: tuple, security_types: tuple) -> pd.DataFrame:
    rows = []
    for code, sec_type in zip(ts_codes, security_types):
        try:
            profile_df = load_security_profile(code, sec_type)
            ts_df = load_security_timeseries(code, sec_type)
            
            profile = profile_df.iloc[0] if profile_df is not None and len(profile_df) > 0 else {}
            
            close = profile.get("close")
            name = profile.get("name") or code
            pe_ttm = profile.get("pe_ttm")
            pb = profile.get("pb")
            total_mv = profile.get("total_mv", 0) / 10000.0 if profile.get("total_mv") else None
            roe = profile.get("roe")
            
            ret_1d, ret_5d, ret_20d = np.nan, np.nan, np.nan
            trend_score = np.nan
            trend_label = ""
            prob_up_5d = np.nan
            prob_up_20d = np.nan
            risk_score = np.nan
            risk_level = ""
            rsi14 = np.nan
            macd_hist = np.nan
            support = np.nan
            resistance = np.nan
            volume_ratio = np.nan
            turnover_rate = np.nan
            volatility20 = np.nan
            latest_volume_wan = np.nan
            latest_amount_yi = np.nan
            latest_trade_date = ""
            trend_summary = ""
            signal_label = ""
            sparkline_prices = []
            
            if ts_df is not None and not ts_df.empty and 'close' in ts_df.columns:
                trend = build_security_trend_analysis(ts_df, sec_type)
                if trend:
                    trend_score = trend.get("trend_score", np.nan)
                    trend_label = trend.get("trend", "")
                    
                    ts_sorted = ts_df.sort_values('trade_date').reset_index(drop=True)
                    if len(ts_sorted) >= 2:
                        prev_close = ts_sorted['close'].iloc[-2]
                        curr_close = ts_sorted['close'].iloc[-1]
                        if prev_close and prev_close > 0:
                            ret_1d = (curr_close / prev_close - 1) * 100
                            
                    if len(ts_sorted) > 0:
                        sparkline_prices = ts_sorted['close'].tail(20).tolist()
                        latest_date_value = ts_sorted['trade_date'].iloc[-1] if 'trade_date' in ts_sorted.columns else ""
                        parsed_latest_date = pd.to_datetime(latest_date_value, errors="coerce")
                        if pd.notna(parsed_latest_date):
                            latest_trade_date = parsed_latest_date.strftime("%Y-%m-%d")
                        if "vol" in ts_sorted.columns:
                            latest_volume_raw = _watchlist_to_float(ts_sorted["vol"].iloc[-1])
                            if latest_volume_raw is not None:
                                latest_volume_wan = latest_volume_raw / 10000.0
                        if "amount" in ts_sorted.columns:
                            latest_amount_raw = _watchlist_to_float(ts_sorted["amount"].iloc[-1])
                            if latest_amount_raw is not None:
                                latest_amount_yi = latest_amount_raw / 100000.0
                    
                    ret_5d = trend.get("ret_5", np.nan) * 100
                    ret_20d = trend.get("ret_20", np.nan) * 100
                    prob_up_5d = trend.get("prob_up_5d", np.nan) * 100
                    prob_up_20d = trend.get("prob_up_20d", np.nan) * 100
                    risk_score = trend.get("risk_score", np.nan)
                    risk_level = trend.get("risk_level", "")
                    rsi14 = trend.get("rsi14", np.nan)
                    macd_hist = trend.get("macd_hist", np.nan)
                    support = trend.get("support", np.nan)
                    resistance = trend.get("resistance", np.nan)
                    volume_ratio = trend.get("volume_ratio", np.nan)
                    turnover_rate = trend.get("turnover_rate", np.nan)
                    volatility20 = trend.get("volatility20", np.nan)
                    trend_summary = trend.get("summary", "")
                    
                    if pd.notna(trend_score):
                        if trend_score >= 72:
                            signal_label = "🔥 强势"
                        elif trend_score >= 58:
                            signal_label = "⚡ 偏强"
                        elif trend_score >= 45:
                            signal_label = "⚠️ 震荡"
                        else:
                            signal_label = "🔻 弱势"
                            
                    if pd.notna(rsi14):
                        if rsi14 > 80:
                            signal_label += " (超买)"
                        elif rsi14 < 30:
                            signal_label += " (超跌)"
            
            rows.append({
                "ts_code": code,
                "security_type": sec_type,
                "名称": name,
                "代码": code,
                "最新价": close,
                "涨跌幅(%)": ret_1d,
                "5日涨跌(%)": ret_5d,
                "20日涨跌(%)": ret_20d,
                "5日胜率(%)": prob_up_5d,
                "20日胜率(%)": prob_up_20d,
                "PE_TTM": pe_ttm,
                "PB": pb,
                "总市值(亿)": total_mv,
                "ROE(%)": roe,
                "趋势得分": trend_score,
                "趋势状态": trend_label,
                "风险得分": risk_score,
                "风险等级": risk_level,
                "RSI14": rsi14,
                "MACD柱": macd_hist,
                "支撑位": support,
                "压力位": resistance,
                "量比": volume_ratio,
                "换手率(%)": turnover_rate,
                "波动率20": volatility20,
                "成交量(万手)": latest_volume_wan,
                "成交额(亿)": latest_amount_yi,
                "数据日期": latest_trade_date,
                "趋势摘要": trend_summary,
                "操作信号": signal_label,
                "sparkline_prices": sparkline_prices,
            })
        except Exception as e:
            logger.warning(f"Error loading enriched data for {code}: {e}")
            continue
            
    return pd.DataFrame(rows)


WATCHLIST_SESSION_CACHE_TTL_SECONDS = 900
WATCHLIST_STATUS_SESSION_CACHE_TTL_SECONDS = 300


def load_watchlist_enriched_data_session_cached(
    username: str,
    ts_codes: tuple,
    security_types: tuple,
) -> pd.DataFrame:
    """Reuse the loaded watchlist frame during card focus changes in one session."""
    normalized_codes = tuple(str(code or "").strip().upper() for code in ts_codes)
    normalized_types = tuple(str(sec_type or "").strip().lower() for sec_type in security_types)
    cache_key = (str(username or "").strip(), normalized_codes, normalized_types)
    cache_payload = st.session_state.get("watchlist_enriched_session_cache")
    now = time.monotonic()

    if (
        isinstance(cache_payload, dict)
        and cache_payload.get("key") == cache_key
        and now - float(cache_payload.get("saved_at", 0.0)) < WATCHLIST_SESSION_CACHE_TTL_SECONDS
        and isinstance(cache_payload.get("df"), pd.DataFrame)
    ):
        return cache_payload["df"].copy(deep=True)

    df = load_watchlist_enriched_data(normalized_codes, normalized_types)
    st.session_state["watchlist_enriched_session_cache"] = {
        "key": cache_key,
        "saved_at": now,
        "df": df.copy(deep=True),
    }
    return df.copy(deep=True)


def load_watchlist_report_status_maps_session_cached(
    report_engine,
    stock_report_codes: tuple,
) -> tuple[dict[str, dict], dict[str, dict]]:
    normalized_codes = tuple(str(code or "").strip().upper() for code in stock_report_codes if str(code or "").strip())
    cache_key = normalized_codes
    cache_payload = st.session_state.get("watchlist_report_status_session_cache")
    now = time.monotonic()

    if (
        isinstance(cache_payload, dict)
        and cache_payload.get("key") == cache_key
        and now - float(cache_payload.get("saved_at", 0.0)) < WATCHLIST_STATUS_SESSION_CACHE_TTL_SECONDS
    ):
        return dict(cache_payload.get("distribution", {})), dict(cache_payload.get("research", {}))

    report_status_map: dict[str, dict] = {}
    research_status_map: dict[str, dict] = {}
    if report_engine is not None and normalized_codes:
        try:
            report_status_map = get_report_statuses(report_engine, normalized_codes)
        except Exception as exc:
            logger.warning("Failed to load distribution report statuses: %s", exc)
        try:
            research_status_map = get_stock_research_report_statuses(report_engine, normalized_codes)
        except Exception as exc:
            logger.warning("Failed to load stock research report statuses: %s", exc)

    st.session_state["watchlist_report_status_session_cache"] = {
        "key": cache_key,
        "saved_at": now,
        "distribution": dict(report_status_map),
        "research": dict(research_status_map),
    }
    return report_status_map, research_status_map


def load_watchlist_alert_text_map_session_cached(ts_codes: tuple) -> dict[str, str]:
    normalized_codes = tuple(str(code or "").strip().upper() for code in ts_codes if str(code or "").strip())
    cache_payload = st.session_state.get("watchlist_alert_text_session_cache")
    now = time.monotonic()

    if (
        isinstance(cache_payload, dict)
        and cache_payload.get("key") == normalized_codes
        and now - float(cache_payload.get("saved_at", 0.0)) < WATCHLIST_STATUS_SESSION_CACHE_TTL_SECONDS
    ):
        return dict(cache_payload.get("alerts", {}))

    alerts_dict: dict[str, str] = {}
    try:
        alerts_df = get_latest_alerts_for_stocks(list(normalized_codes))
        if not alerts_df.empty:
            for _, r in alerts_df.iterrows():
                if r["alert_level"] != "NONE":
                    try:
                        details = json.loads(r["alert_details"]) if isinstance(r["alert_details"], str) else r.get("alert_details", {})
                        signals = details.get("signals", [])
                        if signals:
                            alerts_dict[str(r["ts_code"]).strip().upper()] = f"🚨 {', '.join(signals)}"
                    except Exception:
                        pass
    except Exception as e:
        logger.warning("Failed to load distribution alerts: %s", e)

    st.session_state["watchlist_alert_text_session_cache"] = {
        "key": normalized_codes,
        "saved_at": now,
        "alerts": dict(alerts_dict),
    }
    return alerts_dict


def generate_sparkline_svg(prices, is_up=True):
    if not prices or len(prices) < 2:
        return ""
    w = 120
    h = 30
    min_p = min(prices)
    max_p = max(prices)
    
    color = "var(--ws-color-up)" if is_up else "var(--ws-color-down)"
    
    if max_p == min_p:
        points = f"0,{h/2} {w},{h/2}"
    else:
        pts = []
        for i, p in enumerate(prices):
            x = i / (len(prices) - 1) * w
            y = h - ((p - min_p) / (max_p - min_p) * h)
            pts.append(f"{x:.1f},{y:.1f}")
        points = " ".join(pts)
        
    return f'''<svg viewBox="0 0 {w} {h}" width="100%" height="30px" preserveAspectRatio="none">
        <polyline points="{points}" fill="none" stroke="{color}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
    </svg>'''


def _watchlist_to_float(value, default=None):
    try:
        if value is None:
            return default
        number = float(value)
        if pd.isna(number) or not np.isfinite(number):
            return default
        return number
    except Exception:
        return default


def _watchlist_value_text(value, digits: int = 2, suffix: str = "", signed: bool = False) -> str:
    number = _watchlist_to_float(value)
    if number is None:
        return "-"
    sign = "+" if signed and number > 0 else ""
    return f"{sign}{number:,.{digits}f}{suffix}"


def _watchlist_compact_text(value, digits: int = 2, suffix: str = "") -> str:
    number = _watchlist_to_float(value)
    if number is None:
        return "-"
    abs_number = abs(number)
    if abs_number >= 10000:
        return f"{number / 10000:,.{digits}f}万{suffix}"
    return f"{number:,.{digits}f}{suffix}"


def _watchlist_html_text(value, fallback: str = "-") -> str:
    try:
        if value is None or pd.isna(value):
            return escape(fallback)
    except Exception:
        if value is None:
            return escape(fallback)
    text = str(value).strip()
    return escape(text or fallback)


def _watchlist_score_value(value, fallback: float = 50.0) -> int:
    number = _watchlist_to_float(value, fallback)
    return int(round(clamp_value(number, 0, 100)))


def _watchlist_signal_tone(ret_pct) -> tuple[str, str, str]:
    ret_number = _watchlist_to_float(ret_pct, 0.0)
    if ret_number < 0:
        return "ws-negative", "▼", "#20dfb8"
    return "", "▲", "#ff3f55"


def build_watchlist_price_chart_svg(prices, is_up: bool = True) -> str:
    clean_prices = [_watchlist_to_float(p) for p in prices or []]
    clean_prices = [p for p in clean_prices if p is not None]
    if len(clean_prices) < 2:
        return """
        <div class="ws-watchboard-linechart" style="display:grid;place-items:center;color:#8fa8ce;border:1px dashed rgba(70,126,255,.24);">
            暂无足够价格序列
        </div>
        """

    min_price = min(clean_prices)
    max_price = max(clean_prices)
    price_span = max(max_price - min_price, abs(max_price) * 0.004, 0.01)
    line_color = "#ff4055" if is_up else "#20dfb8"

    chart_left = 6.4
    chart_right = 97.8
    chart_top = 5.6
    chart_bottom = 82.0
    chart_w = chart_right - chart_left
    chart_h = chart_bottom - chart_top
    chart_aspect = 820 / 320

    def _x_pct(idx: int) -> float:
        return chart_left + idx / (len(clean_prices) - 1) * chart_w

    def _y_pct(price: float) -> float:
        return chart_bottom - ((price - min_price) / price_span * chart_h)

    points = [(_x_pct(idx), _y_pct(price)) for idx, price in enumerate(clean_prices)]

    grid_parts = []
    for i in range(5):
        y = chart_top + i * chart_h / 4
        value = max_price - i * price_span / 4
        grid_parts.append(
            f'<div class="ws-watchboard-grid-h" style="--grid-top:{y:.2f}%;"></div>'
            f'<span class="ws-watchboard-axis-label" style="--label-top:{y:.2f}%;">{value:.2f}</span>'
        )
    for i in range(6):
        x = chart_left + i * chart_w / 5
        grid_parts.append(
            f'<div class="ws-watchboard-grid-v" style="--grid-left:{x:.2f}%;"></div>'
        )

    changes = [0.0]
    changes.extend([clean_prices[i] - clean_prices[i - 1] for i in range(1, len(clean_prices))])
    max_change = max([abs(v) for v in changes] or [1.0]) or 1.0
    bar_parts = []
    bar_width = min(2.2, max(0.4, chart_w / len(clean_prices) * 0.34))
    for idx, change in enumerate(changes):
        x = _x_pct(idx)
        bar_h = 2.4 + abs(change) / max_change * 11.4
        color = "#ff4055" if change >= 0 else "#20dfb8"
        bar_parts.append(
            f'<div class="ws-watchboard-volume" style="--x:{x:.2f}%;--bar-width:{bar_width:.2f}%;--bar-height:{bar_h:.2f}%;--bar-color:{color};"></div>'
        )

    segment_parts = []
    for idx in range(1, len(points)):
        x1, y1 = points[idx - 1]
        x2, y2 = points[idx]
        dx = x2 - x1
        dy = y2 - y1
        length = float(np.sqrt(dx * dx + (dy / chart_aspect) * (dy / chart_aspect)))
        angle = float(np.degrees(np.arctan2(dy / chart_aspect, dx)))
        segment_parts.append(
            f'<div class="ws-watchboard-segment" style="--x:{x1:.2f}%;--y:{y1:.2f}%;--len:{length:.2f}%;--angle:{angle:.2f}deg;--line-color:{line_color};"></div>'
        )

    last_x, last_y = points[-1]
    point_part = (
        f'<div class="ws-watchboard-point" style="--x:{last_x:.2f}%;--y:{last_y:.2f}%;--line-color:{line_color};"></div>'
    )
    area_points = ", ".join([f"{x:.2f}% {y:.2f}%" for x, y in points])
    area_path = f"polygon({chart_left:.2f}% {chart_bottom:.2f}%, {area_points}, {chart_right:.2f}% {chart_bottom:.2f}%)"
    grid_html = "".join(grid_parts)
    bars_html = "".join(bar_parts)
    segments_html = "".join(segment_parts)

    return f"""
    <div class="ws-watchboard-linechart" style="--line-color:{line_color};--area-path:{area_path};" role="img" aria-label="价格走势">
        {grid_html}
        <div class="ws-watchboard-area"></div>
        {segments_html}
        {point_part}
        {bars_html}
        <span class="ws-watchboard-x-label is-start">近20日</span>
        <span class="ws-watchboard-x-label is-end">最新</span>
    </div>
    """


def build_watchlist_dimension_scores(row: pd.Series) -> list[tuple[str, int, str]]:
    ret_1d = _watchlist_to_float(row.get("涨跌幅(%)"), 0.0)
    trend_score = _watchlist_to_float(row.get("趋势得分"), 50.0)
    volume_ratio = _watchlist_to_float(row.get("量比"), 1.0)
    turnover_rate = _watchlist_to_float(row.get("换手率(%)"), 0.0)
    pe_ttm = _watchlist_to_float(row.get("PE_TTM"))
    pb = _watchlist_to_float(row.get("PB"))
    roe = _watchlist_to_float(row.get("ROE(%)"), 0.0)
    prob_up_5d = _watchlist_to_float(row.get("5日胜率(%)"))
    alert_text = str(row.get("主力异动") or "").strip()

    capital_score = 66 + ret_1d * 2.4 + (volume_ratio - 1) * 4 + min(turnover_rate, 12) * 0.8
    technical_score = trend_score
    basic_score = 60 + clamp_value(roe, -10, 28) * 0.85
    if pe_ttm is not None:
        if 0 < pe_ttm <= 35:
            basic_score += 7
        elif pe_ttm > 80:
            basic_score -= 9
    if pb is not None:
        if 0 < pb <= 4:
            basic_score += 4
        elif pb > 8:
            basic_score -= 6
    sentiment_score = 82 if not alert_text else 64
    model_score = prob_up_5d if prob_up_5d is not None else (trend_score * 0.72 + 18)

    return [
        ("资金面", _watchlist_score_value(capital_score), "#ff4055"),
        ("技术面", _watchlist_score_value(technical_score), "#2f7bff"),
        ("基本面", _watchlist_score_value(basic_score), "#49f2e0"),
        ("消息面", _watchlist_score_value(sentiment_score), "#d66cff"),
        ("量化模型", _watchlist_score_value(model_score), "#ff9b73"),
    ]


def render_watchlist_focus_detail_card(
    display_df: pd.DataFrame,
    focus_row: pd.Series,
    *,
    current_username: str,
    total_count: int,
    up_count: int,
    down_count: int,
    avg_trend,
) -> None:
    st.markdown(WATCHLIST_CYBER_DASHBOARD_CSS, unsafe_allow_html=True)

    name = _watchlist_html_text(focus_row.get("名称") or focus_row.get("代码"))
    code = _watchlist_html_text(focus_row.get("代码"))
    price = _watchlist_to_float(focus_row.get("最新价"))
    ret_1d = _watchlist_to_float(focus_row.get("涨跌幅(%)"), 0.0)
    tone_class, arrow, accent_color = _watchlist_signal_tone(ret_1d)
    price_text = _watchlist_value_text(price, digits=2)
    ret_text = _watchlist_value_text(ret_1d, digits=2, suffix="%", signed=True)
    delta_value = None
    if price is not None and ret_1d is not None and ret_1d > -99.0:
        try:
            previous_price = price / (1 + ret_1d / 100)
            delta_value = price - previous_price
        except Exception:
            delta_value = None
    delta_text = _watchlist_value_text(delta_value, digits=2, signed=True)

    trend_score = _watchlist_score_value(focus_row.get("趋势得分"))
    trend_color = "#ff4055" if trend_score >= 58 else "#20dfb8"
    risk_score = _watchlist_score_value(focus_row.get("风险得分"), fallback=max(0, 100 - trend_score))
    beat_pct = 0.0
    score_series = pd.to_numeric(display_df.get("趋势得分"), errors="coerce").dropna()
    if not score_series.empty:
        beat_pct = float((score_series < trend_score).mean() * 100)

    volume_text = _watchlist_value_text(focus_row.get("成交量(万手)"), digits=2, suffix="万手")
    turnover_text = _watchlist_value_text(focus_row.get("换手率(%)"), digits=2, suffix="%")
    pe_text = _watchlist_value_text(focus_row.get("PE_TTM"), digits=1)
    mv_text = _watchlist_value_text(focus_row.get("总市值(亿)"), digits=1, suffix="亿")
    volume_ratio_text = _watchlist_value_text(focus_row.get("量比"), digits=2)
    support_text = _watchlist_value_text(focus_row.get("支撑位"), digits=2)
    resistance_text = _watchlist_value_text(focus_row.get("压力位"), digits=2)
    signal_text = _watchlist_html_text(focus_row.get("操作信号"))
    risk_level = _watchlist_html_text(focus_row.get("风险等级"))
    alert_text = _watchlist_html_text(focus_row.get("主力异动") or "主力异动暂无明显预警")
    summary_raw = str(focus_row.get("趋势摘要") or "").strip()
    if not summary_raw:
        summary_raw = f"当前趋势信号为 {focus_row.get('操作信号') or '-'}，结合趋势得分、短线涨跌与风险项进行跟踪。"
    if len(summary_raw) > 118:
        summary_raw = summary_raw[:118] + "..."
    summary_text = escape(summary_raw)
    latest_date = _watchlist_html_text(focus_row.get("数据日期") or "最新交易日")

    now = datetime.now()
    weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    clock_text = f"{now:%Y-%m-%d} {weekday_names[now.weekday()]} {now:%H:%M:%S}"
    user_text = _watchlist_html_text(current_username or "未登录")
    avg_trend_text = _watchlist_value_text(avg_trend, digits=1)

    dim_rows = []
    for label, score, bar_color in build_watchlist_dimension_scores(focus_row):
        dim_rows.append(
            f"""
            <div class="ws-watchboard-score-row">
                <span>{escape(label)}</span>
                <div class="ws-score-track"><div class="ws-score-fill" style="width:{score}%;--bar-color:{bar_color};"></div></div>
                <strong>{score}</strong>
            </div>
            """
        )
    dim_rows_html = "".join(dim_rows)

    spark_prices = focus_row.get("sparkline_prices")
    chart_svg = build_watchlist_price_chart_svg(
        spark_prices if isinstance(spark_prices, list) else [],
        is_up=(ret_1d is None or ret_1d >= 0),
    )

    board_html = f"""
    <div id="watchlist-detail-card" class="ws-watchboard-shell is-detail">
        <div class="ws-watchboard-topbar">
            <div class="ws-watchboard-title">
                <strong>{name}</strong>
                <span>{code} · 自选用户 {user_text}</span>
            </div>
            <div class="ws-watchboard-chip">AI 量化分析</div>
            <div class="ws-watchboard-clock">{clock_text}</div>
        </div>
        <div class="ws-watchboard-hero">
            <div class="ws-watchboard-panel ws-watchboard-price">
                <div>
                    <div class="ws-watchboard-symbol">焦点标的 · {latest_date}</div>
                    <div class="ws-watchboard-bigprice {tone_class}">{price_text}<span class="ws-watchboard-arrow">{arrow}</span></div>
                </div>
                <div>
                    <div class="ws-watchboard-delta {tone_class}">{delta_text}<br>{ret_text}</div>
                    <div class="ws-watchboard-status">监控中</div>
                </div>
            </div>
            <div class="ws-watchboard-panel ws-watchboard-stats">
                <div class="ws-watchboard-stat"><span class="ws-watchboard-stat-icon">V</span><div><label>成交量</label><strong>{volume_text}</strong></div></div>
                <div class="ws-watchboard-stat"><span class="ws-watchboard-stat-icon">R</span><div><label>换手率</label><strong>{turnover_text}</strong></div></div>
                <div class="ws-watchboard-stat"><span class="ws-watchboard-stat-icon">P</span><div><label>市盈率</label><strong>{pe_text}</strong></div></div>
                <div class="ws-watchboard-stat"><span class="ws-watchboard-stat-icon">M</span><div><label>总市值</label><strong>{mv_text}</strong></div></div>
            </div>
        </div>
        <div class="ws-watchboard-main">
            <div class="ws-watchboard-panel ws-watchboard-chart">
                <div class="ws-watchboard-tabs">
                    <div class="ws-watchboard-tab is-active">走势</div>
                    <div class="ws-watchboard-tab">日K</div>
                    <div class="ws-watchboard-tab">周K</div>
                    <div class="ws-watchboard-tab">月K</div>
                    <div class="ws-watchboard-fullscreen">全屏</div>
                </div>
                <div class="ws-watchboard-chart-body">
                    <div class="ws-watchboard-chart-title"><span>{name}</span><span>{price_text} {arrow} {ret_text}</span></div>
                    {chart_svg}
                </div>
                <div class="ws-watchboard-footer-metrics">
                    <div class="ws-watchboard-foot"><label>自选广度</label><strong>{total_count}只 · {up_count}涨 / {down_count}跌</strong></div>
                    <div class="ws-watchboard-foot"><label>量比</label><strong>{volume_ratio_text}</strong></div>
                    <div class="ws-watchboard-foot"><label>支撑 / 压力</label><strong>{support_text} / {resistance_text}</strong></div>
                    <div class="ws-watchboard-foot"><label>平均趋势</label><strong>{avg_trend_text}</strong></div>
                </div>
            </div>
            <div class="ws-watchboard-side">
                <div class="ws-watchboard-panel ws-watchboard-score">
                    <div class="ws-score-donut" style="--score:{trend_score};--score-color:{trend_color};">
                        <div><strong>{trend_score}</strong><span>综合评分</span></div>
                    </div>
                    <div class="ws-watchboard-score-bars">{dim_rows_html}</div>
                </div>
                <div class="ws-watchboard-panel ws-watchboard-summary">
                    <h4><span>综合评分</span><strong>{trend_score}</strong></h4>
                    <p>今日表现 <span style="color:{accent_color};font-weight:800;">{ret_text}</span> · 风险 {risk_score} · {risk_level}</p>
                    <div class="ws-beat">打败了 <strong>{beat_pct:.2f}%</strong> 的自选标的</div>
                    <p>{alert_text}</p>
                    <p>{summary_text}</p>
                    <p>当前信号：<span style="color:{accent_color};font-weight:800;">{signal_text}</span></p>
                </div>
            </div>
        </div>
    </div>
    """
    st.html(board_html)


def render_watchlist_cyber_dashboard(
    display_df: pd.DataFrame,
    focus_row: pd.Series,
    *,
    current_username: str,
    total_count: int,
    up_count: int,
    down_count: int,
    avg_trend,
) -> None:
    st.markdown(WATCHLIST_CYBER_DASHBOARD_CSS, unsafe_allow_html=True)

    focus_name = _watchlist_html_text(focus_row.get("名称") or focus_row.get("代码"))
    focus_code = str(focus_row.get("代码") or "").strip().upper()
    now = datetime.now()
    weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    clock_text = f"{now:%Y-%m-%d} {weekday_names[now.weekday()]} {now:%H:%M:%S}"
    user_text = _watchlist_html_text(current_username or "未登录")

    ret_series = (
        pd.to_numeric(display_df["涨跌幅(%)"], errors="coerce")
        if "涨跌幅(%)" in display_df.columns
        else pd.Series(dtype=float)
    )
    trend_series = (
        pd.to_numeric(display_df["趋势得分"], errors="coerce")
        if "趋势得分" in display_df.columns
        else pd.Series(dtype=float)
    )
    visible_up_count = int((ret_series > 0).sum()) if not ret_series.empty else up_count
    visible_down_count = int((ret_series < 0).sum()) if not ret_series.empty else down_count
    visible_avg_trend = trend_series.dropna().mean() if not trend_series.dropna().empty else avg_trend
    avg_trend_text = _watchlist_value_text(visible_avg_trend, digits=1)

    leader_text = "-"
    valid_ret_series = ret_series.dropna()
    if not valid_ret_series.empty:
        leader_idx = valid_ret_series.idxmax()
        leader_row = display_df.loc[leader_idx]
        leader_name = _watchlist_html_text(leader_row.get("名称") or leader_row.get("代码"))
        leader_ret = _watchlist_value_text(valid_ret_series.loc[leader_idx], digits=2, suffix="%", signed=True)
        leader_text = f"{leader_name} {leader_ret}"

    stock_card_items = []
    for _, stock_row in display_df.iterrows():
        card_code = str(stock_row.get("代码") or "").strip().upper()
        ret_1d = _watchlist_to_float(stock_row.get("涨跌幅(%)"), 0.0)
        tone_class, arrow, accent_color = _watchlist_signal_tone(ret_1d)
        trend_score = _watchlist_score_value(stock_row.get("趋势得分"))
        card_name = str(stock_row.get("名称") or card_code).strip()
        price_text = _watchlist_value_text(stock_row.get("最新价"), digits=2)
        ret_text = _watchlist_value_text(ret_1d, digits=2, suffix="%", signed=True)
        ret_5d_text = _watchlist_value_text(stock_row.get("5日涨跌(%)"), digits=2, suffix="%", signed=True)
        ret_20d_text = _watchlist_value_text(stock_row.get("20日涨跌(%)"), digits=2, suffix="%", signed=True)
        volume_ratio_text = _watchlist_value_text(stock_row.get("量比"), digits=2)
        signal_text = _watchlist_html_text(stock_row.get("操作信号"))
        trend_status = _watchlist_html_text(stock_row.get("趋势状态"))
        risk_level = _watchlist_html_text(stock_row.get("风险等级"))
        active_class = " is-active" if card_code == focus_code else ""
        safe_code = "".join(ch if ch.isalnum() else "_" for ch in card_code)
        card_html = f"""
        <div class="ws-watchboard-stock-card{active_class}" style="--accent:{accent_color};">
            <div class="ws-watchboard-stock-head">
                <span class="ws-watchboard-stock-name">{_watchlist_html_text(card_name)}</span>
                <span class="ws-watchboard-stock-code">{_watchlist_html_text(card_code)}</span>
            </div>
            <div class="ws-watchboard-stock-price-row">
                <span class="ws-watchboard-stock-price">{price_text}</span>
                <span class="ws-watchboard-stock-ret {tone_class}">{arrow} {ret_text}</span>
            </div>
            <div class="ws-watchboard-stock-metrics">
                <div class="ws-watchboard-stock-metric"><label>5日</label><strong>{ret_5d_text}</strong></div>
                <div class="ws-watchboard-stock-metric"><label>20日</label><strong>{ret_20d_text}</strong></div>
                <div class="ws-watchboard-stock-metric"><label>量比</label><strong>{volume_ratio_text}</strong></div>
            </div>
            <div class="ws-watchboard-stock-score" style="--score:{trend_score}%;"><span></span></div>
            <div class="ws-watchboard-stock-foot">
                <span>{trend_score}分 · {trend_status}</span>
                <span class="ws-watchboard-stock-signal">{signal_text} · {risk_level}</span>
            </div>
        </div>
        """
        stock_card_items.append(
            {
                "code": card_code,
                "safe_code": safe_code,
                "button_label": f"查看 {card_name} 完整详情",
                "card_html": card_html,
            }
        )

    board_html = f"""
    <div class="ws-watchboard-shell is-compact">
        <div class="ws-watchboard-compact-topbar">
            <div class="ws-watchboard-compact-title">
                <strong>自选股全景看板</strong>
                <span>自选用户 {user_text} · 报告/操作焦点 {focus_name} {focus_code} · {clock_text}</span>
            </div>
            <div class="ws-watchboard-compact-meta">AI 量化分析 · 展示 {len(display_df)}/{total_count} 只</div>
        </div>
        <div class="ws-watchboard-summary-row">
            <div class="ws-watchboard-summary-pill"><label>当前展示</label><strong>{len(display_df)} 只</strong></div>
            <div class="ws-watchboard-summary-pill"><label>涨跌分布</label><strong>{visible_up_count} 涨 / {visible_down_count} 跌</strong></div>
            <div class="ws-watchboard-summary-pill"><label>平均趋势</label><strong>{avg_trend_text}</strong></div>
            <div class="ws-watchboard-summary-pill"><label>报告焦点</label><strong>{focus_name}</strong></div>
            <div class="ws-watchboard-summary-pill"><label>最强表现</label><strong>{leader_text}</strong></div>
        </div>
    </div>
    """
    st.html(board_html)

    # ------ 批量选择模式状态 ------
    if "watchlist_batch_mode" not in st.session_state:
        st.session_state["watchlist_batch_mode"] = False

    batch_bar_cols = st.columns([1.2, 1, 1, 1.8])
    with batch_bar_cols[0]:
        if st.button(
            "☑️ 进入批量选择" if not st.session_state["watchlist_batch_mode"] else "✖️ 退出批量选择",
            key="watchlist_toggle_batch_mode",
            use_container_width=True,
        ):
            st.session_state["watchlist_batch_mode"] = not st.session_state["watchlist_batch_mode"]
            # 退出时清空选择
            if not st.session_state["watchlist_batch_mode"]:
                for item in stock_card_items:
                    st.session_state.pop(f"watchlist_batch_sel_{item['safe_code']}", None)
            st.rerun()

    is_batch_mode = st.session_state["watchlist_batch_mode"]

    if is_batch_mode:
        with batch_bar_cols[1]:
            if st.button("✅ 全选", key="watchlist_batch_select_all", use_container_width=True):
                for item in stock_card_items:
                    st.session_state[f"watchlist_batch_sel_{item['safe_code']}"] = True
                st.rerun()
        with batch_bar_cols[2]:
            if st.button("⬜ 取消全选", key="watchlist_batch_deselect_all", use_container_width=True):
                for item in stock_card_items:
                    st.session_state[f"watchlist_batch_sel_{item['safe_code']}"] = False
                st.rerun()

    with st.container(key="watchlist_card_grid"):
        columns_per_row = 6
        for start_idx in range(0, len(stock_card_items), columns_per_row):
            cols = st.columns(columns_per_row)
            for offset, item in enumerate(stock_card_items[start_idx : start_idx + columns_per_row]):
                with cols[offset]:
                    with st.container(key=f"watchlist_card_wrap_{item['safe_code']}"):
                        st.html(item["card_html"])
                        if is_batch_mode:
                            if st.button(
                                f"toggle_{item['code']}",
                                key=f"watchlist_card_btn_{item['safe_code']}",
                                use_container_width=True,
                            ):
                                batch_key = f"watchlist_batch_sel_{item['safe_code']}"
                                st.session_state[batch_key] = not st.session_state.get(batch_key, False)
                                st.rerun()
                            st.checkbox(
                                "选中",
                                key=f"watchlist_batch_sel_{item['safe_code']}",
                                value=st.session_state.get(f"watchlist_batch_sel_{item['safe_code']}", False),
                            )
                        else:
                            if st.button(
                                item["button_label"],
                                key=f"watchlist_card_btn_{item['safe_code']}",
                                use_container_width=True,
                            ):
                                st.session_state["watchlist_pending_focus_code"] = item["code"]
                                st.session_state["watchlist_show_focus_detail"] = True
                                st.rerun()

    # ------ 批量删除操作栏 ------
    if is_batch_mode:
        selected_codes = [
            item["code"]
            for item in stock_card_items
            if st.session_state.get(f"watchlist_batch_sel_{item['safe_code']}", False)
        ]
        selected_count = len(selected_codes)

        st.markdown(
            f"**已选中 {selected_count} 只** / 共 {len(stock_card_items)} 只"
            if selected_count > 0
            else f"请勾选要删除的自选标的（共 {len(stock_card_items)} 只）",
        )

        if selected_count > 0:
            # 查找 security_type 信息
            code_to_type = dict(zip(
                display_df["代码"].astype(str).str.strip().str.upper(),
                display_df["security_type"].astype(str).str.strip().str.lower(),
            ))
            items_to_delete = [
                (code, code_to_type.get(code, "stock"))
                for code in selected_codes
            ]
            selected_names = [
                str(
                    display_df.loc[
                        display_df["代码"].astype(str).str.strip().str.upper() == code, "名称"
                    ].iloc[0]
                )
                if not display_df.loc[
                    display_df["代码"].astype(str).str.strip().str.upper() == code
                ].empty
                else code
                for code in selected_codes
            ]

            with batch_bar_cols[3]:
                if st.button(
                    f"🗑️ 批量删除 {selected_count} 只",
                    key="watchlist_batch_delete_btn",
                    type="primary",
                    use_container_width=True,
                ):
                    st.session_state["watchlist_batch_confirm_pending"] = True
                    st.session_state["watchlist_batch_delete_items"] = items_to_delete
                    st.session_state["watchlist_batch_delete_names"] = selected_names
                    st.rerun()

    # ------ 批量删除确认对话框 ------
    if st.session_state.get("watchlist_batch_confirm_pending"):
        pending_items = st.session_state.get("watchlist_batch_delete_items", [])
        pending_names = st.session_state.get("watchlist_batch_delete_names", [])
        if pending_items:
            st.warning(
                f"⚠️ 确认要从自选中删除以下 **{len(pending_items)}** 只标的？\n\n"
                + "、".join(pending_names[:20])
                + (f"…等共 {len(pending_names)} 只" if len(pending_names) > 20 else "")
            )
            confirm_cols = st.columns([1, 1, 3])
            with confirm_cols[0]:
                if st.button("✅ 确认删除", key="watchlist_batch_confirm_yes", type="primary", use_container_width=True):
                    try:
                        deleted = remove_watchlist_items_batch(current_username, pending_items)
                        st.session_state.pop("watchlist_batch_confirm_pending", None)
                        st.session_state.pop("watchlist_batch_delete_items", None)
                        st.session_state.pop("watchlist_batch_delete_names", None)
                        st.session_state["watchlist_batch_mode"] = False
                        # 清空所有选中状态
                        for item in stock_card_items:
                            st.session_state.pop(f"watchlist_batch_sel_{item['safe_code']}", None)
                        st.success(f"已从自选中删除 {deleted} 只标的")
                        st.rerun()
                    except Exception as batch_del_exc:
                        st.error(f"批量删除失败：{batch_del_exc}")
            with confirm_cols[1]:
                if st.button("❌ 取消", key="watchlist_batch_confirm_no", use_container_width=True):
                    st.session_state.pop("watchlist_batch_confirm_pending", None)
                    st.session_state.pop("watchlist_batch_delete_items", None)
                    st.session_state.pop("watchlist_batch_delete_names", None)
                    st.rerun()


def preload_watchlist_reports_bg(username: str, engine) -> None:
    """在后台静默触发当前登录用户自选股深度出货缓存增量刷新。"""
    import threading

    def _worker():
        try:
            logger.info(f"Started global watchlist distribution refresh after login for {username}")
            refresh_watchlist_distribution_reports(engine)
            logger.info(f"Finished global watchlist distribution refresh after login for {username}")
        except Exception as e:
            logger.error(f"Background preload crashed: {e}")

    # 启动守护线程，不阻塞主程序
    t = threading.Thread(target=_worker, daemon=True)
    t.start()


def trigger_single_stock_research_refresh_bg(username: str, ts_code: str, engine) -> None:
    """在后台静默触发单只自选股票的深度研究报告刷新。"""
    import threading

    normalized_username = normalize_username(username)
    normalized_code = str(ts_code or "").strip().upper()
    if not normalized_username or not normalized_code or engine is None:
        return

    def _worker():
        try:
            logger.info(
                "Started single-stock stock research refresh after add-watchlist for %s / %s",
                normalized_username,
                normalized_code,
            )
            summary = refresh_watchlist_stock_research_reports(
                engine,
                username=normalized_username,
                only_code=normalized_code,
                force=False,
            )
            logger.info(
                "Finished single-stock stock research refresh after add-watchlist for %s / %s: %s",
                normalized_username,
                normalized_code,
                summary,
            )
        except Exception as e:
            logger.error(
                "Single-stock stock research refresh crashed for %s / %s: %s",
                normalized_username,
                normalized_code,
                e,
            )

    t = threading.Thread(target=_worker, daemon=True)
    t.start()


def trigger_single_distribution_refresh_bg(username: str, ts_code: str, engine) -> None:
    """在后台静默触发单只自选股票的主力出货深度分析刷新。"""
    import threading

    normalized_username = normalize_username(username)
    normalized_code = str(ts_code or "").strip().upper()
    if not normalized_username or not normalized_code or engine is None:
        return

    def _worker():
        try:
            logger.info(
                "Started single-stock distribution refresh after add-watchlist for %s / %s",
                normalized_username,
                normalized_code,
            )
            summary = refresh_watchlist_distribution_reports(
                engine,
                username=normalized_username,
                only_code=normalized_code,
            )
            logger.info(
                "Finished single-stock distribution refresh after add-watchlist for %s / %s: %s",
                normalized_username,
                normalized_code,
                summary,
            )
        except Exception as e:
            logger.error(
                "Single-stock distribution refresh crashed for %s / %s: %s",
                normalized_username,
                normalized_code,
                e,
            )

    t = threading.Thread(target=_worker, daemon=True)
    t.start()


@st.dialog("📄 主力出货深度分析报告", width="large")
def show_distribution_report_dialog(report_md: str):
    st.markdown(report_md)


@st.dialog("🧠 个股深度研究报告", width="large")
def show_stock_research_report_dialog(report_md: str, report_html: str | None = None):
    if report_html:
        st.download_button(
            "下载 HTML 报告",
            data=report_html,
            file_name="stock-research-report.html",
            mime="text/html",
            use_container_width=True,
        )
        components.html(report_html, height=820, scrolling=True)
        if report_md:
            with st.expander("Markdown 原文"):
                st.markdown(report_md)
        return
    st.markdown(report_md)


@st.dialog("📑 个股分析报告模板", width="large")
def show_stock_analysis_template_report_dialog(report_html: str, file_name: str = "stock-analysis-template-report.html"):
    st.download_button(
        "下载 HTML 报告",
        data=report_html,
        file_name=file_name,
        mime="text/html",
        use_container_width=True,
    )
    components.html(report_html, height=820, scrolling=True)


def _build_distribution_report_state(ts_code: str, status: dict | None, report_md: str | None = None) -> dict:
    ready_trade_date = (status or {}).get("latest_ready_trade_date")
    return {
        "status": str((status or {}).get("status") or "idle"),
        "ready": bool(report_md) if report_md is not None else bool(ready_trade_date),
        "trade_date": ready_trade_date,
        "report_md": report_md,
        "error_message": (status or {}).get("error_message"),
    }


def _get_distribution_report_state(ts_code: str, engine, *, include_report_md: bool = True) -> dict:
    ts_code_key = str(ts_code or "").strip().upper()
    if not ts_code_key or engine is None:
        return {"status": "missing", "ready": False, "trade_date": None, "report_md": None}

    status = get_report_status(engine, ts_code_key) or {}
    ready_trade_date = status.get("latest_ready_trade_date")
    report_md = get_daily_report(engine, ts_code_key, ready_trade_date) if include_report_md and ready_trade_date else None
    return _build_distribution_report_state(ts_code_key, status, report_md)


def _build_stock_research_report_state(
    ts_code: str,
    status: dict | None,
    report_md: str | None = None,
    report_html: str | None = None,
) -> dict:
    ready_trade_date = (status or {}).get("latest_ready_trade_date")
    return {
        "status": str((status or {}).get("status") or "idle"),
        "ready": bool(report_md or report_html) if report_md is not None or report_html is not None else bool(ready_trade_date),
        "trade_date": ready_trade_date,
        "report_md": report_md,
        "report_html": report_html,
        "error_message": (status or {}).get("error_message"),
    }


def _get_stock_research_report_state(ts_code: str, engine, *, include_report_md: bool = True) -> dict:
    ts_code_key = str(ts_code or "").strip().upper()
    if not ts_code_key or engine is None:
        return {"status": "missing", "ready": False, "trade_date": None, "report_md": None}

    status = get_stock_research_report_status(engine, ts_code_key) or {}
    ready_trade_date = status.get("latest_ready_trade_date")
    record = (
        get_stock_research_daily_report_record(engine, ts_code_key, ready_trade_date)
        if include_report_md and ready_trade_date
        else None
    )
    report_md = str((record or {}).get("report_md") or "") if record else None
    report_html = str((record or {}).get("report_html") or "") if record else None
    return _build_stock_research_report_state(ts_code_key, status, report_md, report_html)


def _format_report_state_text(state: dict, *, ready_prefix: str = "最近报告") -> str:
    if state.get("ready"):
        return f"{ready_prefix}: {state.get('trade_date') or '-'}"
    status = state.get("status")
    if status == "running":
        return "后台更新中"
    if status == "failed":
        error_message = str(state.get("error_message") or "").strip()
        return f"生成失败：{error_message[:36]}" if error_message else "生成失败"
    return "等待后台定时刷新"


def queue_security_search_navigation(ts_code: str, security_type: str) -> None:
    code = str(ts_code or "").strip().upper()
    if not code:
        return

    normalized_type = str(security_type or "").strip().lower()
    st.session_state["pending_security_search_keyword"] = code
    st.session_state["pending_security_search_type"] = "股票" if normalized_type == "stock" else "指数"
    st.session_state["sidebar_nav_group"] = "股票"
    st.session_state["sidebar_expanded_module_id"] = "stock"
    st.session_state["stock_subpage"] = STOCK_SECURITY_SEARCH_LABEL
    st.session_state["jump_to_security_tab"] = True


def should_show_distribution_report_section(security_type: str, already_in_watchlist: bool) -> bool:
    return str(security_type or "").strip().lower() == "stock" and bool(already_in_watchlist)


def _show_watchlist_import_flash() -> None:
    flash = st.session_state.pop("watchlist_import_flash", None)
    if not flash:
        return

    level = flash.get("level", "info")
    message = flash.get("message", "")
    if level == "success":
        st.success(message)
    elif level == "warning":
        st.warning(message)
    elif level == "error":
        st.error(message)
    else:
        st.info(message)


def _clear_watchlist_session_caches() -> None:
    for key in [
        "watchlist_enriched_session_cache",
        "watchlist_report_status_session_cache",
        "watchlist_alert_text_session_cache",
    ]:
        st.session_state.pop(key, None)


def render_watchlist_excel_import_section(
    current_username: str,
    watchlist_df: pd.DataFrame,
    report_engine,
) -> None:
    is_empty_watchlist = watchlist_df is None or watchlist_df.empty
    with st.expander("通过 Excel 批量导入自选池", expanded=is_empty_watchlist):
        st.caption("支持“自选池20260620.xlsx”格式：第一行包含“代码 / 名称”，6 位 A 股代码会自动补全 SH、SZ、BJ 后缀。")
        uploaded_file = st.file_uploader(
            "选择自选池 Excel 文件",
            type=["xlsx", "xlsm"],
            key="watchlist_excel_import_file",
        )
        import_cols = st.columns([1, 2])
        with import_cols[0]:
            import_clicked = st.button(
                "导入到当前用户",
                key="watchlist_excel_import_submit",
                type="primary",
                disabled=not current_username or uploaded_file is None,
                use_container_width=True,
            )
        with import_cols[1]:
            st.caption(f"当前用户：{current_username}" if current_username else "请先登录后再导入")

        if not import_clicked:
            return

        try:
            summary = import_uploaded_watchlist_to_user(
                uploaded_file,
                current_username,
                existing_watchlist_df=watchlist_df,
                report_engine=report_engine,
            )
        except Exception as exc:
            st.error(f"导入失败：{exc}")
            return

        message = (
            f"已解析 {summary.get('parsed', 0)} 只，新增 {summary.get('added', 0)} 只"
            f"，跳过已存在 {summary.get('skipped_existing', 0)} 只"
            f"，跳过无效 {summary.get('skipped_invalid', 0)} 只"
        )
        if summary.get("failed"):
            failed_preview = "；".join(summary.get("failed_items", [])[:3])
            message = f"{message}，失败 {summary.get('failed')} 只：{failed_preview}"

        if summary.get("added", 0) > 0:
            _clear_watchlist_session_caches()
            st.session_state["watchlist_import_flash"] = {"level": "success", "message": message}
            st.rerun()
        elif summary.get("failed"):
            st.warning(message)
        else:
            st.info(message)


def render_user_watchlist_tab() -> None:
    st.subheader("⭐ 自选管理")
    st.caption("登录后管理自己的自选股票，支持从个股查询页一键加入。全景数据总览，辅助投资决策。")

    current_username = get_logged_in_username()
    if not current_username:
        st.info("请先登录用户名，再查看和管理你的自选。")
        return

    _show_watchlist_import_flash()

    try:
        watchlist_df = list_watchlist_items(current_username)
    except Exception as exc:
        st.error(f"加载自选列表失败：{exc}")
        return

    report_engine = get_security_intraday_engine_cached()
    render_watchlist_excel_import_section(current_username, watchlist_df, report_engine)

    if watchlist_df is None or watchlist_df.empty:
        st.info("你的自选还是空的，先去个股/指数查询页加几只吧～")
        return

    ts_codes = tuple(watchlist_df['ts_code'].tolist())
    security_types = tuple(watchlist_df['security_type'].tolist())
    report_status_map: dict[str, dict] = {}
    research_status_map: dict[str, dict] = {}
    stock_report_codes = tuple(
        str(code or "").strip().upper()
        for code, security_type in zip(ts_codes, security_types)
        if str(security_type or "").strip().lower() == "stock" and str(code or "").strip()
    )
    report_status_map, research_status_map = load_watchlist_report_status_maps_session_cached(
        report_engine,
        stock_report_codes,
    )
    
    with st.spinner("正在加载自选股深度数据..."):
        enriched_df = load_watchlist_enriched_data_session_cached(current_username, ts_codes, security_types)
        
    if enriched_df.empty:
        st.warning("数据加载失败，请稍后再试。")
        return

    # 获取并合并预警数据
    alerts_dict = load_watchlist_alert_text_map_session_cached(ts_codes)
    enriched_df['主力异动'] = enriched_df['ts_code'].map(lambda x: alerts_dict.get(str(x).strip().upper(), ""))

    # Metrics Overview
    up_count = len(enriched_df[enriched_df['涨跌幅(%)'] > 0])
    down_count = len(enriched_df[enriched_df['涨跌幅(%)'] < 0])
    avg_trend = enriched_df['趋势得分'].mean()
    
    # 找最大涨跌幅
    valid_ret = enriched_df.dropna(subset=['涨跌幅(%)'])
    if not valid_ret.empty:
        max_up = valid_ret.loc[valid_ret['涨跌幅(%)'].idxmax()]
        max_down = valid_ret.loc[valid_ret['涨跌幅(%)'].idxmin()]
    else:
        max_up, max_down = None, None

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("自选总数", f"{len(enriched_df)} 只")
    col2.metric("今日表现", f"📈 {up_count}上涨 / 📉 {down_count}下跌")
    col3.metric("平均趋势得分", f"{avg_trend:.1f}" if pd.notna(avg_trend) else "-")
    if max_up is not None and max_up['涨跌幅(%)'] > 0:
        col4.metric("最大领涨", f"{max_up['名称']}", f"{max_up['涨跌幅(%)']:.2f}%", delta_color="inverse")
    elif max_down is not None:
        col4.metric("最大领跌", f"{max_down['名称']}", f"{max_down['涨跌幅(%)']:.2f}%", delta_color="inverse")
        
    st.divider()

    # Filters and sorting
    ctrl1, ctrl2, ctrl3 = st.columns([1, 1.5, 2.5])
    with ctrl1:
        view_mode = st.radio("视图模式", ["表格", "看板"], index=1, horizontal=True)
    with ctrl2:
        sort_by = st.selectbox("排序方式", ["趋势得分", "涨跌幅(%)", "总市值(亿)"], index=0)
    with ctrl3:
        filter_signal = st.radio("信号筛选", ["全部", "🔥 强势", "🔻 弱势"], horizontal=True)

    display_df = enriched_df.copy()
    if not display_df.empty:
        display_df['个股研究'] = display_df.apply(
            lambda row: _format_report_state_text(
                _build_stock_research_report_state(
                    str(row.get("代码") or "").strip().upper(),
                    research_status_map.get(str(row.get("代码") or "").strip().upper()),
                ),
                ready_prefix="已生成",
            )
            if str(row.get("security_type") or "").strip().lower() == "stock"
            else "-",
            axis=1,
        )
    
    if filter_signal == "🔥 强势":
        display_df = display_df[display_df['操作信号'].str.contains("🔥", na=False)]
    elif filter_signal == "🔻 弱势":
        display_df = display_df[display_df['操作信号'].str.contains("🔻", na=False)]
        
    display_df = display_df.sort_values(by=sort_by, ascending=False).reset_index(drop=True)

    st.markdown("### 📊 自选数据总览")
    
    if view_mode == "表格":
        hidden_table_cols = ["ts_code", "security_type", "sparkline_prices", "趋势摘要"]
        st.dataframe(
            display_df.drop(columns=hidden_table_cols, errors="ignore"),
            column_config={
                "名称": st.column_config.TextColumn("名称", width="medium"),
                "代码": st.column_config.TextColumn("代码", width="small"),
                "最新价": st.column_config.NumberColumn("最新价", format="%.2f"),
                "涨跌幅(%)": st.column_config.NumberColumn("今日涨跌", format="%+.2f%%"),
                "5日涨跌(%)": st.column_config.NumberColumn("5日涨跌", format="%+.2f%%"),
                "20日涨跌(%)": st.column_config.NumberColumn("20日涨跌", format="%+.2f%%"),
                "5日胜率(%)": st.column_config.NumberColumn("5日胜率", format="%.1f%%"),
                "20日胜率(%)": st.column_config.NumberColumn("20日胜率", format="%.1f%%"),
                "PE_TTM": st.column_config.NumberColumn("PE(TTM)", format="%.1f"),
                "PB": st.column_config.NumberColumn("PB", format="%.2f"),
                "总市值(亿)": st.column_config.NumberColumn("市值(亿)", format="%.0f"),
                "ROE(%)": st.column_config.NumberColumn("ROE", format="%.1f%%"),
                "趋势得分": st.column_config.ProgressColumn(
                    "趋势得分", min_value=0, max_value=100, format="%d"
                ),
                "趋势状态": st.column_config.TextColumn("趋势状态", width="small"),
                "风险得分": st.column_config.ProgressColumn("风险得分", min_value=0, max_value=100, format="%d"),
                "风险等级": st.column_config.TextColumn("风险等级", width="small"),
                "RSI14": st.column_config.NumberColumn("RSI14", format="%.1f"),
                "MACD柱": st.column_config.NumberColumn("MACD柱", format="%.4f"),
                "支撑位": st.column_config.NumberColumn("支撑位", format="%.2f"),
                "压力位": st.column_config.NumberColumn("压力位", format="%.2f"),
                "量比": st.column_config.NumberColumn("量比", format="%.2f"),
                "换手率(%)": st.column_config.NumberColumn("换手率", format="%.2f%%"),
                "波动率20": st.column_config.NumberColumn("20日波动", format="%.1f%%"),
                "成交量(万手)": st.column_config.NumberColumn("成交量(万手)", format="%.2f"),
                "成交额(亿)": st.column_config.NumberColumn("成交额(亿)", format="%.2f"),
                "数据日期": st.column_config.TextColumn("数据日期", width="small"),
                "操作信号": st.column_config.TextColumn("操作信号", width="medium"),
                "主力异动": st.column_config.TextColumn("主力出货预警", width="large"),
                "个股研究": st.column_config.TextColumn("个股深度研究", width="large"),
            },
            use_container_width=True,
            hide_index=True,
        )
    else:
        if display_df.empty:
            st.info("当前筛选条件下暂无自选标的，请调整信号筛选。")
        else:
            focus_options = (
                display_df["名称"].astype(str)
                + " ("
                + display_df["代码"].astype(str)
                + ")"
            ).tolist()
            focus_code_options = display_df["代码"].astype(str).str.strip().str.upper().tolist()
            label_to_code = dict(zip(focus_options, focus_code_options))
            code_to_label = dict(zip(focus_code_options, focus_options))

            requested_focus_code = get_query_param_value("watch_focus").strip().upper()
            requested_detail = get_query_param_value("watch_detail").strip() == "1"
            pending_focus_code = str(st.session_state.pop("watchlist_pending_focus_code", "") or "").strip().upper()
            pending_focus_label = st.session_state.pop("watchlist_pending_focus_label", "")
            if not pending_focus_code and pending_focus_label in label_to_code:
                pending_focus_code = label_to_code[pending_focus_label]

            session_focus_label = st.session_state.get("watchlist_focus_stock_select")
            session_focus_code = label_to_code.get(
                session_focus_label,
                str(st.session_state.get("watchlist_focus_stock_code", "") or "").strip().upper(),
            )
            selected_focus_code = (
                requested_focus_code
                or pending_focus_code
                or session_focus_code
                or focus_code_options[0]
            )
            if selected_focus_code not in code_to_label:
                selected_focus_code = focus_code_options[0]

            if requested_focus_code and requested_focus_code in focus_code_options:
                st.session_state["watchlist_show_focus_detail"] = requested_detail
                try:
                    if "watch_focus" in st.query_params:
                        del st.query_params["watch_focus"]
                    if "watch_detail" in st.query_params:
                        del st.query_params["watch_detail"]
                except Exception:
                    pass
            elif pending_focus_code and pending_focus_code in focus_code_options:
                st.session_state["watchlist_show_focus_detail"] = True

            selected_focus_label = code_to_label[selected_focus_code]
            current_focus_label = st.session_state.get("watchlist_focus_stock_select")
            focus_changed_via_external_trigger = bool(requested_focus_code or pending_focus_code)
            if current_focus_label != selected_focus_label:
                st.session_state["watchlist_focus_stock_select"] = selected_focus_label
                st.session_state["watchlist_focus_stock_code"] = selected_focus_code
                if focus_changed_via_external_trigger:
                    st.rerun()
            else:
                st.session_state["watchlist_focus_stock_code"] = selected_focus_code

            selected_focus_label = st.selectbox(
                "报告/操作焦点",
                options=focus_options,
                key="watchlist_focus_stock_select",
            )
            selected_focus_code = label_to_code.get(selected_focus_label, selected_focus_code)
            st.session_state["watchlist_focus_stock_code"] = selected_focus_code
            focus_idx = focus_code_options.index(selected_focus_code) if selected_focus_code in focus_code_options else 0
            focus_row = display_df.iloc[focus_idx]

            render_watchlist_cyber_dashboard(
                display_df,
                focus_row,
                current_username=current_username,
                total_count=len(enriched_df),
                up_count=up_count,
                down_count=down_count,
                avg_trend=avg_trend,
            )

            report_code = str(focus_row.get("代码") or "").strip().upper()
            if st.session_state.get("watchlist_show_focus_detail"):
                render_watchlist_focus_detail_card(
                    display_df,
                    focus_row,
                    current_username=current_username,
                    total_count=len(enriched_df),
                    up_count=up_count,
                    down_count=down_count,
                    avg_trend=avg_trend,
                )

            action_bar_cols = st.columns([1, 1, 2])
            with action_bar_cols[0]:
                report_state = _build_distribution_report_state(report_code, report_status_map.get(report_code))
                st.caption(_format_report_state_text(report_state, ready_prefix="出货分析"))
                if st.button(
                    "深度出货分析",
                    key=f"btn_dist_cyber_{report_code}",
                    use_container_width=True,
                    disabled=not report_state["ready"],
                ):
                    clicked_state = _get_distribution_report_state(report_code, report_engine, include_report_md=True)
                    if clicked_state["ready"]:
                        show_distribution_report_dialog(clicked_state["report_md"])
                    else:
                        st.warning("报告状态已变化，请等待后台刷新完成。")
            with action_bar_cols[1]:
                research_state = _build_stock_research_report_state(
                    report_code,
                    research_status_map.get(report_code),
                )
                st.caption(_format_report_state_text(research_state, ready_prefix="个股研究"))
                if st.button(
                    "个股深度研究",
                    key=f"btn_research_cyber_{report_code}",
                    use_container_width=True,
                    disabled=not research_state["ready"],
                ):
                    clicked_state = _get_stock_research_report_state(report_code, report_engine, include_report_md=True)
                    if clicked_state["ready"]:
                        show_stock_research_report_dialog(clicked_state["report_md"], clicked_state.get("report_html"))
                    else:
                        st.warning("报告状态已变化，请等待后台定时刷新完成。")
            with action_bar_cols[2]:
                st.caption("上方网格展示当前筛选下的全部自选；这里的按钮跟随“报告/操作焦点”。")

    # 跳转到个股详情
    st.markdown("### 🔍 跳转与管理")
    st.caption("个股深度研究报告只针对自选股票，由后台定时任务生成；页面仅展示缓存状态和已完成报告。")
    action_cols = st.columns([2, 2, 2])
    with action_cols[0]:
        detail_source_df = display_df if not display_df.empty else enriched_df
        options = detail_source_df['名称'] + " (" + detail_source_df['代码'] + ")"
        option_list = options.tolist()
        if option_list:
            selected_for_detail = st.selectbox("选择跳转至详情", options=option_list, key="watchlist_detail_select")
            if st.button("查看详情", type="primary"):
                if selected_for_detail:
                    code = selected_for_detail.split("(")[-1].strip(")")
                    sec_row = detail_source_df[detail_source_df['代码'] == code].iloc[0]
                    queue_security_search_navigation(code, sec_row["security_type"])
                    st.rerun()
        else:
            st.info("暂无可跳转的自选标的。")

    with action_cols[1]:
        all_options = watchlist_df.apply(
            lambda row: f"{str(row.get('security_name') or row.get('ts_code') or '').strip()}（{str(row.get('ts_code') or '').strip()}）",
            axis=1,
        ).tolist()
        if all_options:
            selected_labels = st.multiselect("移除自选（可多选）", options=all_options, key="user_watchlist_remove_multiselect")
            if st.button("删除选中", disabled=len(selected_labels) == 0):
                items_to_remove = []
                for label in selected_labels:
                    idx = all_options.index(label)
                    row = watchlist_df.iloc[idx]
                    items_to_remove.append((
                        str(row.get("ts_code") or ""),
                        str(row.get("security_type") or "stock"),
                    ))
                removed_count = remove_watchlist_items_batch(
                    current_username,
                    items_to_remove,
                )
                if removed_count > 0:
                    st.success(f"已从自选中删除 {removed_count} 只")
                    st.rerun()
                else:
                    st.warning("未删除任何记录，可能已被移除。")

    with action_cols[2]:
        research_source_df = display_df if not display_df.empty else enriched_df
        stock_display_df = research_source_df[
            research_source_df["security_type"].astype(str).str.lower().eq("stock")
        ].copy()
        if stock_display_df.empty:
            st.info("当前自选中没有股票，暂无个股深度研究报告。")
        else:
            research_options = (
                stock_display_df["名称"].astype(str)
                + "（"
                + stock_display_df["代码"].astype(str)
                + "）"
            ).tolist()
            selected_research_label = st.selectbox(
                "个股研究报告",
                options=research_options,
                key="user_watchlist_research_select",
            )
            selected_research_idx = research_options.index(selected_research_label)
            selected_research_row = stock_display_df.iloc[selected_research_idx]
            selected_research_code = str(selected_research_row.get("代码") or "").strip().upper()
            selected_research_name = str(
                selected_research_row.get("名称") or selected_research_code
            ).strip()
            selected_research_state = _build_stock_research_report_state(
                selected_research_code,
                research_status_map.get(selected_research_code),
            )
            st.caption(_format_report_state_text(selected_research_state, ready_prefix="最近生成"))
            if st.button(
                "查看可视化研究",
                key="btn_open_watchlist_stock_research",
                disabled=not selected_research_state["ready"],
            ):
                clicked_state = _get_stock_research_report_state(
                    selected_research_code,
                    report_engine,
                    include_report_md=True,
                )
                if clicked_state["ready"]:
                    show_stock_research_report_dialog(clicked_state["report_md"], clicked_state.get("report_html"))
                else:
                    st.warning("报告状态已变化，请等待后台定时刷新完成。")
            st.caption("模板报告为手动即时生成，不写入原有研究报告缓存。")
            if st.button(
                "生成模板报告",
                key="btn_generate_watchlist_stock_template_report",
                type="primary",
                disabled=not selected_research_code,
            ):
                try:
                    asof_trade_date = str(selected_research_row.get("数据日期") or "").strip() or None
                    with st.spinner(f"正在生成 {selected_research_name} 的模板报告..."):
                        template_bundle = generate_stock_analysis_template_report_bundle(
                            selected_research_code,
                            selected_research_name,
                            engine=report_engine,
                            asof_trade_date=asof_trade_date,
                        )
                    report_html = str(template_bundle.get("report_html") or "")
                    if not report_html:
                        raise RuntimeError("模板 HTML 报告内容为空")
                    if template_bundle.get("cache_hit"):
                        st.info("已读取今日缓存，未重新查询序列或调用大模型。")
                    st.session_state["watchlist_template_report_bundle"] = {
                        "ts_code": selected_research_code,
                        "stock_name": selected_research_name,
                        "report_html": report_html,
                    }
                    show_stock_analysis_template_report_dialog(
                        report_html,
                        file_name=f"{selected_research_code}-stock-analysis-template.html",
                    )
                except Exception as template_report_exc:
                    st.error(f"生成模板报告失败：{template_report_exc}")
            cached_template_report = st.session_state.get("watchlist_template_report_bundle")
            if (
                isinstance(cached_template_report, dict)
                and str(cached_template_report.get("ts_code") or "").strip().upper() == selected_research_code
                and str(cached_template_report.get("report_html") or "").strip()
            ):
                if st.button(
                    "查看本次模板报告",
                    key="btn_open_watchlist_stock_template_report",
                    use_container_width=True,
                ):
                    show_stock_analysis_template_report_dialog(
                        str(cached_template_report.get("report_html") or ""),
                        file_name=f"{selected_research_code}-stock-analysis-template.html",
                    )



def _show_stock_pool_flash() -> None:
    flash = st.session_state.pop("stock_pool_flash", None)
    if not flash:
        return

    level = flash.get("level", "info")
    message = flash.get("message", "")
    if level == "success":
        st.success(message)
    elif level == "warning":
        st.warning(message)
    elif level == "error":
        st.error(message)
    else:
        st.info(message)


def _stock_pool_tag_options(pool_df: pd.DataFrame) -> list[str]:
    tags: list[str] = []
    seen: set[str] = set()
    if pool_df is None or pool_df.empty or "tags" not in pool_df.columns:
        return []
    for value in pool_df["tags"].fillna("").astype(str).tolist():
        for tag in split_stock_pool_tags(value):
            tag_key = tag.lower()
            if tag_key in seen:
                continue
            seen.add(tag_key)
            tags.append(tag)
    return tags


def _stock_pool_industry_options(pool_df: pd.DataFrame) -> list[str]:
    if pool_df is None or pool_df.empty or "industry" not in pool_df.columns:
        return []
    industries = (
        pool_df["industry"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", "未标注行业")
        .drop_duplicates()
        .tolist()
    )
    return sorted([item for item in industries if item])


def _filter_stock_pool_df(
    pool_df: pd.DataFrame,
    keyword: str,
    selected_industries: list[str],
    selected_tags: list[str],
) -> pd.DataFrame:
    if pool_df is None or pool_df.empty:
        return pd.DataFrame()

    filtered_df = pool_df.copy()
    for column in ["security_name", "ts_code", "industry", "tags", "note", "source_file"]:
        if column not in filtered_df.columns:
            filtered_df[column] = ""
        filtered_df[column] = filtered_df[column].fillna("").astype(str)

    normalized_keyword = str(keyword or "").strip().lower()
    if normalized_keyword:
        haystack = (
            filtered_df["security_name"]
            + " "
            + filtered_df["ts_code"]
            + " "
            + filtered_df["industry"]
            + " "
            + filtered_df["tags"]
            + " "
            + filtered_df["note"]
        ).str.lower()
        filtered_df = filtered_df[haystack.str.contains(normalized_keyword, regex=False, na=False)]

    if selected_industries:
        industry_labels = filtered_df["industry"].str.strip().replace("", "未标注行业")
        filtered_df = filtered_df[industry_labels.isin(selected_industries)]

    if selected_tags:
        selected_tag_set = {tag.lower() for tag in selected_tags}
        filtered_df = filtered_df[
            filtered_df["tags"].map(
                lambda value: bool({tag.lower() for tag in split_stock_pool_tags(value)} & selected_tag_set)
            )
        ]

    return filtered_df.reset_index(drop=True)


def _format_stock_pool_datetime(value) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d %H:%M")


def render_stock_pool_import_section(current_username: str, pool_df: pd.DataFrame) -> None:
    is_empty_pool = pool_df is None or pool_df.empty
    with st.expander("通过 Excel 批量导入自选池", expanded=is_empty_pool):
        st.caption(
            "支持“自选池20260620.xlsx”这类表：至少包含“代码 / 名称”，也可包含“行业 / 标签 / 备注”。"
        )
        uploaded_file = st.file_uploader(
            "选择自选池 Excel 文件",
            type=["xlsx", "xlsm"],
            key="stock_pool_excel_import_file",
        )
        import_cols = st.columns([1.6, 1])
        with import_cols[0]:
            default_tags = st.text_input(
                "导入时统一打标签（可选，多个用逗号分隔）",
                value="",
                placeholder="例如：自选池20260620, 中线观察",
                key="stock_pool_import_tags",
            )
        with import_cols[1]:
            st.caption(f"当前用户：{current_username}" if current_username else "请先登录后再导入")
            if uploaded_file is not None:
                st.caption(f"文件：{uploaded_file.name}")

        import_clicked = st.button(
            "导入到当前自选池",
            key="stock_pool_excel_import_submit",
            type="primary",
            disabled=not current_username or uploaded_file is None,
            use_container_width=True,
        )
        if not import_clicked:
            return

        try:
            parsed_rows = parse_stock_pool_import_workbook(uploaded_file)
            summary = import_stock_pool_rows(
                current_username,
                parsed_rows,
                default_tags=default_tags,
                source_file=getattr(uploaded_file, "name", ""),
            )
        except Exception as exc:
            st.error(f"导入失败：{exc}")
            return

        message = (
            f"已解析 {summary.get('parsed', 0)} 只，新增 {summary.get('added', 0)} 只"
            f"，更新 {summary.get('updated', 0)} 只，跳过无效 {summary.get('skipped_invalid', 0)} 只"
        )
        if summary.get("failed"):
            failed_preview = "；".join(summary.get("failed_items", [])[:3])
            message = f"{message}，失败 {summary.get('failed')} 只：{failed_preview}"

        if summary.get("added", 0) > 0 or summary.get("updated", 0) > 0:
            st.session_state["stock_pool_flash"] = {"level": "success", "message": message}
            st.rerun()
        elif summary.get("failed"):
            st.warning(message)
        else:
            st.info(message)


def render_stock_pool_quick_add(current_username: str) -> None:
    with st.expander("手动补充股票", expanded=False):
        add_cols = st.columns([1.2, 1.4, 1.3, 1.6])
        with add_cols[0]:
            ts_code = st.text_input("股票代码", placeholder="688808 / 688808.SH", key="stock_pool_add_code")
        with add_cols[1]:
            security_name = st.text_input("名称（可选）", placeholder="自动留空也可以", key="stock_pool_add_name")
        with add_cols[2]:
            industry = st.text_input("行业（可选）", key="stock_pool_add_industry")
        with add_cols[3]:
            tags = st.text_input("标签（可选）", placeholder="观察, 成长", key="stock_pool_add_tags")

        if st.button(
            "加入自选池",
            key="stock_pool_quick_add_submit",
            type="primary",
            disabled=not current_username or not str(ts_code or "").strip(),
            use_container_width=True,
        ):
            try:
                status = upsert_stock_pool_item(
                    current_username,
                    ts_code,
                    security_name=security_name,
                    industry=industry,
                    tags=tags,
                    source_file="手动添加",
                )
            except Exception as exc:
                st.error(f"加入失败：{exc}")
                return

            action_label = "新增" if status == "inserted" else "更新"
            st.session_state["stock_pool_flash"] = {
                "level": "success",
                "message": f"已{action_label} {ts_code} 到自选池",
            }
            st.rerun()


def render_stock_pool_table(filtered_df: pd.DataFrame) -> None:
    if filtered_df is None or filtered_df.empty:
        st.info("当前筛选条件下没有股票。")
        return

    table_df = filtered_df.copy()
    table_df["个股详情"] = build_security_name_jump_links(
        table_df,
        code_col="ts_code",
        label_col="security_name",
        fallback_col="ts_code",
        nonce_key="stock_pool_detail_link_nonce",
    )
    table_df["名称"] = table_df["security_name"].fillna("").astype(str)
    table_df["代码"] = table_df["ts_code"].fillna("").astype(str)
    table_df["行业"] = table_df["industry"].fillna("").astype(str).str.strip().replace("", "未标注行业")
    table_df["标签"] = table_df["tags"].fillna("").astype(str)
    table_df["备注"] = table_df["note"].fillna("").astype(str)
    table_df["来源"] = table_df["source_file"].fillna("").astype(str)
    table_df["更新时间"] = table_df["updated_at"].map(_format_stock_pool_datetime) if "updated_at" in table_df.columns else ""

    st.dataframe(
        table_df[["个股详情", "名称", "代码", "行业", "标签", "备注", "来源", "更新时间"]],
        use_container_width=True,
        hide_index=True,
        column_config={
            "个股详情": st.column_config.LinkColumn(
                "个股详情",
                help="点击后跳转到个股/指数查询",
                display_text="🔎 查看",
            ),
            "名称": st.column_config.TextColumn("名称", width="medium"),
            "代码": st.column_config.TextColumn("代码", width="small"),
            "行业": st.column_config.TextColumn("行业", width="medium"),
            "标签": st.column_config.TextColumn("标签", width="large"),
            "备注": st.column_config.TextColumn("备注", width="large"),
            "来源": st.column_config.TextColumn("来源", width="medium"),
            "更新时间": st.column_config.TextColumn("更新时间", width="small"),
        },
    )


def render_stock_pool_metadata_editor(current_username: str, filtered_df: pd.DataFrame) -> None:
    if filtered_df is None or filtered_df.empty:
        return

    st.markdown("### 🏷 标签与分组")
    st.caption("直接编辑“行业 / 标签 / 备注”后保存；多个标签用逗号分隔，可用于上方筛选。")
    editor_df = filtered_df[["ts_code", "security_name", "industry", "tags", "note"]].copy()
    editor_df.columns = ["代码", "名称", "行业", "标签", "备注"]
    editor_df["行业"] = editor_df["行业"].fillna("").astype(str)
    editor_df["标签"] = editor_df["标签"].fillna("").astype(str)
    editor_df["备注"] = editor_df["备注"].fillna("").astype(str)

    edited_df = st.data_editor(
        editor_df,
        key="stock_pool_metadata_editor",
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        disabled=["代码", "名称"],
        column_config={
            "代码": st.column_config.TextColumn("代码", width="small"),
            "名称": st.column_config.TextColumn("名称", width="medium"),
            "行业": st.column_config.TextColumn("行业", width="medium"),
            "标签": st.column_config.TextColumn("标签", width="large"),
            "备注": st.column_config.TextColumn("备注", width="large"),
        },
    )

    save_cols = st.columns([1, 2])
    with save_cols[0]:
        save_clicked = st.button(
            "保存标签/备注",
            key="stock_pool_metadata_save",
            type="primary",
            use_container_width=True,
        )
    with save_cols[1]:
        st.caption("保存会写回当前筛选结果中的股票。")

    if not save_clicked:
        return

    original_by_code = {
        str(row.get("ts_code") or "").strip().upper(): row
        for _, row in filtered_df.iterrows()
    }
    update_count = 0
    for _, row in edited_df.iterrows():
        code = str(row.get("代码") or "").strip().upper()
        if not code or code not in original_by_code:
            continue
        original = original_by_code[code]
        new_industry = str(row.get("行业") or "").strip()
        new_tags = format_stock_pool_tags(row.get("标签") or "")
        new_note = str(row.get("备注") or "").strip()
        old_industry = str(original.get("industry") or "").strip()
        old_tags = format_stock_pool_tags(original.get("tags") or "")
        old_note = str(original.get("note") or "").strip()
        if (new_industry, new_tags, new_note) == (old_industry, old_tags, old_note):
            continue

        update_count += update_stock_pool_item_metadata(
            current_username,
            code,
            industry=new_industry,
            tags=new_tags,
            note=new_note,
        )

    if update_count > 0:
        st.session_state["stock_pool_flash"] = {
            "level": "success",
            "message": f"已保存 {update_count} 只股票的标签/备注",
        }
        st.rerun()
    else:
        st.info("没有检测到需要保存的修改。")


def render_stock_pool_delete_section(current_username: str, filtered_df: pd.DataFrame) -> None:
    if filtered_df is None or filtered_df.empty:
        return

    with st.expander("删除自选池条目", expanded=False):
        option_df = filtered_df[["ts_code", "security_name"]].copy()
        option_labels = option_df.apply(
            lambda row: f"{str(row.get('security_name') or row.get('ts_code') or '').strip()}（{str(row.get('ts_code') or '').strip()}）",
            axis=1,
        ).tolist()
        selected_labels = st.multiselect(
            "选择要删除的股票",
            options=option_labels,
            key="stock_pool_remove_multiselect",
        )
        if st.button(
            "删除选中",
            key="stock_pool_remove_submit",
            disabled=len(selected_labels) == 0,
            use_container_width=True,
        ):
            codes_to_remove = []
            for label in selected_labels:
                idx = option_labels.index(label)
                codes_to_remove.append(str(option_df.iloc[idx]["ts_code"] or ""))
            removed_count = remove_stock_pool_items_batch(current_username, codes_to_remove)
            if removed_count > 0:
                st.session_state["stock_pool_flash"] = {
                    "level": "success",
                    "message": f"已从自选池删除 {removed_count} 只股票",
                }
                st.rerun()
            else:
                st.warning("未删除任何记录，可能已被移除。")


def render_user_stock_pool_tab() -> None:
    st.subheader("🗂 自选池")
    st.caption("独立于“自选管理”的用户股票池，适合批量导入、打标签分组、按行业筛选和跳转个股详情。")

    current_username = get_logged_in_username()
    if not current_username:
        st.info("请先登录用户名，再查看和管理你的自选池。")
        return

    _show_stock_pool_flash()

    try:
        pool_df = list_stock_pool_items(current_username)
    except Exception as exc:
        st.error(f"加载自选池失败：{exc}")
        return

    render_stock_pool_import_section(current_username, pool_df)
    render_stock_pool_quick_add(current_username)

    if pool_df is None or pool_df.empty:
        st.info("你的自选池还是空的，可以先导入“自选池20260620.xlsx”这类 Excel。")
        return

    all_tags = _stock_pool_tag_options(pool_df)
    industry_options = _stock_pool_industry_options(pool_df)

    metric_cols = st.columns(4)
    metric_cols[0].metric("自选池股票", f"{len(pool_df):,} 只")
    metric_cols[1].metric("行业数量", f"{len([item for item in industry_options if item != '未标注行业']):,}")
    metric_cols[2].metric("标签数量", f"{len(all_tags):,}")
    latest_update = ""
    if "updated_at" in pool_df.columns and not pool_df["updated_at"].isna().all():
        latest_update = _format_stock_pool_datetime(pool_df["updated_at"].max())
    metric_cols[3].metric("最近更新", latest_update or "-")

    filter_cols = st.columns([1.2, 1.4, 1.4])
    with filter_cols[0]:
        keyword = st.text_input(
            "搜索",
            placeholder="代码、名称、行业、标签、备注",
            key="stock_pool_keyword",
        )
    with filter_cols[1]:
        selected_industries = st.multiselect(
            "行业筛选",
            options=industry_options,
            key="stock_pool_industry_filter",
        )
    with filter_cols[2]:
        selected_tags = st.multiselect(
            "标签筛选",
            options=all_tags,
            key="stock_pool_tag_filter",
        )

    filtered_df = _filter_stock_pool_df(pool_df, keyword, selected_industries, selected_tags)
    st.caption(f"当前显示 {len(filtered_df):,} / {len(pool_df):,} 只股票")

    render_stock_pool_table(filtered_df)
    render_stock_pool_metadata_editor(current_username, filtered_df)
    render_stock_pool_delete_section(current_username, filtered_df)


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
    kline_df = None

    try:
        profile_df = load_security_profile(selected_code, selected_type)
        ts_df = load_security_timeseries(selected_code, selected_type)
        if selected_type == 'stock':
            financial_df = load_security_financial_timeseries(selected_code, selected_type)
            kline_df = load_security_kline_timeseries(selected_code, selected_type)
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

    if kline_df is not None and len(kline_df) > 0:
        kline_df = kline_df.copy()
        kline_df['trade_date'] = pd.to_datetime(kline_df['trade_date'], errors='coerce')
        kline_df = kline_df.dropna(subset=['trade_date']).sort_values('trade_date')

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
    if selected_type == 'stock' and bool(profile.get('has_ever_st')):
        st.warning("🏷️ 标签：曾经ST")

    already_in_watchlist = False
    if selected_type == 'stock':
        current_username = get_logged_in_username()
        watchlist_cols = st.columns([1.4, 2.2])
        if current_username:
            try:
                already_in_watchlist = is_in_watchlist(current_username, selected_code, selected_type)
            except Exception as watchlist_check_exc:
                st.warning(f"检查自选状态失败：{watchlist_check_exc}")

            button_label = "✅ 已在自选" if already_in_watchlist else "⭐ 加入自选"
            if watchlist_cols[0].button(button_label, key=f"btn_add_watchlist_{selected_type}_{selected_code}", disabled=already_in_watchlist):
                try:
                    add_watchlist_item(
                        current_username,
                        selected_code,
                        security_name=title_name,
                        security_type=selected_type,
                    )
                    if selected_type == 'stock':
                        try:
                            report_engine = get_security_intraday_engine_cached()
                            trigger_single_distribution_refresh_bg(current_username, selected_code, report_engine)
                            trigger_single_stock_research_refresh_bg(current_username, selected_code, report_engine)
                        except Exception as trigger_refresh_exc:
                            logger.warning(
                                f"Failed to trigger report refresh for {current_username}/{selected_code}: {trigger_refresh_exc}"
                            )
                    st.success(f"已将 {title_name} 加入 {current_username} 的自选")
                    st.rerun()
                except Exception as add_watchlist_exc:
                    st.error(f"加入自选失败：{add_watchlist_exc}")
            watchlist_cols[1].caption(f"当前登录用户：{current_username}｜加入后可在“{STOCK_USER_WATCHLIST_LABEL}”查看")
        else:
            watchlist_cols[0].button("⭐ 加入自选", key=f"btn_add_watchlist_disabled_{selected_type}_{selected_code}", disabled=True)
            watchlist_cols[1].info("先登录用户名，才能把股票加入个人自选。")

    latest_trade_date = format_optional_date(profile.get('latest_trade_date'))
    if selected_type == 'stock':
        metric_cols_top = st.columns(5)
        metric_cols_top[0].metric("最新交易日", latest_trade_date)
        metric_cols_top[1].metric("收盘价(元)", format_optional_number(profile.get('close')))
        metric_cols_top[2].metric("PE_TTM", format_optional_number(profile.get('pe_ttm')))
        metric_cols_top[3].metric("PB", format_optional_number(profile.get('pb')))
        metric_cols_top[4].metric("总市值(亿元)", format_optional_number(profile.get('total_mv'), scale=10000.0))

        metric_cols_bottom = st.columns(6)
        metric_cols_bottom[0].metric("ROE(%)", format_optional_number(profile.get('roe')))
        metric_cols_bottom[1].metric("ROA(%)", format_optional_number(profile.get('roa')))
        metric_cols_bottom[2].metric("毛利率(%)", format_optional_number(profile.get('gross_margin')))
        metric_cols_bottom[3].metric("净利润(亿元)", format_optional_number(profile.get('n_income'), scale=100000000.0))
        metric_cols_bottom[4].metric("经营现金流(亿元)", format_optional_number(profile.get('n_cashflow_act'), scale=100000000.0))
        holder_metric_value, holder_metric_delta = format_holder_number_metric(
            profile.get('holder_num'),
            profile.get('holder_end_date'),
        )
        metric_cols_bottom[5].metric("最新股东人数", holder_metric_value, holder_metric_delta)

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
                    {"字段": "股东人数截止日", "值": format_optional_date(profile.get('holder_end_date'))},
                    {"字段": "股东人数公告日", "值": format_optional_date(profile.get('holder_ann_date'))},
                    {"字段": "总资产(亿元)", "值": format_optional_number(profile.get('total_assets'), scale=100000000.0)},
                    {"字段": "总负债(亿元)", "值": format_optional_number(profile.get('total_liab'), scale=100000000.0)},
                ]),
                use_container_width=True,
                hide_index=True
            )
        
        st.markdown("##### 📜 主营与产品")
        st.info(f"**主要业务**：{profile.get('main_business') or '-'}")
        st.info(f"**产品及业务范围**：{profile.get('business_scope') or '-'}")

        if should_show_distribution_report_section(selected_type, already_in_watchlist):
            st.markdown("##### 🚨 主力出货深度分析")
            st.info("报告改为后台增量刷新后生成。按钮仅在数据库中已有可用缓存时启用，点击后直接秒开缓存报告。")
            detail_report_engine = get_security_intraday_engine_cached()
            detail_report_state = _get_distribution_report_state(
                selected_code,
                detail_report_engine,
                include_report_md=False,
            )
            if detail_report_state["ready"]:
                st.caption(f"最近报告日期：{detail_report_state['trade_date']}")
            elif detail_report_state["status"] == "running":
                st.caption("后台更新中")
            elif detail_report_state["status"] == "failed":
                st.caption("最近一次生成失败，等待后台重试")
            else:
                st.caption("后台尚未生成可用报告")

            if st.button(
                f"🔮 查看【{title_name}】深度出货报告",
                key=f"btn_dist_{selected_code}",
                disabled=not detail_report_state["ready"],
            ):
                clicked_state = _get_distribution_report_state(
                    selected_code,
                    detail_report_engine,
                    include_report_md=True,
                )
                if clicked_state["ready"]:
                    show_distribution_report_dialog(clicked_state["report_md"])
                else:
                    st.warning("报告状态已变化，请等待后台刷新完成。")


        top10_cache = st.session_state.setdefault("security_top10_cache", {})
        top10_pack = top10_cache.get(selected_code)
        if not isinstance(top10_pack, dict):
            try:
                from src.fund_hot_stocks import query_stock_top10_shareholders
                with st.spinner("加载股东情况..."):
                    top10_pack = query_stock_top10_shareholders(symbol=selected_code)
            except Exception as top10_exc:
                logger.warning(f"query_stock_top10_shareholders failed in security search: {top10_exc}", exc_info=True)
                top10_pack = {
                    "top10_holders": pd.DataFrame(),
                    "top10_floatholders": pd.DataFrame(),
                    "errors": {"query": str(top10_exc)},
                }
            top10_cache[selected_code] = top10_pack

        _render_top10_shareholder_panel(
            top10_pack.get("top10_holders", pd.DataFrame()),
            top10_pack.get("top10_floatholders", pd.DataFrame()),
            stock_title_for_top10=f"{title_name}（{selected_code}）",
            top10_errors=top10_pack.get("errors", {}) or {},
            expanded=False,
        )

        # ── 股价 & 股东人数趋势图 ──────────────────────────────────────────
        with st.expander("📊 股价与股东人数趋势", expanded=False):
            try:
                holder_ts_df = load_stock_holder_number_timeseries(selected_code)
            except Exception as holder_ts_exc:
                logger.warning(f"load holder number timeseries failed: {holder_ts_exc}", exc_info=True)
                holder_ts_df = pd.DataFrame()

            if holder_ts_df is not None and not holder_ts_df.empty:
                holder_ts_df = holder_ts_df.copy()
                holder_ts_df['end_date'] = pd.to_datetime(holder_ts_df['end_date'], errors='coerce')
                holder_ts_df['holder_num'] = pd.to_numeric(holder_ts_df['holder_num'], errors='coerce')
                holder_ts_df = holder_ts_df.dropna(subset=['end_date', 'holder_num']).sort_values('end_date')

                if len(holder_ts_df) >= 2:
                    # Prepare price data aligned to holder_num date range
                    holder_min_date = holder_ts_df['end_date'].min()
                    holder_max_date = holder_ts_df['end_date'].max()
                    price_for_holder = filtered_df[
                        (filtered_df['trade_date'] >= holder_min_date) &
                        (filtered_df['trade_date'] <= holder_max_date + pd.Timedelta(days=30))
                    ].copy() if not filtered_df.empty else pd.DataFrame()
                    if price_for_holder.empty:
                        price_for_holder = ts_df[
                            (ts_df['trade_date'] >= holder_min_date) &
                            (ts_df['trade_date'] <= holder_max_date + pd.Timedelta(days=30))
                        ].copy()

                    fig_holder_trend = make_subplots(specs=[[{"secondary_y": True}]])
                    price_plot = pd.DataFrame()

                    # Stock price line (left Y)
                    if not price_for_holder.empty:
                        price_for_holder['close'] = pd.to_numeric(price_for_holder['close'], errors='coerce')
                        price_plot = price_for_holder.dropna(subset=['close'])
                        fig_holder_trend.add_trace(
                            go.Scatter(
                                x=price_plot['trade_date'],
                                y=price_plot['close'],
                                mode='lines',
                                name='收盘价(元)',
                                line=dict(width=2.2, shape='spline', color=THEME_NAVY),
                                fill='tozeroy',
                                fillcolor=CHART_NAVY_SOFT_FILL,
                                hovertemplate='%{x|%Y-%m-%d}<br>收盘价: %{y:,.2f}元<extra></extra>',
                            ),
                            secondary_y=False,
                        )

                    # Holder number bar + line (right Y)
                    holder_num_values = holder_ts_df['holder_num'].values
                    holder_colors = []
                    for i, val in enumerate(holder_num_values):
                        if i == 0:
                            holder_colors.append(THEME_NEUTRAL)
                        elif val > holder_num_values[i - 1]:
                            holder_colors.append(THEME_DOWN)  # green = more holders (bearish signal)
                        elif val < holder_num_values[i - 1]:
                            holder_colors.append(THEME_UP)    # red = fewer holders (bullish signal)
                        else:
                            holder_colors.append(THEME_NEUTRAL)

                    fig_holder_trend.add_trace(
                        go.Bar(
                            x=holder_ts_df['end_date'],
                            y=holder_ts_df['holder_num'],
                            name='股东人数',
                            marker=dict(color=holder_colors, opacity=0.5),
                            hovertemplate='%{x|%Y-%m-%d}<br>股东人数: %{y:,.0f}<extra></extra>',
                            width=86400000 * 20,  # ~20 days in ms
                        ),
                        secondary_y=True,
                    )
                    fig_holder_trend.add_trace(
                        go.Scatter(
                            x=holder_ts_df['end_date'],
                            y=holder_ts_df['holder_num'],
                            mode='lines+markers',
                            name='股东人数趋势',
                            line=dict(width=2, shape='spline', color=THEME_PURPLE, dash='dot'),
                            marker=dict(size=6, color=THEME_PURPLE),
                            hovertemplate='%{x|%Y-%m-%d}<br>股东人数: %{y:,.0f}<extra></extra>',
                        ),
                        secondary_y=True,
                    )

                    fig_holder_trend.update_layout(
                        title=dict(
                            text=f'{title_name} — 股价与股东人数趋势',
                            x=0.02,
                            font=dict(size=18, color=THEME_TEXT),
                        ),
                        hovermode='x unified',
                        height=480,
                        template='plotly_white',
                        plot_bgcolor=CHART_BG,
                        paper_bgcolor=CHART_PAPER_BG,
                        font=dict(family='Inter, PingFang SC, sans-serif'),
                        margin=dict(l=20, r=20, t=55, b=20),
                        legend=dict(
                            orientation='h', yanchor='bottom', y=1.02,
                            xanchor='left', x=0,
                            font=dict(size=12),
                        ),
                        barmode='overlay',
                    )
                    apply_time_series_hover_affordance(
                        fig_holder_trend,
                        holder_ts_df['end_date'],
                        price_plot['close'] if not price_plot.empty else holder_ts_df['holder_num'],
                        add_hover_target=not price_plot.empty,
                    )
                    fig_holder_trend.update_xaxes(
                        showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR,
                    )
                    fig_holder_trend.update_yaxes(
                        title_text='收盘价(元)', secondary_y=False,
                        showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR,
                    )
                    fig_holder_trend.update_yaxes(
                        title_text='股东人数', secondary_y=True,
                        showgrid=False,
                    )
                    st.plotly_chart(fig_holder_trend, use_container_width=True)

                    # Summary metrics
                    latest_holder = holder_ts_df.iloc[-1]
                    prev_holder = holder_ts_df.iloc[-2] if len(holder_ts_df) >= 2 else None
                    hm_cols = st.columns(4)
                    hm_cols[0].metric(
                        '最新股东人数',
                        f"{int(latest_holder['holder_num']):,}",
                        f"截止 {latest_holder['end_date'].strftime('%Y-%m-%d')}",
                    )
                    if prev_holder is not None:
                        delta_num = int(latest_holder['holder_num'] - prev_holder['holder_num'])
                        delta_pct = (
                            (latest_holder['holder_num'] - prev_holder['holder_num'])
                            / prev_holder['holder_num'] * 100
                            if prev_holder['holder_num'] > 0 else 0
                        )
                        sign = '+' if delta_num >= 0 else ''
                        hm_cols[1].metric(
                            '较上期变动',
                            f"{sign}{delta_num:,}",
                            f"{sign}{delta_pct:.2f}%",
                            delta_color='inverse',  # fewer holders = bullish = green
                        )
                    hm_cols[2].metric('数据期数', f"{len(holder_ts_df)} 期")
                    holder_range_min = holder_ts_df['holder_num'].min()
                    holder_range_max = holder_ts_df['holder_num'].max()
                    hm_cols[3].metric(
                        '区间极值',
                        f"{int(holder_range_min):,} ~ {int(holder_range_max):,}",
                    )
                    st.caption(
                        '💡 股东人数减少通常表示筹码集中（看多信号），增加则表示筹码分散。'
                        '红色柱=人数减少，绿色柱=人数增加。'
                    )
                else:
                    st.info('股东人数历史数据不足（少于 2 期），暂无法绘制趋势图。')
            else:
                st.info('暂无股东人数历史数据。')

        
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

        fund_holding_period_options = load_fund_hot_stock_periods()
        _render_security_fund_holding_panel(selected_code, title_name, fund_holding_period_options)

        top10_panel_pref_key = "security_top10_panel_expanded"
        if top10_panel_pref_key not in st.session_state:
            st.session_state[top10_panel_pref_key] = False
        st.toggle(
            "展开股东结构模块",
            key=top10_panel_pref_key,
            help="记住当前会话的展开偏好。",
        )

        with st.expander(
            "🧱 前十大股东 / 前十大流通股东",
            expanded=bool(st.session_state.get(top10_panel_pref_key, False)),
        ):
            st.caption("入口已迁移到个股查询页，可按报告期直接查询股东结构（缓存 5 分钟，可强制刷新）。")

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
                    "查询前十大股东",
                    type="primary",
                    key=f"btn_security_top10_{selected_code}",
                )
                force_refresh_top10_clicked = st.button(
                    "强制刷新(忽略缓存)",
                    key=f"btn_security_top10_force_refresh_{selected_code}",
                    help="清理 5 分钟缓存并重新请求最新数据。",
                )

            if force_refresh_top10_clicked:
                load_security_top10_shareholders.clear()

            top10_query_signature = f"{selected_code}|{top10_period}"
            auto_query_needed = st.session_state.get("security_top10_last_signature") != top10_query_signature

            if query_top10_clicked or force_refresh_top10_clicked or auto_query_needed:
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

                prev_period = None
                try:
                    cur_idx = top10_period_options.index(top10_period)
                    if cur_idx + 1 < len(top10_period_options):
                        prev_period = top10_period_options[cur_idx + 1]
                except ValueError:
                    prev_period = None

                if prev_period:
                    prev_top10_holders = pd.DataFrame()
                    prev_top10_floatholders = pd.DataFrame()
                    prev_error = ""
                    try:
                        prev_pack = load_security_top10_shareholders(
                            symbol=selected_code,
                            period=str(prev_period).replace("-", ""),
                        )
                        prev_top10_holders = prev_pack.get("top10_holders", pd.DataFrame())
                        prev_top10_floatholders = prev_pack.get("top10_floatholders", pd.DataFrame())
                    except Exception as prev_exc:
                        prev_error = str(prev_exc)

                    prev_has_holder = isinstance(prev_top10_holders, pd.DataFrame) and not prev_top10_holders.empty
                    prev_has_float = isinstance(prev_top10_floatholders, pd.DataFrame) and not prev_top10_floatholders.empty

                    if prev_has_holder or prev_has_float:
                        prev_holder_total_ratio = pd.to_numeric(prev_top10_holders.get("hold_ratio"), errors="coerce").fillna(0).sum() if prev_has_holder else 0
                        prev_holder_top3_ratio = pd.to_numeric(prev_top10_holders.get("hold_ratio"), errors="coerce").fillna(0).head(3).sum() if prev_has_holder else 0
                        prev_float_total_ratio = pd.to_numeric(prev_top10_floatholders.get("hold_float_ratio"), errors="coerce").fillna(0).sum() if prev_has_float else 0

                        def _fmt_delta_pct(v):
                            if pd.isna(v):
                                return "-"
                            sign = "+" if float(v) >= 0 else ""
                            return f"{sign}{float(v):.2f}%"

                        curr_holder_names = set(str(x).strip() for x in top10_holders.get("holder_name", pd.Series(dtype=str)).dropna().tolist() if str(x).strip())
                        prev_holder_names = set(str(x).strip() for x in prev_top10_holders.get("holder_name", pd.Series(dtype=str)).dropna().tolist() if str(x).strip())
                        holder_new_count = len(curr_holder_names - prev_holder_names)
                        holder_exit_count = len(prev_holder_names - curr_holder_names)

                        st.markdown("##### 📊 与上期对比")
                        cmp_cols = st.columns(4)
                        cmp_cols[0].metric("前十合计持股变化", _fmt_pct(holder_total_ratio), _fmt_delta_pct(holder_total_ratio - prev_holder_total_ratio))
                        cmp_cols[1].metric("前三集中度变化", _fmt_pct(holder_top3_ratio), _fmt_delta_pct(holder_top3_ratio - prev_holder_top3_ratio))
                        cmp_cols[2].metric("前十流通锁仓变化", _fmt_pct(float_total_ratio), _fmt_delta_pct(float_total_ratio - prev_float_total_ratio))
                        cmp_cols[3].metric("股东名单变化", f"+{holder_new_count} / -{holder_exit_count}")
                        st.caption(f"对比基准报告期：{prev_period}（当前：{top10_period}）")

                        with st.expander("🧾 新增/退出股东名单", expanded=False):
                            holder_name_col = "holder_name"
                            curr_holder_df = top10_holders.copy() if has_top10_holders else pd.DataFrame()
                            prev_holder_df = prev_top10_holders.copy() if prev_has_holder else pd.DataFrame()

                            if holder_name_col in curr_holder_df.columns:
                                curr_holder_df[holder_name_col] = curr_holder_df[holder_name_col].astype(str).str.strip()
                                curr_holder_df = curr_holder_df[curr_holder_df[holder_name_col] != ""]
                            else:
                                curr_holder_df = pd.DataFrame(columns=[holder_name_col])

                            if holder_name_col in prev_holder_df.columns:
                                prev_holder_df[holder_name_col] = prev_holder_df[holder_name_col].astype(str).str.strip()
                                prev_holder_df = prev_holder_df[prev_holder_df[holder_name_col] != ""]
                            else:
                                prev_holder_df = pd.DataFrame(columns=[holder_name_col])

                            new_holder_names = sorted(curr_holder_names - prev_holder_names)
                            exit_holder_names = sorted(prev_holder_names - curr_holder_names)

                            list_col_new, list_col_exit = st.columns(2)

                            with list_col_new:
                                st.markdown(f"**🆕 新增股东（{len(new_holder_names)}）**")
                                if new_holder_names:
                                    show_new = curr_holder_df[curr_holder_df[holder_name_col].isin(new_holder_names)].copy()
                                    if not show_new.empty:
                                        for col in ["hold_ratio", "hold_float_ratio", "hold_amount", "hold_change"]:
                                            if col in show_new.columns:
                                                show_new[col] = pd.to_numeric(show_new[col], errors="coerce")
                                        show_new = show_new.rename(columns={
                                            "holder_name": "股东名称",
                                            "hold_ratio": "占总股本比(%)",
                                            "hold_float_ratio": "占流通股比(%)",
                                            "hold_amount": "持股数量",
                                            "hold_change": "持股变动",
                                        })
                                        if "占总股本比(%)" in show_new.columns:
                                            show_new["占总股本比(%)"] = show_new["占总股本比(%)"].map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
                                        if "占流通股比(%)" in show_new.columns:
                                            show_new["占流通股比(%)"] = show_new["占流通股比(%)"].map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
                                        if "持股数量" in show_new.columns:
                                            show_new["持股数量"] = show_new["持股数量"].map(lambda v: f"{v:,.0f}" if pd.notna(v) else "-")
                                        if "持股变动" in show_new.columns:
                                            show_new["持股变动"] = show_new["持股变动"].map(lambda v: f"{v:,.0f}" if pd.notna(v) else "-")
                                        keep_cols = [c for c in ["股东名称", "占总股本比(%)", "占流通股比(%)", "持股数量", "持股变动"] if c in show_new.columns]
                                        st.dataframe(show_new[keep_cols], use_container_width=True, hide_index=True)
                                    else:
                                        st.write("- " + "\n- ".join(new_holder_names))
                                else:
                                    st.info("本期无新增股东")

                            with list_col_exit:
                                st.markdown(f"**📤 退出股东（{len(exit_holder_names)}）**")
                                if exit_holder_names:
                                    show_exit = prev_holder_df[prev_holder_df[holder_name_col].isin(exit_holder_names)].copy()
                                    if not show_exit.empty:
                                        for col in ["hold_ratio", "hold_float_ratio", "hold_amount", "hold_change"]:
                                            if col in show_exit.columns:
                                                show_exit[col] = pd.to_numeric(show_exit[col], errors="coerce")
                                        show_exit = show_exit.rename(columns={
                                            "holder_name": "股东名称",
                                            "hold_ratio": "占总股本比(%)",
                                            "hold_float_ratio": "占流通股比(%)",
                                            "hold_amount": "持股数量",
                                            "hold_change": "持股变动",
                                        })
                                        if "占总股本比(%)" in show_exit.columns:
                                            show_exit["占总股本比(%)"] = show_exit["占总股本比(%)"].map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
                                        if "占流通股比(%)" in show_exit.columns:
                                            show_exit["占流通股比(%)"] = show_exit["占流通股比(%)"].map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
                                        if "持股数量" in show_exit.columns:
                                            show_exit["持股数量"] = show_exit["持股数量"].map(lambda v: f"{v:,.0f}" if pd.notna(v) else "-")
                                        if "持股变动" in show_exit.columns:
                                            show_exit["持股变动"] = show_exit["持股变动"].map(lambda v: f"{v:,.0f}" if pd.notna(v) else "-")
                                        keep_cols = [c for c in ["股东名称", "占总股本比(%)", "占流通股比(%)", "持股数量", "持股变动"] if c in show_exit.columns]
                                        st.dataframe(show_exit[keep_cols], use_container_width=True, hide_index=True)
                                    else:
                                        st.write("- " + "\n- ".join(exit_holder_names))
                                else:
                                    st.info("本期无退出股东")
                    elif prev_error:
                        st.caption(f"上期对比数据加载失败：{prev_error}")
                    else:
                        st.caption(f"上期（{prev_period}）暂无可对比数据。")
                else:
                    st.caption("暂无更早报告期，暂不展示上期对比。")

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
        line=dict(width=2.6, shape='spline', color=THEME_NAVY),
        fill='tozeroy',
        fillcolor=CHART_NAVY_SOFT_FILL,
        hovertemplate=f"<b>{title_name}</b><br>%{{x|%Y-%m-%d}}<br>{metric_label}: %{{y:,.{metric_digits}f}}<extra></extra>"
    ))
    fig.update_layout(
        title=dict(text=f'{title_name} — {metric_label}趋势', x=0.02, font=dict(size=20, color=THEME_TEXT)),
        xaxis_title='日期',
        yaxis_title=metric_label,
        hovermode='x unified',
        height=500,
        template='plotly_white',
        plot_bgcolor=CHART_BG,
        paper_bgcolor=CHART_PAPER_BG,
        font=dict(family='Inter, PingFang SC, sans-serif'),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    apply_time_series_hover_affordance(fig, chart_df['trade_date'], chart_df['metric_value'])
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR)
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR, fixedrange=True)
    st.plotly_chart(fig, use_container_width=True)

    if selected_type == 'stock':
        tab_kline, tab_valuation, tab_financial, tab_capital = st.tabs(["🕯️ K线", "📈 估值", "🧾 财务", "🏦 市值股本"])

        with tab_kline:
            st.caption("支持日线/周线/月线K线，默认联动上方时间范围。当前价格序列为不复权日线 + 周/月聚合。")
            if kline_df is None or kline_df.empty:
                st.info("暂无可用K线数据。")
            else:
                kline_ctl_cols = st.columns([1.2, 1.4, 1.0])
                with kline_ctl_cols[0]:
                    kline_freq = st.radio(
                        "K线周期",
                        options=["日线", "周线", "月线"],
                        horizontal=True,
                        key=f"security_kline_freq_{selected_code}",
                    )
                with kline_ctl_cols[1]:
                    if kline_freq == "日线":
                        kline_quick = st.radio(
                            "日线快捷区间",
                            options=["60日", "120日", "250日", "自定义"],
                            horizontal=True,
                            key=f"security_kline_quick_{selected_code}",
                        )
                        if kline_quick == "自定义":
                            kline_bars = st.slider(
                                "显示最近K线数量",
                                min_value=30,
                                max_value=400,
                                value=160,
                                step=10,
                                key=f"security_kline_bars_{selected_code}",
                            )
                        else:
                            quick_map = {"60日": 60, "120日": 120, "250日": 250}
                            kline_bars = quick_map.get(kline_quick, 120)
                            st.caption(f"快捷显示最近 {kline_bars} 根日K")
                    else:
                        kline_bars = st.slider(
                            "显示最近K线数量",
                            min_value=30,
                            max_value=400,
                            value=160,
                            step=10,
                            key=f"security_kline_bars_{selected_code}",
                        )
                with kline_ctl_cols[2]:
                    st.metric("K线样本", f"{len(kline_df):,} 条")

                ma_ctl_cols = st.columns([1.0, 1.0, 1.4])
                with ma_ctl_cols[0]:
                    show_ma5 = st.checkbox("MA5", value=True, key=f"security_kline_ma5_{selected_code}")
                with ma_ctl_cols[1]:
                    show_ma10 = st.checkbox("MA10", value=True, key=f"security_kline_ma10_{selected_code}")
                with ma_ctl_cols[2]:
                    show_ma20 = st.checkbox("MA20", value=False, key=f"security_kline_ma20_{selected_code}")
                ma_windows = []
                if show_ma5:
                    ma_windows.append(5)
                if show_ma10:
                    ma_windows.append(10)
                if show_ma20:
                    ma_windows.append(20)
                if not ma_windows:
                    ma_windows = [5, 10]

                vol_ma_ctl_cols = st.columns([1.0, 1.0, 1.4])
                with vol_ma_ctl_cols[0]:
                    show_vol_ma5 = st.checkbox("VOL_MA5", value=True, key=f"security_kline_vol_ma5_{selected_code}")
                with vol_ma_ctl_cols[1]:
                    show_vol_ma10 = st.checkbox("VOL_MA10", value=True, key=f"security_kline_vol_ma10_{selected_code}")
                with vol_ma_ctl_cols[2]:
                    show_vol_ma20 = st.checkbox("VOL_MA20", value=False, key=f"security_kline_vol_ma20_{selected_code}")

                macd_cols = st.columns([1.0, 2.0])
                with macd_cols[0]:
                    show_macd = st.checkbox("显示MACD", value=True, key=f"security_kline_macd_{selected_code}")
                with macd_cols[1]:
                    if show_macd:
                        st.caption("MACD 参数：12,26,9（DIF/DEA）")
                volume_ma_windows = []
                if show_vol_ma5:
                    volume_ma_windows.append(5)
                if show_vol_ma10:
                    volume_ma_windows.append(10)
                if show_vol_ma20:
                    volume_ma_windows.append(20)
                if not volume_ma_windows:
                    volume_ma_windows = [5, 10]

                prefix = 'd' if kline_freq == '日线' else ('w' if kline_freq == '周线' else 'm')
                date_filtered_kline = kline_df[
                    (kline_df['trade_date'].dt.date >= date_range[0]) &
                    (kline_df['trade_date'].dt.date <= date_range[1])
                ].copy()
                if date_filtered_kline.empty:
                    date_filtered_kline = kline_df.copy()

                date_filtered_kline = date_filtered_kline.sort_values('trade_date').tail(int(kline_bars))
                enable_intraday_click = kline_freq == "日线"
                trade_date_candidates = (
                    date_filtered_kline["trade_date"].dt.strftime("%Y-%m-%d").tolist()
                    if enable_intraday_click else []
                )
                selected_intraday_key = f"security_intraday_selected_date_{selected_code}" if enable_intraday_click else ""
                clicked_intraday_date = ""
                click_signature_key = f"security_intraday_click_signature_{selected_code}" if enable_intraday_click else ""
                current_selected_trade_date = str(st.session_state.get(selected_intraday_key, "")).strip() if enable_intraday_click else ""
                if current_selected_trade_date not in trade_date_candidates and trade_date_candidates:
                    current_selected_trade_date = trade_date_candidates[-1]
                kline_chart = create_security_kline_chart(
                    date_filtered_kline,
                    prefix=prefix,
                    title=f"{title_name} — {kline_freq}K线",
                    ma_windows=ma_windows,
                    volume_ma_windows=volume_ma_windows,
                    show_macd=show_macd,
                    enable_select_points=enable_intraday_click,
                    selected_trade_date=current_selected_trade_date,
                )
                if kline_chart is not None:
                    if enable_intraday_click:
                        st.caption("💡 直接点击某根日K蜡烛，可按需拉取该交易日 1 分钟分时图；下载后会自动写入 PostgreSQL 缓存。")
                        chart_key = f"security_kline_chart_{selected_code}"
                        if plotly_events is not None:
                            click_points = plotly_events(
                                kline_chart,
                                click_event=True,
                                select_event=False,
                                hover_event=False,
                                override_height=620,
                                override_width="100%",
                                key=chart_key,
                            )
                            click_signature = json.dumps(click_points or [], ensure_ascii=False, sort_keys=True, default=str)
                            previous_signature = str(st.session_state.get(click_signature_key, "") or "")
                            selected_trade_date = ""
                            if click_points and click_signature != previous_signature:
                                st.session_state[click_signature_key] = click_signature
                                selected_trade_date = extract_trade_date_from_plotly_event(
                                    click_points,
                                    fallback_dates=trade_date_candidates,
                                )
                        else:
                            kline_event = st.plotly_chart(
                                kline_chart,
                                use_container_width=True,
                                key=chart_key,
                                on_select="rerun",
                                selection_mode=["points"],
                                config={"scrollZoom": False, "displayModeBar": False, "staticPlot": False},
                            )
                            selected_trade_date = extract_trade_date_from_plotly_event(
                                kline_event,
                                fallback_dates=trade_date_candidates,
                            )
                            if not selected_trade_date:
                                selected_trade_date = extract_trade_date_from_plotly_event(
                                    st.session_state.get(chart_key),
                                    fallback_dates=trade_date_candidates,
                                )
                        if selected_trade_date:
                            clicked_intraday_date = selected_trade_date
                            st.session_state[selected_intraday_key] = selected_trade_date
                    else:
                        st.plotly_chart(kline_chart, use_container_width=True)
                else:
                    st.info(f"当前时间范围内暂无{kline_freq}K线数据")

                if enable_intraday_click:
                    st.markdown("#### ⏱️ 当日分时")
                    if trade_date_candidates:
                        latest_trade_date = trade_date_candidates[-1]
                        current_selected_trade_date = str(st.session_state.get(selected_intraday_key, "")).strip()
                        if current_selected_trade_date not in trade_date_candidates:
                            st.session_state[selected_intraday_key] = latest_trade_date
                        st.caption("可点击上方日K自动切换；如果浏览器里点击不稳定，也可以直接在这里选交易日。")
                        selected_intraday_date = st.selectbox(
                            "选择要查看分时的交易日",
                            options=list(reversed(trade_date_candidates)),
                            key=selected_intraday_key,
                            help="上方日K点击成功后，这里会自动同步到对应交易日。",
                        )
                        if selected_intraday_date:
                            st.caption(f"当前已选交易日：{selected_intraday_date}")
                    else:
                        selected_intraday_date = ""
                        st.info("当前时间范围内暂无可选交易日，暂时无法展示分时图。")

                    if not selected_intraday_date:
                        st.info("请先选择一个交易日，再加载对应交易日的分时图。")
                    else:
                        effective_intraday_date = clicked_intraday_date or selected_intraday_date
                        intraday_trigger = "点击日K" if clicked_intraday_date else "下拉选择"
                        intraday_df, intraday_source, intraday_error = load_security_intraday_timeseries(
                            ts_code=selected_code,
                            trade_date=effective_intraday_date,
                            freq="1min",
                        )
                        source_label_map = {
                            "db": "数据库缓存",
                            "db:mootdx.minutes": "数据库缓存（原始来源：mootdx）",
                            "db:tushare.stk_mins": "数据库缓存（原始来源：Tushare）",
                            "mootdx": "mootdx 实时拉取并已入库",
                            "tushare": "Tushare 拉取并已入库",
                            "fallback-empty": "mootdx/Tushare 均无返回数据",
                            "error": "加载失败",
                        }
                        source_label = source_label_map.get(intraday_source, intraday_source or "未知")
                        st.caption(f"交易日：{effective_intraday_date} ｜ 触发方式：{intraday_trigger} ｜ 数据来源：{source_label}")
                        if intraday_error:
                            st.warning(f"分时数据加载失败：{intraday_error}")
                        elif intraday_df is None or intraday_df.empty:
                            st.info("该交易日暂无可展示的分时数据。")
                        else:
                            intraday_reference_close = None
                            selected_intraday_ts = pd.to_datetime(effective_intraday_date, errors="coerce")
                            if kline_df is not None and not kline_df.empty and not pd.isna(selected_intraday_ts):
                                previous_close_series = pd.to_numeric(
                                    kline_df.loc[kline_df["trade_date"] < selected_intraday_ts, "close"],
                                    errors="coerce",
                                ).dropna()
                                if not previous_close_series.empty:
                                    intraday_reference_close = float(previous_close_series.iloc[-1])

                            intraday_chart = create_security_intraday_chart(
                                intraday_df,
                                title=f"{title_name} — {effective_intraday_date} 分时图",
                                reference_close=intraday_reference_close,
                            )
                            if intraday_chart is not None:
                                st.plotly_chart(intraday_chart, use_container_width=True)
                            metric_intraday_cols = st.columns(4)
                            metric_intraday_cols[0].metric("分钟数", f"{len(intraday_df):,}")
                            metric_intraday_cols[1].metric("日内开盘", format_optional_number(intraday_df['open'].iloc[0]))
                            metric_intraday_cols[2].metric("日内收盘", format_optional_number(intraday_df['close'].iloc[-1]))
                            metric_intraday_cols[3].metric("日内振幅", format_optional_number(intraday_df['high'].max() - intraday_df['low'].min()))

        with tab_valuation:
            st.caption("展示静态市盈率、动态市盈率与股息率曲线")
            valuation_metrics = [
                ('静态市盈率曲线', 'pe', '静态市盈率PE', 1.0, 2, THEME_NAVY),
                ('动态市盈率曲线', 'pe_ttm', '动态市盈率PE_TTM', 1.0, 2, THEME_PURPLE),
                ('股息率曲线', 'dv_ratio', '股息率(%)', 1.0, 2, THEME_WARN),
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
                ('营业总收入柱状图', 'total_revenue', '营业总收入(亿元)', 100000000.0, 2, THEME_NAVY, "#4F6785"),
                ('净利润柱状图', 'net_profit', '净利润(亿元)', 100000000.0, 2, THEME_PURPLE, THEME_DOWN),
                ('扣非净利润柱状图', 'profit_dedt', '扣非净利润(亿元)', 100000000.0, 2, THEME_WARN, THEME_DOWN),
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
                ('总市值曲线', 'total_mv', '总市值(亿元)', 10000.0, 2, THEME_UP),
                ('流通市值曲线', 'circ_mv', '流通市值(亿元)', 10000.0, 2, "#C28C4E"),
                ('总股本曲线', 'total_share', '总股本(亿股)', 10000.0, 2, "#6FA3B8"),
                ('流通股本曲线', 'float_share', '流通股本(亿股)', 10000.0, 2, THEME_DOWN),
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
                ('静态市盈率曲线', 'pe', '静态市盈率PE', 1.0, 2, THEME_NAVY),
                ('动态市盈率曲线', 'pe_ttm', '动态市盈率PE_TTM', 1.0, 2, THEME_PURPLE),
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
                ('当日总市值曲线', 'total_mv', '总市值(亿元)', 10000.0, 2, THEME_UP),
                ('当日流通市值曲线', 'float_mv', '流通市值(亿元)', 10000.0, 2, "#C28C4E"),
                ('当日总股本曲线', 'total_share', '总股本(亿股)', 10000.0, 2, "#6FA3B8"),
                ('当日流通股本曲线', 'float_share', '流通股本(亿股)', 10000.0, 2, THEME_DOWN),
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
                color="#5B8E7D"
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
        THEME_NAVY, THEME_PURPLE, "#C28C4E", "#5B8E7D", "#B86A84",
        THEME_PRIMARY, "#6FA3B8", THEME_UP, "#8AA05A", "#4F6785",
        THEME_DOWN, THEME_WARN
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
            font=dict(size=20, weight=700, color=THEME_TEXT),
            x=0.02
        ),
        xaxis_title='日期',
        yaxis_title=metric_title,
        hovermode='x unified',
        height=520,
        template='plotly_white',
        plot_bgcolor=CHART_BG,
        paper_bgcolor=CHART_PAPER_BG,
        font=dict(family='Inter, PingFang SC, sans-serif'),
        legend=dict(
            orientation='h', yanchor='bottom', y=-0.28,
            xanchor='center', x=0.5,
            bgcolor='rgba(255,255,255,0)', font=dict(size=11)
        ),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    apply_time_series_hover_affordance(fig, chart_df['trade_date'], chart_df[metric_col])
    fig.update_xaxes(
        showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR,
        showline=True, linewidth=1, linecolor=CHART_AXIS_COLOR
    )
    fig.update_yaxes(
        showgrid=True, gridwidth=1, gridcolor=CHART_GRID_COLOR,
        showline=True, linewidth=1, linecolor=CHART_AXIS_COLOR,
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


def render_etf_deposit_tab():
    st.subheader("🏦 本外币存款")
    st.caption("展示本外币存款月度余额与增量变化，支持手工录入与 Excel 批量导入。")

    for state_key, default_value in (
        ("deposit_manual_open", False),
        ("deposit_import_open", False),
        ("deposit_edit_month", ""),
        ("deposit_history_limit", "最近12个月"),
        ("deposit_detail_window", "最近12个月"),
        ("deposit_overwrite_mode", "跳过已存在月份"),
    ):
        if state_key not in st.session_state:
            st.session_state[state_key] = default_value

    try:
        engine = get_deposit_engine()
        df = load_deposit_monthly_df(engine)
    except Exception as exc:
        st.error(f"加载本外币存款数据失败: {exc}")
        st.info("请确认 PostgreSQL 连接配置可用后重试。")
        return

    action_col, status_col = st.columns([1, 3])
    with action_col:
        if st.button("新增月份", key="deposit_add_month"):
            if has_deposit_edit_permission():
                st.session_state["deposit_manual_open"] = True
                st.session_state["deposit_import_open"] = False
                st.session_state["deposit_edit_month"] = ""
            else:
                st.warning("请先完成编辑权限验证，再新增月份。")
        if st.button("批量导入", key="deposit_import_file"):
            if has_deposit_edit_permission():
                st.session_state["deposit_import_open"] = True
                st.session_state["deposit_manual_open"] = False
            else:
                st.warning("请先完成编辑权限验证，再批量导入。")

    with status_col:
        if df.empty:
            st.caption("最新数据月份：- | 数据来源：- | 最近更新时间：-")
        else:
            latest_row = df.sort_values("month").iloc[-1]
            latest_month = pd.to_datetime(latest_row["month"]).strftime("%Y-%m")
            source_type = latest_row.get("source_type") or "-"
            updated_at_raw = latest_row.get("updated_at")
            updated_at = (
                pd.to_datetime(updated_at_raw).strftime("%Y-%m-%d %H:%M")
                if pd.notna(updated_at_raw)
                else "-"
            )
            st.caption(
                f"最新数据月份：{latest_month} | 数据来源：{source_type} | 最近更新时间：{updated_at}"
            )

    permission_cols = st.columns([4, 1.2])
    if has_deposit_edit_permission():
        permission_cols[0].success("当前会话已获得本外币存款编辑权限。")
        if permission_cols[1].button("退出权限", key="revoke_deposit_edit_permission"):
            clear_deposit_edit_permission()
            st.rerun()
    else:
        configured_password = get_deposit_edit_password()
        if not configured_password:
            st.warning("当前未配置本外币存款编辑权限密码，新增/导入/删除功能已禁用。请设置 ETF_DEPOSIT_EDIT_PASSWORD 或 ETF_EDIT_PASSWORD 后重启应用。")
        else:
            access_password = permission_cols[0].text_input(
                "本外币存款编辑权限密码",
                type="password",
                key="deposit_edit_password_input",
            )
            if permission_cols[1].button("权限验证", key="grant_deposit_edit_permission"):
                if grant_deposit_edit_permission(access_password):
                    st.success("权限验证成功，现在可以新增、导入、删除本外币存款数据。")
                    st.rerun()
                st.error("权限验证失败，请检查密码。")
            st.info("仅通过权限验证的会话可以新增、导入或删除本外币存款数据。")

    if df.empty:
        st.info("暂无本外币存款数据，请先新增月份或批量导入。")
    else:
        summary = build_deposit_summary(df)
        metric_cols = st.columns(4)
        metric_cols[0].metric("最新月份", summary["latest_month"] or "-")
        metric_cols[1].metric(
            "本外币存款余额",
            f'{summary["latest_value"]:.2f}' if summary["latest_value"] is not None else "-",
        )
        metric_cols[2].metric(
            "环比变动",
            f'{summary["mom_delta"]:.2f}' if summary["mom_delta"] is not None else "-",
        )
        metric_cols[3].metric(
            "同比变动",
            f'{summary["yoy_delta"]:.2f}' if summary["yoy_delta"] is not None else "-",
        )

        trend_df = build_balance_trend_df(df)
        change_trend_df = build_change_trend_df(df)
        window = st.radio(
            "时间范围",
            ["最近12个月", "最近24个月", "全部"],
            horizontal=True,
            key="deposit_history_limit",
        )
        if not trend_df.empty and window != "全部":
            cutoff = trend_df["month"].max() - pd.DateOffset(
                months=11 if window == "最近12个月" else 23
            )
            trend_df = trend_df[trend_df["month"] >= cutoff]
            change_trend_df = change_trend_df[change_trend_df["month"] >= cutoff]

        st.plotly_chart(
            px.line(
                trend_df,
                x="month",
                y="value",
                color="metric",
                markers=True,
                title="余额趋势",
            ),
            use_container_width=True,
        )
        st.plotly_chart(
            px.line(
                change_trend_df,
                x="month",
                y="value",
                color="metric",
                markers=True,
                title="同比 / 环比变动额趋势",
            ),
            use_container_width=True,
        )

        increment_cols = [
            "household_deposit_increase",
            "corp_deposit_increase",
            "fiscal_deposit_increase",
            "nonbank_deposit_increase",
            "total_deposit_increase",
        ]
        increment_label_map = {
            "household_deposit_increase": "住户存款增加额",
            "corp_deposit_increase": "非金融企业存款增加额",
            "fiscal_deposit_increase": "财政性存款增加额",
            "nonbank_deposit_increase": "非银行业金融机构存款增加额",
            "total_deposit_increase": "存款合计增加额",
        }
        increment_df = (
            df.copy()
            .assign(month=pd.to_datetime(df["month"]))
            .melt(id_vars=["month"], value_vars=increment_cols, var_name="metric", value_name="value")
        )
        increment_df["metric"] = increment_df["metric"].map(increment_label_map)
        st.plotly_chart(
            px.bar(
                increment_df,
                x="month",
                y="value",
                color="metric",
                barmode="group",
                title="增量结构",
            ),
            use_container_width=True,
        )

        detail_df = df.sort_values("month", ascending=False).copy()
        history_choice = st.radio(
            "明细范围",
            ["最近12个月", "全部历史"],
            horizontal=True,
            key="deposit_detail_window",
        )
        if history_choice == "最近12个月":
            detail_df = detail_df.head(12)

        edit_options = detail_df["month"].apply(lambda x: pd.to_datetime(x).strftime("%Y-%m-%d")).tolist()
        edit_month = st.selectbox(
            "编辑已有月份",
            options=[""] + edit_options,
            index=0,
            key="deposit_edit_select",
        )
        can_edit_deposit = has_deposit_edit_permission()
        if st.button("编辑选中月份", key="deposit_edit_button"):
            if not can_edit_deposit:
                st.warning("请先完成编辑权限验证，再编辑已有月份。")
            elif not edit_month:
                st.info("请先选择要编辑的月份。")
            else:
                st.session_state["deposit_manual_open"] = True
                st.session_state["deposit_import_open"] = False
                st.session_state["deposit_edit_month"] = edit_month

        if not can_edit_deposit:
            st.caption("要编辑或删除已有月份，请先完成权限验证。")
        elif not edit_month:
            st.caption("已完成权限验证：请选择一个月份后再点“编辑选中月份”。")

        display_df = to_deposit_display_df(detail_df)
        if not display_df.empty:
            display_df.insert(0, "删除", False)
        editor_df = st.data_editor(
            display_df,
            use_container_width=True,
            hide_index=True,
            disabled=[column for column in display_df.columns if column != "删除"],
            column_config={
                "删除": st.column_config.CheckboxColumn("删除", help="勾选后可删除对应月份"),
            },
            key="deposit_detail_editor",
        )
        selected_delete_months = editor_df.loc[editor_df["删除"], "月份"].tolist() if not editor_df.empty else []

        if can_edit_deposit and not selected_delete_months:
            st.caption("已完成权限验证：勾选表格左侧“删除”后即可删除对应月份。")

        if st.button("删除选中月份", key="deposit_delete_selected"):
            if not can_edit_deposit:
                st.warning("请先完成编辑权限验证，再删除已有月份。")
            elif not selected_delete_months:
                st.info("请先勾选要删除的月份。")
            else:
                try:
                    deleted_count = delete_deposit_months(engine, selected_delete_months)
                except Exception as exc:
                    st.error(f"删除失败: {exc}")
                else:
                    st.session_state["deposit_manual_open"] = False
                    st.session_state["deposit_import_open"] = False
                    st.session_state["deposit_edit_month"] = ""
                    st.success(f"已删除 {deleted_count} 个月份")
                    st.rerun()

    if st.session_state.get("deposit_manual_open", False):
        if not has_deposit_edit_permission():
            st.warning("当前会话没有本外币存款编辑权限，请先完成权限验证。")
            st.session_state["deposit_manual_open"] = False
            st.session_state["deposit_edit_month"] = ""
        else:
            edit_row = None
            if st.session_state.get("deposit_edit_month") and not df.empty:
                mask = pd.to_datetime(df["month"]).dt.strftime("%Y-%m-%d") == st.session_state["deposit_edit_month"]
                if mask.any():
                    edit_row = df.loc[mask].iloc[-1].to_dict()

            with st.form("deposit_manual_form", clear_on_submit=False):
                month = st.text_input(
                    "月份",
                    value=pd.to_datetime(edit_row["month"]).strftime("%Y-%m") if edit_row else "",
                )
                rmb = st.number_input(
                    "人民币存款余额",
                    value=float(edit_row["rmb_deposit_balance"]) if edit_row else 0.0,
                    format="%.4f",
                )
                fx = st.number_input(
                    "外币存款余额",
                    value=float(edit_row["fx_deposit_balance"]) if edit_row else 0.0,
                    format="%.4f",
                )
                total = st.number_input(
                    "本外币存款余额",
                    value=float(edit_row["total_deposit_balance"]) if edit_row else 0.0,
                    format="%.4f",
                )
                household = st.number_input(
                    "住户存款增加额",
                    value=float(edit_row["household_deposit_increase"]) if edit_row else 0.0,
                    format="%.4f",
                )
                corp = st.number_input(
                    "非金融企业存款增加额",
                    value=float(edit_row["corp_deposit_increase"]) if edit_row else 0.0,
                    format="%.4f",
                )
                fiscal = st.number_input(
                    "财政性存款增加额",
                    value=float(edit_row["fiscal_deposit_increase"]) if edit_row else 0.0,
                    format="%.4f",
                )
                nonbank = st.number_input(
                    "非银行业金融机构存款增加额",
                    value=float(edit_row["nonbank_deposit_increase"]) if edit_row else 0.0,
                    format="%.4f",
                )
                total_increase = st.number_input(
                    "存款合计增加额",
                    value=float(edit_row["total_deposit_increase"]) if edit_row else 0.0,
                    format="%.4f",
                )
                household_loan = st.number_input(
                    "居民长期贷款增加额",
                    value=float(edit_row["household_long_loan_increase"]) if edit_row else 0.0,
                    format="%.4f",
                )
                submitted = st.form_submit_button("保存本月数据")
                canceled = st.form_submit_button("取消")

            if canceled:
                st.session_state["deposit_manual_open"] = False
                st.session_state["deposit_edit_month"] = ""
                st.rerun()

            if submitted:
                try:
                    rows = build_upsert_rows(
                        [
                            {
                                "month": month,
                                "rmb_deposit_balance": rmb,
                                "fx_deposit_balance": fx,
                                "total_deposit_balance": total,
                                "household_deposit_increase": household,
                                "corp_deposit_increase": corp,
                                "fiscal_deposit_increase": fiscal,
                                "nonbank_deposit_increase": nonbank,
                                "total_deposit_increase": total_increase,
                                "household_long_loan_increase": household_loan,
                            }
                        ],
                        source_type="manual",
                        source_file=None,
                    )
                    upsert_deposit_rows(engine, rows)
                except Exception as exc:
                    st.error(f"保存失败: {exc}")
                else:
                    st.session_state["deposit_manual_open"] = False
                    st.session_state["deposit_edit_month"] = ""
                    st.success("保存成功")
                    st.rerun()

    if st.session_state.get("deposit_import_open", False):
        if not has_deposit_edit_permission():
            st.warning("当前会话没有本外币存款编辑权限，请先完成权限验证。")
            st.session_state["deposit_import_open"] = False
        else:
            upload = st.file_uploader(
                "上传本外币存款 Excel",
                type=["xlsx"],
                key="deposit_uploader",
            )
            if upload is not None:
                try:
                    imported_df = parse_deposit_workbook(upload)
                    preview = classify_import_rows(imported_df, df)
                except Exception as exc:
                    st.error(f"导入预览失败: {exc}")
                else:
                    st.radio(
                        "重复月份处理",
                        ["跳过已存在月份", "覆盖已存在月份"],
                        horizontal=True,
                        key="deposit_overwrite_mode",
                    )
                    st.write("新增月份")
                    st.dataframe(
                        to_deposit_display_df(preview["to_insert"]),
                        use_container_width=True,
                        hide_index=True,
                    )
                    st.write("覆盖月份")
                    st.dataframe(
                        to_deposit_display_df(preview["to_overwrite"]),
                        use_container_width=True,
                        hide_index=True,
                    )
                    if st.button("确认写入", key="deposit_confirm_import"):
                        write_df = imported_df.copy()
                        if st.session_state["deposit_overwrite_mode"] == "跳过已存在月份":
                            write_df = preview["to_insert"].copy()
                        if write_df.empty:
                            st.warning("没有需要写入的月份。")
                        else:
                            try:
                                rows = build_upsert_rows(
                                    write_df.to_dict(orient="records"),
                                    source_type="import",
                                    source_file=getattr(upload, "name", None),
                                )
                                upsert_deposit_rows(engine, rows)
                            except Exception as exc:
                                st.error(f"导入写入失败: {exc}")
                            else:
                                st.session_state["deposit_import_open"] = False
                                st.success(f"已写入 {len(rows)} 个月份")
                                st.rerun()

INDEX_MONITOR_DEFAULT_NAMES = [
    "上证指数",
    "深证成指",
    "沪深300指数",
    "上证50指数",
    "中证500指数",
    "中证1000指数",
    "中证2000指数",
    "中证全指",
    "中小100指数",
]

INDEX_MONITOR_FIELD_LABELS = {
    "monthly_change_pct": "当月涨幅",
    "open_price": "开盘价格",
    "close_price": "收盘价格",
    "low_price": "最低点",
    "high_price": "最高点",
    "static_pe": "期末静态市盈率",
    "dynamic_pe": "期末动态市盈率",
    "mom_change_pct": "环比涨幅变化",
    "mom_open_price": "环比开盘价格变化",
    "mom_close_price": "环比收盘价格变化",
    "mom_low_price": "环比最低点变化",
    "mom_high_price": "环比最高点变化",
    "mom_static_pe": "环比静态市盈率变化",
    "mom_dynamic_pe": "环比动态市盈率变化",
    "mom_static_pe_change_rate": "静态市盈率变化率",
    "mom_dynamic_pe_change_rate": "动态市盈率变化率",
    "yoy_change_pct": "同比涨幅变化",
    "yoy_open_price": "同比开盘价格变化",
    "yoy_close_price": "同比收盘价格变化",
    "yoy_low_price": "同比最低点变化",
    "yoy_high_price": "同比最高点变化",
    "yoy_static_pe": "同比静态市盈率变化",
    "yoy_dynamic_pe": "同比动态市盈率变化",
}


def _to_optional_float(value):
    if value is None or (isinstance(value, float) and pd.isna(value)) or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_index_batch_editor_df(existing_df: pd.DataFrame, month_text: str) -> pd.DataFrame:
    columns = ["index_name"] + list(INDEX_MONITOR_FIELD_LABELS.keys())
    base_names = list(dict.fromkeys(INDEX_MONITOR_DEFAULT_NAMES))

    if existing_df is not None and not existing_df.empty:
        base_names = list(dict.fromkeys(base_names + existing_df["index_name"].dropna().astype(str).tolist()))

    if month_text and existing_df is not None and not existing_df.empty:
        month_mask = pd.to_datetime(existing_df["month"]).dt.strftime("%Y-%m") == month_text
        month_df = existing_df.loc[month_mask].copy()
        if not month_df.empty:
            for column in columns:
                if column not in month_df.columns:
                    month_df[column] = None
            return month_df[columns].reset_index(drop=True)

    return pd.DataFrame([{"index_name": name} for name in base_names], columns=columns)


def _collect_index_batch_rows(editor_df: pd.DataFrame, month_text: str) -> list[dict]:
    payload_rows = []
    for row in editor_df.to_dict(orient="records"):
        index_name = str(row.get("index_name") or "").strip()
        if not index_name:
            continue
        payload = {"month": month_text, "index_name": index_name}
        has_value = False
        for field in INDEX_MONITOR_FIELD_LABELS:
            numeric_value = _to_optional_float(row.get(field))
            payload[field] = numeric_value
            if numeric_value is not None:
                has_value = True
        if has_value:
            payload_rows.append(payload)
    return payload_rows


def render_index_monitor_tab():
    st.subheader("📊 指数监测")
    st.caption("展示股票指数月度表现、估值趋势与同比环比变化，支持手工录入与 Excel 批量导入。")

    for state_key, default_value in (
        ("index_manual_month_open", False),
        ("index_single_edit_open", False),
        ("index_import_open", False),
        ("index_history_limit", "最近12个月"),
        ("index_overwrite_mode", "跳过已存在记录"),
    ):
        if state_key not in st.session_state:
            st.session_state[state_key] = default_value

    try:
        engine = get_index_monitor_engine()
        df = load_index_monitor_df(engine)
    except Exception as exc:
        st.error(f"加载指数监测数据失败: {exc}")
        st.info("请确认 PostgreSQL 连接配置可用后重试。")
        return

    action_col, status_col = st.columns([1, 3])
    with action_col:
        if st.button("新增月份", key="index_add_month"):
            if has_index_monitor_edit_permission():
                st.session_state["index_manual_month_open"] = True
                st.session_state["index_single_edit_open"] = False
                st.session_state["index_import_open"] = False
            else:
                st.warning("请先完成编辑权限验证，再新增月份。")
        if st.button("单指数补录/修改", key="index_edit_single"):
            if has_index_monitor_edit_permission():
                st.session_state["index_single_edit_open"] = True
                st.session_state["index_manual_month_open"] = False
                st.session_state["index_import_open"] = False
            else:
                st.warning("请先完成编辑权限验证，再单指数补录或修改。")
        if st.button("批量导入 Excel", key="index_import_file"):
            if has_index_monitor_edit_permission():
                st.session_state["index_import_open"] = True
                st.session_state["index_manual_month_open"] = False
                st.session_state["index_single_edit_open"] = False
            else:
                st.warning("请先完成编辑权限验证，再批量导入。")

    with status_col:
        if df.empty:
            st.caption("最新数据月份：- | 记录数：0 | 最近更新时间：-")
        else:
            latest_month = pd.to_datetime(df["month"]).max().strftime("%Y-%m")
            updated_at = pd.to_datetime(df["updated_at"]).max().strftime("%Y-%m-%d %H:%M")
            st.caption(f"最新数据月份：{latest_month} | 记录数：{len(df)} | 最近更新时间：{updated_at}")

    permission_cols = st.columns([4, 1.2])
    if has_index_monitor_edit_permission():
        permission_cols[0].success("当前会话已获得指数监测编辑权限。")
        if permission_cols[1].button("退出权限", key="revoke_index_monitor_edit_permission"):
            clear_index_monitor_edit_permission()
            st.rerun()
    else:
        configured_password = get_index_monitor_edit_password()
        if not configured_password:
            st.warning("当前未配置指数监测编辑权限密码，新增/补录/导入功能已禁用。请设置 ETF_INDEX_MONITOR_EDIT_PASSWORD 或 ETF_EDIT_PASSWORD 后重启应用。")
        else:
            access_password = permission_cols[0].text_input(
                "指数监测编辑权限密码",
                type="password",
                key="index_monitor_edit_password_input",
            )
            if permission_cols[1].button("权限验证", key="grant_index_monitor_edit_permission"):
                if grant_index_monitor_edit_permission(access_password):
                    st.success("权限验证成功，现在可以新增、补录或导入指数监测数据。")
                    st.rerun()
                st.error("权限验证失败，请检查密码。")
            st.info("仅通过权限验证的会话可以新增、补录或批量导入指数监测数据。")

    if df.empty:
        st.info("暂无指数监测数据，请先新增月份或批量导入。")
        all_index_names = INDEX_MONITOR_DEFAULT_NAMES.copy()
        month_options = []
        selected_snapshot = ""
        filtered_df = pd.DataFrame()
    else:
        all_index_names = sorted(df["index_name"].dropna().astype(str).unique().tolist())
        month_options = sorted(pd.to_datetime(df["month"]).dt.strftime("%Y-%m").unique().tolist(), reverse=True)
        selected_snapshot = st.selectbox("月份", options=month_options, index=0, key="index_snapshot_month")
        selected_indices = st.multiselect(
            "指数筛选",
            options=all_index_names,
            default=all_index_names[: min(4, len(all_index_names))],
            key="index_monitor_names",
        )
        filtered_df = df[df["index_name"].isin(selected_indices)].copy() if selected_indices else df.copy()
        filtered_df["month"] = pd.to_datetime(filtered_df["month"])

        summary_df = filtered_df[
            filtered_df["month"].dt.strftime("%Y-%m") == selected_snapshot
        ].copy()
        summary = build_index_monitor_summary(summary_df if not summary_df.empty else filtered_df)
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("最新月份", summary["latest_month"] or "-")
        c2.metric(
            "指数平均涨幅",
            f'{summary["avg_change_pct"]:.2f}%' if summary["avg_change_pct"] is not None else "-",
        )
        c3.metric("当月最强指数", summary["strongest_index"] or "-")
        c4.metric("当月最弱指数", summary["weakest_index"] or "-")
        c5.metric("平均静态PE", f'{summary["avg_static_pe"]:.2f}' if summary["avg_static_pe"] is not None else "-")
        c6.metric("平均动态PE", f'{summary["avg_dynamic_pe"]:.2f}' if summary["avg_dynamic_pe"] is not None else "-")

        window = st.radio(
            "时间范围",
            ["最近12个月", "最近24个月", "全部"],
            horizontal=True,
            key="index_history_limit",
        )
        if window != "全部" and not filtered_df.empty:
            cutoff = filtered_df["month"].max() - pd.DateOffset(months=11 if window == "最近12个月" else 23)
            filtered_df = filtered_df[filtered_df["month"] >= cutoff]

        price_trend_df = build_price_trend_df(filtered_df, value_field="close_price")
        st.plotly_chart(
            px.line(
                price_trend_df,
                x="month",
                y="value",
                color="index_name",
                markers=True,
                title="价格趋势（收盘价）",
            ),
            use_container_width=True,
        )

        valuation_trend_df = build_valuation_trend_df(filtered_df)
        valuation_trend_df["series"] = valuation_trend_df["index_name"] + " / " + valuation_trend_df["metric"]
        st.plotly_chart(
            px.line(
                valuation_trend_df,
                x="month",
                y="value",
                color="series",
                markers=True,
                title="估值趋势",
            ),
            use_container_width=True,
        )

        change_metric_options = list(CHANGE_TREND_FIELD_LABELS.items())
        change_metric_key = st.selectbox(
            "同比 / 环比曲线指标",
            options=[item[0] for item in change_metric_options],
            format_func=lambda value: CHANGE_TREND_FIELD_LABELS.get(value, value),
            index=0,
            key="index_change_metric_key",
        )
        change_trend_df = build_index_change_trend_df(filtered_df, metric_key=change_metric_key)
        if change_trend_df.empty:
            st.info("当前筛选范围内暂无可展示的同比 / 环比曲线。")
        else:
            st.plotly_chart(
                px.line(
                    change_trend_df,
                    x="month",
                    y="value",
                    color="index_name",
                    line_dash="change_type",
                    markers=True,
                    title=f'{CHANGE_TREND_FIELD_LABELS[change_metric_key]}同比 / 环比曲线',
                ),
                use_container_width=True,
            )

        snapshot_df = filtered_df[
            filtered_df["month"].dt.strftime("%Y-%m") == selected_snapshot
        ].copy()
        st.markdown("#### 最新月度快照")
        st.dataframe(
            to_index_monitor_display_df(snapshot_df.sort_values("index_name")),
            use_container_width=True,
            hide_index=True,
        )

    if st.session_state.get("index_manual_month_open", False):
        if not has_index_monitor_edit_permission():
            st.warning("当前会话没有指数监测编辑权限，请先完成权限验证。")
            st.session_state["index_manual_month_open"] = False
        else:
            st.markdown("#### 新增月份")
            manual_month = st.text_input("录入月份", value=month_options[0] if month_options else "", key="index_manual_month_value")
            editor_seed_df = _build_index_batch_editor_df(df, manual_month)
            editor_df = st.data_editor(
                editor_seed_df,
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
                key="index_month_editor",
            )
            save_col, cancel_col = st.columns(2)
            if save_col.button("保存本月指数数据", key="index_month_save"):
                if not manual_month:
                    st.error("请先填写录入月份")
                else:
                    rows = _collect_index_batch_rows(editor_df, manual_month)
                    if not rows:
                        st.warning("没有可写入的指数记录。")
                    else:
                        try:
                            payload_rows = build_index_upsert_rows(
                                rows,
                                source_type="manual",
                                source_file=None,
                            )
                            upsert_index_monitor_rows(engine, payload_rows)
                        except Exception as exc:
                            st.error(f"保存失败: {exc}")
                        else:
                            st.session_state["index_manual_month_open"] = False
                            st.success(f"已写入 {len(payload_rows)} 条指数记录")
                            st.rerun()
            if cancel_col.button("取消新增月份", key="index_month_cancel"):
                st.session_state["index_manual_month_open"] = False
                st.rerun()

    if st.session_state.get("index_single_edit_open", False):
        if not has_index_monitor_edit_permission():
            st.warning("当前会话没有指数监测编辑权限，请先完成权限验证。")
            st.session_state["index_single_edit_open"] = False
        else:
            st.markdown("#### 单指数补录 / 修改")
            if df.empty:
                st.info("当前暂无历史记录，请先新增月份或导入数据。")
            else:
                edit_month = st.selectbox("选择月份", options=month_options, key="index_edit_month")
                month_df = df[pd.to_datetime(df["month"]).dt.strftime("%Y-%m") == edit_month].copy()
                month_names = sorted(month_df["index_name"].dropna().astype(str).unique().tolist())
                edit_name = st.selectbox("选择指数", options=month_names, key="index_edit_name")
                edit_row = month_df[month_df["index_name"] == edit_name].iloc[0].to_dict() if edit_name else {}
                with st.form("index_single_edit_form", clear_on_submit=False):
                    edit_values = {}
                    for field, label in INDEX_MONITOR_FIELD_LABELS.items():
                        edit_values[field] = st.number_input(
                            label,
                            value=float(edit_row[field]) if edit_row.get(field) is not None and not pd.isna(edit_row.get(field)) else 0.0,
                            format="%.4f",
                        )
                    submitted = st.form_submit_button("保存单指数数据")
                    canceled = st.form_submit_button("取消")

                if canceled:
                    st.session_state["index_single_edit_open"] = False
                    st.rerun()

                if submitted:
                    try:
                        rows = build_index_upsert_rows(
                            [
                                {
                                    "month": edit_month,
                                    "index_name": edit_name,
                                    **edit_values,
                                }
                            ],
                            source_type="manual",
                            source_file=None,
                        )
                        upsert_index_monitor_rows(engine, rows)
                    except Exception as exc:
                        st.error(f"保存失败: {exc}")
                    else:
                        st.session_state["index_single_edit_open"] = False
                        st.success("保存成功")
                        st.rerun()

    if st.session_state.get("index_import_open", False):
        if not has_index_monitor_edit_permission():
            st.warning("当前会话没有指数监测编辑权限，请先完成权限验证。")
            st.session_state["index_import_open"] = False
        else:
            upload = st.file_uploader(
                "上传股票指数 Excel",
                type=["xlsx"],
                key="index_monitor_uploader",
            )
            if upload is not None:
                try:
                    imported_df = parse_index_monitor_workbook(upload)
                    preview = classify_index_import_rows(imported_df, df)
                except Exception as exc:
                    st.error(f"导入预览失败: {exc}")
                else:
                    st.radio(
                        "重复记录处理",
                        ["跳过已存在记录", "覆盖已存在记录"],
                        horizontal=True,
                        key="index_overwrite_mode",
                    )
                    st.write("新增记录")
                    st.dataframe(
                        to_index_monitor_display_df(preview["to_insert"]),
                        use_container_width=True,
                        hide_index=True,
                    )
                    st.write("覆盖记录")
                    st.dataframe(
                        to_index_monitor_display_df(preview["to_overwrite"]),
                        use_container_width=True,
                        hide_index=True,
                    )
                    if st.button("确认写入指数数据", key="index_confirm_import"):
                        write_df = imported_df.copy()
                        if st.session_state["index_overwrite_mode"] == "跳过已存在记录":
                            write_df = preview["to_insert"].copy()
                        if write_df.empty:
                            st.warning("没有需要写入的记录。")
                        else:
                            try:
                                rows = build_index_upsert_rows(
                                    write_df.to_dict(orient="records"),
                                    source_type="import",
                                    source_file=getattr(upload, "name", None),
                                )
                                upsert_index_monitor_rows(engine, rows)
                            except Exception as exc:
                                st.error(f"导入写入失败: {exc}")
                            else:
                                st.session_state["index_import_open"] = False
                                st.success(f"已写入 {len(rows)} 条指数记录")
                                st.rerun()

FUND_MONITOR_CATEGORY_CONFIG = {
    "合计": {"group": "public_fund", "level": "total", "sort_order": 10},
    "封闭式基金": {"group": "public_fund", "level": "primary", "sort_order": 20},
    "开放式基金": {"group": "public_fund", "level": "primary", "sort_order": 30},
    "其中：股票基金": {"group": "public_fund", "level": "subtype", "sort_order": 40},
    "其中：混合基金": {"group": "public_fund", "level": "subtype", "sort_order": 50},
    "其中：债券基金": {"group": "public_fund", "level": "subtype", "sort_order": 60},
    "其中：货币基金": {"group": "public_fund", "level": "subtype", "sort_order": 70},
    "其中：QDII基金": {"group": "public_fund", "level": "subtype", "sort_order": 80},
    "私募证券投资基金": {"group": "private_fund", "level": "primary", "sort_order": 90},
    "私募资管权益基金": {"group": "private_fund", "level": "primary", "sort_order": 100},
    "权益类理财产品": {"group": "wealth_mgmt", "level": "primary", "sort_order": 110},
}
FUND_MONITOR_DEFAULT_NAMES = list(FUND_MONITOR_CATEGORY_CONFIG.keys())
FUND_MONITOR_PUBLIC_STRUCTURE_NAMES = [
    "其中：股票基金",
    "其中：混合基金",
    "其中：债券基金",
    "其中：货币基金",
    "其中：QDII基金",
]
FUND_MONITOR_DEFAULT_SELECTED = [
    "合计",
    "其中：股票基金",
    "其中：混合基金",
    "私募证券投资基金",
    "私募资管权益基金",
]
FUND_MONITOR_METRIC_LABELS = {
    "nav_amount": "净值（亿元）",
    "share_amount": "份额（亿份）",
    "fund_count": "基金数量（只）",
    "unit_nav": "单位净值（元）",
}
FUND_MONITOR_FIELD_LABELS = {
    "fund_count": "基金数量（只）",
    "share_amount": "份额（亿份）",
    "nav_amount": "净值（亿元）",
    "unit_nav": "单位净值（元）",
    "mom_fund_count": "环比基金数量变动",
    "mom_share_amount": "环比份额变动",
    "mom_nav_amount": "环比净值变动",
    "mom_unit_nav": "环比单位净值变动",
    "yoy_fund_count": "同比基金数量变动",
    "yoy_share_amount": "同比份额变动",
    "yoy_nav_amount": "同比净值变动",
    "yoy_unit_nav": "同比单位净值变动",
}


def _sort_fund_categories(names: list[str]) -> list[str]:
    unique_names = list(dict.fromkeys(name for name in names if name))
    return sorted(
        unique_names,
        key=lambda name: (
            FUND_MONITOR_CATEGORY_CONFIG.get(name, {}).get("sort_order", 9999),
            name,
        ),
    )


def _enrich_fund_monitor_rows(rows: list[dict]) -> list[dict]:
    enriched_rows = []
    for row in rows:
        category_name = str(row.get("category_name") or "").strip()
        if not category_name:
            continue
        meta = FUND_MONITOR_CATEGORY_CONFIG.get(category_name, {})
        enriched_row = row.copy()
        enriched_row["category_name"] = category_name
        enriched_row["category_group"] = meta.get("group")
        enriched_row["category_level"] = meta.get("level")
        enriched_row["sort_order"] = meta.get("sort_order")
        enriched_rows.append(enriched_row)
    return enriched_rows


def _build_fund_monitor_batch_editor_df(existing_df: pd.DataFrame, month_text: str) -> pd.DataFrame:
    columns = ["category_name"] + list(FUND_MONITOR_FIELD_LABELS.keys())
    base_names = list(dict.fromkeys(FUND_MONITOR_DEFAULT_NAMES))

    if existing_df is not None and not existing_df.empty:
        base_names = _sort_fund_categories(base_names + existing_df["category_name"].dropna().astype(str).tolist())

    if month_text and existing_df is not None and not existing_df.empty:
        month_mask = pd.to_datetime(existing_df["month"]).dt.strftime("%Y-%m") == month_text
        month_df = existing_df.loc[month_mask].copy()
        if not month_df.empty:
            for column in columns:
                if column not in month_df.columns:
                    month_df[column] = None
            month_df["category_name"] = pd.Categorical(
                month_df["category_name"],
                categories=FUND_MONITOR_DEFAULT_NAMES,
                ordered=True,
            )
            month_df = month_df.sort_values("category_name")
            month_df["category_name"] = month_df["category_name"].astype(str)
            return month_df[columns].reset_index(drop=True)

    return pd.DataFrame([{"category_name": name} for name in base_names], columns=columns)


def _collect_fund_monitor_batch_rows(editor_df: pd.DataFrame, month_text: str) -> list[dict]:
    payload_rows = []
    for row in editor_df.to_dict(orient="records"):
        category_name = str(row.get("category_name") or "").strip()
        if not category_name:
            continue
        payload = {"month": month_text, "category_name": category_name}
        has_value = False
        for field in FUND_MONITOR_FIELD_LABELS:
            numeric_value = _to_optional_float(row.get(field))
            payload[field] = numeric_value
            if numeric_value is not None:
                has_value = True
        if has_value:
            payload_rows.append(payload)
    return _enrich_fund_monitor_rows(payload_rows)


def _open_fund_monitor_action_panel(active_key: str) -> None:
    if active_key == "fund_delete_month_open":
        st.session_state["fund_delete_month_open_version"] = (
            int(st.session_state.get("fund_delete_month_open_version", 0)) + 1
        )
    for panel_key in (
        "fund_manual_month_open",
        "fund_single_edit_open",
        "fund_import_open",
        "fund_delete_month_open",
    ):
        st.session_state[panel_key] = panel_key == active_key


def _close_fund_monitor_action_panels() -> None:
    for panel_key in (
        "fund_manual_month_open",
        "fund_single_edit_open",
        "fund_import_open",
        "fund_delete_month_open",
    ):
        st.session_state[panel_key] = False


def _render_fund_monitor_action_panels(engine, df: pd.DataFrame, month_options: list[str]) -> None:
    if st.session_state.get("fund_manual_month_open", False):
        st.markdown("#### 新增 / 编辑整月数据")
        manual_month = st.text_input("录入月份", value=month_options[0] if month_options else "", key="fund_manual_month_value")
        editor_seed_df = _build_fund_monitor_batch_editor_df(df, manual_month)
        editor_df = st.data_editor(
            editor_seed_df,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            key="fund_month_editor",
        )
        save_col, cancel_col = st.columns(2)
        if save_col.button("保存本月基金监测数据", key="fund_month_save"):
            if not manual_month:
                st.error("请先填写录入月份")
            else:
                rows = _collect_fund_monitor_batch_rows(editor_df, manual_month)
                if not rows:
                    st.warning("没有可写入的分类记录。")
                else:
                    try:
                        payload_rows = build_fund_monitor_upsert_rows(rows, source_type="manual", source_file=None)
                        upsert_fund_monitor_rows(engine, payload_rows)
                    except Exception as exc:
                        st.error(f"保存失败: {exc}")
                    else:
                        _close_fund_monitor_action_panels()
                        st.success(f"已写入 {len(payload_rows)} 条基金监测记录")
                        st.rerun()
        if cancel_col.button("取消", key="fund_month_cancel"):
            _close_fund_monitor_action_panels()
            st.rerun()

    if st.session_state.get("fund_single_edit_open", False):
        st.markdown("#### 单分类补录 / 修改")
        if df.empty:
            st.info("当前暂无历史记录，请先新增月份或导入数据。")
        else:
            edit_month = st.selectbox("选择月份", options=month_options, key="fund_edit_month")
            month_df = df[pd.to_datetime(df["month"]).dt.strftime("%Y-%m") == edit_month].copy()
            month_names = _sort_fund_categories(month_df["category_name"].dropna().astype(str).unique().tolist())
            edit_name = st.selectbox("选择分类", options=month_names, key="fund_edit_name")
            edit_row = month_df[month_df["category_name"] == edit_name].iloc[0].to_dict() if edit_name else {}
            with st.form("fund_single_edit_form", clear_on_submit=False):
                edit_values = {}
                for field, label in FUND_MONITOR_FIELD_LABELS.items():
                    edit_values[field] = st.number_input(
                        label,
                        value=float(edit_row[field]) if edit_row.get(field) is not None and not pd.isna(edit_row.get(field)) else 0.0,
                        format="%.4f",
                    )
                submitted = st.form_submit_button("保存单分类数据")
                canceled = st.form_submit_button("取消")

            if canceled:
                _close_fund_monitor_action_panels()
                st.rerun()

            if submitted:
                try:
                    rows = build_fund_monitor_upsert_rows(
                        _enrich_fund_monitor_rows(
                            [
                                {
                                    "month": edit_month,
                                    "category_name": edit_name,
                                    **edit_values,
                                }
                            ]
                        ),
                        source_type="manual",
                        source_file=None,
                    )
                    upsert_fund_monitor_rows(engine, rows)
                except Exception as exc:
                    st.error(f"保存失败: {exc}")
                else:
                    _close_fund_monitor_action_panels()
                    st.success("保存成功")
                    st.rerun()

    if st.session_state.get("fund_delete_month_open", False):
        st.markdown("#### 删除整月数据")
        if df.empty or not month_options:
            st.info("当前暂无可删除的基金监测月份。")
        else:
            delete_month = st.selectbox("选择要删除的月份", options=month_options, key="fund_delete_month")
            month_record_count = int((pd.to_datetime(df["month"]).dt.strftime("%Y-%m") == delete_month).sum())
            st.warning(f"将删除 {delete_month} 的全部基金监测数据，共 {month_record_count} 条分类记录。")
            confirm_key = f"fund_delete_month_confirm_{st.session_state.get('fund_delete_month_open_version', 0)}"
            delete_confirmed = st.checkbox(
                f"确认删除 {delete_month} 整月数据",
                key=confirm_key,
            )
            delete_col, cancel_col = st.columns(2)
            if delete_col.button(
                "删除整月数据",
                key="fund_delete_month_submit",
                type="primary",
                disabled=not delete_confirmed,
            ):
                try:
                    deleted_count = delete_fund_monitor_months(engine, [delete_month])
                except Exception as exc:
                    st.error(f"删除失败: {exc}")
                else:
                    _close_fund_monitor_action_panels()
                    st.success(f"已删除 {delete_month} 的 {deleted_count} 条基金监测记录")
                    st.rerun()
            if cancel_col.button("取消", key="fund_delete_month_cancel"):
                _close_fund_monitor_action_panels()
                st.rerun()

    if st.session_state.get("fund_import_open", False):
        st.markdown("#### 批量导入 Excel")
        upload = st.file_uploader(
            "上传公募&私募基金 Excel",
            type=["xlsx"],
            key="fund_monitor_uploader",
        )
        if upload is not None:
            try:
                imported_df = parse_fund_monitor_workbook(upload)
                preview = classify_fund_monitor_import_rows(imported_df, df)
            except Exception as exc:
                st.error(f"导入预览失败: {exc}")
            else:
                st.radio(
                    "重复记录处理",
                    ["跳过已存在记录", "覆盖已存在记录"],
                    horizontal=True,
                    key="fund_overwrite_mode",
                )
                st.write("新增记录")
                st.dataframe(
                    to_fund_monitor_display_df(preview["to_insert"]),
                    use_container_width=True,
                    hide_index=True,
                )
                st.write("覆盖记录")
                st.dataframe(
                    to_fund_monitor_display_df(preview["to_overwrite"]),
                    use_container_width=True,
                    hide_index=True,
                )
                if st.button("确认写入基金监测数据", key="fund_confirm_import"):
                    write_df = imported_df.copy()
                    if st.session_state["fund_overwrite_mode"] == "跳过已存在记录":
                        write_df = preview["to_insert"].copy()
                    if write_df.empty:
                        st.warning("没有需要写入的记录。")
                    else:
                        try:
                            rows = build_fund_monitor_upsert_rows(
                                _enrich_fund_monitor_rows(write_df.to_dict(orient="records")),
                                source_type="import",
                                source_file=getattr(upload, "name", None),
                            )
                            upsert_fund_monitor_rows(engine, rows)
                        except Exception as exc:
                            st.error(f"导入写入失败: {exc}")
                        else:
                            _close_fund_monitor_action_panels()
                            st.success(f"已写入 {len(rows)} 条基金监测记录")
                            st.rerun()


def render_fund_monitor_tab():
    st.subheader("📈 基金监测")
    st.caption("展示公募、私募与权益理财的月度规模、结构趋势与同比环比变化，支持手工录入与 Excel 批量导入。")

    for state_key, default_value in (
        ("fund_manual_month_open", False),
        ("fund_single_edit_open", False),
        ("fund_import_open", False),
        ("fund_delete_month_open", False),
        ("fund_delete_month_open_version", 0),
        ("fund_history_limit", "最近12个月"),
        ("fund_metric_field", "nav_amount"),
        ("fund_overwrite_mode", "跳过已存在记录"),
    ):
        if state_key not in st.session_state:
            st.session_state[state_key] = default_value

    try:
        engine = get_fund_monitor_engine()
        df = load_fund_monitor_df(engine)
    except Exception as exc:
        st.error(f"加载基金监测数据失败: {exc}")
        st.info("请确认 PostgreSQL 连接配置可用后重试。")
        return

    month_options: list[str] = (
        sorted(pd.to_datetime(df["month"]).dt.strftime("%Y-%m").unique().tolist(), reverse=True)
        if not df.empty
        else []
    )

    action_cols = st.columns([1, 1.2, 1.2, 1.2, 3])
    with action_cols[0]:
        if st.button("新增月份", key="fund_add_month"):
            _open_fund_monitor_action_panel("fund_manual_month_open")
    with action_cols[1]:
        if st.button("单分类补录/修改", key="fund_edit_single"):
            _open_fund_monitor_action_panel("fund_single_edit_open")
    with action_cols[2]:
        if st.button("批量导入 Excel", key="fund_import_file"):
            _open_fund_monitor_action_panel("fund_import_open")
    with action_cols[3]:
        if st.button("删除整月数据", key="fund_delete_month_btn"):
            _open_fund_monitor_action_panel("fund_delete_month_open")

    with action_cols[4]:
        if df.empty:
            st.caption("最新数据月份：- | 记录数：0 | 最近更新时间：-")
        else:
            latest_month = pd.to_datetime(df["month"]).max().strftime("%Y-%m")
            updated_at = pd.to_datetime(df["updated_at"]).max().strftime("%Y-%m-%d %H:%M")
            st.caption(f"最新数据月份：{latest_month} | 记录数：{len(df)} | 最近更新时间：{updated_at}")

    _render_fund_monitor_action_panels(engine, df, month_options)

    if df.empty:
        st.info("暂无基金监测数据，请先新增月份或批量导入。")
        all_category_names = FUND_MONITOR_DEFAULT_NAMES.copy()
        selected_snapshot = ""
        filtered_df = pd.DataFrame()
    else:
        all_category_names = _sort_fund_categories(
            FUND_MONITOR_DEFAULT_NAMES + df["category_name"].dropna().astype(str).unique().tolist()
        )
        selected_snapshot = st.selectbox("月份", options=month_options, index=0, key="fund_snapshot_month")
        selected_categories = st.multiselect(
            "分类筛选",
            options=all_category_names,
            default=[name for name in FUND_MONITOR_DEFAULT_SELECTED if name in all_category_names],
            key="fund_monitor_categories",
        )
        metric_field = st.selectbox(
            "主维度",
            options=list(FUND_MONITOR_METRIC_LABELS.keys()),
            format_func=lambda value: FUND_MONITOR_METRIC_LABELS.get(value, value),
            index=list(FUND_MONITOR_METRIC_LABELS.keys()).index(st.session_state["fund_metric_field"])
            if st.session_state["fund_metric_field"] in FUND_MONITOR_METRIC_LABELS
            else 0,
            key="fund_metric_field",
        )
        filtered_df = df[df["category_name"].isin(selected_categories)].copy() if selected_categories else df.copy()
        filtered_df["month"] = pd.to_datetime(filtered_df["month"])

        summary_df = filtered_df[filtered_df["month"].dt.strftime("%Y-%m") == selected_snapshot].copy()
        summary = build_fund_monitor_summary(summary_df if not summary_df.empty else filtered_df)
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("最新月份", summary["latest_month"] or "-")
        c2.metric("公募合计净值", f'{summary["public_total_nav"]:.2f}' if summary["public_total_nav"] is not None else "-")
        c3.metric("股票基金净值", f'{summary["equity_fund_nav"]:.2f}' if summary["equity_fund_nav"] is not None else "-")
        c4.metric("混合基金净值", f'{summary["hybrid_fund_nav"]:.2f}' if summary["hybrid_fund_nav"] is not None else "-")
        c5.metric("私募证券净值", f'{summary["private_nav"]:.2f}' if summary["private_nav"] is not None else "-")
        c6.metric("权益理财净值", f'{summary["wealth_nav"]:.2f}' if summary["wealth_nav"] is not None else "-")

        window = st.radio(
            "时间范围",
            ["最近12个月", "最近24个月", "全部"],
            horizontal=True,
            key="fund_history_limit",
        )
        if window != "全部" and not filtered_df.empty:
            cutoff = filtered_df["month"].max() - pd.DateOffset(months=11 if window == "最近12个月" else 23)
            filtered_df = filtered_df[filtered_df["month"] >= cutoff]

        if filtered_df.empty:
            st.info("当前筛选范围内暂无基金监测数据。")
        else:
            total_trend_df = build_fund_monitor_trend_df(filtered_df, value_field=metric_field)
            st.plotly_chart(
                px.line(
                    total_trend_df,
                    x="month",
                    y="value",
                    color="category_name",
                    markers=True,
                    title=f'{FUND_MONITOR_METRIC_LABELS[metric_field]}趋势',
                ),
                use_container_width=True,
            )

            structure_df = filtered_df[filtered_df["category_name"].isin(FUND_MONITOR_PUBLIC_STRUCTURE_NAMES)].copy()
            if not structure_df.empty:
                public_trend_df = build_fund_monitor_trend_df(structure_df, value_field=metric_field)
                st.plotly_chart(
                    px.area(
                        public_trend_df,
                        x="month",
                        y="value",
                        color="category_name",
                        title=f'公募结构趋势（{FUND_MONITOR_METRIC_LABELS[metric_field]}）',
                    ),
                    use_container_width=True,
                )

            change_metric_key = st.selectbox(
                "同比 / 环比曲线指标",
                options=list(FUND_CHANGE_TREND_FIELD_LABELS.keys()),
                format_func=lambda value: FUND_CHANGE_TREND_FIELD_LABELS.get(value, value),
                index=list(FUND_CHANGE_TREND_FIELD_LABELS.keys()).index(metric_field)
                if metric_field in FUND_CHANGE_TREND_FIELD_LABELS
                else 0,
                key="fund_change_metric_key",
            )
            change_trend_df = build_fund_monitor_change_trend_df(filtered_df, metric_key=change_metric_key)
            if change_trend_df.empty:
                st.info("当前筛选范围内暂无可展示的同比 / 环比曲线。")
            else:
                st.plotly_chart(
                    px.line(
                        change_trend_df,
                        x="month",
                        y="value",
                        color="category_name",
                        line_dash="change_type",
                        markers=True,
                        title=f'{FUND_CHANGE_TREND_FIELD_LABELS[change_metric_key]}同比 / 环比曲线',
                    ),
                    use_container_width=True,
                )

            snapshot_df = filtered_df[filtered_df["month"].dt.strftime("%Y-%m") == selected_snapshot].copy()
            if not snapshot_df.empty:
                snapshot_df["category_name"] = pd.Categorical(
                    snapshot_df["category_name"],
                    categories=FUND_MONITOR_DEFAULT_NAMES,
                    ordered=True,
                )
                snapshot_df = snapshot_df.sort_values("category_name")
                snapshot_df["category_name"] = snapshot_df["category_name"].astype(str)
            st.markdown("#### 最新月度快照")
            st.dataframe(
                to_fund_monitor_display_df(snapshot_df),
                use_container_width=True,
                hide_index=True,
            )



FUND_WATCHLIST_SESSION_CACHE_TTL_SECONDS = 900


def _clear_fund_watchlist_session_cache() -> None:
    st.session_state.pop("fund_watchlist_dashboard_cache", None)


def _show_fund_watchlist_flash() -> None:
    flash = st.session_state.pop("fund_watchlist_flash", None)
    if not isinstance(flash, dict):
        return
    level = str(flash.get("level") or "info")
    message = str(flash.get("message") or "")
    if message:
        getattr(st, level, st.info)(message)


def load_fund_watchlist_dashboard_data(
    watchlist_df: pd.DataFrame,
    fund_engine,
) -> list[dict]:
    from src.fund_hot_stocks import query_fund_preference_snapshot, search_funds

    items = []
    for _, watchlist_row in watchlist_df.iterrows():
        fund_code = str(watchlist_row.get("ts_code") or "").strip().upper()
        meta_df = pd.DataFrame()
        holding_df = pd.DataFrame()
        errors = []

        try:
            meta_df = search_funds(fund_code, limit=5, engine=fund_engine)
        except Exception as exc:
            logger.warning("fund watchlist metadata load failed for %s: %s", fund_code, exc)
            errors.append(f"基础信息读取失败：{exc}")

        try:
            holding_df = query_fund_preference_snapshot(
                fund_code=fund_code,
                top_n=10,
                engine=fund_engine,
            )
        except Exception as exc:
            logger.warning("fund watchlist holdings load failed for %s: %s", fund_code, exc)
            errors.append(f"持仓读取失败：{exc}")

        items.append(
            build_fund_watchlist_item(
                watchlist_row,
                meta_df,
                holding_df,
                load_error="；".join(errors),
            )
        )
    return items


def load_fund_watchlist_dashboard_data_session_cached(
    username: str,
    watchlist_df: pd.DataFrame,
    fund_engine,
) -> list[dict]:
    codes = tuple(watchlist_df["ts_code"].astype(str).str.strip().str.upper().tolist())
    now = time.time()
    cache = st.session_state.get("fund_watchlist_dashboard_cache")
    if (
        isinstance(cache, dict)
        and cache.get("username") == username
        and cache.get("codes") == codes
        and now - float(cache.get("saved_at", 0.0))
        < FUND_WATCHLIST_SESSION_CACHE_TTL_SECONDS
    ):
        return cache["items"]

    items = load_fund_watchlist_dashboard_data(watchlist_df, fund_engine)
    st.session_state["fund_watchlist_dashboard_cache"] = {
        "username": username,
        "codes": codes,
        "saved_at": now,
        "items": items,
    }
    return items


def _fund_watchlist_text(value, fallback: str = "-") -> str:
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return escape(fallback)
    text = str(value).strip()
    return escape(text or fallback)


def _fund_watchlist_date_label(value) -> str:
    timestamp = pd.to_datetime(value, errors="coerce")
    return timestamp.strftime("%Y-%m-%d") if not pd.isna(timestamp) else "-"


def _fund_watchlist_number_label(value, suffix: str, digits: int = 2) -> str:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return "-"
    return f"{float(number):,.{digits}f} {suffix}".strip()


def _fund_watchlist_ratio_class(value) -> str:
    if value is None:
        return ""
    if value >= 60:
        return " is-high"
    if value <= 40:
        return " is-low"
    return ""


def render_fund_watchlist_summary(summary: dict) -> None:
    latest_label = _fund_watchlist_date_label(summary.get("latest_end_date"))
    average_ratio = summary.get("average_top10_ratio")
    average_ratio_label = f"{float(average_ratio):.2f}%" if average_ratio is not None else "-"
    positive_count = int(summary.get("positive_change_count", 0))
    decrease_count = int(summary.get("decrease_count", 0))
    st.html(
        f"""
        <section class="ws-fund-watchboard" aria-label="自选基金组合总览">
            <div class="ws-fund-watchboard__eyebrow">
                <strong>FUND WATCHBOARD</strong>
                <span>持仓快照 · 结构对比 · 个人自选</span>
            </div>
            <div class="ws-fund-watchboard__summary">
                <div class="ws-fund-watchboard__metric is-accent">
                    <label>自选基金</label>
                    <strong>{int(summary.get("fund_count", 0))} 只</strong>
                    <span>当前账号已保存</span>
                </div>
                <div class="ws-fund-watchboard__metric">
                    <label>最新披露期</label>
                    <strong>{latest_label}</strong>
                    <span>按已加载持仓快照</span>
                </div>
                <div class="ws-fund-watchboard__metric">
                    <label>平均 Top10 集中度</label>
                    <strong>{average_ratio_label}</strong>
                    <span>仅反映持仓结构差异</span>
                </div>
                <div class="ws-fund-watchboard__metric is-change">
                    <label>持仓变动</label>
                    <strong>+{positive_count} / -{decrease_count}</strong>
                    <span>新进与增持 / 减持</span>
                </div>
            </div>
        </section>
        """
    )


def _build_fund_watchlist_card_html(item: dict, focus_code: str) -> str:
    active_class = " is-active" if item["fund_code"] == focus_code else ""
    ratio_class = _fund_watchlist_ratio_class(item.get("top10_ratio"))
    ratio_label = f"{float(item['top10_ratio']):.2f}%" if item.get("top10_ratio") is not None else "-"
    issue_label = _fund_watchlist_number_label(item.get("issue_amount"), "亿份")
    holding_value_label = _fund_watchlist_number_label(item.get("holding_market_value"), "亿元")
    latest_label = _fund_watchlist_date_label(item.get("latest_end_date"))
    status_html = (
        '<span class="is-error">数据加载不完整</span>'
        if item.get("load_error")
        else f"<span>披露 {latest_label}</span>"
    )
    return f"""
    <article class="ws-fund-watchboard__card{active_class}">
        <div class="ws-fund-watchboard__card-head">
            <div class="ws-fund-watchboard__card-title">
                <strong>{_fund_watchlist_text(item.get("fund_name"))}</strong>
                <span>{_fund_watchlist_text(item.get("fund_code"))}</span>
            </div>
            <span class="ws-fund-watchboard__badge">{_fund_watchlist_text(item.get("fund_type"))}</span>
        </div>
        <div class="ws-fund-watchboard__ratio{ratio_class}">
            <small>Top10 集中度</small>
            {ratio_label}
        </div>
        <div class="ws-fund-watchboard__card-metrics">
            <div><label>基金规模</label><strong>{issue_label}</strong></div>
            <div><label>前十大持仓市值</label><strong>{holding_value_label}</strong></div>
        </div>
        <div class="ws-fund-watchboard__changes">
            <div class="is-positive"><label>新进</label><strong>{int(item.get("new_count", 0))}</strong></div>
            <div class="is-positive"><label>增持</label><strong>{int(item.get("increase_count", 0))}</strong></div>
            <div class="is-negative"><label>减持</label><strong>{int(item.get("decrease_count", 0))}</strong></div>
        </div>
        <div class="ws-fund-watchboard__date">
            {status_html}
            <span>持仓 {int(item.get("holding_count", 0))} 只</span>
        </div>
    </article>
    """


def _clear_fund_watchlist_batch_state(items: list[dict], *, clear_mode: bool = False) -> None:
    for item in items:
        st.session_state.pop(f"fund_watchlist_batch_sel_{item['safe_code']}", None)
    st.session_state.pop("fund_watchlist_table_batch_selection", None)
    st.session_state.pop("fund_watchlist_batch_confirm_pending", None)
    st.session_state.pop("fund_watchlist_batch_delete_items", None)
    st.session_state.pop("fund_watchlist_batch_delete_names", None)
    if clear_mode:
        st.session_state["fund_watchlist_batch_mode"] = False


def _selected_fund_watchlist_items(items: list[dict]) -> list[dict]:
    return [
        item
        for item in items
        if st.session_state.get(f"fund_watchlist_batch_sel_{item['safe_code']}", False)
    ]


def _sync_fund_watchlist_table_batch_selection(items: list[dict]) -> None:
    selected_codes = set(st.session_state.get("fund_watchlist_table_batch_selection", []))
    for item in items:
        st.session_state[f"fund_watchlist_batch_sel_{item['safe_code']}"] = (
            item["fund_code"] in selected_codes
        )


def render_fund_watchlist_batch_actions(
    items: list[dict],
    current_username: str,
    *,
    toggle_container,
) -> None:
    if "fund_watchlist_batch_mode" not in st.session_state:
        st.session_state["fund_watchlist_batch_mode"] = False

    with toggle_container:
        if st.button(
            "☑️ 批量管理" if not st.session_state["fund_watchlist_batch_mode"] else "✖️ 退出批量",
            key="fund_watchlist_toggle_batch_mode",
            use_container_width=True,
        ):
            st.session_state["fund_watchlist_batch_mode"] = not st.session_state["fund_watchlist_batch_mode"]
            if not st.session_state["fund_watchlist_batch_mode"]:
                _clear_fund_watchlist_batch_state(items)
            st.rerun()

    if not st.session_state["fund_watchlist_batch_mode"]:
        return

    selected_items = _selected_fund_watchlist_items(items)
    selected_count = len(selected_items)
    action_cols = st.columns([1, 1, 1.35, 2.4])

    with action_cols[0]:
        if st.button("✅ 全选", key="fund_watchlist_batch_select_all", use_container_width=True):
            for item in items:
                st.session_state[f"fund_watchlist_batch_sel_{item['safe_code']}"] = True
            st.session_state["fund_watchlist_table_batch_selection"] = [item["fund_code"] for item in items]
            st.rerun()

    with action_cols[1]:
        if st.button("⬜ 取消全选", key="fund_watchlist_batch_deselect_all", use_container_width=True):
            for item in items:
                st.session_state[f"fund_watchlist_batch_sel_{item['safe_code']}"] = False
            st.session_state["fund_watchlist_table_batch_selection"] = []
            st.rerun()

    action_cols[2].caption(f"已选 {selected_count} / {len(items)} 只基金")

    with action_cols[3]:
        if st.button(
            f"🗑️ 删除已选基金（{selected_count}）",
            key="fund_watchlist_batch_delete_button",
            type="primary",
            disabled=selected_count == 0,
            use_container_width=True,
        ):
            st.session_state["fund_watchlist_batch_confirm_pending"] = True
            st.session_state["fund_watchlist_batch_delete_items"] = [
                (item["fund_code"], "fund") for item in selected_items
            ]
            st.session_state["fund_watchlist_batch_delete_names"] = [item["fund_name"] for item in selected_items]
            st.rerun()

    if not st.session_state.get("fund_watchlist_batch_confirm_pending"):
        return

    pending_items = st.session_state.get("fund_watchlist_batch_delete_items", [])
    pending_names = st.session_state.get("fund_watchlist_batch_delete_names", [])
    if not pending_items:
        return

    st.warning(
        f"确认从自选基金中删除以下 **{len(pending_items)}** 只基金？\n\n"
        + "、".join(str(name) for name in pending_names[:12])
        + ("…" if len(pending_names) > 12 else "")
    )
    confirm_cols = st.columns([1, 1, 3])
    with confirm_cols[0]:
        if st.button(
            "确认删除",
            key="fund_watchlist_batch_confirm_yes",
            type="primary",
            use_container_width=True,
        ):
            try:
                deleted = remove_watchlist_items_batch(current_username, pending_items)
                _clear_fund_watchlist_session_cache()
                st.session_state.pop("fund_watchlist_focus_code", None)
                _clear_fund_watchlist_batch_state(items, clear_mode=True)
                st.session_state["fund_watchlist_flash"] = {
                    "level": "success",
                    "message": f"已从自选基金中删除 {deleted} 只基金",
                }
                st.rerun()
            except Exception as exc:
                st.error(f"删除自选基金失败：{exc}")

    with confirm_cols[1]:
        if st.button("取消", key="fund_watchlist_batch_confirm_no", use_container_width=True):
            st.session_state.pop("fund_watchlist_batch_confirm_pending", None)
            st.session_state.pop("fund_watchlist_batch_delete_items", None)
            st.session_state.pop("fund_watchlist_batch_delete_names", None)
            st.rerun()


def render_fund_watchlist_cards(items: list[dict], *, focus_code: str) -> None:
    is_batch_mode = bool(st.session_state.get("fund_watchlist_batch_mode"))
    st.session_state["fund_watchlist_batch_last_view"] = "看板"

    with st.container(key="fund_watchlist_card_grid"):
        for start_idx in range(0, len(items), 3):
            cols = st.columns(3)
            for offset, item in enumerate(items[start_idx : start_idx + 3]):
                with cols[offset]:
                    with st.container(key=f"fund_watchlist_card_wrap_{item['safe_code']}"):
                        st.html(_build_fund_watchlist_card_html(item, focus_code))
                        if st.button(
                            f"查看 {item['fund_name']} 详情",
                            key=f"fund_watchlist_card_button_{item['safe_code']}",
                            use_container_width=True,
                        ):
                            if is_batch_mode:
                                selection_key = f"fund_watchlist_batch_sel_{item['safe_code']}"
                                st.session_state[selection_key] = not bool(st.session_state.get(selection_key, False))
                            else:
                                st.session_state["fund_watchlist_focus_code"] = item["fund_code"]
                            st.rerun()

                        if is_batch_mode:
                            st.checkbox("选择此基金", key=f"fund_watchlist_batch_sel_{item['safe_code']}")


def render_fund_watchlist_table(items: list[dict], *, focus_code: str) -> str:
    is_batch_mode = bool(st.session_state.get("fund_watchlist_batch_mode"))
    previous_view = st.session_state.get("fund_watchlist_batch_last_view")
    st.session_state["fund_watchlist_batch_last_view"] = "表格"

    if is_batch_mode:
        current_selected_codes = [item["fund_code"] for item in _selected_fund_watchlist_items(items)]
        if previous_view != "表格":
            st.session_state["fund_watchlist_table_batch_selection"] = current_selected_codes
        st.multiselect(
            "选择要批量管理的基金",
            options=[item["fund_code"] for item in items],
            format_func=lambda code: next(
                (f"{item['fund_name']}（{item['fund_code']}）" for item in items if item["fund_code"] == code),
                code,
            ),
            key="fund_watchlist_table_batch_selection",
            on_change=_sync_fund_watchlist_table_batch_selection,
            args=(items,),
            placeholder="勾选要删除的基金",
        )

    with st.container(key="fund_watchlist_table_wrap"):
        st.dataframe(
            build_fund_watchlist_table(items),
            use_container_width=True,
            hide_index=True,
            column_config={
                "基金规模(亿份)": st.column_config.NumberColumn(format="%.2f"),
                "持仓市值(亿元)": st.column_config.NumberColumn(format="%.2f"),
                "Top10 集中度(%)": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.2f%%"),
            },
        )

    focus_labels = {
        f"{item['fund_name']}（{item['fund_code']}）": item["fund_code"]
        for item in items
    }
    valid_labels = list(focus_labels)
    current_label = next(
        (label for label, code in focus_labels.items() if code == focus_code),
        valid_labels[0],
    )
    if st.session_state.get("fund_watchlist_table_focus") not in valid_labels:
        st.session_state["fund_watchlist_table_focus"] = current_label
    selected_label = st.selectbox("报告 / 操作焦点", valid_labels, key="fund_watchlist_table_focus")
    selected_code = focus_labels[selected_label]
    st.session_state["fund_watchlist_focus_code"] = selected_code
    return selected_code


def render_fund_watchlist_focus_detail(item: dict) -> None:
    ratio = item.get("top10_ratio")
    ratio_value = 0.0 if ratio is None else max(0.0, min(float(ratio), 100.0))
    ratio_label = "-" if ratio is None else f"{float(ratio):.2f}%"
    latest_label = _fund_watchlist_date_label(item.get("latest_end_date"))
    added_label = _fund_watchlist_date_label(item.get("added_at"))

    holding_rows = []
    for holding in item.get("holdings", []):
        tone_class = ""
        if holding.get("change_flag") in {"new", "increase"}:
            tone_class = ' class="is-positive"'
        elif holding.get("change_flag") == "decrease":
            tone_class = ' class="is-negative"'
        holding_rows.append(
            "<tr>"
            f"<td>{_fund_watchlist_text(holding.get('stock_name'))}</td>"
            f"<td>{_fund_watchlist_text(holding.get('symbol'))}</td>"
            f"<td>{_fund_watchlist_number_label(holding.get('market_value_yi'), '亿')}</td>"
            f"<td>{_fund_watchlist_number_label(holding.get('weight'), '%')}</td>"
            f"<td{tone_class}>{_fund_watchlist_text(holding.get('change_label'))}</td>"
            "</tr>"
        )

    if holding_rows:
        holdings_html = f"""
        <div class="ws-fund-watchboard__table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>股票名称</th>
                        <th>股票代码</th>
                        <th>持仓市值</th>
                        <th>持仓权重</th>
                        <th>持仓变化</th>
                    </tr>
                </thead>
                <tbody>{''.join(holding_rows)}</tbody>
            </table>
        </div>
        """
    else:
        holdings_html = """
        <div class="ws-fund-watchboard__empty">
            <div><strong>暂无持仓快照</strong><br>当前基金可能尚未披露，或所选数据源暂未覆盖。</div>
        </div>
        """

    error_html = (
        f'<div class="ws-fund-watchboard__error">{_fund_watchlist_text(item.get("load_error"))}</div>'
        if item.get("load_error")
        else ""
    )

    st.html(
        f"""
        <section class="ws-fund-watchboard__focus" aria-label="聚焦基金详情">
            <div class="ws-fund-watchboard__focus-overview">
                <div class="ws-fund-watchboard__focus-kicker">当前聚焦基金</div>
                <h3>{_fund_watchlist_text(item.get("fund_name"))}</h3>
                <div class="ws-fund-watchboard__focus-code">{_fund_watchlist_text(item.get("fund_code"))}</div>
                <div class="ws-fund-watchboard__focus-main">
                    <div class="ws-fund-watchboard__ring" style="--ratio:{ratio_value}%">
                        <span><strong>{ratio_label}</strong>Top10 集中度</span>
                    </div>
                    <div class="ws-fund-watchboard__facts">
                        <div class="ws-fund-watchboard__fact"><span>基金管理人</span><strong>{_fund_watchlist_text(item.get("management"))}</strong></div>
                        <div class="ws-fund-watchboard__fact"><span>基金类型</span><strong>{_fund_watchlist_text(item.get("fund_type"))}</strong></div>
                        <div class="ws-fund-watchboard__fact"><span>最新披露日期</span><strong>{latest_label}</strong></div>
                        <div class="ws-fund-watchboard__fact"><span>持仓数量</span><strong>{int(item.get("holding_count", 0))} 只</strong></div>
                        <div class="ws-fund-watchboard__fact"><span>加入自选日期</span><strong>{added_label}</strong></div>
                    </div>
                </div>
                {error_html}
            </div>
            <div class="ws-fund-watchboard__holdings">
                <div class="ws-fund-watchboard__holdings-head">
                    <strong>前十大持仓明细</strong>
                    <span>持仓市值单位：亿元</span>
                </div>
                {holdings_html}
            </div>
        </section>
        """
    )


def render_fund_watchlist_add_panel(
    current_username: str,
    fund_engine,
    watchlist_df: pd.DataFrame,
) -> None:
    from src.fund_hot_stocks import search_funds

    existing_codes = set()
    if watchlist_df is not None and not watchlist_df.empty and "ts_code" in watchlist_df.columns:
        existing_codes = set(
            watchlist_df["ts_code"].astype(str).str.strip().str.upper()
        )

    with st.container(key="fund_watchlist_add_panel"):
        st.markdown("#### ➕ 添加自选基金")
        st.caption("输入基金代码、名称或管理人，搜索后可直接加入；查看持仓不再是添加自选的前置步骤。")

        with st.form("fund_watchlist_add_search_form", border=False):
            search_cols = st.columns([3.4, 1])
            with search_cols[0]:
                keyword = st.text_input(
                    "搜索基金",
                    placeholder="如 000001.OF、招商中证白酒、易方达",
                    key="fund_watchlist_add_keyword",
                    label_visibility="collapsed",
                ).strip()
            with search_cols[1]:
                search_submitted = st.form_submit_button(
                    "搜索基金",
                    type="primary",
                    use_container_width=True,
                )

        if search_submitted:
            if not keyword:
                st.warning("请输入基金代码、名称或管理人。")
                st.session_state.pop("fund_watchlist_add_search_result", None)
            else:
                try:
                    candidates = search_funds(keyword, limit=30, engine=fund_engine)
                    st.session_state["fund_watchlist_add_search_result"] = {
                        "username": current_username,
                        "keyword": keyword,
                        "candidates": candidates,
                    }
                except Exception as exc:
                    st.session_state.pop("fund_watchlist_add_search_result", None)
                    st.error(f"搜索基金失败：{exc}")

        payload = st.session_state.get("fund_watchlist_add_search_result")
        candidates = pd.DataFrame()
        if isinstance(payload, dict) and payload.get("username") == current_username:
            stored_candidates = payload.get("candidates")
            if isinstance(stored_candidates, pd.DataFrame):
                candidates = stored_candidates

        if search_submitted and candidates.empty and keyword:
            st.info("没有找到匹配基金，请尝试完整代码、基金简称或管理人名称。")

        if candidates.empty:
            return

        candidate_rows = [row for _, row in candidates.iterrows()]
        candidate_options = list(range(len(candidate_rows)))
        if st.session_state.get("fund_watchlist_add_candidate") not in candidate_options:
            st.session_state["fund_watchlist_add_candidate"] = 0
        selected_idx = st.selectbox(
            "选择基金",
            options=candidate_options,
            format_func=lambda idx: (
                f"{str(candidate_rows[idx].get('name') or candidate_rows[idx].get('fund_code') or '-')}"
                f"（{str(candidate_rows[idx].get('fund_code') or '-')}｜"
                f"{str(candidate_rows[idx].get('management') or '未知管理人')}｜"
                f"{str(candidate_rows[idx].get('fund_type') or '未知类型')}）"
            ),
            key="fund_watchlist_add_candidate",
        )
        selected_row = candidate_rows[int(selected_idx)]
        fund_code = str(selected_row.get("fund_code") or "").strip().upper()
        fund_name = str(selected_row.get("name") or fund_code).strip() or fund_code
        already_saved = fund_code in existing_codes

        add_cols = st.columns([1.2, 2.4])
        with add_cols[0]:
            if st.button(
                "已在自选基金" if already_saved else "加入自选基金",
                key=f"fund_watchlist_add_selected_{fund_code}",
                type="primary",
                disabled=already_saved or not fund_code,
                use_container_width=True,
            ):
                try:
                    add_watchlist_item(
                        current_username,
                        fund_code,
                        security_name=fund_name,
                        security_type="fund",
                    )
                    _clear_fund_watchlist_session_cache()
                    st.session_state.pop("fund_watchlist_add_search_result", None)
                    st.session_state["fund_watchlist_flash"] = {
                        "level": "success",
                        "message": f"已将 {fund_name} 加入自选基金",
                    }
                    st.rerun()
                except Exception as exc:
                    st.error(f"加入自选基金失败：{exc}")
        add_cols[1].caption(
            "加入后会立即出现在下方看板中；持仓快照会按现有基金数据自动加载。"
        )


def render_fund_watchlist_tab() -> None:
    from src.fund_hot_stocks import get_engine as get_fund_hot_engine

    st.subheader("⭐ 自选基金")
    st.caption("追踪自选基金的持仓结构、披露进度与集中度变化")
    st.markdown(FUND_WATCHLIST_DASHBOARD_CSS, unsafe_allow_html=True)
    _show_fund_watchlist_flash()

    current_username = get_logged_in_username()
    if not current_username:
        st.info("请先登录用户名，再查看和管理你的自选基金。")
        return

    try:
        watchlist_df = list_watchlist_items(current_username, security_type="fund")
    except Exception as exc:
        st.error(f"加载自选基金失败：{exc}")
        return

    try:
        fund_engine = get_fund_hot_engine()
    except Exception as exc:
        st.error(f"连接基金持仓数据库失败：{exc}")
        return

    render_fund_watchlist_add_panel(current_username, fund_engine, watchlist_df)

    if watchlist_df is None or watchlist_df.empty:
        st.info("你的自选基金还是空的，请从上方搜索并添加第一只基金。")
        return

    with st.spinner("正在加载自选基金持仓数据..."):
        items = load_fund_watchlist_dashboard_data_session_cached(current_username, watchlist_df, fund_engine)

    if not items:
        st.info("当前没有可展示的自选基金，请稍后重试。")
        return

    render_fund_watchlist_summary(build_fund_watchlist_summary(items))

    control_cols = st.columns([1.1, 1.4, 1.2])
    with control_cols[0]:
        view_mode = st.radio("视图模式", ["看板", "表格"], horizontal=True, key="fund_watchlist_view_mode")
    with control_cols[1]:
        sort_label = st.selectbox(
            "排序方式",
            ["Top10 集中度", "基金规模", "持仓市值", "披露日期"],
            key="fund_watchlist_sort_label",
        )

    sorted_items = sort_fund_watchlist_items(items, sort_label)
    valid_codes = [item["fund_code"] for item in sorted_items]
    focus_code = str(st.session_state.get("fund_watchlist_focus_code") or "").strip().upper()
    if focus_code not in valid_codes:
        focus_code = valid_codes[0]
        st.session_state["fund_watchlist_focus_code"] = focus_code

    render_fund_watchlist_batch_actions(
        sorted_items,
        current_username,
        toggle_container=control_cols[2],
    )

    if view_mode == "看板":
        render_fund_watchlist_cards(sorted_items, focus_code=focus_code)
        focus_code = str(st.session_state.get("fund_watchlist_focus_code") or focus_code)
    else:
        focus_code = render_fund_watchlist_table(sorted_items, focus_code=focus_code)

    focus_item = next(item for item in sorted_items if item["fund_code"] == focus_code)
    render_fund_watchlist_focus_detail(focus_item)


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

    sub_top, sub_stock, sub_fund = st.tabs(["🔥 热股榜", "🔎 个股持仓透视", "🏦 基金持仓查询"])

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
                title=dict(text=f"{selected_sort} Top{min(len(plot_df), int(top_n))}", x=0.02, font=dict(size=18, color=THEME_TEXT)),
                xaxis_title=xaxis_title,
                height=max(420, len(plot_df) * 24),
                template="wealthspark_balanced",
                paper_bgcolor=CHART_PAPER_BG,
                plot_bgcolor=CHART_BG,
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
                title=dict(text=f"{stock_title} 持仓基金 Top20", x=0.02, font=dict(size=18, color=THEME_TEXT)),
                xaxis_title="持仓市值（亿元）",
                height=max(420, len(plot_df) * 24),
                template="wealthspark_balanced",
                paper_bgcolor=CHART_PAPER_BG,
                plot_bgcolor=CHART_BG,
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
                    title=dict(text="管理人抱团分布 Top15", x=0.02, font=dict(size=17, color=THEME_TEXT)),
                    xaxis_title="总持仓市值（亿元）",
                    height=max(360, len(mgmt_plot) * 26),
                    template="wealthspark_balanced",
                    paper_bgcolor=CHART_PAPER_BG,
                    plot_bgcolor=CHART_BG,
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
                        marker_color=THEME_NAVY,
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
                        line=dict(color=THEME_WARN, width=3),
                        marker=dict(size=7),
                    ),
                    secondary_y=True,
                )
                fig_trend.update_layout(
                    title=dict(text="近季度持仓趋势", x=0.02, font=dict(size=17, color=THEME_TEXT)),
                    template="wealthspark_balanced",
                    paper_bgcolor=CHART_PAPER_BG,
                    plot_bgcolor=CHART_BG,
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
        st.markdown("#### 🏦 基金持仓查询")
        st.caption("输入基金代码 / 基金名称，查看该基金在选定报告期披露的股票持仓。")

        def _normalize_direct_fund_code(raw) -> str:
            value = str(raw or "").strip().upper()
            if not value:
                return ""
            if "." in value:
                return value
            if len(value) == 6 and value.isdigit():
                return f"{value}.OF"
            return ""

        fund_col1, fund_col2, fund_col3, fund_col4 = st.columns([1.4, 1.8, 1.0, 1.0])
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
            direct_fund_code = _normalize_direct_fund_code(fund_keyword)
            direct_fund_query_mode = False
            if fund_keyword:
                try:
                    fund_candidates = search_funds(fund_keyword, limit=30, engine=_fh_engine)
                except Exception as exc:
                    st.warning(f"匹配基金失败：{exc}")
                    fund_candidates = pd.DataFrame()

            if fund_candidates is not None and len(fund_candidates) > 0 and direct_fund_code:
                normalized_direct_code = direct_fund_code.upper()
                matched_direct_rows = fund_candidates[
                    fund_candidates["fund_code"].astype(str).str.upper() == normalized_direct_code
                ]
                if not matched_direct_rows.empty:
                    fund_selected_row = matched_direct_rows.iloc[0]

            if fund_keyword and (fund_candidates is None or len(fund_candidates) == 0):
                if direct_fund_code:
                    direct_fund_query_mode = True
                    st.info(
                        f"未在基金基础表匹配到候选，将先按基金代码 {direct_fund_code} 直接查询；若该代码已失效或换码，结果可能为空，建议改用基金名称搜索。"
                    )
                    st.text_input("匹配基金", value=direct_fund_code, disabled=True, key="fh_fund_match_placeholder")
                else:
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
        with fund_col4:
            fund_top_n = st.selectbox("显示持仓数", [10, 20, 30, 50, 100, 300], index=3, key="fh_fund_top_n")

        if st.button("查询基金持仓", type="primary", key="btn_fh_fund_query"):
            st.session_state["fh_fund_error"] = ""
            st.session_state["fh_fund_result"] = pd.DataFrame()
            st.session_state["fh_fund_keyword"] = fund_keyword
            st.session_state["fh_fund_last_query_period"] = fund_period
            if fund_selected_row is None and not direct_fund_query_mode:
                st.session_state["fh_fund_code"] = ""
                if fund_keyword:
                    st.session_state["fh_fund_error"] = "没有匹配到基金，请先从候选结果中选择基金。"
                else:
                    st.session_state["fh_fund_error"] = "请先输入基金代码/名称，并从匹配结果中选择基金。"
            else:
                fund_code = (
                    str(fund_selected_row.get("fund_code") or "").strip().upper()
                    if fund_selected_row is not None
                    else direct_fund_code
                )
                st.session_state["fh_fund_code"] = fund_code
                st.session_state["fh_fund_name"] = (
                    str(fund_selected_row.get("name") or fund_code).strip()
                    if fund_selected_row is not None
                    else fund_code
                )
                try:
                    fund_df = query_fund_preference_snapshot(
                        fund_code=fund_code,
                        period=fund_period.replace("-", ""),
                        top_n=int(fund_top_n),
                        engine=_fh_engine,
                    )
                    st.session_state["fh_fund_result"] = fund_df
                    if (fund_df is None or fund_df.empty) and direct_fund_query_mode:
                        st.session_state["fh_fund_error"] = (
                            f"基金代码 {fund_code} 在当前基金基础/持仓数据中未查到结果。这个代码可能已经失效、换码，或当前报告期暂无披露；建议改用基金名称重新搜索。"
                        )
                except Exception as exc:
                    logger.error(f"query_fund_preference_snapshot failed: {exc}", exc_info=True)
                    st.session_state["fh_fund_error"] = "基金持仓查询失败，请检查基金代码、报告期或稍后重试。"

        fund_error = st.session_state.get("fh_fund_error", "")
        if fund_error:
            st.error(fund_error)

        fund_df = st.session_state.get("fh_fund_result")
        if fund_df is not None and not fund_df.empty:
            fund_df = fund_df.copy()
            fund_df["mkv_yi"] = pd.to_numeric(fund_df["mkv"], errors="coerce").fillna(0) / 1e8
            fund_df["delta_mkv_yi"] = pd.to_numeric(fund_df["delta_mkv"], errors="coerce").fillna(0) / 1e8
            fund_df["holding_change_flag"] = fund_df["holding_change_flag"].replace({
                "new": "New",
                "increase": "Increase",
                "decrease": "Decrease",
                "stable": "Stable",
            })

            fund_name = str(
                fund_df.iloc[0].get("fund_name")
                or st.session_state.get("fh_fund_name")
                or st.session_state.get("fh_fund_code", "")
            )
            management = str(fund_df.iloc[0].get("management") or "-")
            fund_type = str(fund_df.iloc[0].get("fund_type") or fund_df.iloc[0].get("invest_type") or "-")
            fund_query_period = st.session_state.get("fh_fund_last_query_period", fund_period)
            st.info(
                f"Current fund: {fund_name} | Manager: {management} | Type: {fund_type} | Report period: {fund_query_period}"
            )

            fund_metrics = st.columns(4)
            fund_metrics[0].metric("Holding Count", f"{len(fund_df):,}")
            fund_metrics[1].metric("Holding MV", f"{fund_df['mkv_yi'].sum():,.2f} bn CNY")
            fund_metrics[2].metric("New Holdings", f"{int((fund_df['holding_change_flag'] == 'New').sum())}")
            top10_ratio = pd.to_numeric(fund_df.get("stk_mkv_ratio"), errors="coerce").fillna(0).head(10).sum()
            fund_metrics[3].metric("Top10 Weight", f"{top10_ratio:,.2f}%")

            current_username = get_logged_in_username()
            fund_watchlist_cols = st.columns([1.2, 1.2, 2.2])
            if fund_watchlist_cols[1].button(
                "查看自选基金",
                key="btn_open_fund_watchlist",
                use_container_width=True,
            ):
                queue_fund_watchlist_navigation()
                st.rerun()

            if current_username:
                try:
                    already_in_fund_watchlist = is_in_watchlist(
                        current_username,
                        st.session_state.get("fh_fund_code", ""),
                        security_type="fund",
                    )
                except Exception as watchlist_check_exc:
                    st.warning(f"读取自选基金状态失败：{watchlist_check_exc}")
                    already_in_fund_watchlist = False

                button_label = "已加入自选基金" if already_in_fund_watchlist else "加入自选基金"
                if fund_watchlist_cols[0].button(
                    button_label,
                    key=f"btn_add_fund_watchlist_{st.session_state.get('fh_fund_code', '')}",
                    disabled=already_in_fund_watchlist,
                ):
                    try:
                        add_watchlist_item(
                            current_username,
                            st.session_state.get("fh_fund_code", ""),
                            security_name=fund_name,
                            security_type="fund",
                        )
                        _clear_fund_watchlist_session_cache()
                        st.success(f"已将 {fund_name} 加入 {current_username} 的自选基金")
                        st.rerun()
                    except Exception as add_watchlist_exc:
                        st.error(f"加入自选基金失败：{add_watchlist_exc}")
                fund_watchlist_cols[2].caption(
                    f"当前用户：{current_username}｜可在这里加入，也可前往“自选基金”页面继续添加和统一管理"
                )
            else:
                fund_watchlist_cols[0].button(
                    "加入自选基金",
                    key=f"btn_add_fund_watchlist_disabled_{st.session_state.get('fh_fund_code', '')}",
                    disabled=True,
                )
                fund_watchlist_cols[2].info(
                    "请先登录用户名，再把该基金加入个人自选基金。"
                )

            pref_plot = fund_df.head(min(len(fund_df), 20)).copy()
            fig_pref = go.Figure(go.Bar(
                x=pref_plot["mkv_yi"],
                y=pref_plot["stock_name"],
                orientation="h",
                marker=dict(color=pref_plot["mkv_yi"], colorscale="Bluered", showscale=False),
                text=pref_plot["mkv_yi"].map(lambda v: f"{v:,.2f} bn" if pd.notna(v) else "-"),
                textposition="outside",
                hovertemplate="%{y}<br>Holding MV: %{x:,.2f} bn<extra></extra>",
            ))
            fig_pref.update_layout(
                title=dict(text=f"Fund Holdings Top {len(pref_plot)}", x=0.02, font=dict(size=17, color=THEME_TEXT)),
                xaxis_title="Holding MV (bn CNY)",
                height=max(380, len(pref_plot) * 26),
                template="wealthspark_balanced",
                paper_bgcolor=CHART_PAPER_BG,
                plot_bgcolor=CHART_BG,
                font=dict(family="Inter, PingFang SC, sans-serif"),
                margin=dict(l=120, r=40, t=55, b=20),
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig_pref, use_container_width=True)

            pref_show = fund_df[[
                "stock_name",
                "symbol",
                "mkv_yi",
                "amount",
                "stk_mkv_ratio",
                "stk_float_ratio",
                "delta_mkv_yi",
                "holding_change_flag",
                "ann_date",
            ]].copy()
            pref_show.columns = [
                "Stock",
                "Code",
                "Holding MV (bn)",
                "Amount",
                "Weight in Fund (%)",
                "Float Weight (%)",
                "MV Change (bn)",
                "Change",
                "Announcement Date",
            ]
            for col in ["Holding MV (bn)", "Weight in Fund (%)", "Float Weight (%)", "MV Change (bn)"]:
                pref_show[col] = pd.to_numeric(pref_show[col], errors="coerce").map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
            pref_show["Amount"] = pd.to_numeric(pref_show["Amount"], errors="coerce").map(lambda v: f"{v:,.0f}" if pd.notna(v) else "-")
            pref_show["Announcement Date"] = pd.to_datetime(pref_show["Announcement Date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("-")
            st.dataframe(pref_show, use_container_width=True, hide_index=True)
        elif fund_df is not None and fund_df.empty and not fund_error and st.session_state.get("fh_fund_code"):
            st.info("该基金在所选报告期暂无可用持仓数据。")
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
                    colorscale=[[0, THEME_DOWN], [0.5, THEME_WARN], [1, THEME_UP]],
                    showscale=False,
                ),
                text=df_disp["net_mf_amount"].apply(lambda v: f"{float(v):,.0f}万"),
                textposition="outside",
                hovertemplate="%{y}<br>主力净流入: %{x:,.0f} 万元<extra></extra>",
            ))
            fig_bar.update_layout(
                title=dict(text="主力净流入（万元）", x=0.02, font=dict(size=18, color=THEME_TEXT)),
                xaxis_title="净流入额（万元）",
                height=max(400, top_n * 22),
                template="wealthspark_balanced",
                paper_bgcolor=CHART_PAPER_BG,
                plot_bgcolor=CHART_BG,
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
            colors = [THEME_UP if v >= 0 else THEME_DOWN for v in net]

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
                    line=dict(color=THEME_PURPLE, width=2),
                    hovertemplate="%{x|%Y-%m-%d}<br>超大单净额: %{y:,.0f} 万元<extra></extra>",
                ))

            selected_stock_title = st.session_state.get('mf_stock_name') or st.session_state.get('mf_stock_code', '')
            if st.session_state.get('mf_stock_code') and st.session_state.get('mf_stock_name'):
                selected_stock_title = f"{st.session_state.get('mf_stock_name')}（{st.session_state.get('mf_stock_code')}）"

            fig_hist.update_layout(
                title=dict(
                    text=f"{selected_stock_title} 主力资金流入趋势",
                    x=0.02, font=dict(size=18, color=THEME_TEXT)
                ),
                xaxis_title="日期",
                yaxis_title="净流入额（万元）",
                hovermode="x unified",
                height=420,
                template="wealthspark_balanced",
                paper_bgcolor=CHART_PAPER_BG,
                plot_bgcolor=CHART_BG,
                font=dict(family="Inter, PingFang SC, sans-serif"),
                margin=dict(l=20, r=20, t=60, b=20),
                legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5),
            )
            apply_time_series_hover_affordance(fig_hist, df_hist["trade_date"], df_hist["net_mf_amount"])
            fig_hist.update_xaxes(showgrid=True, gridcolor=CHART_GRID_COLOR)
            fig_hist.update_yaxes(showgrid=True, gridcolor=CHART_GRID_COLOR, zeroline=True, zerolinecolor=CHART_ZERO_LINE_COLOR, zerolinewidth=1.5)
            st.plotly_chart(fig_hist, use_container_width=True)

            # 买卖力量对比（最近20个交易日）
            recent = df_hist.tail(20).copy()
            if len(recent) > 0:
                fig_force = go.Figure()
                fig_force.add_trace(go.Bar(
                    name="超大单买入",
                    x=recent["trade_date"], y=recent.get("buy_elg_amount", pd.Series(dtype=float)).astype(float),
                    marker_color=THEME_UP, opacity=0.85,
                ))
                fig_force.add_trace(go.Bar(
                    name="超大单卖出",
                    x=recent["trade_date"], y=-recent.get("sell_elg_amount", pd.Series(dtype=float)).astype(float),
                    marker_color=THEME_DOWN, opacity=0.85,
                ))
                fig_force.add_trace(go.Bar(
                    name="大单买入",
                    x=recent["trade_date"], y=recent.get("buy_lg_amount", pd.Series(dtype=float)).astype(float),
                    marker_color=THEME_WARN, opacity=0.7,
                ))
                fig_force.add_trace(go.Bar(
                    name="大单卖出",
                    x=recent["trade_date"], y=-recent.get("sell_lg_amount", pd.Series(dtype=float)).astype(float),
                    marker_color="#5B8E7D", opacity=0.7,
                ))
                fig_force.update_layout(
                    barmode="relative",
                    title=dict(text="近20日买卖力量博弈（万元）", x=0.02, font=dict(size=16, color=THEME_TEXT)),
                    height=360,
                    template="wealthspark_balanced",
                    paper_bgcolor=CHART_PAPER_BG,
                    plot_bgcolor=CHART_BG,
                    font=dict(family="Inter, PingFang SC, sans-serif"),
                    margin=dict(l=20, r=20, t=60, b=20),
                    hovermode="x unified",
                    legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5),
                )
                apply_time_series_hover_affordance(
                    fig_force,
                    recent["trade_date"],
                    [
                        recent.get("buy_elg_amount", pd.Series(dtype=float)).astype(float),
                        -recent.get("sell_elg_amount", pd.Series(dtype=float)).astype(float),
                        recent.get("buy_lg_amount", pd.Series(dtype=float)).astype(float),
                        -recent.get("sell_lg_amount", pd.Series(dtype=float)).astype(float),
                    ],
                )
                fig_force.update_xaxes(showgrid=True, gridcolor=CHART_GRID_COLOR)
                fig_force.update_yaxes(showgrid=True, gridcolor=CHART_GRID_COLOR, zeroline=True, zerolinecolor=CHART_ZERO_LINE_COLOR, zerolinewidth=1.5)
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
                        marker_color=[THEME_UP if v >= 0 else THEME_DOWN for v in _ths["net_amount"].astype(float)],
                        hovertemplate="%{x|%Y-%m-%d}<br>THS净流入: %{y:,.0f} 万元<extra></extra>",
                    ))
                    if "net_d5_amount" in _ths.columns:
                        fig_ths.add_trace(go.Scatter(
                            x=_ths["trade_date"],
                            y=_ths["net_d5_amount"].astype(float),
                            mode="lines",
                            name="THS 5日主力净额",
                            line=dict(color=THEME_NAVY, width=2),
                            hovertemplate="%{x|%Y-%m-%d}<br>THS 5日净额: %{y:,.0f} 万元<extra></extra>",
                        ))

                    fig_ths.update_layout(
                        title=dict(text="THS 个股资金流向趋势", x=0.02, font=dict(size=16, color=THEME_TEXT)),
                        xaxis_title="日期",
                        yaxis_title="净流入额（万元）",
                        hovermode="x unified",
                        height=360,
                        template="wealthspark_balanced",
                        paper_bgcolor=CHART_PAPER_BG,
                        plot_bgcolor=CHART_BG,
                        font=dict(family="Inter, PingFang SC, sans-serif"),
                        margin=dict(l=20, r=20, t=60, b=20),
                    )
                    apply_time_series_hover_affordance(fig_ths, _ths["trade_date"], _ths["net_amount"])
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
                        marker_color=[THEME_UP if v >= 0 else THEME_DOWN for v in _dc["net_amount"].astype(float)],
                        hovertemplate="%{x|%Y-%m-%d}<br>DC净流入: %{y:,.0f} 万元<extra></extra>",
                    ))
                    if "net_amount_rate" in _dc.columns:
                        fig_dc.add_trace(go.Scatter(
                            x=_dc["trade_date"],
                            y=_dc["net_amount_rate"].astype(float),
                            mode="lines",
                            name="DC净占比(%)",
                            yaxis="y2",
                            line=dict(color=THEME_WARN, width=2),
                            hovertemplate="%{x|%Y-%m-%d}<br>DC净占比: %{y:.2f}%<extra></extra>",
                        ))

                    fig_dc.update_layout(
                        title=dict(text="DC 个股资金流向趋势", x=0.02, font=dict(size=16, color=THEME_TEXT)),
                        xaxis_title="日期",
                        yaxis_title="净流入额（万元）",
                        yaxis2=dict(title="净占比(%)", overlaying="y", side="right", showgrid=False),
                        hovermode="x unified",
                        height=360,
                        template="wealthspark_balanced",
                        paper_bgcolor=CHART_PAPER_BG,
                        plot_bgcolor=CHART_BG,
                        font=dict(family="Inter, PingFang SC, sans-serif"),
                        margin=dict(l=20, r=20, t=60, b=20),
                    )
                    apply_time_series_hover_affordance(fig_dc, _dc["trade_date"], _dc["net_amount"])
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
                title=dict(text="连续净流入天数 vs 累计净流入额", x=0.02, font=dict(size=18, color=THEME_TEXT)),
                xaxis_title="连续净流入天数",
                yaxis_title="累计主力净流入（万元）",
                height=480,
                template="wealthspark_balanced",
                paper_bgcolor=CHART_PAPER_BG,
                plot_bgcolor=CHART_BG,
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
                        color_discrete_map={"净流入": THEME_UP, "净流出": THEME_DOWN},
                        orientation="h",
                        range_x=[-max_abs * 1.15, max_abs * 1.15],
                        hover_data={"net_amount_yi": ':.2f', "sector_name": True, "date_label": True},
                    )
                    fig_anim.update_layout(
                        title=dict(text=f"{sector_anim_source} 资金流向轮动（从 {str(sector_anim_start)} 到 {latest_date}）", x=0.02, font=dict(size=17, color=THEME_TEXT)),
                        template="wealthspark_balanced",
                        paper_bgcolor=CHART_PAPER_BG,
                        plot_bgcolor=CHART_BG,
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
                                    font=dict(size=12, color=THEME_TEXT),
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
                            title=dict(text=f"{sector_anim_source} 资金曲线动画（从 {str(sector_anim_start)} 到 {latest_date}）", x=0.02, font=dict(size=17, color=THEME_TEXT)),
                            template="wealthspark_balanced",
                            paper_bgcolor=CHART_PAPER_BG,
                            plot_bgcolor=CHART_BG,
                            font=dict(family="Inter, PingFang SC, sans-serif"),
                            height=720,
                            margin=dict(l=30, r=260, t=80, b=30),
                            xaxis_title="日期",
                            yaxis_title="净流入（亿元）",
                            legend_title_text="板块",
                            hovermode="x unified",
                            hoverdistance=TIME_SERIES_HOVER_DISTANCE,
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

                    colors_ths = [THEME_UP if v >= 0 else THEME_DOWN for v in df_ths["net_amount"]]
                    fig_ths = go.Figure(go.Bar(
                        x=df_ths["net_amount"],
                        y=df_ths["industry"].fillna("未知"),
                        orientation="h",
                        marker_color=colors_ths,
                        hovertemplate="%{y}<br>净流入: %{x:,.2f} 亿元<extra></extra>",
                    ))
                    fig_ths.update_layout(
                        title=dict(text="THS 行业净流入（亿元）", x=0.02, font=dict(size=15, color=THEME_TEXT)),
                        height=max(380, len(df_ths) * 20),
                        template="wealthspark_balanced",
                        paper_bgcolor=CHART_PAPER_BG,
                        plot_bgcolor=CHART_BG,
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

                    colors_dc = [THEME_UP if v >= 0 else THEME_DOWN for v in df_dc["net_amount"]]
                    fig_dc = go.Figure(go.Bar(
                        x=df_dc["net_amount"],
                        y=df_dc["name"].fillna("未知"),
                        orientation="h",
                        marker_color=colors_dc,
                        hovertemplate="%{y}<br>净流入: %{x:,.2f} 亿元<extra></extra>",
                    ))
                    fig_dc.update_layout(
                        title=dict(text="DC 板块净流入（亿元）", x=0.02, font=dict(size=15, color=THEME_TEXT)),
                        height=max(380, len(df_dc) * 20),
                        template="wealthspark_balanced",
                        paper_bgcolor=CHART_PAPER_BG,
                        plot_bgcolor=CHART_BG,
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
                colors_n = [THEME_UP if v >= 0 else THEME_DOWN for v in north_vals]
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
                    line=dict(color=THEME_PURPLE, width=2.5),
                    hovertemplate="%{x|%Y-%m-%d}<br>5日均线: %{y:,.2f} 亿<extra></extra>",
                ))

                fig_hsgt.update_layout(
                    title=dict(text="北向资金每日净流入（亿元）", x=0.02, font=dict(size=18, color=THEME_TEXT)),
                    xaxis_title="日期",
                    yaxis_title="净流入额（亿元）",
                    hovermode="x unified",
                    height=420,
                    template="wealthspark_balanced",
                    paper_bgcolor=CHART_PAPER_BG,
                    plot_bgcolor=CHART_BG,
                    font=dict(family="Inter, PingFang SC, sans-serif"),
                    margin=dict(l=20, r=20, t=60, b=20),
                    legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5),
                )
                apply_time_series_hover_affordance(fig_hsgt, df_hsgt["trade_date"], north_vals)
                fig_hsgt.update_xaxes(showgrid=True, gridcolor=CHART_GRID_COLOR)
                fig_hsgt.update_yaxes(
                    showgrid=True, gridcolor=CHART_GRID_COLOR,
                    zeroline=True, zerolinecolor=CHART_ZERO_LINE_COLOR, zerolinewidth=1.5
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
                        line=dict(color=THEME_NAVY, width=2),
                        hovertemplate="%{x|%Y-%m-%d}<br>沪股通: %{y:,.2f} 亿<extra></extra>",
                    ))
                    fig_detail.add_trace(go.Scatter(
                        x=df_hsgt["trade_date"],
                        y=df_hsgt["sgt"].astype(float),
                        mode="lines",
                        name="深股通",
                        line=dict(color=THEME_WARN, width=2),
                        hovertemplate="%{x|%Y-%m-%d}<br>深股通: %{y:,.2f} 亿<extra></extra>",
                    ))
                    fig_detail.update_layout(
                        title=dict(text="沪股通 / 深股通 净流入分项", x=0.02, font=dict(size=16, color=THEME_TEXT)),
                        height=360,
                        template="wealthspark_balanced",
                        paper_bgcolor=CHART_PAPER_BG,
                        plot_bgcolor=CHART_BG,
                        font=dict(family="Inter, PingFang SC, sans-serif"),
                        margin=dict(l=20, r=20, t=60, b=20),
                        hovermode="x unified",
                        legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5),
                    )
                    apply_time_series_hover_affordance(
                        fig_detail,
                        df_hsgt["trade_date"],
                        [df_hsgt["hgt"].astype(float), df_hsgt["sgt"].astype(float)],
                    )
                    fig_detail.update_xaxes(showgrid=True, gridcolor=CHART_GRID_COLOR)
                    fig_detail.update_yaxes(showgrid=True, gridcolor=CHART_GRID_COLOR)
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
