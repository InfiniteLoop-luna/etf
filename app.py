# -*- coding: utf-8 -*-
"""ETF份额变动可视化 - Streamlit Web应用"""

# Version: 2.0 - Fixed data_only issue for formula cells
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import logging
from typing import Optional, List
from src.data_loader import load_etf_data
from src.volume_fetcher import load_volume_dataframe
from src.etf_classifier import fetch_etf_data, process_etf_classification, export_etfs_to_excel
from src.etf_stats import (
    get_available_dates, get_category_daily_summary,
    get_category_tree, get_category_timeseries, get_agg_summary,
    get_wide_index_available_dates, get_wide_index_timeseries,
    search_security, get_security_profile, get_security_timeseries
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 页面配置
st.set_page_config(
    page_title="交易数据可视化",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 自定义CSS样式 - 金融专业风格
st.markdown("""
<style>
    /* 导入专业字体 */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    /* 全局字体设置 */
    html, body, [class*="css"] {
        font-family: 'Inter', 'PingFang SC', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    /* 隐藏Streamlit默认元素 */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* 深色专业侧边栏 */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1E293B 0%, #0F172A 100%);
        padding: 2rem 1rem;
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
        color: #F8FAFC !important;
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
        color: #E2E8F0 !important;
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
        color: #CBD5E1 !important;
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
        background: white;
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08), 0 1px 2px rgba(0, 0, 0, 0.06);
        margin: 1rem 0;
        transition: box-shadow 0.3s ease;
    }

    .stPlotlyChart:hover {
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1), 0 2px 4px rgba(0, 0, 0, 0.06);
    }

    /* 数据表格样式 */
    [data-testid="stDataFrame"] {
        background: white;
        border-radius: 12px;
        padding: 1rem;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
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


def get_security_metric_config(security_type: str) -> dict[str, dict[str, str | float | int]]:
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
        paper_bgcolor='white',
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
        paper_bgcolor='white',
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
        paper_bgcolor='white',
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
    st.title("交易数据可视化")

    # 显示版本信息（用于验证部署）
    st.caption("📌 Version 3.1 - 新增ETF分类占比饼图 (2026-03-27)")

    # 显示最后更新时间
    try:
        import json
        import os
        if os.path.exists('last_update.json'):
            with open('last_update.json', 'r') as f:
                update_info = json.load(f)
                update_date = update_info.get('update_date', '未知')
                last_update = update_info.get('last_update', '未知')
                st.info(f"📅 数据最后更新: {update_date} (GitHub Action 执行时间: {last_update})")
    except Exception as e:
        pass  # 如果文件不存在或读取失败，不显示更新时间

    # 创建Tab页
    tab_etf, tab_volume, tab_etf_classification, tab_etf_ratio, tab_etf_trend, tab_wide_index, tab_security = st.tabs(
        ["📈 ETF份额变动", "📊 每日成交量", "📊 ETF分类统计", "🥧 ETF分类占比", "📈 ETF分类趋势", "📊 宽基指数ETF", "🔎 个股/指数查询"]
    )

    # ========== ETF 份额变动 Tab ==========
    with tab_etf:
        render_etf_tab()

    # ========== 每日成交量 Tab ==========
    with tab_volume:
        render_volume_tab()

    # ========== ETF分类统计 Tab ==========
    with tab_etf_classification:
        render_etf_classification_tab()

    with tab_etf_ratio:
        render_etf_category_ratio_tab()

    # ========== ETF分类趋势 Tab ==========
    with tab_etf_trend:
        render_etf_trend_tab()

    with tab_wide_index:
        render_wide_index_tab()

    with tab_security:
        render_security_search_tab()


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

    # 显示数据加载信息
    st.sidebar.success(f"✅ 已加载 {len(df)} 条数据记录")

    # 侧边栏 - 数据筛选
    st.sidebar.header("🔍 数据筛选")

    # 1. 指标选择器 - 使用更直观的单选按钮
    metric_types = sorted(df['metric_type'].unique())

    # 检查是否有指标
    if len(metric_types) == 0:
        st.error("❌ 未检测到任何指标数据，请检查Excel文件格式")
        st.info("Excel文件应包含section标题行，标题中应包含关键词：市值、份额、变动、申赎、比例、涨跌幅")
        st.stop()

    # 创建指标分类映射
    metric_categories = {
        "市值类": [m for m in metric_types if "市值" in m],
        "份额类": [m for m in metric_types if "份额" in m],
        "变动类": [m for m in metric_types if "变动" in m or "申赎" in m],
        "比例类": [m for m in metric_types if "比例" in m],
        "涨跌类": [m for m in metric_types if "涨跌" in m],
        "其他": [m for m in metric_types if not any(keyword in m for keyword in ["市值", "份额", "变动", "申赎", "比例", "涨跌"])]
    }

    # 移除空分类
    metric_categories = {k: v for k, v in metric_categories.items() if v}

    # 如果有多个分类，显示分类选择器
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

    # 筛选当前指标的数据
    metric_df = df[df['metric_type'] == selected_metric].copy()

    # 2. 智能ETF选择器
    # 检查是否有汇总数据且指标名称包含"总市值"
    has_aggregate = metric_df['is_aggregate'].any()
    contains_total_market_value = '总市值' in selected_metric if selected_metric else False

    selected_etfs = None
    if has_aggregate and contains_total_market_value:
        # 显示信息消息，不显示ETF选择器
        st.sidebar.info("📊 当前显示所有ETF的总和")
        selected_etfs = None
    else:
        # 显示多选框，默认选择所有ETF
        etf_names = sorted(metric_df[metric_df['is_aggregate'] == False]['name'].unique())
        default_etfs = etf_names

        selected_etfs = st.sidebar.multiselect(
            "选择ETF",
            options=etf_names,
            default=default_etfs
        )

    # 3. 日期范围滑块
    min_date = metric_df['date'].min().date()
    max_date = metric_df['date'].max().date()

    # 默认结束日期为数据中的最大日期（已根据GitHub Action更新日期过滤）
    default_end_date = max_date

    # 检查是否只有一个日期
    if min_date == max_date:
        st.sidebar.info(f"📅 当前数据日期: {min_date}")
        date_range = (min_date, max_date)
    else:
        date_range = st.sidebar.slider(
            "选择日期范围",
            min_value=min_date,
            max_value=max_date,
            value=(min_date, default_end_date),
            format="YYYY-MM-DD"
        )

    # 4. 图表类型选择
    st.sidebar.header("📊 图表设置")

    # 快速指标切换（在侧边栏顶部）
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
                template="plotly_white",
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
                template="plotly_white",
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
        paper_bgcolor='white',
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
        paper_bgcolor='white',
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

    # 侧边栏筛选器
    st.sidebar.header("📂 分类趋势筛选")

    primary_options = ['全部'] + sorted(category_tree.keys())
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
        paper_bgcolor='white',
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
            value=st.session_state.get("security_search_keyword", ""),
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

    try:
        profile_df = load_security_profile(selected_code, selected_type)
        ts_df = load_security_timeseries(selected_code, selected_type)
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

    min_date = ts_df['trade_date'].min().date()
    max_date = ts_df['trade_date'].max().date()
    default_start = max(min_date, max_date - timedelta(days=365))
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
        metric_cols_bottom[3].metric("净利润(亿元)", format_optional_number(profile.get('n_income'), scale=10000.0))
        metric_cols_bottom[4].metric("经营现金流(亿元)", format_optional_number(profile.get('n_cashflow_act'), scale=10000.0))

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
                    {"字段": "总资产(亿元)", "值": format_optional_number(profile.get('total_assets'), scale=10000.0)},
                    {"字段": "总负债(亿元)", "值": format_optional_number(profile.get('total_liab'), scale=10000.0)},
                ]),
                use_container_width=True,
                hide_index=True
            )
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
        paper_bgcolor='white',
        font=dict(family='Inter, PingFang SC, sans-serif'),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(226,232,240,0.5)')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(226,232,240,0.5)', fixedrange=True)
    st.plotly_chart(fig, use_container_width=True)

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
        paper_bgcolor='white',
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


if __name__ == "__main__":
    main()

