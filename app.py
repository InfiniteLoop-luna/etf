# -*- coding: utf-8 -*-
"""ETF份额变动可视化 - Streamlit Web应用"""

# Version: 2.0 - Fixed data_only issue for formula cells
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import logging
from src.data_loader import load_etf_data

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 页面配置
st.set_page_config(
    page_title="ETF份额变动可视化",
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
    # 判断涨跌
    is_positive = delta.startswith('+') if delta != '-' else None

    if is_positive is None:
        arrow = ""
        color = "#64748B"
    elif is_positive:
        arrow = "↑"
        color = "#10B981"
    else:
        arrow = "↓"
        color = "#EF4444"

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
            font=dict(size=24, weight=700, color='#1E293B'),
            x=0.02
        ),
        xaxis_title='日期',
        yaxis_title=metric_name,
        hovermode='x unified',
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.98,
            xanchor="right",
            x=0.98,
            bgcolor="rgba(255, 255, 255, 0.9)",
            bordercolor="#E2E8F0",
            borderwidth=1,
            font=dict(size=11)
        ),
        height=600,
        template='plotly_white',
        plot_bgcolor='rgba(248, 250, 252, 0.5)',
        paper_bgcolor='white',
        font=dict(family='Inter, PingFang SC, sans-serif'),
        margin=dict(l=60, r=60, t=80, b=60)
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
        linecolor='#E2E8F0'
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


# 主应用
def main():
    """主应用逻辑"""
    st.title("ETF份额变动可视化")

    # 显示版本信息（用于验证部署）
    st.caption("📌 Version 2.1 - Formula evaluation fix (2026-02-05)")

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
        st.stop()

    # 确定是否为汇总模式
    is_aggregate = has_aggregate and contains_total_market_value

    # 验证ETF选择（非汇总模式）
    if not is_aggregate and (selected_etfs is None or len(selected_etfs) == 0):
        st.info("ℹ️ 请至少选择一个ETF")
        st.stop()

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


if __name__ == "__main__":
    main()
