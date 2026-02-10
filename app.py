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
                    line=dict(width=2, shape='spline'),
                    hovertemplate='<b>%{x|%Y-%m-%d}</b><br>%{y:.2f}<extra></extra>'
                ))
            else:
                fig.add_trace(go.Scatter(
                    x=agg_data['date'],
                    y=agg_data['value'],
                    mode='lines',
                    name='所有ETF总和',
                    line=dict(width=2, shape='spline'),
                    hovertemplate='<b>%{x|%Y-%m-%d}</b><br>%{y:.2f}<extra></extra>'
                ))
    else:
        # 多条线显示各个ETF
        if selected_etfs:
            for etf_name in selected_etfs:
                etf_data = filtered_df[filtered_df['name'] == etf_name].sort_values('date')
                if len(etf_data) > 0:
                    if chart_type == 'area':
                        fig.add_trace(go.Scatter(
                            x=etf_data['date'],
                            y=etf_data['value'],
                            mode='lines',
                            name=etf_name,
                            fill='tonexty',
                            line=dict(width=1.5, shape='spline'),
                            hovertemplate=f'<b>{etf_name}</b><br>%{{x|%Y-%m-%d}}<br>%{{y:.4f}}<extra></extra>'
                        ))
                    elif chart_type == 'scatter':
                        fig.add_trace(go.Scatter(
                            x=etf_data['date'],
                            y=etf_data['value'],
                            mode='markers',
                            name=etf_name,
                            marker=dict(size=6, opacity=0.7),
                            hovertemplate=f'<b>{etf_name}</b><br>%{{x|%Y-%m-%d}}<br>%{{y:.4f}}<extra></extra>'
                        ))
                    else:  # line
                        fig.add_trace(go.Scatter(
                            x=etf_data['date'],
                            y=etf_data['value'],
                            mode='lines',
                            name=etf_name,
                            line=dict(width=1.5, shape='spline'),
                            hovertemplate=f'<b>{etf_name}</b><br>%{{x|%Y-%m-%d}}<br>%{{y:.4f}}<extra></extra>'
                        ))

    # 布局配置
    fig.update_layout(
        title=f'{metric_name} 变动趋势',
        xaxis_title='日期',
        yaxis_title=metric_name,
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        height=600,
        template='plotly_white'
    )

    fig.update_xaxes(rangeslider_visible=False)

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

    # 显示数据加载信息
    st.sidebar.success(f"✅ 已加载 {len(df)} 条数据记录")

    # 侧边栏 - 数据筛选
    st.sidebar.header("🔍 数据筛选")

    # 1. 指标选择器
    metric_types = sorted(df['metric_type'].unique())

    # 检查是否有指标
    if len(metric_types) == 0:
        st.error("❌ 未检测到任何指标数据，请检查Excel文件格式")
        st.info("Excel文件应包含section标题行，标题中应包含关键词：市值、份额、变动、申赎、比例、涨跌幅")
        st.stop()

    selected_metric = st.sidebar.selectbox(
        "选择指标",
        options=metric_types,
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
        # 显示多选框，默认选择前3个ETF
        etf_names = sorted(metric_df[metric_df['is_aggregate'] == False]['name'].unique())
        default_etfs = etf_names[:3] if len(etf_names) >= 3 else etf_names

        selected_etfs = st.sidebar.multiselect(
            "选择ETF",
            options=etf_names,
            default=default_etfs
        )

    # 3. 日期范围滑块
    min_date = metric_df['date'].min().date()
    max_date = metric_df['date'].max().date()

    # 默认结束日期为数据中的最大日期（最后更新的日期）
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
