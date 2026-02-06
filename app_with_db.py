# -*- coding: utf-8 -*-
"""ETF份额变动可视化 - Streamlit Web应用（支持数据库）"""

# Version: 3.0 - Added database support
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import logging
import os

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

# 数据源配置
DATA_SOURCE = os.getenv('DATA_SOURCE', 'excel')  # 'excel' 或 'database'
DATA_FILE = "主要ETF基金份额变动情况.xlsx"
DB_TYPE = os.getenv('DB_TYPE', 'sqlite')  # 'sqlite' 或 'postgresql'
DB_PATH = os.getenv('DB_PATH', 'etf_data.db')


@st.cache_data(ttl=300)
def load_data_from_excel(file_path: str) -> pd.DataFrame:
    """从Excel加载数据"""
    from src.data_loader import load_etf_data

    try:
        logger.info(f"Loading data from Excel: {file_path}")
        df = load_etf_data(file_path)
        logger.info(f"Data loaded successfully: {len(df)} rows")
        return df
    except FileNotFoundError:
        st.error(f"文件未找到: {file_path}")
        st.stop()
    except Exception as e:
        st.error(f"加载数据时出错: {str(e)}")
        logger.error(f"Error loading data: {e}", exc_info=True)
        st.stop()


@st.cache_resource
def get_database_connection():
    """获取数据库连接（使用cache_resource保持连接）"""
    from src.db_loader import create_sqlite_connection, create_postgresql_connection

    try:
        if DB_TYPE == 'sqlite':
            logger.info(f"Connecting to SQLite: {DB_PATH}")
            return create_sqlite_connection(DB_PATH)
        elif DB_TYPE == 'postgresql':
            logger.info("Connecting to PostgreSQL")
            # 从Streamlit secrets读取连接字符串
            connection_string = st.secrets.get("DATABASE_URL")
            return create_postgresql_connection(connection_string)
        else:
            raise ValueError(f"不支持的数据库类型: {DB_TYPE}")
    except Exception as e:
        st.error(f"数据库连接失败: {str(e)}")
        logger.error(f"Database connection error: {e}", exc_info=True)
        st.stop()


@st.cache_data(ttl=300)
def load_data_from_database(_conn) -> pd.DataFrame:
    """从数据库加载数据"""
    from src.db_loader import DatabaseLoader

    try:
        logger.info("Loading data from database")
        loader = DatabaseLoader(_conn)
        df = loader.load_etf_data()
        logger.info(f"Data loaded successfully: {len(df)} rows")
        return df
    except Exception as e:
        st.error(f"从数据库加载数据时出错: {str(e)}")
        logger.error(f"Error loading data from database: {e}", exc_info=True)
        st.stop()


def load_data() -> pd.DataFrame:
    """
    根据配置加载数据

    Returns:
        DataFrame with columns: code, name, date, metric_type, value, is_aggregate
    """
    if DATA_SOURCE == 'database':
        conn = get_database_connection()
        return load_data_from_database(conn)
    else:
        return load_data_from_excel(DATA_FILE)


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
    计算统计信息

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
        if len(agg_data) > 0:
            start_value = agg_data.iloc[0]['value']
            end_value = agg_data.iloc[-1]['value']
            max_value = agg_data['value'].max()
            min_value = agg_data['value'].min()
            change_pct = ((end_value - start_value) / start_value * 100) if start_value != 0 else 0

            stats_list.append({
                'ETF名称': '所有ETF总和',
                '期初值': f'{start_value:.2f}',
                '期末值': f'{end_value:.2f}',
                '涨跌幅': f'{change_pct:+.2f}%',
                '最大值': f'{max_value:.2f}',
                '最小值': f'{min_value:.2f}'
            })
    else:
        # 计算各个ETF的统计信息
        if selected_etfs:
            for etf_name in selected_etfs:
                etf_data = filtered_df[filtered_df['name'] == etf_name].sort_values('date')

                if len(etf_data) == 0:
                    continue

                start_value = etf_data.iloc[0]['value']
                end_value = etf_data.iloc[-1]['value']
                max_value = etf_data['value'].max()
                min_value = etf_data['value'].min()
                change_pct = ((end_value - start_value) / start_value * 100) if start_value != 0 else 0

                # 根据数值大小确定小数位数
                decimals = 2 if start_value > 100 else 4

                stats_list.append({
                    'ETF名称': etf_name,
                    '期初值': f'{start_value:.{decimals}f}',
                    '期末值': f'{end_value:.{decimals}f}',
                    '涨跌幅': f'{change_pct:+.2f}%',
                    '最大值': f'{max_value:.{decimals}f}',
                    '最小值': f'{min_value:.{decimals}f}'
                })

    return pd.DataFrame(stats_list)


# 主应用
def main():
    """主应用逻辑"""
    st.title("ETF份额变动可视化")

    # 显示版本信息和数据源
    data_source_text = "数据库" if DATA_SOURCE == 'database' else "Excel"
    st.caption(f"Version 3.0 - Database support | 数据源: {data_source_text}")

    # 加载数据
    df = load_data()

    # 验证数据
    if df is None or len(df) == 0:
        st.error("未能加载任何数据，请检查数据源")
        st.stop()

    # 显示数据加载信息
    st.sidebar.success(f"已加载 {len(df)} 条数据记录")

    # 侧边栏 - 数据筛选
    st.sidebar.header("数据筛选")

    # 1. 指标选择器
    metric_types = sorted(df['metric_type'].unique())

    # 检查是否有指标
    if len(metric_types) == 0:
        st.error("未检测到任何指标数据")
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
        st.sidebar.info("当前显示所有ETF的总和")
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

    # 默认结束日期为当前日期（但不超过数据的最大日期）
    from datetime import date
    today = date.today()
    default_end_date = min(today, max_date)

    # 检查是否只有一个日期
    if min_date == max_date:
        st.sidebar.info(f"当前数据日期: {min_date}")
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
    st.sidebar.header("图表设置")
    chart_type = st.sidebar.radio(
        "图表类型",
        options=['line', 'area', 'scatter'],
        format_func=lambda x: {'line': '平滑曲线', 'area': '面积图', 'scatter': '散点图'}[x],
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
        st.warning("所选条件下没有数据，请调整筛选条件")
        st.stop()

    # 确定是否为汇总模式
    is_aggregate = has_aggregate and contains_total_market_value

    # 验证ETF选择（非汇总模式）
    if not is_aggregate and (selected_etfs is None or len(selected_etfs) == 0):
        st.info("请至少选择一个ETF")
        st.stop()

    # 创建并显示图表
    fig = create_line_chart(filtered_df, selected_metric, is_aggregate, selected_etfs, chart_type)
    st.plotly_chart(fig, use_container_width=True)

    # 显示统计信息
    st.subheader("统计信息")
    stats_df = calculate_statistics(filtered_df, is_aggregate, selected_etfs)

    if len(stats_df) > 0:
        st.dataframe(
            stats_df,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("没有可显示的统计信息")


if __name__ == "__main__":
    main()
