# -*- coding: utf-8 -*-
"""ETF份额变动可视化 - Streamlit Web应用"""

import streamlit as st
import pandas as pd
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


# 主应用
def main():
    """主应用逻辑"""
    st.title("ETF份额变动可视化")

    # 加载数据
    df = load_data(DATA_FILE)

    # 侧边栏 - 数据筛选
    st.sidebar.header("🔍 数据筛选")

    # 1. 指标选择器
    metric_types = sorted(df['metric_type'].unique())
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
    contains_total_market_value = '总市值' in selected_metric

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

    date_range = st.sidebar.slider(
        "选择日期范围",
        min_value=min_date,
        max_value=max_date,
        value=(min_date, max_date),
        format="YYYY-MM-DD"
    )

    # 主区域 - 显示选择的信息（占位符）
    st.header("数据概览")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("选择的指标", selected_metric)

    with col2:
        date_range_str = f"{date_range[0]} 至 {date_range[1]}"
        st.metric("日期范围", date_range_str)

    with col3:
        if selected_etfs is not None:
            st.metric("选择的ETF数量", len(selected_etfs))
        else:
            st.metric("显示模式", "汇总数据")

    # 显示筛选后的数据信息
    st.subheader("筛选条件")
    st.write(f"**指标**: {selected_metric}")
    st.write(f"**日期范围**: {date_range[0]} 至 {date_range[1]}")
    if selected_etfs is not None:
        st.write(f"**选择的ETF**: {', '.join(selected_etfs) if selected_etfs else '未选择'}")
    else:
        st.write(f"**显示模式**: 所有ETF的总和")


if __name__ == "__main__":
    main()
