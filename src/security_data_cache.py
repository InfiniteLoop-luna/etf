import logging
from typing import List

import pandas as pd
import streamlit as st

from src.etf_stats import (
    get_security_financial_timeseries,
    get_security_kline_timeseries,
    get_security_profile,
    get_security_timeseries,
    get_stock_basic_summary,
    get_stock_holder_number_timeseries,
    search_security,
)
from src.stock_research_akshare_enrichment import (
    StockResearchAkshareConfig,
    build_stock_research_supplemental,
    load_stock_research_akshare_config,
)

logger = logging.getLogger(__name__)


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


@st.cache_data(ttl=1200)
def load_stock_news_and_reports(ts_code: str, *, stock_name: str = "", industry: str = "") -> dict[str, dict]:
    cfg = load_stock_research_akshare_config()
    forced_cfg = StockResearchAkshareConfig(
        enabled=True,
        business_limit=0,
        news_limit=cfg.news_limit,
        research_report_limit=cfg.research_report_limit,
        money_flow_limit=0,
        lhb_limit=0,
        industry_peer_limit=0,
    )
    return build_stock_research_supplemental(
        ts_code,
        stock_name=stock_name,
        industry=industry,
        enabled=True,
        config=forced_cfg,
    )

