import logging
from datetime import datetime, timedelta
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


@st.cache_data(ttl=1200)
def load_stock_announcements(ts_code: str, notice_type: str = "全部", days: int = 180) -> pd.DataFrame:
    try:
        import akshare as ak  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"akshare import failed: {exc}") from exc

    symbol = str(ts_code or "").strip().upper().split(".", 1)[0]
    lookback_days = max(30, min(720, int(days or 180)))
    end_date = datetime.now().strftime("%Y%m%d")
    begin_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y%m%d")
    df = ak.stock_individual_notice_report(
        security=symbol,
        symbol=str(notice_type or "全部").strip() or "全部",
        begin_date=begin_date,
        end_date=end_date,
    )
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "公告日期" in out.columns:
        out["公告日期"] = pd.to_datetime(out["公告日期"], errors="coerce")
        out = out.sort_values("公告日期", ascending=False)
        out["公告日期"] = out["公告日期"].dt.strftime("%Y-%m-%d")
    return out.reset_index(drop=True)

