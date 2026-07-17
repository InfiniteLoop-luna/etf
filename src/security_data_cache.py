import logging
from datetime import datetime, timedelta
from typing import Any
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


def _build_data_block(
    name: str,
    *,
    source: str,
    items: list[dict[str, Any]] | None = None,
    error: str | None = None,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    safe_items = items or []
    return {
        "name": name,
        "source": source,
        "status": "ok" if safe_items else ("failed" if error else "empty"),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "row_count": len(safe_items),
        "items": safe_items,
        "error": str(error or "")[:500] or None,
        "meta": meta or {},
    }


def _records_from_dataframe(df: pd.DataFrame, limit: int = 20) -> list[dict[str, Any]]:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return []
    out = df.head(limit).copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            out[col] = out[col].astype("string").fillna("").astype(str).str.strip()
    return out.to_dict(orient="records")


def _load_stock_news_direct(ts_code: str, limit: int = 8) -> dict[str, Any]:
    try:
        import akshare as ak  # type: ignore
    except Exception as exc:
        return _build_data_block("news", source="akshare.stock_news_em", error=f"akshare import failed: {exc}")

    symbol = str(ts_code or "").strip().upper().split(".", 1)[0]
    previous_string_storage = pd.options.mode.string_storage
    try:
        # Work around ArrowString replace incompatibility inside akshare.stock_news_em.
        pd.options.mode.string_storage = "python"
        df = ak.stock_news_em(symbol=symbol)
        return _build_data_block(
            "news",
            source="akshare.stock_news_em",
            items=_records_from_dataframe(df, limit=limit),
            meta={"symbol": symbol},
        )
    except Exception as exc:
        return _build_data_block("news", source="akshare.stock_news_em", error=str(exc), meta={"symbol": symbol})
    finally:
        pd.options.mode.string_storage = previous_string_storage


def _load_stock_report_direct(ts_code: str, limit: int = 6) -> dict[str, Any]:
    try:
        import akshare as ak  # type: ignore
    except Exception as exc:
        return _build_data_block(
            "research_reports",
            source="akshare.stock_research_report_em",
            error=f"akshare import failed: {exc}",
        )

    symbol = str(ts_code or "").strip().upper().split(".", 1)[0]
    try:
        df = ak.stock_research_report_em(symbol=symbol)
        return _build_data_block(
            "research_reports",
            source="akshare.stock_research_report_em",
            items=_records_from_dataframe(df, limit=limit),
            meta={"symbol": symbol},
        )
    except Exception as exc:
        return _build_data_block(
            "research_reports",
            source="akshare.stock_research_report_em",
            error=str(exc),
            meta={"symbol": symbol},
        )


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
    supplemental = build_stock_research_supplemental(
        ts_code,
        stock_name=stock_name,
        industry=industry,
        enabled=True,
        config=forced_cfg,
    )
    news_block = supplemental.get("news") if isinstance(supplemental, dict) else None
    if isinstance(news_block, dict) and str(news_block.get("status") or "") == "failed":
        supplemental["news"] = _load_stock_news_direct(ts_code, limit=cfg.news_limit)

    report_block = supplemental.get("research_reports") if isinstance(supplemental, dict) else None
    if not isinstance(report_block, dict) or str(report_block.get("status") or "") in {"failed", "empty"}:
        supplemental["research_reports"] = _load_stock_report_direct(ts_code, limit=cfg.research_report_limit)

    return supplemental


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


@st.cache_data(ttl=1200)
def load_stock_event_stream(ts_code: str, *, stock_name: str = "", industry: str = "", days: int = 180) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    notice_df = load_stock_announcements(ts_code, notice_type="全部", days=days)
    if notice_df is not None and not notice_df.empty:
        for item in notice_df.to_dict(orient="records"):
            rows.append(
                {
                    "日期": item.get("公告日期"),
                    "类型": "公告",
                    "子类型": item.get("公告类型") or "",
                    "标题": item.get("公告标题") or "",
                    "来源": "东方财富",
                    "机构": "",
                    "评级": "",
                    "链接": item.get("网址") or "",
                }
            )

    supplemental = load_stock_news_and_reports(ts_code, stock_name=stock_name, industry=industry)
    news_items = ((supplemental or {}).get("news") or {}).get("items") or []
    for item in news_items:
        rows.append(
            {
                "日期": item.get("发布时间") or item.get("日期") or item.get("时间") or "",
                "类型": "新闻",
                "子类型": item.get("文章来源") or "",
                "标题": item.get("新闻标题") or item.get("标题") or "",
                "来源": item.get("文章来源") or "东方财富",
                "机构": "",
                "评级": "",
                "链接": item.get("新闻链接") or item.get("链接") or "",
            }
        )

    report_items = ((supplemental or {}).get("research_reports") or {}).get("items") or []
    for item in report_items:
        rows.append(
            {
                "日期": item.get("日期") or item.get("发布日期") or item.get("报告日期") or "",
                "类型": "研报",
                "子类型": "机构研报",
                "标题": item.get("报告名称") or item.get("标题") or "",
                "来源": "东方财富",
                "机构": item.get("机构") or "",
                "评级": item.get("东财评级") or item.get("评级") or "",
                "链接": item.get("报告PDF链接") or item.get("链接") or "",
            }
        )

    if not rows:
        return pd.DataFrame(columns=["日期", "类型", "子类型", "标题", "来源", "机构", "评级", "链接"])

    out = pd.DataFrame(rows)
    out["排序时间"] = pd.to_datetime(out["日期"], errors="coerce")
    out = out.sort_values("排序时间", ascending=False, na_position="last").drop(columns=["排序时间"])
    out["日期"] = out["日期"].astype("string").fillna("").astype(str).str.slice(0, 19)
    return out.reset_index(drop=True)

