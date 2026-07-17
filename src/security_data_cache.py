import logging
import math
from datetime import datetime, timedelta
from typing import Any
from typing import List
from urllib.parse import quote

import pandas as pd
import requests
import streamlit as st
from sqlalchemy import text

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


@st.cache_data(ttl=300, show_spinner=False)
def load_fund_search(keyword: str, limit: int = 20) -> pd.DataFrame:
    from src.fund_hot_stocks import get_engine as get_fund_hot_engine
    from src.fund_hot_stocks import search_funds

    engine = get_fund_hot_engine()
    return search_funds(keyword=keyword, limit=int(limit), engine=engine)


@st.cache_data(ttl=600, show_spinner=False)
def load_fund_object_model(fund_code: str, period: str = "", top_n: int = 10) -> dict[str, Any]:
    from src.fund_estimate_snapshot_store import (
        ensure_fund_estimate_snapshot_table,
        get_fund_estimate_snapshot,
        get_latest_fund_estimate_snapshot,
    )
    from src.fund_hot_stocks import get_engine as get_fund_hot_engine
    from src.fund_hot_stocks import query_fund_preference_snapshot, search_funds
    from src.fund_nav import fetch_latest_fund_nav_snapshot
    from src.fund_watchlist_dashboard import (
        attach_latest_closing_estimate,
        build_fund_watchlist_item,
    )

    normalized_code = str(fund_code or "").strip().upper()
    if not normalized_code:
        return {
            "item": {},
            "meta": {},
            "holdings": pd.DataFrame(),
            "errors": ["基金代码不能为空"],
            "nav_snapshot": {},
            "estimate_snapshot": {},
            "latest_estimate_snapshot": {},
        }

    engine = get_fund_hot_engine()
    target_period = str(period or "").strip()
    meta_df = pd.DataFrame()
    holding_df = pd.DataFrame()
    nav_snapshot: dict[str, Any] = {}
    estimate_snapshot: dict[str, Any] = {}
    latest_estimate_snapshot: dict[str, Any] = {}
    errors: list[str] = []
    estimate_store_ready = False

    try:
        ensure_fund_estimate_snapshot_table(engine)
        estimate_store_ready = True
    except Exception as exc:
        logger.warning("fund object estimate snapshot store unavailable: %s", exc)
        errors.append("15:00估值快照暂不可用")

    try:
        meta_df = search_funds(normalized_code, limit=5, engine=engine)
    except Exception as exc:
        logger.warning("fund object metadata load failed for %s: %s", normalized_code, exc)
        errors.append(f"基础信息读取失败：{exc}")

    try:
        holding_df = query_fund_preference_snapshot(
            fund_code=normalized_code,
            period=target_period or None,
            top_n=int(top_n),
            engine=engine,
        )
    except Exception as exc:
        logger.warning("fund object holdings load failed for %s: %s", normalized_code, exc)
        errors.append(f"持仓读取失败：{exc}")

    try:
        nav_snapshot = fetch_latest_fund_nav_snapshot(normalized_code)
    except Exception as exc:
        logger.warning("fund object nav load failed for %s: %s", normalized_code, exc)
        errors.append(f"净值读取失败：{exc}")

    nav_date = pd.to_datetime(nav_snapshot.get("nav_date"), errors="coerce")
    if estimate_store_ready and not pd.isna(nav_date):
        try:
            estimate_snapshot = get_fund_estimate_snapshot(
                engine,
                normalized_code,
                nav_date,
                ensure_table=False,
            )
        except Exception as exc:
            logger.warning(
                "fund object estimate snapshot load failed for %s / %s: %s",
                normalized_code,
                nav_date.date(),
                exc,
            )
            errors.append("15:00估值快照读取失败")

    if estimate_store_ready:
        try:
            latest_estimate_snapshot = get_latest_fund_estimate_snapshot(
                engine,
                normalized_code,
                ensure_table=False,
            )
        except Exception as exc:
            logger.warning("fund object latest estimate snapshot load failed for %s: %s", normalized_code, exc)
            errors.append("最近估值快照读取失败")

    meta_row = meta_df.iloc[0].to_dict() if meta_df is not None and not meta_df.empty else {}
    watchlist_row = pd.Series(
        {
            "ts_code": normalized_code,
            "security_name": meta_row.get("name") or normalized_code,
            "created_at": pd.NaT,
        }
    )
    item = build_fund_watchlist_item(
        watchlist_row,
        meta_df,
        holding_df,
        nav_snapshot=nav_snapshot,
        estimate_snapshot=estimate_snapshot,
        load_error="；".join(errors),
    )
    item = attach_latest_closing_estimate(item, latest_estimate_snapshot)
    return {
        "item": item,
        "meta": meta_row,
        "holdings": holding_df,
        "errors": errors,
        "nav_snapshot": nav_snapshot,
        "estimate_snapshot": estimate_snapshot,
        "latest_estimate_snapshot": latest_estimate_snapshot,
    }


@st.cache_data(ttl=900, show_spinner=False)
def load_fund_peer_comparison(
    fund_code: str,
    *,
    fund_type: str = "",
    management: str = "",
    limit: int = 6,
) -> pd.DataFrame:
    from src.fund_hot_stocks import get_engine as get_fund_hot_engine

    normalized_code = str(fund_code or "").strip().upper()
    normalized_type = str(fund_type or "").strip()
    normalized_management = str(management or "").strip()
    if not normalized_code:
        return pd.DataFrame()

    engine = get_fund_hot_engine()
    peer_slots = max(1, int(limit or 6) - 1)
    candidate_df = pd.read_sql(
        text(
            """
        WITH latest_portfolio AS (
            SELECT
                fund_code,
                MAX(end_date) AS latest_end_date
            FROM vw_fund_portfolio
            GROUP BY fund_code
        ),
        peers AS (
            SELECT
                fb.fund_code,
                COALESCE(NULLIF(fb.name, ''), fb.fund_code) AS name,
                COALESCE(NULLIF(fb.management, ''), '持仓表补全') AS management,
                COALESCE(NULLIF(fb.fund_type, ''), NULLIF(fb.invest_type, ''), '未知类型') AS fund_type,
                fb.issue_amount,
                lp.latest_end_date,
                CASE
                    WHEN :management <> ''
                     AND COALESCE(NULLIF(fb.management, ''), '') = :management
                     AND :fund_type <> ''
                     AND COALESCE(NULLIF(fb.fund_type, ''), NULLIF(fb.invest_type, ''), '') = :fund_type
                    THEN 0
                    WHEN :management <> ''
                     AND COALESCE(NULLIF(fb.management, ''), '') = :management
                    THEN 1
                    WHEN :fund_type <> ''
                     AND COALESCE(NULLIF(fb.fund_type, ''), NULLIF(fb.invest_type, ''), '') = :fund_type
                    THEN 2
                    ELSE 9
                END AS compare_rank,
                CASE
                    WHEN :management <> ''
                     AND COALESCE(NULLIF(fb.management, ''), '') = :management
                     AND :fund_type <> ''
                     AND COALESCE(NULLIF(fb.fund_type, ''), NULLIF(fb.invest_type, ''), '') = :fund_type
                    THEN '同管理人 + 同类型'
                    WHEN :management <> ''
                     AND COALESCE(NULLIF(fb.management, ''), '') = :management
                    THEN '同管理人'
                    WHEN :fund_type <> ''
                     AND COALESCE(NULLIF(fb.fund_type, ''), NULLIF(fb.invest_type, ''), '') = :fund_type
                    THEN '同类型'
                    ELSE '候选基金'
                END AS compare_reason
            FROM vw_fund_basic fb
            LEFT JOIN latest_portfolio lp ON lp.fund_code = fb.fund_code
            WHERE UPPER(TRIM(fb.fund_code)) <> :fund_code
              AND (
                    (:management <> '' AND COALESCE(NULLIF(fb.management, ''), '') = :management)
                 OR (:fund_type <> '' AND COALESCE(NULLIF(fb.fund_type, ''), NULLIF(fb.invest_type, ''), '') = :fund_type)
              )
        )
        SELECT fund_code, name, management, fund_type, issue_amount, latest_end_date, compare_reason
        FROM peers
        WHERE compare_rank < 9
        ORDER BY compare_rank, issue_amount DESC NULLS LAST, latest_end_date DESC NULLS LAST, fund_code
        LIMIT :peer_slots
        """
        ),
        engine,
        params={
            "fund_code": normalized_code,
            "fund_type": normalized_type,
            "management": normalized_management,
            "peer_slots": peer_slots,
        },
    )

    rows: list[dict[str, Any]] = []
    self_payload = load_fund_object_model(normalized_code, top_n=10)
    self_item = self_payload.get("item") or {}
    self_latest_estimate = self_item.get("closing_estimate_pct")
    if self_latest_estimate is None:
        self_latest_estimate = self_item.get("latest_closing_estimate_pct")
    rows.append(
        {
            "标记": "当前基金",
            "基金名称": self_item.get("fund_name") or normalized_code,
            "基金代码": normalized_code,
            "比较来源": "当前基金",
            "管理人": self_item.get("management") or "-",
            "基金类型": self_item.get("fund_type") or "-",
            "基金规模(亿份)": self_item.get("issue_amount"),
            "净值日期": _format_timestamp_like(self_item.get("nav_date")),
            "单位净值": self_item.get("unit_nav"),
            "日涨跌幅(%)": self_item.get("daily_change_pct"),
            "15:00估值(%)": self_latest_estimate,
            "估值偏差(百分点)": self_item.get("estimate_deviation_pct"),
            "Top10 集中度(%)": self_item.get("top10_ratio"),
            "最近披露期": _format_timestamp_like(self_item.get("latest_end_date")),
            "基金详情": (
                f"?security_query={quote(normalized_code)}"
                f"&security_type=fund"
                f"&open_tab=fund_object"
                f"&jump_nonce=fund-peer-{quote(normalized_code)}"
            ),
        }
    )

    for candidate in candidate_df.to_dict(orient="records"):
        code = str(candidate.get("fund_code") or "").strip().upper()
        if not code:
            continue
        payload = load_fund_object_model(code, top_n=10)
        item = payload.get("item") or {}
        latest_estimate = item.get("closing_estimate_pct")
        if latest_estimate is None:
            latest_estimate = item.get("latest_closing_estimate_pct")
        rows.append(
            {
                "标记": "对比基金",
                "基金名称": item.get("fund_name") or candidate.get("name") or code,
                "基金代码": code,
                "比较来源": candidate.get("compare_reason") or "同类型",
                "管理人": item.get("management") or candidate.get("management") or "-",
                "基金类型": item.get("fund_type") or candidate.get("fund_type") or "-",
                "基金规模(亿份)": item.get("issue_amount"),
                "净值日期": _format_timestamp_like(item.get("nav_date")),
                "单位净值": item.get("unit_nav"),
                "日涨跌幅(%)": item.get("daily_change_pct"),
                "15:00估值(%)": latest_estimate,
                "估值偏差(百分点)": item.get("estimate_deviation_pct"),
                "Top10 集中度(%)": item.get("top10_ratio"),
                "最近披露期": _format_timestamp_like(item.get("latest_end_date")),
                "基金详情": (
                    f"?security_query={quote(code)}"
                    f"&security_type=fund"
                    f"&open_tab=fund_object"
                    f"&jump_nonce=fund-peer-{quote(code)}"
                ),
            }
        )

    return pd.DataFrame(rows)


def _format_timestamp_like(value) -> str:
    raw = pd.to_datetime(value, errors="coerce")
    if pd.isna(raw):
        return "-"
    return pd.Timestamp(raw).strftime("%Y-%m-%d")


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
    symbol = str(ts_code or "").strip().upper().split(".", 1)[0]
    lookback_days = max(30, min(720, int(days or 180)))
    end_date = datetime.now().strftime("%Y%m%d")
    begin_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y%m%d")
    begin_text = f"{begin_date[:4]}-{begin_date[4:6]}-{begin_date[6:]}"
    end_text = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"

    def _fetch_with_builtin_akshare() -> pd.DataFrame:
        import akshare as ak  # type: ignore

        if hasattr(ak, "stock_individual_notice_report"):
            return ak.stock_individual_notice_report(
                security=symbol,
                symbol=str(notice_type or "全部").strip() or "全部",
                begin_date=begin_text,
                end_date=end_text,
            )

        report_map = {
            "全部": "0",
            "财务报告": "1",
            "融资公告": "2",
            "风险提示": "3",
            "信息变更": "4",
            "重大事项": "5",
            "资产重组": "6",
            "持股变动": "7",
        }
        params = {
            "sr": "-1",
            "page_size": "100",
            "page_index": "1",
            "ann_type": "A",
            "client_source": "web",
            "f_node": report_map.get(str(notice_type or "全部").strip() or "全部", "0"),
            "s_node": "0",
            "stock_list": symbol,
            "begin_time": begin_text,
            "end_time": end_text,
        }
        url = "https://np-anotice-stock.eastmoney.com/api/security/ann"
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        payload = response.json() or {}
        data = payload.get("data") or {}
        total_hits = int(data.get("total_hits") or 0)
        total_page = max(1, math.ceil(total_hits / 100)) if total_hits else 1
        frames: list[pd.DataFrame] = []
        for page in range(1, total_page + 1):
            params["page_index"] = str(page)
            page_response = requests.get(url, params=params, timeout=15)
            page_response.raise_for_status()
            page_payload = page_response.json() or {}
            page_list = ((page_payload.get("data") or {}).get("list") or [])
            if not page_list:
                continue

            row_frame = pd.DataFrame(page_list)
            code_rows = []
            column_rows = []
            for item in page_list:
                codes = item.get("codes") or []
                matched_code = None
                for code_item in codes:
                    if str(code_item.get("stock_code") or "") == symbol:
                        matched_code = code_item
                        break
                if matched_code is None and codes:
                    matched_code = codes[0]
                code_rows.append(matched_code or {})

                columns = item.get("columns") or []
                column_rows.append(columns[0] if columns else {})

            code_frame = pd.DataFrame(code_rows)
            column_frame = pd.DataFrame(column_rows)
            for col in ("codes", "columns"):
                if col in row_frame.columns:
                    del row_frame[col]
            merged = pd.concat([row_frame, column_frame, code_frame], axis=1)
            frames.append(merged)

        if not frames:
            return pd.DataFrame()

        out = pd.concat(frames, ignore_index=True)
        out.rename(
            columns={
                "art_code": "编码",
                "notice_date": "公告日期",
                "title": "公告标题",
                "column_name": "公告类型",
                "short_name": "名称",
                "stock_code": "代码",
            },
            inplace=True,
        )
        if "编码" in out.columns and "代码" in out.columns:
            out["网址"] = "https://data.eastmoney.com/notices/detail/" + out["代码"].astype(str) + "/" + out["编码"].astype(str) + ".html"
        keep_cols = [c for c in ["代码", "名称", "公告标题", "公告类型", "公告日期", "网址"] if c in out.columns]
        return out[keep_cols]

    try:
        df = _fetch_with_builtin_akshare()
    except Exception as exc:
        logger.warning("load_stock_announcements fallback fetch failed: %s", exc, exc_info=True)
        raise RuntimeError(f"stock announcement fetch failed: {exc}") from exc

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
    out["排序时间"] = pd.to_datetime(out["日期"], errors="coerce", format="mixed")
    out = out.sort_values("排序时间", ascending=False, na_position="last").drop(columns=["排序时间"])
    out["日期"] = out["日期"].astype("string").fillna("").astype(str).str.slice(0, 19)
    return out.reset_index(drop=True)

