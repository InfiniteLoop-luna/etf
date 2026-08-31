from __future__ import annotations

import hashlib
import json
import logging
import re
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import text

from src.distribution_llm_analysis import make_json_safe, parse_llm_json_object
from src.fund_hot_stocks import get_engine as get_fund_engine
from src.stock_research_llm_analysis import load_stock_research_llm_config

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = PROJECT_ROOT / "data" / "morning_reports"
REPORT_SCHEMA_VERSION = "etf-morning-report-v2"
LLM_SCHEMA_VERSION = "etf-morning-report-llm-v2"
BEIJING_TZ = ZoneInfo("Asia/Shanghai")

SOURCE_DEFINITIONS = (
    {"key": "stock_daily", "label": "A股日行情", "table": "ts_stock_daily", "required": True},
    {"key": "etf_share", "label": "ETF份额规模", "table": "etf_share_size", "required": True},
    {"key": "limit_sentiment", "label": "涨跌停与炸板", "table": "ts_limit_list_d", "required": True},
    {"key": "ths_industry_flow", "label": "THS行业资金流", "table": "ts_moneyflow_ind_ths", "required": False},
    {"key": "dc_industry_flow", "label": "DC板块资金流", "table": "ts_moneyflow_dc_ind", "required": False},
    {"key": "margin", "label": "融资融券", "table": "ts_margin", "required": False},
    {"key": "northbound", "label": "沪深港通资金流", "table": "ts_moneyflow_hsgt", "required": False},
    {"key": "dragon_tiger", "label": "龙虎榜", "table": "ts_lhb_top_list", "required": False},
)


def _safe_date(value: Any) -> str | None:
    parsed = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(parsed) else pd.Timestamp(parsed).strftime("%Y-%m-%d")


def _query_frame(engine, sql: str, params: dict | None = None) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(sql), conn, params=params or {})
    except Exception as exc:
        logger.warning("morning report query failed: %s", exc)
        return pd.DataFrame()


def _latest_date(engine, table_name: str, column: str = "trade_date") -> str | None:
    frame = _query_frame(engine, f"SELECT MAX({column}) AS latest_date FROM {table_name}")
    return _safe_date(frame.iloc[0]["latest_date"]) if not frame.empty else None


def find_previous_trade_date(engine, *, today: date | None = None) -> str | None:
    """Resolve T-1 from the canonical A-share daily table.

    The previous implementation selected the maximum date from any source,
    which allowed one leading optional dataset to move the entire report date
    forward.  ETF share data is retained as a fallback for deployments where
    the stock landing table is temporarily unavailable.
    """
    upper = (today or datetime.now(BEIJING_TZ).date()) - timedelta(days=1)
    for table in ("ts_stock_daily", "etf_share_size"):
        value = _query_frame(
            engine,
            f"SELECT MAX(trade_date) AS latest_date FROM {table} WHERE trade_date <= :upper",
            {"upper": upper},
        )
        parsed = _safe_date(value.iloc[0]["latest_date"]) if not value.empty else None
        if parsed:
            return parsed
    return None


def _source_status(engine, definition: dict, target: str) -> dict:
    table = str(definition["table"])
    frame = _query_frame(
        engine,
        f"""
        SELECT MAX(trade_date) FILTER (WHERE trade_date <= :trade_date) AS latest_date,
               COUNT(*) FILTER (WHERE trade_date = :trade_date) AS row_count
        FROM {table}
        """,
        {"trade_date": target},
    )
    latest_date = None
    row_count = 0
    if not frame.empty:
        latest_date = _safe_date(frame.iloc[0].get("latest_date"))
        parsed_count = pd.to_numeric(frame.iloc[0].get("row_count"), errors="coerce")
        row_count = 0 if pd.isna(parsed_count) else int(parsed_count)
    if latest_date == target and row_count > 0:
        status = "fresh"
    elif latest_date:
        status = "stale"
    else:
        status = "missing"
    return {
        "key": definition["key"],
        "label": definition["label"],
        "table": table,
        "required": bool(definition.get("required")),
        "target_date": target,
        "latest_date": latest_date,
        "row_count": row_count,
        "status": status,
    }


def build_source_readiness(engine, target: str) -> dict:
    sources = [_source_status(engine, definition, target) for definition in SOURCE_DEFINITIONS]
    required = [source for source in sources if source["required"]]
    fresh_required = [source for source in required if source["status"] == "fresh"]
    fresh_all = [source for source in sources if source["status"] == "fresh"]
    coverage_score = round(len(fresh_all) / len(sources) * 100) if sources else 0
    return {
        "report_status": "complete" if len(fresh_required) == len(required) else "partial",
        "coverage_score": coverage_score,
        "required_ready": len(fresh_required),
        "required_total": len(required),
        "sources": sources,
    }


def _summarize_rows(frame: pd.DataFrame, columns: list[str], limit: int = 10) -> list[dict]:
    if frame is None or frame.empty:
        return []
    out = frame.copy().head(limit)
    return out[[column for column in columns if column in out.columns]].to_dict(orient="records")


def collect_fact_pack(trade_date: str | None = None, engine=None) -> dict:
    engine = engine or get_fund_engine()
    target = _safe_date(trade_date) if trade_date else find_previous_trade_date(engine)
    warnings: list[str] = []
    if not target:
        missing_date_warning = "报告交易日格式无效" if trade_date else "未找到上一交易日"
        return {
            "schema_version": REPORT_SCHEMA_VERSION,
            "report_trade_date": None,
            "generated_at": datetime.now(BEIJING_TZ).isoformat(timespec="seconds"),
            "data_quality": {
                "report_status": "partial",
                "coverage_score": 0,
                "warnings": [missing_date_warning],
                "sources": [],
            },
        }

    readiness = build_source_readiness(engine, target)
    for source in readiness["sources"]:
        if source["status"] == "fresh":
            continue
        prefix = "关键数据源" if source["required"] else "辅助数据源"
        if source["latest_date"]:
            warnings.append(
                f"{prefix} {source['label']} 未更新至 {target}，最近日期为 {source['latest_date']}"
            )
        else:
            warnings.append(f"{prefix} {source['label']} 在 {target} 前无可用数据")

    etf = _query_frame(
        engine,
        """
        SELECT s.trade_date, e.primary_category, e.secondary_category,
               SUM(COALESCE(s.total_share, 0)) AS total_share_size,
               SUM(COALESCE(s.total_share, 0)) / 10000.0 AS total_share_yi,
               SUM(COALESCE(s.total_size, 0)) AS total_size,
               SUM(COALESCE(s.total_size, 0)) / 10000.0 AS total_size_yi
        FROM etf_share_size s
        LEFT JOIN etf_summary e ON e.fund_trade_code = s.ts_code
        WHERE s.trade_date = (
            SELECT MAX(trade_date) FROM etf_share_size WHERE trade_date <= :trade_date
        )
        GROUP BY s.trade_date, e.primary_category, e.secondary_category
        ORDER BY total_share_size DESC
        LIMIT 20
        """,
        {"trade_date": target},
    )
    if etf.empty:
        warnings.append("ETF份额数据缺失")
    else:
        etf_source_date = _safe_date(etf.iloc[0].get("trade_date"))
        if etf_source_date and etf_source_date != target:
            warnings.append(f"ETF份额数据采用最近可用日期：{etf_source_date}")

    industry_etf_growth = _query_frame(
        engine,
        """
        WITH available_dates AS (
            SELECT DISTINCT trade_date
            FROM etf_share_size
            WHERE trade_date <= :trade_date
            ORDER BY trade_date DESC
            LIMIT 2
        ), ranked AS (
            SELECT s.trade_date, s.ts_code, s.etf_name, s.total_share, s.total_size,
                   COALESCE(NULLIF(e.index_name, ''), NULLIF(e.etf_expanded_name, ''), '未识别行业') AS industry,
                   COALESCE(NULLIF(e.fund_name_cn, ''), NULLIF(s.etf_name, ''), s.ts_code) AS industry_etf,
                   ROW_NUMBER() OVER (PARTITION BY s.ts_code ORDER BY s.trade_date DESC) AS rn
            FROM etf_share_size s
            JOIN available_dates d ON d.trade_date = s.trade_date
            LEFT JOIN etf_summary e ON e.fund_trade_code = s.ts_code
            WHERE e.primary_category = '指数'
              AND e.secondary_category = '行业&其他'
        )
        SELECT ts_code, etf_name, industry, industry_etf,
               MAX(trade_date) FILTER (WHERE rn = 1) AS current_date,
               MAX(trade_date) FILTER (WHERE rn = 2) AS previous_date,
               MAX(total_share) FILTER (WHERE rn = 1) AS current_share,
               MAX(total_share) FILTER (WHERE rn = 2) AS previous_share,
               MAX(total_size) FILTER (WHERE rn = 1) AS current_size,
               CASE WHEN MAX(total_share) FILTER (WHERE rn = 2) IS NULL
                           OR MAX(total_share) FILTER (WHERE rn = 2) = 0 THEN NULL
                    ELSE (
                        MAX(total_share) FILTER (WHERE rn = 1)
                        - MAX(total_share) FILTER (WHERE rn = 2)
                    ) / MAX(total_share) FILTER (WHERE rn = 2) * 100 END AS share_growth_pct,
               MAX(total_share) FILTER (WHERE rn = 1)
               - COALESCE(MAX(total_share) FILTER (WHERE rn = 2), 0) AS share_change
        FROM ranked
        GROUP BY ts_code, etf_name, industry, industry_etf
        HAVING MAX(total_share) FILTER (WHERE rn = 1) IS NOT NULL
        ORDER BY industry, share_growth_pct DESC NULLS LAST, current_share DESC
        """,
        {"trade_date": target},
    )
    if industry_etf_growth.empty:
        warnings.append("行业ETF份额增长数据缺失")

    ths = _query_frame(
        engine,
        """
        SELECT payload->>'industry' AS industry,
               (payload->>'net_amount')::numeric AS net_amount,
               (payload->>'net_amount')::numeric AS net_amount_yi,
               (payload->>'pct_change')::numeric AS pct_change,
               payload->>'lead_stock' AS lead_stock
        FROM ts_moneyflow_ind_ths
        WHERE trade_date = :trade_date
        ORDER BY (payload->>'net_amount')::numeric DESC NULLS LAST
        LIMIT 20
        """,
        {"trade_date": target},
    )
    dc = _query_frame(
        engine,
        """
        SELECT payload->>'name' AS industry,
               (payload->>'net_amount')::numeric AS net_amount,
               (payload->>'net_amount')::numeric / 100000000.0 AS net_amount_yi,
               (payload->>'pct_change')::numeric AS pct_change
        FROM ts_moneyflow_dc_ind
        WHERE trade_date = :trade_date
        ORDER BY (payload->>'net_amount')::numeric DESC NULLS LAST
        LIMIT 20
        """,
        {"trade_date": target},
    )
    volume = _query_frame(
        engine,
        """
        SELECT trade_date,
               SUM((payload->>'amount')::numeric) AS total_amount,
               SUM((payload->>'amount')::numeric) / 100000.0 AS total_amount_yi,
               SUM((payload->>'vol')::numeric) AS total_volume
        FROM ts_stock_daily
        WHERE trade_date = :trade_date
          AND (payload->>'amount') IS NOT NULL
        GROUP BY trade_date
        """,
        {"trade_date": target},
    )
    margin = _query_frame(
        engine,
        """
        SELECT trade_date, SUM((payload->>'rzmre')::numeric) AS financing_buy,
               SUM((payload->>'rzche')::numeric) AS financing_repay,
               SUM((payload->>'rzye')::numeric) AS financing_balance,
               SUM((payload->>'rzmre')::numeric) / 100000000.0 AS financing_buy_yi,
               SUM((payload->>'rzche')::numeric) / 100000000.0 AS financing_repay_yi,
               SUM((payload->>'rzye')::numeric) / 100000000.0 AS financing_balance_yi
        FROM ts_margin
        WHERE trade_date = :trade_date
        GROUP BY trade_date
        """,
        {"trade_date": target},
    )

    sentiment = _query_frame(
        engine,
        """
        SELECT
          COUNT(*) FILTER (WHERE UPPER(COALESCE(payload->>'limit', '')) = 'U') AS up_cnt,
          COUNT(*) FILTER (WHERE UPPER(COALESCE(payload->>'limit', '')) = 'Z') AS zha_cnt,
          COUNT(*) AS total_cnt
        FROM ts_limit_list_d
        WHERE trade_date = :trade_date
        """,
        {"trade_date": target},
    )
    if sentiment.empty:
        warnings.append("涨停情绪数据缺失")

    northbound = _query_frame(
        engine,
        """
        SELECT trade_date, (payload->>'north_money')::numeric AS north_money,
               (payload->>'north_money')::numeric / 100.0 AS north_money_yi,
               (payload->>'hgt')::numeric AS hgt,
               (payload->>'hgt')::numeric / 100.0 AS hgt_yi,
               (payload->>'sgt')::numeric AS sgt,
               (payload->>'sgt')::numeric / 100.0 AS sgt_yi
        FROM ts_moneyflow_hsgt
        WHERE trade_date = :trade_date
        """,
        {"trade_date": target},
    )
    if northbound.empty:
        warnings.append("北向资金数据缺失")

    lhb = _query_frame(
        engine,
        """
        SELECT COUNT(*) AS stock_count,
               COUNT(DISTINCT ts_code) AS distinct_stock_count
        FROM ts_lhb_top_list
        WHERE trade_date = :trade_date
        """,
        {"trade_date": target},
    )
    if lhb.empty:
        warnings.append("龙虎榜数据缺失")

    trend_payload: dict = {}
    try:
        from src.trend_reco_store import fetch_trend_reco_payload
        trend_payload = fetch_trend_reco_payload(engine, target) or {}
    except Exception as exc:
        warnings.append(f"趋势推荐读取失败：{exc}")

    funds: list[dict] = []
    watchlist = _query_frame(
        engine,
        """
        SELECT DISTINCT ts_code, COALESCE(NULLIF(security_name, ''), ts_code) AS security_name
        FROM app_user_watchlist
        WHERE security_type IN ('fund', '基金', 'funds')
        ORDER BY ts_code
        """,
    )
    for row in watchlist.to_dict(orient="records") if not watchlist.empty else []:
        code = str(row.get("ts_code") or "").strip().upper()
        if not code:
            continue
        try:
            from src.fund_nav import fetch_latest_fund_nav_snapshot
            target_date = pd.Timestamp(target).date()
            nav = fetch_latest_fund_nav_snapshot(
                code,
                as_of_date=target_date + timedelta(days=1),
            ) or {}
            nav_date = _safe_date(nav.get("nav_date"))
            if nav_date != target:
                warnings.append(
                    f"基金 {code} 净值未对齐报告交易日 {target}，最近披露日期为 {nav_date or '未知'}"
                )
            funds.append({
                "fund_code": code,
                "fund_name": row.get("security_name") or code,
                "nav_date": nav_date,
                "daily_change_pct": nav.get("daily_change_pct"),
                "unit_nav": nav.get("unit_nav"),
            })
        except Exception as exc:
            warnings.append(f"基金 {code} 净值读取失败：{exc}")

    fact_pack = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "report_trade_date": target,
        "generated_at": datetime.now(BEIJING_TZ).isoformat(timespec="seconds"),
        "etf_overview": {
            "category_share_rows": _summarize_rows(
                etf,
                ["primary_category", "secondary_category", "total_share_size", "total_share_yi", "total_size", "total_size_yi"],
            ),
            "industry_etf_growth": _summarize_rows(
                industry_etf_growth,
                ["industry", "ts_code", "industry_etf", "current_date", "previous_date", "current_share", "previous_share", "share_change", "share_growth_pct", "current_size"],
                1000,
            ),
            "industry_etf_groups": _build_industry_etf_groups(industry_etf_growth),
        },
        "fund_watchlist": {"funds": funds},
        "trend_recommendations": {
            "trade_date": trend_payload.get("trade_date"),
            "top_uptrend": (trend_payload.get("top_uptrend") or [])[:10],
            "top_avoid": (trend_payload.get("top_avoid") or [])[:10],
        },
        "market_sentiment": {"limitup": _summarize_rows(sentiment, ["up_cnt", "zha_cnt", "total_cnt"], 1)},
        "northbound": {
            "daily": _summarize_rows(
                northbound,
                ["trade_date", "north_money", "north_money_yi", "hgt", "hgt_yi", "sgt", "sgt_yi"],
                1,
            )
        },
        "dragon_tiger": {"daily": _summarize_rows(lhb, ["stock_count", "distinct_stock_count"], 1)},
        "money_flow": {
            "ths_top_inflow": _summarize_rows(ths, ["industry", "net_amount", "net_amount_yi", "pct_change", "lead_stock"]),
            "dc_top_inflow": _summarize_rows(dc, ["industry", "net_amount", "net_amount_yi", "pct_change"]),
        },
        "volume": {"daily": _summarize_rows(volume, ["trade_date", "total_amount", "total_amount_yi", "total_volume"], 1)},
        "margin": {
            "daily": _summarize_rows(
                margin,
                [
                    "trade_date", "financing_buy", "financing_repay", "financing_balance",
                    "financing_buy_yi", "financing_repay_yi", "financing_balance_yi",
                ],
                1,
            )
        },
        "data_quality": {
            "report_status": readiness["report_status"],
            "coverage_score": readiness["coverage_score"],
            "required_ready": readiness["required_ready"],
            "required_total": readiness["required_total"],
            "warnings": warnings,
            "sources": readiness["sources"],
        },
    }
    fact_pack["evidence"] = build_evidence_ledger(fact_pack)
    return make_json_safe(fact_pack)


def _build_industry_etf_groups(frame: pd.DataFrame) -> list[dict]:
    if frame is None or frame.empty:
        return []
    groups = []
    for industry, group in frame.groupby("industry", dropna=False, sort=True):
        current_shares = pd.to_numeric(group.get("current_share"), errors="coerce")
        previous_shares = pd.to_numeric(group.get("previous_share"), errors="coerce")
        current_share = current_shares.sum(min_count=1)
        previous_share = previous_shares.sum(min_count=1)
        share_change = None
        share_growth_pct = None
        if not pd.isna(current_share) and not pd.isna(previous_share):
            share_change = float(current_share - previous_share)
            if float(previous_share) != 0:
                share_growth_pct = share_change / float(previous_share) * 100
        rows = []
        for row in group.sort_values("share_growth_pct", ascending=False, na_position="last").to_dict(orient="records"):
            rows.append({
                "ts_code": row.get("ts_code"),
                "etf_name": row.get("industry_etf") or row.get("etf_name") or row.get("ts_code"),
                "current_date": row.get("current_date"),
                "previous_date": row.get("previous_date"),
                "current_share": row.get("current_share"),
                "previous_share": row.get("previous_share"),
                "share_change": row.get("share_change"),
                "current_share_yi": None if pd.isna(pd.to_numeric(row.get("current_share"), errors="coerce")) else float(row.get("current_share")) / 10000.0,
                "previous_share_yi": None if pd.isna(pd.to_numeric(row.get("previous_share"), errors="coerce")) else float(row.get("previous_share")) / 10000.0,
                "share_change_yi": None if pd.isna(pd.to_numeric(row.get("share_change"), errors="coerce")) else float(row.get("share_change")) / 10000.0,
                "share_growth_pct": row.get("share_growth_pct"),
            })
        groups.append({
            "industry": industry or "未识别行业",
            "etf_count": len(rows),
            "current_date": max((str(value) for value in group["current_date"].dropna()), default=None),
            "previous_date": max((str(value) for value in group["previous_date"].dropna()), default=None),
            "current_share": None if pd.isna(current_share) else float(current_share),
            "previous_share": None if pd.isna(previous_share) else float(previous_share),
            "share_change": share_change,
            "current_share_yi": None if pd.isna(current_share) else float(current_share) / 10000.0,
            "previous_share_yi": None if pd.isna(previous_share) else float(previous_share) / 10000.0,
            "share_change_yi": None if share_change is None else float(share_change) / 10000.0,
            "share_growth_pct": share_growth_pct,
            "etfs": make_json_safe(rows),
        })
    groups.sort(
        key=lambda item: (
            item.get("share_growth_pct") is None,
            -(item.get("share_growth_pct") or 0),
            str(item.get("industry") or ""),
        )
    )
    return make_json_safe(groups)


def _evidence_item(
    evidence_id: str,
    label: str,
    value: Any,
    unit: str,
    as_of: str | None,
    source: str,
    *,
    status: str = "verified",
    note: str = "",
) -> dict:
    return {
        "evidence_id": evidence_id,
        "label": label,
        "value": make_json_safe(value),
        "unit": unit,
        "as_of": as_of,
        "source": source,
        "status": status,
        "note": note,
    }


def _int_or_zero(value: Any) -> int:
    parsed = pd.to_numeric(value, errors="coerce")
    return 0 if pd.isna(parsed) else int(parsed)


def build_evidence_ledger(fact_pack: dict) -> list[dict]:
    """Build a compact, addressable evidence ledger for UI and LLM claims."""
    target = fact_pack.get("report_trade_date")
    quality = fact_pack.get("data_quality", {}) or {}
    source_status = {
        source.get("key"): source.get("status", "missing")
        for source in quality.get("sources") or []
    }
    evidence: list[dict] = [
        _evidence_item(
            "quality.coverage",
            "晨报数据覆盖率",
            quality.get("coverage_score", 0),
            "%",
            target,
            "morning_report.readiness",
            status="verified",
        )
    ]

    sentiment_rows = (fact_pack.get("market_sentiment", {}) or {}).get("limitup") or []
    sentiment = sentiment_rows[0] if sentiment_rows else {}
    up_count = _int_or_zero(sentiment.get("up_cnt"))
    blowup_count = _int_or_zero(sentiment.get("zha_cnt"))
    sentiment_status = source_status.get("limit_sentiment", "missing")
    evidence.extend([
        _evidence_item("sentiment.limitup", "涨停家数", up_count, "家", target, "ts_limit_list_d", status=sentiment_status),
        _evidence_item("sentiment.blowup", "炸板家数", blowup_count, "家", target, "ts_limit_list_d", status=sentiment_status),
    ])
    attempts = up_count + blowup_count
    evidence.append(
        _evidence_item(
            "sentiment.blowup_rate",
            "炸板率",
            None if attempts <= 0 else round(blowup_count / attempts * 100, 2),
            "%",
            target,
            "derived:ts_limit_list_d",
            status=sentiment_status,
            note="炸板家数 /（涨停家数 + 炸板家数）",
        )
    )

    for rows, prefix, source_table, status_key in [
        ((fact_pack.get("money_flow", {}) or {}).get("ths_top_inflow") or [], "flow.ths", "ts_moneyflow_ind_ths", "ths_industry_flow"),
        ((fact_pack.get("money_flow", {}) or {}).get("dc_top_inflow") or [], "flow.dc", "ts_moneyflow_dc_ind", "dc_industry_flow"),
    ]:
        for index, row in enumerate(rows[:5]):
            evidence.append(
                _evidence_item(
                    f"{prefix}.{index}",
                    f"{row.get('industry') or '未知板块'}净流入",
                    row.get("net_amount_yi"),
                    "亿元",
                    target,
                    source_table,
                    status=source_status.get(status_key, "missing"),
                    note="已统一换算为亿元；THS与DC仍为不同供应商口径",
                )
            )

    groups = (fact_pack.get("etf_overview", {}) or {}).get("industry_etf_groups") or []
    for index, group in enumerate(groups[:8]):
        evidence.append(
            _evidence_item(
                f"etf.industry.{index}",
                f"{group.get('industry') or '未识别行业'}ETF份额变化",
                group.get("share_growth_pct"),
                "%",
                group.get("current_date") or target,
                "etf_share_size + etf_summary",
                status=source_status.get("etf_share", "missing"),
                note=f"合计份额变动 {group.get('share_change_yi')} 亿份",
            )
        )

    funds = (fact_pack.get("fund_watchlist", {}) or {}).get("funds") or []
    for index, fund in enumerate(funds[:20]):
        evidence.append(
            _evidence_item(
                f"fund.watchlist.{index}",
                f"{fund.get('fund_name') or fund.get('fund_code') or '基金'}净值涨跌",
                fund.get("daily_change_pct"),
                "%",
                fund.get("nav_date"),
                "fund_nav.confirmed",
                status="verified" if fund.get("nav_date") == target else "stale",
            )
        )

    north_rows = (fact_pack.get("northbound", {}) or {}).get("daily") or []
    if north_rows:
        evidence.append(
            _evidence_item(
                "northbound.net",
                "北向资金净流入",
                north_rows[0].get("north_money_yi"),
                "亿元",
                _safe_date(north_rows[0].get("trade_date")) or target,
                "ts_moneyflow_hsgt",
                status=source_status.get("northbound", "missing"),
                note="由Tushare百万元口径统一换算为亿元",
            )
        )

    trend = fact_pack.get("trend_recommendations", {}) or {}
    trend_date = _safe_date(trend.get("trade_date")) or target
    for direction, items in [("up", trend.get("top_uptrend") or []), ("avoid", trend.get("top_avoid") or [])]:
        for index, item in enumerate(items[:5]):
            evidence.append(
                _evidence_item(
                    f"trend.{direction}.{index}",
                    "趋势候选" if direction == "up" else "谨慎候选",
                    item.get("name") or item.get("ts_code"),
                    "",
                    trend_date,
                    "trend_reco_items",
                    status="generated",
                    note=f"行业：{item.get('industry') or '-'}；模型生成结果，不是原始行情",
                )
            )
    return make_json_safe(evidence)


def _evidence_by_id(fact_pack: dict) -> dict[str, dict]:
    return {
        str(item.get("evidence_id")): item
        for item in fact_pack.get("evidence") or []
        if item.get("evidence_id")
    }


def build_report_digest(fact_pack: dict) -> dict:
    warnings = fact_pack.get("data_quality", {}).get("warnings") or []
    funds = fact_pack.get("fund_watchlist", {}).get("funds") or []
    ths = fact_pack.get("money_flow", {}).get("ths_top_inflow") or []
    dc = fact_pack.get("money_flow", {}).get("dc_top_inflow") or []
    uptrend = fact_pack.get("trend_recommendations", {}).get("top_uptrend") or []
    avoid = fact_pack.get("trend_recommendations", {}).get("top_avoid") or []
    sentiment_rows = fact_pack.get("market_sentiment", {}).get("limitup") or []
    sentiment = sentiment_rows[0] if sentiment_rows else {}
    up_cnt = float(sentiment.get("up_cnt") or 0)
    zha_cnt = float(sentiment.get("zha_cnt") or 0)
    quality = fact_pack.get("data_quality", {}) or {}
    report_status = str(quality.get("report_status") or "complete")
    attempts = up_cnt + zha_cnt
    blowup_rate = None if attempts <= 0 else zha_cnt / attempts
    if report_status != "complete":
        risk = "灰色"
        risk_text = "关键数据未齐，暂不判断"
    elif blowup_rate is None:
        risk = "灰色"
        risk_text = "涨停情绪数据不足，保持观察"
    elif blowup_rate >= 0.35:
        risk = "红色"
        risk_text = "炸板率偏高，短线情绪谨慎"
    elif blowup_rate >= 0.2:
        risk = "黄色"
        risk_text = "市场存在分化，注意追涨节奏"
    else:
        risk = "绿色"
        risk_text = "涨停结构相对稳定"
    top_fund = funds[0] if funds else {}
    top_sector = ths[0].get("industry") if ths else (dc[0].get("industry") if dc else "-")
    digest = {
        "risk_color": risk,
        "risk_label": "短线情绪灯",
        "risk_text": risk_text,
        "blowup_rate": None if blowup_rate is None else round(blowup_rate * 100, 2),
        "fund_count": len(funds),
        "top_sector": top_sector or "-",
        "top_sector_net_amount": (ths[0].get("net_amount") if ths else None),
        "top_fund_name": top_fund.get("fund_name") or "-",
        "top_fund_code": top_fund.get("fund_code") or "-",
        "top_uptrend": uptrend[:3],
        "top_avoid": avoid[:3],
        "warning_count": len(warnings),
        "limitup_count": int(up_cnt),
        "blowup_count": int(zha_cnt),
        "coverage_score": int(quality.get("coverage_score") or 0),
        "report_status": report_status,
    }
    evidence = _evidence_by_id(fact_pack)

    def usable(evidence_id: str) -> bool:
        item = evidence.get(evidence_id) or {}
        return bool(item) and item.get("status", "verified") not in {"missing", "stale"}

    focus_items: list[dict] = []
    groups = (fact_pack.get("etf_overview", {}) or {}).get("industry_etf_groups") or []
    if groups and usable("etf.industry.0"):
        group = groups[0]
        growth = pd.to_numeric(group.get("share_growth_pct"), errors="coerce")
        growth_text = "--" if pd.isna(growth) else f"{float(growth):+.2f}%"
        focus_items.append({
            "text": f"{group.get('industry') or '行业ETF'}份额较前一日 {growth_text}",
            "evidence_ids": ["etf.industry.0"],
        })
    if ths and usable("flow.ths.0"):
        amount = pd.to_numeric(ths[0].get("net_amount_yi"), errors="coerce")
        amount_text = "--" if pd.isna(amount) else f"{float(amount):+,.2f}亿元"
        focus_items.append({
            "text": f"THS资金主线为{ths[0].get('industry') or '-'}，净流入 {amount_text}",
            "evidence_ids": ["flow.ths.0"],
        })
    sentiment_ids = ["sentiment.limitup", "sentiment.blowup", "sentiment.blowup_rate"]
    if all(usable(evidence_id) for evidence_id in sentiment_ids):
        rate_text = "--" if blowup_rate is None else f"{blowup_rate * 100:.2f}%"
        focus_items.append({
            "text": f"涨停 {int(up_cnt)} 家、炸板 {int(zha_cnt)} 家，炸板率 {rate_text}",
            "evidence_ids": sentiment_ids,
        })
    digest["focus_items"] = focus_items[:3]
    return digest


def _fallback_markdown(fact_pack: dict) -> str:
    target = fact_pack.get("report_trade_date") or "未知日期"
    warnings = fact_pack.get("data_quality", {}).get("warnings") or []
    digest = build_report_digest(fact_pack)
    etf_overview = fact_pack.get("etf_overview", {}) or {}
    etf_rows = etf_overview.get("category_share_rows") or []
    industry_etf_growth = etf_overview.get("industry_etf_growth") or []
    industry_etf_groups = etf_overview.get("industry_etf_groups") or []
    ths_rows = fact_pack.get("money_flow", {}).get("ths_top_inflow") or []
    dc_rows = fact_pack.get("money_flow", {}).get("dc_top_inflow") or []
    northbound_rows = fact_pack.get("northbound", {}).get("daily") or []
    sentiment_rows = fact_pack.get("market_sentiment", {}).get("limitup") or []
    margin_rows = fact_pack.get("margin", {}).get("daily") or []
    volume_rows = fact_pack.get("volume", {}).get("daily") or []
    trend = fact_pack.get("trend_recommendations", {}) or {}
    funds = fact_pack.get("fund_watchlist", {}).get("funds") or []

    lines = [
        f"# ETF 晨报｜{target}",
        "",
        f"> 当前为结构化事实版报告：LLM 未配置、关键数据未齐或本次模型结果未通过校验。"
        f"短线情绪灯：{digest['risk_color']}｜{digest['risk_text']}",
        "",
        "## 一、核心摘要",
        f"- 自选基金：{digest['fund_count']} 只",
        f"- 资金流入靠前行业：{digest['top_sector']}",
        f"- 涨停 / 炸板：{digest['limitup_count']} / {digest['blowup_count']}",
        f"- 数据覆盖率：{digest['coverage_score']}%（{digest['report_status']}）",
        f"- 数据质量提示：{digest['warning_count']} 条",
        "",
        "## 二、ETF / 市场概览",
        f"- ETF 分类份额记录：{len(etf_rows)} 条",
        f"- 行业 ETF 份额增长记录：{len(industry_etf_growth)} 条",
        f"- 北向资金记录：{len(northbound_rows)} 条",
        f"- 成交量记录：{len(volume_rows)} 条",
        f"- 两融记录：{len(margin_rows)} 条",
        "",
        "## 三、资金与情绪",
    ]
    if industry_etf_growth:
        lines.extend(["", "### 行业 ETF 较前一日份额变化"])
        for group in industry_etf_groups[:10]:
            group_growth = group.get("share_growth_pct")
            group_growth_text = "--" if group_growth is None else f"{float(group_growth):+.2f}%"
            lines.append(
                f"#### {group.get('industry') or '未识别行业'}（{group.get('etf_count') or 0} 只）｜"
                f"行业合计份额较前一日 {group_growth_text}｜增减 {group.get('share_change_yi') or 0} 亿份"
            )
            for row in (group.get("etfs") or [])[:5]:
                growth = row.get("share_growth_pct")
                growth_text = "--" if growth is None else f"{float(growth):+.2f}%"
                lines.append(
                    f"- {row.get('etf_name') or '-'}（{row.get('ts_code') or '-'}）｜份额变化 {growth_text}｜"
                    f"增减 {row.get('share_change_yi') or 0} 亿份｜当前份额 {row.get('current_share_yi') or '-'} 亿份"
                )
    if ths_rows:
        top_ths = ths_rows[:3]
        for row in top_ths:
            lines.append(f"- THS {row.get('industry') or '-'}｜净流入 {row.get('net_amount_yi') or '-'} 亿元｜涨跌 {row.get('pct_change') or '-'}%")
    if dc_rows:
        top_dc = dc_rows[:3]
        for row in top_dc:
            lines.append(f"- DC {row.get('industry') or '-'}｜净流入 {row.get('net_amount_yi') or '-'} 亿元｜涨跌 {row.get('pct_change') or '-'}%")
    if sentiment_rows:
        sentiment = sentiment_rows[0]
        lines.append(f"- 涨停 {sentiment.get('up_cnt') or 0} 家，炸板 {sentiment.get('zha_cnt') or 0} 家")
    if northbound_rows:
        nb = northbound_rows[0]
        lines.append(f"- 北向资金净流入 {nb.get('north_money_yi') or '-'} 亿元，沪股通 {nb.get('hgt_yi') or '-'} 亿元，深股通 {nb.get('sgt_yi') or '-'} 亿元")
    if margin_rows:
        m = margin_rows[0]
        lines.append(f"- 两融买入 {m.get('financing_buy_yi') or '-'} 亿元，偿还 {m.get('financing_repay_yi') or '-'} 亿元，余额 {m.get('financing_balance_yi') or '-'} 亿元")
    if volume_rows:
        v = volume_rows[0]
        lines.append(f"- 成交额 {v.get('total_amount_yi') or '-'} 亿元，成交量 {v.get('total_volume') or '-'} 手")

    lines.extend(["", "## 四、趋势推荐"])
    top_uptrend = trend.get("top_uptrend") or []
    top_avoid = trend.get("top_avoid") or []
    if top_uptrend:
        for item in top_uptrend[:3]:
            lines.append(f"- 看多：{item.get('name') or item.get('ts_code') or '-'}｜行业 {item.get('industry') or '-'}")
    if top_avoid:
        for item in top_avoid[:3]:
            lines.append(f"- 谨慎：{item.get('name') or item.get('ts_code') or '-'}｜行业 {item.get('industry') or '-'}")

    lines.extend(["", "## 五、自选基金上一交易日表现"])
    for fund in funds:
        change = fund.get("daily_change_pct")
        change_text = "--" if change is None else f"{float(change):+.2f}%"
        lines.append(
            f"- {fund.get('fund_name')}（{fund.get('fund_code')}）｜"
            f"净值日期：{fund.get('nav_date') or '-'}｜上一交易日涨跌幅：{change_text}"
        )

    lines.extend(["", "## 六、数据说明"])
    lines.append("- 自选基金采用最近已披露净值；若净值日期未对齐报告交易日，会在数据缺口中明确提示。")
    lines.append("- 如后续引用基金持仓，只能采用最近一期披露数据，不等同于上一交易日实时持仓。")
    lines.append("- THS 与 DC 资金流已统一显示为亿元，但仍属于不同供应商口径，不做绝对值横向比较。")
    if warnings:
        lines.extend(["", "### 数据缺口"])
        lines.extend([f"- {warning}" for warning in warnings])
    return "\n".join(lines) + "\n"


def _build_llm_fact_pack(fact_pack: dict) -> dict:
    """Send the addressable evidence ledger, not a large untyped data dump."""
    quality = fact_pack.get("data_quality", {}) or {}
    digest = build_report_digest(fact_pack)
    return make_json_safe({
        "schema_version": fact_pack.get("schema_version"),
        "report_trade_date": fact_pack.get("report_trade_date"),
        "generated_at": fact_pack.get("generated_at"),
        "data_quality": {
            "report_status": quality.get("report_status"),
            "coverage_score": quality.get("coverage_score"),
            "warnings": (quality.get("warnings") or [])[:20],
        },
        "deterministic_digest": {
            "short_term_sentiment": digest.get("risk_text"),
            "top_sector": digest.get("top_sector"),
            "limitup_count": digest.get("limitup_count"),
            "blowup_count": digest.get("blowup_count"),
        },
        "evidence": [
            item
            for item in (fact_pack.get("evidence") or [])
            if item.get("status", "verified") != "missing" and item.get("value") is not None
        ][:80],
    })


def _coerce_report_text(value: Any, max_length: int) -> str:
    text_value = str(value or "").strip()
    if len(text_value) > max_length:
        return text_value[: max_length - 1].rstrip() + "…"
    return text_value


def _claim_numbers_supported(text_value: str, evidence_items: list[dict]) -> bool:
    tokens = re.findall(r"(?<![A-Za-z])[-+]?\d+(?:\.\d+)?", str(text_value or ""))
    if not tokens:
        return True
    haystack = json.dumps(make_json_safe(evidence_items), ensure_ascii=False)
    numeric_values: list[float] = []
    for item in evidence_items:
        parsed = pd.to_numeric(item.get("value"), errors="coerce")
        if not pd.isna(parsed):
            numeric_values.append(float(parsed))
    for token in tokens:
        normalized = token.lstrip("+")
        if normalized in haystack:
            continue
        try:
            token_value = float(token)
        except Exception:
            return False
        if any(abs(token_value - value) <= max(0.01, abs(value) * 0.0001) for value in numeric_values):
            continue
        return False
    return True


def _normalize_claim_item(value: Any, evidence_map: dict[str, dict], *, max_length: int = 220) -> dict | None:
    if not isinstance(value, dict):
        return None
    text_value = _coerce_report_text(value.get("text"), max_length)
    requested_ids = value.get("evidence_ids") if isinstance(value.get("evidence_ids"), list) else []
    evidence_ids = []
    for evidence_id in requested_ids:
        key = str(evidence_id or "").strip()
        if key in evidence_map and key not in evidence_ids:
            evidence_ids.append(key)
    selected_evidence = [evidence_map[key] for key in evidence_ids]
    evidence_statuses = {str(item.get("status") or "verified") for item in selected_evidence}
    caveat = _coerce_report_text(value.get("caveat"), 140)
    if not text_value or not evidence_ids or not _claim_numbers_supported(text_value, selected_evidence):
        return None
    if "missing" in evidence_statuses:
        return None
    if evidence_statuses.intersection({"stale", "generated"}) and not caveat:
        return None
    return {
        "text": text_value,
        "evidence_ids": evidence_ids[:6],
        "caveat": caveat,
    }


def normalize_morning_llm_result(result: dict | None, fact_pack: dict) -> dict | None:
    if not isinstance(result, dict) or not result:
        return None
    evidence_map = _evidence_by_id(fact_pack)
    summary = _normalize_claim_item(
        {
            "text": result.get("summary"),
            "evidence_ids": result.get("summary_evidence_ids"),
            "caveat": "",
        },
        evidence_map,
        max_length=360,
    )
    focus_items = []
    raw_focus = result.get("focus_items") if isinstance(result.get("focus_items"), list) else []
    for item in raw_focus:
        normalized = _normalize_claim_item(item, evidence_map)
        if normalized:
            focus_items.append(normalized)
        if len(focus_items) >= 3:
            break
    risk_note = _normalize_claim_item(result.get("risk_note"), evidence_map, max_length=260)
    if not summary and not focus_items:
        return None
    return {
        "schema_version": LLM_SCHEMA_VERSION,
        "headline": _coerce_report_text(result.get("headline"), 80),
        "summary": summary or {},
        "focus_items": focus_items,
        "risk_note": risk_note or {},
        "data_quality_note": _coerce_report_text(result.get("data_quality_note"), 260),
        "validated": True,
    }


def render_morning_llm_markdown(fact_pack: dict, analysis: dict) -> str:
    target = fact_pack.get("report_trade_date") or "未知日期"
    quality = fact_pack.get("data_quality", {}) or {}
    lines = [f"# ETF 晨报｜{target}", ""]
    if analysis.get("headline"):
        lines.extend([f"> {analysis['headline']}", ""])
    summary = analysis.get("summary") or {}
    if summary.get("text"):
        lines.extend(["## 核心结论", summary["text"], ""])
        lines.append(f"证据：{', '.join(summary.get('evidence_ids') or [])}")
    focus_items = analysis.get("focus_items") or []
    if focus_items:
        lines.extend(["", "## 今天先看三件事"])
        for item in focus_items:
            evidence_text = ", ".join(item.get("evidence_ids") or [])
            caveat = f"；限制：{item['caveat']}" if item.get("caveat") else ""
            lines.append(f"- {item.get('text')}（证据：{evidence_text}{caveat}）")
    risk_note = analysis.get("risk_note") or {}
    if risk_note.get("text"):
        lines.extend(["", "## 风险提示", risk_note["text"]])
        lines.append(f"证据：{', '.join(risk_note.get('evidence_ids') or [])}")
    lines.extend([
        "",
        "## 数据质量",
        f"- 数据覆盖率：{quality.get('coverage_score', 0)}%",
        f"- 报告状态：{quality.get('report_status') or 'partial'}",
    ])
    if analysis.get("data_quality_note"):
        lines.append(f"- 模型说明：{analysis['data_quality_note']}")
    for warning in (quality.get("warnings") or [])[:12]:
        lines.append(f"- {warning}")
    lines.extend(["", f"> 分析版本：{LLM_SCHEMA_VERSION}。仅展示通过证据绑定与数字校验的模型结论。"])
    return "\n".join(lines) + "\n"


def generate_llm_markdown(fact_pack: dict) -> tuple[str, dict | None]:
    config = load_stock_research_llm_config()
    quality = fact_pack.get("data_quality", {}) or {}
    if quality.get("report_status") != "complete":
        quality.setdefault("warnings", []).append("关键数据源未齐，已跳过LLM综合并使用事实版")
        return _fallback_markdown(fact_pack), None
    if not config.configured:
        return _fallback_markdown(fact_pack), None
    llm_fact_pack = _build_llm_fact_pack(fact_pack)
    system = (
        "你是审慎的ETF晨报分析员。只能使用给定evidence数组，不得补充外部知识、新闻、数字或因果解释。"
        "每个结论必须列出真实存在的evidence_id；不同供应商资金流口径不可直接互相比较。"
        "不得引用status=missing的证据；引用status=stale或generated的证据时必须在caveat明确说明其时效或模型属性。"
        "只返回一个JSON对象，字段固定为headline, summary, summary_evidence_ids, focus_items, risk_note, data_quality_note。"
        "focus_items最多3项，每项为{text,evidence_ids,caveat}；risk_note也使用相同结构。"
        "不得给出绝对买卖指令，不得输出Markdown。"
    )
    user = "请生成60-90秒可读的晨报JSON。没有证据支持的内容宁可不写：\n\n" + json.dumps(make_json_safe(llm_fact_pack), ensure_ascii=False)
    last_error = ""
    try:
        import requests
        request_body = {
            "model": config.model,
            "temperature": min(float(config.temperature), 0.1),
            "max_tokens": max(3200, config.max_tokens),
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }
        for attempt in range(2):
            try:
                response = requests.post(
                    config.base_url.rstrip("/") + "/chat/completions",
                    headers={"Authorization": f"Bearer {config.api_key}", "Content-Type": "application/json"},
                    json=request_body,
                    timeout=config.timeout_seconds,
                )
                response.raise_for_status()
                content = (((response.json().get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
                parsed = parse_llm_json_object(content) if content else None
                normalized = normalize_morning_llm_result(parsed, fact_pack)
                if normalized:
                    return render_morning_llm_markdown(fact_pack, normalized), {
                        "model": config.model,
                        "attempt": attempt + 1,
                        "schema_version": LLM_SCHEMA_VERSION,
                        "analysis": normalized,
                        "validation": {"evidence_bound": True, "numbers_checked": True},
                    }
                last_error = "LLM未返回通过证据校验的结构化结果"
            except Exception as exc:
                last_error = str(exc)
                logger.warning("ETF morning report LLM attempt %s failed: %s", attempt + 1, exc)
    except Exception as exc:
        last_error = str(exc)
    if last_error:
        quality = fact_pack.setdefault("data_quality", {})
        quality.setdefault("warnings", []).append(f"LLM生成失败，已降级为事实版：{last_error}")
    return _fallback_markdown(fact_pack), None


def save_report(fact_pack: dict, markdown: str, llm_meta: dict | None = None) -> dict:
    target = str(fact_pack.get("report_trade_date") or datetime.now(BEIJING_TZ).date())
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_hash = hashlib.sha256(
        json.dumps(
            {"fact_pack": make_json_safe(fact_pack), "markdown": markdown},
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    payload = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "fact_pack": fact_pack,
        "markdown": markdown,
        "llm": llm_meta or {},
        "report_mode": "llm" if llm_meta else "facts",
        "report_hash": report_hash,
        "saved_at": datetime.now(BEIJING_TZ).isoformat(timespec="seconds"),
    }
    payload_text = json.dumps(payload, ensure_ascii=False, indent=2)
    _atomic_write_text(REPORT_DIR / f"{target}.json", payload_text)
    _atomic_write_text(REPORT_DIR / f"{target}.md", markdown)
    _atomic_write_text(REPORT_DIR / "latest.json", payload_text)
    _atomic_write_text(REPORT_DIR / "latest.md", markdown)
    return payload


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            handle.write(content)
            handle.flush()
            temporary_path = Path(handle.name)
        temporary_path.replace(path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink(missing_ok=True)


def generate_and_save_report(trade_date: str | None = None) -> dict:
    fact_pack = collect_fact_pack(trade_date)
    markdown, llm_meta = generate_llm_markdown(fact_pack)
    return save_report(fact_pack, markdown, llm_meta)


def list_saved_report_dates() -> list[str]:
    if not REPORT_DIR.exists():
        return []
    return sorted({path.stem for path in REPORT_DIR.glob("????-??-??.json")}, reverse=True)


def load_saved_report(trade_date: str | None = None) -> dict | None:
    path = REPORT_DIR / (f"{trade_date}.json" if trade_date else "latest.json")
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or not isinstance(payload.get("fact_pack"), dict):
            logger.warning("Invalid morning report payload: %s", path)
            return None
        return payload
    except Exception as exc:
        logger.warning("Failed to load morning report %s: %s", path, exc)
        return None
