from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.distribution_llm_analysis import make_json_safe
from src.fund_hot_stocks import get_engine as get_fund_engine
from src.stock_research_llm_analysis import load_stock_research_llm_config

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = PROJECT_ROOT / "data" / "morning_reports"
REPORT_SCHEMA_VERSION = "etf-morning-report-v1"


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
    upper = (today or datetime.now().date()) - timedelta(days=1)
    candidates: list[str] = []
    for table, column in [
        ("etf_share_size", "trade_date"),
        ("ts_stock_daily", "trade_date"),
        ("ts_moneyflow", "trade_date"),
        ("ts_margin", "trade_date"),
        ("ts_limit_list_d", "trade_date"),
    ]:
        value = _query_frame(
            engine,
            f"SELECT MAX({column}) AS latest_date FROM {table} WHERE {column} <= :upper",
            {"upper": upper},
        )
        parsed = _safe_date(value.iloc[0]["latest_date"]) if not value.empty else None
        if parsed:
            candidates.append(parsed)
    return max(candidates) if candidates else None


def _summarize_rows(frame: pd.DataFrame, columns: list[str], limit: int = 10) -> list[dict]:
    if frame is None or frame.empty:
        return []
    out = frame.copy().head(limit)
    return out[[column for column in columns if column in out.columns]].to_dict(orient="records")


def collect_fact_pack(trade_date: str | None = None, engine=None) -> dict:
    engine = engine or get_fund_engine()
    target = trade_date or find_previous_trade_date(engine)
    warnings: list[str] = []
    if not target:
        return {
            "schema_version": REPORT_SCHEMA_VERSION,
            "report_trade_date": None,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "data_quality": {"warnings": ["未找到上一交易日"]},
        }

    etf = _query_frame(
        engine,
        """
        SELECT s.trade_date, e.primary_category, e.secondary_category,
               SUM(COALESCE(s.total_share, 0)) AS total_share_size,
               SUM(COALESCE(s.total_size, 0)) AS total_size
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
               SUM((payload->>'rzye')::numeric) AS financing_balance
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
               (payload->>'hgt')::numeric AS hgt,
               (payload->>'sgt')::numeric AS sgt
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
            nav = fetch_latest_fund_nav_snapshot(code) or {}
            funds.append({
                "fund_code": code,
                "fund_name": row.get("security_name") or code,
                "nav_date": _safe_date(nav.get("nav_date")),
                "daily_change_pct": nav.get("daily_change_pct"),
                "unit_nav": nav.get("unit_nav"),
            })
        except Exception as exc:
            warnings.append(f"基金 {code} 净值读取失败：{exc}")

    return make_json_safe({
        "schema_version": REPORT_SCHEMA_VERSION,
        "report_trade_date": target,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "etf_overview": {
            "category_share_rows": _summarize_rows(etf, ["primary_category", "secondary_category", "total_share_size", "total_size"]),
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
        "northbound": {"daily": _summarize_rows(northbound, ["trade_date", "north_money", "hgt", "sgt"], 1)},
        "dragon_tiger": {"daily": _summarize_rows(lhb, ["stock_count", "distinct_stock_count"], 1)},
        "money_flow": {
            "ths_top_inflow": _summarize_rows(ths, ["industry", "net_amount", "pct_change", "lead_stock"]),
            "dc_top_inflow": _summarize_rows(dc, ["industry", "net_amount", "pct_change"]),
        },
        "volume": {"daily": _summarize_rows(volume, ["trade_date", "total_amount", "total_volume"], 1)},
        "margin": {"daily": _summarize_rows(margin, ["trade_date", "financing_buy", "financing_repay", "financing_balance"], 1)},
        "data_quality": {"warnings": warnings},
    })


def _build_industry_etf_groups(frame: pd.DataFrame) -> list[dict]:
    if frame is None or frame.empty:
        return []
    groups = []
    for industry, group in frame.groupby("industry", dropna=False, sort=True):
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
                "share_growth_pct": row.get("share_growth_pct"),
            })
        groups.append({"industry": industry or "未识别行业", "etf_count": len(rows), "etfs": make_json_safe(rows)})
    return groups


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
    if up_cnt <= 0:
        risk = "灰色"
        risk_text = "涨停情绪数据不足，保持观察"
    elif zha_cnt / up_cnt >= 0.35:
        risk = "红色"
        risk_text = "炸板率偏高，短线情绪谨慎"
    elif zha_cnt / up_cnt >= 0.2:
        risk = "黄色"
        risk_text = "市场存在分化，注意追涨节奏"
    else:
        risk = "绿色"
        risk_text = "涨停结构相对稳定"
    top_fund = funds[0] if funds else {}
    top_sector = ths[0].get("industry") if ths else (dc[0].get("industry") if dc else "-")
    return {
        "risk_color": risk,
        "risk_text": risk_text,
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
    }


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
        f"> 当前为结构化事实版报告：LLM 未配置或本次调用失败。风险灯：{digest['risk_color']}｜{digest['risk_text']}",
        "",
        "## 一、核心摘要",
        f"- 自选基金：{digest['fund_count']} 只",
        f"- 资金流入靠前行业：{digest['top_sector']}",
        f"- 涨停 / 炸板：{digest['limitup_count']} / {digest['blowup_count']}",
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
        for group in industry_etf_groups:
            lines.append(f"#### {group.get('industry') or '未识别行业'}（{group.get('etf_count') or 0} 只）")
            for row in group.get("etfs") or []:
                growth = row.get("share_growth_pct")
                growth_text = "--" if growth is None else f"{float(growth):+.2f}%"
                lines.append(
                    f"- {row.get('etf_name') or '-'}（{row.get('ts_code') or '-'}）｜份额变化 {growth_text}｜"
                    f"增减 {row.get('share_change') or 0}｜当前份额 {row.get('current_share') or '-'}"
                )
    if ths_rows:
        top_ths = ths_rows[:3]
        for row in top_ths:
            lines.append(f"- THS {row.get('industry') or '-'}｜净流入 {row.get('net_amount') or '-'}｜涨跌 {row.get('pct_change') or '-'}%")
    if dc_rows:
        top_dc = dc_rows[:3]
        for row in top_dc:
            lines.append(f"- DC {row.get('industry') or '-'}｜净流入 {row.get('net_amount') or '-'}｜涨跌 {row.get('pct_change') or '-'}%")
    if sentiment_rows:
        sentiment = sentiment_rows[0]
        lines.append(f"- 涨停 {sentiment.get('up_cnt') or 0} 家，炸板 {sentiment.get('zha_cnt') or 0} 家")
    if northbound_rows:
        nb = northbound_rows[0]
        lines.append(f"- 北向资金净流入 {nb.get('north_money') or '-'}，沪股通 {nb.get('hgt') or '-'}，深股通 {nb.get('sgt') or '-'}")
    if margin_rows:
        m = margin_rows[0]
        lines.append(f"- 两融买入 {m.get('financing_buy') or '-'}，偿还 {m.get('financing_repay') or '-'}，余额 {m.get('financing_balance') or '-'}")
    if volume_rows:
        v = volume_rows[0]
        lines.append(f"- 成交额 {v.get('total_amount') or '-'}，成交量 {v.get('total_volume') or '-'}")

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
    lines.append("- 基金持仓采用最近一期披露数据，不等同于上一交易日实时持仓。")
    if warnings:
        lines.extend(["", "### 数据缺口"])
        lines.extend([f"- {warning}" for warning in warnings])
    return "\n".join(lines) + "\n"


def _build_llm_fact_pack(fact_pack: dict) -> dict:
    """Trim verbose evidence before sending it to the LLM; full facts stay on disk."""
    compact = dict(fact_pack)
    compact["etf_overview"] = {
        "category_share_rows": (fact_pack.get("etf_overview", {}).get("category_share_rows") or [])[:12],
        "industry_etf_growth": (fact_pack.get("etf_overview", {}).get("industry_etf_growth") or [])[:300],
    }
    compact["money_flow"] = {
        "ths_top_inflow": (fact_pack.get("money_flow", {}).get("ths_top_inflow") or [])[:8],
        "dc_top_inflow": (fact_pack.get("money_flow", {}).get("dc_top_inflow") or [])[:8],
    }
    compact["trend_recommendations"] = {
        "trade_date": fact_pack.get("trend_recommendations", {}).get("trade_date"),
        "top_uptrend": (fact_pack.get("trend_recommendations", {}).get("top_uptrend") or [])[:6],
        "top_avoid": (fact_pack.get("trend_recommendations", {}).get("top_avoid") or [])[:6],
    }
    compact["fund_watchlist"] = {"funds": []}
    for fund in fact_pack.get("fund_watchlist", {}).get("funds") or []:
        compact["fund_watchlist"]["funds"].append({
            "fund_code": fund.get("fund_code"),
            "fund_name": fund.get("fund_name"),
            "nav_date": fund.get("nav_date"),
            "daily_change_pct": fund.get("daily_change_pct"),
            "unit_nav": fund.get("unit_nav"),
        })
    return compact


def generate_llm_markdown(fact_pack: dict) -> tuple[str, dict | None]:
    config = load_stock_research_llm_config()
    if not config.configured:
        return _fallback_markdown(fact_pack), None
    llm_fact_pack = _build_llm_fact_pack(fact_pack)
    system = (
        "你是ETF晨报分析员。只能基于给定JSON事实数据，不得编造数字、日期或新闻。"
        "必须区分上一交易日行情数据与基金最近一期披露持仓。输出完整中文Markdown报告，"
        "包含：核心结论、ETF方向、行业ETF份额增长、自选基金上一交易日涨跌幅、资金流、趋势推荐、风险提示、数据缺口。"
        "不得给出绝对买卖指令，缺数据要明确写出。"
    )
    user = "请基于以下Fact Pack生成一份5-8分钟可读的ETF晨报，只输出Markdown：\n\n" + json.dumps(make_json_safe(llm_fact_pack), ensure_ascii=False)
    last_error = ""
    try:
        import requests
        request_body = {
            "model": config.model,
            "temperature": 0.2,
            "max_tokens": max(3200, config.max_tokens),
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
                if content:
                    return content, {"model": config.model, "attempt": attempt + 1}
                last_error = "LLM返回空内容"
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
    target = str(fact_pack.get("report_trade_date") or datetime.now().date())
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "fact_pack": fact_pack,
        "markdown": markdown,
        "llm": llm_meta or {},
        "report_mode": "llm" if llm_meta else "facts",
        "saved_at": datetime.now().isoformat(timespec="seconds"),
    }
    (REPORT_DIR / f"{target}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (REPORT_DIR / f"{target}.md").write_text(markdown, encoding="utf-8")
    (REPORT_DIR / "latest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (REPORT_DIR / "latest.md").write_text(markdown, encoding="utf-8")
    return payload


def generate_and_save_report(trade_date: str | None = None) -> dict:
    fact_pack = collect_fact_pack(trade_date)
    markdown, llm_meta = generate_llm_markdown(fact_pack)
    return save_report(fact_pack, markdown, llm_meta)


def list_saved_report_dates() -> list[str]:
    if not REPORT_DIR.exists():
        return []
    return sorted({path.stem for path in REPORT_DIR.glob("????-??-??.md")}, reverse=True)


def load_saved_report(trade_date: str | None = None) -> dict | None:
    path = REPORT_DIR / (f"{trade_date}.json" if trade_date else "latest.json")
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
