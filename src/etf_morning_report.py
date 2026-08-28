from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.distribution_llm_analysis import make_json_safe
from src.fund_hot_stocks import get_engine as get_fund_engine, query_fund_preference_snapshot
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
        SELECT trade_date, secondary_category, SUM(COALESCE(total_share, 0)) AS total_share_size
        FROM etf_share_size
        WHERE trade_date = :trade_date
        GROUP BY trade_date, secondary_category
        ORDER BY total_share_size DESC
        LIMIT 20
        """,
        {"trade_date": target},
    )
    if etf.empty:
        warnings.append("ETF份额数据缺失")

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
        SELECT payload->>'industry' AS industry,
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
        SELECT trade_date, SUM(amount) AS total_amount, SUM(vol) AS total_volume
        FROM ts_stock_daily
        WHERE trade_date = :trade_date
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
            holdings = query_fund_preference_snapshot(code, top_n=10, engine=engine)
            funds.append({
                "fund_code": code,
                "fund_name": row.get("security_name") or code,
                "holdings": _summarize_rows(
                    holdings,
                    ["symbol", "stock_name", "stk_mkv_ratio", "stock_industry", "stock_market"],
                    10,
                ),
                "holding_count": int(len(holdings)),
            })
        except Exception as exc:
            warnings.append(f"基金 {code} 持仓读取失败：{exc}")

    return make_json_safe({
        "schema_version": REPORT_SCHEMA_VERSION,
        "report_trade_date": target,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "etf_overview": {"category_share_rows": _summarize_rows(etf, ["secondary_category", "total_share_size"])},
        "fund_watchlist": {"funds": funds},
        "trend_recommendations": {
            "trade_date": trend_payload.get("trade_date"),
            "top_uptrend": (trend_payload.get("top_uptrend") or [])[:10],
            "top_avoid": (trend_payload.get("top_avoid") or [])[:10],
        },
        "money_flow": {
            "ths_top_inflow": _summarize_rows(ths, ["industry", "net_amount", "pct_change", "lead_stock"]),
            "dc_top_inflow": _summarize_rows(dc, ["industry", "net_amount", "pct_change"]),
        },
        "volume": {"daily": _summarize_rows(volume, ["trade_date", "total_amount", "total_volume"], 1)},
        "margin": {"daily": _summarize_rows(margin, ["trade_date", "financing_buy", "financing_repay", "financing_balance"], 1)},
        "data_quality": {"warnings": warnings},
    })


def _fallback_markdown(fact_pack: dict) -> str:
    target = fact_pack.get("report_trade_date") or "未知日期"
    warnings = fact_pack.get("data_quality", {}).get("warnings") or []
    lines = [f"# ETF 晨报｜{target}", "", "> 当前为结构化事实版报告：LLM 未配置或本次调用失败。", "", "## 一、核心数据"]
    etf_rows = fact_pack.get("etf_overview", {}).get("category_share_rows") or []
    lines.append(f"- ETF 分类份额记录：{len(etf_rows)} 条")
    ths_rows = fact_pack.get("money_flow", {}).get("ths_top_inflow") or []
    lines.append(f"- THS 行业资金流记录：{len(ths_rows)} 条")
    funds = fact_pack.get("fund_watchlist", {}).get("funds") or []
    lines.append(f"- 纳入分析的自选基金：{len(funds)} 只")
    lines.extend(["", "## 二、自选基金持仓"])
    for fund in funds:
        lines.append(f"### {fund.get('fund_name')}（{fund.get('fund_code')}）")
        lines.append(f"- 前十大持仓可用：{fund.get('holding_count', 0)} 只")
        for holding in fund.get("holdings", [])[:5]:
            lines.append(f"- {holding.get('stock_name') or holding.get('symbol')}｜行业：{holding.get('stock_industry') or '-'}｜权重：{holding.get('stk_mkv_ratio') or '-'}%")
    lines.extend(["", "## 三、数据说明"])
    lines.append("- 基金持仓采用最近一期披露数据，不等同于上一交易日实时持仓。")
    if warnings:
        lines.extend(["", "### 数据缺口"])
        lines.extend([f"- {warning}" for warning in warnings])
    return "\n".join(lines) + "\n"


def generate_llm_markdown(fact_pack: dict) -> tuple[str, dict | None]:
    config = load_stock_research_llm_config()
    if not config.configured:
        return _fallback_markdown(fact_pack), None
    system = (
        "你是ETF晨报分析员。只能基于给定JSON事实数据，不得编造数字、日期或新闻。"
        "必须区分上一交易日行情数据与基金最近一期披露持仓。输出完整中文Markdown报告，"
        "包含：核心结论、ETF方向、自选基金持仓、行业/细分板块、资金流、趋势推荐、风险提示、数据缺口。"
        "不得给出绝对买卖指令，缺数据要明确写出。"
    )
    user = "请基于以下Fact Pack生成一份5-8分钟可读的ETF晨报，只输出Markdown：\n\n" + json.dumps(make_json_safe(fact_pack), ensure_ascii=False)
    try:
        import requests
        response = requests.post(
            config.base_url.rstrip("/") + "/chat/completions",
            headers={"Authorization": f"Bearer {config.api_key}", "Content-Type": "application/json"},
            json={"model": config.model, "temperature": 0.2, "max_tokens": max(3200, config.max_tokens), "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}]},
            timeout=config.timeout_seconds,
        )
        response.raise_for_status()
        content = (((response.json().get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
        if content:
            return content, {"model": config.model}
    except Exception as exc:
        logger.warning("ETF morning report LLM failed: %s", exc)
    return _fallback_markdown(fact_pack), None


def save_report(fact_pack: dict, markdown: str, llm_meta: dict | None = None) -> dict:
    target = str(fact_pack.get("report_trade_date") or datetime.now().date())
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"fact_pack": fact_pack, "markdown": markdown, "llm": llm_meta or {}, "saved_at": datetime.now().isoformat(timespec="seconds")}
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
