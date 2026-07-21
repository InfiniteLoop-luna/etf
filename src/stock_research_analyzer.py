from __future__ import annotations

from typing import Any

from sqlalchemy.engine import Engine

from src.stock_research_fact_pack import build_stock_research_fact_pack
from src.stock_research_html_renderer import render_stock_research_html
from src.stock_research_llm_analysis import (
    analyze_stock_research_payload,
    render_stock_research_llm_markdown,
)


def _fmt(value: Any, digits: int = 2, suffix: str = "") -> str:
    try:
        parsed = float(value)
    except Exception:
        return "-"
    return f"{parsed:,.{digits}f}{suffix}"


def _append_fact_pack_overview(md: list[str], fact_pack: dict[str, Any]) -> None:
    profile = fact_pack.get("profile") or {}
    price = fact_pack.get("price_metrics") or {}
    valuation = fact_pack.get("valuation_snapshot") or {}
    financial = (fact_pack.get("financial_metrics") or {}).get("latest") or {}
    quality = fact_pack.get("data_quality") or {}

    md.extend(
        [
            "## 数据底稿概览",
            "",
            f"- **股票**：{fact_pack.get('stock_name') or '-'}（{fact_pack.get('ts_code') or '-'}）",
            f"- **数据日期**：{fact_pack.get('asof_trade_date') or '-'}",
            f"- **行业/板块**：{profile.get('industry') or '-'} / {profile.get('market') or '-'}",
            f"- **主营业务**：{profile.get('main_business') or profile.get('business_scope') or '-'}",
            "",
            "### 行情与估值",
            "",
            "| 指标 | 数值 |",
            "|---|---:|",
            f"| 最新收盘 | {_fmt(price.get('latest_close'))} |",
            f"| 近5日涨跌 | {_fmt(price.get('ret_5d_pct'), suffix='%')} |",
            f"| 近20日涨跌 | {_fmt(price.get('ret_20d_pct'), suffix='%')} |",
            f"| 52周高点回撤 | {_fmt(price.get('drawdown_from_52w_high_pct'), suffix='%')} |",
            f"| PE_TTM | {_fmt(valuation.get('pe_ttm'))} |",
            f"| PB | {_fmt(valuation.get('pb'))} |",
            f"| 总市值 | {_fmt(valuation.get('total_mv_yi'), suffix=' 亿')} |",
            "",
            "### 财务快照",
            "",
            "| 指标 | 数值 |",
            "|---|---:|",
            f"| 最近财报期 | {financial.get('fina_end_date') or '-'} |",
            f"| ROE | {_fmt(financial.get('roe'), suffix='%')} |",
            f"| 毛利率 | {_fmt(financial.get('gross_margin'), suffix='%')} |",
            f"| 资产负债率 | {_fmt(financial.get('debt_to_assets'), suffix='%')} |",
            f"| 营收 | {_fmt(financial.get('total_revenue_yi'), suffix=' 亿')} |",
            f"| 净利润 | {_fmt(financial.get('net_profit_yi'), suffix=' 亿')} |",
            f"| 经营现金流 | {_fmt(financial.get('operating_cashflow_yi'), suffix=' 亿')} |",
            "",
            "### 数据质量",
            "",
            f"- profile={quality.get('profile_rows', 0)} 行，daily={quality.get('daily_rows', 0)} 行，kline={quality.get('kline_rows', 0)} 行，financial={quality.get('financial_rows', 0)} 行",
        ]
    )
    errors = quality.get("errors") or []
    if errors:
        md.append(f"- 数据缺口：{'; '.join(str(item) for item in errors[:4])}")


def _append_supplemental_overview(md: list[str], fact_pack: dict[str, Any]) -> None:
    supplemental = fact_pack.get("supplemental") or {}
    if not isinstance(supplemental, dict) or not supplemental:
        return
    titles = {
        "business_composition": "主营构成",
        "news": "近期新闻",
        "research_reports": "机构研报",
        "money_flow": "资金流",
        "lhb": "龙虎榜",
        "industry_peer_hint": "行业成分参考",
    }
    md.extend(["", "### 补充证据覆盖", "", "| 数据块 | 状态 | 条数 |", "|---|---:|---:|"])
    for key, title in titles.items():
        block = supplemental.get(key) or {}
        status = str(block.get("status") or "missing")
        row_count = block.get("row_count")
        item_count = len(block.get("items") or [])
        md.append(f"| {title} | {status} | {row_count if row_count is not None else item_count} |")


def render_stock_research_markdown(
    fact_pack: dict[str, Any],
    llm_result: dict[str, Any],
) -> str:
    md: list[str] = [
        f"# {fact_pack.get('stock_name') or fact_pack.get('ts_code')}（{fact_pack.get('ts_code')}）个股深度研究",
        "",
        "> 本报告面向自选股跟踪，由本地行情/财务数据底稿与 LLM 结构化分析共同生成。",
        "",
    ]
    _append_fact_pack_overview(md, fact_pack)
    _append_supplemental_overview(md, fact_pack)
    md.extend(render_stock_research_llm_markdown(llm_result))
    return "\n".join(md)


def render_stock_research_markdown_without_llm(
    fact_pack: dict[str, Any],
    *,
    reason: str = "LLM 不可用",
) -> str:
    md: list[str] = [
        f"# {fact_pack.get('stock_name') or fact_pack.get('ts_code')}（{fact_pack.get('ts_code')}）个股深度研究（基础版）",
        "",
        "> 本报告由本地行情/财务数据底稿自动生成。当前 LLM 结构化分析暂不可用，因此先展示基础版研究底稿，供自选跟踪使用。",
        "",
        f"> 降级原因：{reason}",
        "",
    ]
    _append_fact_pack_overview(md, fact_pack)
    _append_supplemental_overview(md, fact_pack)
    md.extend(
        [
            "",
            "## 降级说明",
            "",
            "- 当前未生成 LLM 的综合判断、风险等级、置信度与 Step 0-8 分析。",
            "- 但底层行情、估值、财务与补充证据仍已按最新交易日刷新。",
            "- 待 LLM 服务恢复后，可再补生成完整版个股深度研究报告。",
            "",
            "> 本报告仅供研究跟踪使用，不构成投资建议。",
        ]
    )
    return "\n".join(md)


def generate_stock_research_report_bundle(
    ts_code: str,
    stock_name: str,
    *,
    engine: Engine,
    asof_trade_date: str | None = None,
    allow_live_fetch: bool = False,
) -> dict[str, Any]:
    fact_pack = build_stock_research_fact_pack(
        ts_code,
        stock_name,
        engine=engine,
        asof_trade_date=asof_trade_date,
        allow_live_fetch=allow_live_fetch,
    )
    llm_result = analyze_stock_research_payload(fact_pack)
    if not llm_result:
        fallback_reason = "个股深度研究 LLM 未配置或未返回有效结构化结果"
        report_md = render_stock_research_markdown_without_llm(
            fact_pack,
            reason=fallback_reason,
        )
        report_html = render_stock_research_html(fact_pack, {}, report_md=report_md)
        return {
            "report_md": report_md,
            "report_html": report_html,
            "fact_pack": fact_pack,
            "llm_result": {},
        }
    report_md = render_stock_research_markdown(fact_pack, llm_result)
    report_html = render_stock_research_html(fact_pack, llm_result, report_md=report_md)
    return {
        "report_md": report_md,
        "report_html": report_html,
        "fact_pack": fact_pack,
        "llm_result": llm_result,
    }


def generate_stock_research_report_markdown(
    ts_code: str,
    stock_name: str,
    engine: Engine,
    asof_trade_date: str | None = None,
    allow_live_fetch: bool = False,
    use_report_cache: bool = False,
    save_report: bool = False,
) -> str:
    _ = (use_report_cache, save_report)
    return generate_stock_research_report_bundle(
        ts_code,
        stock_name,
        engine=engine,
        asof_trade_date=asof_trade_date,
        allow_live_fetch=allow_live_fetch,
    )["report_md"]
