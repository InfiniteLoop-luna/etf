from __future__ import annotations

import json
from html import escape
from typing import Any

from src.distribution_llm_analysis import make_json_safe
from src.stock_research_llm_analysis import (
    STOCK_RESEARCH_LLM_SCHEMA_VERSION,
    normalize_stock_research_llm_result,
)

STOCK_RESEARCH_HTML_MARKER = "<!-- stock-research-html-v1 -->"


def _text(value: Any, default: str = "-") -> str:
    text = str(value or "").strip()
    return escape(text if text else default)


def _raw_text(value: Any, default: str = "-") -> str:
    text = str(value or "").strip()
    return text if text else default


def _num(value: Any, digits: int = 2, suffix: str = "") -> str:
    try:
        parsed = float(value)
    except Exception:
        return "-"
    return f"{parsed:,.{digits}f}{suffix}"


def _int_num(value: Any, suffix: str = "") -> str:
    try:
        parsed = float(value)
    except Exception:
        return "-"
    return f"{parsed:,.0f}{suffix}"


def _json_script(value: Any) -> str:
    return json.dumps(make_json_safe(value), ensure_ascii=False).replace("</", "<\\/")


def _metric_card(label: str, value: str, hint: str = "") -> str:
    return (
        '<div class="metric-card">'
        f'<span class="metric-label">{escape(label)}</span>'
        f'<strong>{escape(value)}</strong>'
        f'<small>{escape(hint)}</small>'
        "</div>"
    )


def _list_items(items: Any, empty: str = "暂无") -> str:
    values = items if isinstance(items, list) else []
    clean = [_raw_text(item) for item in values if _raw_text(item, "")]
    if not clean:
        return f"<li>{escape(empty)}</li>"
    return "".join(f"<li>{escape(item)}</li>" for item in clean)


def _table_rows(rows: list[tuple[str, str]]) -> str:
    return "".join(
        f"<tr><th>{escape(label)}</th><td>{escape(value)}</td></tr>"
        for label, value in rows
    )


def _price_chart_rows(fact_pack: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in fact_pack.get("price_tail") or []:
        if not isinstance(item, dict):
            continue
        date_value = _raw_text(item.get("trade_date") or item.get("date"), "")
        try:
            open_price = float(item.get("open"))
            close_price = float(item.get("close"))
            low_price = float(item.get("low"))
            high_price = float(item.get("high"))
        except Exception:
            continue
        rows.append(
            {
                "date": date_value[:10],
                "ohlc": [open_price, close_price, low_price, high_price],
                "volume": item.get("vol") or item.get("volume"),
            }
        )
    return rows[-90:]


def _financial_history_rows(fact_pack: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in fact_pack.get("financial_tail") or []:
        if isinstance(item, dict):
            rows.append(item)
    return rows[-8:]


def _render_step_grid(llm_result: dict[str, Any]) -> str:
    step_titles = {
        "step0": "任务锁定",
        "step1": "宏观与周期",
        "step2": "产业链拆解",
        "step3": "公司质量",
        "step4": "业绩弹性",
        "step5": "风险分析",
        "step6": "估值与时机",
        "step7": "对标分析",
        "step8": "跟踪计划",
    }
    step_analysis = llm_result.get("step_analysis") or {}
    cards: list[str] = []
    for key, title in step_titles.items():
        content = _raw_text(step_analysis.get(key), "")
        if not content:
            continue
        cards.append(
            '<article class="step-card">'
            f"<span>{escape(key.replace('step', 'Step '))}</span>"
            f"<h3>{escape(title)}</h3>"
            f"<p>{escape(content)}</p>"
            "</article>"
        )
    if not cards:
        return '<p class="muted">暂无 Step 0-8 结构化分析。</p>'
    return "".join(cards)


def _supplemental_block(supplemental: dict[str, Any], name: str) -> dict[str, Any]:
    block = supplemental.get(name) if isinstance(supplemental, dict) else None
    return block if isinstance(block, dict) else {"status": "missing", "items": []}


def _item_value(item: dict[str, Any], aliases: list[str], default: str = "") -> str:
    for alias in aliases:
        value = item.get(alias)
        text = _raw_text(value, "")
        if text:
            return text
    return default


def _render_evidence_block(
    title: str,
    block: dict[str, Any],
    headline_aliases: list[str],
    detail_fields: list[tuple[str, list[str]]],
    *,
    empty: str = "暂无可用记录",
    limit: int = 5,
) -> str:
    status = _raw_text(block.get("status"), "missing")
    items = [item for item in (block.get("items") or []) if isinstance(item, dict)][:limit]
    rows: list[str] = []
    for item in items:
        headline = _item_value(item, headline_aliases, "未命名记录")
        details = []
        for label, aliases in detail_fields:
            value = _item_value(item, aliases, "")
            if value:
                details.append(f"{label}: {value}")
        detail_text = " · ".join(details)
        rows.append(
            "<li>"
            f"<strong>{escape(headline)}</strong>"
            f"<small>{escape(detail_text)}</small>"
            "</li>"
        )
    if not rows:
        rows.append(f"<li><strong>{escape(empty)}</strong><small>{escape(_raw_text(block.get('error'), ''))}</small></li>")
    return (
        '<article class="evidence-card">'
        f"<h3>{escape(title)}<span>{escape(status)}</span></h3>"
        f"<ul>{''.join(rows)}</ul>"
        "</article>"
    )


def _render_supplemental_grid(supplemental: dict[str, Any]) -> str:
    business = _supplemental_block(supplemental, "business_composition")
    news = _supplemental_block(supplemental, "news")
    research = _supplemental_block(supplemental, "research_reports")
    money_flow = _supplemental_block(supplemental, "money_flow")
    lhb = _supplemental_block(supplemental, "lhb")
    peers = _supplemental_block(supplemental, "industry_peer_hint")
    return "".join(
        [
            _render_evidence_block(
                "主营构成",
                business,
                ["分类", "项目名称", "产品名称", "业务名称", "主营构成", "报告期"],
                [
                    ("日期", ["报告日期", "报告期", "日期"]),
                    ("收入", ["主营收入", "营业收入", "收入"]),
                    ("占比", ["收入比例", "主营收入占比", "营业收入比例"]),
                    ("毛利率", ["毛利率", "销售毛利率"]),
                ],
            ),
            _render_evidence_block(
                "近期新闻",
                news,
                ["新闻标题", "标题", "title"],
                [
                    ("时间", ["发布时间", "时间", "日期", "publish_time"]),
                    ("来源", ["文章来源", "来源", "source"]),
                ],
            ),
            _render_evidence_block(
                "机构研报",
                research,
                ["报告名称", "标题", "研报标题", "title"],
                [
                    ("机构", ["机构", "研究机构", "org", "source"]),
                    ("日期", ["发布日期", "报告日期", "日期", "publish_date"]),
                    ("评级", ["评级", "投资评级", "rating"]),
                ],
            ),
            _render_evidence_block(
                "资金流",
                money_flow,
                ["日期", "交易日期"],
                [
                    ("主力净流入", ["主力净流入-净额", "主力净流入", "主力净额"]),
                    ("主力占比", ["主力净流入-净占比", "主力净占比"]),
                    ("涨跌幅", ["涨跌幅", "涨跌幅(%)"]),
                ],
            ),
            _render_evidence_block(
                "龙虎榜",
                lhb,
                ["上榜日", "日期", "交易日期"],
                [
                    ("原因", ["解读", "上榜原因", "类型"]),
                    ("买入", ["买入金额", "买入总计"]),
                    ("卖出", ["卖出金额", "卖出总计"]),
                ],
            ),
            _render_evidence_block(
                "行业成分参考",
                peers,
                ["名称", "股票简称", "代码"],
                [
                    ("代码", ["代码", "股票代码"]),
                    ("涨跌幅", ["涨跌幅", "涨跌幅(%)"]),
                    ("市值", ["总市值", "流通市值"]),
                ],
            ),
        ]
    )


def render_stock_research_html(
    fact_pack: dict[str, Any] | None,
    llm_result: dict[str, Any] | None,
    *,
    report_md: str | None = None,
) -> str:
    """Render a deterministic standalone HTML report from FactPack and LLM JSON."""
    fact_pack = fact_pack if isinstance(fact_pack, dict) else {}
    normalized_llm = normalize_stock_research_llm_result(llm_result) or {}
    profile = fact_pack.get("profile") or {}
    price = fact_pack.get("price_metrics") or {}
    valuation = fact_pack.get("valuation_snapshot") or {}
    financial = (fact_pack.get("financial_metrics") or {}).get("latest") or {}
    quality = fact_pack.get("data_quality") or {}
    supplemental = fact_pack.get("supplemental") or {}

    stock_name = _raw_text(fact_pack.get("stock_name") or profile.get("name") or fact_pack.get("ts_code"), "未知股票")
    ts_code = _raw_text(fact_pack.get("ts_code") or profile.get("ts_code"), "-")
    asof_trade_date = _raw_text(fact_pack.get("asof_trade_date"), "-")
    generated_at = _raw_text(fact_pack.get("generated_at"), "-")
    verdict = _raw_text(normalized_llm.get("verdict"), "观察")
    risk_level = _raw_text(normalized_llm.get("risk_level"), "中")
    confidence = normalized_llm.get("confidence")
    quality_score = normalized_llm.get("quality_score") or {}
    chart_rows = _price_chart_rows(fact_pack)
    financial_rows = _financial_history_rows(fact_pack)
    chart_data_json = _json_script(chart_rows)
    financial_data_json = _json_script(financial_rows)
    supplemental_grid = _render_supplemental_grid(supplemental if isinstance(supplemental, dict) else {})

    metric_cards = "".join(
        [
            _metric_card("最新收盘", _num(price.get("latest_close")), f"截至 {asof_trade_date}"),
            _metric_card("近20日涨跌", _num(price.get("ret_20d_pct"), suffix="%"), "位置与节奏"),
            _metric_card("PE(TTM)", _num(valuation.get("pe_ttm")), "估值快照"),
            _metric_card("总市值", _num(valuation.get("total_mv_yi"), suffix=" 亿"), "规模"),
            _metric_card("ROE", _num(financial.get("roe"), suffix="%"), "盈利质量"),
            _metric_card("经营现金流", _num(financial.get("operating_cashflow_yi"), suffix=" 亿"), "现金创造"),
        ]
    )

    profile_rows = _table_rows(
        [
            ("行业/板块", f"{_raw_text(profile.get('industry'))} / {_raw_text(profile.get('market'))}"),
            ("上市日期", _raw_text(profile.get("list_date"))),
            ("主营业务", _raw_text(profile.get("main_business") or profile.get("business_scope"))),
            ("股东人数", _int_num(profile.get("holder_num"))),
            ("股东截止日", _raw_text(profile.get("holder_end_date"))),
        ]
    )
    valuation_rows = _table_rows(
        [
            ("PB", _num(valuation.get("pb"))),
            ("PS(TTM)", _num(valuation.get("ps_ttm"))),
            ("流通市值", _num(valuation.get("circ_mv_yi"), suffix=" 亿")),
            ("52周高点回撤", _num(price.get("drawdown_from_52w_high_pct"), suffix="%")),
            ("量比20日均量", _num(price.get("volume_ratio_20"))),
        ]
    )
    financial_rows_html = _table_rows(
        [
            ("最近财报期", _raw_text(financial.get("fina_end_date"))),
            ("毛利率", _num(financial.get("gross_margin"), suffix="%")),
            ("资产负债率", _num(financial.get("debt_to_assets"), suffix="%")),
            ("营收", _num(financial.get("total_revenue_yi"), suffix=" 亿")),
            ("净利润", _num(financial.get("net_profit_yi"), suffix=" 亿")),
        ]
    )

    data_quality_rows = _table_rows(
        [
            ("公司资料", f"{int(quality.get('profile_rows') or 0)} 行"),
            ("行情序列", f"{int(quality.get('daily_rows') or 0)} 行"),
            ("K线序列", f"{int(quality.get('kline_rows') or 0)} 行"),
            ("财务序列", f"{int(quality.get('financial_rows') or 0)} 行"),
            ("实时抓取", "已启用" if quality.get("allow_live_fetch") else "未启用"),
            ("补充证据", "已启用" if quality.get("supplemental_enabled") else "未启用"),
        ]
    )

    summary = _raw_text(normalized_llm.get("summary"), "暂无摘要")
    investment_thesis = _raw_text(normalized_llm.get("investment_thesis"), "暂无核心投资命题")
    valuation_view = _raw_text(normalized_llm.get("valuation_view"), "暂无估值分析")
    timing_view = _raw_text(normalized_llm.get("timing_view"), "暂无位置与节奏分析")
    report_md_block = (
        f"<details><summary>查看 Markdown 原文</summary><pre>{escape(report_md)}</pre></details>"
        if report_md
        else ""
    )

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{_text(stock_name)} {_text(ts_code)} 个股深度研究</title>
  {STOCK_RESEARCH_HTML_MARKER}
  <style>
    :root {{
      --bg: #f7f8fb;
      --surface: #ffffff;
      --surface-2: #f2f6f8;
      --ink: #17202a;
      --muted: #6c7680;
      --border: #dde4ea;
      --accent: #126e82;
      --accent-soft: #e1f2f4;
      --warn: #a86800;
      --warn-soft: #fff3d8;
      --risk: #b8324b;
      --risk-soft: #ffe5ea;
      --ok: #247a4d;
      --ok-soft: #e5f4eb;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Microsoft YaHei", sans-serif;
      line-height: 1.6;
    }}
    .report {{ max-width: 1180px; margin: 0 auto; padding: 28px 20px 42px; }}
    .hero {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 24px;
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 20px;
    }}
    .eyebrow {{ color: var(--muted); font-size: 13px; margin: 0 0 6px; }}
    h1 {{ font-size: 28px; line-height: 1.25; margin: 0 0 12px; letter-spacing: 0; }}
    h2 {{ font-size: 19px; margin: 0 0 14px; letter-spacing: 0; }}
    h3 {{ font-size: 15px; margin: 0 0 8px; letter-spacing: 0; }}
    p {{ margin: 0; }}
    .summary {{ max-width: 760px; color: #2c3a43; }}
    .badge-row {{ display: flex; gap: 8px; flex-wrap: wrap; margin-top: 16px; }}
    .badge {{
      display: inline-flex;
      align-items: center;
      min-height: 30px;
      padding: 4px 10px;
      border-radius: 999px;
      background: var(--surface-2);
      color: var(--ink);
      font-size: 13px;
      font-weight: 700;
    }}
    .badge.verdict {{ background: var(--accent-soft); color: var(--accent); }}
    .badge.risk-high {{ background: var(--risk-soft); color: var(--risk); }}
    .badge.risk-mid {{ background: var(--warn-soft); color: var(--warn); }}
    .badge.risk-low {{ background: var(--ok-soft); color: var(--ok); }}
    .score-panel {{
      min-width: 190px;
      padding: 16px;
      border-radius: 8px;
      background: var(--surface-2);
      border: 1px solid var(--border);
    }}
    .score-panel strong {{ display: block; font-size: 34px; line-height: 1; margin: 6px 0; }}
    .metric-grid {{
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 12px;
      margin: 18px 0;
    }}
    .metric-card {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 14px;
      min-height: 96px;
    }}
    .metric-label, .metric-card small, .muted {{ color: var(--muted); }}
    .metric-label {{ display: block; font-size: 12px; margin-bottom: 6px; }}
    .metric-card strong {{ display: block; font-size: 20px; margin-bottom: 5px; }}
    .grid-2 {{ display: grid; grid-template-columns: minmax(0, 1.25fr) minmax(0, .75fr); gap: 16px; }}
    .grid-3 {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 16px; }}
    .section {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 18px;
      margin-top: 16px;
    }}
    .chart {{ height: 360px; width: 100%; }}
    .chart-fallback {{
      height: 100%;
      display: grid;
      place-items: center;
      color: var(--muted);
      background: var(--surface-2);
      border-radius: 8px;
    }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ padding: 9px 8px; border-bottom: 1px solid var(--border); vertical-align: top; }}
    th {{ color: var(--muted); text-align: left; width: 34%; font-weight: 600; }}
    td {{ color: var(--ink); }}
    ul {{ margin: 0; padding-left: 18px; }}
    li + li {{ margin-top: 6px; }}
    .step-grid {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }}
    .step-card {{ border: 1px solid var(--border); border-radius: 8px; padding: 14px; background: #fbfcfd; }}
    .step-card span {{ color: var(--accent); font-size: 12px; font-weight: 700; }}
    .evidence-grid {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }}
    .evidence-card {{ border: 1px solid var(--border); border-radius: 8px; padding: 14px; background: #fbfcfd; }}
    .evidence-card h3 {{ display: flex; justify-content: space-between; gap: 8px; align-items: center; }}
    .evidence-card h3 span {{
      color: var(--muted);
      background: var(--surface-2);
      border-radius: 999px;
      padding: 2px 8px;
      font-size: 11px;
      font-weight: 700;
    }}
    .evidence-card li strong {{ display: block; font-size: 13px; }}
    .evidence-card li small {{ display: block; color: var(--muted); font-size: 12px; }}
    .footer {{ color: var(--muted); font-size: 12px; margin-top: 18px; }}
    details {{ margin-top: 16px; }}
    summary {{ cursor: pointer; color: var(--accent); font-weight: 700; }}
    pre {{
      white-space: pre-wrap;
      background: #101820;
      color: #e9eef2;
      border-radius: 8px;
      padding: 14px;
      overflow: auto;
    }}
    @media (max-width: 980px) {{
      .hero, .grid-2, .grid-3 {{ grid-template-columns: 1fr; }}
      .metric-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .step-grid, .evidence-grid {{ grid-template-columns: 1fr; }}
      .score-panel {{ min-width: 0; }}
    }}
  </style>
</head>
<body>
  <main class="report">
    <section class="hero">
      <div>
        <p class="eyebrow">自选股深度研究 · 数据日期 {_text(asof_trade_date)}</p>
        <h1>{_text(stock_name)}（{_text(ts_code)}）</h1>
        <p class="summary">{escape(summary)}</p>
        <div class="badge-row">
          <span class="badge verdict">综合判断：{escape(verdict)}</span>
          <span class="badge {'risk-high' if risk_level == '高' else 'risk-low' if risk_level == '低' else 'risk-mid'}">风险：{escape(risk_level)}</span>
          <span class="badge">置信度：{_int_num(confidence)}/100</span>
          <span class="badge">模型版本：{escape(STOCK_RESEARCH_LLM_SCHEMA_VERSION)}</span>
        </div>
      </div>
      <aside class="score-panel">
        <span class="muted">公司质地评分</span>
        <strong>{_int_num(quality_score.get("score"))}</strong>
        <span class="badge">等级 {escape(_raw_text(quality_score.get("grade"), "-"))}</span>
      </aside>
    </section>

    <section class="metric-grid">{metric_cards}</section>

    <section class="grid-2">
      <article class="section">
        <h2>K 线走势</h2>
        <div id="kline-chart" class="chart"><div class="chart-fallback">等待图表数据</div></div>
      </article>
      <article class="section">
        <h2>公司与估值快照</h2>
        <table>{profile_rows}{valuation_rows}</table>
      </article>
    </section>

    <section class="grid-3">
      <article class="section">
        <h2>核心投资命题</h2>
        <p>{escape(investment_thesis)}</p>
      </article>
      <article class="section">
        <h2>估值与赔率</h2>
        <p>{escape(valuation_view)}</p>
      </article>
      <article class="section">
        <h2>位置与节奏</h2>
        <p>{escape(timing_view)}</p>
      </article>
    </section>

    <section class="grid-3">
      <article class="section">
        <h2>关键证据</h2>
        <ul>{_list_items(normalized_llm.get("key_evidence"))}</ul>
      </article>
      <article class="section">
        <h2>主要风险</h2>
        <ul>{_list_items(normalized_llm.get("risk_factors"))}</ul>
      </article>
      <article class="section">
        <h2>后续跟踪清单</h2>
        <ul>{_list_items(normalized_llm.get("watch_items"))}</ul>
      </article>
    </section>

    <section class="section">
      <h2>补充证据层</h2>
      <div class="evidence-grid">{supplemental_grid}</div>
    </section>

    <section class="grid-2">
      <article class="section">
        <h2>Step 0-8 分析框架</h2>
        <div class="step-grid">{_render_step_grid(normalized_llm)}</div>
      </article>
      <article class="section">
        <h2>财务与数据质量</h2>
        <table>{financial_rows_html}{data_quality_rows}</table>
        <p class="footer">报告生成时间：{_text(generated_at)}。本报告仅供研究跟踪使用，不构成投资建议。</p>
      </article>
    </section>

    {report_md_block}
  </main>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
  <script>
    const klineRows = {chart_data_json};
    const financialRows = {financial_data_json};
    void financialRows;
    const chartEl = document.getElementById("kline-chart");
    if (chartEl && window.echarts && Array.isArray(klineRows) && klineRows.length > 0) {{
      chartEl.innerHTML = "";
      const chart = window.echarts.init(chartEl, null, {{ renderer: "canvas" }});
      chart.setOption({{
        animation: false,
        tooltip: {{ trigger: "axis" }},
        grid: {{ left: 48, right: 18, top: 24, bottom: 42 }},
        xAxis: {{ type: "category", data: klineRows.map(row => row.date), boundaryGap: true }},
        yAxis: {{ scale: true, splitLine: {{ lineStyle: {{ color: "#edf1f4" }} }} }},
        dataZoom: [{{ type: "inside" }}, {{ type: "slider", height: 18, bottom: 10 }}],
        series: [{{
          name: "K线",
          type: "candlestick",
          data: klineRows.map(row => row.ohlc),
          itemStyle: {{
            color: "#c3423f",
            color0: "#247a4d",
            borderColor: "#c3423f",
            borderColor0: "#247a4d"
          }}
        }}]
      }});
      window.addEventListener("resize", () => chart.resize());
    }} else if (chartEl) {{
      chartEl.innerHTML = '<div class="chart-fallback">暂无可渲染的 K 线数据</div>';
    }}
  </script>
</body>
</html>"""
