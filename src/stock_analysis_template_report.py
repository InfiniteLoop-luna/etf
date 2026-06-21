from __future__ import annotations

import logging
import re
from html import escape
from typing import Any, Callable

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.stock_analysis_template_report_store import (
    get_cached_template_report,
    save_template_report,
    today_report_date,
)
from src.stock_research_akshare_enrichment import should_enable_stock_research_akshare
from src.stock_research_fact_pack import build_stock_research_fact_pack
from src.stock_research_llm_analysis import (
    analyze_stock_research_payload,
    normalize_stock_research_llm_result,
)


logger = logging.getLogger(__name__)

FactPackBuilder = Callable[..., dict[str, Any]]
LLMAnalyzer = Callable[[dict[str, Any]], dict[str, Any] | None]
ChartDataLoader = Callable[..., dict[str, Any]]
FINANCIAL_CHART_MARKER = "[[TEMPLATE_FINANCIAL_CHARTS]]"


def _raw_text(value: Any, default: str = "-") -> str:
    text = str(value or "").strip()
    return text if text else default


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _fmt(value: Any, digits: int = 2, suffix: str = "") -> str:
    parsed = _to_float(value)
    if parsed is None:
        return "-"
    return f"{parsed:,.{digits}f}{suffix}"


def _fmt_bool_st(value: Any) -> str:
    if isinstance(value, bool):
        return "是" if value else "否"
    text = str(value or "").strip()
    if not text:
        return "-"
    return "是" if text.lower() in {"1", "true", "yes", "y", "是"} else text


def _normalize_date_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.notna(parsed):
        return parsed.strftime("%Y-%m-%d")
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text[:10]


def _read_sql_frame(engine: Engine, sql: str, params: dict[str, Any], source_name: str) -> pd.DataFrame:
    try:
        return pd.read_sql(text(sql), engine, params=params)
    except Exception as exc:
        logger.warning("Failed to load template report data from %s: %s", source_name, exc)
        return pd.DataFrame()


def _latest_period_rows(df: pd.DataFrame, date_col: str, value_cols: list[str]) -> pd.DataFrame:
    if df is None or df.empty or date_col not in df.columns:
        return pd.DataFrame()
    work = df.copy()
    work[date_col] = pd.to_datetime(work[date_col], errors="coerce")
    if "ann_date" in work.columns:
        work["ann_date"] = pd.to_datetime(work["ann_date"], errors="coerce")
    for column in value_cols:
        if column in work.columns:
            work[column] = pd.to_numeric(work[column], errors="coerce")
    work = work.dropna(subset=[date_col])
    if work.empty:
        return work
    sort_cols = [date_col] + (["ann_date"] if "ann_date" in work.columns else [])
    work = work.sort_values(sort_cols, na_position="first")
    return work.drop_duplicates(subset=[date_col], keep="last").sort_values(date_col)


def _pct_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in {None, 0}:
        return None
    return (current / previous - 1.0) * 100.0


def _scaled(value: Any, scale: float) -> float | None:
    parsed = _to_float(value)
    if parsed is None:
        return None
    return parsed / scale


def _annual_series(df: pd.DataFrame, value_col: str, *, scale: float = 1.0, limit: int = 8) -> list[dict[str, Any]]:
    work = _latest_period_rows(df, "end_date", [value_col])
    if work.empty or value_col not in work.columns:
        return []
    annual = work[
        work["end_date"].dt.month.eq(12)
        & work["end_date"].dt.day.eq(31)
    ].copy()
    rows: list[dict[str, Any]] = []
    previous_value: float | None = None
    for _, row in annual.iterrows():
        value = _scaled(row.get(value_col), scale)
        if value is None:
            continue
        rows.append(
            {
                "label": str(int(row["end_date"].year)),
                "value": value,
                "growth_pct": _pct_change(value, previous_value),
            }
        )
        previous_value = value
    return rows[-limit:]


def _quarterly_single_period_series(
    df: pd.DataFrame,
    value_col: str,
    *,
    scale: float = 1.0,
    limit: int = 12,
) -> list[dict[str, Any]]:
    work = _latest_period_rows(df, "end_date", [value_col])
    if work.empty or value_col not in work.columns:
        return []
    work["year"] = work["end_date"].dt.year.astype(int)
    work["quarter"] = work["end_date"].dt.quarter.astype(int)

    cumulative_by_period: dict[tuple[int, int], float] = {}
    for _, row in work.iterrows():
        raw_value = _to_float(row.get(value_col))
        if raw_value is not None:
            cumulative_by_period[(int(row["year"]), int(row["quarter"]))] = raw_value

    rows: list[dict[str, Any]] = []
    single_by_period: dict[tuple[int, int], float] = {}
    for _, row in work.iterrows():
        year = int(row["year"])
        quarter = int(row["quarter"])
        cumulative_value = cumulative_by_period.get((year, quarter))
        if cumulative_value is None:
            continue
        if quarter == 1:
            single_value = cumulative_value
        else:
            previous_cumulative = cumulative_by_period.get((year, quarter - 1))
            if previous_cumulative is None:
                continue
            single_value = cumulative_value - previous_cumulative
        value = single_value / scale
        single_by_period[(year, quarter)] = value
        rows.append(
            {
                "label": f"{year}Q{quarter}",
                "value": value,
                "year": year,
                "quarter": quarter,
            }
        )

    for row in rows:
        previous = single_by_period.get((int(row["year"]) - 1, int(row["quarter"])))
        row["growth_pct"] = _pct_change(_to_float(row.get("value")), previous)
    return rows[-limit:]


def _daily_line_series(
    df: pd.DataFrame,
    value_col: str,
    *,
    scale: float = 1.0,
    limit: int = 180,
    fallback_col: str | None = None,
) -> list[dict[str, Any]]:
    value_cols = [value_col] + ([fallback_col] if fallback_col else [])
    work = _latest_period_rows(df, "trade_date", [column for column in value_cols if column])
    if work.empty:
        return []
    rows: list[dict[str, Any]] = []
    for _, row in work.tail(limit).iterrows():
        value = _scaled(row.get(value_col), scale)
        if value is None and fallback_col:
            value = _scaled(row.get(fallback_col), scale)
        if value is None:
            continue
        rows.append(
            {
                "label": pd.to_datetime(row["trade_date"]).strftime("%Y-%m-%d"),
                "value": value,
            }
        )
    return rows


def _chart(
    chart_id: str,
    title: str,
    kind: str,
    rows: list[dict[str, Any]],
    *,
    value_label: str,
    unit: str,
    source: str,
    line_label: str = "增长率",
    line_unit: str = "%",
    empty_reason: str = "",
) -> dict[str, Any]:
    has_values = any(_to_float(row.get("value")) is not None for row in rows)
    return {
        "id": chart_id,
        "title": title,
        "kind": kind,
        "value_label": value_label,
        "unit": unit,
        "line_label": line_label,
        "line_unit": line_unit,
        "source": source,
        "rows": rows if has_values else [],
        "empty_reason": empty_reason or f"{source} 暂无可用序列。",
    }


def _latest_daily_snapshot(daily_df: pd.DataFrame) -> dict[str, Any]:
    work = _latest_period_rows(
        daily_df,
        "trade_date",
        ["pe", "pe_ttm", "dv_ratio", "dv_ttm", "total_mv", "circ_mv", "total_share", "float_share"],
    )
    if work.empty:
        return {}
    latest = work.iloc[-1]
    return {
        "trade_date": pd.to_datetime(latest["trade_date"]).strftime("%Y-%m-%d"),
        "pe": _to_float(latest.get("pe")),
        "pe_ttm": _to_float(latest.get("pe_ttm")),
        "dv_ratio": _to_float(latest.get("dv_ratio")),
        "dv_ttm": _to_float(latest.get("dv_ttm")),
        "total_mv_yi": _scaled(latest.get("total_mv"), 10000.0),
        "circ_mv_yi": _scaled(latest.get("circ_mv"), 10000.0),
        "total_share_yi": _scaled(latest.get("total_share"), 10000.0),
        "float_share_yi": _scaled(latest.get("float_share"), 10000.0),
    }


def load_stock_analysis_template_chart_data(
    ts_code: str,
    *,
    engine: Engine,
    asof_trade_date: str | None = None,
) -> dict[str, Any]:
    ts_code_key = str(ts_code or "").strip().upper()
    asof_date = _normalize_date_text(asof_trade_date)
    if not ts_code_key:
        return {"charts": [], "latest": {}, "source_rows": {}}

    financial_params: dict[str, Any] = {"ts_code": ts_code_key}
    financial_date_filter = ""
    daily_date_filter = ""
    if asof_date:
        financial_date_filter = " AND end_date <= :asof_trade_date"
        daily_date_filter = " AND trade_date <= :asof_trade_date"
        financial_params["asof_trade_date"] = asof_date
    daily_params = dict(financial_params)

    income_df = _read_sql_frame(
        engine,
        f"""
        SELECT ts_code, end_date, ann_date, total_revenue,
               COALESCE(n_income_attr_p, n_income) AS net_profit
        FROM vw_ts_stock_income
        WHERE ts_code = :ts_code{financial_date_filter}
        ORDER BY end_date, ann_date
        """,
        financial_params,
        "vw_ts_stock_income",
    )
    fina_df = _read_sql_frame(
        engine,
        f"""
        SELECT ts_code, end_date, ann_date, profit_dedt
        FROM vw_ts_stock_fina_indicator
        WHERE ts_code = :ts_code{financial_date_filter}
        ORDER BY end_date, ann_date
        """,
        financial_params,
        "vw_ts_stock_fina_indicator",
    )
    cashflow_df = _read_sql_frame(
        engine,
        f"""
        SELECT ts_code, end_date, ann_date, n_cashflow_act
        FROM vw_ts_stock_cashflow
        WHERE ts_code = :ts_code{financial_date_filter}
        ORDER BY end_date, ann_date
        """,
        financial_params,
        "vw_ts_stock_cashflow",
    )
    daily_df = _read_sql_frame(
        engine,
        f"""
        SELECT ts_code, trade_date, pe, pe_ttm, dv_ratio, dv_ttm,
               total_mv, circ_mv, total_share, float_share
        FROM vw_ts_stock_daily_basic
        WHERE ts_code = :ts_code{daily_date_filter}
        ORDER BY trade_date
        """,
        daily_params,
        "vw_ts_stock_daily_basic",
    )

    charts = [
        _chart(
            "revenue_annual",
            "图1：收入（柱状图）及增长率（折线图）（年度）",
            "bar_line",
            _annual_series(income_df, "total_revenue", scale=100000000.0),
            value_label="营业收入",
            unit="亿元",
            source="vw_ts_stock_income.total_revenue",
        ),
        _chart(
            "revenue_quarterly",
            "图2：收入及增长率（季度）",
            "bar_line",
            _quarterly_single_period_series(income_df, "total_revenue", scale=100000000.0),
            value_label="单季营业收入",
            unit="亿元",
            source="vw_ts_stock_income.total_revenue",
        ),
        _chart(
            "net_profit_annual",
            "图3：净利润及增长率（年度）",
            "bar_line",
            _annual_series(income_df, "net_profit", scale=100000000.0),
            value_label="净利润",
            unit="亿元",
            source="vw_ts_stock_income.n_income_attr_p/n_income",
        ),
        _chart(
            "net_profit_quarterly",
            "图4：净利润及增长率（季度）",
            "bar_line",
            _quarterly_single_period_series(income_df, "net_profit", scale=100000000.0),
            value_label="单季净利润",
            unit="亿元",
            source="vw_ts_stock_income.n_income_attr_p/n_income",
        ),
        _chart(
            "deducted_profit_annual",
            "图5：扣非净利润及增长率（年度）",
            "bar_line",
            _annual_series(fina_df, "profit_dedt", scale=100000000.0),
            value_label="扣非净利润",
            unit="亿元",
            source="vw_ts_stock_fina_indicator.profit_dedt",
        ),
        _chart(
            "deducted_profit_quarterly",
            "图6：扣非净利润及增长率（季度）",
            "bar_line",
            _quarterly_single_period_series(fina_df, "profit_dedt", scale=100000000.0),
            value_label="单季扣非净利润",
            unit="亿元",
            source="vw_ts_stock_fina_indicator.profit_dedt",
        ),
        _chart(
            "operating_cashflow_quarterly",
            "图7：经营性现金流净额（季度）",
            "bar",
            _quarterly_single_period_series(cashflow_df, "n_cashflow_act", scale=100000000.0),
            value_label="单季经营性现金流净额",
            unit="亿元",
            source="vw_ts_stock_cashflow.n_cashflow_act",
        ),
        _chart(
            "static_pe",
            "图8：静态市盈率",
            "line",
            _daily_line_series(daily_df, "pe"),
            value_label="静态市盈率",
            unit="倍",
            source="vw_ts_stock_daily_basic.pe",
        ),
        _chart(
            "dynamic_pe",
            "图9：动态市盈率",
            "line",
            _daily_line_series(daily_df, "pe_ttm"),
            value_label="动态市盈率",
            unit="倍",
            source="vw_ts_stock_daily_basic.pe_ttm",
        ),
        _chart(
            "dividend_yield",
            "图10：股息率",
            "line",
            _daily_line_series(daily_df, "dv_ttm", fallback_col="dv_ratio"),
            value_label="股息率",
            unit="%",
            source="vw_ts_stock_daily_basic.dv_ttm/dv_ratio",
        ),
        _chart(
            "total_market_value",
            "图11：总市值",
            "line",
            _daily_line_series(daily_df, "total_mv", scale=10000.0),
            value_label="总市值",
            unit="亿元",
            source="vw_ts_stock_daily_basic.total_mv",
        ),
        _chart(
            "circulating_market_value",
            "图12：流通市值",
            "line",
            _daily_line_series(daily_df, "circ_mv", scale=10000.0),
            value_label="流通市值",
            unit="亿元",
            source="vw_ts_stock_daily_basic.circ_mv",
        ),
        _chart(
            "total_share",
            "图13：总股本",
            "line",
            _daily_line_series(daily_df, "total_share", scale=10000.0),
            value_label="总股本",
            unit="亿股",
            source="vw_ts_stock_daily_basic.total_share",
        ),
        _chart(
            "float_share",
            "图14：流通股本",
            "line",
            _daily_line_series(daily_df, "float_share", scale=10000.0),
            value_label="流通股本",
            unit="亿股",
            source="vw_ts_stock_daily_basic.float_share",
        ),
    ]
    return {
        "charts": charts,
        "latest": _latest_daily_snapshot(daily_df),
        "source_rows": {
            "vw_ts_stock_income": int(len(income_df)) if income_df is not None else 0,
            "vw_ts_stock_fina_indicator": int(len(fina_df)) if fina_df is not None else 0,
            "vw_ts_stock_cashflow": int(len(cashflow_df)) if cashflow_df is not None else 0,
            "vw_ts_stock_daily_basic": int(len(daily_df)) if daily_df is not None else 0,
        },
    }


def _markdown_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    if not rows:
        rows = [["-" for _ in headers]]
    return [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
        *["| " + " | ".join(str(value) for value in row) + " |" for row in rows],
    ]


def _inline_html(value: str) -> str:
    escaped = escape(str(value or ""))
    return re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)


def _markdown_table_to_html(table_lines: list[str]) -> str:
    if len(table_lines) < 2:
        return ""
    rows = [
        [cell.strip() for cell in line.strip().strip("|").split("|")]
        for line in table_lines
        if line.strip().startswith("|")
    ]
    if not rows:
        return ""
    headers = rows[0]
    body_rows = rows[2:] if len(rows) >= 2 else []
    head_html = "".join(f"<th>{_inline_html(header)}</th>" for header in headers)
    body_html = "".join(
        "<tr>" + "".join(f"<td>{_inline_html(cell)}</td>" for cell in row) + "</tr>"
        for row in body_rows
    )
    return f"<table><thead><tr>{head_html}</tr></thead><tbody>{body_html}</tbody></table>"


def _markdown_to_body_html(markdown_text: str) -> str:
    lines = str(markdown_text or "").splitlines()
    html: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i].rstrip()
        stripped = line.strip()
        if not stripped:
            i += 1
            continue
        if stripped == "---":
            html.append("<hr>")
            i += 1
            continue
        if stripped.startswith("|"):
            table_lines: list[str] = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                table_lines.append(lines[i])
                i += 1
            html.append(_markdown_table_to_html(table_lines))
            continue
        if stripped.startswith("- "):
            items: list[str] = []
            while i < len(lines) and lines[i].strip().startswith("- "):
                items.append(lines[i].strip()[2:])
                i += 1
            html.append("<ul>" + "".join(f"<li>{_inline_html(item)}</li>" for item in items) + "</ul>")
            continue
        if stripped.startswith("> "):
            html.append(f'<blockquote>{_inline_html(stripped[2:].strip())}</blockquote>')
            i += 1
            continue
        if stripped.startswith("### "):
            html.append(f"<h3>{_inline_html(stripped[4:].strip())}</h3>")
            i += 1
            continue
        if stripped.startswith("## "):
            html.append(f"<h2>{_inline_html(stripped[3:].strip())}</h2>")
            i += 1
            continue
        if stripped.startswith("# "):
            html.append(f"<h1>{_inline_html(stripped[2:].strip())}</h1>")
            i += 1
            continue
        html.append(f"<p>{_inline_html(stripped)}</p>")
        i += 1
    return "\n".join(html)


def _svg_text(value: Any) -> str:
    return escape(str(value or ""))


def _chart_bounds(values: list[float]) -> tuple[float, float]:
    valid = [float(value) for value in values if _to_float(value) is not None]
    if not valid:
        return 0.0, 1.0
    low = min(valid + [0.0])
    high = max(valid + [0.0])
    if low == high:
        padding = max(abs(high) * 0.15, 1.0)
        return low - padding, high + padding
    padding = (high - low) * 0.12
    return low - padding, high + padding


def _chart_latest_caption(chart: dict[str, Any], rows: list[dict[str, Any]]) -> str:
    latest = rows[-1] if rows else {}
    value = _to_float(latest.get("value"))
    growth = _to_float(latest.get("growth_pct"))
    label = _raw_text(latest.get("label"), "-")
    value_label = _raw_text(chart.get("value_label"), "指标")
    unit = _raw_text(chart.get("unit"), "")
    parts = [f"{label} {value_label} {_fmt(value)}{unit if value is not None else ''}"]
    if growth is not None:
        parts.append(f"{_raw_text(chart.get('line_label'), '增长率')} {_fmt(growth, suffix='%')}")
    return "，".join(parts)


def _render_chart_svg(chart: dict[str, Any]) -> str:
    rows = [
        row for row in (chart.get("rows") or [])
        if isinstance(row, dict) and _to_float(row.get("value")) is not None
    ]
    if not rows:
        reason = _raw_text(chart.get("empty_reason"), "暂无可绘制数据。")
        source = _raw_text(chart.get("source"), "-")
        return (
            '<div class="chart-empty">'
            f"<p>数据缺口：{escape(reason)}</p>"
            f"<p>数据源：{escape(source)}</p>"
            "</div>"
        )

    width = 760
    height = 270
    left = 58
    right = 704
    top = 28
    bottom = 214
    plot_width = right - left
    plot_height = bottom - top
    values = [_to_float(row.get("value")) or 0.0 for row in rows]
    value_min, value_max = _chart_bounds(values)

    def y_left(value: float) -> float:
        return bottom - ((value - value_min) / (value_max - value_min)) * plot_height

    def x_at(index: int) -> float:
        if len(rows) == 1:
            return left + plot_width / 2
        return left + index * (plot_width / (len(rows) - 1))

    zero_y = y_left(0.0)
    pieces: list[str] = [
        f'<svg class="chart-svg" viewBox="0 0 {width} {height}" role="img" aria-label="{_svg_text(chart.get("title"))}">',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{bottom}" class="axis"/>',
        f'<line x1="{left}" y1="{bottom}" x2="{right}" y2="{bottom}" class="axis"/>',
        f'<line x1="{left}" y1="{zero_y:.1f}" x2="{right}" y2="{zero_y:.1f}" class="zero-line"/>',
        f'<text x="{left}" y="18" class="axis-label">{_svg_text(chart.get("unit"))}</text>',
        f'<text x="{left}" y="{bottom + 30}" class="tick-label">{_svg_text(rows[0].get("label"))}</text>',
        f'<text x="{right}" y="{bottom + 30}" text-anchor="end" class="tick-label">{_svg_text(rows[-1].get("label"))}</text>',
        f'<text x="{left}" y="{top + 4}" class="tick-label">{_svg_text(_fmt(value_max))}</text>',
        f'<text x="{left}" y="{bottom - 4}" class="tick-label">{_svg_text(_fmt(value_min))}</text>',
    ]

    chart_kind = str(chart.get("kind") or "line")
    if chart_kind in {"bar", "bar_line"}:
        step = plot_width / max(len(rows), 1)
        bar_width = max(min(step * 0.48, 30), 6)
        for index, row in enumerate(rows):
            value = _to_float(row.get("value")) or 0.0
            x = x_at(index) - bar_width / 2
            y = y_left(value)
            rect_y = min(y, zero_y)
            rect_height = max(abs(zero_y - y), 1.0)
            css_class = "bar-positive" if value >= 0 else "bar-negative"
            pieces.append(
                f'<rect x="{x:.1f}" y="{rect_y:.1f}" width="{bar_width:.1f}" height="{rect_height:.1f}" class="{css_class}"/>'
            )

    if chart_kind == "line":
        points = [(x_at(index), y_left(_to_float(row.get("value")) or 0.0)) for index, row in enumerate(rows)]
        if len(points) > 1:
            point_str = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
            pieces.append(f'<polyline points="{point_str}" class="value-line"/>')
        for x, y in points[-24:]:
            pieces.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="2.4" class="value-point"/>')

    growth_rows = [
        (index, _to_float(row.get("growth_pct")))
        for index, row in enumerate(rows)
        if _to_float(row.get("growth_pct")) is not None
    ]
    if chart_kind == "bar_line" and growth_rows:
        growth_values = [float(value) for _, value in growth_rows if value is not None]
        growth_min, growth_max = _chart_bounds(growth_values)

        def y_right(value: float) -> float:
            return bottom - ((value - growth_min) / (growth_max - growth_min)) * plot_height

        pieces.extend(
            [
                f'<line x1="{right}" y1="{top}" x2="{right}" y2="{bottom}" class="axis axis-right"/>',
                f'<text x="{right}" y="18" text-anchor="end" class="axis-label">{_svg_text(chart.get("line_unit"))}</text>',
                f'<text x="{right}" y="{top + 4}" text-anchor="end" class="tick-label">{_svg_text(_fmt(growth_max, suffix="%"))}</text>',
                f'<text x="{right}" y="{bottom - 4}" text-anchor="end" class="tick-label">{_svg_text(_fmt(growth_min, suffix="%"))}</text>',
            ]
        )
        points = [(x_at(index), y_right(float(value))) for index, value in growth_rows if value is not None]
        if len(points) > 1:
            point_str = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
            pieces.append(f'<polyline points="{point_str}" class="growth-line"/>')
        for x, y in points:
            pieces.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="2.8" class="growth-point"/>')

    if len(rows) > 2:
        step = max(1, len(rows) // 5)
        for index, row in enumerate(rows):
            if index not in {0, len(rows) - 1} and index % step == 0:
                pieces.append(
                    f'<text x="{x_at(index):.1f}" y="{bottom + 30}" text-anchor="middle" class="tick-label">{_svg_text(row.get("label"))}</text>'
                )
    pieces.append("</svg>")
    return "\n".join(pieces)


def _render_financial_chart_section(chart_data: dict[str, Any] | None) -> str:
    chart_data = chart_data if isinstance(chart_data, dict) else {}
    charts = [chart for chart in (chart_data.get("charts") or []) if isinstance(chart, dict)]
    source_rows = chart_data.get("source_rows") if isinstance(chart_data.get("source_rows"), dict) else {}
    if not charts:
        return (
            '<section class="financial-chart-section">'
            "<h3>模板财务图表</h3>"
            "<blockquote>数据缺口：尚未查询到可绘制的财务与估值序列。</blockquote>"
            "</section>"
        )
    source_text = "；".join(f"{escape(str(key))}={int(value or 0)} 行" for key, value in source_rows.items())
    cards: list[str] = []
    for chart in charts:
        rows = [row for row in (chart.get("rows") or []) if isinstance(row, dict) and _to_float(row.get("value")) is not None]
        caption = _chart_latest_caption(chart, rows) if rows else _raw_text(chart.get("empty_reason"), "暂无可绘制数据。")
        cards.append(
            "\n".join(
                [
                    f'<div class="chart-card" id="chart-{escape(str(chart.get("id") or ""))}">',
                    f'<h3>{escape(str(chart.get("title") or ""))}</h3>',
                    _render_chart_svg(chart),
                    '<div class="chart-legend">',
                    f'<span><i class="legend-bar"></i>{escape(str(chart.get("value_label") or ""))}</span>',
                    f'<span><i class="legend-line"></i>{escape(str(chart.get("line_label") or ""))}</span>' if chart.get("kind") == "bar_line" else "",
                    "</div>",
                    f'<p class="chart-caption">{escape(caption)}</p>',
                    f'<p class="chart-source">数据源：{escape(str(chart.get("source") or "-"))}</p>',
                    "</div>",
                ]
            )
        )
    return "\n".join(
        [
            '<section class="financial-chart-section">',
            "<h3>模板财务图表</h3>",
            f'<p class="chart-source">已查询数据库：{source_text or "暂无行数信息"}</p>',
            '<div class="chart-grid">',
            *cards,
            "</div>",
            "</section>",
        ]
    )


def _price_frame(fact_pack: dict[str, Any]) -> pd.DataFrame:
    rows = [item for item in fact_pack.get("price_tail") or [] if isinstance(item, dict)]
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    if "trade_date" not in frame.columns or "close" not in frame.columns:
        return pd.DataFrame()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    for column in ["open", "close", "high", "low", "vol", "volume"]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["trade_date", "close"]).sort_values("trade_date")
    return frame


def _latest_ma(close: pd.Series, window: int) -> float | None:
    if close.empty or len(close) < window:
        return None
    return _to_float(close.rolling(window).mean().iloc[-1])


def _latest_ema(close: pd.Series, span: int) -> float | None:
    if close.empty:
        return None
    return _to_float(close.ewm(span=span, adjust=False).mean().iloc[-1])


def _cross_signal(short_line: pd.Series, long_line: pd.Series) -> str:
    if len(short_line) < 2 or len(long_line) < 2:
        return "样本不足"
    previous_short = _to_float(short_line.iloc[-2])
    previous_long = _to_float(long_line.iloc[-2])
    current_short = _to_float(short_line.iloc[-1])
    current_long = _to_float(long_line.iloc[-1])
    if None in {previous_short, previous_long, current_short, current_long}:
        return "样本不足"
    if previous_short <= previous_long and current_short > current_long:
        return "现金叉"
    if previous_short >= previous_long and current_short < current_long:
        return "现死叉"
    return "未触发"


def _technical_snapshot(fact_pack: dict[str, Any]) -> dict[str, str]:
    frame = _price_frame(fact_pack)
    if frame.empty:
        return {
            "latest_date": "-",
            "latest_close": "-",
            "ma_summary": "暂无足够日线数据。",
            "ema_summary": "暂无足够日线数据。",
            "macd_summary": "暂无足够日线数据。",
            "volume_summary": "暂无成交量数据。",
            "daily_summary": "当前底稿缺少可分析的日线序列。",
            "weekly_summary": "当前底稿缺少可分析的周线序列。",
            "monthly_summary": "当前底稿缺少可分析的月线序列。",
        }

    close = frame["close"].astype(float)
    latest = frame.iloc[-1]
    ma_values = {window: _latest_ma(close, window) for window in [5, 10, 20, 30, 60, 120, 233, 250]}
    ema5 = _latest_ema(close, 5)
    ema30 = _latest_ema(close, 30)
    exp12 = close.ewm(span=12, adjust=False).mean()
    exp26 = close.ewm(span=26, adjust=False).mean()
    dif = exp12 - exp26
    dea = dif.ewm(span=9, adjust=False).mean()
    macd = (dif - dea) * 2
    ma5 = close.rolling(5).mean()
    ma20 = close.rolling(20).mean()
    ma_cross = _cross_signal(ma5.dropna(), ma20.dropna())
    macd_cross = _cross_signal(dif.dropna(), dea.dropna())
    latest_close = _to_float(latest.get("close"))
    ma20_value = ma_values.get(20)
    ret20 = _to_float((fact_pack.get("price_metrics") or {}).get("ret_20d_pct"))
    if latest_close is not None and ma20_value is not None:
        daily_bias = (latest_close / ma20_value - 1.0) * 100.0 if ma20_value else None
        daily_summary = f"收盘价相对 MA20 偏离 {_fmt(daily_bias, suffix='%')}，MA5/MA20 信号为{ma_cross}。"
    elif ret20 is not None:
        daily_summary = f"近20日涨跌幅 {_fmt(ret20, suffix='%')}，均线样本不足时以阶段涨跌幅辅助判断。"
    else:
        daily_summary = "日线样本不足，需补齐更长行情序列后再判断趋势强弱。"

    if "vol" in frame.columns and pd.notna(frame["vol"].iloc[-1]):
        vol_col = frame["vol"]
    elif "volume" in frame.columns and pd.notna(frame["volume"].iloc[-1]):
        vol_col = frame["volume"]
    else:
        vol_col = pd.Series(dtype=float)
    if not vol_col.empty:
        latest_vol = _to_float(vol_col.iloc[-1])
        avg_vol = _to_float(vol_col.tail(20).mean())
        volume_summary = f"最新成交量 {_fmt(latest_vol, 0)}，近20条均量 {_fmt(avg_vol, 0)}。"
    else:
        volume_summary = "底稿未覆盖成交量，需补充成交量后判断量价配合。"

    weekly_summary = _resample_trend_summary(frame, "W-FRI", "周线")
    monthly_summary = _resample_trend_summary(frame, "ME", "月线")
    ma_summary = "；".join(
        f"MA{window}={_fmt(value)}" for window, value in ma_values.items() if value is not None
    ) or "MA 样本不足"
    ema_summary = f"EMA5={_fmt(ema5)}；EMA30={_fmt(ema30)}"
    macd_summary = f"DIF={_fmt(dif.iloc[-1], 4)}；DEA={_fmt(dea.iloc[-1], 4)}；MACD={_fmt(macd.iloc[-1], 4)}；信号：{macd_cross}"
    return {
        "latest_date": pd.to_datetime(latest["trade_date"]).strftime("%Y-%m-%d"),
        "latest_close": _fmt(latest_close),
        "ma_summary": ma_summary,
        "ema_summary": ema_summary,
        "macd_summary": macd_summary,
        "volume_summary": volume_summary,
        "daily_summary": daily_summary,
        "weekly_summary": weekly_summary,
        "monthly_summary": monthly_summary,
    }


def _resample_trend_summary(frame: pd.DataFrame, rule: str, label: str) -> str:
    if frame.empty or len(frame) < 2:
        return f"{label}样本不足。"
    resampled = (
        frame.set_index("trade_date")["close"]
        .resample(rule)
        .last()
        .dropna()
    )
    if len(resampled) < 2:
        return f"{label}样本不足。"
    latest = _to_float(resampled.iloc[-1])
    previous = _to_float(resampled.iloc[-2])
    if latest is None or previous in {None, 0}:
        return f"{label}样本不足。"
    change = (latest / previous - 1.0) * 100.0
    direction = "走强" if change > 0 else "走弱" if change < 0 else "持平"
    return f"{label}最近一期收盘 {_fmt(latest)}，较上一期 {_fmt(change, suffix='%')}，趋势{direction}。"


def _business_composition_rows(fact_pack: dict[str, Any]) -> list[list[str]]:
    supplemental = fact_pack.get("supplemental") if isinstance(fact_pack.get("supplemental"), dict) else {}
    block = supplemental.get("business_composition") if isinstance(supplemental, dict) else None
    items = [item for item in ((block or {}).get("items") or []) if isinstance(item, dict)]
    rows: list[list[str]] = []
    for item in items[:8]:
        name = _first_text(item, ["主营构成", "项目名称", "产品名称", "业务名称", "分类", "报告期"])
        revenue = _first_text(item, ["主营收入", "营业收入", "收入", "金额"])
        ratio = _first_text(item, ["收入比例", "主营收入占比", "营业收入比例", "占比"])
        margin = _first_text(item, ["毛利率", "销售毛利率"])
        rows.append([name or "-", revenue or "-", ratio or "-", margin or "-"])
    if rows:
        return rows
    profile = fact_pack.get("profile") or {}
    business = _raw_text(profile.get("main_business") or profile.get("business_scope"), "")
    if business:
        return [[business, "-", "-", "-"]]
    return []


def _first_text(item: dict[str, Any], aliases: list[str]) -> str:
    for alias in aliases:
        text = _raw_text(item.get(alias), "")
        if text:
            return text
    return ""


def _append_template_gap(lines: list[str], message: str) -> None:
    lines.append(f"> 数据缺口：{message}")


def render_stock_analysis_template_markdown(
    fact_pack: dict[str, Any] | None,
    llm_result: dict[str, Any] | None,
) -> str:
    """Render a markdown report following the local docx stock-analysis template order."""
    fact_pack = fact_pack if isinstance(fact_pack, dict) else {}
    normalized_llm = normalize_stock_research_llm_result(llm_result) or {}
    profile = fact_pack.get("profile") or {}
    price = fact_pack.get("price_metrics") or {}
    valuation = fact_pack.get("valuation_snapshot") or {}
    chart_data = fact_pack.get("template_chart_data") if isinstance(fact_pack.get("template_chart_data"), dict) else {}
    chart_latest = chart_data.get("latest") if isinstance(chart_data.get("latest"), dict) else {}
    financial = (fact_pack.get("financial_metrics") or {}).get("latest") or {}
    quality = fact_pack.get("data_quality") or {}
    technical = _technical_snapshot(fact_pack)
    stock_name = _raw_text(fact_pack.get("stock_name") or profile.get("name") or fact_pack.get("ts_code"), "未知股票")
    ts_code = _raw_text(fact_pack.get("ts_code") or profile.get("ts_code"), "-")
    asof_trade_date = _raw_text(fact_pack.get("asof_trade_date"), "-")
    generated_at = _raw_text(fact_pack.get("generated_at"), "-")

    lines: list[str] = [
        f"# {stock_name}公司分析报告",
        "",
        "## 基本面分析",
        "",
        f"### {stock_name}公司（{ts_code}）基本信息",
        "",
        "数据&信息来源——WealthSpark 决策看板 & tushare，* 标注是否曾经ST",
        "",
        *_markdown_table(
            ["字段", "内容"],
            [
                ["股票代码", ts_code],
                ["数据截止日", asof_trade_date],
                ["所属行业", _raw_text(profile.get("industry"))],
                ["市场板块", _raw_text(profile.get("market"))],
                ["是否曾经ST", _fmt_bool_st(profile.get("has_ever_st"))],
                ["实际控制人", _raw_text(profile.get("act_name") or profile.get("controller"))],
                ["行业排名", "暂无行业排名数据，需接入同行指标排名后补充。"],
                ["美国同类公司对比", "暂无海外映射数据，需接入可比公司库后补充。"],
            ],
        ),
        "",
        f"### {stock_name}公司所属行业、业务范围及产品",
        "",
        "数据&信息来源——WealthSpark 决策看板",
        "",
        f"- 所属行业：{_raw_text(profile.get('industry'))}",
        f"- 业务范围：{_raw_text(profile.get('business_scope'))}",
        f"- 主营业务：{_raw_text(profile.get('main_business') or profile.get('business_scope'))}",
        "",
        f"### {stock_name}公司产品应用领域",
        "",
        "数据&信息来源——最新一期年度报告/主营业务分析&公司未来发展的展望",
        "",
        "- 当前底稿优先使用主营业务和主营构成描述产品应用领域；若需精确到应用场景，请补充年报经营计划或产品矩阵数据。",
        "",
        "### 高管股东增减持情况",
        "",
    ]
    _append_template_gap(lines, "当前 FactPack 未包含高管股东增减持明细。")
    lines.extend([
        "",
        "### 公司历年融资情况（价格、金额）",
        "",
    ])
    _append_template_gap(lines, "当前 FactPack 未包含历年融资价格与金额。")
    lines.extend([
        "",
        "### 公司历年现金分红情况（金额）",
        "",
    ])
    _append_template_gap(lines, "当前 FactPack 未包含历年现金分红明细。")

    lines.extend(
        [
            "",
            f"## {stock_name}公司财务分析",
            "",
            *_markdown_table(
                ["指标", "最新值"],
                [
                    ["最近财报期", _raw_text(financial.get("fina_end_date"))],
                    ["营业收入", _fmt(financial.get("total_revenue_yi"), suffix=" 亿")],
                    ["净利润", _fmt(financial.get("net_profit_yi"), suffix=" 亿")],
                    ["经营性现金流净额", _fmt(financial.get("operating_cashflow_yi"), suffix=" 亿")],
                    ["ROE", _fmt(financial.get("roe"), suffix="%")],
                    ["毛利率", _fmt(financial.get("gross_margin"), suffix="%")],
                    ["资产负债率", _fmt(financial.get("debt_to_assets"), suffix="%")],
                    ["静态市盈率", _fmt(chart_latest.get("pe") or valuation.get("pe"))],
                    ["动态市盈率", _fmt(chart_latest.get("pe_ttm") or valuation.get("pe_ttm"))],
                    ["股息率", _fmt(chart_latest.get("dv_ttm") or chart_latest.get("dv_ratio"), suffix="%")],
                    ["总市值", _fmt(chart_latest.get("total_mv_yi") or valuation.get("total_mv_yi"), suffix=" 亿")],
                    ["流通市值", _fmt(chart_latest.get("circ_mv_yi") or valuation.get("circ_mv_yi"), suffix=" 亿")],
                    ["总股本", _fmt(chart_latest.get("total_share_yi"), suffix=" 亿股")],
                    ["流通股本", _fmt(chart_latest.get("float_share_yi"), suffix=" 亿股")],
                ],
            ),
            "",
            FINANCIAL_CHART_MARKER,
            "",
            f"### {stock_name}公司各产品&服务收入分析",
            "",
            *_markdown_table(["产品/服务", "收入", "收入占比", "毛利率"], _business_composition_rows(fact_pack)),
            "",
            "## 技术面分析",
            "",
            "MACD、MA（5、10、20、30、60、120、233、250）、EMA（5、30）、交易量、金叉&死叉等指标分析",
            "",
            "### 日线技术分析",
            "",
            f"- 最新交易日：{technical['latest_date']}，收盘价：{technical['latest_close']}",
            f"- 均线：{technical['ma_summary']}",
            f"- EMA：{technical['ema_summary']}",
            f"- MACD：{technical['macd_summary']}",
            f"- 交易量：{technical['volume_summary']}",
            f"- 结论：{technical['daily_summary']}",
            "",
            "### 周线技术分析",
            "",
            f"- {technical['weekly_summary']}",
            "",
            "### 月线技术分析",
            "",
            f"- {technical['monthly_summary']}",
            "",
            "### 30分钟线技术分析",
            "",
            "> 数据缺口：当前底稿未包含 30 分钟线行情，点击生成时不会额外触发分钟级同步。",
            "",
            "### 60分钟线技术分析",
            "",
            "> 数据缺口：当前底稿未包含 60 分钟线行情，点击生成时不会额外触发分钟级同步。",
            "",
            f"### {stock_name}公司&一级行业指数日线对比分析（最近的一个自然年&最近一年）",
            "",
            "> 数据缺口：当前底稿未包含一级行业指数行情。可在后续接入行业指数后绘制相对强弱。",
            "",
            f"### {stock_name}公司&所属宽基指数日线对比分析（最近的一个自然年&最近一年）",
            "",
            "> 数据缺口：当前底稿未包含宽基指数行情。可在后续接入宽基指数后绘制相对强弱。",
            "",
            "## 股东数量分析",
            "",
            f"- 最新股东数量：{_fmt(profile.get('holder_num'), 0)}",
            f"- 股东数量截止日：{_raw_text(profile.get('holder_end_date'))}",
            "- 图15：股东数量（柱状）及变化率（折线）：当前底稿只含最新股东数量，历史序列需补充后绘制。",
            "- 图16：股东数量（柱状）与股价趋势（折线）：当前底稿可提供股价趋势，股东历史序列需补充。",
            "",
            "### 所有报告期前十大股东分析",
            "",
            "> 数据缺口：当前 FactPack 未包含所有报告期前十大股东名称、出现次数及对应报告期。",
            "",
            "### 所有报告期前十大流通股东分析",
            "",
            "> 数据缺口：当前 FactPack 未包含所有报告期前十大流通股东名称、出现次数及对应报告期。",
            "",
            "- 图17：前十大股东：待接入股东结构数据。",
            "- 图18：前十大股东变化情况（持股数量及持股比例）：待接入股东结构历史数据。",
            "- 图19：前十大流通股东：待接入流通股东结构数据。",
            "- 图20：前十大流通股东变化情况（持股数量及持股比例）：待接入流通股东结构历史数据。",
            "",
            "## 投资建议",
            "",
        ]
    )

    if normalized_llm:
        quality_score = normalized_llm.get("quality_score") or {}
        lines.extend(
            [
                f"- 综合判断：{_raw_text(normalized_llm.get('verdict'), '观察')}",
                f"- 风险等级：{_raw_text(normalized_llm.get('risk_level'), '中')}",
                f"- 置信度：{_fmt(normalized_llm.get('confidence'), 0)}/100",
                f"- 公司质地评分：{_fmt(quality_score.get('score'), 0)}/100（{_raw_text(quality_score.get('grade'))}级）",
                "",
                f"**核心投资命题**：{_raw_text(normalized_llm.get('investment_thesis'), '大模型未返回核心投资命题。')}",
                "",
                f"**估值与赔率**：{_raw_text(normalized_llm.get('valuation_view'), '大模型未返回估值观点。')}",
                "",
                f"**位置与节奏**：{_raw_text(normalized_llm.get('timing_view'), '大模型未返回节奏观点。')}",
                "",
                f"**结论摘要**：{_raw_text(normalized_llm.get('summary'), '大模型未返回摘要。')}",
                "",
                "### 关键证据",
                "",
                *_bullet_lines(normalized_llm.get("key_evidence")),
                "",
                "### 主要风险",
                "",
                *_bullet_lines(normalized_llm.get("risk_factors")),
                "",
                "### 后续跟踪清单",
                "",
                *_bullet_lines(normalized_llm.get("watch_items")),
            ]
        )
    else:
        lines.extend(
            [
                "> 大模型未配置或未返回有效结构化结果，本次仅生成数据模板报告；投资建议部分需配置 LLM 后重新点击按钮生成。",
            ]
        )

    lines.extend(
        [
            "",
            "---",
            f"- 数据覆盖：profile={quality.get('profile_rows', 0)} 行，daily={quality.get('daily_rows', 0)} 行，kline={quality.get('kline_rows', 0)} 行，financial={quality.get('financial_rows', 0)} 行。",
            f"- 报告生成时间：{generated_at}",
            "- 免责声明：本报告仅供研究跟踪使用，不构成投资建议；投资有风险，决策需谨慎。",
        ]
    )
    return "\n".join(lines)


def render_stock_analysis_template_html(
    fact_pack: dict[str, Any] | None,
    llm_result: dict[str, Any] | None,
) -> str:
    """Render a standalone HTML report following the local docx template order."""
    fact_pack = fact_pack if isinstance(fact_pack, dict) else {}
    profile = fact_pack.get("profile") or {}
    stock_name = _raw_text(fact_pack.get("stock_name") or profile.get("name") or fact_pack.get("ts_code"), "未知股票")
    title = f"{stock_name}公司分析报告"
    markdown_text = render_stock_analysis_template_markdown(fact_pack, llm_result)
    body_html = _markdown_to_body_html(markdown_text)
    chart_html = _render_financial_chart_section(fact_pack.get("template_chart_data") if isinstance(fact_pack.get("template_chart_data"), dict) else {})
    body_html = body_html.replace(f"<p>{FINANCIAL_CHART_MARKER}</p>", chart_html)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(title)}</title>
  <style>
    :root {{
      --bg: #f6f7fb;
      --paper: #ffffff;
      --ink: #17202a;
      --muted: #667482;
      --line: #d9e1ea;
      --accent: #1f4e79;
      --accent-soft: #e8f1fb;
      --warn: #b26b00;
      --shadow: 0 18px 44px rgba(31, 41, 55, 0.10);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Microsoft YaHei", sans-serif;
      line-height: 1.7;
    }}
    .report-shell {{
      max-width: 1120px;
      margin: 0 auto;
      padding: 28px 18px 48px;
    }}
    .report-paper {{
      background: var(--paper);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
      padding: 42px;
    }}
    h1 {{
      margin: 0 0 24px;
      padding-bottom: 18px;
      border-bottom: 3px solid var(--accent);
      font-size: 30px;
      text-align: center;
      letter-spacing: 0;
    }}
    h2 {{
      margin: 30px 0 14px;
      padding: 10px 14px;
      background: var(--accent);
      color: #fff;
      font-size: 20px;
      letter-spacing: 0;
    }}
    h3 {{
      margin: 22px 0 10px;
      padding-left: 10px;
      border-left: 4px solid var(--accent);
      color: var(--accent);
      font-size: 16px;
      letter-spacing: 0;
    }}
    p, li, blockquote {{ font-size: 14px; }}
    p {{ margin: 8px 0; }}
    ul {{ margin: 8px 0 14px; padding-left: 22px; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin: 10px 0 18px;
      font-size: 13px;
    }}
    th, td {{
      border: 1px solid var(--line);
      padding: 9px 10px;
      vertical-align: top;
      text-align: left;
    }}
    th {{
      background: var(--accent-soft);
      color: var(--accent);
      font-weight: 700;
    }}
    blockquote {{
      margin: 10px 0 16px;
      padding: 10px 12px;
      border-left: 4px solid var(--warn);
      background: #fff8e8;
      color: #6f4b00;
    }}
    hr {{ border: 0; border-top: 1px solid var(--line); margin: 28px 0 16px; }}
    strong {{ color: var(--accent); }}
    .financial-chart-section {{
      margin: 18px 0 24px;
    }}
    .chart-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
      margin-top: 10px;
    }}
    .chart-card {{
      border: 1px solid var(--line);
      background: #fbfdff;
      padding: 14px;
      break-inside: avoid;
    }}
    .chart-card h3 {{
      margin-top: 0;
      font-size: 14px;
    }}
    .chart-svg {{
      width: 100%;
      height: auto;
      display: block;
      background: #fff;
      border: 1px solid #edf1f5;
    }}
    .axis, .zero-line {{
      stroke: #c9d4df;
      stroke-width: 1;
    }}
    .zero-line {{
      stroke-dasharray: 4 4;
    }}
    .axis-label, .tick-label {{
      fill: var(--muted);
      font-size: 11px;
    }}
    .bar-positive {{
      fill: #2f7bbd;
      opacity: 0.86;
    }}
    .bar-negative {{
      fill: #c65d4a;
      opacity: 0.86;
    }}
    .value-line {{
      fill: none;
      stroke: #1f4e79;
      stroke-width: 2.4;
    }}
    .value-point {{
      fill: #1f4e79;
    }}
    .growth-line {{
      fill: none;
      stroke: #c05a00;
      stroke-width: 2.2;
    }}
    .growth-point {{
      fill: #c05a00;
    }}
    .chart-legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      margin-top: 8px;
      color: var(--muted);
      font-size: 12px;
    }}
    .chart-legend i {{
      display: inline-block;
      width: 14px;
      height: 8px;
      margin-right: 5px;
      vertical-align: 1px;
    }}
    .legend-bar {{ background: #2f7bbd; }}
    .legend-line {{
      height: 2px !important;
      background: #c05a00;
    }}
    .chart-caption, .chart-source {{
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 12px;
    }}
    .chart-empty {{
      min-height: 160px;
      display: flex;
      flex-direction: column;
      justify-content: center;
      border: 1px dashed var(--line);
      background: #fff;
      padding: 12px;
    }}
    .footer-note {{
      margin-top: 20px;
      color: var(--muted);
      font-size: 12px;
      text-align: center;
    }}
    @media print {{
      body {{ background: #fff; }}
      .report-shell {{ padding: 0; }}
      .report-paper {{ border: 0; box-shadow: none; padding: 0; }}
    }}
    @media (max-width: 760px) {{
      .report-paper {{ padding: 24px 16px; }}
      .chart-grid {{ grid-template-columns: 1fr; }}
      h1 {{ font-size: 24px; }}
      h2 {{ font-size: 18px; }}
    }}
  </style>
</head>
<body>
  <main class="report-shell">
    <article class="report-paper">
{body_html}
      <p class="footer-note">HTML 报告由 WealthSpark 自选管理页手动生成。</p>
    </article>
  </main>
</body>
</html>"""


def _bullet_lines(items: Any) -> list[str]:
    values = items if isinstance(items, list) else []
    clean = [_raw_text(item, "") for item in values if _raw_text(item, "")]
    if not clean:
        return ["- 暂无"]
    return [f"- {item}" for item in clean]


def generate_stock_analysis_template_report_bundle(
    ts_code: str,
    stock_name: str,
    *,
    engine: Engine,
    asof_trade_date: str | None = None,
    allow_live_fetch: bool | None = None,
    report_date: str | None = None,
    use_report_cache: bool = True,
    save_report: bool = True,
    force_refresh: bool = False,
    fact_pack_builder: FactPackBuilder = build_stock_research_fact_pack,
    llm_analyzer: LLMAnalyzer = analyze_stock_research_payload,
    chart_data_loader: ChartDataLoader = load_stock_analysis_template_chart_data,
) -> dict[str, Any]:
    """Generate the docx-template-style stock report on demand."""
    ts_code_key = str(ts_code or "").strip().upper()
    report_date_key = _normalize_date_text(report_date) or today_report_date()
    if use_report_cache and not force_refresh:
        cached_report = get_cached_template_report(engine, ts_code_key, report_date_key)
        if cached_report and str(cached_report.get("report_html") or "").strip():
            return {
                "report_html": str(cached_report.get("report_html") or ""),
                "fact_pack": cached_report.get("fact_pack") or {},
                "llm_result": cached_report.get("llm_result"),
                "report_date": report_date_key,
                "cache_hit": True,
            }

    live_fetch_enabled = (
        should_enable_stock_research_akshare()
        if allow_live_fetch is None
        else bool(allow_live_fetch)
    )
    fact_pack = fact_pack_builder(
        ts_code_key,
        stock_name,
        engine=engine,
        asof_trade_date=asof_trade_date,
        allow_live_fetch=live_fetch_enabled,
    )
    llm_result = llm_analyzer(fact_pack)
    normalized_llm = normalize_stock_research_llm_result(llm_result)
    chart_data = chart_data_loader(
        ts_code_key,
        engine=engine,
        asof_trade_date=asof_trade_date or fact_pack.get("asof_trade_date"),
    )
    fact_pack = dict(fact_pack)
    fact_pack["template_chart_data"] = chart_data
    report_html = render_stock_analysis_template_html(fact_pack, normalized_llm)
    if save_report:
        save_template_report(
            engine,
            ts_code_key,
            report_html,
            stock_name=stock_name or str(fact_pack.get("stock_name") or ""),
            report_date=report_date_key,
            asof_trade_date=str(fact_pack.get("asof_trade_date") or asof_trade_date or ""),
            fact_pack=fact_pack,
            llm_result=normalized_llm,
        )
    return {
        "report_html": report_html,
        "fact_pack": fact_pack,
        "llm_result": normalized_llm,
        "report_date": report_date_key,
        "cache_hit": False,
    }
