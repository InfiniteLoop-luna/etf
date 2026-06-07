# -*- coding: utf-8 -*-
"""Board-style views for daily Dragon Tiger List data."""

from __future__ import annotations

import math
import re
from datetime import date, datetime
from typing import Any, Iterable, Mapping, Optional

import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.lhb_monitor import prepare_lhb_inst_frame, prepare_lhb_top_list_frame


_TS_CODE_RE = re.compile(r"\b\d{6}\.(?:SZ|SH|BJ)\b", re.IGNORECASE)


def _clean_text(value: Any, fallback: str = "-") -> str:
    text_value = str(value or "").strip()
    if not text_value or text_value.lower() == "nan":
        return fallback
    return text_value


def _numeric(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float(default)
    return float(default) if math.isnan(number) else number


def _format_signed_yi(value: Any) -> str:
    number = _numeric(value)
    sign = "+" if number > 0 else "-" if number < 0 else ""
    abs_value = abs(number)
    if abs_value >= 1:
        return f"{sign}{abs_value:.2f}亿"
    if abs_value >= 0.0001:
        return f"{sign}{abs_value * 10000:.0f}万"
    return "0"


def _format_percent(value: Any) -> str:
    return f"{_numeric(value):+.2f}%"


def _compress_tile_value(value: Any) -> float:
    return math.sqrt(max(_numeric(value), 0.03))


def _cap_tile_values(values: Iterable[Any], *, max_share: float = 0.15) -> list[float]:
    weights = [max(_numeric(value), 0.03) for value in values]
    if not weights:
        return []

    if len(weights) * float(max_share) <= 1.0:
        return weights

    feasible_share = float(max_share)
    total = sum(weights)
    if total <= 0:
        return weights
    if max(weights) <= total * feasible_share:
        return weights

    low = 0.0
    high = max(weights)
    for _ in range(48):
        cap = (low + high) / 2.0
        capped_total = sum(min(weight, cap) for weight in weights)
        if capped_total <= 0 or cap / capped_total > feasible_share:
            high = cap
        else:
            low = cap

    cap = high
    return [max(min(weight, cap), 0.03) for weight in weights]


def _compact_reasons(values: Iterable[Any], max_items: int = 2) -> str:
    reasons: list[str] = []
    for value in values:
        reason = _clean_text(value, "")
        if reason and reason not in reasons:
            reasons.append(reason)
    if not reasons:
        return "-"
    label = "；".join(reasons[:max_items])
    return label if len(reasons) <= max_items else f"{label} 等{len(reasons)}类"


def _first_non_empty(values: Iterable[Any]) -> str:
    for value in values:
        text_value = _clean_text(value, "")
        if text_value:
            return text_value
    return "-"


def _normalize_industry_map(industry_map: Mapping[str, Any] | pd.DataFrame | None) -> dict[str, str]:
    if industry_map is None:
        return {}
    if isinstance(industry_map, pd.DataFrame):
        if industry_map.empty or "ts_code" not in industry_map.columns:
            return {}
        value_column = "industry" if "industry" in industry_map.columns else "sector" if "sector" in industry_map.columns else ""
        if not value_column:
            return {}
        return {
            str(row["ts_code"]).strip().upper(): _clean_text(row[value_column], "")
            for _, row in industry_map.iterrows()
            if _clean_text(row.get("ts_code"), "") and _clean_text(row.get(value_column), "")
        }
    return {str(code).strip().upper(): _clean_text(industry, "") for code, industry in industry_map.items() if _clean_text(industry, "")}


def _fallback_sector_from_code(ts_code: Any) -> str:
    code = _clean_text(ts_code, "").upper()
    if code.startswith("688"):
        return "科创板"
    if code.startswith(("300", "301")):
        return "创业板"
    if code.startswith(("4", "8", "9")) or code.endswith(".BJ"):
        return "北交所"
    if code.endswith(".SH"):
        return "沪市主板"
    if code.endswith(".SZ"):
        return "深市主板"
    return "未归类板块"


def _resolve_sector(row: pd.Series, industry_lookup: Mapping[str, str]) -> str:
    ts_code = _clean_text(row.get("ts_code"), "").upper()
    for column in ("industry", "sector", "板块", "行业"):
        if column in row:
            value = _clean_text(row.get(column), "")
            if value:
                return value
    mapped = _clean_text(industry_lookup.get(ts_code), "")
    return mapped or _fallback_sector_from_code(ts_code)


def _normalize_trade_date(value: Any) -> pd.Timestamp | None:
    if value is None or value == "":
        return None
    if isinstance(value, pd.Timestamp):
        return value.normalize()
    if isinstance(value, datetime):
        return pd.Timestamp(value.date())
    if isinstance(value, date):
        return pd.Timestamp(value)
    text_value = str(value).strip().replace("-", "")
    parsed = pd.to_datetime(text_value[:8], format="%Y%m%d", errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).normalize()


def _empty_model() -> dict[str, Any]:
    return {
        "trade_date": None,
        "trade_date_label": "-",
        "record_count": 0,
        "stock_count": 0,
        "sector_count": 0,
        "net_amount_yi": 0.0,
        "inst_net_yi": 0.0,
        "combined_net_yi": 0.0,
        "sectors": [],
        "stock_codes": [],
    }


def build_lhb_today_board_model(
    top_list_df: pd.DataFrame | None,
    inst_df: pd.DataFrame | None = None,
    *,
    industry_map: Mapping[str, Any] | pd.DataFrame | None = None,
    trade_date: Any = None,
    max_sectors: int = 18,
    max_stocks_per_sector: int = 18,
) -> dict[str, Any]:
    """Aggregate latest-day Dragon Tiger List rows into sector and stock tiles."""
    top_list = prepare_lhb_top_list_frame(top_list_df)
    if top_list.empty:
        return _empty_model()

    requested_date = _normalize_trade_date(trade_date)
    target_date = requested_date or pd.Timestamp(top_list["trade_date"].dropna().max()).normalize()
    if pd.isna(target_date):
        return _empty_model()

    today_rows = top_list[top_list["trade_date"].dt.normalize() == target_date].copy()
    if today_rows.empty:
        return _empty_model()

    industry_lookup = _normalize_industry_map(industry_map)
    today_rows["sector"] = today_rows.apply(lambda row: _resolve_sector(row, industry_lookup), axis=1)

    stock_summary = (
        today_rows.groupby("ts_code", dropna=False)
        .agg(
            name=("name", _first_non_empty),
            sector=("sector", _first_non_empty),
            record_count=("ts_code", "size"),
            pct_change=("pct_change", "mean"),
            turnover_rate=("turnover_rate", "max"),
            l_buy_yi=("l_buy_yi", "sum"),
            l_sell_yi=("l_sell_yi", "sum"),
            lhb_amount_yi=("l_amount_yi", "sum"),
            net_amount_yi=("net_amount_yi", "sum"),
            amount_rate=("amount_rate", "max"),
            reason=("reason", _compact_reasons),
        )
        .reset_index()
    )

    inst = prepare_lhb_inst_frame(inst_df)
    if not inst.empty:
        inst_today = inst[inst["trade_date"].dt.normalize() == target_date].copy()
        inst_summary = (
            inst_today.groupby("ts_code", dropna=False)
            .agg(
                inst_hit_count=("ts_code", "size"),
                inst_org_count=("exalter", lambda values: len({_clean_text(value, "") for value in values if _clean_text(value, "")})),
                inst_buy_yi=("buy_yi", "sum"),
                inst_sell_yi=("sell_yi", "sum"),
                inst_net_yi=("net_buy_yi", "sum"),
            )
            .reset_index()
            if not inst_today.empty
            else pd.DataFrame(columns=["ts_code", "inst_hit_count", "inst_org_count", "inst_buy_yi", "inst_sell_yi", "inst_net_yi"])
        )
        stock_summary = stock_summary.merge(inst_summary, on="ts_code", how="left")
    else:
        for column in ["inst_hit_count", "inst_org_count", "inst_buy_yi", "inst_sell_yi", "inst_net_yi"]:
            stock_summary[column] = 0

    for column in ["inst_hit_count", "inst_org_count", "inst_buy_yi", "inst_sell_yi", "inst_net_yi"]:
        if column not in stock_summary.columns:
            stock_summary[column] = 0
        stock_summary[column] = pd.to_numeric(stock_summary[column], errors="coerce").fillna(0.0)

    stock_summary["combined_net_yi"] = stock_summary["net_amount_yi"] + stock_summary["inst_net_yi"]
    stock_summary["abs_combined_net_yi"] = stock_summary["combined_net_yi"].abs()
    stock_summary["tile_raw_value"] = stock_summary.apply(
        lambda row: max(
            _numeric(row.get("lhb_amount_yi")),
            _numeric(row.get("abs_combined_net_yi")),
            _numeric(row.get("record_count")) * 0.03,
            0.03,
        ),
        axis=1,
    )
    stock_summary["tile_compressed_value"] = stock_summary["tile_raw_value"].map(_compress_tile_value)
    stock_summary["tile_value"] = _cap_tile_values(stock_summary["tile_compressed_value"], max_share=0.15)
    stock_summary["direction"] = stock_summary["combined_net_yi"].map(
        lambda value: "positive" if value > 0 else "negative" if value < 0 else "neutral"
    )
    stock_summary["net_label"] = stock_summary["combined_net_yi"].map(_format_signed_yi)
    stock_summary["pct_label"] = stock_summary["pct_change"].map(_format_percent)

    stock_summary = stock_summary.sort_values(["tile_value", "record_count", "abs_combined_net_yi"], ascending=False)
    sector_summary = (
        stock_summary.groupby("sector", dropna=False)
        .agg(
            stock_count=("ts_code", "nunique"),
            record_count=("record_count", "sum"),
            tile_value=("tile_value", "sum"),
            net_amount_yi=("net_amount_yi", "sum"),
            inst_net_yi=("inst_net_yi", "sum"),
            combined_net_yi=("combined_net_yi", "sum"),
            avg_pct_change=("pct_change", "mean"),
        )
        .reset_index()
        .sort_values(["tile_value", "stock_count", "sector"], ascending=[False, False, True])
        .head(max(1, int(max_sectors)))
    )

    sectors: list[dict[str, Any]] = []
    for _, sector_row in sector_summary.iterrows():
        sector_name = _clean_text(sector_row.get("sector"), "未归类板块")
        sector_stocks = stock_summary[stock_summary["sector"] == sector_name].head(max(1, int(max_stocks_per_sector)))
        stock_items = []
        for _, stock_row in sector_stocks.iterrows():
            stock_items.append(
                {
                    "ts_code": _clean_text(stock_row.get("ts_code")),
                    "name": _clean_text(stock_row.get("name")),
                    "sector": sector_name,
                    "record_count": int(_numeric(stock_row.get("record_count"))),
                    "pct_change": _numeric(stock_row.get("pct_change")),
                    "turnover_rate": _numeric(stock_row.get("turnover_rate")),
                    "l_buy_yi": _numeric(stock_row.get("l_buy_yi")),
                    "l_sell_yi": _numeric(stock_row.get("l_sell_yi")),
                    "lhb_amount_yi": _numeric(stock_row.get("lhb_amount_yi")),
                    "net_amount_yi": _numeric(stock_row.get("net_amount_yi")),
                    "inst_net_yi": _numeric(stock_row.get("inst_net_yi")),
                    "combined_net_yi": _numeric(stock_row.get("combined_net_yi")),
                    "tile_raw_value": _numeric(stock_row.get("tile_raw_value"), 0.03),
                    "tile_compressed_value": _numeric(stock_row.get("tile_compressed_value"), 0.03),
                    "tile_value": _numeric(stock_row.get("tile_value"), 0.03),
                    "amount_rate": _numeric(stock_row.get("amount_rate")),
                    "reason": _clean_text(stock_row.get("reason")),
                    "direction": _clean_text(stock_row.get("direction"), "neutral"),
                    "net_label": _clean_text(stock_row.get("net_label"), "0"),
                    "pct_label": _clean_text(stock_row.get("pct_label"), "+0.00%"),
                }
            )
        sectors.append(
            {
                "sector": sector_name,
                "stock_count": int(_numeric(sector_row.get("stock_count"))),
                "visible_stock_count": len(stock_items),
                "record_count": int(_numeric(sector_row.get("record_count"))),
                "tile_value": _numeric(sector_row.get("tile_value"), 0.03),
                "net_amount_yi": _numeric(sector_row.get("net_amount_yi")),
                "inst_net_yi": _numeric(sector_row.get("inst_net_yi")),
                "combined_net_yi": _numeric(sector_row.get("combined_net_yi")),
                "avg_pct_change": _numeric(sector_row.get("avg_pct_change")),
                "net_label": _format_signed_yi(sector_row.get("combined_net_yi")),
                "stocks": stock_items,
            }
        )

    stock_codes = [stock["ts_code"] for sector in sectors for stock in sector["stocks"]]
    return {
        "trade_date": target_date,
        "trade_date_label": target_date.strftime("%Y-%m-%d"),
        "record_count": int(today_rows.shape[0]),
        "stock_count": int(stock_summary["ts_code"].nunique()),
        "sector_count": len(sectors),
        "net_amount_yi": _numeric(stock_summary["net_amount_yi"].sum()),
        "inst_net_yi": _numeric(stock_summary["inst_net_yi"].sum()),
        "combined_net_yi": _numeric(stock_summary["combined_net_yi"].sum()),
        "sectors": sectors,
        "stock_codes": stock_codes,
    }


def create_lhb_today_treemap_figure(
    model: Mapping[str, Any],
    *,
    selected_ts_code: str = "",
    colors: Mapping[str, str] | None = None,
) -> go.Figure:
    """Create a professional market-heatmap style treemap for Streamlit."""
    palette = {
        "paper": "#121722",
        "plot": "#121722",
        "text": "#F8FAFC",
        "muted": "#AAB6C5",
        "line": "#242C3A",
        "selected": "#F8D57E",
        "up": "#C83D4A",
        "down": "#1FA463",
        "neutral": "#485160",
    }
    if colors:
        palette.update({key: value for key, value in colors.items() if value})

    labels: list[str] = ["当日龙虎榜"]
    ids: list[str] = ["root"]
    parents: list[str] = [""]
    values: list[float] = [max(_numeric(model.get("combined_net_yi"), 0.0), 0.03)]
    marker_colors: list[float] = [0.0]
    customdata: list[list[str]] = [["", "全部", _clean_text(model.get("trade_date_label")), "", ""]]
    texts: list[str] = [
        f"{_clean_text(model.get('trade_date_label'))}<br>{int(_numeric(model.get('stock_count')))}股 / {int(_numeric(model.get('record_count')))}条"
    ]
    line_widths: list[float] = [1.0]
    line_colors: list[str] = [palette["line"]]

    total_value = 0.0
    normalized_selected = _clean_text(selected_ts_code, "").upper()
    for sector in model.get("sectors", []):
        sector_name = _clean_text(sector.get("sector"), "未归类板块")
        sector_id = f"sector:{sector_name}"
        sector_value = max(_numeric(sector.get("tile_value")), 0.03)
        total_value += sector_value
        labels.append(sector_name)
        ids.append(sector_id)
        parents.append("root")
        values.append(sector_value)
        marker_colors.append(_numeric(sector.get("avg_pct_change")))
        customdata.append(["", sector_name, _clean_text(model.get("trade_date_label")), _clean_text(sector.get("net_label")), ""])
        texts.append(
            f"{int(_numeric(sector.get('stock_count')))}股 / {int(_numeric(sector.get('record_count')))}次<br>合计{_clean_text(sector.get('net_label'), '0')}"
        )
        line_widths.append(1.5)
        line_colors.append(palette["line"])

        for stock in sector.get("stocks", []):
            ts_code = _clean_text(stock.get("ts_code"), "").upper()
            labels.append(_clean_text(stock.get("name"), ts_code))
            ids.append(f"stock:{ts_code}")
            parents.append(sector_id)
            values.append(max(_numeric(stock.get("tile_value")), 0.03))
            marker_colors.append(_numeric(stock.get("pct_change")))
            customdata.append(
                [
                    ts_code,
                    _clean_text(stock.get("name"), ts_code),
                    sector_name,
                    _clean_text(stock.get("net_label"), "0"),
                    _clean_text(stock.get("reason"), "-"),
                ]
            )
            texts.append(f"{_clean_text(stock.get('pct_label'), '+0.00%')}<br>{_clean_text(stock.get('net_label'), '0')}")
            line_widths.append(3.0 if ts_code and ts_code == normalized_selected else 0.9)
            line_colors.append(palette["selected"] if ts_code and ts_code == normalized_selected else palette["line"])

    if total_value > 0:
        values[0] = total_value

    fig = go.Figure(
        go.Treemap(
            labels=labels,
            ids=ids,
            parents=parents,
            values=values,
            branchvalues="total",
            customdata=customdata,
            text=texts,
            texttemplate="<b>%{label}</b><br>%{text}",
            hovertemplate=(
                "<b>%{label}</b><br>"
                "板块：%{customdata[2]}<br>"
                "合计净买：%{customdata[3]}<br>"
                "原因：%{customdata[4]}"
                "<extra></extra>"
            ),
            marker=dict(
                colors=marker_colors,
                colorscale=[
                    [0.0, palette["down"]],
                    [0.42, palette["neutral"]],
                    [0.50, "#596170"],
                    [0.58, "#8E4351"],
                    [1.0, palette["up"]],
                ],
                cmin=-10,
                cmid=0,
                cmax=10,
                line=dict(color=line_colors, width=line_widths),
            ),
            maxdepth=3,
            pathbar=dict(visible=False),
        )
    )
    fig.update_layout(
        paper_bgcolor=palette["paper"],
        plot_bgcolor=palette["plot"],
        margin=dict(l=0, r=0, t=8, b=0),
        height=560,
        font=dict(family="Inter, PingFang SC, Microsoft YaHei, sans-serif", color=palette["text"], size=13),
        uniformtext=dict(minsize=10, mode="hide"),
    )
    return fig


def _iter_event_points(event_payload: Any) -> list[Mapping[str, Any]]:
    if not event_payload:
        return []
    if isinstance(event_payload, list):
        return [point for point in event_payload if isinstance(point, Mapping)]
    if isinstance(event_payload, Mapping):
        points = event_payload.get("points")
        if isinstance(points, list):
            return [point for point in points if isinstance(point, Mapping)]
        selection = event_payload.get("selection")
        if isinstance(selection, Mapping):
            selected_points = selection.get("points")
            if isinstance(selected_points, list):
                return [point for point in selected_points if isinstance(point, Mapping)]
        return [event_payload]
    selection = getattr(event_payload, "selection", None)
    if selection is not None:
        return _iter_event_points(selection)
    points = getattr(event_payload, "points", None)
    if points is not None:
        return _iter_event_points(points)
    return []


def _extract_code_from_value(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        for item in value:
            code = _extract_code_from_value(item)
            if code:
                return code
        return ""
    match = _TS_CODE_RE.search(str(value or "").upper())
    return match.group(0).upper() if match else ""


def extract_lhb_treemap_stock_code(event_payload: Any) -> str:
    """Return the stock code from a Plotly treemap click/selection payload."""
    for point in _iter_event_points(event_payload):
        custom_code = _extract_code_from_value(point.get("customdata"))
        if custom_code:
            return custom_code
        point_id = str(point.get("id") or "").strip().upper()
        if point_id.startswith("STOCK:"):
            code = _extract_code_from_value(point_id)
            if code:
                return code
        for key in ("label", "text", "hovertext"):
            code = _extract_code_from_value(point.get(key))
            if code:
                return code
    return ""


def load_lhb_industry_map(ts_codes: Iterable[str], engine: Optional[Engine] = None) -> dict[str, str]:
    """Load stock industry labels for the visible Dragon Tiger List codes."""
    codes = sorted({_clean_text(code, "").upper() for code in ts_codes if _clean_text(code, "")})
    if not codes:
        return {}
    try:
        actual_engine = engine
        if actual_engine is None:
            from src.moneyflow_fetcher import _get_engine_cached

            actual_engine = _get_engine_cached()
        placeholders = ", ".join(f":code_{index}" for index, _ in enumerate(codes))
        params = {f"code_{index}": code for index, code in enumerate(codes)}
        sql = f"""
            SELECT ts_code, COALESCE(NULLIF(industry, ''), NULLIF(market, ''), '') AS industry
            FROM vw_ts_stock_basic
            WHERE ts_code IN ({placeholders})
        """
        df = pd.read_sql(text(sql), actual_engine, params=params)
    except Exception:
        return {}
    return _normalize_industry_map(df)
