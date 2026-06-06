# -*- coding: utf-8 -*-
"""Build non-image hot-money relationship tree HTML for Streamlit."""

from __future__ import annotations

import math
import re
from html import escape
from typing import Any

import pandas as pd


_GROUP_COLORS = ["#09AEA8", "#0F9EA0", "#40524E", "#2EDFD1", "#6A8D73"]


def _clean_text(value: Any, fallback: str = "-") -> str:
    text = str(value or "").strip()
    return text if text else fallback


def _numeric(value: Any) -> float:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if math.isnan(num) else num


def _trim_number(value: float) -> str:
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _format_amount_label(amount_yi: Any) -> str:
    amount_yi = _numeric(amount_yi)
    sign = "+" if amount_yi > 0 else "-" if amount_yi < 0 else ""
    abs_yi = abs(amount_yi)
    if abs_yi >= 1:
        return f"{sign}{_trim_number(abs_yi)}亿"
    return f"{sign}{int(round(abs_yi * 10000))}万"


def _split_orgs(value: Any, max_items: int) -> list[str]:
    text = _clean_text(value, "")
    if not text:
        return ["关联席位未披露"]
    orgs = [item.strip() for item in re.split(r"[、,，;；|/]+", text) if item.strip()]
    deduped: list[str] = []
    for org in orgs:
        if org not in deduped:
            deduped.append(org)
        if len(deduped) >= max_items:
            break
    return deduped or ["关联席位未披露"]


def _column_or_empty(df: pd.DataFrame, column: str) -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series([""] * len(df), index=df.index)


def _prepare_tree_frame(detail_df: pd.DataFrame | None) -> pd.DataFrame:
    if detail_df is None or detail_df.empty:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "hm_name",
                "ts_name",
                "ts_code",
                "hm_orgs",
                "net_amount_yi",
                "abs_net_amount_yi",
            ]
        )

    work = detail_df.copy()
    work["trade_date"] = pd.to_datetime(_column_or_empty(work, "trade_date"), errors="coerce")
    work["hm_name"] = _column_or_empty(work, "hm_name").map(lambda value: _clean_text(value, "未知游资"))
    work["ts_code"] = _column_or_empty(work, "ts_code").map(lambda value: _clean_text(value, "-"))
    work["ts_name"] = _column_or_empty(work, "ts_name").map(lambda value: _clean_text(value, ""))
    work["ts_name"] = work.apply(lambda row: row["ts_name"] or row["ts_code"], axis=1)
    work["hm_orgs"] = _column_or_empty(work, "hm_orgs").map(lambda value: _clean_text(value, ""))

    if "net_amount_yi" in work.columns:
        work["net_amount_yi"] = pd.to_numeric(work["net_amount_yi"], errors="coerce").fillna(0.0)
    elif "net_amount" in work.columns:
        work["net_amount_yi"] = pd.to_numeric(work["net_amount"], errors="coerce").fillna(0.0) / 1e8
    else:
        work["net_amount_yi"] = 0.0
    work["abs_net_amount_yi"] = work["net_amount_yi"].abs()
    return work


def _trade_date_label(work: pd.DataFrame) -> str:
    dates = work["trade_date"].dropna()
    if dates.empty:
        return "-"
    start = dates.min().strftime("%Y-%m-%d")
    end = dates.max().strftime("%Y-%m-%d")
    return start if start == end else f"{start}~{end}"


def build_hotmoney_tree_model(
    detail_df: pd.DataFrame | None,
    *,
    max_hotmoney: int = 8,
    max_stocks_per_hotmoney: int = 6,
    max_orgs_per_stock: int = 4,
) -> dict[str, Any]:
    """Build a deterministic model for the non-image hot-money tree."""
    work = _prepare_tree_frame(detail_df)
    if work.empty:
        return {"trade_date_label": "-", "total_records": 0, "groups": []}

    row_summary = (
        work.groupby(["hm_name", "ts_code", "ts_name", "hm_orgs"], dropna=False)
        .agg(
            net_amount_yi=("net_amount_yi", "sum"),
            abs_net_amount_yi=("abs_net_amount_yi", "sum"),
            hit_count=("hm_name", "size"),
        )
        .reset_index()
    )
    hm_rank = (
        row_summary.groupby("hm_name", dropna=False)
        .agg(
            total_abs_yi=("abs_net_amount_yi", "sum"),
            total_net_yi=("net_amount_yi", "sum"),
            hit_count=("hit_count", "sum"),
            stock_count=("ts_code", "nunique"),
        )
        .sort_values(["total_abs_yi", "hit_count"], ascending=[False, False])
        .head(max(1, int(max_hotmoney)))
        .reset_index()
    )

    groups: list[dict[str, Any]] = []
    for _, hm_row in hm_rank.iterrows():
        hm_name = _clean_text(hm_row["hm_name"], "未知游资")
        hm_rows = row_summary[row_summary["hm_name"] == hm_name].copy()
        stock_rank = (
            hm_rows.groupby(["ts_code", "ts_name"], dropna=False)
            .agg(
                net_amount_yi=("net_amount_yi", "sum"),
                abs_net_amount_yi=("abs_net_amount_yi", "sum"),
                hit_count=("hit_count", "sum"),
            )
            .sort_values(["abs_net_amount_yi", "hit_count"], ascending=[False, False])
            .head(max(1, int(max_stocks_per_hotmoney)))
            .reset_index()
        )

        stocks: list[dict[str, Any]] = []
        for _, stock_row in stock_rank.iterrows():
            ts_code = _clean_text(stock_row["ts_code"], "-")
            stock_name = _clean_text(stock_row["ts_name"], ts_code)
            stock_rows = hm_rows[(hm_rows["ts_code"] == ts_code) & (hm_rows["ts_name"] == stock_name)]
            stock_rows = stock_rows.sort_values(["abs_net_amount_yi", "hit_count"], ascending=[False, False])
            orgs: list[dict[str, str]] = []
            for _, org_row in stock_rows.iterrows():
                amount_label = _format_amount_label(org_row["net_amount_yi"])
                for org in _split_orgs(org_row["hm_orgs"], max_items=max_orgs_per_stock):
                    orgs.append({"name": org, "label": f"{org} {amount_label}"})
                    if len(orgs) >= max_orgs_per_stock:
                        break
                if len(orgs) >= max_orgs_per_stock:
                    break

            amount_yi = _numeric(stock_row["net_amount_yi"])
            stocks.append(
                {
                    "stock_name": stock_name,
                    "ts_code": ts_code,
                    "amount_yi": amount_yi,
                    "amount_label": _format_amount_label(amount_yi),
                    "hit_count": int(stock_row["hit_count"]),
                    "orgs": orgs or [{"name": "关联席位未披露", "label": f"关联席位未披露 {_format_amount_label(amount_yi)}"}],
                }
            )

        groups.append(
            {
                "hm_name": hm_name,
                "total_abs_yi": _numeric(hm_row["total_abs_yi"]),
                "total_net_yi": _numeric(hm_row["total_net_yi"]),
                "amount_label": _format_amount_label(hm_row["total_net_yi"]),
                "hit_count": int(hm_row["hit_count"]),
                "stock_count": int(hm_row["stock_count"]),
                "stocks": stocks,
            }
        )

    return {
        "trade_date_label": _trade_date_label(work),
        "total_records": int(len(work)),
        "groups": groups,
    }


def _render_stock(stock: dict[str, Any]) -> str:
    amount_yi = _numeric(stock.get("amount_yi"))
    direction_class = "is-positive" if amount_yi > 0 else "is-negative" if amount_yi < 0 else "is-neutral"
    stock_label = f"{_clean_text(stock.get('stock_name'))} {_clean_text(stock.get('amount_label'))}"
    org_html = "".join(
        f'<div class="ws-hotmoney-org-node">{escape(_clean_text(org.get("label")))}</div>'
        for org in stock.get("orgs", [])
    )
    return (
        '<div class="ws-hotmoney-stock-row">'
        f'<div class="ws-hotmoney-stock-node {direction_class}" title="{escape(_clean_text(stock.get("ts_code")))}">'
        f"{escape(stock_label)}</div>"
        f'<div class="ws-hotmoney-org-list">{org_html}</div>'
        "</div>"
    )


def _render_group(group: dict[str, Any], index: int) -> str:
    color = _GROUP_COLORS[index % len(_GROUP_COLORS)]
    stocks = "".join(_render_stock(stock) for stock in group.get("stocks", []))
    hm_name = escape(_clean_text(group.get("hm_name"), "未知游资"))
    group_meta = f'{int(group.get("stock_count") or 0)}股 / {int(group.get("hit_count") or 0)}次'
    return (
        f'<div class="ws-hotmoney-group" style="--hm-color:{color};">'
        '<div class="ws-hotmoney-group-line" aria-hidden="true"></div>'
        '<div class="ws-hotmoney-group-body">'
        f'<div class="ws-hotmoney-hm-node"><span>{hm_name}</span><small>{escape(group_meta)}</small></div>'
        f'<div class="ws-hotmoney-stock-list">{stocks}</div>'
        "</div>"
        "</div>"
    )


def render_hotmoney_tree_html(
    detail_df: pd.DataFrame | None,
    *,
    title: str = "游资龙虎图谱",
    subtitle: str = "",
    max_hotmoney: int = 8,
    max_stocks_per_hotmoney: int = 6,
    max_orgs_per_stock: int = 4,
) -> str:
    """Render the hot-money relationship tree as HTML/CSS, not an image."""
    model = build_hotmoney_tree_model(
        detail_df,
        max_hotmoney=max_hotmoney,
        max_stocks_per_hotmoney=max_stocks_per_hotmoney,
        max_orgs_per_stock=max_orgs_per_stock,
    )
    date_label = model.get("trade_date_label") or "-"
    subtitle_text = subtitle or f"{date_label} · {model.get('total_records', 0)}条明细"
    groups = model.get("groups", [])
    if groups:
        branch_html = "".join(_render_group(group, index) for index, group in enumerate(groups))
    else:
        branch_html = '<div class="ws-hotmoney-tree-empty">当前筛选条件下暂无可展示的游资关系。</div>'

    return f"""
<style>
.ws-hotmoney-tree {{
    margin: 0.75rem 0 1.15rem 0;
    overflow-x: auto;
    padding: 0.35rem 0.1rem 0.45rem 0.1rem;
}}
.ws-hotmoney-tree-stage {{
    min-width: 980px;
    display: grid;
    grid-template-columns: 260px minmax(640px, 1fr);
    gap: 32px;
    align-items: center;
    padding: 20px 8px 18px 8px;
    color: #123f42;
}}
.ws-hotmoney-tree-root-wrap {{
    position: sticky;
    left: 0;
    z-index: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 132px;
}}
.ws-hotmoney-tree-root {{
    width: 228px;
    min-height: 94px;
    border-radius: 8px;
    background: linear-gradient(135deg, #2dbbd2, #24b8cf);
    color: #fff;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    box-shadow: 0 14px 34px rgba(45, 187, 210, 0.20);
    text-align: center;
    position: relative;
}}
.ws-hotmoney-tree-root::after {{
    content: "";
    position: absolute;
    right: -32px;
    top: 50%;
    width: 32px;
    border-top: 4px solid #28d9c5;
}}
.ws-hotmoney-tree-root strong {{
    font-size: 1.28rem;
    line-height: 1.25;
}}
.ws-hotmoney-tree-root span {{
    margin-top: 0.35rem;
    font-size: 0.92rem;
    font-weight: 700;
    opacity: 0.94;
}}
.ws-hotmoney-branches {{
    position: relative;
    display: flex;
    flex-direction: column;
    gap: 34px;
}}
.ws-hotmoney-branches::before {{
    content: "";
    position: absolute;
    left: -16px;
    top: 28px;
    bottom: 28px;
    border-left: 4px solid #28d9c5;
}}
.ws-hotmoney-group {{
    position: relative;
}}
.ws-hotmoney-group-line {{
    position: absolute;
    left: -16px;
    top: 31px;
    width: 34px;
    border-top: 4px solid var(--hm-color);
}}
.ws-hotmoney-group-body {{
    display: grid;
    grid-template-columns: 160px minmax(520px, 1fr);
    gap: 22px;
    align-items: center;
}}
.ws-hotmoney-hm-node {{
    min-height: 62px;
    border-radius: 8px;
    background: var(--hm-color);
    color: #fff;
    display: flex;
    flex-direction: column;
    justify-content: center;
    padding: 0.55rem 0.9rem;
    box-shadow: 0 10px 24px rgba(16, 82, 78, 0.14);
}}
.ws-hotmoney-hm-node span {{
    font-size: 1.08rem;
    font-weight: 800;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}}
.ws-hotmoney-hm-node small {{
    margin-top: 0.22rem;
    font-size: 0.76rem;
    opacity: 0.9;
}}
.ws-hotmoney-stock-list {{
    display: flex;
    flex-direction: column;
    gap: 12px;
    position: relative;
}}
.ws-hotmoney-stock-list::before {{
    content: "";
    position: absolute;
    left: -13px;
    top: 20px;
    bottom: 20px;
    border-left: 2px solid var(--hm-color);
    opacity: 0.82;
}}
.ws-hotmoney-stock-row {{
    display: grid;
    grid-template-columns: 220px minmax(280px, 1fr);
    gap: 24px;
    align-items: center;
    position: relative;
}}
.ws-hotmoney-stock-row::before {{
    content: "";
    position: absolute;
    left: -13px;
    top: 21px;
    width: 13px;
    border-top: 2px solid var(--hm-color);
}}
.ws-hotmoney-stock-node {{
    min-height: 42px;
    border-radius: 8px;
    color: #fff;
    display: flex;
    align-items: center;
    padding: 0 0.75rem;
    font-size: 0.95rem;
    font-weight: 800;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}}
.ws-hotmoney-stock-node.is-positive {{
    background: #f20f0f;
}}
.ws-hotmoney-stock-node.is-negative {{
    background: #11822b;
}}
.ws-hotmoney-stock-node.is-neutral {{
    background: #64757a;
}}
.ws-hotmoney-org-list {{
    display: flex;
    flex-direction: column;
    gap: 8px;
    position: relative;
}}
.ws-hotmoney-org-list::before {{
    content: "";
    position: absolute;
    left: -13px;
    top: 20px;
    bottom: 20px;
    border-left: 2px solid #40524e;
    opacity: 0.65;
}}
.ws-hotmoney-org-node {{
    min-height: 38px;
    border-radius: 8px;
    background: #d9faf5;
    color: #0e4a4c;
    display: flex;
    align-items: center;
    padding: 0 0.72rem;
    font-size: 0.93rem;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    position: relative;
}}
.ws-hotmoney-org-node::before {{
    content: "";
    position: absolute;
    left: -13px;
    top: 19px;
    width: 13px;
    border-top: 2px solid #40524e;
    opacity: 0.65;
}}
.ws-hotmoney-tree-empty {{
    border-radius: 8px;
    border: 1px dashed rgba(14, 74, 76, 0.25);
    padding: 1rem;
    color: #607086;
    background: rgba(217, 250, 245, 0.32);
}}
@media (max-width: 900px) {{
    .ws-hotmoney-tree-stage {{
        min-width: 880px;
        grid-template-columns: 220px minmax(600px, 1fr);
    }}
    .ws-hotmoney-tree-root {{
        width: 196px;
    }}
}}
</style>
<section class="ws-hotmoney-tree" role="region" aria-label="游资关系图">
  <div class="ws-hotmoney-tree-stage">
    <div class="ws-hotmoney-tree-root-wrap">
      <div class="ws-hotmoney-tree-root">
        <strong>{escape(title)}</strong>
        <span>{escape(date_label)}</span>
      </div>
    </div>
    <div class="ws-hotmoney-branches">
      <div class="ws-hotmoney-tree-caption">{escape(subtitle_text)}</div>
      {branch_html}
    </div>
  </div>
</section>
"""
