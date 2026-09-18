"""Multi-fund comparison on the watchlist's already loaded holding snapshots."""

from __future__ import annotations

import hashlib
import math

import pandas as pd
import streamlit as st

from src.fund_watchlist_comparison import build_fund_comparison


COMPARISON_MODES = ["股票持仓", "行业分布", "基金重叠"]


def _fund_label(code: str, funds: pd.DataFrame) -> str:
    return f"{funds.at[code, 'fund_name']}（{code}）"


def _cell_text(value, presence, *, counts: bool = False) -> str:
    if pd.isna(presence):
        return "暂无数据"
    if not bool(presence):
        return "未观察到"
    if pd.isna(value) or not math.isfinite(float(value)):
        return "权重缺失" if not counts else "暂无数据"
    return f"{int(value)} 只" if counts else f"{float(value):.2f}%"


def build_comparison_display(
    comparison: dict,
    mode: str,
    *,
    stock_scope: str = "全部已加载股票",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return a readable matrix and its styles without replacing unknowns by zero."""
    funds = comparison["funds"]
    labels = {code: _fund_label(code, funds) for code in funds.index}
    counts = mode == "基金重叠"
    if counts:
        numeric = comparison["overlap_counts"].copy()
        presence = numeric.notna().astype("boolean").mask(numeric.isna(), pd.NA)
        display = pd.DataFrame({"基金": [labels[code] for code in numeric.index]}, index=numeric.index)
    elif mode == "行业分布":
        numeric = comparison["industries"].copy()
        presence = comparison["industry_presence"].reindex_like(numeric)
        order = numeric.max(axis=1).sort_values(ascending=False, kind="stable", na_position="last").index
        numeric, presence = numeric.loc[order], presence.loc[order]
        display = pd.DataFrame({"行业": numeric.index}, index=numeric.index)
    else:
        details = comparison["stock_details"].copy()
        if stock_scope == "共同持仓":
            details = details[details["observed_fund_count"] >= 2]
        elif stock_scope == "仅单只基金出现":
            details = details[details["observed_fund_count"] == 1]
        details["_max_weight"] = comparison["holdings"].max(axis=1).reindex(details.index)
        details = details.sort_values(
            ["observed_fund_count", "_max_weight"],
            ascending=[False, False], kind="stable", na_position="last",
        )
        numeric = comparison["holdings"].reindex(details.index)
        presence = comparison["holding_presence"].reindex_like(numeric)
        display = pd.DataFrame(
            {
                "股票": [f"{row['stock_name']}（{symbol}）" for symbol, row in details.iterrows()],
                "行业": details["industry"],
                "出现基金数": details["observed_fund_count"],
            }, index=details.index,
        )
    for code in numeric.columns:
        display[labels[code]] = [
            _cell_text(value, seen, counts=counts)
            for value, seen in zip(numeric[code], presence[code])
        ]
    styles = pd.DataFrame("", index=display.index, columns=display.columns)
    maximum = numeric.max().max() if not numeric.empty else 0.0
    maximum = float(maximum) if pd.notna(maximum) and maximum > 0 else 1.0
    for code in numeric.columns:
        for row, value in numeric[code].items():
            if pd.notna(value) and math.isfinite(float(value)):
                strength = min(1.0, max(0.0, float(value) / maximum))
                # A single blue scale represents magnitude, not investment merit.
                red, green, blue = (round(239 - 209 * strength), round(246 - 182 * strength), round(255 - 80 * strength))
                foreground = "#ffffff" if strength >= 0.55 else "#172554"
                styles.at[row, labels[code]] = f"background-color:rgb({red},{green},{blue});color:{foreground}"
    return display.reset_index(drop=True), styles.reset_index(drop=True)


def _render_coverage(comparison: dict) -> None:
    funds = comparison["funds"].copy()
    coverage = pd.DataFrame(
        {
            "基金": [_fund_label(code, funds) for code in funds.index],
            "持仓报告期": [
                pd.Timestamp(value).strftime("%Y-%m-%d") if pd.notna(value) else "未知"
                for value in funds["report_date"]
            ],
            "已加载股票数": funds["holding_count"].to_numpy(),
            "已加载股票权重合计(%)": funds["observed_weight"].to_numpy(),
            "数据状态": funds["data_status"].map({
                "available": "样本权重可用", "partial": "部分权重待核实", "no_data": "暂无持仓数据",
            }).fillna("待核实").to_numpy(),
        }
    )
    with st.expander("持仓报告期与数据覆盖", expanded=False):
        st.dataframe(
            coverage, hide_index=True, width="stretch",
            column_config={
                "已加载股票权重合计(%)": st.column_config.NumberColumn(format="%.2f%%"),
            },
        )
        st.caption("权重合计仅对应当前已加载股票，不代表基金的总股票仓位；报告期是持仓截止日。")


def render_fund_watchlist_comparison(items: list[dict], *, username: str) -> None:
    """Render matrices scoped to this user's visible funds, with no data fetches."""
    comparison = build_fund_comparison(items)
    funds = comparison["funds"]
    st.markdown("### 多基金差异矩阵")
    st.caption("横向比较同一只股票、同一行业在不同基金中的披露权重，也可查看基金之间的共同持股数量。")
    if funds.empty:
        st.info("当前没有可对比的自选基金。")
        return

    user_key = hashlib.sha256(str(username).encode("utf-8")).hexdigest()[:16]
    prefix = f"fund_comparison_{user_key}"
    codes = funds.index.tolist()
    options_key, selection_key = f"{prefix}_options", f"{prefix}_funds"
    previous_options = st.session_state.get(options_key)
    previous_selection = st.session_state.get(selection_key)
    if previous_selection is None or (
        previous_options and set(previous_selection) == set(previous_options)
    ):
        next_selection = list(codes)
    else:
        next_selection = [code for code in previous_selection if code in codes]
    if previous_selection != next_selection:
        st.session_state[selection_key] = next_selection
    st.session_state[options_key] = list(codes)
    selected = st.multiselect(
        "参与对比的基金", options=codes,
        format_func=lambda code: _fund_label(code, funds), key=selection_key,
        placeholder="选择至少两只自选基金",
    )
    if len(selected) < 2:
        st.info("请选择至少两只基金进行对比。")
        return
    comparison = build_fund_comparison(
        [item for item in items if str(item.get("fund_code", "")).strip().upper() in selected]
    )
    st.caption(
        "范围：本页已加载的股票持仓，每只基金最多 10 只，可能不包含港股、境外股票等资产。"
        "权重为占基金股票资产的比例，不是占基金净资产或你的持仓金额；不将前十只重新归一到 100%。"
    )
    if comparison["warnings"]:
        st.warning("\n\n".join(dict.fromkeys(comparison["warnings"])))
    _render_coverage(comparison)

    mode = st.radio("对比维度", COMPARISON_MODES, horizontal=True, key=f"{prefix}_mode")
    stock_scope = "全部已加载股票"
    if mode == "股票持仓":
        stock_scope = st.radio(
            "股票范围", ["全部已加载股票", "共同持仓", "仅单只基金出现"],
            horizontal=True, key=f"{prefix}_scope",
        )
    display, styles = build_comparison_display(comparison, mode, stock_scope=stock_scope)
    if display.empty:
        st.info("当前选择下没有符合条件的持仓记录。")
        return
    numeric_columns = list(display.columns[(1 if mode != "股票持仓" else 3):])
    column_config = {column: st.column_config.TextColumn(width="medium") for column in numeric_columns}
    column_config[display.columns[0]] = st.column_config.TextColumn(width="medium", pinned=True)
    st.dataframe(
        display.style.apply(lambda _: styles, axis=None),
        width="stretch", hide_index=True,
        height=min(720, max(200, (len(display) + 1) * 38)),
        column_config=column_config, key=f"{prefix}_matrix_{mode}",
    )
    if mode == "基金重叠":
        st.caption(
            "颜色越深，共同股票越多。对角线为各基金已加载的独立股票数；"
            "0 只仅表示当前样本没有共同股票，不能推断完整持仓不重叠或未来回撤较低。"
        )
    else:
        st.caption(
            "颜色越深，已披露权重越大。未观察到：未出现在当前样本，不等于未持有；"
            "权重缺失：已观察到持仓但权重不可用；暂无数据：该基金没有可对比的持仓。"
        )
