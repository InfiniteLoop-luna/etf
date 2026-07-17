import pandas as pd
import streamlit as st

from src.security_data_cache import (
    load_fund_hot_stock_periods,
    load_fund_object_model,
    load_fund_search,
)
from src.user_watchlist_store import (
    add_watchlist_item,
    is_in_watchlist,
    normalize_username,
    remove_watchlist_item,
)


def _format_number(value, digits: int = 2, scale: float = 1.0, suffix: str = "") -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "-"
    return f"{float(numeric) / float(scale):,.{digits}f}{suffix}"


def _format_pct(value, digits: int = 2) -> str:
    return _format_number(value, digits=digits, suffix="%")


def _format_date(value) -> str:
    raw = pd.to_datetime(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(raw):
        return "-"
    return pd.Timestamp(raw).strftime("%Y-%m-%d")


def _format_datetime(value) -> str:
    raw = pd.to_datetime(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(raw):
        return "-"
    return pd.Timestamp(raw).strftime("%Y-%m-%d %H:%M:%S")


def _resolve_logged_in_username() -> str:
    return normalize_username(st.session_state.get("logged_in_username", ""))


def _holdings_frame(holdings: list[dict]) -> pd.DataFrame:
    rows = []
    for row in holdings or []:
        rows.append(
            {
                "股票名称": row.get("stock_name") or "-",
                "股票代码": row.get("symbol") or "-",
                "持仓市值(亿元)": row.get("market_value_yi"),
                "权重(%)": row.get("weight"),
                "变动": row.get("change_label") or "-",
            }
        )
    return pd.DataFrame(rows)


def _change_frame(holdings: list[dict], change_flag: str) -> pd.DataFrame:
    filtered = [row for row in holdings or [] if str(row.get("change_flag") or "") == change_flag]
    return _holdings_frame(filtered)


def _render_watchlist_action(username: str, fund_code: str, fund_name: str, *, key_prefix: str) -> None:
    cols = st.columns([1.2, 1.2, 2.6])
    if not username:
        cols[0].button("⭐ 加入自选", key=f"{key_prefix}_btn_fund_object_add_disabled_{fund_code}", disabled=True)
        cols[1].button("移除自选", key=f"{key_prefix}_btn_fund_object_remove_disabled_{fund_code}", disabled=True)
        cols[2].info("先在侧边栏登录用户名，才能把基金加入个人自选。")
        return

    already_in_watchlist = False
    try:
        already_in_watchlist = is_in_watchlist(username, fund_code, "fund")
    except Exception:
        already_in_watchlist = False

    if cols[0].button(
        "✅ 已在自选" if already_in_watchlist else "⭐ 加入自选",
        key=f"{key_prefix}_btn_fund_object_add_{fund_code}",
        disabled=already_in_watchlist,
    ):
        try:
            add_watchlist_item(username, fund_code, security_name=fund_name, security_type="fund")
            st.success(f"已加入 {username} 的自选基金：{fund_name}")
            st.rerun()
        except Exception as exc:
            st.error(f"加入自选失败：{exc}")

    if cols[1].button(
        "🗑 移除自选",
        key=f"{key_prefix}_btn_fund_object_remove_{fund_code}",
        disabled=not already_in_watchlist,
    ):
        try:
            removed = remove_watchlist_item(username, fund_code, "fund")
            if removed:
                st.success(f"已从 {username} 的自选基金中移除：{fund_name}")
                st.rerun()
            else:
                st.info("当前基金不在自选中。")
        except Exception as exc:
            st.error(f"移除自选失败：{exc}")

    cols[2].caption(f"当前登录用户：{username}")


def render_fund_object_page() -> None:
    st.subheader("🧩 基金对象页")
    st.caption("围绕单只基金聚合：画像、净值、15:00估值、前十大持仓。")

    query = st.text_input(
        "搜索基金",
        placeholder="输入基金代码、名称或管理人，例如 512480、国联安中证全指半导体ETF、易方达",
        key="fund_object_query",
    ).strip()
    if not query:
        st.info("请输入关键字开始检索基金。")
        return

    try:
        candidate_df = load_fund_search(query, limit=30)
    except Exception as exc:
        st.error(f"检索基金失败：{exc}")
        return

    if candidate_df is None or candidate_df.empty:
        st.warning("未检索到匹配基金，请尝试更换关键字。")
        return

    option_rows = candidate_df.copy()
    option_rows["label"] = option_rows.apply(
        lambda row: (
            f"{row.get('name') or row.get('fund_code') or '-'}"
            f"（{row.get('fund_code') or '-'} · {row.get('management') or '-'}）"
        ),
        axis=1,
    )
    option_labels = option_rows["label"].tolist()
    selected_label = st.selectbox(
        "匹配结果",
        options=option_labels,
        index=0,
        key="fund_object_selected",
    )
    selected_row = option_rows.iloc[option_labels.index(selected_label)]
    fund_code = str(selected_row.get("fund_code") or "").strip().upper()
    if not fund_code:
        st.warning("当前选中的基金代码为空。")
        return

    period_options = ["最新披露"] + load_fund_hot_stock_periods()
    selected_period = st.selectbox(
        "持仓披露期",
        options=period_options,
        index=0,
        key=f"fund_object_period_{fund_code}",
    )
    target_period = "" if selected_period == "最新披露" else selected_period

    try:
        payload = load_fund_object_model(fund_code, period=target_period, top_n=10)
    except Exception as exc:
        st.error(f"加载基金对象页失败：{exc}")
        return

    item = payload.get("item") or {}
    meta = payload.get("meta") or {}
    holdings = list(item.get("holdings") or [])
    errors = list(payload.get("errors") or [])

    fund_name = str(item.get("fund_name") or meta.get("name") or fund_code)
    st.markdown(f"### {fund_name}")
    subtitle_parts = [fund_code]
    if item.get("management") and item.get("management") != "-":
        subtitle_parts.append(str(item.get("management")))
    if item.get("fund_type") and item.get("fund_type") != "-":
        subtitle_parts.append(str(item.get("fund_type")))
    st.caption(" | ".join(subtitle_parts))

    _render_watchlist_action(_resolve_logged_in_username(), fund_code, fund_name, key_prefix="top")

    if errors:
        for message in errors:
            st.warning(message)

    latest_estimate_value = item.get("closing_estimate_pct")
    latest_estimate_date = item.get("closing_estimate_date")
    if latest_estimate_value is None:
        latest_estimate_value = item.get("latest_closing_estimate_pct")
        latest_estimate_date = item.get("latest_closing_estimate_date")

    metric_cols = st.columns(6)
    metric_cols[0].metric("净值日期", _format_date(item.get("nav_date")))
    metric_cols[1].metric("单位净值", _format_number(item.get("unit_nav"), digits=4))
    metric_cols[2].metric("日涨跌幅", _format_pct(item.get("daily_change_pct")))
    metric_cols[3].metric("15:00估值", _format_pct(latest_estimate_value))
    metric_cols[4].metric("估值偏差", _format_pct(item.get("estimate_deviation_pct")))
    metric_cols[5].metric("Top10 集中度", _format_pct(item.get("top10_ratio")))
    st.caption(
        " | ".join(
            [
                f"最近估值日期：{_format_date(latest_estimate_date)}",
                f"估值覆盖权重：{_format_pct(item.get('latest_closing_estimate_covered_weight_pct'))}",
                f"最近披露期：{_format_date(item.get('latest_end_date'))}",
            ]
        )
    )

    tab_overview, tab_nav, tab_holdings, tab_changes, tab_watch = st.tabs(
        ["📌 概览", "📈 净值与估值", "🏦 持仓", "🔍 持仓变化", "⭐ 自选与跟踪"]
    )

    with tab_overview:
        left, right = st.columns([1.0, 1.0])
        with left:
            st.markdown("##### 🧾 基金画像")
            st.dataframe(
                pd.DataFrame(
                    [
                        {"字段": "基金代码", "值": fund_code},
                        {"字段": "基金名称", "值": fund_name},
                        {"字段": "基金类型", "值": item.get("fund_type") or meta.get("fund_type") or "-"},
                        {"字段": "管理人", "值": item.get("management") or meta.get("management") or "-"},
                        {"字段": "基金规模(亿份)", "值": _format_number(item.get("issue_amount"), digits=2)},
                        {"字段": "最近披露期", "值": _format_date(item.get("latest_end_date"))},
                    ]
                ),
                use_container_width=True,
                hide_index=True,
            )
        with right:
            st.markdown("##### 📊 净值与估值摘要")
            st.dataframe(
                pd.DataFrame(
                    [
                        {"字段": "净值日期", "值": _format_date(item.get("nav_date"))},
                        {"字段": "单位净值", "值": _format_number(item.get("unit_nav"), digits=4)},
                        {"字段": "日涨跌幅", "值": _format_pct(item.get("daily_change_pct"))},
                        {"字段": "15:00估值", "值": _format_pct(latest_estimate_value)},
                        {"字段": "估值偏差", "值": _format_pct(item.get("estimate_deviation_pct"))},
                        {"字段": "估值时间", "值": _format_datetime(item.get("latest_closing_estimate_quote_time"))},
                    ]
                ),
                use_container_width=True,
                hide_index=True,
            )

        st.markdown("##### 🧺 前十大持仓预览")
        holdings_preview = _holdings_frame(holdings)
        if holdings_preview.empty:
            st.info("当前披露期暂无持仓数据。")
        else:
            st.dataframe(holdings_preview, use_container_width=True, hide_index=True, height=360)

    with tab_nav:
        st.markdown("##### 净值快照")
        st.dataframe(
            pd.DataFrame(
                [
                    {"字段": "净值日期", "值": _format_date(item.get("nav_date"))},
                    {"字段": "单位净值", "值": _format_number(item.get("unit_nav"), digits=4)},
                    {"字段": "日涨跌幅", "值": _format_pct(item.get("daily_change_pct"))},
                    {"字段": "净值来源", "值": item.get("nav_source") or "-"},
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )
        st.markdown("##### 15:00 估值快照")
        st.dataframe(
            pd.DataFrame(
                [
                    {"字段": "最近估值日期", "值": _format_date(latest_estimate_date)},
                    {"字段": "15:00估值", "值": _format_pct(latest_estimate_value)},
                    {"字段": "估值偏差", "值": _format_pct(item.get("estimate_deviation_pct"))},
                    {
                        "字段": "覆盖权重",
                        "值": _format_pct(item.get("latest_closing_estimate_covered_weight_pct")),
                    },
                    {"字段": "估值时间", "值": _format_datetime(item.get("latest_closing_estimate_quote_time"))},
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )
        if item.get("closing_estimate_pct") is None and item.get("latest_closing_estimate_pct") is not None:
            st.info("最近估值快照与当前净值日期未完全对齐，因此估值偏差暂不计算。")

    with tab_holdings:
        st.markdown("##### 前十大持仓")
        st.caption(f"当前查看披露期：{selected_period}")
        holdings_df = _holdings_frame(holdings)
        if holdings_df.empty:
            st.info("当前披露期暂无持仓数据。")
        else:
            st.dataframe(holdings_df, use_container_width=True, hide_index=True, height=420)

    with tab_changes:
        st.markdown("##### 持仓变化摘要")
        change_cols = st.columns(4)
        change_cols[0].metric("新进", f"{int(item.get('new_count', 0))}")
        change_cols[1].metric("增持", f"{int(item.get('increase_count', 0))}")
        change_cols[2].metric("减持", f"{int(item.get('decrease_count', 0))}")
        change_cols[3].metric("稳定", f"{int(item.get('stable_count', 0))}")

        for label, flag in [("新进", "new"), ("增持", "increase"), ("减持", "decrease")]:
            st.markdown(f"##### {label}")
            frame = _change_frame(holdings, flag)
            if frame.empty:
                st.info(f"当前披露期暂无{label}持仓。")
            else:
                st.dataframe(frame, use_container_width=True, hide_index=True, height=240)

    with tab_watch:
        st.markdown("##### 跟踪建议")
        st.markdown(
            "\n".join(
                [
                    f"- 最近披露期：`{_format_date(item.get('latest_end_date'))}`",
                    f"- Top10 集中度：`{_format_pct(item.get('top10_ratio'))}`",
                    f"- 最近估值快照：`{_format_date(latest_estimate_date)}` / `{_format_pct(latest_estimate_value)}`",
                ]
            )
        )
        st.markdown("##### 自选操作")
        _render_watchlist_action(_resolve_logged_in_username(), fund_code, fund_name, key_prefix="track")
        if item.get("load_error"):
            st.info(f"数据备注：{item.get('load_error')}")
