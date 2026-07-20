import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from src.margin_fetcher import (
    _get_engine_cached as get_margin_engine_cached,
    build_margin_latest_metrics,
    build_margin_price_divergence_summary,
    build_margin_price_overlay_frame,
    build_margin_signal_summary,
    get_margin_latest_date,
    prepare_margin_display_frame,
    query_margin_detail_history,
    query_margin_exchange_history,
    query_margin_exchange_snapshot,
    query_margin_market_latest,
    query_stock_price_history,
)
from src.security_data_cache import load_security_profile, load_security_search


def _format_number(value, digits: int = 2, scale: float = 1.0) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "-"
    return f"{float(numeric) / float(scale):,.{digits}f}"


def _format_signed_number(value, digits: int = 2, unit: str = "") -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "-"
    return f"{float(numeric):+,.{digits}f}{unit}"


def _format_date(value) -> str:
    raw = pd.to_datetime(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(raw):
        return "-"
    return pd.Timestamp(raw).strftime("%Y-%m-%d")


def _render_margin_exchange_snapshot(snapshot_df: pd.DataFrame) -> None:
    if snapshot_df is None or snapshot_df.empty:
        st.info("暂无市场两融汇总。")
        return

    display_df = snapshot_df.copy()
    display_df["trade_date"] = pd.to_datetime(display_df["trade_date"], errors="coerce")
    display_df["交易所"] = display_df["exchange_id"].replace(
        {"SSE": "上交所", "SZSE": "深交所", "BSE": "北交所"}
    )
    display_df["融资余额(亿)"] = pd.to_numeric(display_df["rzye"], errors="coerce") / 100000000.0
    display_df["融券余额(亿)"] = pd.to_numeric(display_df["rqye"], errors="coerce") / 100000000.0
    display_df["融资融券余额(亿)"] = pd.to_numeric(display_df["rzrqye"], errors="coerce") / 100000000.0
    display_df["融资买入额(亿)"] = pd.to_numeric(display_df["rzmre"], errors="coerce") / 100000000.0
    display_df["融资偿还额(亿)"] = pd.to_numeric(display_df["rzche"], errors="coerce") / 100000000.0
    show_cols = [
        col
        for col in [
            "交易所",
            "trade_date",
            "融资余额(亿)",
            "融券余额(亿)",
            "融资融券余额(亿)",
            "融资买入额(亿)",
            "融资偿还额(亿)",
        ]
        if col in display_df.columns
    ]
    st.dataframe(display_df[show_cols], use_container_width=True, hide_index=True)


def _build_market_overview_frame(exchange_history_df: pd.DataFrame) -> pd.DataFrame:
    if exchange_history_df is None or exchange_history_df.empty:
        return pd.DataFrame()

    work = exchange_history_df.copy()
    work["trade_date"] = pd.to_datetime(work["trade_date"], errors="coerce")
    numeric_cols = ["rzye", "rqye", "rzmre", "rzche", "rqmcl", "rzrqye", "rqyl"]
    for col in numeric_cols:
        work[col] = pd.to_numeric(work.get(col), errors="coerce")
    work = work.dropna(subset=["trade_date"])
    if work.empty:
        return pd.DataFrame()

    grouped = (
        work.groupby("trade_date", as_index=False)[numeric_cols]
        .sum(min_count=1)
        .sort_values("trade_date")
        .reset_index(drop=True)
    )
    grouped["rzye_yi"] = grouped["rzye"] / 100000000.0
    grouped["rqye_yi"] = grouped["rqye"] / 100000000.0
    grouped["rzrqye_yi"] = grouped["rzrqye"] / 100000000.0
    grouped["rz_net_buy_yi"] = (grouped["rzmre"] - grouped["rzche"]) / 100000000.0
    grouped["rqmcl_wan"] = grouped["rqmcl"] / 10000.0
    grouped["rqyl_wan"] = grouped["rqyl"] / 10000.0
    denominator = grouped["rzrqye"].where(grouped["rzrqye"] != 0)
    grouped["rqye_ratio_pct"] = grouped["rqye"] / denominator * 100.0
    return grouped


def _build_market_latest_leaderboard(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    prepared = prepare_margin_display_frame(snapshot_df)
    if prepared.empty:
        return pd.DataFrame()

    prepared["name"] = prepared.get("name", "").fillna("").astype(str)
    prepared["ts_code"] = prepared.get("ts_code", "").fillna("").astype(str).str.upper()
    prepared["融资净买入强度(%)"] = (
        pd.to_numeric(prepared["rz_net_buy_yi"], errors="coerce")
        / pd.to_numeric(prepared["rzye_yi"], errors="coerce").replace(0, pd.NA)
        * 100.0
    )
    prepared["融券净卖出强度(万股/万余量)"] = (
        pd.to_numeric(prepared["rq_net_sell_wan"], errors="coerce")
        / pd.to_numeric(prepared["rqyl_wan"], errors="coerce").replace(0, pd.NA)
    )

    def _build_tag(row: pd.Series) -> str:
        tags: list[str] = []
        rz_net_buy_yi = pd.to_numeric(pd.Series([row.get("rz_net_buy_yi")]), errors="coerce").iloc[0]
        rq_net_sell_wan = pd.to_numeric(pd.Series([row.get("rq_net_sell_wan")]), errors="coerce").iloc[0]
        rzrqye_yi = pd.to_numeric(pd.Series([row.get("rzrqye_yi")]), errors="coerce").iloc[0]
        if pd.notna(rz_net_buy_yi) and rz_net_buy_yi >= 1:
            tags.append("融资净买入强")
        if pd.notna(rq_net_sell_wan) and rq_net_sell_wan >= 100:
            tags.append("融券压力抬升")
        if pd.notna(rzrqye_yi) and rzrqye_yi >= 30:
            tags.append("高余额标的")
        return " / ".join(tags) if tags else "常规观察"

    prepared["观察标签"] = prepared.apply(_build_tag, axis=1)
    return prepared


def _render_market_overview_section(margin_engine) -> None:
    try:
        latest_snapshot_df = query_margin_exchange_snapshot(engine=margin_engine)
    except Exception as exc:
        st.warning(f"加载市场两融汇总失败：{exc}")
        latest_snapshot_df = pd.DataFrame()

    try:
        history_df = query_margin_exchange_history(engine=margin_engine)
    except Exception as exc:
        st.warning(f"加载市场两融历史失败：{exc}")
        history_df = pd.DataFrame()

    market_overview_df = _build_market_overview_frame(history_df)
    if market_overview_df.empty:
        st.info("暂无可用的市场两融总览数据。")
    else:
        latest = market_overview_df.iloc[-1]
        previous = market_overview_df.iloc[-2] if len(market_overview_df) > 1 else latest
        rank_pct = float((market_overview_df["rzrqye_yi"] <= latest["rzrqye_yi"]).mean() * 100.0)
        market_cols = st.columns(4)
        market_cols[0].metric(
            "全市场两融余额(亿)",
            _format_number(latest.get("rzrqye_yi"), digits=2),
            _format_signed_number(
                pd.to_numeric(pd.Series([latest.get("rzrqye_yi") - previous.get("rzrqye_yi")]), errors="coerce").iloc[0],
                digits=2,
                unit=" 亿",
            ),
        )
        market_cols[1].metric(
            "当日融资净买入(亿)",
            _format_signed_number(latest.get("rz_net_buy_yi"), digits=2),
        )
        market_cols[2].metric(
            "融券余额占比",
            f"{pd.to_numeric(pd.Series([latest.get('rqye_ratio_pct')]), errors='coerce').iloc[0]:.2f}%"
            if pd.notna(pd.to_numeric(pd.Series([latest.get("rqye_ratio_pct")]), errors="coerce").iloc[0])
            else "-",
        )
        market_cols[3].metric("近窗余额分位", f"{rank_pct:.0f}%")

        st.markdown(
            "\n".join(
                [
                    f"- 最新交易日：`{_format_date(latest.get('trade_date'))}`",
                    f"- 全市场融资余额 `{_format_number(latest.get('rzye_yi'), digits=2)}` 亿，融券余额 `{_format_number(latest.get('rqye_yi'), digits=2)}` 亿",
                    f"- 近窗融资净买入读法：若余额分位高且当日净买入继续放大，说明杠杆情绪仍在升温",
                ]
            )
        )

        history_window = st.selectbox(
            "市场总览窗口",
            options=[20, 60, 120, 240, 9999],
            index=2,
            format_func=lambda value: "全部" if value >= 9999 else f"近 {value} 个交易日",
            key="market_margin_overview_window",
        )
        plot_df = market_overview_df if history_window >= 9999 else market_overview_df.tail(int(history_window))

        overview_fig = make_subplots(specs=[[{"secondary_y": True}]])
        overview_fig.add_trace(
            go.Bar(
                x=plot_df["trade_date"],
                y=plot_df["rz_net_buy_yi"],
                name="全市场融资净买入(亿)",
                marker_color="#2563eb",
                opacity=0.55,
            ),
            secondary_y=False,
        )
        overview_fig.add_trace(
            go.Scatter(
                x=plot_df["trade_date"],
                y=plot_df["rzrqye_yi"],
                name="全市场两融余额(亿)",
                mode="lines",
                line=dict(color="#f59e0b", width=2.2),
            ),
            secondary_y=True,
        )
        overview_fig.add_trace(
            go.Scatter(
                x=plot_df["trade_date"],
                y=plot_df["rqye_yi"],
                name="全市场融券余额(亿)",
                mode="lines",
                line=dict(color="#ef4444", width=1.5, dash="dot"),
            ),
            secondary_y=True,
        )
        overview_fig.update_layout(
            title="市场两融温度",
            height=400,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.01),
            margin=dict(l=20, r=20, t=55, b=20),
        )
        overview_fig.update_yaxes(title_text="融资净买入(亿)", secondary_y=False)
        overview_fig.update_yaxes(title_text="余额(亿)", secondary_y=True)
        st.plotly_chart(overview_fig, use_container_width=True)

    st.markdown("##### 当日市场两融汇总")
    _render_margin_exchange_snapshot(latest_snapshot_df)

    try:
        market_latest_df = query_margin_market_latest(limit=300, engine=margin_engine)
    except Exception as exc:
        st.warning(f"加载两融异动榜失败：{exc}")
        market_latest_df = pd.DataFrame()

    leaderboard_df = _build_market_latest_leaderboard(market_latest_df)
    st.markdown("##### 两融异动榜")
    if leaderboard_df.empty:
        st.info("暂无可用的个股两融异动榜数据。")
        return

    sort_label = st.selectbox(
        "异动榜排序",
        options=["融资净买入(亿)", "融资净买入强度(%)", "融券净卖出(万股)", "两融余额(亿)"],
        index=0,
        key="market_margin_leaderboard_sort",
    )
    sort_column_map = {
        "融资净买入(亿)": "rz_net_buy_yi",
        "融资净买入强度(%)": "融资净买入强度(%)",
        "融券净卖出(万股)": "rq_net_sell_wan",
        "两融余额(亿)": "rzrqye_yi",
    }
    sort_col = sort_column_map[sort_label]
    sorted_df = leaderboard_df.sort_values(by=sort_col, ascending=False, na_position="last").head(30)
    st.caption("口径说明：异动榜基于最新交易日两融明细快照，适合先筛候选，再点进个股页看连续性。")
    st.dataframe(
        sorted_df[
            [
                "name",
                "ts_code",
                "rzrqye_yi",
                "rz_net_buy_yi",
                "融资净买入强度(%)",
                "rq_net_sell_wan",
                "rqye_yi",
                "观察标签",
            ]
        ].rename(
            columns={
                "name": "名称",
                "ts_code": "代码",
                "rzrqye_yi": "两融余额(亿)",
                "rz_net_buy_yi": "融资净买入(亿)",
                "融资净买入强度(%)": "融资净买入强度(%)",
                "rq_net_sell_wan": "融券净卖出(万股)",
                "rqye_yi": "融券余额(亿)",
                "观察标签": "观察标签",
            }
        ),
        use_container_width=True,
        hide_index=True,
        height=560,
    )

    candidate_options = sorted_df.apply(
        lambda row: f"{str(row.get('name') or row.get('ts_code') or '').strip()}（{str(row.get('ts_code') or '').strip()}）",
        axis=1,
    ).tolist()
    if candidate_options:
        selected_candidate = st.selectbox(
            "候选股联动",
            options=candidate_options,
            index=0,
            key="market_margin_candidate_select",
        )
        selected_candidate_row = sorted_df.iloc[candidate_options.index(selected_candidate)]
        candidate_code = str(selected_candidate_row.get("ts_code") or "").strip().upper()
        candidate_name = str(selected_candidate_row.get("name") or candidate_code).strip()
        action_cols = st.columns([1.3, 2.7])
        with action_cols[0]:
            if st.button("带入下方个股分析", key="market_margin_apply_candidate", type="primary", use_container_width=True):
                st.session_state["margin_page_prefill_query"] = candidate_code
                st.session_state["margin_page_prefill_source"] = candidate_name or candidate_code
                st.success(f"已把 {candidate_name or candidate_code} 带入下方“个股两融”查询。")
        with action_cols[1]:
            st.caption(
                f"当前候选：`{candidate_code}` · {candidate_name or '-'}，切到“个股两融”标签后会自动带入查询框。"
            )


def render_margin_dashboard(ts_code: str, title: str, *, key_prefix: str = "margin_page") -> None:
    try:
        margin_engine = get_margin_engine_cached()
        latest_margin_date = get_margin_latest_date(margin_engine)
    except Exception as exc:
        st.warning(f"融资融券引擎不可用：{exc}")
        margin_engine = None
        latest_margin_date = None

    st.caption(f"个股两融数据范围：2022-01-01 起 · 最新日期：{_format_date(latest_margin_date)}")
    st.caption("读法建议：先看余额是否抬升，再看当日融资净买入与融券净卖出是否同步放大。")

    if margin_engine is None:
        st.info("融资融券数据库未就绪。")
        return

    try:
        margin_df = query_margin_detail_history(ts_code, engine=margin_engine)
    except Exception as exc:
        st.warning(f"加载融资融券历史失败：{exc}")
        margin_df = pd.DataFrame()
    try:
        price_df = query_stock_price_history(ts_code, engine=margin_engine)
    except Exception as exc:
        st.warning(f"加载股价历史失败：{exc}")
        price_df = pd.DataFrame()

    margin_plot = prepare_margin_display_frame(margin_df)
    margin_metrics = build_margin_latest_metrics(margin_df)
    margin_summary = build_margin_signal_summary(margin_df, lookback_days=20)
    divergence_summary = build_margin_price_divergence_summary(margin_df, price_df, lookback_days=20)

    if margin_plot is None or margin_plot.empty:
        st.info("暂无融资融券数据（可能尚未同步 ts_margin_detail，或该标的不在两融标的范围内）。")
    else:
        view_window = st.selectbox(
            "观察窗口",
            options=[60, 120, 240, 500, 9999],
            index=2,
            format_func=lambda value: "全部" if value >= 9999 else f"近 {value} 个交易日",
            key=f"{key_prefix}_window_{ts_code}",
        )
        plot_window = margin_plot if view_window >= 9999 else margin_plot.tail(int(view_window))

        metric_cols = st.columns(4)
        metric_cols[0].metric(
            "最新两融余额(亿)",
            _format_number(margin_metrics.get("rzrqye_yi"), digits=2),
            _format_signed_number(margin_metrics.get("rzrqye_delta_yi"), digits=2, unit=" 亿"),
        )
        metric_cols[1].metric("融资余额(亿)", _format_number(margin_metrics.get("rzye_yi"), digits=2))
        metric_cols[2].metric(
            "当日融资净买入(亿)",
            _format_signed_number(margin_metrics.get("rz_net_buy_yi"), digits=2),
        )
        metric_cols[3].metric(
            "当日融券净卖出(万股)",
            _format_signed_number(margin_metrics.get("rq_net_sell_wan"), digits=2),
        )

        alerts = margin_summary.get("alerts") or []
        if alerts:
            st.markdown("##### 两融异动提醒")
            for alert in alerts:
                level = str(alert.get("level") or "info")
                title_text = str(alert.get("title") or "两融提醒")
                message = str(alert.get("message") or "").strip()
                content = f"**{title_text}**：{message}" if message else title_text
                if level == "success":
                    st.success(content)
                elif level == "warning":
                    st.warning(content)
                else:
                    st.info(content)

        st.markdown("##### 两融信号摘要")
        summary_lines: list[str] = []
        rank_comment = margin_summary.get("rank_comment")
        if rank_comment:
            summary_lines.append(f"- {rank_comment}")
        financing_signal = margin_summary.get("financing_signal")
        if financing_signal:
            summary_lines.append(
                f"- {financing_signal}，近5日累计 `{_format_signed_number(margin_summary.get('rz_5d_sum_yi'), digits=2, unit=' 亿')}`"
            )
        short_signal = margin_summary.get("short_signal")
        if short_signal:
            summary_lines.append(
                f"- {short_signal}，近5日累计 `{_format_signed_number(margin_summary.get('rq_5d_sum_wan'), digits=2, unit=' 万股')}`"
            )
        rqye_ratio = margin_metrics.get("rqye_ratio_pct")
        if rqye_ratio is not None:
            summary_lines.append(f"- 当前融券余额占两融余额比重约 `{rqye_ratio:.2f}%`")
        if summary_lines:
            st.markdown("\n".join(summary_lines))

        price_alerts = divergence_summary.get("alerts") or []
        price_observations = divergence_summary.get("observations") or []
        if price_alerts or price_observations:
            st.markdown("##### 价格联动观察")
            for alert in price_alerts:
                level = str(alert.get("level") or "info")
                title_text = str(alert.get("title") or "价格联动")
                message = str(alert.get("message") or "").strip()
                content = f"**{title_text}**：{message}" if message else title_text
                if level == "success":
                    st.success(content)
                elif level == "warning":
                    st.warning(content)
                else:
                    st.info(content)
            if price_observations:
                st.markdown("\n".join(f"- {item}" for item in price_observations))

        balance_fig = make_subplots(specs=[[{"secondary_y": True}]])
        balance_fig.add_trace(
            go.Bar(
                x=plot_window["trade_date"],
                y=plot_window["rz_net_buy_yi"],
                name="融资净买入(亿)",
                marker_color="#3b82f6",
                opacity=0.6,
            ),
            secondary_y=False,
        )
        balance_fig.add_trace(
            go.Scatter(
                x=plot_window["trade_date"],
                y=plot_window["rzrqye_yi"],
                name="两融余额(亿)",
                mode="lines",
                line=dict(color="#f59e0b", width=2.2),
            ),
            secondary_y=True,
        )
        balance_fig.add_trace(
            go.Scatter(
                x=plot_window["trade_date"],
                y=plot_window["rqye_yi"],
                name="融券余额(亿)",
                mode="lines",
                line=dict(color="#ef4444", width=1.6, dash="dot"),
            ),
            secondary_y=True,
        )
        balance_fig.update_layout(
            title=f"{title} 融资净买入与两融余额",
            height=430,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.01),
            margin=dict(l=20, r=20, t=55, b=20),
        )
        balance_fig.update_yaxes(title_text="融资净买入(亿)", secondary_y=False)
        balance_fig.update_yaxes(title_text="余额(亿)", secondary_y=True)
        st.plotly_chart(balance_fig, use_container_width=True)

        flow_fig = px.bar(
            plot_window,
            x="trade_date",
            y=["rqmcl_wan", "rqchl_wan"],
            barmode="group",
            title="融券卖出 / 融券偿还（万股）",
        )
        flow_fig.update_layout(height=360, legend_title_text="")
        flow_fig.update_traces(selector=dict(name="rqmcl_wan"), name="融券卖出")
        flow_fig.update_traces(selector=dict(name="rqchl_wan"), name="融券偿还")
        st.plotly_chart(flow_fig, use_container_width=True)

        overlay_days = int(view_window if view_window < 9999 else 240)
        overlay_df = build_margin_price_overlay_frame(margin_df, price_df, lookback_days=overlay_days)
        if overlay_df is not None and not overlay_df.empty:
            compare_fig = go.Figure()
            compare_fig.add_trace(
                go.Scatter(
                    x=overlay_df["trade_date"],
                    y=overlay_df["price_index"],
                    mode="lines",
                    name="股价指数(起点=100)",
                    line=dict(color="#10b981", width=2),
                )
            )
            compare_fig.add_trace(
                go.Scatter(
                    x=overlay_df["trade_date"],
                    y=overlay_df["margin_index"],
                    mode="lines",
                    name="两融余额指数(起点=100)",
                    line=dict(color="#8b5cf6", width=2),
                )
            )
            compare_fig.update_layout(
                title="股价 vs 两融余额（标准化对比）",
                height=360,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.01),
                margin=dict(l=20, r=20, t=55, b=20),
            )
            st.plotly_chart(compare_fig, use_container_width=True)

        st.markdown("##### 最近 20 个交易日明细")
        recent_margin_df = plot_window.tail(20).copy().sort_values("trade_date", ascending=False)
        recent_margin_df["trade_date"] = recent_margin_df["trade_date"].dt.strftime("%Y-%m-%d")
        st.dataframe(
            recent_margin_df[
                [
                    "trade_date",
                    "rzye_yi",
                    "rqye_yi",
                    "rzrqye_yi",
                    "rzmre_yi",
                    "rzche_yi",
                    "rz_net_buy_yi",
                    "rqyl_wan",
                    "rqmcl_wan",
                    "rqchl_wan",
                    "rq_net_sell_wan",
                ]
            ].rename(
                columns={
                    "trade_date": "交易日",
                    "rzye_yi": "融资余额(亿)",
                    "rqye_yi": "融券余额(亿)",
                    "rzrqye_yi": "两融余额(亿)",
                    "rzmre_yi": "融资买入额(亿)",
                    "rzche_yi": "融资偿还额(亿)",
                    "rz_net_buy_yi": "融资净买入(亿)",
                    "rqyl_wan": "融券余量(万股)",
                    "rqmcl_wan": "融券卖出(万股)",
                    "rqchl_wan": "融券偿还(万股)",
                    "rq_net_sell_wan": "融券净卖出(万股)",
                }
            ),
            use_container_width=True,
            hide_index=True,
            height=420,
        )

    try:
        exchange_snapshot_df = query_margin_exchange_snapshot(engine=margin_engine)
    except Exception as exc:
        st.warning(f"加载市场两融汇总失败：{exc}")
        exchange_snapshot_df = pd.DataFrame()

    st.markdown("##### 当日市场两融汇总")
    _render_margin_exchange_snapshot(exchange_snapshot_df)


def render_margin_page() -> None:
    st.subheader("🏦 两融数据")
    st.caption("先看全市场两融温度，再下钻到个股的杠杆资金、融券压力与价格联动。")

    try:
        margin_engine = get_margin_engine_cached()
    except Exception as exc:
        st.warning(f"融资融券引擎不可用：{exc}")
        st.info("融资融券数据库未就绪。")
        return

    market_tab, stock_tab = st.tabs(["📊 市场总览", "🔎 个股两融"])

    with market_tab:
        _render_market_overview_section(margin_engine)

    with stock_tab:
        default_query = str(st.session_state.get("margin_page_prefill_query") or "").strip()
        prefill_source = str(st.session_state.get("margin_page_prefill_source") or "").strip()
        if default_query:
            source_text = prefill_source or default_query
            st.info(f"已从异动榜带入候选标的：{source_text}")
        query = st.text_input(
            "搜索股票",
            placeholder="输入代码、简称或拼音，例如 600519、贵州茅台、000001.SZ",
            value=default_query,
            key="margin_page_query",
        ).strip()
        if default_query:
            st.session_state["margin_page_prefill_query"] = ""
            st.session_state["margin_page_prefill_source"] = ""
        if not query:
            st.info("请输入关键字开始检索股票。")
            return

        try:
            candidate_df = load_security_search(query, "stock", limit=30)
        except Exception as exc:
            st.error(f"检索失败：{exc}")
            return

        if candidate_df is None or candidate_df.empty:
            st.warning("未检索到匹配股票，请尝试更换关键字。")
            return

        option_rows = candidate_df.copy()
        option_rows["label"] = option_rows.apply(
            lambda row: f"{row.get('name') or row.get('ts_code') or '-'}（{row.get('ts_code') or '-'}）",
            axis=1,
        )
        option_labels = option_rows["label"].tolist()
        selected_label = st.selectbox(
            "匹配结果",
            options=option_labels,
            index=0,
            key="margin_page_selected",
        )
        selected_row = option_rows.iloc[option_labels.index(selected_label)]
        ts_code = str(selected_row.get("ts_code") or "").strip().upper()
        stock_name = str(selected_row.get("name") or ts_code or "").strip()
        if not ts_code:
            st.warning("当前选中的股票代码为空。")
            return

        try:
            profile_df = load_security_profile(ts_code, "stock")
        except Exception as exc:
            st.error(f"加载股票画像失败：{exc}")
            return
        profile = profile_df.iloc[0].to_dict() if profile_df is not None and not profile_df.empty else {}

        title = stock_name or str(profile.get("name") or ts_code)
        st.markdown(f"### {title}")
        subtitle_parts = [ts_code]
        if profile.get("industry"):
            subtitle_parts.append(str(profile.get("industry")))
        if profile.get("market"):
            subtitle_parts.append(str(profile.get("market")))
        st.caption(" | ".join(subtitle_parts))

        metric_cols = st.columns(5)
        metric_cols[0].metric("最新交易日", _format_date(profile.get("latest_trade_date") or profile.get("trade_date")))
        metric_cols[1].metric("收盘价(元)", _format_number(profile.get("close"), digits=2))
        metric_cols[2].metric("涨跌幅(%)", _format_number(profile.get("pct_chg"), digits=2))
        metric_cols[3].metric("PE_TTM", _format_number(profile.get("pe_ttm"), digits=2))
        metric_cols[4].metric("总市值(亿元)", _format_number(profile.get("total_mv"), digits=2, scale=10000.0))

        render_margin_dashboard(ts_code, title, key_prefix="money_margin_page")
