import logging
from datetime import datetime, timedelta
from urllib.parse import quote

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from src.margin_fetcher import (
    _get_engine_cached as get_margin_engine_cached,
    build_margin_signal_summary,
    build_margin_price_divergence_summary,
    build_margin_latest_metrics,
    build_margin_price_overlay_frame,
    get_margin_latest_date,
    prepare_margin_display_frame,
    query_margin_detail_history,
    query_stock_price_history,
    query_margin_exchange_snapshot,
)
from src.lhb_monitor import get_lhb_sync_meta, load_lhb_data_from_db
from src.moneyflow_fetcher import (
    _get_engine_cached as get_moneyflow_engine_cached,
    get_moneyflow_latest_date,
    query_moneyflow_stock_history,
)
from src.security_data_cache import (
    load_stock_announcements,
    load_stock_event_stream,
    load_fund_hot_stock_periods,
    load_security_profile,
    load_security_search,
    load_stock_news_and_reports,
)
from src.user_watchlist_store import add_watchlist_item, is_in_watchlist, normalize_username

logger = logging.getLogger(__name__)


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


def _resolve_logged_in_username() -> str:
    return normalize_username(st.session_state.get("logged_in_username", ""))


IMPORTANT_EVENT_KEYWORDS = (
    "涨停",
    "跌停",
    "回购",
    "增持",
    "减持",
    "并购",
    "重组",
    "签署",
    "合同",
    "订单",
    "中标",
    "诉讼",
    "仲裁",
    "分红",
    "派息",
    "质押",
    "解除质押",
    "停牌",
    "复牌",
    "解禁",
    "问询",
    "处罚",
    "立案",
    "业绩预告",
    "快报",
    "亏损",
    "扭亏",
    "激励",
    "归属",
    "辞职",
    "变更",
)

IMPORTANT_NOTICE_TYPES = {
    "重大事项",
    "财务报告",
    "融资公告",
    "风险提示",
    "资产重组",
    "信息变更",
    "持股变动",
}


def _build_recent_summary(event_df: pd.DataFrame, days: int = 7) -> tuple[pd.DataFrame, dict[str, int]]:
    if event_df is None or event_df.empty or "日期" not in event_df.columns:
        return pd.DataFrame(), {}

    work = event_df.copy()
    work["排序时间"] = pd.to_datetime(work["日期"], errors="coerce", format="mixed")
    cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=max(1, int(days)) - 1)
    recent = work[work["排序时间"].notna() & (work["排序时间"] >= cutoff)].copy()
    counts = recent["类型"].value_counts().to_dict() if "类型" in recent.columns else {}
    return recent, counts


def _pick_important_events(event_df: pd.DataFrame, limit: int = 8) -> pd.DataFrame:
    if event_df is None or event_df.empty:
        return pd.DataFrame()

    work = event_df.copy()
    work["排序时间"] = pd.to_datetime(work["日期"], errors="coerce", format="mixed")
    work["标题"] = work.get("标题", "").astype("string").fillna("").astype(str)
    work["子类型"] = work.get("子类型", "").astype("string").fillna("").astype(str)
    work["类型"] = work.get("类型", "").astype("string").fillna("").astype(str)
    work["机构"] = work.get("机构", "").astype("string").fillna("").astype(str)
    work["评级"] = work.get("评级", "").astype("string").fillna("").astype(str)

    def _score(row) -> int:
        score = 0
        event_type = str(row.get("类型") or "")
        subtype = str(row.get("子类型") or "")
        title = str(row.get("标题") or "")
        rating = str(row.get("评级") or "")
        text = f"{subtype} {title}"
        if event_type == "公告":
            score += 3
        elif event_type == "研报":
            score += 2
        else:
            score += 1
        if subtype in IMPORTANT_NOTICE_TYPES:
            score += 4
        if any(keyword in text for keyword in IMPORTANT_EVENT_KEYWORDS):
            score += 4
        if rating and rating not in {"", "-", "中性"}:
            score += 1
        if "首次" in title or "最新" in title:
            score += 1
        return score

    work["重要度"] = work.apply(_score, axis=1)
    work = work.sort_values(["重要度", "排序时间"], ascending=[False, False], na_position="last")
    work = work[work["重要度"] >= 4]
    return work.head(limit).drop(columns=["重要度", "排序时间"], errors="ignore")


def _render_recent_summary(event_df: pd.DataFrame) -> None:
    recent_df, counts = _build_recent_summary(event_df, days=7)
    cols = st.columns(4)
    cols[0].metric("近7天事件", f"{len(recent_df):,}")
    cols[1].metric("公告", f"{counts.get('公告', 0):,}")
    cols[2].metric("新闻", f"{counts.get('新闻', 0):,}")
    cols[3].metric("研报", f"{counts.get('研报', 0):,}")

    if recent_df.empty:
        st.info("近 7 天暂无事件。")
        return

    recent_titles = []
    for _, row in recent_df.head(5).iterrows():
        label = f"{row.get('日期', '')[:10]} [{row.get('类型', '-')}] {row.get('标题', '-')}"
        recent_titles.append(f"- {label}")
    st.markdown("##### 最近 7 天摘要")
    st.markdown("\n".join(recent_titles))


def _render_important_events(event_df: pd.DataFrame) -> None:
    important_df = _pick_important_events(event_df, limit=8)
    st.markdown("##### 重要事件")
    if important_df.empty:
        st.info("当前未识别到需要特别高亮的事件。")
        return

    for _, row in important_df.iterrows():
        event_type = str(row.get("类型") or "-")
        subtype = str(row.get("子类型") or "")
        title = str(row.get("标题") or "-")
        event_date = str(row.get("日期") or "")[:10]
        source = str(row.get("来源") or "")
        institution = str(row.get("机构") or "")
        rating = str(row.get("评级") or "")
        link = str(row.get("链接") or "").strip()
        extra_parts = [part for part in [subtype, source, institution, rating] if part]
        detail = " · ".join(extra_parts)
        if link:
            st.markdown(f"- `{event_date}` [{event_type}] [{title}]({link})")
        else:
            st.markdown(f"- `{event_date}` [{event_type}] {title}")
        if detail:
            st.caption(detail)


def _render_event_table(df: pd.DataFrame, *, height: int = 520) -> None:
    if df is None or df.empty:
        st.info("暂无可展示数据。")
        return

    display_df = df.copy()
    if "链接" in display_df.columns:
        display_df["链接"] = display_df["链接"].astype("string").fillna("").astype(str)
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=height,
        column_config={
            "链接": st.column_config.LinkColumn("链接", display_text="打开"),
        } if "链接" in display_df.columns else None,
    )


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


def render_stock_object_page() -> None:
    st.subheader("🧩 股票对象页")
    st.caption("围绕单只股票聚合：概览、事件、资金面、龙虎榜、公募基金持仓。")

    default_query = str(st.session_state.get("stock_object_prefill_query") or "").strip()
    query = st.text_input(
        "搜索股票",
        placeholder="输入代码、简称或拼音，例如 600519、贵州茅台、000001.SZ",
        value=default_query,
        key="stock_object_query",
    ).strip()
    if default_query:
        st.session_state["stock_object_prefill_query"] = ""
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
    default_label = option_labels[0] if option_labels else ""
    selected_label = st.selectbox(
        "匹配结果",
        options=option_labels,
        index=0,
        key="stock_object_selected",
    )
    if selected_label not in option_labels:
        selected_label = default_label
    selected_idx = option_labels.index(selected_label)
    selected_row = option_rows.iloc[selected_idx]
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

    username = _resolve_logged_in_username()
    watchlist_cols = st.columns([1.2, 2.8])
    already_in_watchlist = False
    if username:
        try:
            already_in_watchlist = is_in_watchlist(username, ts_code, "stock")
        except Exception:
            already_in_watchlist = False
        btn_label = "✅ 已在自选" if already_in_watchlist else "⭐ 加入自选"
        if watchlist_cols[0].button(
            btn_label,
            key=f"btn_stock_object_add_watchlist_{ts_code}",
            disabled=already_in_watchlist,
        ):
            try:
                add_watchlist_item(username, ts_code, security_name=title, security_type="stock")
                st.success(f"已加入 {username} 的自选：{title}")
                st.rerun()
            except Exception as exc:
                st.error(f"加入自选失败：{exc}")
        watchlist_cols[1].caption(f"当前登录用户：{username}")
    else:
        watchlist_cols[0].button(
            "⭐ 加入自选",
            key=f"btn_stock_object_add_watchlist_disabled_{ts_code}",
            disabled=True,
        )
        watchlist_cols[1].info("先在侧边栏登录用户名，才能把股票加入个人自选。")

    metric_cols = st.columns(6)
    metric_cols[0].metric("最新交易日", _format_date(profile.get("latest_trade_date") or profile.get("trade_date")))
    metric_cols[1].metric("收盘价(元)", _format_number(profile.get("close"), digits=2))
    metric_cols[2].metric("涨跌幅(%)", _format_number(profile.get("pct_chg"), digits=2))
    metric_cols[3].metric("PE_TTM", _format_number(profile.get("pe_ttm"), digits=2))
    metric_cols[4].metric("PB", _format_number(profile.get("pb"), digits=2))
    metric_cols[5].metric("总市值(亿元)", _format_number(profile.get("total_mv"), digits=2, scale=10000.0))

    tab_overview, tab_events, tab_notice, tab_news, tab_reports, tab_capital, tab_lhb, tab_fund = st.tabs(
        ["📌 概览", "🧭 事件流", "📢 公告", "📰 新闻", "📄 研报", "💰 资金面", "🐉 龙虎榜", "🏦 公募基金持仓"]
    )

    with tab_overview:
        left, right = st.columns([1.05, 0.95])
        with left:
            st.markdown("##### 🧾 基本信息")
            st.dataframe(
                pd.DataFrame(
                    [
                        {"字段": "上市日期", "值": _format_date(profile.get("list_date"))},
                        {"字段": "所属行业", "值": profile.get("industry") or "-"},
                        {"字段": "市场板块", "值": profile.get("market") or "-"},
                        {"字段": "上市状态", "值": profile.get("list_status") or "-"},
                        {"字段": "曾经ST", "值": "是" if bool(profile.get("has_ever_st")) else "否"},
                    ]
                ),
                use_container_width=True,
                hide_index=True,
            )
        with right:
            st.markdown("##### 🧠 财务快照")
            st.dataframe(
                pd.DataFrame(
                    [
                        {"字段": "ROE(%)", "值": _format_number(profile.get("roe"), digits=2)},
                        {"字段": "ROA(%)", "值": _format_number(profile.get("roa"), digits=2)},
                        {"字段": "毛利率(%)", "值": _format_number(profile.get("gross_margin"), digits=2)},
                        {"字段": "净利润(亿元)", "值": _format_number(profile.get("n_income"), digits=2, scale=100000000.0)},
                        {"字段": "经营现金流(亿元)", "值": _format_number(profile.get("n_cashflow_act"), digits=2, scale=100000000.0)},
                        {"字段": "最近财报期", "值": _format_date(profile.get("fina_end_date"))},
                    ]
                ),
                use_container_width=True,
                hide_index=True,
            )

        st.markdown("##### 📜 主营与产品")
        st.info(f"**主要业务**：{profile.get('main_business') or '-'}")
        st.info(f"**产品及业务范围**：{profile.get('business_scope') or '-'}")

    supplemental = {}
    with tab_events:
        event_days = st.selectbox(
            "事件流范围",
            options=[30, 90, 180, 365],
            index=2,
            format_func=lambda value: f"近 {value} 天",
            key=f"stock_object_event_days_{ts_code}",
        )
        try:
            event_df = load_stock_event_stream(
                ts_code,
                stock_name=title,
                industry=str(profile.get("industry") or ""),
                days=int(event_days),
            )
        except Exception as exc:
            st.warning(f"加载事件流失败：{exc}")
            event_df = pd.DataFrame()

        if event_df is None or event_df.empty:
            st.info("当前时间范围内暂无事件流数据。")
        else:
            counts = event_df["类型"].value_counts().to_dict() if "类型" in event_df.columns else {}
            st.caption(
                f"事件总数：{len(event_df):,} · "
                f"公告 {counts.get('公告', 0):,} · "
                f"新闻 {counts.get('新闻', 0):,} · "
                f"研报 {counts.get('研报', 0):,}"
            )
            _render_recent_summary(event_df)
            _render_important_events(event_df)
            st.markdown("##### 全部事件")
            show_cols = [c for c in ["日期", "类型", "子类型", "标题", "来源", "机构", "评级", "链接"] if c in event_df.columns]
            _render_event_table(event_df[show_cols], height=560)

    with tab_notice:
        st.caption("数据来源：AkShare（东方财富公告大全）。")
        notice_type = st.selectbox(
            "公告类型",
            ["全部", "重大事项", "财务报告", "融资公告", "风险提示", "资产重组", "信息变更", "持股变动"],
            index=0,
            key=f"stock_object_notice_type_{ts_code}",
        )
        notice_days = st.selectbox(
            "时间范围",
            options=[90, 180, 365],
            index=1,
            format_func=lambda value: f"近 {value} 天",
            key=f"stock_object_notice_days_{ts_code}",
        )
        try:
            notice_df = load_stock_announcements(ts_code, notice_type=notice_type, days=int(notice_days))
        except Exception as exc:
            st.warning(f"加载公告失败：{exc}")
            notice_df = pd.DataFrame()

        if notice_df is None or notice_df.empty:
            st.info("当前条件下暂无公告数据。")
        else:
            st.caption(f"公告条数：{len(notice_df):,}")
            show_df = notice_df.rename(columns={"网址": "链接"})
            show_cols = [c for c in ["公告日期", "公告类型", "公告标题", "链接"] if c in show_df.columns]
            _render_event_table(show_df[show_cols], height=520)

    with tab_news:
        st.caption("数据来源：AkShare（东方财富）。")
        try:
            supplemental = load_stock_news_and_reports(ts_code, stock_name=title, industry=str(profile.get("industry") or ""))
        except Exception as exc:
            st.warning(f"加载新闻失败：{exc}")
            supplemental = {}

        news_block = supplemental.get("news") if isinstance(supplemental, dict) else None
        if not isinstance(news_block, dict):
            st.info("暂无新闻数据。")
        else:
            status = str(news_block.get("status") or "")
            if status in {"failed"}:
                st.warning(f"新闻抓取失败：{news_block.get('error') or '-'}")
            items = news_block.get("items") or []
            if not items:
                st.info("暂无新闻数据。")
            else:
                df = pd.DataFrame(items).rename(columns={"新闻链接": "链接"})
                show_cols = [c for c in ["发布时间", "文章来源", "新闻标题", "链接", "新闻内容"] if c in df.columns]
                _render_event_table(df[show_cols], height=520)

    with tab_reports:
        st.caption("数据来源：AkShare（东方财富）。")
        if not supplemental:
            try:
                supplemental = load_stock_news_and_reports(ts_code, stock_name=title, industry=str(profile.get("industry") or ""))
            except Exception as exc:
                st.warning(f"加载研报失败：{exc}")
                supplemental = {}

        report_block = supplemental.get("research_reports") if isinstance(supplemental, dict) else None
        if not isinstance(report_block, dict):
            st.info("暂无研报数据。")
        else:
            status = str(report_block.get("status") or "")
            if status in {"failed"}:
                st.warning(f"研报抓取失败：{report_block.get('error') or '-'}")
            items = report_block.get("items") or []
            if not items:
                st.info("暂无研报数据。")
            else:
                df = pd.DataFrame(items).rename(columns={"报告PDF链接": "链接"})
                show_cols = [c for c in ["日期", "机构", "东财评级", "报告名称", "链接"] if c in df.columns]
                _render_event_table(df[show_cols], height=520)

    with tab_capital:
        capital_tab_moneyflow, capital_tab_margin = st.tabs(["💹 资金流", "🏦 融资融券"])

        with capital_tab_moneyflow:
            try:
                engine = get_moneyflow_engine_cached()
                latest_mf = get_moneyflow_latest_date(engine)
            except Exception as exc:
                st.warning(f"资金流引擎不可用：{exc}")
                engine = None
                latest_mf = None

            st.caption(f"资金流最新日期：{_format_date(latest_mf)}")
            try:
                mf_df = query_moneyflow_stock_history(ts_code, engine=engine) if engine is not None else pd.DataFrame()
            except Exception as exc:
                st.warning(f"加载资金流历史失败：{exc}")
                mf_df = pd.DataFrame()

            if mf_df is None or mf_df.empty:
                st.info("暂无资金流数据（可能尚未同步 ts_moneyflow）。")
            else:
                mf_plot = mf_df.copy()
                mf_plot["trade_date"] = pd.to_datetime(mf_plot["trade_date"], errors="coerce")
                mf_plot = mf_plot.dropna(subset=["trade_date"])
                mf_plot["net_mf_yi"] = pd.to_numeric(mf_plot["net_mf_amount"], errors="coerce") / 10000.0
                mf_plot = mf_plot.dropna(subset=["net_mf_yi"]).sort_values("trade_date")
                if mf_plot.empty:
                    st.info("资金流数据字段缺失或为空。")
                else:
                    show_tail = mf_plot.tail(120)
                    fig = px.bar(
                        show_tail,
                        x="trade_date",
                        y="net_mf_yi",
                        title=f"{title} 资金净流入（万元）",
                    )
                    fig.update_layout(height=420, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)

                    last_value = show_tail["net_mf_yi"].iloc[-1]
                    rolling5 = show_tail["net_mf_yi"].tail(5).sum()
                    metric_cols2 = st.columns(3)
                    metric_cols2[0].metric("最新净流入(万元)", f"{last_value:,.0f}")
                    metric_cols2[1].metric("近5日合计(万元)", f"{rolling5:,.0f}")
                    metric_cols2[2].metric("样本天数", f"{len(show_tail):,}")

        with capital_tab_margin:
            try:
                margin_engine = get_margin_engine_cached()
                latest_margin_date = get_margin_latest_date(margin_engine)
            except Exception as exc:
                st.warning(f"融资融券引擎不可用：{exc}")
                margin_engine = None
                latest_margin_date = None

            st.caption(
                f"个股两融数据范围：2022-01-01 起 · 最新日期：{_format_date(latest_margin_date)}"
            )
            st.caption("读法建议：先看余额是否抬升，再看当日融资净买入与融券净卖出是否同步放大。")

            if margin_engine is None:
                st.info("融资融券数据库未就绪。")
            else:
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
                        key=f"stock_object_margin_window_{ts_code}",
                    )
                    plot_window = margin_plot if view_window >= 9999 else margin_plot.tail(int(view_window))

                    metric_cols3 = st.columns(4)
                    metric_cols3[0].metric(
                        "最新两融余额(亿)",
                        _format_number(margin_metrics.get("rzrqye_yi"), digits=2),
                        _format_signed_number(margin_metrics.get("rzrqye_delta_yi"), digits=2, unit=" 亿"),
                    )
                    metric_cols3[1].metric(
                        "融资余额(亿)",
                        _format_number(margin_metrics.get("rzye_yi"), digits=2),
                    )
                    metric_cols3[2].metric(
                        "当日融资净买入(亿)",
                        _format_signed_number(margin_metrics.get("rz_net_buy_yi"), digits=2),
                    )
                    metric_cols3[3].metric(
                        "当日融券净卖出(万股)",
                        _format_signed_number(margin_metrics.get("rq_net_sell_wan"), digits=2),
                    )

                    st.markdown("##### 两融信号摘要")
                    alerts = margin_summary.get("alerts") or []
                    if alerts:
                        st.markdown("##### 两融异动提醒")
                        for alert in alerts:
                            level = str(alert.get("level") or "info")
                            title = str(alert.get("title") or "两融提醒")
                            message = str(alert.get("message") or "").strip()
                            content = f"**{title}**：{message}" if message else title
                            if level == "success":
                                st.success(content)
                            elif level == "warning":
                                st.warning(content)
                            else:
                                st.info(content)

                    summary_lines = []
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
                            title = str(alert.get("title") or "价格联动")
                            message = str(alert.get("message") or "").strip()
                            content = f"**{title}**：{message}" if message else title
                            if level == "success":
                                st.success(content)
                            elif level == "warning":
                                st.warning(content)
                            else:
                                st.info(content)
                        if price_observations:
                            st.markdown("\n".join(f"- {item}" for item in price_observations))

                    subfig = make_subplots(specs=[[{"secondary_y": True}]])
                    subfig.add_trace(
                        go.Bar(
                            x=plot_window["trade_date"],
                            y=plot_window["rz_net_buy_yi"],
                            name="融资净买入(亿)",
                            marker_color="#3b82f6",
                            opacity=0.6,
                        ),
                        secondary_y=False,
                    )
                    subfig.add_trace(
                        go.Scatter(
                            x=plot_window["trade_date"],
                            y=plot_window["rzrqye_yi"],
                            name="两融余额(亿)",
                            mode="lines",
                            line=dict(color="#f59e0b", width=2.2),
                        ),
                        secondary_y=True,
                    )
                    subfig.add_trace(
                        go.Scatter(
                            x=plot_window["trade_date"],
                            y=plot_window["rqye_yi"],
                            name="融券余额(亿)",
                            mode="lines",
                            line=dict(color="#ef4444", width=1.6, dash="dot"),
                        ),
                        secondary_y=True,
                    )
                    subfig.update_layout(
                        title=f"{title} 融资净买入与两融余额",
                        height=430,
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.01),
                        margin=dict(l=20, r=20, t=55, b=20),
                    )
                    subfig.update_yaxes(title_text="融资净买入(亿)", secondary_y=False)
                    subfig.update_yaxes(title_text="余额(亿)", secondary_y=True)
                    st.plotly_chart(subfig, use_container_width=True)

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

                    overlay_df = build_margin_price_overlay_frame(margin_df, price_df, lookback_days=int(view_window if view_window < 9999 else 240))
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

    with tab_lhb:
        today = datetime.now().date()
        start_dt = today - timedelta(days=180)
        st.caption("数据来源：数据库缓存（若未同步龙虎榜数据，该页可能为空）。")

        try:
            lhb_meta = get_lhb_sync_meta()
        except Exception:
            lhb_meta = {}
        if lhb_meta:
            st.caption(
                "龙虎榜最新交易日："
                f"{_format_date(lhb_meta.get('latest_trade_date'))} · "
                f"总记录：{int(lhb_meta.get('total_rows') or 0):,}"
            )

        try:
            lhb_pack = load_lhb_data_from_db(
                start_date=start_dt.strftime("%Y%m%d"),
                end_date=today.strftime("%Y%m%d"),
                ts_code=ts_code,
                include_inst=True,
                limit=4000,
            )
        except Exception as exc:
            st.warning(f"加载龙虎榜失败：{exc}")
            lhb_pack = {}

        top_list = lhb_pack.get("top_list", pd.DataFrame())
        top_inst = lhb_pack.get("top_inst", pd.DataFrame())
        trade_dates = lhb_pack.get("trade_dates", []) or []
        mcols = st.columns(3)
        mcols[0].metric("交易日", f"{len(trade_dates):,}")
        mcols[1].metric("榜单明细", f"{len(top_list):,}")
        mcols[2].metric("机构明细", f"{len(top_inst):,}")

        if top_list is None or top_list.empty:
            st.info("近 180 天没有龙虎榜记录。")
        else:
            show_cols = [c for c in ["trade_date", "name", "reason", "l_amount_yi", "net_amount_yi", "pct_change"] if c in top_list.columns]
            st.dataframe(top_list[show_cols].head(200), use_container_width=True, hide_index=True, height=420)

    with tab_fund:
        try:
            from src.fund_hot_stocks import get_engine as get_fund_hot_engine
            from src.fund_hot_stocks import query_stock_fund_holding_detail, query_stock_holding_trend

            engine = get_fund_hot_engine()
        except Exception as exc:
            st.info(f"基金持仓引擎不可用：{exc}")
            return

        periods = load_fund_hot_stock_periods()
        period = st.selectbox(
            "报告期",
            options=periods if periods else ["自动(最新)"],
            index=0,
            key=f"stock_object_fund_period_{ts_code}",
        )
        if period == "自动(最新)":
            period = None

        try:
            holding_df = query_stock_fund_holding_detail(ts_code, period=period, top_n=200, engine=engine)
        except Exception as exc:
            st.warning(f"加载基金持仓明细失败：{exc}")
            holding_df = pd.DataFrame()

        if holding_df is None or holding_df.empty:
            st.info("该报告期暂无基金持仓披露数据。")
        else:
            st.caption(f"基金持仓记录：{len(holding_df):,}（季度披露，非实时）")
            display_df = holding_df.copy()
            if "fund_code" in display_df.columns:
                display_df["基金详情"] = display_df["fund_code"].astype("string").fillna("").astype(str).map(
                    lambda code: (
                        f"?security_query={quote(code)}"
                        f"&security_type=fund"
                        f"&open_tab=fund_object"
                        f"&jump_nonce=stock-object-fund-{quote(code)}"
                    )
                    if code and code != "-"
                    else ""
                )
            show_cols = [
                c
                for c in [
                    "基金详情",
                    "fund_code",
                    "fund_name",
                    "management",
                    "mkv",
                    "delta_mkv",
                    "stk_mkv_ratio",
                    "stk_float_ratio",
                    "holding_change_flag",
                ]
                if c in display_df.columns
            ]
            st.dataframe(
                display_df[show_cols],
                use_container_width=True,
                hide_index=True,
                height=420,
                column_config={
                    "基金详情": st.column_config.LinkColumn("基金详情", display_text="查看基金"),
                },
            )

        try:
            trend_df = query_stock_holding_trend(ts_code, periods=8, engine=engine)
        except Exception as exc:
            logger.warning(f"query_stock_holding_trend failed: {exc}", exc_info=True)
            trend_df = pd.DataFrame()

        if trend_df is not None and not trend_df.empty and "end_date" in trend_df.columns:
            plot_df = trend_df.copy()
            plot_df["end_date"] = pd.to_datetime(plot_df["end_date"], errors="coerce")
            plot_df = plot_df.dropna(subset=["end_date"]).sort_values("end_date")
            if "holding_fund_count" in plot_df.columns:
                fig = px.line(
                    plot_df,
                    x="end_date",
                    y="holding_fund_count",
                    markers=True,
                    title="持有基金数量趋势（季度）",
                )
                fig.update_layout(height=360)
                st.plotly_chart(fig, use_container_width=True)

