import logging
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from src.lhb_monitor import get_lhb_sync_meta, load_lhb_data_from_db
from src.moneyflow_fetcher import (
    _get_engine_cached as get_moneyflow_engine_cached,
    get_moneyflow_latest_date,
    query_moneyflow_stock_history,
)
from src.security_data_cache import (
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


def _format_date(value) -> str:
    raw = pd.to_datetime(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(raw):
        return "-"
    return pd.Timestamp(raw).strftime("%Y-%m-%d")


def _resolve_logged_in_username() -> str:
    return normalize_username(st.session_state.get("logged_in_username", ""))


def render_stock_object_page() -> None:
    st.subheader("🧩 股票对象页")
    st.caption("围绕单只股票聚合：概览、资金流、龙虎榜、公募基金持仓。")

    query = st.text_input(
        "搜索股票",
        placeholder="输入代码、简称或拼音，例如 600519、贵州茅台、000001.SZ",
        key="stock_object_query",
    ).strip()
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

    tab_overview, tab_news, tab_reports, tab_moneyflow, tab_lhb, tab_fund = st.tabs(
        ["📌 概览", "📰 新闻", "📄 研报", "💹 资金流", "🐉 龙虎榜", "🏦 公募基金持仓"]
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
                df = pd.DataFrame(items)
                st.dataframe(df, use_container_width=True, hide_index=True, height=520)

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
                df = pd.DataFrame(items)
                st.dataframe(df, use_container_width=True, hide_index=True, height=520)

    with tab_moneyflow:
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
            show_cols = [
                c
                for c in [
                    "fund_code",
                    "fund_name",
                    "management",
                    "mkv",
                    "delta_mkv",
                    "stk_mkv_ratio",
                    "stk_float_ratio",
                    "holding_change_flag",
                ]
                if c in holding_df.columns
            ]
            st.dataframe(holding_df[show_cols], use_container_width=True, hide_index=True, height=420)

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

