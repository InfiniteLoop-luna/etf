from __future__ import annotations

from html import escape

import pandas as pd
import plotly.express as px
import streamlit as st

from src.apple_theme import build_apple_plotly_template
from src.nasdaq_sector_data import PERIOD_TO_DAYS, load_or_refresh_snapshot
from src.theme_registry import get_active_theme_id


NASDAQ_SECTOR_PAGE_CSS = """
<style>
.ws-us-market-strip{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:.65rem;margin:.55rem 0 .85rem}
.ws-us-market-card{padding:.8rem .9rem;background:var(--ws-bg-surface);border:1px solid var(--ws-border-soft);border-radius:var(--ws-radius-lg);box-shadow:var(--ws-shadow)}
.ws-us-market-card span{display:block;color:var(--ws-text-muted);font-size:.88rem}.ws-us-market-card strong{display:block;margin-top:.2rem;color:var(--ws-text-main);font-size:1.25rem}.ws-us-market-card small{color:var(--ws-text-soft)}
.ws-us-sector-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:.65rem;margin:.4rem 0 1rem}.ws-us-sector-tile{padding:.8rem;border:1px solid var(--ws-border-soft);border-radius:var(--ws-radius-lg);background:var(--tile-bg);box-shadow:var(--ws-shadow)}
.ws-us-sector-tile h4{margin:0!important;font-size:1rem!important}.ws-us-sector-tile strong{display:block;margin:.35rem 0;color:var(--tile-color);font-size:1.35rem}.ws-us-sector-tile span,.ws-us-sector-tile small{display:block;color:var(--ws-text-muted)}
.ws-us-theme-marker{display:none}.st-key-nasdaq-sector-toolbar{margin:.55rem 0 .75rem;padding:.7rem .8rem;border-radius:var(--ws-radius-lg)}
/* Apple: restrained, flat, precise. */
.stApp:has(.ws-us-theme-marker--apple) .st-key-nasdaq-sector-toolbar{background:#fff;border:1px solid #D2D2D7;box-shadow:none}
.stApp:has(.ws-us-theme-marker--apple) .ws-us-market-card{background:#fff;border-color:#D2D2D7;border-radius:11px;box-shadow:none}
.stApp:has(.ws-us-theme-marker--apple) .ws-us-market-card:first-child{border-top:3px solid #0066CC}
.stApp:has(.ws-us-theme-marker--apple) .ws-us-sector-tile{border-color:#D2D2D7;border-radius:11px;box-shadow:none}
.stApp:has(.ws-us-theme-marker--apple) .ws-us-sector-tile:hover{border-color:#0066CC}
/* Doraemon: airy blue canvas, rounded floating cards, red nose/yellow bell accents. */
.stApp:has(.ws-us-theme-marker--doraemon) .st-key-nasdaq-sector-toolbar{background:#F0F8FF;border:1px solid #CFE7F6;box-shadow:0 8px 22px rgba(42,136,192,.08)}
.stApp:has(.ws-us-theme-marker--doraemon) .ws-us-market-card{position:relative;background:#fff;border-color:#CFE7F6;border-radius:20px;box-shadow:0 8px 24px rgba(42,136,192,.09)}
.stApp:has(.ws-us-theme-marker--doraemon) .ws-us-market-card:first-child{border-top:5px solid #11A9EE}
.stApp:has(.ws-us-theme-marker--doraemon) .ws-us-market-card:nth-child(3)::after{position:absolute;top:12px;right:14px;width:10px;height:10px;background:#F46968;border-radius:50%;content:""}
.stApp:has(.ws-us-theme-marker--doraemon) .ws-us-market-card:nth-child(4)::after{position:absolute;top:11px;right:13px;width:12px;height:12px;background:#FCCD3D;border:1px solid #D4A91E;border-radius:50%;content:""}
.stApp:has(.ws-us-theme-marker--doraemon) .ws-us-sector-tile{border-color:#CFE7F6;border-radius:20px;box-shadow:0 8px 22px rgba(42,136,192,.08);transition:transform .15s ease,box-shadow .15s ease}
.stApp:has(.ws-us-theme-marker--doraemon) .ws-us-sector-tile:hover{transform:translateY(-2px);box-shadow:0 12px 28px rgba(42,136,192,.14)}
@media(max-width:1100px){.ws-us-sector-grid{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media(max-width:800px){.ws-us-market-strip,.ws-us-sector-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:560px){.ws-us-market-strip,.ws-us-sector-grid{grid-template-columns:1fr}}
</style>
"""


def _pct(value) -> str:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return "-" if pd.isna(number) else f"{float(number):+.2f}%"


def _tile_style(value: float) -> tuple[str, str]:
    strength = min(abs(float(value or 0)) / 4.0, 1.0)
    if value > 0:
        return f"rgba(244,105,104,{0.08 + strength * 0.22:.3f})", "#D94C51"
    if value < 0:
        return f"rgba(47,163,107,{0.08 + strength * 0.20:.3f})", "#248A3D"
    return "var(--ws-surface-soft)", "var(--ws-text-muted)"


def _render_market_strip(snapshot: dict, sector_df: pd.DataFrame) -> None:
    benchmarks = snapshot.get("benchmark_returns") or {}
    strongest = sector_df.iloc[0] if not sector_df.empty else {}
    weakest = sector_df.iloc[-1] if not sector_df.empty else {}
    cards = [
        ("纳斯达克100（QQQ）", _pct(benchmarks.get("QQQ")), f"周期：{snapshot.get('period') or '-'}"),
        ("上涨板块", f"{int((sector_df['return_pct'] > 0).sum())} / {len(sector_df)}" if not sector_df.empty else "-", "红涨绿跌"),
        ("最强板块", str(strongest.get("sector") or "-"), _pct(strongest.get("return_pct"))),
        ("领涨龙头", str(strongest.get("leader_symbol") or "-"), f"{strongest.get('leader_name') or '-'} {_pct(strongest.get('leader_return_pct'))}"),
    ]
    html = "".join(
        f'<div class="ws-us-market-card"><span>{escape(label)}</span><strong>{escape(value)}</strong><small>{escape(detail)}</small></div>'
        for label, value, detail in cards
    )
    st.html(f'<div class="ws-us-market-strip">{html}</div>')


def _render_sector_heatmap(sector_df: pd.DataFrame) -> None:
    tiles = []
    for row in sector_df.to_dict("records"):
        bg, color = _tile_style(float(row.get("return_pct") or 0))
        tiles.append(
            f"""
            <div class="ws-us-sector-tile" style="--tile-bg:{bg};--tile-color:{color}">
                <h4>{escape(str(row.get('sector') or '-'))}</h4>
                <strong>{_pct(row.get('return_pct'))}</strong>
                <span>{int(row.get('up_count') or 0)}涨 / {int(row.get('down_count') or 0)}跌 · 相对QQQ {_pct(row.get('relative_qqq_pct'))}</span>
                <small>领涨：{escape(str(row.get('leader_symbol') or '-'))} {_pct(row.get('leader_return_pct'))}</small>
            </div>
            """
        )
    st.html(f'<div class="ws-us-sector-grid">{"".join(tiles)}</div>')


def _stock_view(stock_df: pd.DataFrame, sector: str, period: str) -> pd.DataFrame:
    return_col = f"return_{period}"
    view = stock_df[stock_df["sector"] == sector].copy()
    if view.empty:
        return view
    view["龙头分"] = (
        view["core_weight"].rank(pct=True) * 35
        + view[return_col].rank(pct=True) * 40
        + view["volume_ratio_20d"].fillna(1.0).rank(pct=True) * 25
    ).round(1)
    view = view.sort_values(["龙头分", return_col], ascending=False)
    return view.rename(
        columns={
            "symbol": "代码",
            "name": "公司",
            "close": "收盘价",
            return_col: f"{period}涨跌(%)",
            "volume_ratio_20d": "成交量/20日均量",
            "trade_date": "交易日",
        }
    )[["代码", "公司", "收盘价", f"{period}涨跌(%)", "成交量/20日均量", "龙头分", "交易日"]]


def render_nasdaq_sector_page() -> None:
    theme_id = get_active_theme_id()
    st.markdown(NASDAQ_SECTOR_PAGE_CSS, unsafe_allow_html=True)
    st.html(f'<span class="ws-us-theme-marker ws-us-theme-marker--{escape(theme_id)}"></span>')
    st.subheader("🌐 纳斯达克板块与龙头")
    st.caption("以纳斯达克核心成长股为观察池，按交易主题聚合板块表现；板块收益采用核心代表股加权，不等同于官方行业指数。")

    with st.container(key="nasdaq-sector-toolbar"):
        toolbar = st.columns([1.2, 1, 4])
        with toolbar[0]:
            period = st.selectbox("观察周期", list(PERIOD_TO_DAYS), index=0, key="nasdaq_sector_period")
        with toolbar[1]:
            force_refresh = st.button("刷新数据", use_container_width=True, key="nasdaq_sector_refresh")

    try:
        with st.spinner("正在加载纳斯达克板块快照……"):
            snapshot = load_or_refresh_snapshot(period_label=period, force=force_refresh)
    except Exception as exc:
        st.error(f"纳斯达克行情暂不可用，且尚无最近成功快照：{exc}")
        return

    sector_df = pd.DataFrame(snapshot.get("sectors") or [])
    stock_df = pd.DataFrame(snapshot.get("stocks") or [])
    if sector_df.empty or stock_df.empty:
        st.info("当前快照暂无可展示的板块数据。")
        return

    source_note = (
        f"交易日 {snapshot.get('trade_date') or '-'} · 数据源 {snapshot.get('source') or '-'} · "
        f"覆盖 {snapshot.get('coverage', {}).get('loaded', 0)}/{snapshot.get('coverage', {}).get('total', 0)} 只"
    )
    if snapshot.get("is_stale"):
        st.warning(f"行情源刷新失败，当前展示最近成功快照。{source_note}")
    else:
        st.caption(source_note)

    _render_market_strip(snapshot, sector_df)
    st.markdown("#### 板块热力图")
    _render_sector_heatmap(sector_df)

    left, right = st.columns([1.15, 1])
    with left:
        st.markdown("#### 板块排行榜")
        rank_df = sector_df.copy()
        rank_df.insert(0, "排名", range(1, len(rank_df) + 1))
        rank_df = rank_df.rename(
            columns={
                "sector": "板块",
                "return_pct": f"{period}涨跌(%)",
                "relative_qqq_pct": "相对QQQ(百分点)",
                "breadth_pct": "上涨家数占比(%)",
                "leader_symbol": "领涨代码",
                "leader_name": "领涨公司",
                "leader_return_pct": "领涨幅度(%)",
            }
        )
        st.dataframe(
            rank_df[["排名", "板块", f"{period}涨跌(%)", "相对QQQ(百分点)", "上涨家数占比(%)", "领涨代码", "领涨公司", "领涨幅度(%)"]],
            use_container_width=True,
            hide_index=True,
            height=455,
        )
    with right:
        st.markdown("#### 板块强弱分布")
        chart_df = sector_df.sort_values("return_pct")
        fig = px.bar(
            chart_df,
            x="return_pct",
            y="sector",
            orientation="h",
            color="return_pct",
            color_continuous_scale=["#248A3D", "#E8EEF3", "#D94C51"],
            color_continuous_midpoint=0,
            labels={"return_pct": f"{period}涨跌(%)", "sector": "板块"},
        )
        fig.update_layout(
            template=build_apple_plotly_template(),
            height=455,
            coloraxis_showscale=False,
            margin=dict(l=10, r=15, t=10, b=25),
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### 板块龙头")
    focus_sector = st.selectbox("选择板块", sector_df["sector"].tolist(), key="nasdaq_sector_focus")
    leader_df = _stock_view(stock_df, focus_sector, period)
    if leader_df.empty:
        st.info("该板块暂无可用股票数据。")
    else:
        st.dataframe(
            leader_df,
            use_container_width=True,
            hide_index=True,
            height=min(520, 42 + len(leader_df) * 36),
            column_config={
                f"{period}涨跌(%)": st.column_config.NumberColumn(format="%+.2f%%"),
                "成交量/20日均量": st.column_config.NumberColumn(format="%.2fx"),
                "龙头分": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.1f"),
            },
        )
        leader = leader_df.iloc[0]
        st.caption(
            f"当前综合龙头：{leader['公司']}（{leader['代码']}） · 龙头分 {leader['龙头分']:.1f}。"
            "龙头分综合核心代表权重、周期涨幅和成交量活跃度，仅用于板块内部排序。"
        )
