from __future__ import annotations

import pandas as pd
import streamlit as st

from .store import build_author_summary, get_engine, list_cycle_events, list_cycles_with_scores

TRACKING_PAGE_LABEL = "🧭 观点跟踪"


def build_dashboard_payload(rows: list[dict]) -> dict:
    active_cycles = [row for row in rows if row.get("cycle_status") in {"active", "trimmed"}]
    closed_cycles = [row for row in rows if row.get("cycle_status") in {"closed", "expired"}]
    return {
        "summary": build_author_summary(rows),
        "active_cycles": active_cycles,
        "closed_cycles": closed_cycles,
    }


def _to_cycle_display_df(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    display_df = df.copy()
    if "total_return" in display_df.columns:
        display_df["total_return_pct"] = (pd.to_numeric(display_df["total_return"], errors="coerce") * 100).round(2)
    if "max_drawdown" in display_df.columns:
        display_df["max_drawdown_pct"] = (pd.to_numeric(display_df["max_drawdown"], errors="coerce") * 100).round(2)
    preferred = [
        "cycle_id",
        "ts_code",
        "cycle_status",
        "cycle_open_time",
        "cycle_close_time",
        "close_reason",
        "total_return_pct",
        "max_drawdown_pct",
        "hold_days",
    ]
    ordered = [column for column in preferred if column in display_df.columns]
    return display_df[ordered]


def render_author_tracking_tab(engine=None) -> None:
    st.subheader(TRACKING_PAGE_LABEL)
    st.caption("跟踪东方财富作者提及个股的活跃周期、关闭周期与整体评分。")

    try:
        actual_engine = engine or get_engine()
        rows = list_cycles_with_scores(actual_engine)
    except Exception as exc:
        st.warning(f"观点跟踪数据暂不可用：{exc}")
        return

    if not rows:
        st.info("当前暂无观点跟踪数据。先运行 `python scripts/sync_eastmoney_author.py --author-uid <UID>` 完成首轮同步。")
        return

    payload = build_dashboard_payload(rows)
    summary = payload["summary"]

    metric_cols = st.columns(4)
    metric_cols[0].metric("周期数", summary["cycle_count"])
    metric_cols[1].metric("活跃周期", summary["active_count"])
    metric_cols[2].metric("已关闭周期", summary["closed_count"])
    metric_cols[3].metric("胜率", f"{summary['win_rate'] * 100:.1f}%")

    st.markdown("#### 活跃周期")
    active_df = _to_cycle_display_df(payload["active_cycles"])
    if active_df.empty:
        st.caption("当前没有活跃周期。")
    else:
        st.dataframe(active_df, use_container_width=True, hide_index=True)

    st.markdown("#### 已关闭周期")
    closed_df = _to_cycle_display_df(payload["closed_cycles"])
    if closed_df.empty:
        st.caption("当前没有已关闭周期。")
    else:
        st.dataframe(closed_df, use_container_width=True, hide_index=True)

    cycle_options = {row["cycle_id"]: row for row in rows if row.get("cycle_id")}
    if not cycle_options:
        return

    selected_cycle_id = st.selectbox("查看周期事件", options=list(cycle_options.keys()))
    event_rows = list_cycle_events(actual_engine, selected_cycle_id)
    st.markdown("#### 周期事件")
    if not event_rows:
        st.caption("当前周期暂无事件明细。")
    else:
        st.dataframe(pd.DataFrame(event_rows), use_container_width=True, hide_index=True)
