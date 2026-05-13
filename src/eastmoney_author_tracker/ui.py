from __future__ import annotations

from typing import Any
from urllib.parse import quote

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from .models import normalize_timestamp
from .store import (
    build_author_summary,
    get_author_tracking_metadata,
    get_engine,
    list_cycle_event_details,
    list_cycle_price_history,
    list_cycles_with_scores,
)

TRACKING_PAGE_LABEL = "🧭 观点跟踪"

CYCLE_STATUS_LABELS = {
    "active": "活跃",
    "trimmed": "减仓跟踪",
    "closed": "已关闭",
    "expired": "超时关闭",
}
SOURCE_LABELS = {
    "stockbar": "股吧吧名",
    "title_body": "标题/正文",
    "author_reply": "作者回复",
    "image_ocr": "图片OCR",
}
DIRECTION_LABELS = {
    "bullish": "看多",
    "trim_signal": "减仓",
    "exit_signal": "出货",
    "bearish": "转空",
    "neutral": "中性",
}
CLOSE_REASON_LABELS = {
    "explicit_exit": "明确出货",
    "thesis_reversal": "观点反转",
    "timeout": "超时关闭",
}
DIRECTION_COLORS = {
    "bullish": "#0F766E",
    "trim_signal": "#D97706",
    "exit_signal": "#DC2626",
    "bearish": "#7C3AED",
    "neutral": "#475569",
}


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_percent(value: Any) -> float | None:
    numeric = _to_float(value)
    return None if numeric is None else round(numeric * 100, 2)


def _truncate_text(value: Any, max_len: int = 36) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= max_len:
        return text
    return f"{text[: max_len - 1]}…"


def _format_cycle_status(value: Any) -> str:
    return CYCLE_STATUS_LABELS.get(str(value or "").strip(), str(value or "-").strip() or "-")


def _format_direction(value: Any) -> str:
    return DIRECTION_LABELS.get(str(value or "").strip(), str(value or "-").strip() or "-")


def _format_latest_stance(direction: Any, cycle_status: Any) -> str:
    direction_text = str(direction or "").strip()
    status_text = str(cycle_status or "").strip()
    if direction_text == "exit_signal":
        return "已出货"
    if direction_text == "bearish":
        return "已转空"
    if status_text == "trimmed" or direction_text == "trim_signal":
        return "减仓跟踪"
    if status_text in {"closed", "expired"}:
        return "已结束"
    if direction_text == "bullish":
        return "继续看多"
    return _format_direction(direction)


def _format_source(value: Any) -> str:
    return SOURCE_LABELS.get(str(value or "").strip(), str(value or "-").strip() or "-")


def _format_close_reason(value: Any) -> str:
    if not value:
        return "-"
    return CLOSE_REASON_LABELS.get(str(value).strip(), str(value).strip())


def _format_exit_quality(value: Any) -> str:
    if value is True:
        return "有效"
    if value is False:
        return "待观察"
    return "-"


def _coalesce_evidence_text(row: dict[str, Any]) -> str:
    for key in ("reply_text", "reason_text", "post_title", "post_content"):
        text = str(row.get(key) or "").strip()
        if text:
            return text
    return ""


def _build_security_jump_link(
    query: Any,
    display_label: Any = None,
    nonce_key: str = "author_tracking_jump_render_nonce",
) -> str:
    query_text = str(query or "").strip()
    if not query_text:
        return "#"
    render_nonce = st.session_state.get(nonce_key, 0) + 1
    st.session_state[nonce_key] = render_nonce
    label_text = str(display_label or query_text).strip() or query_text
    return (
        f"?security_query={quote(query_text)}"
        f"&security_type=stock&open_tab=security&jump_nonce={render_nonce}_{quote(query_text)}"
        f"#{label_text}"
    )


def build_dashboard_payload(rows: list[dict], metadata: dict[str, Any] | None = None) -> dict:
    active_cycles = [row for row in rows if row.get("cycle_status") in {"active", "trimmed"}]
    closed_cycles = [row for row in rows if row.get("cycle_status") in {"closed", "expired"}]
    return {
        "summary": build_author_summary(rows),
        "active_cycles": active_cycles,
        "closed_cycles": closed_cycles,
        "metadata": metadata or {},
    }


def _to_cycle_display_df(rows: list[dict]) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for row in rows:
        ts_code = row.get("ts_code")
        security_name = str(row.get("security_name") or ts_code or "").strip() or str(ts_code or "-")
        records.append(
            {
                "周期ID": row.get("cycle_id"),
                "股票名称": _build_security_jump_link(ts_code, security_name),
                "代码": ts_code,
                "状态": _format_cycle_status(row.get("cycle_status")),
                "开始时间": row.get("cycle_open_time"),
                "最新提及": row.get("latest_mention_time"),
                "关闭时间": row.get("cycle_close_time"),
                "事件数": row.get("event_count"),
                "最新动作": _format_direction(row.get("latest_direction")),
                "关闭原因": _format_close_reason(row.get("close_reason")),
                "收益%": _to_percent(row.get("total_return")),
                "最大回撤%": _to_percent(row.get("max_drawdown")),
                "持有天数": row.get("hold_days"),
                "退出质量": _format_exit_quality(row.get("exit_quality_2d")),
                "最新观点": _truncate_text(row.get("latest_reason_text"), max_len=28),
            }
        )
    return pd.DataFrame(records)


def _match_event_trade_date(price_df: pd.DataFrame, mention_time: Any):
    if price_df.empty:
        return pd.NaT
    mention_dt = pd.to_datetime(mention_time, errors="coerce")
    if pd.isna(mention_dt):
        return pd.NaT
    event_date = mention_dt.normalize()
    eligible = price_df[price_df["trade_date"] <= event_date]
    if not eligible.empty:
        return eligible.iloc[-1]["trade_date"]
    future = price_df[price_df["trade_date"] >= event_date]
    if not future.empty:
        return future.iloc[0]["trade_date"]
    return pd.NaT


def _build_marker_df(event_rows: list[dict[str, Any]], price_df: pd.DataFrame) -> pd.DataFrame:
    if price_df.empty or not event_rows:
        return pd.DataFrame(columns=["日期", "trade_date", "close", "动作", "来源", "marker_text", "color", "证据"])

    records: list[dict[str, Any]] = []
    for row in event_rows:
        matched_trade_date = _match_event_trade_date(price_df, row.get("mention_time"))
        if pd.isna(matched_trade_date):
            continue
        price_row = price_df.loc[price_df["trade_date"] == matched_trade_date].tail(1)
        if price_row.empty:
            continue
        records.append(
            {
                "日期": matched_trade_date.strftime("%Y-%m-%d"),
                "trade_date": matched_trade_date,
                "close": float(price_row.iloc[0]["close"]),
                "动作": _format_direction(row.get("direction")),
                "来源": _format_source(row.get("source_type")),
                "marker_text": f"#{row.get('event_sequence')} {_format_direction(row.get('direction'))}",
                "color": DIRECTION_COLORS.get(str(row.get("direction") or "").strip(), "#475569"),
                "证据": _truncate_text(_coalesce_evidence_text(row), max_len=24),
            }
        )
    return pd.DataFrame(records)


def build_cycle_detail_payload(
    cycle_row: dict[str, Any],
    event_rows: list[dict[str, Any]],
    price_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    price_df = pd.DataFrame(price_rows or [])
    if not price_df.empty:
        price_df = price_df.copy()
        price_df["trade_date"] = pd.to_datetime(price_df["trade_date"], errors="coerce")
        price_df["close"] = pd.to_numeric(price_df["close"], errors="coerce")
        price_df = price_df.dropna(subset=["trade_date", "close"]).sort_values("trade_date").reset_index(drop=True)

    event_records: list[dict[str, Any]] = []
    evidence_items: list[dict[str, Any]] = []
    for row in event_rows:
        evidence_text = _coalesce_evidence_text(row)
        event_records.append(
            {
                "序号": row.get("event_sequence"),
                "时间": row.get("mention_time"),
                "来源": _format_source(row.get("source_type")),
                "动作": _format_direction(row.get("direction")),
                "置信度": round((_to_float(row.get("confidence_score")) or 0.0) * 100, 1),
                "目标价": row.get("target_text") or "",
                "证据": _truncate_text(evidence_text, max_len=60),
            }
        )
        evidence_items.append(
            {
                "event_sequence": row.get("event_sequence"),
                "mention_time": row.get("mention_time"),
                "source_label": _format_source(row.get("source_type")),
                "action_label": _format_direction(row.get("direction")),
                "confidence_pct": round((_to_float(row.get("confidence_score")) or 0.0) * 100, 1),
                "target_text": row.get("target_text") or "",
                "post_title": str(row.get("post_title") or "").strip(),
                "post_content": str(row.get("post_content") or "").strip(),
                "reply_text": str(row.get("reply_text") or "").strip(),
                "reason_text": str(row.get("reason_text") or "").strip(),
                "evidence_text": evidence_text,
                "post_id": row.get("post_id"),
                "reply_id": row.get("reply_id"),
            }
        )

    marker_df = _build_marker_df(event_rows, price_df)
    overview = {
        "cycle_id": cycle_row.get("cycle_id"),
        "ts_code": cycle_row.get("ts_code"),
        "status_label": _format_cycle_status(cycle_row.get("cycle_status")),
        "latest_stance_label": _format_latest_stance(cycle_row.get("latest_direction"), cycle_row.get("cycle_status")),
        "close_reason_label": _format_close_reason(cycle_row.get("close_reason")),
        "event_count": int(cycle_row.get("event_count") or len(event_rows)),
        "latest_mention_time": cycle_row.get("latest_mention_time"),
        "cycle_open_time": cycle_row.get("cycle_open_time"),
        "cycle_close_time": cycle_row.get("cycle_close_time"),
        "total_return_pct": _to_percent(cycle_row.get("total_return")),
        "max_drawdown_pct": _to_percent(cycle_row.get("max_drawdown")),
        "hold_days": cycle_row.get("hold_days"),
        "exit_quality_label": _format_exit_quality(cycle_row.get("exit_quality_2d")),
    }
    return {
        "overview": overview,
        "event_df": pd.DataFrame(event_records),
        "price_df": price_df,
        "marker_df": marker_df,
        "evidence_items": evidence_items,
    }


def _build_cycle_chart(detail_payload: dict[str, Any]) -> go.Figure | None:
    price_df = detail_payload["price_df"]
    marker_df = detail_payload["marker_df"]
    if price_df.empty:
        return None

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=price_df["trade_date"],
            y=price_df["close"],
            mode="lines",
            name="收盘价",
            line={"color": "#0F766E", "width": 2},
        )
    )
    if not marker_df.empty:
        fig.add_trace(
            go.Scatter(
                x=marker_df["trade_date"],
                y=marker_df["close"],
                mode="markers+text",
                text=marker_df["marker_text"],
                textposition="top center",
                name="观点事件",
                marker={"size": 10, "color": marker_df["color"].tolist(), "line": {"color": "white", "width": 1}},
                customdata=marker_df[["动作", "来源", "证据"]].values,
                hovertemplate=(
                    "日期=%{x|%Y-%m-%d}<br>"
                    "收盘=%{y:.2f}<br>"
                    "动作=%{customdata[0]}<br>"
                    "来源=%{customdata[1]}<br>"
                    "证据=%{customdata[2]}<extra></extra>"
                ),
            )
        )
    fig.update_layout(
        height=360,
        margin={"l": 20, "r": 20, "t": 30, "b": 20},
        legend={"orientation": "h", "y": 1.08, "x": 0},
        xaxis_title="交易日",
        yaxis_title="收盘价",
    )
    return fig


def _format_cycle_option(row: dict[str, Any]) -> str:
    open_time = str(row.get("cycle_open_time") or "").strip()
    security_name = str(row.get("security_name") or row.get("ts_code") or "-").strip() or "-"
    return f"{security_name}（{row.get('ts_code') or '-'}） | {_format_cycle_status(row.get('cycle_status'))} | {open_time or '-'}"


def _format_metadata_caption(metadata: dict[str, Any]) -> str:
    if not metadata:
        return ""
    parts = []
    if metadata.get("last_mention_time"):
        parts.append(f"最近提及：{metadata['last_mention_time']}")
    if metadata.get("last_post_time"):
        parts.append(f"最近发帖：{metadata['last_post_time']}")
    if metadata.get("post_count") is not None:
        parts.append(f"原帖数：{metadata['post_count']}")
    if metadata.get("mention_count") is not None:
        parts.append(f"提及事件：{metadata['mention_count']}")
    return " | ".join(parts)


def render_author_tracking_tab(engine=None) -> None:
    st.subheader(TRACKING_PAGE_LABEL)
    st.caption("跟踪东方财富作者提及个股的活跃周期、关闭周期、事件证据与整体评分。")

    try:
        actual_engine = engine or get_engine()
        rows = list_cycles_with_scores(actual_engine)
        metadata = get_author_tracking_metadata(actual_engine)
    except Exception as exc:
        st.warning(f"观点跟踪数据暂不可用：{exc}")
        return

    if not rows:
        st.info("当前暂无观点跟踪数据。先运行 `python scripts/sync_eastmoney_author.py --author-uid <UID>` 完成首轮同步。")
        return

    payload = build_dashboard_payload(rows, metadata=metadata)
    summary = payload["summary"]

    metric_cols = st.columns(5)
    metric_cols[0].metric("周期数", summary["cycle_count"])
    metric_cols[1].metric("活跃周期", summary["active_count"])
    metric_cols[2].metric("已关闭周期", summary["closed_count"])
    metric_cols[3].metric("已关闭胜率", f"{summary['win_rate'] * 100:.1f}%")
    effective_exit_rate = summary.get("effective_exit_rate")
    metric_cols[4].metric("有效出货率", "-" if effective_exit_rate is None else f"{effective_exit_rate * 100:.1f}%")

    metadata_caption = _format_metadata_caption(payload["metadata"])
    if metadata_caption:
        st.caption(metadata_caption)

    st.markdown("#### 活跃周期")
    active_df = _to_cycle_display_df(payload["active_cycles"])
    if active_df.empty:
        st.caption("当前没有活跃周期。")
    else:
        st.caption("点击“股票名称”列可跳转到“个股/指数查询”，并自动带入该股票代码。")
        st.dataframe(
            active_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "股票名称": st.column_config.LinkColumn(
                    "股票名称",
                    help="点击后跳转到个股/指数查询",
                    display_text=r".*#(.*)$",
                )
            },
        )

    st.markdown("#### 已关闭周期")
    closed_df = _to_cycle_display_df(payload["closed_cycles"])
    if closed_df.empty:
        st.caption("当前没有已关闭周期。")
    else:
        st.caption("点击“股票名称”列可跳转到“个股/指数查询”，并自动带入该股票代码。")
        st.dataframe(
            closed_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "股票名称": st.column_config.LinkColumn(
                    "股票名称",
                    help="点击后跳转到个股/指数查询",
                    display_text=r".*#(.*)$",
                )
            },
        )

    cycle_options = {row["cycle_id"]: row for row in rows if row.get("cycle_id")}
    if not cycle_options:
        return

    selected_cycle_id = st.selectbox(
        "查看周期详情",
        options=list(cycle_options.keys()),
        format_func=lambda cid: _format_cycle_option(cycle_options[cid]),
    )
    selected_cycle = cycle_options[selected_cycle_id]

    try:
        event_rows = list_cycle_event_details(actual_engine, selected_cycle_id)
    except Exception as exc:
        st.warning(f"周期事件暂不可用：{exc}")
        return

    price_rows: list[dict[str, Any]] = []
    price_error = None
    try:
        price_rows = list_cycle_price_history(
            actual_engine,
            selected_cycle.get("ts_code"),
            start_date=selected_cycle.get("cycle_open_time"),
            end_date=selected_cycle.get("cycle_close_time"),
        )
    except Exception as exc:
        price_error = exc

    detail_payload = build_cycle_detail_payload(selected_cycle, event_rows, price_rows)
    overview = detail_payload["overview"]

    st.markdown("#### 周期总览")
    overview_cols = st.columns(5)
    overview_cols[0].metric("股票", f"{selected_cycle.get('security_name') or overview['ts_code'] or '-'}（{overview['ts_code'] or '-'}）")
    overview_cols[1].metric("状态", overview["status_label"])
    overview_cols[2].metric("最新动作", overview["latest_stance_label"])
    overview_cols[3].metric("收益", "-" if overview["total_return_pct"] is None else f"{overview['total_return_pct']:.2f}%")
    overview_cols[4].metric("最大回撤", "-" if overview["max_drawdown_pct"] is None else f"{overview['max_drawdown_pct']:.2f}%")

    st.caption(
        " | ".join(
            [
                f"开始：{overview.get('cycle_open_time') or '-'}",
                f"最近提及：{overview.get('latest_mention_time') or '-'}",
                f"关闭：{overview.get('cycle_close_time') or '-'}",
                f"关闭原因：{overview.get('close_reason_label') or '-'}",
                f"事件数：{overview.get('event_count') or 0}",
                f"退出质量：{overview.get('exit_quality_label') or '-'}",
            ]
        )
    )

    chart = _build_cycle_chart(detail_payload)
    st.markdown("#### 价格与事件")
    if chart is None:
        st.caption("当前未加载到该周期的价格曲线。")
    else:
        st.plotly_chart(chart, use_container_width=True)
    if price_error is not None:
        st.caption(f"价格曲线暂不可用：{price_error}")

    st.markdown("#### 事件时间线")
    event_df = detail_payload["event_df"]
    if event_df.empty:
        st.caption("当前周期暂无事件明细。")
    else:
        st.dataframe(event_df, use_container_width=True, hide_index=True)

    st.markdown("#### 证据明细")
    evidence_items = detail_payload["evidence_items"]
    if not evidence_items:
        st.caption("当前周期暂无证据明细。")
        return

    for item in evidence_items:
        expander_label = (
            f"#{item['event_sequence']} {item['mention_time']} | "
            f"{item['action_label']} | {item['source_label']}"
        )
        with st.expander(expander_label, expanded=False):
            st.caption(
                f"post_id={item.get('post_id') or '-'} | "
                f"reply_id={item.get('reply_id') or '-'} | "
                f"置信度={item.get('confidence_pct', 0):.1f}%"
            )
            if item.get("target_text"):
                st.markdown(f"**目标价**：{item['target_text']}")
            if item.get("post_title"):
                st.markdown(f"**帖子标题**：{item['post_title']}")
            if item.get("reply_text"):
                st.markdown(f"**作者回复**：{item['reply_text']}")
            if item.get("reason_text") and item["reason_text"] != item.get("reply_text"):
                st.markdown(f"**提取证据**：{item['reason_text']}")
            if item.get("post_content") and item["post_content"] not in {item.get("reply_text"), item.get("reason_text")}:
                st.markdown(f"**帖子正文**：{item['post_content']}")
