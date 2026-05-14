from __future__ import annotations

import json
from typing import Any
from urllib.parse import quote

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from .service import rebuild_author_tracking_from_archive
from .store import (
    build_author_summary,
    get_author_tracking_metadata,
    get_engine,
    list_author_score_snapshots,
    list_cycle_event_details,
    list_cycle_price_history,
    list_cycles_with_scores,
    upsert_mention_override,
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
    "manual_split": "人工拆分",
}
DIRECTION_COLORS = {
    "bullish": "#E63946",
    "trim_signal": "#F4A261",
    "exit_signal": "#2A9D8F",
    "bearish": "#7D6B91",
    "neutral": "#6E7C8C",
}
TREND_DATE_COL = "\u65e5\u671f"
TREND_WIN_RATE_COL = "\u80dc\u7387%"
TREND_AVG_RETURN_COL = "\u5e73\u5747\u6536\u76ca%"
TREND_CYCLE_COUNT_COL = "\u7d2f\u8ba1\u5468\u671f"
TREND_CLOSED_COUNT_COL = "\u5df2\u5173\u95ed\u5468\u671f"


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


def _format_exit_quality_triplet(row: dict[str, Any]) -> str:
    return "/".join(
        [
            f"2D:{_format_exit_quality(row.get('exit_quality_2d'))}",
            f"5D:{_format_exit_quality(row.get('exit_quality_5d'))}",
            f"10D:{_format_exit_quality(row.get('exit_quality_10d'))}",
        ]
    )


def _format_override_direction(value: Any) -> str:
    direction = str(value or "").strip()
    return _format_direction(direction) if direction else "保持原判断"


def _coalesce_evidence_text(row: dict[str, Any]) -> str:
    for key in ("reply_text", "reason_text", "post_title", "post_content"):
        text = str(row.get(key) or "").strip()
        if text:
            return text
    return ""


def _load_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        loaded = json.loads(text)
    except Exception:
        return {}
    return dict(loaded) if isinstance(loaded, dict) else {}


def _load_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    text = str(value or "").strip()
    if not text:
        return []
    try:
        loaded = json.loads(text)
    except Exception:
        return []
    return list(loaded) if isinstance(loaded, list) else []


def _resolve_evidence_images(row: dict[str, Any]) -> dict[str, Any]:
    evidence_payload = _load_json_dict(row.get("evidence_payload_json") or row.get("evidence_payload"))
    raw_image_urls = row.get("post_image_urls")
    if not isinstance(raw_image_urls, list):
        raw_image_urls = _load_json_list(row.get("post_pic_url_json"))

    image_urls: list[str] = []
    for item in raw_image_urls:
        image_url = str(item or "").strip()
        if image_url and image_url not in image_urls:
            image_urls.append(image_url)

    direct_image_url = str(row.get("image_url") or "").strip()
    if direct_image_url and direct_image_url not in image_urls:
        image_urls.append(direct_image_url)

    image_index = evidence_payload.get("image_index")
    try:
        resolved_image_index = int(image_index) if image_index is not None else None
    except (TypeError, ValueError):
        resolved_image_index = None

    primary_image_url = None
    if resolved_image_index is not None and 0 <= resolved_image_index < len(image_urls):
        primary_image_url = image_urls[resolved_image_index]
    elif direct_image_url:
        primary_image_url = direct_image_url

    return {
        "image_urls": image_urls,
        "image_index": resolved_image_index,
        "primary_image_url": primary_image_url,
    }


def _render_evidence_images(item: dict[str, Any]) -> None:
    image_urls = [str(url or "").strip() for url in item.get("image_urls") or [] if str(url or "").strip()]
    if not image_urls:
        return

    st.markdown("**相关图片**")
    primary_image_url = str(item.get("primary_image_url") or "").strip()
    highlight_suffix = "OCR命中图" if str(item.get("source_label") or "").strip() == SOURCE_LABELS.get("image_ocr") else "当前证据图"

    for index, image_url in enumerate(image_urls, start=1):
        suffix = f"（{highlight_suffix}）" if primary_image_url and image_url == primary_image_url else ""
        st.image(image_url, caption=f"图片#{index}{suffix}", use_container_width=True)


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


def build_dashboard_payload(
    rows: list[dict],
    metadata: dict[str, Any] | None = None,
    snapshots: list[dict[str, Any]] | None = None,
) -> dict:
    active_cycles = [row for row in rows if row.get("cycle_status") in {"active", "trimmed"}]
    closed_cycles = [row for row in rows if row.get("cycle_status") in {"closed", "expired"}]
    return {
        "summary": build_author_summary(rows),
        "active_cycles": active_cycles,
        "closed_cycles": closed_cycles,
        "metadata": metadata or {},
        "snapshots": list(snapshots or []),
        "trend_df": build_summary_trend_df(snapshots or []),
    }


def build_summary_trend_df(snapshots: list[dict[str, Any]]) -> pd.DataFrame:
    if not snapshots:
        return pd.DataFrame(
            columns=[
                TREND_DATE_COL,
                TREND_WIN_RATE_COL,
                TREND_AVG_RETURN_COL,
                TREND_CYCLE_COUNT_COL,
                TREND_CLOSED_COUNT_COL,
            ]
        )

    records: list[dict[str, Any]] = []
    for row in sorted(snapshots, key=lambda item: str(item.get("snapshot_date") or "")):
        snapshot_date = str(row.get("snapshot_date") or "").strip()
        if not snapshot_date:
            continue
        records.append(
            {
                TREND_DATE_COL: snapshot_date,
                TREND_WIN_RATE_COL: round((_to_float(row.get("win_rate")) or 0.0) * 100, 2),
                TREND_AVG_RETURN_COL: round((_to_float(row.get("avg_return")) or 0.0) * 100, 2),
                TREND_CYCLE_COUNT_COL: int(row.get("cycle_count") or 0),
                TREND_CLOSED_COUNT_COL: int(row.get("closed_count") or 0),
            }
        )
    return pd.DataFrame(records)


def _build_summary_trend_chart(trend_df: pd.DataFrame) -> go.Figure | None:
    if trend_df.empty:
        return None

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=trend_df[TREND_DATE_COL],
            y=trend_df[TREND_WIN_RATE_COL],
            mode="lines+markers",
            name=TREND_WIN_RATE_COL,
            line={"color": "#0F766E", "width": 2},
            customdata=trend_df[[TREND_CYCLE_COUNT_COL, TREND_CLOSED_COUNT_COL]].values,
            hovertemplate=(
                f"{TREND_DATE_COL}=%{{x}}<br>"
                f"{TREND_WIN_RATE_COL}=%{{y:.2f}}<br>"
                f"{TREND_CYCLE_COUNT_COL}=%{{customdata[0]}}<br>"
                f"{TREND_CLOSED_COUNT_COL}=%{{customdata[1]}}<extra></extra>"
            ),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=trend_df[TREND_DATE_COL],
            y=trend_df[TREND_AVG_RETURN_COL],
            mode="lines+markers",
            name=TREND_AVG_RETURN_COL,
            line={"color": "#D97706", "width": 2},
            customdata=trend_df[[TREND_CYCLE_COUNT_COL, TREND_CLOSED_COUNT_COL]].values,
            hovertemplate=(
                f"{TREND_DATE_COL}=%{{x}}<br>"
                f"{TREND_AVG_RETURN_COL}=%{{y:.2f}}<br>"
                f"{TREND_CYCLE_COUNT_COL}=%{{customdata[0]}}<br>"
                f"{TREND_CLOSED_COUNT_COL}=%{{customdata[1]}}<extra></extra>"
            ),
        )
    )
    fig.update_layout(
        height=260,
        margin={"l": 20, "r": 20, "t": 20, "b": 20},
        legend={"orientation": "h", "y": 1.12, "x": 0},
        xaxis_title=TREND_DATE_COL,
        yaxis_title="\u6bd4\u7387 / \u6536\u76ca%",
    )
    return fig


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
                "超额收益%": _to_percent(row.get("excess_return")),
                "最大回撤%": _to_percent(row.get("max_drawdown")),
                "持有天数": row.get("hold_days"),
                "退出质量": _format_exit_quality(row.get("exit_quality_2d")),
                "10日退出质量": _format_exit_quality(row.get("exit_quality_10d")),
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
        image_payload = _resolve_evidence_images(row)
        has_override = any(
            [
                row.get("override_ts_code"),
                row.get("override_direction"),
                bool(row.get("is_excluded")),
                bool(row.get("force_new_cycle")),
                row.get("override_note"),
            ]
        )
        event_records.append(
            {
                "序号": row.get("event_sequence"),
                "时间": row.get("mention_time"),
                "来源": _format_source(row.get("source_type")),
                "动作": _format_direction(row.get("direction")),
                "置信度": round((_to_float(row.get("confidence_score")) or 0.0) * 100, 1),
                "目标价": row.get("target_text") or "",
                "证据": _truncate_text(evidence_text, max_len=60),
                "人工修正": "是" if has_override else "否",
            }
        )
        evidence_items.append(
            {
                "mention_id": row.get("mention_id"),
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
                "image_urls": image_payload["image_urls"],
                "image_index": image_payload["image_index"],
                "primary_image_url": image_payload["primary_image_url"],
                "post_id": row.get("post_id"),
                "reply_id": row.get("reply_id"),
                "ts_code": row.get("ts_code"),
                "override_ts_code": row.get("override_ts_code"),
                "override_direction": row.get("override_direction"),
                "is_excluded": bool(row.get("is_excluded")),
                "force_new_cycle": bool(row.get("force_new_cycle")),
                "override_note": row.get("override_note"),
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
        "benchmark_return_pct": _to_percent(cycle_row.get("benchmark_return")),
        "excess_return_pct": _to_percent(cycle_row.get("excess_return")),
        "max_drawdown_pct": _to_percent(cycle_row.get("max_drawdown")),
        "hold_days": cycle_row.get("hold_days"),
        "exit_quality_label": _format_exit_quality(cycle_row.get("exit_quality_2d")),
        "exit_quality_5d_label": _format_exit_quality(cycle_row.get("exit_quality_5d")),
        "exit_quality_10d_label": _format_exit_quality(cycle_row.get("exit_quality_10d")),
        "exit_quality_20d_label": _format_exit_quality(cycle_row.get("exit_quality_20d")),
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
    if metadata.get("pending_image_count") is not None:
        parts.append(f"待OCR：{metadata['pending_image_count']}")
    if metadata.get("ocr_processed_image_count") is not None:
        parts.append(f"已补录：{metadata['ocr_processed_image_count']}")
    if metadata.get("last_ocr_update_time"):
        parts.append(f"最近OCR更新：{metadata['last_ocr_update_time']}")
    return " | ".join(parts)


def _render_ocr_status_metrics(metadata: dict[str, Any]) -> None:
    pending_count = int(metadata.get("pending_image_count") or 0)
    processed_count = metadata.get("ocr_processed_image_count")
    last_ocr_update_time = str(metadata.get("last_ocr_update_time") or "").strip() or "-"

    ocr_cols = st.columns(3)
    ocr_cols[0].metric("待OCR", pending_count)
    ocr_cols[1].metric("已补录", "-" if processed_count is None else int(processed_count))
    ocr_cols[2].metric("最近OCR更新", last_ocr_update_time)


def _render_manual_override_form(engine, author_uid: str, evidence_items: list[dict[str, Any]]) -> None:
    if not author_uid or not evidence_items:
        return

    item_by_mention_id = {str(item.get("mention_id") or ""): item for item in evidence_items if item.get("mention_id")}
    if not item_by_mention_id:
        return

    st.markdown("#### 人工修正")
    selected_mention_id = st.selectbox(
        "选择要修正的事件",
        options=list(item_by_mention_id.keys()),
        format_func=lambda mention_id: (
            f"#{item_by_mention_id[mention_id].get('event_sequence')} "
            f"{item_by_mention_id[mention_id].get('mention_time')} | "
            f"{item_by_mention_id[mention_id].get('action_label')} | "
            f"{item_by_mention_id[mention_id].get('source_label')}"
        ),
    )
    selected_item = item_by_mention_id[selected_mention_id]
    has_existing_override = any(
        [
            selected_item.get("override_ts_code"),
            selected_item.get("override_direction"),
            bool(selected_item.get("is_excluded")),
            bool(selected_item.get("force_new_cycle")),
            selected_item.get("override_note"),
        ]
    )
    if has_existing_override:
        st.caption(
            "当前修正："
            f" 代码={selected_item.get('override_ts_code') or '-'} |"
            f" 方向={_format_override_direction(selected_item.get('override_direction'))} |"
            f" 排除={'是' if selected_item.get('is_excluded') else '否'} |"
            f" 新开周期={'是' if selected_item.get('force_new_cycle') else '否'}"
        )

    with st.form(f"manual_override_{selected_mention_id}"):
        override_ts_code = st.text_input(
            "修正股票代码",
            value=str(selected_item.get("override_ts_code") or ""),
            help="留空表示不改代码",
        )
        direction_options = {
            "": "保持原判断",
            "bullish": "改成看多",
            "trim_signal": "改成减仓",
            "exit_signal": "改成出货",
            "bearish": "改成转空",
        }
        direction_keys = list(direction_options.keys())
        current_direction = str(selected_item.get("override_direction") or "")
        if current_direction not in direction_keys:
            current_direction = ""
        override_direction = st.selectbox(
            "修正方向",
            options=direction_keys,
            index=direction_keys.index(current_direction),
            format_func=lambda key: direction_options[key],
        )
        is_excluded = st.checkbox("排除该事件（假阳性、错误抓取等）", value=bool(selected_item.get("is_excluded")))
        force_new_cycle = st.checkbox("从该事件开始强制新开一个周期", value=bool(selected_item.get("force_new_cycle")))
        override_note = st.text_area("修正备注", value=str(selected_item.get("override_note") or ""), height=80)
        save_override = st.form_submit_button("保存人工修正并重建", use_container_width=True)

    clear_override = False
    if has_existing_override:
        clear_override = st.button("清除该事件人工修正", type="secondary", use_container_width=True)

    if save_override or clear_override:
        upsert_mention_override(
            engine,
            selected_mention_id,
            override_ts_code=None if clear_override else override_ts_code,
            override_direction=None if clear_override else override_direction,
            is_excluded=False if clear_override else is_excluded,
            force_new_cycle=False if clear_override else force_new_cycle,
            override_note=None if clear_override else override_note,
        )
        rebuild_author_tracking_from_archive(engine, author_uid)
        st.success("人工修正已保存，并已按当前归档重建周期。")
        st.rerun()


def render_author_tracking_tab(engine=None) -> None:
    st.subheader(TRACKING_PAGE_LABEL)
    st.markdown(
        '<div class="ws-tracker-shell">'
        '<span class="ws-tracker-eyebrow">观点跟踪工作台</span>'
        '<h4>从首次提及到出货，按周期回看作者观点质量</h4>'
        '<p>集中查看活跃周期、已关闭周期、收益表现、事件证据和人工复核入口。</p>'
        '</div>',
        unsafe_allow_html=True,
    )
    st.caption("跟踪东方财富作者提及个股的活跃周期、关闭周期、事件证据与整体评分。")

    try:
        actual_engine = engine or get_engine()
        rows = list_cycles_with_scores(actual_engine)
        metadata = get_author_tracking_metadata(actual_engine)
        author_uid = next((str(row.get("author_uid") or "").strip() for row in rows if str(row.get("author_uid") or "").strip()), "")
        snapshot_rows = list_author_score_snapshots(actual_engine, author_uid, limit=60) if author_uid else []
    except Exception as exc:
        st.warning(f"观点跟踪数据暂不可用：{exc}")
        return

    payload = build_dashboard_payload(rows, metadata=metadata, snapshots=snapshot_rows)
    metadata_caption = _format_metadata_caption(payload["metadata"])
    if metadata_caption:
        st.caption(metadata_caption)
    _render_ocr_status_metrics(payload["metadata"])

    if not rows:
        st.info(
            "当前暂无观点跟踪数据。先运行 "
            "`python -m scripts.sync_eastmoney_author --author-uid <UID>` "
            "或 `scripts\\run_eastmoney_author_daily.bat` 完成首轮同步。"
        )
        return

    summary = payload["summary"]

    metric_cols = st.columns(6)
    metric_cols[0].metric("周期数", summary["cycle_count"])
    metric_cols[1].metric("活跃周期", summary["active_count"])
    metric_cols[2].metric("已关闭周期", summary["closed_count"])
    metric_cols[3].metric("已关闭胜率", f"{summary['win_rate'] * 100:.1f}%")
    metric_cols[4].metric("Payoff", "-" if summary.get("payoff_ratio") is None else f"{summary['payoff_ratio']:.2f}")
    metric_cols[5].metric("平均持有", "-" if summary.get("avg_hold_days") is None else f"{summary['avg_hold_days']:.1f}天")

    secondary_metric_cols = st.columns(3)
    secondary_metric_cols[0].metric("平均收益", "-" if summary.get("avg_return") is None else f"{summary['avg_return'] * 100:.1f}%")
    secondary_metric_cols[1].metric(
        "平均超额收益",
        "-" if summary.get("avg_excess_return") is None else f"{summary['avg_excess_return'] * 100:.1f}%",
    )
    effective_exit_rate = summary.get("effective_exit_rate")
    secondary_metric_cols[2].metric("有效出货率", "-" if effective_exit_rate is None else f"{effective_exit_rate * 100:.1f}%")

    trend_chart = _build_summary_trend_chart(payload["trend_df"])
    if trend_chart is not None:
        st.plotly_chart(trend_chart, use_container_width=True)

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
    benchmark_return_label = "-" if overview.get("benchmark_return_pct") is None else f"{overview['benchmark_return_pct']:.2f}%"

    st.markdown("#### 周期总览")
    overview_cols = st.columns(6)
    overview_cols[0].metric("股票", f"{selected_cycle.get('security_name') or overview['ts_code'] or '-'}（{overview['ts_code'] or '-'}）")
    overview_cols[1].metric("状态", overview["status_label"])
    overview_cols[2].metric("最新动作", overview["latest_stance_label"])
    overview_cols[3].metric("收益", "-" if overview["total_return_pct"] is None else f"{overview['total_return_pct']:.2f}%")
    overview_cols[4].metric("超额收益", "-" if overview["excess_return_pct"] is None else f"{overview['excess_return_pct']:.2f}%")
    overview_cols[5].metric("最大回撤", "-" if overview["max_drawdown_pct"] is None else f"{overview['max_drawdown_pct']:.2f}%")

    st.caption(
        " | ".join(
            [
                f"开始：{overview.get('cycle_open_time') or '-'}",
                f"最近提及：{overview.get('latest_mention_time') or '-'}",
                f"关闭：{overview.get('cycle_close_time') or '-'}",
                f"关闭原因：{overview.get('close_reason_label') or '-'}",
                f"事件数：{overview.get('event_count') or 0}",
                f"基准收益：{benchmark_return_label}",
                f"2/5/10/20D退出：{overview.get('exit_quality_label') or '-'} / {overview.get('exit_quality_5d_label') or '-'} / {overview.get('exit_quality_10d_label') or '-'} / {overview.get('exit_quality_20d_label') or '-'}",
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
            _render_evidence_images(item)
            if any([item.get("override_ts_code"), item.get("override_direction"), item.get("is_excluded"), item.get("force_new_cycle"), item.get("override_note")]):
                st.markdown("**人工修正**")
                st.caption(
                    f"代码={item.get('override_ts_code') or '-'} | "
                    f"方向={_format_override_direction(item.get('override_direction'))} | "
                    f"排除={'是' if item.get('is_excluded') else '否'} | "
                    f"新开周期={'是' if item.get('force_new_cycle') else '否'}"
                )
                if item.get("override_note"):
                    st.markdown(f"**修正备注**：{item['override_note']}")

    _render_manual_override_form(actual_engine, str(selected_cycle.get("author_uid") or ""), evidence_items)
