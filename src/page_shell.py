from __future__ import annotations

from dataclasses import dataclass
from html import escape
from typing import Any, Callable, Mapping, TypeVar


ICON_ROOT = "/app/static/icons"
T = TypeVar("T")


@dataclass(frozen=True)
class PageStatus:
    update_date: str = "-"
    update_timestamp: str = "-"
    update_source: str = "last_update.json"
    freshness_tone: str = "neutral"
    freshness_label: str = "资金链状态未知"
    freshness_detail: str = "暂无新鲜度结果"


def _display_value(value: Any, fallback: str = "-") -> str:
    text = str(value or "").strip()
    return text or fallback


def build_page_status(
    update_summary: Mapping[str, Any] | None,
    funding_freshness: Mapping[str, Any] | None,
) -> PageStatus:
    update_meta = dict((update_summary or {}).get("last_update") or {})
    update_date = _display_value(
        update_meta.get("effective_update_date") or update_meta.get("update_date")
    )
    update_timestamp = _display_value(
        update_meta.get("effective_last_update") or update_meta.get("last_update")
    )
    update_source = _display_value(update_meta.get("source"), "last_update.json")

    freshness = dict(funding_freshness or {})
    if not freshness:
        return PageStatus(
            update_date=update_date,
            update_timestamp=update_timestamp,
            update_source=update_source,
        )

    target_date = _display_value(freshness.get("target_date"))
    items = [item for item in (freshness.get("items") or []) if isinstance(item, Mapping)]
    stale_items = [item for item in items if not item.get("ok")]
    if not stale_items:
        return PageStatus(
            update_date=update_date,
            update_timestamp=update_timestamp,
            update_source=update_source,
            freshness_tone="healthy",
            freshness_label=f"资金链正常 · 目标 {target_date}",
            freshness_detail=f"{len(items)} 项检查通过",
        )

    stale_detail = "；".join(
        f"{_display_value(item.get('key'), '未命名')}={_display_value(item.get('latest_date'))}"
        for item in stale_items[:6]
    )
    return PageStatus(
        update_date=update_date,
        update_timestamp=update_timestamp,
        update_source=update_source,
        freshness_tone="warning",
        freshness_label=f"资金链滞后 · {len(stale_items)} 项",
        freshness_detail=f"目标 {target_date}；{stale_detail}",
    )


def build_page_status_bar_html(status: PageStatus) -> str:
    tone_icon = {
        "healthy": "circle-check",
        "warning": "triangle-alert",
        "neutral": "info",
    }.get(status.freshness_tone, "info")
    update_title = escape(
        f"记录时间：{status.update_timestamp} / 来源：{status.update_source}",
        quote=True,
    )
    freshness_title = escape(status.freshness_detail, quote=True)

    return f"""
<div class="ws-page-status-bar" role="status" aria-live="polite">
    <span class="ws-page-status-bar__item" title="{update_title}">
        <img src="{ICON_ROOT}/calendar-days.svg" alt="">
        <span>数据 {escape(status.update_date)}</span>
    </span>
    <span class="ws-page-status-bar__divider" aria-hidden="true"></span>
    <span class="ws-page-status-bar__item ws-page-status-bar__item--{status.freshness_tone}" title="{freshness_title}">
        <img src="{ICON_ROOT}/{tone_icon}.svg" alt="">
        <span>{escape(status.freshness_label)}</span>
    </span>
    <span class="ws-page-status-bar__meta">{escape(status.update_source)} · {escape(status.update_timestamp)}</span>
</div>
"""


def build_page_loading_mask_html() -> str:
    return f"""
<div class="ws-page-loading-mask" role="status" aria-live="polite" aria-label="页面加载中">
    <span class="ws-page-loading-mask__indicator">
        <img src="{ICON_ROOT}/refresh-cw.svg" alt="">
        <span>加载中</span>
    </span>
</div>
"""


def render_with_page_loading_mask(streamlit_ui: Any, render_page: Callable[[], T]) -> T:
    loading_slot = streamlit_ui.empty()
    loading_slot.markdown(
        build_page_loading_mask_html(),
        unsafe_allow_html=True,
    )
    try:
        return render_page()
    finally:
        loading_slot.empty()
