from unittest.mock import Mock

import pytest

from src.apple_theme import build_global_apple_theme_css
from src.page_shell import (
    build_page_loading_mask_html,
    build_page_status,
    build_page_status_bar_html,
    render_with_page_loading_mask,
)


def test_page_status_builds_compact_healthy_footer_payload():
    status = build_page_status(
        {
            "last_update": {
                "effective_update_date": "2026-08-05",
                "effective_last_update": "2026-08-05T19:52:54Z",
                "source": "last_update.json",
            }
        },
        {
            "target_date": "2026-08-05",
            "items": [{"key": "moneyflow", "ok": True}],
        },
    )

    assert status.update_date == "2026-08-05"
    assert status.freshness_tone == "healthy"
    assert status.freshness_label == "资金链正常 · 目标 2026-08-05"

    html = build_page_status_bar_html(status)
    assert 'class="ws-page-status-bar"' in html
    assert 'circle-check.svg' in html
    assert "数据 2026-08-05" in html
    assert "stAlert" not in html


def test_page_status_summarizes_stale_items_without_large_alerts():
    status = build_page_status(
        {"last_update": {"update_date": "2026-08-04", "source": "<snapshot>"}},
        {
            "target_date": "2026-08-05",
            "items": [
                {"key": "moneyflow", "latest_date": "2026-08-04", "ok": False},
                {"key": "margin", "latest_date": "2026-08-03", "ok": False},
            ],
        },
    )

    assert status.freshness_tone == "warning"
    assert status.freshness_label == "资金链滞后 · 2 项"
    assert "moneyflow=2026-08-04" in status.freshness_detail

    html = build_page_status_bar_html(status)
    assert 'triangle-alert.svg' in html
    assert "&lt;snapshot&gt;" in html


def test_page_loading_mask_uses_local_svg_and_accessible_status_markup():
    html = build_page_loading_mask_html()

    assert 'class="ws-page-loading-mask"' in html
    assert 'class="ws-page-loading-mask__spinner"' in html
    assert 'role="status"' in html
    assert '/app/static/icons/refresh-cw.svg' in html
    assert "加载中" in html


def test_page_loading_mask_is_removed_after_successful_render():
    streamlit_ui = Mock()
    loading_slot = streamlit_ui.empty.return_value
    page_status = build_page_status({}, {})

    result = render_with_page_loading_mask(streamlit_ui, lambda: page_status)

    assert result is page_status
    loading_slot.markdown.assert_called_once()
    loading_slot.empty.assert_called_once_with()


def test_page_loading_mask_is_removed_when_page_render_fails():
    streamlit_ui = Mock()
    loading_slot = streamlit_ui.empty.return_value

    def fail_render():
        raise RuntimeError("page failed")

    with pytest.raises(RuntimeError, match="page failed"):
        render_with_page_loading_mask(streamlit_ui, fail_render)

    loading_slot.empty.assert_called_once_with()


def test_global_theme_positions_loading_mask_and_status_bar_in_main_area():
    css = build_global_apple_theme_css()

    assert ".ws-page-loading-mask {" in css
    assert "inset: 0 0 32px var(--ws-sidebar-width)" in css
    assert ".ws-page-status-bar {" in css
    assert "inset: auto 0 0 var(--ws-sidebar-width)" in css


def test_global_theme_removes_header_space_and_allows_sidebar_collapse():
    css = build_global_apple_theme_css()

    assert 'header,\n[data-testid="stHeader"] {' in css
    header_rule = css.split('header,\n[data-testid="stHeader"] {', 1)[1].split("}", 1)[0]
    assert "display: none !important" in header_rule
    assert "height: 0 !important" in header_rule
    assert "transform: none !important" not in css
    assert '[data-testid="stSidebarCollapseButton"]' in css
    assert '[data-testid="stExpandSidebarButton"]' in css
    assert '.stApp:has([data-testid="stSidebar"][aria-expanded="false"])' in css


def test_global_theme_keeps_loading_spinner_rotating():
    css = build_global_apple_theme_css()

    assert ".ws-page-loading-mask__spinner {" in css
    assert "animation: ws-page-loading-spin 0.8s linear infinite" in css
    assert "prefers-reduced-motion" not in css
