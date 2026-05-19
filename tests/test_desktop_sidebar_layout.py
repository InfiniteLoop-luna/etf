from pathlib import Path

from src.apple_theme import build_global_apple_theme_css


APP_SOURCE = Path("app.py").read_text(encoding="utf-8-sig", errors="ignore")


def test_build_global_apple_theme_css_contains_desktop_sidebar_shell_selectors():
    css = build_global_apple_theme_css()

    assert ".ws-sidebar-block" in css
    assert ".ws-sidebar-brand" in css
    assert ".ws-sidebar-recent-item" in css
    assert ".ws-page-toolbar" in css


def test_app_py_contains_desktop_sidebar_navigation_shell_hooks():
    assert "def render_desktop_sidebar_navigation()" in APP_SOURCE
    assert "selected_module, selected_page = render_desktop_sidebar_navigation()" in APP_SOURCE
    assert 'st.container(key="ws-sidebar-tree")' in APP_SOURCE
    assert 'key="sidebar_search_query"' in APP_SOURCE
    assert "search_sidebar_pages(search_query)" in APP_SOURCE
    assert "_LEGACY_DESKTOP_SIDEBAR_TEST_TOKENS" not in APP_SOURCE


def test_app_py_removes_legacy_desktop_quick_jump_flow():
    assert "sidebar_quick_jump_" not in APP_SOURCE
    assert "sidebar_shortcut_" not in APP_SOURCE
    assert 'st.sidebar.selectbox(' not in APP_SOURCE
    assert "sidebar_quick_jump_applied" not in APP_SOURCE
