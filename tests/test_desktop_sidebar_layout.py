from pathlib import Path

from src.apple_theme import build_global_apple_theme_css


APP_SOURCE = Path("app.py").read_text(encoding="utf-8", errors="ignore")


def test_build_global_apple_theme_css_contains_desktop_sidebar_shell_selectors():
    css = build_global_apple_theme_css()

    assert ".ws-sidebar-block" in css
    assert ".ws-sidebar-brand" in css
    assert ".ws-sidebar-recent-item" in css
    assert ".ws-page-toolbar" in css


def test_app_py_contains_desktop_sidebar_navigation_shell_hooks():
    assert "def render_desktop_sidebar_navigation()" in APP_SOURCE
    assert "selected_module, selected_page = render_desktop_sidebar_navigation()" in APP_SOURCE
    assert "record_recent_visit(st.session_state, selected_module, selected_page)" in APP_SOURCE
    assert "快速跳转" in APP_SOURCE
