import ast
from pathlib import Path


APP_PATH = Path("app.py")
APP_SOURCE = APP_PATH.read_text(encoding="utf-8-sig", errors="ignore")
APP_AST = ast.parse(APP_SOURCE)


def _get_function_source(function_name: str) -> str:
    for node in APP_AST.body:
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            return ast.get_source_segment(APP_SOURCE, node) or ""
    raise AssertionError(f"Function {function_name!r} not found in app.py")


def test_render_desktop_sidebar_navigation_uses_tree_search_and_recent_visits():
    function_source = _get_function_source("render_desktop_sidebar_navigation")

    assert "sidebar_search_query" in function_source
    assert "sidebar_expanded_module_id" in function_source
    assert "search_sidebar_pages(search_query)" in function_source
    assert "get_recent_visits(st.session_state)" in function_source
    assert "resolve_expanded_module_id(" in function_source
    assert "SIDEBAR_MODULES" in function_source
    assert 'st.container(key="ws-sidebar-tree")' in function_source


def test_render_desktop_sidebar_navigation_removes_legacy_quick_jump_and_shortcuts():
    function_source = _get_function_source("render_desktop_sidebar_navigation")

    assert "sidebar_quick_jump_" not in function_source
    assert "get_default_shortcuts" not in function_source
    assert "sidebar_shortcut_" not in function_source
    assert 'st.sidebar.selectbox(' not in function_source


def test_security_deep_links_expand_the_stock_module():
    hydrate_source = _get_function_source("hydrate_security_jump_from_query_params")
    trigger_source = _get_function_source("trigger_security_tab_jump_if_needed")

    assert 'st.session_state["sidebar_expanded_module_id"] = "stock"' in hydrate_source
    assert 'st.session_state["sidebar_expanded_module_id"] = "stock"' in trigger_source


def test_app_py_does_not_keep_legacy_sidebar_token_block():
    assert "_LEGACY_DESKTOP_SIDEBAR_TEST_TOKENS" not in APP_SOURCE
