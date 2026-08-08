import ast
from pathlib import Path
from textwrap import dedent

import pytest

from src.navigation_config import STOCK_COMPANY_SCREENER_LABEL, STOCK_SECURITY_SEARCH_LABEL

AppTest = pytest.importorskip("streamlit.testing.v1").AppTest


APP_PATH = Path("app.py")
APP_SOURCE = APP_PATH.read_text(encoding="utf-8-sig", errors="ignore")
APP_AST = ast.parse(APP_SOURCE)
SIDEBAR_INTERACTION_SCRIPT = dedent(
    """
    import streamlit as st
    from app import render_desktop_sidebar_navigation
    from src.navigation_config import DECISION_TODAY_PAGE_LABEL, STOCK_SECURITY_SEARCH_LABEL

    if "initialized" not in st.session_state:
        st.session_state["initialized"] = True
        st.session_state["sidebar_nav_group"] = "决策"
        st.session_state["decision_subpage"] = DECISION_TODAY_PAGE_LABEL
        st.session_state["stock_subpage"] = STOCK_SECURITY_SEARCH_LABEL
        st.session_state["sidebar_recent_pages"] = [
            {"module_id": "stock", "page_id": "company_screener"},
        ]

    selected_module, selected_page = render_desktop_sidebar_navigation()
    st.write(f"selected={selected_module}/{selected_page}")
    """
)


def _get_function_source(function_name: str) -> str:
    for node in APP_AST.body:
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            return ast.get_source_segment(APP_SOURCE, node) or ""
    raise AssertionError(f"Function {function_name!r} not found in app.py")


def test_render_desktop_sidebar_navigation_uses_tree_search_and_recent_visits():
    function_source = _get_function_source("render_desktop_sidebar_navigation")

    assert "sidebar_search_query" in function_source
    assert "sidebar_expanded_module_ids" in function_source
    assert "search_sidebar_pages(search_query)" in function_source
    assert "get_recent_visits(st.session_state)" in function_source
    assert "_get_sidebar_expanded_module_ids(" in function_source
    assert "SIDEBAR_MODULES" in function_source
    assert 'sidebar_middle.container(key="ws-sidebar-tree")' in function_source
    assert '"current" if is_active_page else ""' in function_source


def test_sidebar_module_expansion_is_fragment_scoped_without_full_app_rerun():
    function_node = next(
        node
        for node in APP_AST.body
        if isinstance(node, ast.FunctionDef) and node.name == "render_desktop_sidebar_navigation"
    )
    function_source = _get_function_source("render_desktop_sidebar_navigation")
    decorators = [ast.unparse(decorator) for decorator in function_node.decorator_list]

    assert "st.fragment" in decorators
    assert "on_click=_toggle_sidebar_module_expansion" in function_source
    module_toggle = function_source.split("for module in ordered_modules:", 1)[1].split(
        "for page in module.pages:", 1
    )[0]
    assert "st.rerun()" not in module_toggle


def test_render_desktop_sidebar_navigation_removes_legacy_quick_jump_and_shortcuts():
    function_source = _get_function_source("render_desktop_sidebar_navigation")

    assert "sidebar_quick_jump_" not in function_source
    assert "get_default_shortcuts" not in function_source
    assert "sidebar_shortcut_" not in function_source
    assert 'st.sidebar.selectbox(' not in function_source


def test_security_deep_links_expand_the_stock_module():
    hydrate_source = _get_function_source("hydrate_security_jump_from_query_params")
    trigger_source = _get_function_source("trigger_security_tab_jump_if_needed")

    assert '_expand_sidebar_module("stock")' in hydrate_source
    assert '_expand_sidebar_module("stock")' in trigger_source


def test_app_py_does_not_keep_legacy_sidebar_token_block():
    assert "_LEGACY_DESKTOP_SIDEBAR_TEST_TOKENS" not in APP_SOURCE


def test_clicking_search_result_clears_search_and_navigates_without_error():
    app_test = AppTest.from_string(SIDEBAR_INTERACTION_SCRIPT)
    app_test.run(timeout=10)

    after_search = app_test.text_input[0].set_value("security").run(timeout=10)
    result_button = next(
        button
        for button in after_search.button
        if button.key.startswith("ws-sidebar-search-result-security_search-")
    )

    after_click = result_button.click().run(timeout=10)

    assert len(after_click.exception) == 0
    assert after_click.session_state["sidebar_nav_group"] == "股票"
    assert after_click.session_state["stock_subpage"] == STOCK_SECURITY_SEARCH_LABEL
    assert after_click.text_input[0].value == ""
    assert any(
        button.key.startswith("ws-sidebar-page-security_search-active-current")
        for button in after_click.button
    )


def test_clicking_recent_visit_clears_search_and_navigates_without_error():
    app_test = AppTest.from_string(SIDEBAR_INTERACTION_SCRIPT)
    initial_run = app_test.run(timeout=10)
    recent_button = next(
        button
        for button in initial_run.button
        if button.key.startswith("ws-sidebar-recent-link-company_screener-")
    )

    after_click = recent_button.click().run(timeout=10)

    assert len(after_click.exception) == 0
    assert after_click.session_state["sidebar_nav_group"] == "股票"
    assert after_click.session_state["stock_subpage"] == STOCK_COMPANY_SCREENER_LABEL
    assert after_click.text_input[0].value == ""
    assert any(
        button.key.startswith("ws-sidebar-page-company_screener-active-current")
        for button in after_click.button
    )


def test_recent_visits_can_collapse_without_changing_the_selected_page():
    app_test = AppTest.from_string(SIDEBAR_INTERACTION_SCRIPT)
    initial_run = app_test.run(timeout=10)
    recent_toggle = next(
        button
        for button in initial_run.button
        if button.key.startswith("ws-sidebar-recent-toggle-expanded")
    )

    after_collapse = recent_toggle.click().run(timeout=10)

    assert len(after_collapse.exception) == 0
    assert after_collapse.session_state["sidebar_recent_expanded"] is False
    assert not any(
        button.key.startswith("ws-sidebar-recent-link-")
        for button in after_collapse.button
    )
    assert any(
        "selected=决策/" in markdown.value
        for markdown in after_collapse.markdown
    )
