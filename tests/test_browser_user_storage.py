from pathlib import Path

from src.browser_user_storage import (
    LOCAL_STORAGE_USERNAME_KEY,
    parse_browser_storage_result,
)


COMPONENT_HTML = (
    Path("src") / "user_storage_component" / "index.html"
).read_text(encoding="utf-8")
APP_SOURCE = Path("app.py").read_text(encoding="utf-8-sig", errors="ignore")


def test_browser_storage_component_reads_writes_and_removes_username():
    assert LOCAL_STORAGE_USERNAME_KEY == "wealthspark.username"
    assert "window.localStorage.getItem(storageKey)" in COMPONENT_HTML
    assert "window.localStorage.setItem(storageKey, username)" in COMPONENT_HTML
    assert "window.localStorage.removeItem(storageKey)" in COMPONENT_HTML


def test_parse_browser_storage_result_waits_for_matching_request():
    payload = {
        "action": "read",
        "request_id": "hydrate-1",
        "username": " nicky ",
        "ok": True,
    }

    assert (
        parse_browser_storage_result(
            payload,
            expected_action="write",
            expected_request_id="hydrate-1",
        )
        is None
    )
    assert (
        parse_browser_storage_result(
            payload,
            expected_action="read",
            expected_request_id="hydrate-2",
        )
        is None
    )


def test_parse_browser_storage_result_keeps_storage_status():
    result = parse_browser_storage_result(
        {
            "action": "read",
            "request_id": "hydrate-1",
            "username": " nicky ",
            "ok": False,
            "error": "storage blocked",
        },
        expected_action="read",
        expected_request_id="hydrate-1",
    )

    assert result is not None
    assert result.username == " nicky "
    assert result.ok is False
    assert result.error == "storage blocked"


def test_app_uses_login_dialog_instead_of_inline_login_expander():
    assert '@st.dialog("用户登录", dismissible=False' in APP_SOURCE
    assert 'with st.expander("👤 用户登录"' not in APP_SOURCE
    assert 'render_browser_user_storage(action="idle"' in APP_SOURCE


def test_app_hydrates_browser_username_before_rendering_page_content():
    render_start = APP_SOURCE.index("def _render_application_page()")
    hydrate_login = APP_SOURCE.index("render_user_login_status()", render_start)
    hydrate_navigation = APP_SOURCE.index(
        "hydrate_security_jump_from_query_params()",
        render_start,
    )

    assert hydrate_login < hydrate_navigation
    assert 'st.session_state["logged_in_username"] = stored_username' in APP_SOURCE
    assert '_queue_browser_username_sync("write", normalized_username)' in APP_SOURCE
    assert '_queue_browser_username_sync("remove")' in APP_SOURCE
