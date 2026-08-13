from __future__ import annotations

import logging
from typing import Any

import streamlit as st


logger = logging.getLogger(__name__)

THEME_META: dict[str, dict[str, Any]] = {
    "apple": {
        "name": "Apple 专业",
        "name_en": "Apple Pro",
        "preview_colors": ["#0066CC", "#F5F5F7", "#FFFFFF", "#248A3D", "#D70015"],
    },
    "doraemon": {
        "name": "哆啦A梦",
        "name_en": "Doraemon",
        "preview_colors": ["#11A9EE", "#E5F3FE", "#FFFFFF", "#F46968", "#FCCD3D"],
    },
}

_DEFAULT_THEME_ID = "apple"
_SESSION_KEY = "active_theme_id"
_QUERY_PARAM_KEY = "theme"
_USER_THEME_HYDRATED_KEY = "user_theme_hydrated_for"


def is_valid_theme_id(theme_id: object) -> bool:
    return str(theme_id or "") in THEME_META


def _read_query_theme_id() -> str | None:
    try:
        value = st.query_params.get(_QUERY_PARAM_KEY) or None
        return str(value) if is_valid_theme_id(value) else None
    except Exception:
        return None


def _write_query_theme_id(theme_id: str) -> None:
    try:
        st.query_params[_QUERY_PARAM_KEY] = theme_id
    except Exception:
        pass


def get_active_theme_id() -> str:
    """Return current session/query theme; user hydration happens after login is known."""
    try:
        theme_id = st.session_state.get(_SESSION_KEY)
        if is_valid_theme_id(theme_id):
            return str(theme_id)

        query_theme = _read_query_theme_id()
        if query_theme:
            st.session_state[_SESSION_KEY] = query_theme
            return query_theme

        return _DEFAULT_THEME_ID
    except Exception:
        return _DEFAULT_THEME_ID


def set_active_theme_id(theme_id: str, username: str = "") -> bool:
    """Set the active theme and persist it for a logged-in user when provided."""
    if not is_valid_theme_id(theme_id):
        raise ValueError(f"Unknown theme_id: {theme_id!r}")

    st.session_state[_SESSION_KEY] = theme_id
    _write_query_theme_id(theme_id)

    normalized_username = str(username or "").strip()
    if not normalized_username:
        return True

    try:
        from src.user_preference_store import set_user_theme

        saved = set_user_theme(normalized_username, theme_id)
        if saved:
            st.session_state[_USER_THEME_HYDRATED_KEY] = normalized_username
        return bool(saved)
    except Exception as exc:
        logger.warning("Failed to save theme preference for %s: %s", normalized_username, exc)
        return False


def sync_theme_for_logged_in_user(username: str) -> str:
    """Hydrate a user's saved theme once per Streamlit session.

    A saved database preference wins in every newly opened page. If the user has
    no saved preference yet, an explicit theme query is promoted to the account;
    otherwise the default is used and stored for future pages.
    """
    normalized_username = str(username or "").strip()
    if not normalized_username:
        return get_active_theme_id()

    if st.session_state.get(_USER_THEME_HYDRATED_KEY) == normalized_username:
        return get_active_theme_id()

    try:
        from src.user_preference_store import get_user_theme, set_user_theme

        saved_theme = get_user_theme(normalized_username)
        if is_valid_theme_id(saved_theme):
            resolved = str(saved_theme)
        else:
            resolved = _read_query_theme_id() or get_active_theme_id()
            if not is_valid_theme_id(resolved):
                resolved = _DEFAULT_THEME_ID
            set_user_theme(normalized_username, resolved)

        st.session_state[_SESSION_KEY] = resolved
        st.session_state[_USER_THEME_HYDRATED_KEY] = normalized_username
        _write_query_theme_id(resolved)
        return resolved
    except Exception as exc:
        logger.warning("Failed to hydrate theme preference for %s: %s", normalized_username, exc)
        return get_active_theme_id()


def clear_user_theme_session() -> None:
    st.session_state.pop(_USER_THEME_HYDRATED_KEY, None)
    st.session_state.pop(_SESSION_KEY, None)
    try:
        if _QUERY_PARAM_KEY in st.query_params:
            del st.query_params[_QUERY_PARAM_KEY]
    except Exception:
        pass


def _load_presets() -> dict[str, dict]:
    from src.apple_theme import APPLE_THEME_DEFAULT_TOKENS
    from src.doraemon_theme import DORAEMON_THEME_TOKENS

    return {
        "apple": dict(APPLE_THEME_DEFAULT_TOKENS),
        "doraemon": dict(DORAEMON_THEME_TOKENS),
    }


def get_active_theme_tokens() -> dict:
    theme_id = get_active_theme_id()
    presets = _load_presets()
    return presets.get(theme_id, presets[_DEFAULT_THEME_ID])


def list_available_themes() -> list[dict]:
    return [{"id": theme_id, **meta} for theme_id, meta in THEME_META.items()]


def get_theme_extra_css() -> str:
    if get_active_theme_id() == "doraemon":
        from src.doraemon_theme import build_doraemon_extra_css

        return build_doraemon_extra_css()
    return ""
