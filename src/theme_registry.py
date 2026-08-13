from __future__ import annotations

from typing import Any

import streamlit as st


THEME_META: dict[str, dict[str, Any]] = {
    "apple": {
        "name": "Apple 专业",
        "name_en": "Apple Pro",
        "preview_colors": ["#0066CC", "#F5F5F7", "#FFFFFF", "#248A3D", "#D70015"],
    },
    "doraemon": {
        "name": "哆啦A梦",
        "name_en": "Doraemon",
        "preview_colors": ["#4DB7FF", "#F5FAFF", "#FFFFFF", "#FF6B6B", "#FFD23F"],
    },
}

_DEFAULT_THEME_ID = "apple"
_SESSION_KEY = "active_theme_id"
_QUERY_PARAM_KEY = "theme"


def _read_persisted_theme_id() -> str | None:
    """Read the theme ID from the URL query params (persistent across reloads)."""
    try:
        params = st.query_params
        value = params.get(_QUERY_PARAM_KEY) or None
        if value and value in THEME_META:
            return value
    except Exception:
        pass
    return None


def get_active_theme_id() -> str:
    """Return the current theme ID from session state, query params, or default."""
    try:
        # 1. Session state takes priority (set during this session)
        theme_id = st.session_state.get(_SESSION_KEY)
        if theme_id and theme_id in THEME_META:
            return theme_id

        # 2. Fall back to URL query param (persisted from previous session)
        persisted = _read_persisted_theme_id()
        if persisted:
            st.session_state[_SESSION_KEY] = persisted
            return persisted

        # 3. Default
        return _DEFAULT_THEME_ID
    except Exception:
        return _DEFAULT_THEME_ID


def set_active_theme_id(theme_id: str) -> None:
    """Set the active theme and persist to URL query params."""
    if theme_id not in THEME_META:
        raise ValueError(f"Unknown theme_id: {theme_id!r}")
    try:
        st.session_state[_SESSION_KEY] = theme_id
        # Persist to URL so the theme survives page refreshes
        st.query_params[_QUERY_PARAM_KEY] = theme_id
    except Exception:
        pass


def _load_presets() -> dict[str, dict]:
    """Lazy-load theme token dictionaries to avoid circular imports."""
    from src.apple_theme import APPLE_THEME_DEFAULT_TOKENS
    from src.doraemon_theme import DORAEMON_THEME_TOKENS

    return {
        "apple": dict(APPLE_THEME_DEFAULT_TOKENS),
        "doraemon": dict(DORAEMON_THEME_TOKENS),
    }


def get_active_theme_tokens() -> dict:
    """Return the full token dictionary for the active theme."""
    theme_id = get_active_theme_id()
    presets = _load_presets()
    return presets.get(theme_id, presets[_DEFAULT_THEME_ID])


def list_available_themes() -> list[dict]:
    """Return metadata for all registered themes."""
    return [{"id": tid, **meta} for tid, meta in THEME_META.items()]


def get_theme_extra_css() -> str:
    """Return any theme-specific CSS beyond the base global stylesheet."""
    theme_id = get_active_theme_id()
    if theme_id == "doraemon":
        from src.doraemon_theme import build_doraemon_extra_css

        return build_doraemon_extra_css()
    return ""
