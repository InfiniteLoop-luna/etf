from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import streamlit.components.v1 as components


LOCAL_STORAGE_USERNAME_KEY = "wealthspark.username"
_ALLOWED_ACTIONS = {"idle", "read", "remove", "write"}
_COMPONENT = components.declare_component(
    "browser_user_storage",
    path=Path(__file__).resolve().parent / "user_storage_component",
)


@dataclass(frozen=True)
class BrowserStorageResult:
    action: str
    request_id: str
    username: str
    ok: bool
    error: str = ""


def render_browser_user_storage(
    *,
    action: str,
    request_id: str,
    username: str = "",
    key: str = "ws-user-storage-bridge",
) -> Any:
    if action not in _ALLOWED_ACTIONS:
        raise ValueError(f"Unsupported browser storage action: {action}")

    return _COMPONENT(
        storage_key=LOCAL_STORAGE_USERNAME_KEY,
        action=action,
        request_id=str(request_id),
        username=str(username or ""),
        key=key,
        default=None,
    )


def parse_browser_storage_result(
    payload: Any,
    *,
    expected_action: str,
    expected_request_id: str,
) -> BrowserStorageResult | None:
    if not isinstance(payload, dict):
        return None
    if payload.get("action") != expected_action:
        return None
    if str(payload.get("request_id") or "") != str(expected_request_id):
        return None

    return BrowserStorageResult(
        action=expected_action,
        request_id=str(expected_request_id),
        username=str(payload.get("username") or ""),
        ok=bool(payload.get("ok", False)),
        error=str(payload.get("error") or ""),
    )
