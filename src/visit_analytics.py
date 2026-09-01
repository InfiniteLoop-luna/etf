from __future__ import annotations

import ipaddress
import threading
from datetime import datetime, timezone
from typing import Mapping
from uuid import uuid4
from weakref import WeakSet

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


TABLE_NAME = "app_page_visits"
VISIT_ANALYTICS_ADMIN_USERNAME = "lijing"
EMPTY_VISIT_COLUMNS = [
    "visit_id",
    "visited_at",
    "username",
    "ip_address",
    "module_id",
    "module_label",
    "page_id",
    "page_label",
    "session_id",
    "user_agent",
    "referrer",
]
_INITIALIZED_ENGINES: WeakSet[Engine] = WeakSet()
_TABLE_INIT_LOCK = threading.Lock()


def is_visit_analytics_admin(username: str) -> bool:
    return str(username or "").strip().casefold() == VISIT_ANALYTICS_ADMIN_USERNAME


def _bounded_text(value: object, max_length: int) -> str:
    return str(value or "").strip()[:max_length]


def _header_value(headers: Mapping[str, object] | None, name: str) -> str:
    if not headers:
        return ""
    normalized_name = name.casefold()
    for key, value in headers.items():
        if str(key).casefold() == normalized_name:
            return str(value or "").strip()
    return ""


def _normalize_ip_candidate(value: str) -> str:
    candidate = str(value or "").strip().strip('"')
    if not candidate:
        return ""
    if candidate.startswith("[") and "]" in candidate:
        candidate = candidate[1 : candidate.index("]")]
    elif candidate.count(":") == 1 and "." in candidate:
        host, port = candidate.rsplit(":", 1)
        if port.isdigit():
            candidate = host
    try:
        return str(ipaddress.ip_address(candidate))
    except ValueError:
        return ""


def extract_client_ip(headers: Mapping[str, object] | None) -> str:
    """Extract the first valid client IP reported by common reverse proxies."""
    for header_name in (
        "CF-Connecting-IP",
        "X-Forwarded-For",
        "X-Real-IP",
        "Remote-Addr",
    ):
        raw_value = _header_value(headers, header_name)
        for candidate in raw_value.split(","):
            normalized = _normalize_ip_candidate(candidate)
            if normalized:
                return normalized
    return ""


def extract_request_metadata(headers: Mapping[str, object] | None) -> dict[str, str]:
    return {
        "ip_address": extract_client_ip(headers),
        "user_agent": _bounded_text(_header_value(headers, "User-Agent"), 1000),
        "referrer": _bounded_text(
            _header_value(headers, "Referer") or _header_value(headers, "Referrer"),
            1000,
        ),
    }


def ensure_page_visits_table(engine: Engine) -> None:
    if engine in _INITIALIZED_ENGINES:
        return
    with _TABLE_INIT_LOCK:
        if engine in _INITIALIZED_ENGINES:
            return
        timestamp_type = "TIMESTAMPTZ" if engine.dialect.name == "postgresql" else "TIMESTAMP"
        statements = [
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                visit_id VARCHAR(36) PRIMARY KEY,
                visited_at {timestamp_type} NOT NULL,
                username VARCHAR(64) NOT NULL,
                ip_address VARCHAR(64),
                module_id VARCHAR(64) NOT NULL,
                module_label VARCHAR(120) NOT NULL,
                page_id VARCHAR(64) NOT NULL,
                page_label VARCHAR(180) NOT NULL,
                session_id VARCHAR(80),
                user_agent TEXT,
                referrer TEXT
            )
            """,
            f"""
            CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_visited_at
                ON {TABLE_NAME} (visited_at DESC)
            """,
            f"""
            CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_page_visited_at
                ON {TABLE_NAME} (page_id, visited_at DESC)
            """,
            f"""
            CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_username_visited_at
                ON {TABLE_NAME} (username, visited_at DESC)
            """,
        ]
        with engine.begin() as conn:
            for statement in statements:
                conn.execute(text(statement.strip()))
        _INITIALIZED_ENGINES.add(engine)


def record_page_visit(
    engine: Engine,
    *,
    username: str,
    module_id: str,
    module_label: str,
    page_id: str,
    page_label: str,
    ip_address: str = "",
    session_id: str = "",
    user_agent: str = "",
    referrer: str = "",
    visited_at: datetime | None = None,
) -> str:
    normalized_username = _bounded_text(username, 64)
    normalized_module_id = _bounded_text(module_id, 64)
    normalized_module_label = _bounded_text(module_label, 120)
    normalized_page_id = _bounded_text(page_id, 64)
    normalized_page_label = _bounded_text(page_label, 180)
    if not all(
        (
            normalized_username,
            normalized_module_id,
            normalized_module_label,
            normalized_page_id,
            normalized_page_label,
        )
    ):
        raise ValueError("username、模块和版面信息不能为空")

    ensure_page_visits_table(engine)
    visit_id = str(uuid4())
    actual_visited_at = visited_at or datetime.now(timezone.utc)
    if actual_visited_at.tzinfo is None:
        actual_visited_at = actual_visited_at.replace(tzinfo=timezone.utc)

    sql = f"""
    INSERT INTO {TABLE_NAME} (
        visit_id,
        visited_at,
        username,
        ip_address,
        module_id,
        module_label,
        page_id,
        page_label,
        session_id,
        user_agent,
        referrer
    ) VALUES (
        :visit_id,
        :visited_at,
        :username,
        :ip_address,
        :module_id,
        :module_label,
        :page_id,
        :page_label,
        :session_id,
        :user_agent,
        :referrer
    )
    """
    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "visit_id": visit_id,
                "visited_at": actual_visited_at,
                "username": normalized_username,
                "ip_address": _bounded_text(ip_address, 64),
                "module_id": normalized_module_id,
                "module_label": normalized_module_label,
                "page_id": normalized_page_id,
                "page_label": normalized_page_label,
                "session_id": _bounded_text(session_id, 80),
                "user_agent": _bounded_text(user_agent, 1000),
                "referrer": _bounded_text(referrer, 1000),
            },
        )
    return visit_id


def list_page_visits(
    engine: Engine,
    *,
    start_at: datetime | None = None,
    end_at: datetime | None = None,
    username: str = "",
    ip_address: str = "",
    limit: int = 50_000,
) -> pd.DataFrame:
    ensure_page_visits_table(engine)
    where_clauses: list[str] = []
    params: dict[str, object] = {}
    if start_at is not None:
        where_clauses.append("visited_at >= :start_at")
        params["start_at"] = start_at
    if end_at is not None:
        where_clauses.append("visited_at < :end_at")
        params["end_at"] = end_at
    if str(username or "").strip():
        where_clauses.append("username = :username")
        params["username"] = _bounded_text(username, 64)
    if str(ip_address or "").strip():
        where_clauses.append("ip_address = :ip_address")
        params["ip_address"] = _bounded_text(ip_address, 64)

    bounded_limit = max(1, min(int(limit), 100_000))
    params["limit"] = bounded_limit
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    sql = f"""
    SELECT {', '.join(EMPTY_VISIT_COLUMNS)}
    FROM {TABLE_NAME}
    {where_sql}
    ORDER BY visited_at DESC
    LIMIT :limit
    """
    return pd.read_sql(text(sql), engine, params=params)


def infer_device_type(user_agent: str) -> str:
    normalized = str(user_agent or "").casefold()
    if not normalized:
        return "未知"
    if any(token in normalized for token in ("bot", "spider", "crawler", "headless")):
        return "机器人"
    if any(token in normalized for token in ("ipad", "tablet")):
        return "平板"
    if any(token in normalized for token in ("mobile", "iphone", "android")):
        return "手机"
    return "桌面"
