from __future__ import annotations

import re

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.sync_tushare_security_data import build_db_url


TABLE_NAME = "app_user_watchlist"
DEFAULT_SECURITY_TYPE = "stock"
EMPTY_WATCHLIST_COLUMNS = [
    "username",
    "ts_code",
    "security_type",
    "security_name",
    "created_at",
    "updated_at",
]


def normalize_username(username: str) -> str:
    normalized = re.sub(r"\s+", " ", str(username or "").strip())
    return normalized[:64]


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


def ensure_user_watchlist_table(engine: Engine) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        username VARCHAR(64) NOT NULL,
        ts_code VARCHAR(20) NOT NULL,
        security_type VARCHAR(20) NOT NULL DEFAULT '{DEFAULT_SECURITY_TYPE}',
        security_name VARCHAR(120),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (username, ts_code, security_type)
    );

    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_username_updated_at
        ON {TABLE_NAME} (username, updated_at DESC);
    """
    with engine.begin() as conn:
        for statement in [item.strip() for item in sql.split(";") if item.strip()]:
            conn.execute(text(statement))


def list_watchlist_items(
    username: str,
    engine: Engine | None = None,
    security_type: str | None = None,
) -> pd.DataFrame:
    normalized_username = normalize_username(username)
    if not normalized_username:
        return pd.DataFrame(columns=EMPTY_WATCHLIST_COLUMNS)

    normalized_type = str(security_type or "").strip().lower()
    actual_engine = engine or get_engine()
    ensure_user_watchlist_table(actual_engine)

    where_clauses = ["username = :username"]
    params = {"username": normalized_username}
    if normalized_type:
        where_clauses.append("security_type = :security_type")
        params["security_type"] = normalized_type

    sql = f"""
    SELECT
        username,
        ts_code,
        security_type,
        COALESCE(NULLIF(security_name, ''), ts_code) AS security_name,
        created_at,
        updated_at
    FROM {TABLE_NAME}
    WHERE {' AND '.join(where_clauses)}
    ORDER BY updated_at DESC, ts_code ASC
    """
    return pd.read_sql(text(sql), actual_engine, params=params)


def is_in_watchlist(
    username: str,
    ts_code: str,
    security_type: str = DEFAULT_SECURITY_TYPE,
    engine: Engine | None = None,
) -> bool:
    normalized_username = normalize_username(username)
    normalized_code = str(ts_code or "").strip().upper()
    normalized_type = str(security_type or DEFAULT_SECURITY_TYPE).strip().lower() or DEFAULT_SECURITY_TYPE
    if not normalized_username or not normalized_code:
        return False

    actual_engine = engine or get_engine()
    ensure_user_watchlist_table(actual_engine)

    sql = f"""
    SELECT 1
    FROM {TABLE_NAME}
    WHERE username = :username
      AND ts_code = :ts_code
      AND security_type = :security_type
    LIMIT 1
    """
    with actual_engine.begin() as conn:
        result = conn.execute(
            text(sql),
            {
                "username": normalized_username,
                "ts_code": normalized_code,
                "security_type": normalized_type,
            },
        ).first()
    return result is not None


def add_watchlist_item(
    username: str,
    ts_code: str,
    security_name: str = "",
    security_type: str = DEFAULT_SECURITY_TYPE,
    engine: Engine | None = None,
) -> None:
    normalized_username = normalize_username(username)
    normalized_code = str(ts_code or "").strip().upper()
    normalized_type = str(security_type or DEFAULT_SECURITY_TYPE).strip().lower() or DEFAULT_SECURITY_TYPE
    normalized_name = str(security_name or "").strip()

    if not normalized_username:
        raise ValueError("username 不能为空")
    if not normalized_code:
        raise ValueError("ts_code 不能为空")

    actual_engine = engine or get_engine()
    ensure_user_watchlist_table(actual_engine)

    sql = f"""
    INSERT INTO {TABLE_NAME} (
        username,
        ts_code,
        security_type,
        security_name,
        created_at,
        updated_at
    )
    VALUES (
        :username,
        :ts_code,
        :security_type,
        :security_name,
        NOW(),
        NOW()
    )
    ON CONFLICT (username, ts_code, security_type)
    DO UPDATE SET
        security_name = COALESCE(NULLIF(EXCLUDED.security_name, ''), {TABLE_NAME}.security_name),
        updated_at = NOW()
    """
    with actual_engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "username": normalized_username,
                "ts_code": normalized_code,
                "security_type": normalized_type,
                "security_name": normalized_name,
            },
        )


def remove_watchlist_item(
    username: str,
    ts_code: str,
    security_type: str = DEFAULT_SECURITY_TYPE,
    engine: Engine | None = None,
) -> int:
    normalized_username = normalize_username(username)
    normalized_code = str(ts_code or "").strip().upper()
    normalized_type = str(security_type or DEFAULT_SECURITY_TYPE).strip().lower() or DEFAULT_SECURITY_TYPE
    if not normalized_username or not normalized_code:
        return 0

    actual_engine = engine or get_engine()
    ensure_user_watchlist_table(actual_engine)

    sql = f"""
    DELETE FROM {TABLE_NAME}
    WHERE username = :username
      AND ts_code = :ts_code
      AND security_type = :security_type
    """
    with actual_engine.begin() as conn:
        result = conn.execute(
            text(sql),
            {
                "username": normalized_username,
                "ts_code": normalized_code,
                "security_type": normalized_type,
            },
        )
    return int(result.rowcount or 0)


def remove_watchlist_items_batch(
    username: str,
    items: list[tuple[str, str]],
    engine: Engine | None = None,
) -> int:
    """批量删除自选股票。

    Parameters
    ----------
    username : str
        用户名
    items : list[tuple[str, str]]
        每个元素为 ``(ts_code, security_type)``
    engine : Engine | None
        数据库引擎，为 ``None`` 时自动创建

    Returns
    -------
    int
        实际删除的行数
    """
    normalized_username = normalize_username(username)
    if not normalized_username or not items:
        return 0

    actual_engine = engine or get_engine()
    ensure_user_watchlist_table(actual_engine)

    total_deleted = 0
    with actual_engine.begin() as conn:
        for ts_code, security_type in items:
            normalized_code = str(ts_code or "").strip().upper()
            normalized_type = str(security_type or DEFAULT_SECURITY_TYPE).strip().lower() or DEFAULT_SECURITY_TYPE
            if not normalized_code:
                continue
            result = conn.execute(
                text(
                    f"DELETE FROM {TABLE_NAME} "
                    "WHERE username = :username AND ts_code = :ts_code AND security_type = :security_type"
                ),
                {
                    "username": normalized_username,
                    "ts_code": normalized_code,
                    "security_type": normalized_type,
                },
            )
            total_deleted += int(result.rowcount or 0)
    return total_deleted
