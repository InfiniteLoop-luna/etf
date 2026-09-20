from __future__ import annotations

import math
import re

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from src.sync_tushare_security_data import build_db_url


TABLE_NAME = "app_user_watchlist"
DEFAULT_SECURITY_TYPE = "stock"
MAX_HOLDING_SHARES = 1_000_000_000_000_000_000.0
EMPTY_WATCHLIST_COLUMNS = [
    "username",
    "ts_code",
    "security_type",
    "security_name",
    "holding_shares",
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
        holding_shares NUMERIC(24, 6),
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (username, ts_code, security_type)
    );

    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_username_updated_at
        ON {TABLE_NAME} (username, updated_at DESC);
    """
    with engine.begin() as conn:
        for statement in [item.strip() for item in sql.split(";") if item.strip()]:
            conn.execute(text(statement))

    # Existing installations predate personal fund positions.  Keep the
    # migration here so the feature is immediately usable without a separate
    # deployment step.  A fresh inspector is used after a possible concurrent
    # migration because SQLAlchemy inspectors cache schema metadata.
    column_names = {column["name"] for column in inspect(engine).get_columns(TABLE_NAME)}
    if "holding_shares" not in column_names:
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        f"ALTER TABLE {TABLE_NAME} "
                        "ADD COLUMN holding_shares NUMERIC(24, 6)"
                    )
                )
        except Exception:
            refreshed_names = {
                column["name"] for column in inspect(engine).get_columns(TABLE_NAME)
            }
            if "holding_shares" not in refreshed_names:
                raise


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

    # Some tests and one-off local databases intentionally bypass the schema
    # initializer.  Reading those legacy tables should still be harmless.
    column_names = {
        column["name"] for column in inspect(actual_engine).get_columns(TABLE_NAME)
    }
    holding_shares_select = (
        "holding_shares" if "holding_shares" in column_names else "NULL AS holding_shares"
    )

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
        {holding_shares_select},
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


def _normalize_holding_shares(value) -> float | None:
    if value is None:
        return None
    numeric_value = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric_value):
        raise ValueError("holding_shares 必须是有效数字")
    normalized_value = float(numeric_value)
    if (
        not math.isfinite(normalized_value)
        or normalized_value <= 0
        or normalized_value >= MAX_HOLDING_SHARES
    ):
        raise ValueError("holding_shares 必须是大于 0 的有限有效数字")
    return normalized_value


def add_watchlist_items_batch(
    username: str,
    items: list[dict],
    engine: Engine | None = None,
) -> int:
    """Atomically add or update multiple watchlist rows.

    ``holding_shares=None`` preserves an existing position.  Set
    ``clear_holding_shares=True`` to explicitly clear it while retaining the
    security in the watchlist.
    """
    normalized_username = normalize_username(username)
    if not normalized_username:
        raise ValueError("username 不能为空")
    if not items:
        return 0

    params = []
    for item in items:
        normalized_code = str(item.get("ts_code") or "").strip().upper()
        normalized_type = (
            str(item.get("security_type") or DEFAULT_SECURITY_TYPE).strip().lower()
            or DEFAULT_SECURITY_TYPE
        )
        normalized_name = str(item.get("security_name") or "").strip()
        clear_holding_shares = bool(item.get("clear_holding_shares", False))
        normalized_holding_shares = (
            None
            if clear_holding_shares
            else _normalize_holding_shares(item.get("holding_shares"))
        )
        if not normalized_code:
            raise ValueError("ts_code 不能为空")
        params.append(
            {
                "username": normalized_username,
                "ts_code": normalized_code,
                "security_type": normalized_type,
                "security_name": normalized_name,
                "holding_shares": normalized_holding_shares,
                "clear_holding_shares": clear_holding_shares,
            }
        )

    actual_engine = engine or get_engine()
    ensure_user_watchlist_table(actual_engine)
    sql = f"""
    INSERT INTO {TABLE_NAME} (
        username,
        ts_code,
        security_type,
        security_name,
        holding_shares,
        created_at,
        updated_at
    )
    VALUES (
        :username,
        :ts_code,
        :security_type,
        :security_name,
        :holding_shares,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    )
    ON CONFLICT (username, ts_code, security_type)
    DO UPDATE SET
        security_name = COALESCE(NULLIF(EXCLUDED.security_name, ''), {TABLE_NAME}.security_name),
        holding_shares = CASE
            WHEN :clear_holding_shares THEN NULL
            ELSE COALESCE(EXCLUDED.holding_shares, {TABLE_NAME}.holding_shares)
        END,
        updated_at = CURRENT_TIMESTAMP
    """
    with actual_engine.begin() as conn:
        conn.execute(text(sql), params)
    return len(params)


def add_watchlist_item(
    username: str,
    ts_code: str,
    security_name: str = "",
    security_type: str = DEFAULT_SECURITY_TYPE,
    engine: Engine | None = None,
    holding_shares: float | None = None,
    clear_holding_shares: bool = False,
) -> None:
    add_watchlist_items_batch(
        username,
        [
            {
                "ts_code": ts_code,
                "security_type": security_type,
                "security_name": security_name,
                "holding_shares": holding_shares,
                "clear_holding_shares": clear_holding_shares,
            }
        ],
        engine=engine,
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
