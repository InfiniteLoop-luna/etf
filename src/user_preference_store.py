from __future__ import annotations

import re

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.sync_tushare_security_data import build_db_url


TABLE_NAME = "app_user_preferences"
THEME_PREFERENCE_KEY = "theme"


def normalize_username(username: str) -> str:
    normalized = re.sub(r"\s+", " ", str(username or "").strip())
    return normalized[:64]


def normalize_preference_key(key: str) -> str:
    return str(key or "").strip().lower()[:64]


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


def ensure_user_preferences_table(engine: Engine) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        username VARCHAR(64) NOT NULL,
        preference_key VARCHAR(64) NOT NULL,
        preference_value TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (username, preference_key)
    );

    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_username_updated_at
        ON {TABLE_NAME} (username, updated_at DESC);
    """
    with engine.begin() as conn:
        for statement in [item.strip() for item in sql.split(";") if item.strip()]:
            conn.execute(text(statement))


def get_user_preference(
    username: str,
    preference_key: str,
    engine: Engine | None = None,
) -> str | None:
    normalized_username = normalize_username(username)
    normalized_key = normalize_preference_key(preference_key)
    if not normalized_username or not normalized_key:
        return None

    actual_engine = engine or get_engine()
    ensure_user_preferences_table(actual_engine)
    sql = f"""
    SELECT preference_value
    FROM {TABLE_NAME}
    WHERE username = :username
      AND preference_key = :preference_key
    LIMIT 1
    """
    with actual_engine.begin() as conn:
        row = conn.execute(
            text(sql),
            {"username": normalized_username, "preference_key": normalized_key},
        ).first()
    return str(row[0]) if row is not None else None


def set_user_preference(
    username: str,
    preference_key: str,
    preference_value: str,
    engine: Engine | None = None,
) -> bool:
    normalized_username = normalize_username(username)
    normalized_key = normalize_preference_key(preference_key)
    normalized_value = str(preference_value or "").strip()
    if not normalized_username or not normalized_key or not normalized_value:
        return False

    actual_engine = engine or get_engine()
    ensure_user_preferences_table(actual_engine)
    sql = f"""
    INSERT INTO {TABLE_NAME} (
        username, preference_key, preference_value, created_at, updated_at
    )
    VALUES (
        :username, :preference_key, :preference_value,
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    )
    ON CONFLICT (username, preference_key)
    DO UPDATE SET
        preference_value = EXCLUDED.preference_value,
        updated_at = CURRENT_TIMESTAMP
    """
    with actual_engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "username": normalized_username,
                "preference_key": normalized_key,
                "preference_value": normalized_value,
            },
        )
    return True


def get_user_theme(username: str, engine: Engine | None = None) -> str | None:
    return get_user_preference(username, THEME_PREFERENCE_KEY, engine=engine)


def set_user_theme(username: str, theme_id: str, engine: Engine | None = None) -> bool:
    return set_user_preference(
        username,
        THEME_PREFERENCE_KEY,
        theme_id,
        engine=engine,
    )
