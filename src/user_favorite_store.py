from __future__ import annotations

import re

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.sync_tushare_security_data import build_db_url


TABLE_NAME = "app_user_favorite_pages"
EMPTY_FAVORITE_COLUMNS = [
    "username",
    "module_id",
    "page_id",
    "page_label",
    "created_at",
    "updated_at",
]


def normalize_username(username: str) -> str:
    normalized = re.sub(r"\s+", " ", str(username or "").strip())
    return normalized[:64]


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


def ensure_user_favorite_pages_table(engine: Engine) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        username VARCHAR(64) NOT NULL,
        module_id VARCHAR(64) NOT NULL,
        page_id VARCHAR(64) NOT NULL,
        page_label VARCHAR(180) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (username, module_id, page_id)
    );

    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_username_updated_at
        ON {TABLE_NAME} (username, updated_at DESC);
    """
    with engine.begin() as conn:
        for statement in [item.strip() for item in sql.split(";") if item.strip()]:
            conn.execute(text(statement))


def list_favorite_pages(username: str, engine: Engine | None = None) -> pd.DataFrame:
    normalized_username = normalize_username(username)
    if not normalized_username:
        return pd.DataFrame(columns=EMPTY_FAVORITE_COLUMNS)

    actual_engine = engine or get_engine()
    ensure_user_favorite_pages_table(actual_engine)

    sql = f"""
    SELECT
        username,
        module_id,
        page_id,
        page_label,
        created_at,
        updated_at
    FROM {TABLE_NAME}
    WHERE username = :username
    ORDER BY updated_at DESC, page_label ASC
    """
    return pd.read_sql(text(sql), actual_engine, params={"username": normalized_username})


def add_favorite_page(
    username: str,
    module_id: str,
    page_id: str,
    page_label: str,
    engine: Engine | None = None,
) -> bool:
    normalized_username = normalize_username(username)
    normalized_module = str(module_id or "").strip()
    normalized_page = str(page_id or "").strip()
    normalized_label = str(page_label or "").strip()
    if not normalized_username or not normalized_module or not normalized_page or not normalized_label:
        return False

    actual_engine = engine or get_engine()
    ensure_user_favorite_pages_table(actual_engine)

    sql = f"""
    INSERT INTO {TABLE_NAME} (username, module_id, page_id, page_label)
    VALUES (:username, :module_id, :page_id, :page_label)
    ON CONFLICT (username, module_id, page_id)
    DO UPDATE SET
        page_label = EXCLUDED.page_label,
        updated_at = NOW()
    """
    with actual_engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "username": normalized_username,
                "module_id": normalized_module,
                "page_id": normalized_page,
                "page_label": normalized_label,
            },
        )
    return True


def remove_favorite_page(
    username: str,
    module_id: str,
    page_id: str,
    engine: Engine | None = None,
) -> int:
    normalized_username = normalize_username(username)
    normalized_module = str(module_id or "").strip()
    normalized_page = str(page_id or "").strip()
    if not normalized_username or not normalized_module or not normalized_page:
        return 0

    actual_engine = engine or get_engine()
    ensure_user_favorite_pages_table(actual_engine)

    sql = f"""
    DELETE FROM {TABLE_NAME}
    WHERE username = :username
      AND module_id = :module_id
      AND page_id = :page_id
    """
    with actual_engine.begin() as conn:
        result = conn.execute(
            text(sql),
            {
                "username": normalized_username,
                "module_id": normalized_module,
                "page_id": normalized_page,
            },
        )
    return int(result.rowcount or 0)

