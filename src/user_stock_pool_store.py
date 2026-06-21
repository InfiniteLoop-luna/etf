from __future__ import annotations

import re

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.sync_tushare_security_data import build_db_url
from src.user_watchlist_store import normalize_username
from src.watchlist_excel_importer import normalize_stock_ts_code


TABLE_NAME = "app_user_stock_pool"
EMPTY_STOCK_POOL_COLUMNS = [
    "username",
    "ts_code",
    "security_name",
    "industry",
    "tags",
    "note",
    "source_file",
    "imported_at",
    "created_at",
    "updated_at",
]


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


def normalize_tag_text(value) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\n", " ").strip())[:40]


def split_tags(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        parts: list[str] = []
        for item in value:
            parts.extend(split_tags(item))
    else:
        parts = re.split(r"[,，、;；|\r\n\t]+", str(value or ""))

    tags: list[str] = []
    seen: set[str] = set()
    for part in parts:
        tag = normalize_tag_text(part)
        tag_key = tag.lower()
        if not tag or tag_key in seen:
            continue
        seen.add(tag_key)
        tags.append(tag)
    return tags


def format_tags(value) -> str:
    return ", ".join(split_tags(value))


def merge_tag_values(*values) -> str:
    merged: list[str] = []
    seen: set[str] = set()
    for value in values:
        for tag in split_tags(value):
            tag_key = tag.lower()
            if tag_key in seen:
                continue
            seen.add(tag_key)
            merged.append(tag)
    return ", ".join(merged)


def ensure_user_stock_pool_table(engine: Engine) -> None:
    statements = [
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            username VARCHAR(64) NOT NULL,
            ts_code VARCHAR(20) NOT NULL,
            security_name VARCHAR(120),
            industry VARCHAR(120),
            tags TEXT NOT NULL DEFAULT '',
            note TEXT NOT NULL DEFAULT '',
            source_file VARCHAR(255),
            imported_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (username, ts_code)
        )
        """,
        f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS industry VARCHAR(120)",
        f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS tags TEXT NOT NULL DEFAULT ''",
        f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS note TEXT NOT NULL DEFAULT ''",
        f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS source_file VARCHAR(255)",
        f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS imported_at TIMESTAMPTZ",
        f"""
        CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_username_updated_at
            ON {TABLE_NAME} (username, updated_at DESC)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_username_industry
            ON {TABLE_NAME} (username, industry)
        """,
    ]
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement.strip()))


def _empty_stock_pool_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=EMPTY_STOCK_POOL_COLUMNS)


def list_stock_pool_items(username: str, engine: Engine | None = None) -> pd.DataFrame:
    normalized_username = normalize_username(username)
    if not normalized_username:
        return _empty_stock_pool_frame()

    actual_engine = engine or get_engine()
    ensure_user_stock_pool_table(actual_engine)

    params = {"username": normalized_username}
    joined_sql = f"""
    SELECT
        p.username,
        p.ts_code,
        COALESCE(NULLIF(p.security_name, ''), NULLIF(b.name, ''), p.ts_code) AS security_name,
        COALESCE(NULLIF(p.industry, ''), NULLIF(b.industry, ''), '') AS industry,
        COALESCE(p.tags, '') AS tags,
        COALESCE(p.note, '') AS note,
        COALESCE(p.source_file, '') AS source_file,
        p.imported_at,
        p.created_at,
        p.updated_at
    FROM {TABLE_NAME} p
    LEFT JOIN vw_ts_stock_basic b ON b.ts_code = p.ts_code
    WHERE p.username = :username
    ORDER BY p.updated_at DESC, p.ts_code ASC
    """
    fallback_sql = f"""
    SELECT
        username,
        ts_code,
        COALESCE(NULLIF(security_name, ''), ts_code) AS security_name,
        COALESCE(industry, '') AS industry,
        COALESCE(tags, '') AS tags,
        COALESCE(note, '') AS note,
        COALESCE(source_file, '') AS source_file,
        imported_at,
        created_at,
        updated_at
    FROM {TABLE_NAME}
    WHERE username = :username
    ORDER BY updated_at DESC, ts_code ASC
    """
    try:
        return pd.read_sql(text(joined_sql), actual_engine, params=params)
    except Exception:
        return pd.read_sql(text(fallback_sql), actual_engine, params=params)


def _get_existing_pool_item(
    username: str,
    ts_code: str,
    engine: Engine,
) -> dict | None:
    sql = f"""
    SELECT ts_code, tags
    FROM {TABLE_NAME}
    WHERE username = :username AND ts_code = :ts_code
    LIMIT 1
    """
    with engine.begin() as conn:
        row = conn.execute(text(sql), {"username": username, "ts_code": ts_code}).mappings().first()
    return dict(row) if row else None


def upsert_stock_pool_item(
    username: str,
    ts_code: str,
    security_name: str = "",
    industry: str = "",
    tags: str | list[str] | tuple[str, ...] = "",
    note: str = "",
    source_file: str = "",
    engine: Engine | None = None,
) -> str:
    normalized_username = normalize_username(username)
    normalized_code = normalize_stock_ts_code(ts_code)
    if not normalized_username:
        raise ValueError("username cannot be empty")
    if not normalized_code:
        raise ValueError("ts_code cannot be empty")

    actual_engine = engine or get_engine()
    ensure_user_stock_pool_table(actual_engine)

    existing_item = _get_existing_pool_item(normalized_username, normalized_code, actual_engine)
    merged_tags = merge_tag_values((existing_item or {}).get("tags", ""), tags)
    status = "updated" if existing_item else "inserted"

    sql = f"""
    INSERT INTO {TABLE_NAME} (
        username,
        ts_code,
        security_name,
        industry,
        tags,
        note,
        source_file,
        imported_at,
        created_at,
        updated_at
    )
    VALUES (
        :username,
        :ts_code,
        :security_name,
        :industry,
        :tags,
        :note,
        :source_file,
        NOW(),
        NOW(),
        NOW()
    )
    ON CONFLICT (username, ts_code)
    DO UPDATE SET
        security_name = COALESCE(NULLIF(EXCLUDED.security_name, ''), {TABLE_NAME}.security_name),
        industry = COALESCE(NULLIF(EXCLUDED.industry, ''), {TABLE_NAME}.industry),
        tags = EXCLUDED.tags,
        note = COALESCE(NULLIF(EXCLUDED.note, ''), {TABLE_NAME}.note),
        source_file = COALESCE(NULLIF(EXCLUDED.source_file, ''), {TABLE_NAME}.source_file),
        imported_at = CASE
            WHEN NULLIF(EXCLUDED.source_file, '') IS NULL THEN {TABLE_NAME}.imported_at
            ELSE NOW()
        END,
        updated_at = NOW()
    """
    with actual_engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "username": normalized_username,
                "ts_code": normalized_code,
                "security_name": str(security_name or "").strip()[:120],
                "industry": str(industry or "").strip()[:120],
                "tags": merged_tags,
                "note": str(note or "").strip(),
                "source_file": str(source_file or "").strip()[:255],
            },
        )
    return status


def update_stock_pool_item_metadata(
    username: str,
    ts_code: str,
    *,
    security_name: str | None = None,
    industry: str | None = None,
    tags: str | list[str] | tuple[str, ...] | None = None,
    note: str | None = None,
    engine: Engine | None = None,
) -> int:
    normalized_username = normalize_username(username)
    normalized_code = normalize_stock_ts_code(ts_code)
    if not normalized_username or not normalized_code:
        return 0

    actual_engine = engine or get_engine()
    ensure_user_stock_pool_table(actual_engine)

    sql = f"""
    UPDATE {TABLE_NAME}
    SET
        security_name = CASE WHEN :security_name IS NULL THEN security_name ELSE :security_name END,
        industry = CASE WHEN :industry IS NULL THEN industry ELSE :industry END,
        tags = CASE WHEN :tags IS NULL THEN tags ELSE :tags END,
        note = CASE WHEN :note IS NULL THEN note ELSE :note END,
        updated_at = NOW()
    WHERE username = :username
      AND ts_code = :ts_code
    """
    params = {
        "username": normalized_username,
        "ts_code": normalized_code,
        "security_name": None if security_name is None else str(security_name or "").strip()[:120],
        "industry": None if industry is None else str(industry or "").strip()[:120],
        "tags": None if tags is None else format_tags(tags),
        "note": None if note is None else str(note or "").strip(),
    }
    with actual_engine.begin() as conn:
        result = conn.execute(text(sql), params)
    return int(result.rowcount or 0)


def remove_stock_pool_items_batch(
    username: str,
    ts_codes: list[str] | tuple[str, ...],
    engine: Engine | None = None,
) -> int:
    normalized_username = normalize_username(username)
    normalized_codes = [normalize_stock_ts_code(code) for code in ts_codes or []]
    normalized_codes = [code for code in normalized_codes if code]
    if not normalized_username or not normalized_codes:
        return 0

    actual_engine = engine or get_engine()
    ensure_user_stock_pool_table(actual_engine)

    total_deleted = 0
    sql = f"""
    DELETE FROM {TABLE_NAME}
    WHERE username = :username AND ts_code = :ts_code
    """
    with actual_engine.begin() as conn:
        for code in normalized_codes:
            result = conn.execute(text(sql), {"username": normalized_username, "ts_code": code})
            total_deleted += int(result.rowcount or 0)
    return total_deleted


def import_stock_pool_rows(
    username: str,
    rows: list[dict],
    *,
    default_tags: str | list[str] | tuple[str, ...] = "",
    source_file: str = "",
    upsert_item=upsert_stock_pool_item,
) -> dict:
    normalized_username = normalize_username(username)
    if not normalized_username:
        raise ValueError("username cannot be empty")

    added_codes: list[str] = []
    updated_codes: list[str] = []
    skipped_invalid = 0
    failed_items: list[str] = []

    for row in rows or []:
        row = row or {}
        ts_code = normalize_stock_ts_code(row.get("ts_code") or row.get("代码"))
        if not ts_code:
            skipped_invalid += 1
            continue

        security_name = str(
            row.get("security_name")
            or row.get("name")
            or row.get("名称")
            or row.get("简称")
            or ts_code
            or ""
        ).strip()
        industry = str(row.get("industry") or row.get("行业") or "").strip()
        tags = merge_tag_values(row.get("tags") or row.get("标签") or "", default_tags)
        note = str(row.get("note") or row.get("备注") or "").strip()

        try:
            status = upsert_item(
                normalized_username,
                ts_code,
                security_name=security_name or ts_code,
                industry=industry,
                tags=tags,
                note=note,
                source_file=source_file,
            )
        except Exception as exc:
            failed_items.append(f"{ts_code}: {exc}")
            continue

        if status == "inserted":
            added_codes.append(ts_code)
        else:
            updated_codes.append(ts_code)

    return {
        "parsed": len(rows or []),
        "added": len(added_codes),
        "added_codes": added_codes,
        "updated": len(updated_codes),
        "updated_codes": updated_codes,
        "skipped_invalid": skipped_invalid,
        "failed": len(failed_items),
        "failed_items": failed_items,
    }
