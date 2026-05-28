from __future__ import annotations

import time
import uuid
from datetime import datetime
from typing import Callable

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.distribution_analyzer import generate_detailed_report_markdown
from src.distribution_llm_analysis import should_require_llm_refresh
from src.distribution_report_store import (
    ensure_tables,
    get_daily_report,
    get_report_status,
    release_refresh_lock,
    save_daily_report,
    try_acquire_refresh_lock,
    upsert_report_status,
)


WATCHLIST_REFRESH_LOCK_NAME = "watchlist_distribution_refresh"


def load_watchlist_stock_symbols(engine: Engine, username: str | None = None) -> list[str]:
    params: dict[str, str] = {}
    username_filter = ""
    normalized_username = str(username or "").strip()
    if normalized_username:
        username_filter = " AND username = :username"
        params["username"] = normalized_username
    sql = text(
        f"""
        SELECT DISTINCT ts_code
        FROM app_user_watchlist
        WHERE LOWER(COALESCE(security_type, 'stock')) = 'stock'{username_filter}
        ORDER BY ts_code
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [str(row[0]).strip().upper() for row in rows if row and str(row[0]).strip()]


def load_watchlist_stock_names(engine: Engine, username: str | None = None) -> dict[str, str]:
    params: dict[str, str] = {}
    username_filter = ""
    normalized_username = str(username or "").strip()
    if normalized_username:
        username_filter = " AND username = :username"
        params["username"] = normalized_username
    sql = text(
        f"""
        SELECT
            ts_code,
            MAX(COALESCE(NULLIF(security_name, ''), ts_code)) AS security_name
        FROM app_user_watchlist
        WHERE LOWER(COALESCE(security_type, 'stock')) = 'stock'{username_filter}
        GROUP BY ts_code
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return {
        str(row[0]).strip().upper(): str(row[1] or row[0]).strip()
        for row in rows
        if row and str(row[0]).strip()
    }


def get_latest_source_trade_date(engine: Engine, ts_code: str) -> str | None:
    sql = text(
        """
        SELECT MAX(trade_date)
        FROM vw_ts_stock_daily
        WHERE ts_code = :ts_code
        """
    )
    with engine.connect() as conn:
        value = conn.execute(sql, {"ts_code": ts_code}).scalar()
    if value is None:
        return None
    if isinstance(value, str):
        return value[:10]
    return pd.to_datetime(value, errors="coerce").strftime("%Y-%m-%d")


def refresh_watchlist_distribution_reports(
    engine: Engine,
    report_generator: Callable[..., str] | None = None,
    *,
    username: str | None = None,
) -> dict[str, int]:
    ensure_tables(engine)
    report_generator = report_generator or generate_detailed_report_markdown
    scope_username = str(username or "").strip() or None
    owner_id = f"watchlist-refresh-{uuid.uuid4().hex[:12]}"
    lock_name = WATCHLIST_REFRESH_LOCK_NAME if scope_username is None else f"{WATCHLIST_REFRESH_LOCK_NAME}:{scope_username}"
    if not try_acquire_refresh_lock(engine, lock_name, owner_id=owner_id, timeout_seconds=1800):
        return {"processed": 0, "generated": 0, "skipped": 0, "failed": 0, "locked": 1}

    symbols = load_watchlist_stock_symbols(engine, username=scope_username)
    names = load_watchlist_stock_names(engine, username=scope_username)
    summary = {"processed": 0, "generated": 0, "skipped": 0, "failed": 0, "locked": 0}

    try:
        for ts_code in symbols:
            summary["processed"] += 1
            stock_name = names.get(ts_code, ts_code)
            latest_source_trade_date = get_latest_source_trade_date(engine, ts_code)
            if not latest_source_trade_date:
                upsert_report_status(
                    engine,
                    ts_code,
                    status="failed",
                    error_message="missing source trade date",
                    target_trade_date=None,
                    last_attempt_at=datetime.now(),
                )
                summary["failed"] += 1
                continue

            current_status = get_report_status(engine, ts_code) or {}
            cached_report = get_daily_report(engine, ts_code, latest_source_trade_date)
            has_cached_report = bool(cached_report)
            cache_needs_llm_refresh = has_cached_report and should_require_llm_refresh(cached_report)
            if (
                current_status.get("status") == "ready"
                and current_status.get("latest_ready_trade_date") == latest_source_trade_date
                and has_cached_report
                and not cache_needs_llm_refresh
            ):
                upsert_report_status(
                    engine,
                    ts_code,
                    status="ready",
                    target_trade_date=latest_source_trade_date,
                    latest_ready_trade_date=latest_source_trade_date,
                    last_attempt_at=datetime.now(),
                    last_success_at=current_status.get("last_success_at") or datetime.now(),
                    latest_report_generated_at=current_status.get("latest_report_generated_at") or datetime.now(),
                    error_message=None,
                )
                summary["skipped"] += 1
                continue

            started_at = time.time()
            upsert_report_status(
                engine,
                ts_code,
                status="running",
                target_trade_date=latest_source_trade_date,
                latest_ready_trade_date=current_status.get("latest_ready_trade_date"),
                last_attempt_at=datetime.now(),
                error_message=None,
            )
            try:
                report_md = report_generator(
                    ts_code,
                    stock_name,
                    engine=engine,
                    asof_trade_date=latest_source_trade_date,
                    allow_live_fetch=False,
                    use_report_cache=False,
                    save_report=False,
                )
                save_daily_report(engine, ts_code, latest_source_trade_date, report_md)
                duration_ms = int((time.time() - started_at) * 1000)
                now = datetime.now()
                upsert_report_status(
                    engine,
                    ts_code,
                    status="ready",
                    target_trade_date=latest_source_trade_date,
                    latest_ready_trade_date=latest_source_trade_date,
                    latest_report_generated_at=now,
                    last_attempt_at=now,
                    last_success_at=now,
                    duration_ms=duration_ms,
                    error_message=None,
                )
                summary["generated"] += 1
            except Exception as exc:
                duration_ms = int((time.time() - started_at) * 1000)
                upsert_report_status(
                    engine,
                    ts_code,
                    status="failed",
                    target_trade_date=latest_source_trade_date,
                    latest_ready_trade_date=current_status.get("latest_ready_trade_date"),
                    last_attempt_at=datetime.now(),
                    duration_ms=duration_ms,
                    error_message=str(exc),
                )
                summary["failed"] += 1
    finally:
        release_refresh_lock(engine, lock_name, owner_id=owner_id)

    return summary
