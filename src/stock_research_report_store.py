from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import bindparam, create_engine, inspect, text
from sqlalchemy.engine import Engine

from src.sync_tushare_security_data import build_db_url

logger = logging.getLogger(__name__)

RESEARCH_REPORT_TABLE = "ts_stock_research_reports"
RESEARCH_STATUS_TABLE = "ts_stock_research_report_status"
RESEARCH_REFRESH_LOCK_TABLE = "ts_stock_research_refresh_locks"
RESEARCH_REPORT_VERSION = "v1"


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


def _column_exists(engine: Engine, table_name: str, column_name: str) -> bool:
    try:
        inspector = inspect(engine)
        return any(str(column.get("name") or "") == column_name for column in inspector.get_columns(table_name))
    except Exception:
        return False


def _ensure_report_table_schema(engine: Engine) -> None:
    patch_sql: list[str] = []
    for column_name, ddl in [
        ("fact_pack_json", "TEXT"),
        ("llm_result_json", "TEXT"),
        ("source_updated_at", "TIMESTAMPTZ"),
        ("report_version", f"VARCHAR(32) NOT NULL DEFAULT '{RESEARCH_REPORT_VERSION}'"),
    ]:
        if not _column_exists(engine, RESEARCH_REPORT_TABLE, column_name):
            patch_sql.append(f"ALTER TABLE {RESEARCH_REPORT_TABLE} ADD COLUMN {column_name} {ddl}")
    if not patch_sql:
        return
    with engine.begin() as conn:
        for stmt in patch_sql:
            conn.execute(text(stmt))


def ensure_tables(engine: Engine) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {RESEARCH_REPORT_TABLE} (
        ts_code VARCHAR(20) NOT NULL,
        trade_date VARCHAR(20) NOT NULL,
        report_md TEXT NOT NULL,
        fact_pack_json TEXT,
        llm_result_json TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        source_updated_at TIMESTAMPTZ,
        report_version VARCHAR(32) NOT NULL DEFAULT '{RESEARCH_REPORT_VERSION}',
        PRIMARY KEY (ts_code, trade_date)
    );

    CREATE TABLE IF NOT EXISTS {RESEARCH_STATUS_TABLE} (
        ts_code VARCHAR(20) PRIMARY KEY,
        status VARCHAR(20) NOT NULL DEFAULT 'idle',
        target_trade_date VARCHAR(20),
        latest_ready_trade_date VARCHAR(20),
        latest_report_generated_at TIMESTAMPTZ,
        last_attempt_at TIMESTAMPTZ,
        last_success_at TIMESTAMPTZ,
        duration_ms INTEGER,
        error_message TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_{RESEARCH_STATUS_TABLE}_status
        ON {RESEARCH_STATUS_TABLE} (status);
    CREATE INDEX IF NOT EXISTS idx_{RESEARCH_STATUS_TABLE}_latest_ready_trade_date
        ON {RESEARCH_STATUS_TABLE} (latest_ready_trade_date DESC);

    CREATE TABLE IF NOT EXISTS {RESEARCH_REFRESH_LOCK_TABLE} (
        lock_name VARCHAR(64) PRIMARY KEY,
        owner_id VARCHAR(64) NOT NULL,
        acquired_at VARCHAR(40) NOT NULL,
        expires_at VARCHAR(40) NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_{RESEARCH_REFRESH_LOCK_TABLE}_expires_at
        ON {RESEARCH_REFRESH_LOCK_TABLE} (expires_at);
    """
    with engine.begin() as conn:
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(stmt))
    _ensure_report_table_schema(engine)


def _normalize_trade_date(trade_date: str | None) -> str:
    text_value = str(trade_date or "").strip().replace("/", "-")
    if not text_value:
        return ""
    if len(text_value) == 8 and text_value.isdigit():
        return f"{text_value[:4]}-{text_value[4:6]}-{text_value[6:]}"
    return text_value[:10]


def _normalize_ts_code(ts_code: str | None) -> str:
    return str(ts_code or "").strip().upper()


def _utc_iso(value: datetime | None = None) -> str:
    current = (value or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return current.isoformat(timespec="seconds")


def _json_dumps(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=str)


def _json_loads(value: Any) -> Any:
    if value in {None, ""}:
        return None
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except Exception:
        return None


def try_acquire_refresh_lock(
    engine: Engine,
    lock_name: str,
    *,
    owner_id: str,
    timeout_seconds: int = 3600,
    now: datetime | None = None,
) -> bool:
    try:
        ensure_tables(engine)
        now_dt = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        now_iso = _utc_iso(now_dt)
        expires_iso = _utc_iso(now_dt + timedelta(seconds=max(int(timeout_seconds or 0), 1)))
        with engine.begin() as conn:
            conn.execute(
                text(f"DELETE FROM {RESEARCH_REFRESH_LOCK_TABLE} WHERE lock_name = :lock_name AND expires_at < :now_iso"),
                {"lock_name": lock_name, "now_iso": now_iso},
            )
            result = conn.execute(
                text(
                    f"""
                    INSERT INTO {RESEARCH_REFRESH_LOCK_TABLE} (lock_name, owner_id, acquired_at, expires_at)
                    VALUES (:lock_name, :owner_id, :acquired_at, :expires_at)
                    ON CONFLICT (lock_name) DO NOTHING
                    """
                ),
                {
                    "lock_name": lock_name,
                    "owner_id": owner_id,
                    "acquired_at": now_iso,
                    "expires_at": expires_iso,
                },
            )
        return bool(getattr(result, "rowcount", 0))
    except Exception as exc:
        logger.warning("Failed to acquire stock research refresh lock %s: %s", lock_name, exc)
        return False


def release_refresh_lock(engine: Engine, lock_name: str, *, owner_id: str) -> None:
    try:
        ensure_tables(engine)
        with engine.begin() as conn:
            conn.execute(
                text(f"DELETE FROM {RESEARCH_REFRESH_LOCK_TABLE} WHERE lock_name = :lock_name AND owner_id = :owner_id"),
                {"lock_name": lock_name, "owner_id": owner_id},
            )
    except Exception as exc:
        logger.warning("Failed to release stock research refresh lock %s: %s", lock_name, exc)


def save_daily_report(
    engine: Engine,
    ts_code: str,
    trade_date: str,
    report_md: str,
    *,
    fact_pack: dict[str, Any] | None = None,
    llm_result: dict[str, Any] | None = None,
    source_updated_at=None,
) -> None:
    ensure_tables(engine)
    sql = text(
        f"""
        INSERT INTO {RESEARCH_REPORT_TABLE} (
            ts_code,
            trade_date,
            report_md,
            fact_pack_json,
            llm_result_json,
            source_updated_at,
            report_version
        )
        VALUES (
            :ts_code,
            :trade_date,
            :report_md,
            :fact_pack_json,
            :llm_result_json,
            :source_updated_at,
            :report_version
        )
        ON CONFLICT (ts_code, trade_date) DO UPDATE
        SET report_md = EXCLUDED.report_md,
            fact_pack_json = EXCLUDED.fact_pack_json,
            llm_result_json = EXCLUDED.llm_result_json,
            source_updated_at = COALESCE(EXCLUDED.source_updated_at, {RESEARCH_REPORT_TABLE}.source_updated_at),
            created_at = CURRENT_TIMESTAMP,
            report_version = EXCLUDED.report_version
        """
    )
    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "ts_code": _normalize_ts_code(ts_code),
                "trade_date": _normalize_trade_date(trade_date),
                "report_md": str(report_md or ""),
                "fact_pack_json": _json_dumps(fact_pack),
                "llm_result_json": _json_dumps(llm_result),
                "source_updated_at": source_updated_at,
                "report_version": RESEARCH_REPORT_VERSION,
            },
        )


def get_daily_report(engine: Engine, ts_code: str, trade_date: str) -> str | None:
    try:
        ensure_tables(engine)
        sql = text(f"SELECT report_md FROM {RESEARCH_REPORT_TABLE} WHERE ts_code = :ts_code AND trade_date = :trade_date")
        with engine.connect() as conn:
            row = conn.execute(
                sql,
                {"ts_code": _normalize_ts_code(ts_code), "trade_date": _normalize_trade_date(trade_date)},
            ).fetchone()
        return str(row[0]) if row else None
    except Exception as exc:
        logger.error("Failed to fetch stock research report for %s: %s", ts_code, exc)
        return None


def get_daily_report_record(engine: Engine, ts_code: str, trade_date: str) -> dict | None:
    try:
        ensure_tables(engine)
        sql = text(
            f"""
            SELECT ts_code, trade_date, report_md, fact_pack_json, llm_result_json,
                   created_at, source_updated_at, report_version
            FROM {RESEARCH_REPORT_TABLE}
            WHERE ts_code = :ts_code AND trade_date = :trade_date
            """
        )
        with engine.connect() as conn:
            row = conn.execute(
                sql,
                {"ts_code": _normalize_ts_code(ts_code), "trade_date": _normalize_trade_date(trade_date)},
            ).mappings().first()
        if not row:
            return None
        record = dict(row)
        record["fact_pack"] = _json_loads(record.pop("fact_pack_json", None))
        record["llm_result"] = _json_loads(record.pop("llm_result_json", None))
        return record
    except Exception as exc:
        logger.error("Failed to fetch stock research record for %s: %s", ts_code, exc)
        return None


def get_latest_report_record(engine: Engine, ts_code: str) -> dict | None:
    try:
        ensure_tables(engine)
        sql = text(
            f"""
            SELECT ts_code, trade_date, report_md, created_at, source_updated_at, report_version
            FROM {RESEARCH_REPORT_TABLE}
            WHERE ts_code = :ts_code
            ORDER BY trade_date DESC, created_at DESC
            LIMIT 1
            """
        )
        with engine.connect() as conn:
            row = conn.execute(sql, {"ts_code": _normalize_ts_code(ts_code)}).mappings().first()
        return dict(row) if row else None
    except Exception as exc:
        logger.error("Failed to fetch latest stock research report for %s: %s", ts_code, exc)
        return None


def _build_status_from_latest_report(ts_code: str, latest_report: dict | None) -> dict | None:
    if not latest_report:
        return None
    ready_trade_date = _normalize_trade_date(latest_report.get("trade_date"))
    return {
        "ts_code": ts_code,
        "status": "ready",
        "target_trade_date": ready_trade_date,
        "latest_ready_trade_date": ready_trade_date,
        "latest_report_generated_at": latest_report.get("created_at"),
        "last_attempt_at": latest_report.get("created_at"),
        "last_success_at": latest_report.get("created_at"),
        "duration_ms": None,
        "error_message": None,
        "updated_at": latest_report.get("created_at"),
    }


def _hydrate_status_with_latest_report(ts_code: str, status_row: dict | None, latest_report: dict | None) -> dict | None:
    if not status_row:
        return _build_status_from_latest_report(ts_code, latest_report)

    status = dict(status_row)
    ready_trade_date = _normalize_trade_date(status.get("latest_ready_trade_date"))
    if not ready_trade_date and latest_report:
        ready_trade_date = _normalize_trade_date(latest_report.get("trade_date"))
        status["latest_ready_trade_date"] = ready_trade_date
        status["latest_report_generated_at"] = latest_report.get("created_at")
        status["last_success_at"] = latest_report.get("created_at")
        status["target_trade_date"] = status.get("target_trade_date") or ready_trade_date
        status["status"] = status.get("status") or "ready"
    return status


def get_report_status(engine: Engine, ts_code: str) -> dict | None:
    try:
        ensure_tables(engine)
        ts_code_key = _normalize_ts_code(ts_code)
        sql = text(
            f"""
            SELECT ts_code, status, target_trade_date, latest_ready_trade_date,
                   latest_report_generated_at, last_attempt_at, last_success_at,
                   duration_ms, error_message, updated_at
            FROM {RESEARCH_STATUS_TABLE}
            WHERE ts_code = :ts_code
            """
        )
        with engine.connect() as conn:
            row = conn.execute(sql, {"ts_code": ts_code_key}).mappings().first()
        latest_report = None if row and _normalize_trade_date(row.get("latest_ready_trade_date")) else get_latest_report_record(engine, ts_code_key)
        return _hydrate_status_with_latest_report(ts_code_key, dict(row) if row else None, latest_report)
    except Exception as exc:
        logger.error("Failed to fetch stock research status for %s: %s", ts_code, exc)
        return None


def get_report_statuses(engine: Engine, ts_codes: list[str] | tuple[str, ...]) -> dict[str, dict]:
    normalized_codes = sorted({_normalize_ts_code(code) for code in ts_codes if _normalize_ts_code(code)})
    if not normalized_codes:
        return {}

    try:
        ensure_tables(engine)
        status_sql = text(
            f"""
            SELECT ts_code, status, target_trade_date, latest_ready_trade_date,
                   latest_report_generated_at, last_attempt_at, last_success_at,
                   duration_ms, error_message, updated_at
            FROM {RESEARCH_STATUS_TABLE}
            WHERE ts_code IN :ts_codes
            """
        ).bindparams(bindparam("ts_codes", expanding=True))
        latest_report_sql = text(
            f"""
            SELECT ts_code, trade_date, created_at
            FROM (
                SELECT ts_code, trade_date, created_at,
                       ROW_NUMBER() OVER (
                           PARTITION BY ts_code
                           ORDER BY trade_date DESC, created_at DESC
                       ) AS row_number
                FROM {RESEARCH_REPORT_TABLE}
                WHERE ts_code IN :ts_codes
            ) ranked_reports
            WHERE row_number = 1
            """
        ).bindparams(bindparam("ts_codes", expanding=True))
        with engine.connect() as conn:
            status_rows = conn.execute(status_sql, {"ts_codes": normalized_codes}).mappings().all()
            latest_rows = conn.execute(latest_report_sql, {"ts_codes": normalized_codes}).mappings().all()
        status_map = {str(row["ts_code"]).strip().upper(): dict(row) for row in status_rows}
        latest_map = {str(row["ts_code"]).strip().upper(): dict(row) for row in latest_rows}
        result: dict[str, dict] = {}
        for code in normalized_codes:
            hydrated = _hydrate_status_with_latest_report(code, status_map.get(code), latest_map.get(code))
            if hydrated:
                result[code] = hydrated
        return result
    except Exception as exc:
        logger.error("Failed to fetch stock research statuses: %s", exc)
        return {}


def upsert_report_status(
    engine: Engine,
    ts_code: str,
    *,
    status: str,
    target_trade_date: str | None = None,
    latest_ready_trade_date: str | None = None,
    latest_report_generated_at=None,
    last_attempt_at=None,
    last_success_at=None,
    duration_ms: int | None = None,
    error_message: str | None = None,
) -> None:
    ensure_tables(engine)
    sql = text(
        f"""
        INSERT INTO {RESEARCH_STATUS_TABLE} (
            ts_code, status, target_trade_date, latest_ready_trade_date,
            latest_report_generated_at, last_attempt_at, last_success_at,
            duration_ms, error_message, updated_at
        )
        VALUES (
            :ts_code, :status, :target_trade_date, :latest_ready_trade_date,
            :latest_report_generated_at, :last_attempt_at, :last_success_at,
            :duration_ms, :error_message, CURRENT_TIMESTAMP
        )
        ON CONFLICT (ts_code) DO UPDATE
        SET status = EXCLUDED.status,
            target_trade_date = EXCLUDED.target_trade_date,
            latest_ready_trade_date = COALESCE(EXCLUDED.latest_ready_trade_date, {RESEARCH_STATUS_TABLE}.latest_ready_trade_date),
            latest_report_generated_at = COALESCE(EXCLUDED.latest_report_generated_at, {RESEARCH_STATUS_TABLE}.latest_report_generated_at),
            last_attempt_at = COALESCE(EXCLUDED.last_attempt_at, {RESEARCH_STATUS_TABLE}.last_attempt_at),
            last_success_at = COALESCE(EXCLUDED.last_success_at, {RESEARCH_STATUS_TABLE}.last_success_at),
            duration_ms = COALESCE(EXCLUDED.duration_ms, {RESEARCH_STATUS_TABLE}.duration_ms),
            error_message = EXCLUDED.error_message,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "ts_code": _normalize_ts_code(ts_code),
                "status": str(status or "idle"),
                "target_trade_date": _normalize_trade_date(target_trade_date),
                "latest_ready_trade_date": _normalize_trade_date(latest_ready_trade_date),
                "latest_report_generated_at": latest_report_generated_at,
                "last_attempt_at": last_attempt_at,
                "last_success_at": last_success_at,
                "duration_ms": duration_ms,
                "error_message": error_message,
            },
        )
