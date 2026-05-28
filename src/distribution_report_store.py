from __future__ import annotations

import io
import logging
from datetime import datetime, timedelta, timezone
import pandas as pd
from sqlalchemy import bindparam, create_engine, inspect, text
from sqlalchemy.engine import Engine

from src.sync_tushare_security_data import build_db_url

logger = logging.getLogger(__name__)

REPORT_TABLE = "ts_distribution_reports"
REPORT_STATUS_TABLE = "ts_distribution_report_status"
TICKS_TABLE = "ts_stock_ticks_compressed"
REFRESH_LOCK_TABLE = "ts_distribution_refresh_locks"


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


def _column_exists(engine: Engine, table_name: str, column_name: str) -> bool:
    try:
        inspector = inspect(engine)
        return any(str(column.get("name") or "") == column_name for column in inspector.get_columns(table_name))
    except Exception:
        return False


def _ensure_report_table_schema(engine: Engine):
    patch_sql: list[str] = []
    if not _column_exists(engine, REPORT_TABLE, "source_updated_at"):
        patch_sql.append(f"ALTER TABLE {REPORT_TABLE} ADD COLUMN source_updated_at TIMESTAMPTZ")
    if not _column_exists(engine, REPORT_TABLE, "report_version"):
        patch_sql.append(f"ALTER TABLE {REPORT_TABLE} ADD COLUMN report_version VARCHAR(32) NOT NULL DEFAULT 'v1'")

    if patch_sql:
        with engine.begin() as conn:
            for stmt in patch_sql:
                conn.execute(text(stmt))


def ensure_tables(engine: Engine):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {REPORT_TABLE} (
        ts_code VARCHAR(20) NOT NULL,
        trade_date VARCHAR(20) NOT NULL,
        report_md TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        source_updated_at TIMESTAMPTZ,
        report_version VARCHAR(32) NOT NULL DEFAULT 'v1',
        PRIMARY KEY (ts_code, trade_date)
    );

    CREATE TABLE IF NOT EXISTS {REPORT_STATUS_TABLE} (
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

    CREATE INDEX IF NOT EXISTS idx_{REPORT_STATUS_TABLE}_status
        ON {REPORT_STATUS_TABLE} (status);
    CREATE INDEX IF NOT EXISTS idx_{REPORT_STATUS_TABLE}_latest_ready_trade_date
        ON {REPORT_STATUS_TABLE} (latest_ready_trade_date DESC);

    CREATE TABLE IF NOT EXISTS {TICKS_TABLE} (
        ts_code VARCHAR(20) NOT NULL,
        trade_date VARCHAR(20) NOT NULL,
        parquet_data BYTEA NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (ts_code, trade_date)
    );

    CREATE TABLE IF NOT EXISTS {REFRESH_LOCK_TABLE} (
        lock_name VARCHAR(64) PRIMARY KEY,
        owner_id VARCHAR(64) NOT NULL,
        acquired_at VARCHAR(40) NOT NULL,
        expires_at VARCHAR(40) NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_{REFRESH_LOCK_TABLE}_expires_at
        ON {REFRESH_LOCK_TABLE} (expires_at);
    """
    with engine.begin() as conn:
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(stmt))

    _ensure_report_table_schema(engine)


def _normalize_trade_date(trade_date: str | None) -> str:
    trade_date_text = str(trade_date or "").strip()
    if not trade_date_text:
        return ""
    trade_date_text = trade_date_text.replace("/", "-")
    if len(trade_date_text) == 8 and trade_date_text.isdigit():
        return f"{trade_date_text[:4]}-{trade_date_text[4:6]}-{trade_date_text[6:]}"
    return trade_date_text[:10]


def _compact_trade_date(trade_date: str | None) -> str:
    normalized = _normalize_trade_date(trade_date)
    return normalized.replace("-", "") if normalized else ""


def _normalize_ts_code(ts_code: str | None) -> str:
    return str(ts_code or "").strip().upper()


def _utc_iso(value: datetime | None = None) -> str:
    current = (value or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return current.isoformat(timespec="seconds")


def try_acquire_refresh_lock(
    engine: Engine,
    lock_name: str,
    *,
    owner_id: str,
    timeout_seconds: int = 1800,
    now: datetime | None = None,
) -> bool:
    try:
        ensure_tables(engine)
        now_dt = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        now_iso = _utc_iso(now_dt)
        expires_iso = _utc_iso(now_dt + timedelta(seconds=max(int(timeout_seconds or 0), 1)))
        delete_sql = text(
            f"DELETE FROM {REFRESH_LOCK_TABLE} WHERE lock_name = :lock_name AND expires_at < :now_iso"
        )
        insert_sql = text(
            f"""
            INSERT INTO {REFRESH_LOCK_TABLE} (lock_name, owner_id, acquired_at, expires_at)
            VALUES (:lock_name, :owner_id, :acquired_at, :expires_at)
            ON CONFLICT (lock_name) DO NOTHING
            """
        )
        with engine.begin() as conn:
            conn.execute(delete_sql, {"lock_name": lock_name, "now_iso": now_iso})
            result = conn.execute(
                insert_sql,
                {
                    "lock_name": lock_name,
                    "owner_id": owner_id,
                    "acquired_at": now_iso,
                    "expires_at": expires_iso,
                },
            )
        return bool(getattr(result, "rowcount", 0))
    except Exception as exc:
        logger.warning("Failed to acquire refresh lock %s: %s", lock_name, exc)
        return False


def release_refresh_lock(engine: Engine, lock_name: str, *, owner_id: str) -> None:
    try:
        ensure_tables(engine)
        sql = text(f"DELETE FROM {REFRESH_LOCK_TABLE} WHERE lock_name = :lock_name AND owner_id = :owner_id")
        with engine.begin() as conn:
            conn.execute(sql, {"lock_name": lock_name, "owner_id": owner_id})
    except Exception as exc:
        logger.warning("Failed to release refresh lock %s: %s", lock_name, exc)


def get_daily_report(engine: Engine, ts_code: str, trade_date: str) -> str | None:
    try:
        ensure_tables(engine)
        trade_date_key = _normalize_trade_date(trade_date)
        sql = text(f"SELECT report_md FROM {REPORT_TABLE} WHERE ts_code = :ts AND trade_date = :td")
        with engine.connect() as conn:
            result = conn.execute(sql, {"ts": ts_code, "td": trade_date_key}).fetchone()
            if result:
                return result[0]
    except Exception as e:
        logger.error(f"Failed to fetch report cache for {ts_code}: {e}")
    return None


def save_daily_report(
    engine: Engine,
    ts_code: str,
    trade_date: str,
    report_md: str,
    source_updated_at=None,
):
    try:
        ensure_tables(engine)
        trade_date_key = _normalize_trade_date(trade_date)
        sql = text(
            f"""
            INSERT INTO {REPORT_TABLE} (ts_code, trade_date, report_md, source_updated_at, report_version)
            VALUES (:ts, :td, :md, :source_updated_at, :report_version)
            ON CONFLICT (ts_code, trade_date) DO UPDATE
            SET report_md = EXCLUDED.report_md,
                source_updated_at = COALESCE(EXCLUDED.source_updated_at, {REPORT_TABLE}.source_updated_at),
                created_at = CURRENT_TIMESTAMP,
                report_version = EXCLUDED.report_version
            """
        )
        with engine.begin() as conn:
            conn.execute(
                sql,
                {
                    "ts": ts_code,
                    "td": trade_date_key,
                    "md": report_md,
                    "source_updated_at": source_updated_at,
                    "report_version": "v1",
                },
            )
    except Exception as e:
        logger.error(f"Failed to save report cache for {ts_code}: {e}")
        raise


def get_latest_report_record(engine: Engine, ts_code: str) -> dict | None:
    try:
        ensure_tables(engine)
        sql = text(
            f"""
            SELECT ts_code, trade_date, report_md, created_at, source_updated_at, report_version
            FROM {REPORT_TABLE}
            WHERE ts_code = :ts
            ORDER BY trade_date DESC, created_at DESC
            LIMIT 1
            """
        )
        with engine.connect() as conn:
            row = conn.execute(sql, {"ts": ts_code}).mappings().first()
        if not row:
            return None
        return dict(row)
    except Exception as exc:
        logger.error(f"Failed to read latest report record for {ts_code}: {exc}")
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


def _hydrate_status_with_latest_report(
    ts_code: str,
    status_row: dict | None,
    latest_report: dict | None,
) -> dict | None:
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
            SELECT
                ts_code,
                status,
                target_trade_date,
                latest_ready_trade_date,
                latest_report_generated_at,
                last_attempt_at,
                last_success_at,
                duration_ms,
                error_message,
                updated_at
            FROM {REPORT_STATUS_TABLE}
            WHERE ts_code = :ts
            """
        )
        with engine.connect() as conn:
            row = conn.execute(sql, {"ts": ts_code_key}).mappings().first()
        latest_report = None if row and _normalize_trade_date(row.get("latest_ready_trade_date")) else get_latest_report_record(engine, ts_code_key)
        return _hydrate_status_with_latest_report(ts_code_key, dict(row) if row else None, latest_report)
    except Exception as exc:
        logger.error(f"Failed to fetch report status for {ts_code}: {exc}")
        return None


def get_report_statuses(engine: Engine, ts_codes: list[str] | tuple[str, ...]) -> dict[str, dict]:
    normalized_codes = sorted({_normalize_ts_code(code) for code in ts_codes if _normalize_ts_code(code)})
    if not normalized_codes:
        return {}

    try:
        ensure_tables(engine)
        status_sql = text(
            f"""
            SELECT
                ts_code,
                status,
                target_trade_date,
                latest_ready_trade_date,
                latest_report_generated_at,
                last_attempt_at,
                last_success_at,
                duration_ms,
                error_message,
                updated_at
            FROM {REPORT_STATUS_TABLE}
            WHERE ts_code IN :ts_codes
            """
        ).bindparams(bindparam("ts_codes", expanding=True))
        latest_report_sql = text(
            f"""
            SELECT ts_code, trade_date, created_at
            FROM (
                SELECT
                    ts_code,
                    trade_date,
                    created_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY ts_code
                        ORDER BY trade_date DESC, created_at DESC
                    ) AS row_number
                FROM {REPORT_TABLE}
                WHERE ts_code IN :ts_codes
            ) ranked_reports
            WHERE row_number = 1
            """
        ).bindparams(bindparam("ts_codes", expanding=True))

        with engine.connect() as conn:
            status_rows = conn.execute(status_sql, {"ts_codes": normalized_codes}).mappings().all()
            latest_report_rows = conn.execute(latest_report_sql, {"ts_codes": normalized_codes}).mappings().all()

        statuses = {str(row.get("ts_code") or "").upper(): dict(row) for row in status_rows}
        latest_reports = {str(row.get("ts_code") or "").upper(): dict(row) for row in latest_report_rows}

        hydrated: dict[str, dict] = {}
        for ts_code in normalized_codes:
            status = _hydrate_status_with_latest_report(ts_code, statuses.get(ts_code), latest_reports.get(ts_code))
            if status:
                hydrated[ts_code] = status
        return hydrated
    except Exception as exc:
        logger.error("Failed to fetch report statuses: %s", exc)
        return {}


def upsert_report_status(engine: Engine, ts_code: str, **fields) -> None:
    try:
        ensure_tables(engine)
        payload = {
            "ts_code": ts_code,
            "status": fields.get("status", "idle"),
            "target_trade_date": _normalize_trade_date(fields.get("target_trade_date")),
            "latest_ready_trade_date": _normalize_trade_date(fields.get("latest_ready_trade_date")),
            "latest_report_generated_at": fields.get("latest_report_generated_at"),
            "last_attempt_at": fields.get("last_attempt_at"),
            "last_success_at": fields.get("last_success_at"),
            "duration_ms": fields.get("duration_ms"),
            "error_message": fields.get("error_message"),
            "report_version": fields.get("report_version", "v1"),
        }
        sql = text(
            f"""
            INSERT INTO {REPORT_STATUS_TABLE} (
                ts_code,
                status,
                target_trade_date,
                latest_ready_trade_date,
                latest_report_generated_at,
                last_attempt_at,
                last_success_at,
                duration_ms,
                error_message,
                updated_at
            )
            VALUES (
                :ts_code,
                :status,
                :target_trade_date,
                :latest_ready_trade_date,
                :latest_report_generated_at,
                :last_attempt_at,
                :last_success_at,
                :duration_ms,
                :error_message,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (ts_code) DO UPDATE SET
                status = EXCLUDED.status,
                target_trade_date = EXCLUDED.target_trade_date,
                latest_ready_trade_date = EXCLUDED.latest_ready_trade_date,
                latest_report_generated_at = EXCLUDED.latest_report_generated_at,
                last_attempt_at = EXCLUDED.last_attempt_at,
                last_success_at = EXCLUDED.last_success_at,
                duration_ms = EXCLUDED.duration_ms,
                error_message = EXCLUDED.error_message,
                updated_at = CURRENT_TIMESTAMP
            """
        )
        with engine.begin() as conn:
            conn.execute(sql, payload)
    except Exception as exc:
        logger.error(f"Failed to upsert report status for {ts_code}: {exc}")


def get_latest_ready_report(engine: Engine, ts_code: str) -> dict | None:
    status = get_report_status(engine, ts_code)
    if not status:
        return None

    latest_ready_trade_date = _normalize_trade_date(status.get("latest_ready_trade_date"))
    if not latest_ready_trade_date:
        return status

    report_md = get_daily_report(engine, ts_code, latest_ready_trade_date)
    if report_md:
        status = dict(status)
        status["report_md"] = report_md
    return status


def get_compressed_ticks(engine: Engine, ts_code: str, trade_date: str) -> pd.DataFrame:
    try:
        ensure_tables(engine)
        trade_date_key = _normalize_trade_date(trade_date)
        compact_trade_date_key = _compact_trade_date(trade_date)
        sql = text(
            f"""
            SELECT parquet_data
            FROM {TICKS_TABLE}
            WHERE ts_code = :ts
              AND (trade_date = :td OR trade_date = :compact_td)
            ORDER BY CASE WHEN trade_date = :td THEN 0 ELSE 1 END, created_at DESC
            LIMIT 1
            """
        )
        with engine.connect() as conn:
            result = conn.execute(
                sql,
                {"ts": ts_code, "td": trade_date_key, "compact_td": compact_trade_date_key},
            ).fetchone()
            if result and result[0]:
                buf = io.BytesIO(result[0])
                df = pd.read_pickle(buf, compression='gzip')
                return df
    except Exception as e:
        logger.error(f"Failed to read pickle ticks for {ts_code} on {trade_date}: {e}")
    return None


def save_compressed_ticks(engine: Engine, ts_code: str, trade_date: str, df: pd.DataFrame):
    if df is None or df.empty:
        return
    try:
        ensure_tables(engine)
        trade_date_key = _normalize_trade_date(trade_date)
        buf = io.BytesIO()
        df.to_pickle(buf, compression='gzip')
        binary_data = buf.getvalue()

        sql = text(f"""
            INSERT INTO {TICKS_TABLE} (ts_code, trade_date, parquet_data)
            VALUES (:ts, :td, :data)
            ON CONFLICT (ts_code, trade_date) DO NOTHING
        """)
        with engine.begin() as conn:
            conn.execute(sql, {"ts": ts_code, "td": trade_date_key, "data": binary_data})
    except Exception as e:
        logger.error(f"Failed to save compressed ticks for {ts_code} on {trade_date}: {e}")
