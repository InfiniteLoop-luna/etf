from __future__ import annotations

import io
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.sync_tushare_security_data import build_db_url

logger = logging.getLogger(__name__)

REPORT_TABLE = "ts_distribution_reports"
REPORT_STATUS_TABLE = "ts_distribution_report_status"
TICKS_TABLE = "ts_stock_ticks_compressed"


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


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
    """
    with engine.begin() as conn:
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(stmt))


def _normalize_trade_date(trade_date: str | None) -> str:
    trade_date_text = str(trade_date or "").strip()
    if not trade_date_text:
        return ""
    trade_date_text = trade_date_text.replace("/", "-")
    if len(trade_date_text) == 8 and trade_date_text.isdigit():
        return f"{trade_date_text[:4]}-{trade_date_text[4:6]}-{trade_date_text[6:]}"
    return trade_date_text[:10]


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


def get_report_status(engine: Engine, ts_code: str) -> dict | None:
    try:
        ensure_tables(engine)
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
            row = conn.execute(sql, {"ts": ts_code}).mappings().first()
        if row:
            status_row = dict(row)
            ready_trade_date = _normalize_trade_date(status_row.get("latest_ready_trade_date"))
            if not ready_trade_date:
                latest_report = get_latest_report_record(engine, ts_code)
                if latest_report:
                    ready_trade_date = _normalize_trade_date(latest_report.get("trade_date"))
                    status_row.setdefault("status", "ready")
                    status_row["latest_ready_trade_date"] = ready_trade_date
                    status_row["latest_report_generated_at"] = latest_report.get("created_at")
                    status_row["last_success_at"] = latest_report.get("created_at")
                    status_row["target_trade_date"] = status_row.get("target_trade_date") or ready_trade_date
            return status_row

        latest_report = get_latest_report_record(engine, ts_code)
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
    except Exception as exc:
        logger.error(f"Failed to fetch report status for {ts_code}: {exc}")
        return None


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
        sql = text(f"SELECT parquet_data FROM {TICKS_TABLE} WHERE ts_code = :ts AND trade_date = :td")
        with engine.connect() as conn:
            result = conn.execute(sql, {"ts": ts_code, "td": trade_date_key}).fetchone()
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
