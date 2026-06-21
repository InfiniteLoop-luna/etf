from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from src.sync_tushare_security_data import build_db_url

logger = logging.getLogger(__name__)

TEMPLATE_REPORT_TABLE = "ts_stock_analysis_template_reports"
TEMPLATE_REPORT_VERSION = "stock-analysis-template-html-v5"


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


def today_report_date() -> str:
    return date.today().isoformat()


def _normalize_ts_code(ts_code: str | None) -> str:
    return str(ts_code or "").strip().upper()


def _normalize_date(value: str | None) -> str:
    text_value = str(value or "").strip().replace("/", "-")
    if not text_value:
        return ""
    if len(text_value) == 8 and text_value.isdigit():
        return f"{text_value[:4]}-{text_value[4:6]}-{text_value[6:]}"
    return text_value[:10]


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


def _column_exists(engine: Engine, table_name: str, column_name: str) -> bool:
    try:
        inspector = inspect(engine)
        return any(str(column.get("name") or "") == column_name for column in inspector.get_columns(table_name))
    except Exception:
        return False


def _ensure_template_report_table_schema(engine: Engine) -> None:
    patch_sql: list[str] = []
    for column_name, ddl in [
        ("stock_name", "TEXT"),
        ("asof_trade_date", "VARCHAR(20)"),
        ("fact_pack_json", "TEXT"),
        ("llm_result_json", "TEXT"),
        ("report_version", f"VARCHAR(32) NOT NULL DEFAULT '{TEMPLATE_REPORT_VERSION}'"),
    ]:
        if not _column_exists(engine, TEMPLATE_REPORT_TABLE, column_name):
            patch_sql.append(f"ALTER TABLE {TEMPLATE_REPORT_TABLE} ADD COLUMN {column_name} {ddl}")
    if not patch_sql:
        return
    with engine.begin() as conn:
        for stmt in patch_sql:
            conn.execute(text(stmt))


def ensure_template_report_table(engine: Engine) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TEMPLATE_REPORT_TABLE} (
        ts_code VARCHAR(20) NOT NULL,
        report_date VARCHAR(20) NOT NULL,
        report_version VARCHAR(32) NOT NULL DEFAULT '{TEMPLATE_REPORT_VERSION}',
        stock_name TEXT,
        asof_trade_date VARCHAR(20),
        report_html TEXT NOT NULL,
        fact_pack_json TEXT,
        llm_result_json TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (ts_code, report_date, report_version)
    );
    CREATE INDEX IF NOT EXISTS idx_{TEMPLATE_REPORT_TABLE}_created_at
        ON {TEMPLATE_REPORT_TABLE} (created_at DESC);
    """
    with engine.begin() as conn:
        for stmt in [item.strip() for item in sql.split(";") if item.strip()]:
            conn.execute(text(stmt))
    _ensure_template_report_table_schema(engine)


def get_cached_template_report(
    engine: Engine,
    ts_code: str,
    report_date: str | None = None,
    *,
    report_version: str = TEMPLATE_REPORT_VERSION,
) -> dict[str, Any] | None:
    try:
        ensure_template_report_table(engine)
        sql = text(
            f"""
            SELECT ts_code, report_date, report_version, stock_name, asof_trade_date,
                   report_html, fact_pack_json, llm_result_json, created_at
            FROM {TEMPLATE_REPORT_TABLE}
            WHERE ts_code = :ts_code
              AND report_date = :report_date
              AND report_version = :report_version
            LIMIT 1
            """
        )
        with engine.connect() as conn:
            row = conn.execute(
                sql,
                {
                    "ts_code": _normalize_ts_code(ts_code),
                    "report_date": _normalize_date(report_date) or today_report_date(),
                    "report_version": str(report_version or TEMPLATE_REPORT_VERSION),
                },
            ).mappings().first()
        if not row:
            return None
        record = dict(row)
        record["fact_pack"] = _json_loads(record.pop("fact_pack_json", None))
        record["llm_result"] = _json_loads(record.pop("llm_result_json", None))
        return record
    except Exception as exc:
        logger.warning("Failed to load cached template report for %s: %s", ts_code, exc)
        return None


def save_template_report(
    engine: Engine,
    ts_code: str,
    report_html: str,
    *,
    stock_name: str = "",
    report_date: str | None = None,
    asof_trade_date: str | None = None,
    fact_pack: dict[str, Any] | None = None,
    llm_result: dict[str, Any] | None = None,
    report_version: str = TEMPLATE_REPORT_VERSION,
) -> None:
    ensure_template_report_table(engine)
    sql = text(
        f"""
        INSERT INTO {TEMPLATE_REPORT_TABLE} (
            ts_code,
            report_date,
            report_version,
            stock_name,
            asof_trade_date,
            report_html,
            fact_pack_json,
            llm_result_json,
            created_at
        )
        VALUES (
            :ts_code,
            :report_date,
            :report_version,
            :stock_name,
            :asof_trade_date,
            :report_html,
            :fact_pack_json,
            :llm_result_json,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (ts_code, report_date, report_version) DO UPDATE
        SET stock_name = EXCLUDED.stock_name,
            asof_trade_date = EXCLUDED.asof_trade_date,
            report_html = EXCLUDED.report_html,
            fact_pack_json = EXCLUDED.fact_pack_json,
            llm_result_json = EXCLUDED.llm_result_json,
            created_at = CURRENT_TIMESTAMP
        """
    )
    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "ts_code": _normalize_ts_code(ts_code),
                "report_date": _normalize_date(report_date) or today_report_date(),
                "report_version": str(report_version or TEMPLATE_REPORT_VERSION),
                "stock_name": str(stock_name or ""),
                "asof_trade_date": _normalize_date(asof_trade_date),
                "report_html": str(report_html or ""),
                "fact_pack_json": _json_dumps(fact_pack),
                "llm_result_json": _json_dumps(llm_result),
            },
        )
