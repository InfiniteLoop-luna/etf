# -*- coding: utf-8 -*-
"""Tushare lhb top_list/top_inst sync helpers."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.lhb_monitor import TOP_INST_FIELDS, TOP_LIST_FIELDS, load_lhb_trade_dates, resolve_lhb_date_window
from src.volume_fetcher import _init_tushare

LHB_TABLES = {
    "top_list": "ts_lhb_top_list",
    "top_inst": "ts_lhb_top_inst",
}

DEFAULT_API_SLEEP = float(os.getenv("TUSHARE_LHB_API_SLEEP", "0.35"))
DEFAULT_LOOKBACK_DAYS = int(os.getenv("TUSHARE_LHB_LOOKBACK_DAYS", "2"))
DEFAULT_BATCH_DAYS = int(os.getenv("TUSHARE_LHB_BATCH_DAYS", "3"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def build_db_url():
    from src.sync_tushare_security_data import build_db_url as _sync_build_db_url

    return _sync_build_db_url()


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


def current_year_start(today: Optional[date | datetime] = None) -> str:
    today_value = today.date() if isinstance(today, datetime) else (today or date.today())
    return date(today_value.year, 1, 1).strftime("%Y%m%d")


def ensure_landing_table(engine: Engine, table_name: str):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        business_key TEXT PRIMARY KEY,
        dataset_name VARCHAR(64) NOT NULL,
        ts_code VARCHAR(20),
        trade_date DATE,
        ann_date DATE,
        end_date DATE,
        period VARCHAR(20),
        record_hash VARCHAR(64) NOT NULL,
        payload JSONB NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_ts_code ON {table_name}(ts_code);
    """
    with engine.begin() as conn:
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(stmt))


def ensure_all_tables(engine: Engine):
    for table_name in LHB_TABLES.values():
        ensure_landing_table(engine, table_name)


def compute_record_hash(payload: dict) -> str:
    content = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(content.encode()).hexdigest()[:64]


def make_business_key(dataset_name: str, ts_code: Optional[str], trade_date_raw: Optional[str], extra: Optional[str] = None) -> str:
    parts = [dataset_name]
    if ts_code:
        parts.append(str(ts_code))
    if trade_date_raw:
        parts.append(str(trade_date_raw))
    if extra:
        parts.append(str(extra))
    return "|".join(parts)


def _parse_trade_date(raw) -> Optional[date]:
    if raw is None:
        return None
    text_value = str(raw).replace("-", "").strip()
    if len(text_value) >= 8 and text_value[:8].isdigit():
        return datetime.strptime(text_value[:8], "%Y%m%d").date()
    return None


def get_max_trade_date(engine: Engine, table_name: str) -> Optional[str]:
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT MAX(trade_date) FROM {table_name}")).fetchone()
    if row and row[0]:
        return row[0].strftime("%Y%m%d")
    return None


def resolve_start_date(
    engine: Engine,
    table_name: str,
    force_start: Optional[str] = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    today: Optional[date | datetime] = None,
) -> str:
    year_start = current_year_start(today)
    if force_start:
        return max(str(force_start).replace("-", "")[:8], year_start)

    existing_max = get_max_trade_date(engine, table_name)
    if not existing_max:
        return year_start

    lookback = max(0, int(lookback_days or 0))
    start_day = (datetime.strptime(existing_max, "%Y%m%d") - timedelta(days=lookback)).strftime("%Y%m%d")
    return max(year_start, start_day)


def upsert_rows(
    engine: Engine,
    table_name: str,
    dataset_name: str,
    rows: list[dict],
    ts_code_key: str = "ts_code",
    trade_date_key: str = "trade_date",
    extra_key: Optional[str] = None,
) -> int:
    if not rows:
        return 0

    records = []
    for row in rows:
        payload = {}
        for key, value in row.items():
            if isinstance(value, float) and pd.isna(value):
                payload[key] = None
            elif hasattr(value, "item"):
                payload[key] = value.item()
            else:
                payload[key] = value

        ts_code = payload.get(ts_code_key)
        trade_date_raw = payload.get(trade_date_key)
        extra_val = payload.get(extra_key) if extra_key else None
        trade_date_val = _parse_trade_date(trade_date_raw)

        records.append(
            {
                "business_key": make_business_key(dataset_name, ts_code, trade_date_raw, extra=extra_val),
                "dataset_name": dataset_name,
                "ts_code": ts_code,
                "trade_date": trade_date_val,
                "ann_date": None,
                "end_date": None,
                "period": None,
                "record_hash": compute_record_hash(payload),
                "payload": json.dumps(payload, ensure_ascii=False, default=str),
            }
        )

    sql = f"""
    INSERT INTO {table_name} (
        business_key, dataset_name, ts_code, trade_date,
        ann_date, end_date, period, record_hash, payload, ingested_at
    ) VALUES (
        :business_key, :dataset_name, :ts_code, :trade_date,
        :ann_date, :end_date, :period, :record_hash, CAST(:payload AS jsonb), NOW()
    )
    ON CONFLICT (business_key) DO UPDATE SET
        record_hash = EXCLUDED.record_hash,
        payload = EXCLUDED.payload,
        ingested_at = NOW()
    WHERE {table_name}.record_hash <> EXCLUDED.record_hash
    """
    with engine.begin() as conn:
        conn.execute(text(sql), records)
    return len(records)


def _lhb_extra_key(dataset_name: str, row: dict) -> str:
    if dataset_name == "top_inst":
        return "|".join(
            [
                str(row.get("exalter") or "").strip(),
                str(row.get("side") or "").strip(),
                str(row.get("reason") or "").strip(),
            ]
        )
    return str(row.get("reason") or "").strip()


def sync_lhb_dataset(
    engine: Engine,
    pro,
    dataset_name: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    batch_days: int = DEFAULT_BATCH_DAYS,
    request_sleep_seconds: float = DEFAULT_API_SLEEP,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    today: Optional[date | datetime] = None,
) -> int:
    if dataset_name not in LHB_TABLES:
        raise ValueError(f"Unsupported lhb dataset: {dataset_name}")

    table_name = LHB_TABLES[dataset_name]
    resolved_start = resolve_start_date(engine, table_name, force_start=start_date, lookback_days=lookback_days, today=today)
    resolved_start, resolved_end = resolve_lhb_date_window(resolved_start, end_date, today=today)
    trade_dates = load_lhb_trade_dates(pro, resolved_start, resolved_end)
    if batch_days is not None and int(batch_days) > 0:
        trade_dates = trade_dates[-int(batch_days):]

    fields = TOP_LIST_FIELDS if dataset_name == "top_list" else TOP_INST_FIELDS
    api = getattr(pro, dataset_name)
    sleep_seconds = max(0.0, float(request_sleep_seconds or 0))
    total = 0

    logger.info(
        "%s: fetch %s -> %s, trade_days=%s, batch_days=%s",
        dataset_name,
        resolved_start,
        resolved_end,
        len(trade_dates),
        batch_days,
    )

    for idx, trade_date in enumerate(trade_dates, start=1):
        try:
            df = api(trade_date=trade_date, fields=fields)
            if df is not None and not df.empty:
                rows = []
                for row in df.to_dict("records"):
                    enriched = dict(row)
                    enriched["__lhb_extra_key"] = _lhb_extra_key(dataset_name, enriched)
                    rows.append(enriched)
                written = upsert_rows(engine, table_name, dataset_name, rows, extra_key="__lhb_extra_key")
                total += written
                logger.info("%s[%s] wrote %s rows", dataset_name, trade_date, written)
            else:
                logger.info("%s[%s] returned empty", dataset_name, trade_date)
        except Exception as exc:
            logger.warning("%s[%s] failed: %s", dataset_name, trade_date, exc)

        if sleep_seconds and idx < len(trade_dates):
            time.sleep(sleep_seconds)

    logger.info("%s completed, total wrote %s rows", dataset_name, total)
    return total


def run_sync(
    datasets: Optional[list[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    batch_days: int = DEFAULT_BATCH_DAYS,
    request_sleep_seconds: float = DEFAULT_API_SLEEP,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
):
    target_datasets = datasets or ["top_list", "top_inst"]
    logger.info("=== lhb sync started ===")
    logger.info("datasets: %s", target_datasets)
    logger.info("date range: %s -> %s", start_date or "incremental", end_date or "today")

    engine = get_engine()
    ensure_all_tables(engine)
    pro = _init_tushare()

    results = {}
    for dataset_name in target_datasets:
        results[dataset_name] = sync_lhb_dataset(
            engine,
            pro,
            dataset_name,
            start_date=start_date,
            end_date=end_date,
            batch_days=batch_days,
            request_sleep_seconds=request_sleep_seconds,
            lookback_days=lookback_days,
        )
        if request_sleep_seconds:
            time.sleep(max(0.0, float(request_sleep_seconds)))

    total = sum(results.values()) if results else 0
    logger.info("=== lhb sync completed, total wrote %s rows ===", total)
    return results
