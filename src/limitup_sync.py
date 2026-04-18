# -*- coding: utf-8 -*-
"""
打板情绪数据同步模块
覆盖接口：
- limit_list_d
- limit_step
- limit_cpt_list
- kpl_list

存储：PostgreSQL JSONB landing tables
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.volume_fetcher import _init_tushare

DEFAULT_START_DATE = "20240101"
DEFAULT_API_SLEEP = float(os.getenv("TUSHARE_LIMIT_API_SLEEP", "0.25"))
DEFAULT_LOOKBACK_DAYS = int(os.getenv("TUSHARE_LIMIT_LOOKBACK_DAYS", "1"))
DEFAULT_DB_HOST = "67.216.207.73"
DEFAULT_DB_PORT = 5432
DEFAULT_DB_NAME = "postgres"
DEFAULT_DB_USER = "postgres"
DEFAULT_DB_SSLMODE = "disable"

LIMIT_TABLES = {
    "limit_list_d": "ts_limit_list_d",
    "limit_step": "ts_limit_step",
    "limit_cpt_list": "ts_limit_cpt_list",
    "kpl_list": "ts_kpl_list",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def build_db_url():
    try:
        from src.sync_tushare_security_data import build_db_url as _sync_build_db_url
        return _sync_build_db_url()
    except Exception:
        pass

    direct_url = os.getenv("ETF_PG_URL") or os.getenv("DATABASE_URL")
    if direct_url:
        return direct_url

    password = os.getenv("ETF_PG_PASSWORD") or os.getenv("PGPASSWORD")
    if not password:
        try:
            import streamlit as st
            password = (
                st.secrets.get("ETF_PG_PASSWORD")
                or st.secrets.get("PGPASSWORD")
                or st.secrets.get("database", {}).get("password")
            )
            if password:
                os.environ["ETF_PG_PASSWORD"] = str(password)
        except Exception:
            pass

    password = os.getenv("ETF_PG_PASSWORD") or os.getenv("PGPASSWORD")
    if not password:
        raise RuntimeError("未配置数据库密码 ETF_PG_PASSWORD / PGPASSWORD")

    return URL.create(
        "postgresql+psycopg2",
        username=os.getenv("ETF_PG_USER", DEFAULT_DB_USER),
        password=password,
        host=os.getenv("ETF_PG_HOST", DEFAULT_DB_HOST),
        port=int(os.getenv("ETF_PG_PORT", str(DEFAULT_DB_PORT))),
        database=os.getenv("ETF_PG_DATABASE", DEFAULT_DB_NAME),
        query={"sslmode": os.getenv("ETF_PG_SSLMODE", DEFAULT_DB_SSLMODE)},
    )


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


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
    for table_name in LIMIT_TABLES.values():
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


def _parse_trade_date(raw) -> Optional[datetime.date]:
    if raw is None:
        return None
    s = str(raw).replace("-", "").strip()
    if len(s) >= 8 and s[:8].isdigit():
        return datetime.strptime(s[:8], "%Y%m%d").date()
    return None


def get_max_trade_date(engine: Engine, table_name: str) -> Optional[str]:
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT MAX(trade_date) FROM {table_name}")).fetchone()
    if row and row[0]:
        return row[0].strftime("%Y%m%d")
    return None


def resolve_start_date(engine: Engine, table_name: str, force_start: Optional[str] = None, lookback_days: int = 0) -> str:
    if force_start:
        return force_start
    existing_max = get_max_trade_date(engine, table_name)
    if not existing_max:
        return DEFAULT_START_DATE
    start_day = (datetime.strptime(existing_max, "%Y%m%d") - timedelta(days=max(0, int(lookback_days or 0)))).strftime("%Y%m%d")
    return max(DEFAULT_START_DATE, start_day)


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
        for k, v in row.items():
            if isinstance(v, float) and pd.isna(v):
                payload[k] = None
            elif hasattr(v, "item"):
                payload[k] = v.item()
            else:
                payload[k] = v

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


def sync_limit_list_d(engine: Engine, pro, start_date: Optional[str] = None, end_date: Optional[str] = None) -> int:
    table = LIMIT_TABLES["limit_list_d"]
    s = resolve_start_date(engine, table, start_date, lookback_days=DEFAULT_LOOKBACK_DAYS)
    e = end_date or datetime.now().strftime("%Y%m%d")
    logger.info(f"limit_list_d: 拉取 {s} -> {e}")
    total = 0
    try:
        df = pro.limit_list_d(start_date=s, end_date=e)
        if df is not None and not df.empty:
            total = upsert_rows(engine, table, "limit_list_d", df.to_dict("records"))
    except Exception as exc:
        logger.warning(f"limit_list_d 拉取失败: {exc}")
    logger.info(f"limit_list_d 完成，共写入 {total} 行")
    return total


def sync_limit_step(engine: Engine, pro, start_date: Optional[str] = None, end_date: Optional[str] = None) -> int:
    table = LIMIT_TABLES["limit_step"]
    s = resolve_start_date(engine, table, start_date, lookback_days=DEFAULT_LOOKBACK_DAYS)
    e = end_date or datetime.now().strftime("%Y%m%d")
    logger.info(f"limit_step: 拉取 {s} -> {e}")
    total = 0
    try:
        df = pro.limit_step(start_date=s, end_date=e)
        if df is not None and not df.empty:
            total = upsert_rows(engine, table, "limit_step", df.to_dict("records"))
    except Exception as exc:
        logger.warning(f"limit_step 拉取失败: {exc}")
    logger.info(f"limit_step 完成，共写入 {total} 行")
    return total


def sync_limit_cpt_list(engine: Engine, pro, start_date: Optional[str] = None, end_date: Optional[str] = None) -> int:
    table = LIMIT_TABLES["limit_cpt_list"]
    s = resolve_start_date(engine, table, start_date, lookback_days=DEFAULT_LOOKBACK_DAYS)
    e = end_date or datetime.now().strftime("%Y%m%d")
    logger.info(f"limit_cpt_list: 拉取 {s} -> {e}")
    total = 0
    try:
        df = pro.limit_cpt_list(start_date=s, end_date=e)
        if df is not None and not df.empty:
            total = upsert_rows(engine, table, "limit_cpt_list", df.to_dict("records"))
    except Exception as exc:
        logger.warning(f"limit_cpt_list 拉取失败: {exc}")
    logger.info(f"limit_cpt_list 完成，共写入 {total} 行")
    return total


def sync_kpl_list(engine: Engine, pro, start_date: Optional[str] = None, end_date: Optional[str] = None, tag: str = "涨停") -> int:
    table = LIMIT_TABLES["kpl_list"]
    s = resolve_start_date(engine, table, start_date, lookback_days=DEFAULT_LOOKBACK_DAYS)
    e = end_date or datetime.now().strftime("%Y%m%d")
    logger.info(f"kpl_list({tag}): 拉取 {s} -> {e}")
    total = 0
    try:
        df = pro.kpl_list(start_date=s, end_date=e, tag=tag)
        if df is not None and not df.empty:
            rows = []
            for row in df.to_dict("records"):
                r = dict(row)
                r["tag_name"] = tag
                rows.append(r)
            total = upsert_rows(engine, table, "kpl_list", rows, extra_key="tag_name")
    except Exception as exc:
        logger.warning(f"kpl_list 拉取失败: {exc}")
    logger.info(f"kpl_list 完成，共写入 {total} 行")
    return total


def run_sync(datasets: Optional[list[str]] = None, start_date: Optional[str] = None, end_date: Optional[str] = None):
    all_datasets = ["limit_list_d", "limit_step", "limit_cpt_list", "kpl_list"]
    target_datasets = datasets or all_datasets

    logger.info("=== 打板专题数据同步开始 ===")
    logger.info(f"数据集: {target_datasets}")
    logger.info(f"日期范围: {start_date or '增量'} -> {end_date or '今天'}")

    engine = get_engine()
    ensure_all_tables(engine)
    pro = _init_tushare()

    results = {}
    if "limit_list_d" in target_datasets:
        results["limit_list_d"] = sync_limit_list_d(engine, pro, start_date, end_date)
        time.sleep(DEFAULT_API_SLEEP)
    if "limit_step" in target_datasets:
        results["limit_step"] = sync_limit_step(engine, pro, start_date, end_date)
        time.sleep(DEFAULT_API_SLEEP)
    if "limit_cpt_list" in target_datasets:
        results["limit_cpt_list"] = sync_limit_cpt_list(engine, pro, start_date, end_date)
        time.sleep(DEFAULT_API_SLEEP)
    if "kpl_list" in target_datasets:
        results["kpl_list"] = sync_kpl_list(engine, pro, start_date, end_date, tag="涨停")

    total = sum(results.values()) if results else 0
    logger.info(f"=== 同步完成，共写入 {total} 行 ===")
    for ds, n in results.items():
        logger.info(f"  {ds}: {n} 行")
    return results
