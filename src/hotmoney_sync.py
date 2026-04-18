# -*- coding: utf-8 -*-
"""
游资名录 / 游资每日明细 同步模块
覆盖接口：
- hm_list
- hm_detail

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

DEFAULT_ROSTER_START_DATE = "20220801"
DEFAULT_DETAIL_START_DATE = "20240101"
DEFAULT_API_SLEEP = float(os.getenv("TUSHARE_HM_API_SLEEP", "0.25"))
DEFAULT_LOOKBACK_DAYS = int(os.getenv("TUSHARE_HM_LOOKBACK_DAYS", "2"))

# hm_detail 限频很严，默认走“慢速小窗口补数”
DEFAULT_DETAIL_BATCH_DAYS = int(os.getenv("TUSHARE_HM_DETAIL_BATCH_DAYS", "1"))
DEFAULT_DETAIL_REQUEST_SLEEP_SECONDS = float(os.getenv("TUSHARE_HM_DETAIL_REQUEST_SLEEP_SECONDS", "35"))
DEFAULT_DETAIL_LOOKBACK_DAYS = int(os.getenv("TUSHARE_HM_DETAIL_LOOKBACK_DAYS", "0"))
DEFAULT_RATE_LIMIT_COOLDOWN_SECONDS = float(os.getenv("TUSHARE_HM_RATE_LIMIT_COOLDOWN_SECONDS", "90"))

DEFAULT_DB_HOST = "67.216.207.73"
DEFAULT_DB_PORT = 5432
DEFAULT_DB_NAME = "postgres"
DEFAULT_DB_USER = "postgres"
DEFAULT_DB_SSLMODE = "disable"

HM_TABLES = {
    "hm_list": "ts_hm_list",
    "hm_detail": "ts_hm_detail",
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
    for table_name in HM_TABLES.values():
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


def _is_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc)
    tokens = ["每分钟最多访问", "每小时最多访问", "频次", "rate limit", "too many requests"]
    return any(t in msg for t in tokens)


def _calc_batch_end(start_date: str, hard_end_date: str, batch_days: int) -> str:
    s_dt = datetime.strptime(start_date, "%Y%m%d").date()
    e_dt = datetime.strptime(hard_end_date, "%Y%m%d").date()
    if batch_days <= 0:
        return hard_end_date
    b_dt = s_dt + timedelta(days=batch_days - 1)
    return min(e_dt, b_dt).strftime("%Y%m%d")


def get_max_trade_date(engine: Engine, table_name: str) -> Optional[str]:
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT MAX(trade_date) FROM {table_name}")).fetchone()
    if row and row[0]:
        return row[0].strftime("%Y%m%d")
    return None


def resolve_start_date(engine: Engine, table_name: str, default_start: str, force_start: Optional[str] = None, lookback_days: int = 0) -> str:
    if force_start:
        return force_start
    existing_max = get_max_trade_date(engine, table_name)
    if not existing_max:
        return default_start
    start_day = (datetime.strptime(existing_max, "%Y%m%d") - timedelta(days=max(0, int(lookback_days or 0)))).strftime("%Y%m%d")
    return max(default_start, start_day)


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


def sync_hm_list(engine: Engine, pro, name: Optional[str] = None) -> int:
    table = HM_TABLES["hm_list"]
    logger.info("hm_list: 拉取游资名录")
    total = 0
    try:
        df = pro.hm_list(name=name) if name else pro.hm_list()
        if df is not None and not df.empty:
            total = upsert_rows(
                engine,
                table,
                "hm_list",
                df.to_dict("records"),
                ts_code_key="name",
                trade_date_key="trade_date",
                extra_key="name",
            )
    except Exception as exc:
        logger.warning(f"hm_list 拉取失败: {exc}")
    logger.info(f"hm_list 完成，共写入 {total} 行")
    return total


def sync_hm_detail(
    engine: Engine,
    pro,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    batch_days: int = DEFAULT_DETAIL_BATCH_DAYS,
    request_sleep_seconds: float = DEFAULT_DETAIL_REQUEST_SLEEP_SECONDS,
    lookback_days: int = DEFAULT_DETAIL_LOOKBACK_DAYS,
    stop_on_rate_limit: bool = True,
    max_days: Optional[int] = None,
) -> int:
    table = HM_TABLES["hm_detail"]
    s = resolve_start_date(engine, table, DEFAULT_DETAIL_START_DATE, start_date, lookback_days=lookback_days)
    hard_end = end_date or datetime.now().strftime("%Y%m%d")
    e = _calc_batch_end(s, hard_end, batch_days)

    logger.info(
        f"hm_detail: 拉取 {s} -> {e} (hard_end={hard_end}, batch_days={batch_days}, sleep={request_sleep_seconds}s)"
    )

    total = 0
    dt_range = list(pd.date_range(pd.to_datetime(s), pd.to_datetime(e), freq="D"))
    if max_days is not None and max_days > 0:
        dt_range = dt_range[:max_days]

    for idx, dt in enumerate(dt_range, start=1):
        trade_date = dt.strftime("%Y%m%d")
        try:
            df = pro.hm_detail(trade_date=trade_date)
            if df is not None and not df.empty:
                rows = []
                for row in df.to_dict("records"):
                    r = dict(row)
                    r["hm_name_key"] = r.get("hm_name")
                    rows.append(r)
                total += upsert_rows(engine, table, "hm_detail", rows, extra_key="hm_name_key")
                logger.info(f"hm_detail[{trade_date}] 写入 {len(rows)} 行")
            else:
                logger.info(f"hm_detail[{trade_date}] 返回空")
        except Exception as exc:
            if _is_rate_limit_error(exc):
                logger.warning(f"hm_detail[{trade_date}] 触发限频: {exc}")
                if stop_on_rate_limit:
                    logger.warning("检测到限频，提前停止本轮补数，等待下次任务继续。")
                    break
                time.sleep(max(request_sleep_seconds, DEFAULT_RATE_LIMIT_COOLDOWN_SECONDS))
            else:
                logger.warning(f"hm_detail[{trade_date}] 拉取失败: {exc}")

        if idx < len(dt_range):
            time.sleep(max(0.1, request_sleep_seconds))

    logger.info(f"hm_detail 完成，共写入 {total} 行")
    return total


def run_sync(
    datasets: Optional[list[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    detail_batch_days: int = DEFAULT_DETAIL_BATCH_DAYS,
    detail_request_sleep_seconds: float = DEFAULT_DETAIL_REQUEST_SLEEP_SECONDS,
    detail_lookback_days: int = DEFAULT_DETAIL_LOOKBACK_DAYS,
    detail_stop_on_rate_limit: bool = True,
    detail_max_days: Optional[int] = None,
):
    all_datasets = ["hm_list", "hm_detail"]
    target_datasets = datasets or all_datasets

    logger.info("=== 游资数据同步开始 ===")
    logger.info(f"数据集: {target_datasets}")
    logger.info(f"日期范围: {start_date or '增量'} -> {end_date or '今天'}")

    engine = get_engine()
    ensure_all_tables(engine)
    pro = _init_tushare()

    results = {}
    if "hm_list" in target_datasets:
        results["hm_list"] = sync_hm_list(engine, pro)
        time.sleep(DEFAULT_API_SLEEP)
    if "hm_detail" in target_datasets:
        results["hm_detail"] = sync_hm_detail(
            engine,
            pro,
            start_date,
            end_date,
            batch_days=detail_batch_days,
            request_sleep_seconds=detail_request_sleep_seconds,
            lookback_days=detail_lookback_days,
            stop_on_rate_limit=detail_stop_on_rate_limit,
            max_days=detail_max_days,
        )

    total = sum(results.values()) if results else 0
    logger.info(f"=== 同步完成，共写入 {total} 行 ===")
    for ds, n in results.items():
        logger.info(f"  {ds}: {n} 行")
    return results
