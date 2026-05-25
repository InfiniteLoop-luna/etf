from __future__ import annotations

import json
from datetime import date
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.sync_tushare_security_data import build_db_url

TABLE_NAME = "ts_distribution_alerts"

def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)

def ensure_distribution_alert_table(engine: Engine) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        ts_code VARCHAR(20) NOT NULL,
        trade_date DATE NOT NULL,
        alert_level VARCHAR(20) NOT NULL,
        alert_details JSONB,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (ts_code, trade_date)
    );

    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_trade_date 
        ON {TABLE_NAME} (trade_date DESC);
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_ts_code 
        ON {TABLE_NAME} (ts_code);
    """
    with engine.begin() as conn:
        for statement in [item.strip() for item in sql.split(";") if item.strip()]:
            conn.execute(text(statement))

def upsert_alerts(alerts: list[dict[str, Any]], engine: Engine | None = None) -> None:
    """
    插入或更新预警数据。
    alerts 的格式应为：
    [
        {
            "ts_code": "000811.SZ",
            "trade_date": "2026-05-25", # date object or string
            "alert_level": "HIGH",      # e.g., "HIGH", "MEDIUM", "LOW"
            "alert_details": {"patterns": [...], "summary": "..."} # dict
        }, ...
    ]
    """
    if not alerts:
        return
    
    actual_engine = engine or get_engine()
    ensure_distribution_alert_table(actual_engine)

    sql = f"""
    INSERT INTO {TABLE_NAME} (
        ts_code, trade_date, alert_level, alert_details, updated_at
    ) VALUES (
        :ts_code, :trade_date, :alert_level, CAST(:alert_details AS jsonb), NOW()
    )
    ON CONFLICT (ts_code, trade_date) DO UPDATE SET
        alert_level = EXCLUDED.alert_level,
        alert_details = EXCLUDED.alert_details,
        updated_at = NOW();
    """
    
    payload = []
    for item in alerts:
        payload.append({
            "ts_code": item["ts_code"],
            "trade_date": item["trade_date"],
            "alert_level": item["alert_level"],
            "alert_details": json.dumps(item.get("alert_details", {}), ensure_ascii=False)
        })

    with actual_engine.begin() as conn:
        conn.execute(text(sql), payload)


def get_latest_alerts_for_stocks(ts_codes: list[str], engine: Engine | None = None) -> pd.DataFrame:
    """
    获取指定股票列表最新的预警数据。
    """
    if not ts_codes:
        return pd.DataFrame()
        
    actual_engine = engine or get_engine()
    ensure_distribution_alert_table(actual_engine)
    
    # 查找这些股票最新交易日的预警数据
    # 为每只股票找到最大的 trade_date
    sql = f"""
    WITH LatestDates AS (
        SELECT ts_code, MAX(trade_date) as max_trade_date
        FROM {TABLE_NAME}
        WHERE ts_code = ANY(:ts_codes)
        GROUP BY ts_code
    )
    SELECT a.*
    FROM {TABLE_NAME} a
    INNER JOIN LatestDates b 
        ON a.ts_code = b.ts_code AND a.trade_date = b.max_trade_date
    """
    
    with actual_engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params={"ts_codes": ts_codes})
