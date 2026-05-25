import io
import logging
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

REPORT_TABLE = "ts_distribution_reports"
TICKS_TABLE = "ts_stock_ticks_compressed"

def ensure_tables(engine: Engine):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {REPORT_TABLE} (
        ts_code VARCHAR(20) NOT NULL,
        trade_date VARCHAR(20) NOT NULL,
        report_md TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (ts_code, trade_date)
    );
    
    CREATE TABLE IF NOT EXISTS {TICKS_TABLE} (
        ts_code VARCHAR(20) NOT NULL,
        trade_date VARCHAR(20) NOT NULL,
        parquet_data BYTEA NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (ts_code, trade_date)
    );
    """
    with engine.begin() as conn:
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(stmt))

def get_daily_report(engine: Engine, ts_code: str, trade_date: str) -> str:
    try:
        ensure_tables(engine)
        sql = text(f"SELECT report_md FROM {REPORT_TABLE} WHERE ts_code = :ts AND trade_date = :td")
        with engine.connect() as conn:
            result = conn.execute(sql, {"ts": ts_code, "td": trade_date}).fetchone()
            if result:
                return result[0]
    except Exception as e:
        logger.error(f"Failed to fetch report cache for {ts_code}: {e}")
    return None

def save_daily_report(engine: Engine, ts_code: str, trade_date: str, report_md: str):
    try:
        ensure_tables(engine)
        sql = text(f"""
            INSERT INTO {REPORT_TABLE} (ts_code, trade_date, report_md)
            VALUES (:ts, :td, :md)
            ON CONFLICT (ts_code, trade_date) DO UPDATE
            SET report_md = EXCLUDED.report_md, created_at = NOW()
        """)
        with engine.begin() as conn:
            conn.execute(sql, {"ts": ts_code, "td": trade_date, "md": report_md})
    except Exception as e:
        logger.error(f"Failed to save report cache for {ts_code}: {e}")

def get_compressed_ticks(engine: Engine, ts_code: str, trade_date: str) -> pd.DataFrame:
    try:
        ensure_tables(engine)
        sql = text(f"SELECT parquet_data FROM {TICKS_TABLE} WHERE ts_code = :ts AND trade_date = :td")
        with engine.connect() as conn:
            result = conn.execute(sql, {"ts": ts_code, "td": trade_date}).fetchone()
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
        buf = io.BytesIO()
        df.to_pickle(buf, compression='gzip')
        binary_data = buf.getvalue()
        
        sql = text(f"""
            INSERT INTO {TICKS_TABLE} (ts_code, trade_date, parquet_data)
            VALUES (:ts, :td, :data)
            ON CONFLICT (ts_code, trade_date) DO NOTHING
        """)
        with engine.begin() as conn:
            conn.execute(sql, {"ts": ts_code, "td": trade_date, "data": binary_data})
    except Exception as e:
        logger.error(f"Failed to save compressed ticks for {ts_code} on {trade_date}: {e}")
