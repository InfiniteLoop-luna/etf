import os
import sys
import logging
import uuid
import pandas as pd
from datetime import datetime
from sqlalchemy import text

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.sync_tushare_security_data import get_engine

def ensure_output_table(engine):
    sql = """
    CREATE TABLE IF NOT EXISTS ts_stock_technical_signals (
        ts_code VARCHAR(20),
        trade_date DATE,
        w_ema5 NUMERIC(15,4),
        w_ema30 NUMERIC(15,4),
        m_ema5 NUMERIC(15,4),
        m_ema30 NUMERIC(15,4),
        is_weekly_ema_bearish BOOLEAN,
        is_monthly_ema_bearish BOOLEAN,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (ts_code, trade_date)
    );
    CREATE INDEX IF NOT EXISTS idx_ts_stock_technical_signals_trade_date ON ts_stock_technical_signals(trade_date);
    """
    with engine.begin() as conn:
        conn.execute(text(sql))

def calculate_ema_for_stock(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values('trade_date').reset_index(drop=True)
    for col in ['w_close', 'm_close']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    if df['w_close'].isna().all() and df['m_close'].isna().all():
        return pd.DataFrame()

    base_df = df[['ts_code', 'trade_date']].drop_duplicates().sort_values('trade_date').reset_index(drop=True)

    weekly_df = df.dropna(subset=['w_close'])[['trade_date', 'w_close']].drop_duplicates(subset=['trade_date'], keep='last')
    monthly_df = df.dropna(subset=['m_close'])[['trade_date', 'm_close']].drop_duplicates(subset=['trade_date'], keep='last')

    if not weekly_df.empty:
        weekly_df = weekly_df.sort_values('trade_date').reset_index(drop=True)
        weekly_df['w_ema5'] = weekly_df['w_close'].ewm(span=5, adjust=False).mean()
        weekly_df['w_ema30'] = weekly_df['w_close'].ewm(span=30, adjust=False).mean()
        weekly_df = weekly_df[['trade_date', 'w_ema5', 'w_ema30']]

    if not monthly_df.empty:
        monthly_df = monthly_df.sort_values('trade_date').reset_index(drop=True)
        monthly_df['m_ema5'] = monthly_df['m_close'].ewm(span=5, adjust=False).mean()
        monthly_df['m_ema30'] = monthly_df['m_close'].ewm(span=30, adjust=False).mean()
        monthly_df = monthly_df[['trade_date', 'm_ema5', 'm_ema30']]

    result_df = base_df
    if not weekly_df.empty:
        result_df = result_df.merge(weekly_df, on='trade_date', how='left')
        result_df[['w_ema5', 'w_ema30']] = result_df[['w_ema5', 'w_ema30']].ffill()
    else:
        result_df['w_ema5'] = pd.NA
        result_df['w_ema30'] = pd.NA

    if not monthly_df.empty:
        result_df = result_df.merge(monthly_df, on='trade_date', how='left')
        result_df[['m_ema5', 'm_ema30']] = result_df[['m_ema5', 'm_ema30']].ffill()
    else:
        result_df['m_ema5'] = pd.NA
        result_df['m_ema30'] = pd.NA

    result_df['is_weekly_ema_bearish'] = (
        result_df['w_ema5'].notna() & result_df['w_ema30'].notna() & (result_df['w_ema5'] < result_df['w_ema30'])
    )
    result_df['is_monthly_ema_bearish'] = (
        result_df['m_ema5'].notna() & result_df['m_ema30'].notna() & (result_df['m_ema5'] < result_df['m_ema30'])
    )

    result_df['trade_date'] = result_df['trade_date'].dt.strftime('%Y-%m-%d')
    return result_df[['ts_code', 'trade_date', 'w_ema5', 'w_ema30', 'm_ema5', 'm_ema30', 'is_weekly_ema_bearish', 'is_monthly_ema_bearish']]


def process_all_stocks():
    engine = get_engine()
    ensure_output_table(engine)

    logger.info("Fetching ts_codes from vw_ts_stk_week_month_adj...")
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT DISTINCT ts_code FROM vw_ts_stk_week_month_adj WHERE ts_code IS NOT NULL")).fetchall()
        stk_codes = [r[0] for r in rows]

    if not stk_codes:
        logger.info("No data found in vw_ts_stk_week_month_adj. Exiting.")
        return

    logger.info(f"Found {len(stk_codes)} stocks to process.")

    chunk_size = 100
    for i in range(0, len(stk_codes), chunk_size):
        chunk_codes = stk_codes[i:i+chunk_size]
        logger.info(f"Processing chunk {i//chunk_size + 1}/{(len(stk_codes)-1)//chunk_size + 1} ({len(chunk_codes)} stocks)...")

        # 1. Fetch raw data chunk
        query = f"""
            SELECT ts_code, trade_date, w_close, m_close
            FROM vw_ts_stk_week_month_adj
            WHERE ts_code IN ({','.join([f"'{c}'" for c in chunk_codes])})
            ORDER BY ts_code, trade_date
        """
        raw_df = pd.read_sql(query, engine)

        # 2. Process calculating EMA per stock
        res_frames = []
        for code, group in raw_df.groupby('ts_code'):
            res = calculate_ema_for_stock(group)
            if not res.empty:
                res_frames.append(res)

        if not res_frames:
            continue

        result_df = pd.concat(res_frames, ignore_index=True)

        # 3. Upsert back to database
        tmp_table = f"_tmp_ema_{uuid.uuid4().hex[:8]}"
        with engine.begin() as conn:
            result_df.to_sql(tmp_table, conn, if_exists="replace", index=False, method="multi", chunksize=1000)
            conn.execute(text(f"""
                INSERT INTO ts_stock_technical_signals (
                    ts_code, trade_date, w_ema5, w_ema30, m_ema5, m_ema30, is_weekly_ema_bearish, is_monthly_ema_bearish
                )
                SELECT
                    ts_code,
                    trade_date::date,
                    w_ema5,
                    w_ema30,
                    m_ema5,
                    m_ema30,
                    is_weekly_ema_bearish,
                    is_monthly_ema_bearish
                FROM {tmp_table}
                ON CONFLICT (ts_code, trade_date) DO UPDATE SET
                    w_ema5 = EXCLUDED.w_ema5,
                    w_ema30 = EXCLUDED.w_ema30,
                    m_ema5 = EXCLUDED.m_ema5,
                    m_ema30 = EXCLUDED.m_ema30,
                    is_weekly_ema_bearish = EXCLUDED.is_weekly_ema_bearish,
                    is_monthly_ema_bearish = EXCLUDED.is_monthly_ema_bearish,
                    updated_at = NOW()
            """))
            conn.execute(text(f"DROP TABLE {tmp_table}"))

    logger.info("Process completed.")

if __name__ == "__main__":
    process_all_stocks()
