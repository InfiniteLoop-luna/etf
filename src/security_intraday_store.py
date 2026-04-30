from __future__ import annotations

import logging
import os
from datetime import date, datetime
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.volume_fetcher import _init_tushare

INTRADAY_TABLE = "ts_stock_intraday_mins"
DEFAULT_FREQ = "1min"

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
        raise RuntimeError("未配置数据库密码，请设置 ETF_PG_PASSWORD 或 PGPASSWORD")

    from sqlalchemy.engine import URL

    return URL.create(
        "postgresql+psycopg2",
        username=os.getenv("ETF_PG_USER", "postgres"),
        password=password,
        host=os.getenv("ETF_PG_HOST", "67.216.207.73"),
        port=int(os.getenv("ETF_PG_PORT", "5432")),
        database=os.getenv("ETF_PG_DATABASE", "postgres"),
        query={"sslmode": os.getenv("ETF_PG_SSLMODE", "disable")},
    )


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


def normalize_trade_date(value) -> date:
    if value is None:
        raise ValueError("trade_date 不能为空")

    text_value = str(value).strip()
    if not text_value:
        raise ValueError("trade_date 不能为空字符串")

    candidates = [text_value, text_value.replace("-", "")]
    for candidate in candidates:
        try:
            if len(candidate) == 8 and candidate.isdigit():
                return datetime.strptime(candidate, "%Y%m%d").date()
            if len(text_value) == 10 and text_value.count("-") == 2:
                return datetime.strptime(text_value, "%Y-%m-%d").date()
        except ValueError:
            continue

    raise ValueError(f"无法解析 trade_date: {value}")


def ensure_intraday_table(engine: Engine):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {INTRADAY_TABLE} (
        ts_code VARCHAR(20) NOT NULL,
        trade_date DATE NOT NULL,
        trade_time TIMESTAMP NOT NULL,
        freq VARCHAR(10) NOT NULL DEFAULT '{DEFAULT_FREQ}',
        open NUMERIC(18, 4),
        high NUMERIC(18, 4),
        low NUMERIC(18, 4),
        close NUMERIC(18, 4),
        vol NUMERIC(20, 4),
        amount NUMERIC(20, 4),
        source VARCHAR(64) NOT NULL DEFAULT 'tushare.stk_mins',
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (ts_code, freq, trade_time)
    );
    CREATE INDEX IF NOT EXISTS idx_{INTRADAY_TABLE}_date
        ON {INTRADAY_TABLE}(trade_date DESC);
    CREATE INDEX IF NOT EXISTS idx_{INTRADAY_TABLE}_code_date_freq
        ON {INTRADAY_TABLE}(ts_code, trade_date DESC, freq);
    """

    with engine.begin() as conn:
        for statement in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(statement))


def normalize_stk_mins_frame(
    df: pd.DataFrame,
    ts_code: str,
    trade_date,
    freq: str = DEFAULT_FREQ,
) -> pd.DataFrame:
    columns = [
        "ts_code",
        "trade_date",
        "trade_time",
        "freq",
        "open",
        "high",
        "low",
        "close",
        "vol",
        "amount",
    ]
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)

    trade_date_value = normalize_trade_date(trade_date)
    normalized = df.copy()
    normalized["ts_code"] = str(ts_code or "").strip().upper()
    normalized["trade_time"] = pd.to_datetime(normalized.get("trade_time"), errors="coerce")
    normalized = normalized.dropna(subset=["trade_time"])
    if normalized.empty:
        return pd.DataFrame(columns=columns)

    normalized["trade_date"] = normalized["trade_time"].dt.date
    normalized = normalized[normalized["trade_date"] == trade_date_value].copy()
    if normalized.empty:
        return pd.DataFrame(columns=columns)

    normalized["freq"] = str(freq or DEFAULT_FREQ).strip() or DEFAULT_FREQ
    for column in ["open", "high", "low", "close", "vol", "amount"]:
        if column not in normalized.columns:
            normalized[column] = None
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized = normalized[columns]
    normalized = normalized.drop_duplicates(subset=["ts_code", "freq", "trade_time"], keep="last")
    normalized = normalized.sort_values("trade_time").reset_index(drop=True)
    return normalized


def fetch_stock_intraday_from_tushare(
    ts_code: str,
    trade_date,
    freq: str = DEFAULT_FREQ,
    pro=None,
) -> pd.DataFrame:
    ts_code_text = str(ts_code or "").strip().upper()
    if not ts_code_text:
        raise ValueError("ts_code 不能为空")

    trade_date_value = normalize_trade_date(trade_date)
    freq_text = str(freq or DEFAULT_FREQ).strip() or DEFAULT_FREQ
    start_date = trade_date_value.strftime("%Y%m%d") + " 09:30:00"
    end_date = trade_date_value.strftime("%Y%m%d") + " 15:00:00"

    if pro is None:
        pro = _init_tushare()

    logger.info(
        "fetch_stock_intraday_from_tushare ts_code=%s trade_date=%s freq=%s",
        ts_code_text,
        trade_date_value,
        freq_text,
    )
    raw_df = pro.stk_mins(
        ts_code=ts_code_text,
        start_date=start_date,
        end_date=end_date,
        freq=freq_text,
    )
    return normalize_stk_mins_frame(raw_df, ts_code=ts_code_text, trade_date=trade_date_value, freq=freq_text)


def upsert_stock_intraday_timeseries(
    engine: Engine,
    df: pd.DataFrame,
    source: str = "tushare.stk_mins",
) -> int:
    ensure_intraday_table(engine)
    if df is None or df.empty:
        return 0

    insert_sql = text(
        f"""
        INSERT INTO {INTRADAY_TABLE} (
            ts_code, trade_date, trade_time, freq,
            open, high, low, close, vol, amount, source
        ) VALUES (
            :ts_code, :trade_date, :trade_time, :freq,
            :open, :high, :low, :close, :vol, :amount, :source
        )
        ON CONFLICT (ts_code, freq, trade_time) DO UPDATE SET
            trade_date = EXCLUDED.trade_date,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            vol = EXCLUDED.vol,
            amount = EXCLUDED.amount,
            source = EXCLUDED.source,
            updated_at = NOW();
        """
    )

    payload_rows = []
    for row in df.to_dict(orient="records"):
        payload_rows.append(
            {
                "ts_code": str(row.get("ts_code") or "").strip().upper(),
                "trade_date": row.get("trade_date"),
                "trade_time": row.get("trade_time"),
                "freq": str(row.get("freq") or DEFAULT_FREQ).strip() or DEFAULT_FREQ,
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "vol": row.get("vol"),
                "amount": row.get("amount"),
                "source": source,
            }
        )

    with engine.begin() as conn:
        conn.execute(insert_sql, payload_rows)
    return len(payload_rows)


def get_stock_intraday_timeseries(
    ts_code: str,
    trade_date,
    freq: str = DEFAULT_FREQ,
    engine: Optional[Engine] = None,
) -> pd.DataFrame:
    if engine is None:
        engine = get_engine()

    ensure_intraday_table(engine)
    trade_date_value = normalize_trade_date(trade_date)
    freq_text = str(freq or DEFAULT_FREQ).strip() or DEFAULT_FREQ
    ts_code_text = str(ts_code or "").strip().upper()

    sql = text(
        f"""
        SELECT
            ts_code,
            trade_date,
            trade_time,
            freq,
            open,
            high,
            low,
            close,
            vol,
            amount,
            source,
            ingested_at,
            updated_at
        FROM {INTRADAY_TABLE}
        WHERE ts_code = :ts_code
          AND trade_date = :trade_date
          AND freq = :freq
        ORDER BY trade_time
        """
    )
    df = pd.read_sql(
        sql,
        engine,
        params={
            "ts_code": ts_code_text,
            "trade_date": trade_date_value,
            "freq": freq_text,
        },
    )
    if df is None or df.empty:
        return pd.DataFrame(columns=["ts_code", "trade_date", "trade_time", "freq", "open", "high", "low", "close", "vol", "amount"])

    if "trade_time" in df.columns:
        df["trade_time"] = pd.to_datetime(df["trade_time"], errors="coerce")
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    for column in ["open", "high", "low", "close", "vol", "amount"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def load_or_fetch_stock_intraday_timeseries(
    ts_code: str,
    trade_date,
    freq: str = DEFAULT_FREQ,
    engine: Optional[Engine] = None,
    pro=None,
) -> tuple[pd.DataFrame, str]:
    if engine is None:
        engine = get_engine()

    cached_df = get_stock_intraday_timeseries(ts_code, trade_date, freq=freq, engine=engine)
    if cached_df is not None and not cached_df.empty:
        return cached_df, "db"

    fetched_df = fetch_stock_intraday_from_tushare(ts_code, trade_date, freq=freq, pro=pro)
    if fetched_df is None or fetched_df.empty:
        return pd.DataFrame(columns=["ts_code", "trade_date", "trade_time", "freq", "open", "high", "low", "close", "vol", "amount"]), "tushare-empty"

    upsert_stock_intraday_timeseries(engine, fetched_df)
    refreshed_df = get_stock_intraday_timeseries(ts_code, trade_date, freq=freq, engine=engine)
    if refreshed_df is not None and not refreshed_df.empty:
        return refreshed_df, "tushare"
    return fetched_df, "tushare"
