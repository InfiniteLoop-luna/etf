from __future__ import annotations

import logging
import os
from datetime import date, datetime
from typing import Any, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.volume_fetcher import _init_tushare

INTRADAY_TABLE = "ts_stock_intraday_mins"
DEFAULT_FREQ = "1min"
INTRADAY_COLUMNS = [
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
DB_INTRADAY_COLUMNS = INTRADAY_COLUMNS + ["source", "ingested_at", "updated_at"]
_MOOTDX_IMPORT_ERROR: Optional[str] = None

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


def empty_intraday_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=INTRADAY_COLUMNS)


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


def normalize_ts_code_text(ts_code: str) -> str:
    ts_code_text = str(ts_code or "").strip().upper()
    if not ts_code_text:
        raise ValueError("ts_code 不能为空")
    return ts_code_text


def infer_stock_market(symbol: str) -> str:
    symbol_text = str(symbol or "").strip().lower()
    if not symbol_text:
        raise ValueError("symbol 不能为空")

    if symbol_text.startswith(("sh", "sz", "bj")) and len(symbol_text) > 2:
        return symbol_text[:2]

    if symbol_text.startswith(("50", "51", "60", "68", "90", "110", "113", "132", "204")):
        return "sh"
    if symbol_text.startswith(("00", "12", "13", "15", "16", "18", "20", "30", "39", "115", "1318")):
        return "sz"
    if symbol_text.startswith(("5", "6", "7", "9")):
        return "sh"
    if symbol_text.startswith(("4", "8")):
        return "bj"
    return "sz"


def normalize_mootdx_code(ts_code: str) -> dict[str, Any]:
    ts_code_text = normalize_ts_code_text(ts_code)
    symbol = ts_code_text
    market_name = ""

    if "." in ts_code_text:
        symbol, market_name = ts_code_text.split(".", 1)
        symbol = symbol.strip().upper()
        market_name = market_name.strip().lower()
    else:
        lowered = ts_code_text.lower()
        if lowered.startswith(("sh", "sz", "bj")) and len(ts_code_text) > 2:
            market_name = lowered[:2]
            symbol = ts_code_text[2:]
        else:
            symbol = ts_code_text

    if market_name not in {"sh", "sz", "bj"}:
        market_name = infer_stock_market(symbol)

    market_code_map = {"sz": 0, "sh": 1, "bj": 2}
    return {
        "ts_code": f"{symbol}.{market_name.upper()}",
        "symbol": symbol,
        "market_name": market_name,
        "market_code": market_code_map.get(market_name),
        "supports_quotes": market_name in {"sh", "sz", "bj"},
        "supports_minutes": market_name in {"sh", "sz"},
    }


def _get_mootdx_quotes_class():
    global _MOOTDX_IMPORT_ERROR
    try:
        from mootdx.quotes import Quotes

        return Quotes
    except Exception as exc:
        message = str(exc)
        if _MOOTDX_IMPORT_ERROR != message:
            logger.info("mootdx import unavailable: %s", message)
            _MOOTDX_IMPORT_ERROR = message
        return None


def _create_mootdx_client(timeout: int = 8):
    quotes_cls = _get_mootdx_quotes_class()
    if quotes_cls is None:
        return None

    try:
        return quotes_cls.factory(market="std", timeout=int(timeout), heartbeat=True, auto_retry=True)
    except Exception as exc:
        logger.warning("create_mootdx_client failed: %s", exc)
        return None


def _close_mootdx_client(client) -> None:
    if client is None:
        return
    try:
        close_func = getattr(client, "close", None)
        if callable(close_func):
            close_func()
            return
        inner_client = getattr(client, "client", None)
        inner_close = getattr(inner_client, "close", None)
        if callable(inner_close):
            inner_close()
    except Exception:
        pass


def _to_optional_float(value) -> Optional[float]:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def fetch_stock_realtime_snapshot_from_mootdx(
    ts_code: str,
    client=None,
) -> dict[str, Any]:
    code_info = normalize_mootdx_code(ts_code)
    if not code_info["supports_quotes"]:
        return {
            "status": "unsupported",
            "source": "mootdx.quotes",
            "ts_code": code_info["ts_code"],
            "symbol": code_info["symbol"],
            "market_name": code_info["market_name"],
        }

    own_client = client is None
    if own_client:
        client = _create_mootdx_client()
    if client is None:
        return {
            "status": "unavailable",
            "source": "mootdx.quotes",
            "ts_code": code_info["ts_code"],
            "symbol": code_info["symbol"],
            "market_name": code_info["market_name"],
        }

    try:
        quote_df = client.quotes(symbol=code_info["symbol"])
        if quote_df is None or quote_df.empty:
            return {
                "status": "empty",
                "source": "mootdx.quotes",
                "ts_code": code_info["ts_code"],
                "symbol": code_info["symbol"],
                "market_name": code_info["market_name"],
            }

        row = quote_df.iloc[0].to_dict()
        price = _to_optional_float(row.get("price"))
        last_close = _to_optional_float(row.get("last_close"))
        change = None
        pct_change = None
        if price is not None and last_close not in {None, 0}:
            change = price - float(last_close)
            pct_change = change / float(last_close) * 100.0

        return {
            "status": "ok",
            "source": "mootdx.quotes",
            "ts_code": code_info["ts_code"],
            "symbol": code_info["symbol"],
            "market_name": code_info["market_name"],
            "price": price,
            "last_close": last_close,
            "open": _to_optional_float(row.get("open")),
            "high": _to_optional_float(row.get("high")),
            "low": _to_optional_float(row.get("low")),
            "change": change,
            "pct_change": pct_change,
            "amount": _to_optional_float(row.get("amount")),
            "vol": _to_optional_float(row.get("vol")),
            "cur_vol": _to_optional_float(row.get("cur_vol")),
            "s_vol": _to_optional_float(row.get("s_vol")),
            "b_vol": _to_optional_float(row.get("b_vol")),
            "bid1": _to_optional_float(row.get("bid1")),
            "ask1": _to_optional_float(row.get("ask1")),
            "bid_vol1": _to_optional_float(row.get("bid_vol1")),
            "ask_vol1": _to_optional_float(row.get("ask_vol1")),
            "servertime": str(row.get("servertime") or "").strip(),
        }
    except Exception as exc:
        logger.warning("fetch_stock_realtime_snapshot_from_mootdx failed for %s: %s", ts_code, exc)
        return {
            "status": "error",
            "source": "mootdx.quotes",
            "ts_code": code_info["ts_code"],
            "symbol": code_info["symbol"],
            "market_name": code_info["market_name"],
            "error": str(exc),
        }
    finally:
        if own_client:
            _close_mootdx_client(client)


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
    if df is None or df.empty:
        return empty_intraday_frame()

    trade_date_value = normalize_trade_date(trade_date)
    normalized = df.copy()
    normalized["ts_code"] = normalize_ts_code_text(ts_code)
    normalized["trade_time"] = pd.to_datetime(normalized.get("trade_time"), errors="coerce")
    normalized = normalized.dropna(subset=["trade_time"])
    if normalized.empty:
        return empty_intraday_frame()

    normalized["trade_date"] = normalized["trade_time"].dt.date
    normalized = normalized[normalized["trade_date"] == trade_date_value].copy()
    if normalized.empty:
        return empty_intraday_frame()

    normalized["freq"] = str(freq or DEFAULT_FREQ).strip() or DEFAULT_FREQ
    for column in ["open", "high", "low", "close", "vol", "amount"]:
        if column not in normalized.columns:
            normalized[column] = None
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized = normalized[INTRADAY_COLUMNS]
    normalized = normalized.drop_duplicates(subset=["ts_code", "freq", "trade_time"], keep="last")
    normalized = normalized.sort_values("trade_time").reset_index(drop=True)
    return normalized


def _build_cn_equity_minute_index(trade_date_value: date, point_count: int) -> pd.DatetimeIndex:
    if point_count <= 0:
        return pd.DatetimeIndex([])

    day_text = trade_date_value.strftime("%Y-%m-%d")
    morning = pd.date_range(f"{day_text} 09:30:00", periods=120, freq="min")
    afternoon = pd.date_range(f"{day_text} 13:00:00", periods=120, freq="min")
    full_session = morning.append(afternoon)

    if point_count <= len(full_session):
        return full_session[:point_count]

    fallback = pd.date_range(f"{day_text} 09:30:00", periods=point_count, freq="min")
    return fallback


def normalize_mootdx_minutes_frame(
    df: pd.DataFrame,
    ts_code: str,
    trade_date,
    freq: str = DEFAULT_FREQ,
) -> pd.DataFrame:
    if df is None or df.empty:
        return empty_intraday_frame()

    trade_date_value = normalize_trade_date(trade_date)
    normalized = df.copy()
    if "price" not in normalized.columns and "close" in normalized.columns:
        normalized["price"] = normalized["close"]
    if "vol" not in normalized.columns and "volume" in normalized.columns:
        normalized["vol"] = normalized["volume"]

    normalized["price"] = pd.to_numeric(normalized.get("price"), errors="coerce")
    normalized["vol"] = pd.to_numeric(normalized.get("vol"), errors="coerce")
    normalized = normalized.dropna(subset=["price"])
    if normalized.empty:
        return empty_intraday_frame()

    normalized = normalized.reset_index(drop=True)
    trade_times = _build_cn_equity_minute_index(trade_date_value, len(normalized))
    normalized["trade_time"] = trade_times
    normalized["close"] = normalized["price"]
    normalized["open"] = normalized["close"].shift(1)
    normalized.loc[normalized["open"].isna(), "open"] = normalized["close"]
    normalized["high"] = normalized[["open", "close"]].max(axis=1)
    normalized["low"] = normalized[["open", "close"]].min(axis=1)
    normalized["amount"] = None
    normalized["ts_code"] = normalize_ts_code_text(ts_code)
    normalized["trade_date"] = trade_date_value
    normalized["freq"] = str(freq or DEFAULT_FREQ).strip() or DEFAULT_FREQ

    normalized = normalized[INTRADAY_COLUMNS]
    normalized = normalized.drop_duplicates(subset=["ts_code", "freq", "trade_time"], keep="last")
    normalized = normalized.sort_values("trade_time").reset_index(drop=True)
    return normalized


def fetch_stock_intraday_from_mootdx(
    ts_code: str,
    trade_date,
    freq: str = DEFAULT_FREQ,
    client=None,
) -> pd.DataFrame:
    code_info = normalize_mootdx_code(ts_code)
    if not code_info["supports_minutes"]:
        logger.info("fetch_stock_intraday_from_mootdx skipped unsupported market: %s", ts_code)
        return empty_intraday_frame()

    trade_date_value = normalize_trade_date(trade_date)
    own_client = client is None
    if own_client:
        client = _create_mootdx_client()
    if client is None:
        return empty_intraday_frame()

    try:
        logger.info(
            "fetch_stock_intraday_from_mootdx ts_code=%s trade_date=%s freq=%s",
            code_info["ts_code"],
            trade_date_value,
            freq,
        )
        raw_df = client.minutes(symbol=code_info["symbol"], date=trade_date_value.strftime("%Y%m%d"))
        return normalize_mootdx_minutes_frame(raw_df, ts_code=code_info["ts_code"], trade_date=trade_date_value, freq=freq)
    finally:
        if own_client:
            _close_mootdx_client(client)


def fetch_stock_intraday_from_tushare(
    ts_code: str,
    trade_date,
    freq: str = DEFAULT_FREQ,
    pro=None,
) -> pd.DataFrame:
    ts_code_text = normalize_ts_code_text(ts_code)
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
                "ts_code": normalize_ts_code_text(row.get("ts_code")),
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
    ts_code_text = normalize_ts_code_text(ts_code)

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
        return pd.DataFrame(columns=DB_INTRADAY_COLUMNS)

    if "trade_time" in df.columns:
        df["trade_time"] = pd.to_datetime(df["trade_time"], errors="coerce")
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    for column in ["open", "high", "low", "close", "vol", "amount"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def _format_cached_intraday_source(df: pd.DataFrame) -> str:
    if df is None or df.empty or "source" not in df.columns:
        return "db"

    non_null_sources = [
        str(value).strip()
        for value in df["source"].tolist()
        if value is not None and str(value).strip()
    ]
    if not non_null_sources:
        return "db"
    return f"db:{non_null_sources[0]}"


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
        return cached_df, _format_cached_intraday_source(cached_df)

    ts_code_text = normalize_ts_code_text(ts_code)
    mootdx_error = None
    try:
        fetched_df = fetch_stock_intraday_from_mootdx(ts_code_text, trade_date, freq=freq)
    except Exception as exc:
        mootdx_error = exc
        logger.warning("fetch_stock_intraday_from_mootdx failed for %s %s: %s", ts_code_text, trade_date, exc)
        fetched_df = empty_intraday_frame()

    if fetched_df is not None and not fetched_df.empty:
        upsert_stock_intraday_timeseries(engine, fetched_df, source="mootdx.minutes")
        refreshed_df = get_stock_intraday_timeseries(ts_code_text, trade_date, freq=freq, engine=engine)
        if refreshed_df is not None and not refreshed_df.empty:
            return refreshed_df, "mootdx"
        return fetched_df, "mootdx"

    try:
        fetched_df = fetch_stock_intraday_from_tushare(ts_code_text, trade_date, freq=freq, pro=pro)
    except Exception as exc:
        if mootdx_error is not None:
            raise RuntimeError(f"mootdx 与 Tushare 拉取均失败：mootdx={mootdx_error}; tushare={exc}") from exc
        raise

    if fetched_df is None or fetched_df.empty:
        return empty_intraday_frame(), "fallback-empty"

    upsert_stock_intraday_timeseries(engine, fetched_df, source="tushare.stk_mins")
    refreshed_df = get_stock_intraday_timeseries(ts_code_text, trade_date, freq=freq, engine=engine)
    if refreshed_df is not None and not refreshed_df.empty:
        return refreshed_df, "tushare"
    return fetched_df, "tushare"
