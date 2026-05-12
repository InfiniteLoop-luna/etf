from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Sequence

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

try:
    from psycopg2.extras import execute_values as _pg_execute_values
except Exception:  # pragma: no cover - fallback for environments without psycopg2 extras
    _pg_execute_values = None

from src.sync_tushare_security_data import build_db_url as _sync_build_db_url

UNIVERSE_TABLE = "ml_stock_universe_daily"
FEATURE_TABLE = "ml_stock_feature_daily"
LABEL_TABLE = "ml_stock_label_daily"
SAMPLE_VIEW = "ml_stock_sample_daily"

DEFAULT_MIN_HISTORY_DAYS = 60
DEFAULT_FEATURE_HISTORY_BUFFER_DAYS = 180
DEFAULT_FUTURE_BUFFER_DAYS = 90

RETURN_HORIZONS = (1, 3, 5, 10, 20)
DRAWDOWN_HORIZONS = (3, 5, 10, 20)
UPSIDE_HORIZONS = (5, 20)
FEATURE_RETURN_HORIZONS = (1, 3, 5, 10, 20, 60)
FEATURE_MA_WINDOWS = (5, 20, 60)
FEATURE_VOLATILITY_WINDOWS = (5, 20)
FEATURE_DISTANCE_WINDOWS = (20, 60)

FINANCIAL_VIEWS = (
    "vw_ts_stock_income",
    "vw_ts_stock_balancesheet",
    "vw_ts_stock_cashflow",
    "vw_ts_stock_fina_indicator",
)

FEATURE_SOURCE_NUMERIC_COLUMNS = (
    "open",
    "high",
    "low",
    "close",
    "vol",
    "amount",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "pe",
    "pe_ttm",
    "pb",
    "ps",
    "ps_ttm",
    "dv_ratio",
    "dv_ttm",
    "total_mv",
    "circ_mv",
    "w_ema5",
    "w_ema30",
    "m_ema5",
    "m_ema30",
)

FEATURE_SOURCE_COLUMNS = (
    "trade_date",
    "ts_code",
    *FEATURE_SOURCE_NUMERIC_COLUMNS,
    "is_weekly_ema_bearish",
    "is_monthly_ema_bearish",
)

SAMPLE_KEY_COLUMNS = (
    "trade_date",
    "ts_code",
)

UNIVERSE_COLUMNS = (
    "trade_date",
    "ts_code",
    "symbol",
    "name",
    "industry",
    "market",
    "exchange",
    "list_date",
    "listing_days",
    "list_status",
    "is_current_st",
    "has_ever_st",
    "has_price",
    "has_daily_basic",
    "has_financial",
    "min_history_ok",
    "sample_eligible",
    "created_at",
    "updated_at",
)

FEATURE_COLUMNS = (
    "trade_date",
    "ts_code",
    "close",
    "open",
    "high",
    "low",
    "vol",
    "amount",
    "ret_1d",
    "ret_3d",
    "ret_5d",
    "ret_10d",
    "ret_20d",
    "ret_60d",
    "close_over_ma5",
    "close_over_ma20",
    "close_over_ma60",
    "ma5_over_ma20",
    "ma20_over_ma60",
    "volatility_5d",
    "volatility_20d",
    "distance_to_20d_high",
    "distance_to_20d_low",
    "distance_to_60d_high",
    "distance_to_60d_low",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "vol_ma5_ratio",
    "amount_ma5_ratio",
    "pe",
    "pe_ttm",
    "pb",
    "ps",
    "ps_ttm",
    "dv_ratio",
    "dv_ttm",
    "total_mv",
    "circ_mv",
    "log_total_mv",
    "log_circ_mv",
    "w_ema5",
    "w_ema30",
    "m_ema5",
    "m_ema30",
    "is_weekly_ema_bearish",
    "is_monthly_ema_bearish",
    "w_ema5_over_30",
    "m_ema5_over_30",
    "has_price_feature",
    "has_daily_basic_feature",
    "has_technical_signal_feature",
    "feature_complete_ratio",
    "quality_flag",
    "created_at",
    "updated_at",
)

LABEL_COLUMNS = (
    "trade_date",
    "ts_code",
    "y_up_1d",
    "y_up_3d",
    "y_up_5d",
    "y_up_10d",
    "y_up_20d",
    "ret_fwd_1d",
    "ret_fwd_3d",
    "ret_fwd_5d",
    "ret_fwd_10d",
    "ret_fwd_20d",
    "max_dd_fwd_3d",
    "max_dd_fwd_5d",
    "max_dd_fwd_10d",
    "max_dd_fwd_20d",
    "max_upside_fwd_5d",
    "max_upside_fwd_20d",
    "future_price_available_5d",
    "future_price_available_20d",
    "suspended_in_horizon_flag",
    "entry_price",
    "entry_basis",
    "created_at",
    "updated_at",
)


def build_db_url():
    return _sync_build_db_url()


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _is_missing(value) -> bool:
    if value is None:
        return True
    try:
        result = pd.isna(value)
    except Exception:
        return False
    return bool(result) if isinstance(result, (bool, np.bool_)) else False


def _to_date(value) -> date | None:
    if _is_missing(value):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _to_timestamp_series(values) -> pd.Series:
    return pd.to_datetime(pd.Series(values), errors="coerce")


def _to_float(value) -> float | None:
    if _is_missing(value):
        return None
    try:
        number = float(value)
    except Exception:
        return None
    return number if np.isfinite(number) else None


def _to_int(value) -> int | None:
    if _is_missing(value):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _to_bool(value) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if _is_missing(value):
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "yes", "y"}
    return bool(value)


def _normalize_record_value(value):
    if _is_missing(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return value
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def _normalize_records(records: Iterable[dict]) -> list[dict]:
    normalized: list[dict] = []
    for record in records:
        normalized.append({key: _normalize_record_value(value) for key, value in record.items()})
    return normalized


def _bulk_upsert_rows(
    engine: Engine,
    table_name: str,
    insert_columns: Sequence[str],
    update_columns: Sequence[str],
    rows: list[dict],
    conflict_columns: Sequence[str] = ("trade_date", "ts_code"),
    page_size: int = 500,
) -> int:
    normalized_rows = _normalize_records(rows)
    if not normalized_rows:
        return 0

    insert_columns_sql = ", ".join(insert_columns)
    conflict_columns_sql = ", ".join(conflict_columns)
    update_assignments = [f"{column} = EXCLUDED.{column}" for column in update_columns]
    if "updated_at" in insert_columns:
        update_assignments.append("updated_at = NOW()")
    update_assignments_sql = ",\n            ".join(update_assignments)

    if _pg_execute_values is None:
        insert_sql = text(
            f"""
            INSERT INTO {table_name} (
                {insert_columns_sql}
            ) VALUES (
                {", ".join(f":{column}" for column in insert_columns)}
            )
            ON CONFLICT ({conflict_columns_sql}) DO UPDATE SET
                {update_assignments_sql};
            """
        )
        with engine.begin() as conn:
            conn.execute(insert_sql, normalized_rows)
        return len(normalized_rows)

    values = [tuple(record.get(column) for column in insert_columns) for record in normalized_rows]
    insert_sql = f"""
        INSERT INTO {table_name} (
            {insert_columns_sql}
        ) VALUES %s
        ON CONFLICT ({conflict_columns_sql}) DO UPDATE SET
            {update_assignments_sql};
    """

    raw_conn = engine.raw_connection()
    cursor = raw_conn.cursor()
    try:
        _pg_execute_values(cursor, insert_sql, values, page_size=page_size)
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        cursor.close()
        raw_conn.close()

    return len(normalized_rows)


def _safe_ratio_series(numerator: pd.Series, denominator: pd.Series, offset: float = 0.0) -> pd.Series:
    numerator_values = pd.to_numeric(numerator, errors="coerce").to_numpy(dtype=float)
    denominator_values = pd.to_numeric(denominator, errors="coerce").to_numpy(dtype=float)
    valid_mask = (
        np.isfinite(numerator_values)
        & np.isfinite(denominator_values)
        & (denominator_values != 0)
    )
    result = np.full(len(numerator_values), np.nan, dtype=float)
    np.divide(numerator_values, denominator_values, out=result, where=valid_mask)
    if offset:
        result[valid_mask] = result[valid_mask] + float(offset)
    return pd.Series(result, index=numerator.index, dtype=float)


def _safe_log_series(values: pd.Series) -> pd.Series:
    numeric_values = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    valid_mask = np.isfinite(numeric_values) & (numeric_values > 0)
    result = np.full(len(numeric_values), np.nan, dtype=float)
    result[valid_mask] = np.log(numeric_values[valid_mask])
    return pd.Series(result, index=values.index, dtype=float)


def compute_feature_quality_flag(
    has_price_feature,
    has_daily_basic_feature,
    has_technical_signal_feature,
) -> str:
    has_price = bool(has_price_feature)
    has_daily_basic = bool(has_daily_basic_feature)
    has_technical_signal = bool(has_technical_signal_feature)

    if has_price and has_daily_basic and has_technical_signal:
        return "complete"
    if has_price and (has_daily_basic or has_technical_signal):
        return "partial"
    if has_price:
        return "price_only"
    return "insufficient"


def compute_listing_days(trade_date, list_date) -> int | None:
    trade_dt = _to_date(trade_date)
    list_dt = _to_date(list_date)
    if trade_dt is None or list_dt is None or trade_dt < list_dt:
        return None
    return (trade_dt - list_dt).days + 1


def is_st_name(name) -> bool:
    if _is_missing(name):
        return False
    text_value = str(name).strip().upper()
    return (
        text_value.startswith("ST")
        or text_value.startswith("*ST")
        or text_value.startswith("S*ST")
        or text_value.startswith("SST")
    )


def is_active_stock(list_status, delist_date=None) -> bool:
    status = str(list_status or "").strip().upper()
    delist_dt = _to_date(delist_date)
    return status == "L" or (status == "" and delist_dt is None)


def build_universe_rows(
    source_df: pd.DataFrame,
    min_history_days: int = DEFAULT_MIN_HISTORY_DAYS,
    now: datetime | None = None,
) -> list[dict]:
    if source_df is None or source_df.empty:
        return []

    timestamp_now = now or _utcnow()
    rows: list[dict] = []

    for record in source_df.to_dict(orient="records"):
        trade_date = _to_date(record.get("trade_date"))
        list_date = _to_date(record.get("list_date"))
        delist_date = _to_date(record.get("delist_date"))
        first_financial_ann_date = _to_date(record.get("first_financial_ann_date"))

        effective_name = record.get("historical_name") or record.get("name")
        close_value = _to_float(record.get("close"))
        daily_basic_close = _to_float(record.get("daily_basic_close"))
        price_history_bars = _to_int(record.get("price_history_bars"))

        has_price = close_value is not None and close_value > 0
        has_daily_basic = daily_basic_close is not None
        has_financial = (
            trade_date is not None
            and first_financial_ann_date is not None
            and first_financial_ann_date <= trade_date
        )
        min_history_ok = price_history_bars is not None and price_history_bars >= int(min_history_days)
        sample_eligible = (
            is_active_stock(record.get("list_status"), delist_date)
            and has_price
            and min_history_ok
        )

        rows.append(
            {
                "trade_date": trade_date,
                "ts_code": str(record.get("ts_code") or "").strip() or None,
                "symbol": record.get("symbol"),
                "name": effective_name,
                "industry": record.get("industry"),
                "market": record.get("market"),
                "exchange": record.get("exchange"),
                "list_date": list_date,
                "listing_days": compute_listing_days(trade_date, list_date),
                "list_status": record.get("list_status"),
                "is_current_st": is_st_name(effective_name),
                "has_ever_st": _to_bool(record.get("has_ever_st")),
                "has_price": has_price,
                "has_daily_basic": has_daily_basic,
                "has_financial": has_financial,
                "min_history_ok": min_history_ok,
                "sample_eligible": sample_eligible,
                "created_at": timestamp_now,
                "updated_at": timestamp_now,
            }
        )

    return rows


def _build_empty_label_frame() -> pd.DataFrame:
    columns = [
        "trade_date",
        "ts_code",
        "y_up_1d",
        "y_up_3d",
        "y_up_5d",
        "y_up_10d",
        "y_up_20d",
        "ret_fwd_1d",
        "ret_fwd_3d",
        "ret_fwd_5d",
        "ret_fwd_10d",
        "ret_fwd_20d",
        "max_dd_fwd_3d",
        "max_dd_fwd_5d",
        "max_dd_fwd_10d",
        "max_dd_fwd_20d",
        "max_upside_fwd_5d",
        "max_upside_fwd_20d",
        "future_price_available_5d",
        "future_price_available_20d",
        "suspended_in_horizon_flag",
        "entry_price",
        "entry_basis",
        "created_at",
        "updated_at",
    ]
    return pd.DataFrame(columns=columns)


def _normalize_market_trade_dates(
    price_df: pd.DataFrame,
    market_trade_dates: Sequence | pd.DatetimeIndex | None = None,
) -> pd.DatetimeIndex:
    if market_trade_dates is None:
        values = price_df.get("trade_date", pd.Series(dtype="datetime64[ns]"))
    else:
        values = list(market_trade_dates)
    parsed = pd.to_datetime(pd.Series(values), errors="coerce").dropna().drop_duplicates().sort_values()
    return pd.DatetimeIndex(parsed.tolist())


def _build_group_label_frame(
    group: pd.DataFrame,
    market_trade_dates: pd.DatetimeIndex,
    market_index: dict[pd.Timestamp, int],
    now: datetime,
) -> pd.DataFrame:
    ordered = group.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="last").reset_index(drop=True)
    trade_dates = pd.to_datetime(ordered["trade_date"], errors="coerce")
    close = pd.to_numeric(ordered["close"], errors="coerce").to_numpy(dtype=float)
    high = pd.to_numeric(ordered["high"], errors="coerce").to_numpy(dtype=float)
    low = pd.to_numeric(ordered["low"], errors="coerce").to_numpy(dtype=float)

    size = len(ordered)
    ret_arrays = {h: np.full(size, np.nan, dtype=float) for h in RETURN_HORIZONS}
    y_arrays = {h: np.full(size, None, dtype=object) for h in RETURN_HORIZONS}
    dd_arrays = {h: np.full(size, np.nan, dtype=float) for h in DRAWDOWN_HORIZONS}
    upside_arrays = {h: np.full(size, np.nan, dtype=float) for h in UPSIDE_HORIZONS}
    future_available_5d = np.zeros(size, dtype=bool)
    future_available_20d = np.zeros(size, dtype=bool)
    suspended_flags = np.zeros(size, dtype=bool)

    max_horizon = max(RETURN_HORIZONS)

    for idx in range(size):
        entry_price = close[idx]
        if not np.isfinite(entry_price) or entry_price <= 0:
            continue

        current_trade_date = trade_dates.iloc[idx]
        current_market_index = market_index.get(current_trade_date)
        if current_market_index is not None:
            max_steps = min(
                max_horizon,
                size - idx - 1,
                len(market_trade_dates) - current_market_index - 1,
            )
            for step in range(1, max_steps + 1):
                expected_trade_date = market_trade_dates[current_market_index + step]
                actual_trade_date = trade_dates.iloc[idx + step]
                if actual_trade_date != expected_trade_date:
                    suspended_flags[idx] = True
                    break

        for horizon in RETURN_HORIZONS:
            future_idx = idx + horizon
            if future_idx >= size or not np.isfinite(close[future_idx]):
                continue

            ret_value = close[future_idx] / entry_price - 1.0
            ret_arrays[horizon][idx] = ret_value
            y_arrays[horizon][idx] = bool(ret_value > 0)

            if horizon == 5:
                future_available_5d[idx] = True
            if horizon == 20:
                future_available_20d[idx] = True

        for horizon in DRAWDOWN_HORIZONS:
            future_idx = idx + horizon
            if future_idx >= size:
                continue
            low_window = low[idx + 1 : future_idx + 1]
            if len(low_window) == horizon and np.all(np.isfinite(low_window)):
                dd_arrays[horizon][idx] = float(np.min(low_window) / entry_price - 1.0)

        for horizon in UPSIDE_HORIZONS:
            future_idx = idx + horizon
            if future_idx >= size:
                continue
            high_window = high[idx + 1 : future_idx + 1]
            if len(high_window) == horizon and np.all(np.isfinite(high_window)):
                upside_arrays[horizon][idx] = float(np.max(high_window) / entry_price - 1.0)

    label_df = pd.DataFrame(
        {
            "trade_date": trade_dates,
            "ts_code": ordered["ts_code"].astype(str),
            "entry_price": close,
            "entry_basis": "close",
            "future_price_available_5d": future_available_5d,
            "future_price_available_20d": future_available_20d,
            "suspended_in_horizon_flag": suspended_flags,
            "created_at": now,
            "updated_at": now,
        }
    )

    for horizon in RETURN_HORIZONS:
        label_df[f"ret_fwd_{horizon}d"] = ret_arrays[horizon]
        label_df[f"y_up_{horizon}d"] = y_arrays[horizon]

    for horizon in DRAWDOWN_HORIZONS:
        label_df[f"max_dd_fwd_{horizon}d"] = dd_arrays[horizon]

    for horizon in UPSIDE_HORIZONS:
        label_df[f"max_upside_fwd_{horizon}d"] = upside_arrays[horizon]

    return label_df


def build_forward_label_frame(
    price_df: pd.DataFrame,
    market_trade_dates: Sequence | pd.DatetimeIndex | None = None,
    now: datetime | None = None,
) -> pd.DataFrame:
    if price_df is None or price_df.empty:
        return _build_empty_label_frame()

    required_columns = {"trade_date", "ts_code", "close", "high", "low"}
    missing_columns = required_columns - set(price_df.columns)
    if missing_columns:
        raise ValueError(f"price_df 缺少必需列: {sorted(missing_columns)}")

    prepared = price_df.copy()
    prepared["trade_date"] = pd.to_datetime(prepared["trade_date"], errors="coerce")
    prepared = prepared.dropna(subset=["trade_date", "ts_code"]).copy()
    if prepared.empty:
        return _build_empty_label_frame()

    market_dates = _normalize_market_trade_dates(prepared, market_trade_dates=market_trade_dates)
    market_index = {trade_date: idx for idx, trade_date in enumerate(market_dates)}
    timestamp_now = now or _utcnow()

    frames = [
        _build_group_label_frame(group, market_dates, market_index, timestamp_now)
        for _, group in prepared.groupby("ts_code", sort=False)
    ]
    if not frames:
        return _build_empty_label_frame()

    combined = pd.concat(frames, ignore_index=True)
    return combined.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)


def build_label_rows(
    price_df: pd.DataFrame,
    market_trade_dates: Sequence | pd.DatetimeIndex | None = None,
    now: datetime | None = None,
) -> list[dict]:
    label_df = build_forward_label_frame(
        price_df,
        market_trade_dates=market_trade_dates,
        now=now,
    )
    return label_df.to_dict(orient="records")


def _build_empty_feature_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(FEATURE_COLUMNS))


def _prepare_feature_source_df(source_df: pd.DataFrame) -> pd.DataFrame:
    prepared = source_df.copy()
    for column in FEATURE_SOURCE_COLUMNS:
        if column not in prepared.columns:
            prepared[column] = pd.NA

    prepared = prepared.loc[:, list(FEATURE_SOURCE_COLUMNS)].copy()
    prepared["trade_date"] = pd.to_datetime(prepared["trade_date"], errors="coerce")
    prepared = prepared.dropna(subset=["trade_date", "ts_code"]).copy()
    prepared["ts_code"] = prepared["ts_code"].astype(str).str.strip()
    prepared = prepared[prepared["ts_code"] != ""].copy()

    for column in FEATURE_SOURCE_NUMERIC_COLUMNS:
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce")

    return prepared


def _build_group_feature_frame(group: pd.DataFrame, now: datetime) -> pd.DataFrame:
    ordered = group.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="last").reset_index(drop=True)
    if ordered.empty:
        return _build_empty_feature_frame()

    trade_dates = pd.to_datetime(ordered["trade_date"], errors="coerce")
    close = ordered["close"]
    open_price = ordered["open"]
    high = ordered["high"]
    low = ordered["low"]
    vol = ordered["vol"]
    amount = ordered["amount"]
    total_mv = ordered["total_mv"]
    circ_mv = ordered["circ_mv"]
    w_ema5 = ordered["w_ema5"]
    w_ema30 = ordered["w_ema30"]
    m_ema5 = ordered["m_ema5"]
    m_ema30 = ordered["m_ema30"]

    daily_ret = close.pct_change()
    moving_averages = {
        window: close.rolling(window, min_periods=window).mean()
        for window in FEATURE_MA_WINDOWS
    }
    rolling_highs = {
        window: high.rolling(window, min_periods=window).max()
        for window in FEATURE_DISTANCE_WINDOWS
    }
    rolling_lows = {
        window: low.rolling(window, min_periods=window).min()
        for window in FEATURE_DISTANCE_WINDOWS
    }
    vol_ma5 = vol.rolling(5, min_periods=5).mean()
    amount_ma5 = amount.rolling(5, min_periods=5).mean()

    has_price_feature = (
        open_price.notna()
        & high.notna()
        & low.notna()
        & close.notna()
        & (close > 0)
        & vol.notna()
        & amount.notna()
    )
    has_daily_basic_feature = ordered[
        [
            "turnover_rate",
            "turnover_rate_f",
            "volume_ratio",
            "pe",
            "pe_ttm",
            "pb",
            "ps",
            "ps_ttm",
            "dv_ratio",
            "dv_ttm",
            "total_mv",
            "circ_mv",
        ]
    ].notna().any(axis=1)
    has_technical_signal_feature = ordered[
        [
            "w_ema5",
            "w_ema30",
            "m_ema5",
            "m_ema30",
            "is_weekly_ema_bearish",
            "is_monthly_ema_bearish",
        ]
    ].notna().any(axis=1)

    feature_complete_ratio = (
        has_price_feature.astype(float)
        + has_daily_basic_feature.astype(float)
        + has_technical_signal_feature.astype(float)
    ) / 3.0
    quality_flags = [
        compute_feature_quality_flag(
            has_price_feature=price_flag,
            has_daily_basic_feature=daily_basic_flag,
            has_technical_signal_feature=technical_flag,
        )
        for price_flag, daily_basic_flag, technical_flag in zip(
            has_price_feature.tolist(),
            has_daily_basic_feature.tolist(),
            has_technical_signal_feature.tolist(),
        )
    ]

    feature_df = pd.DataFrame(
        {
            "trade_date": trade_dates,
            "ts_code": ordered["ts_code"].astype(str),
            "close": close,
            "open": open_price,
            "high": high,
            "low": low,
            "vol": vol,
            "amount": amount,
            "turnover_rate": ordered["turnover_rate"],
            "turnover_rate_f": ordered["turnover_rate_f"],
            "volume_ratio": ordered["volume_ratio"],
            "pe": ordered["pe"],
            "pe_ttm": ordered["pe_ttm"],
            "pb": ordered["pb"],
            "ps": ordered["ps"],
            "ps_ttm": ordered["ps_ttm"],
            "dv_ratio": ordered["dv_ratio"],
            "dv_ttm": ordered["dv_ttm"],
            "total_mv": total_mv,
            "circ_mv": circ_mv,
            "log_total_mv": _safe_log_series(total_mv),
            "log_circ_mv": _safe_log_series(circ_mv),
            "w_ema5": w_ema5,
            "w_ema30": w_ema30,
            "m_ema5": m_ema5,
            "m_ema30": m_ema30,
            "is_weekly_ema_bearish": ordered["is_weekly_ema_bearish"],
            "is_monthly_ema_bearish": ordered["is_monthly_ema_bearish"],
            "w_ema5_over_30": _safe_ratio_series(w_ema5, w_ema30),
            "m_ema5_over_30": _safe_ratio_series(m_ema5, m_ema30),
            "has_price_feature": has_price_feature,
            "has_daily_basic_feature": has_daily_basic_feature,
            "has_technical_signal_feature": has_technical_signal_feature,
            "feature_complete_ratio": feature_complete_ratio.round(6),
            "quality_flag": quality_flags,
            "created_at": now,
            "updated_at": now,
        }
    )

    for horizon in FEATURE_RETURN_HORIZONS:
        feature_df[f"ret_{horizon}d"] = close.pct_change(horizon)

    feature_df["close_over_ma5"] = _safe_ratio_series(close, moving_averages[5])
    feature_df["close_over_ma20"] = _safe_ratio_series(close, moving_averages[20])
    feature_df["close_over_ma60"] = _safe_ratio_series(close, moving_averages[60])
    feature_df["ma5_over_ma20"] = _safe_ratio_series(moving_averages[5], moving_averages[20])
    feature_df["ma20_over_ma60"] = _safe_ratio_series(moving_averages[20], moving_averages[60])
    feature_df["volatility_5d"] = daily_ret.rolling(5, min_periods=5).std(ddof=0)
    feature_df["volatility_20d"] = daily_ret.rolling(20, min_periods=20).std(ddof=0)
    feature_df["distance_to_20d_high"] = _safe_ratio_series(close, rolling_highs[20], offset=-1.0)
    feature_df["distance_to_20d_low"] = _safe_ratio_series(close, rolling_lows[20], offset=-1.0)
    feature_df["distance_to_60d_high"] = _safe_ratio_series(close, rolling_highs[60], offset=-1.0)
    feature_df["distance_to_60d_low"] = _safe_ratio_series(close, rolling_lows[60], offset=-1.0)
    feature_df["vol_ma5_ratio"] = _safe_ratio_series(vol, vol_ma5)
    feature_df["amount_ma5_ratio"] = _safe_ratio_series(amount, amount_ma5)

    return feature_df.loc[:, list(FEATURE_COLUMNS)]


def build_feature_frame(
    source_df: pd.DataFrame,
    now: datetime | None = None,
) -> pd.DataFrame:
    if source_df is None or source_df.empty:
        return _build_empty_feature_frame()

    prepared = _prepare_feature_source_df(source_df)
    if prepared.empty:
        return _build_empty_feature_frame()

    timestamp_now = now or _utcnow()
    frames = [
        _build_group_feature_frame(group, timestamp_now)
        for _, group in prepared.groupby("ts_code", sort=False)
    ]
    if not frames:
        return _build_empty_feature_frame()

    combined = pd.concat(frames, ignore_index=True)
    return combined.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)


def build_feature_rows(
    source_df: pd.DataFrame,
    now: datetime | None = None,
) -> list[dict]:
    feature_df = build_feature_frame(source_df, now=now)
    return feature_df.to_dict(orient="records")


def _prepare_sample_source_df(source_df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    if source_df is None:
        return pd.DataFrame(columns=list(SAMPLE_KEY_COLUMNS))

    missing_columns = set(SAMPLE_KEY_COLUMNS) - set(source_df.columns)
    if missing_columns:
        raise ValueError(f"{source_name} 缺少必需列: {sorted(missing_columns)}")

    prepared = source_df.copy()
    prepared["trade_date"] = pd.to_datetime(prepared["trade_date"], errors="coerce")
    prepared = prepared.dropna(subset=list(SAMPLE_KEY_COLUMNS)).copy()
    prepared["ts_code"] = prepared["ts_code"].astype(str).str.strip()
    prepared = prepared[prepared["ts_code"] != ""].copy()
    if prepared.empty:
        return prepared.reset_index(drop=True)

    return (
        prepared.sort_values(list(SAMPLE_KEY_COLUMNS))
        .drop_duplicates(subset=list(SAMPLE_KEY_COLUMNS), keep="last")
        .reset_index(drop=True)
    )


def _build_sample_column_specs(
    columns: Sequence[str],
    occupied_columns: set[str],
    prefix: str,
) -> list[tuple[str, str]]:
    column_specs: list[tuple[str, str]] = []
    for column in columns:
        if column in SAMPLE_KEY_COLUMNS:
            continue
        output_column = column if column not in occupied_columns else f"{prefix}_{column}"
        column_specs.append((column, output_column))
        occupied_columns.add(output_column)
    return column_specs


def _build_empty_sample_frame(
    universe_df: pd.DataFrame,
    feature_df: pd.DataFrame,
    label_df: pd.DataFrame,
) -> pd.DataFrame:
    occupied_columns = set(universe_df.columns)
    feature_specs = _build_sample_column_specs(feature_df.columns, occupied_columns, "feature")
    label_specs = _build_sample_column_specs(label_df.columns, occupied_columns, "label")
    sample_columns = (
        list(universe_df.columns)
        + [output_column for _, output_column in feature_specs]
        + [output_column for _, output_column in label_specs]
    )
    return pd.DataFrame(columns=sample_columns)


def build_sample_frame(
    universe_df: pd.DataFrame,
    feature_df: pd.DataFrame,
    label_df: pd.DataFrame,
    only_eligible: bool = True,
) -> pd.DataFrame:
    prepared_universe = _prepare_sample_source_df(universe_df, "universe_df")
    prepared_feature = _prepare_sample_source_df(feature_df, "feature_df")
    prepared_label = _prepare_sample_source_df(label_df, "label_df")

    if only_eligible:
        if "sample_eligible" not in prepared_universe.columns:
            raise ValueError("universe_df 缺少必需列: ['sample_eligible']")
        prepared_universe = prepared_universe[
            prepared_universe["sample_eligible"].map(_to_bool)
        ].copy()

    if prepared_universe.empty or prepared_feature.empty or prepared_label.empty:
        return _build_empty_sample_frame(prepared_universe, prepared_feature, prepared_label)

    feature_specs = _build_sample_column_specs(
        prepared_feature.columns,
        occupied_columns=set(prepared_universe.columns),
        prefix="feature",
    )
    label_specs = _build_sample_column_specs(
        prepared_label.columns,
        occupied_columns=set(prepared_universe.columns)
        | {output_column for _, output_column in feature_specs},
        prefix="label",
    )

    feature_payload = prepared_feature.loc[
        :,
        list(SAMPLE_KEY_COLUMNS) + [source_column for source_column, _ in feature_specs],
    ].rename(columns=dict(feature_specs))
    label_payload = prepared_label.loc[
        :,
        list(SAMPLE_KEY_COLUMNS) + [source_column for source_column, _ in label_specs],
    ].rename(columns=dict(label_specs))

    sample_df = prepared_universe.merge(feature_payload, on=list(SAMPLE_KEY_COLUMNS), how="inner")
    sample_df = sample_df.merge(label_payload, on=list(SAMPLE_KEY_COLUMNS), how="inner")
    return sample_df.sort_values(list(SAMPLE_KEY_COLUMNS)).reset_index(drop=True)


def ensure_universe_table(engine: Engine) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {UNIVERSE_TABLE} (
        trade_date DATE NOT NULL,
        ts_code VARCHAR(20) NOT NULL,
        symbol VARCHAR(16),
        name TEXT,
        industry TEXT,
        market TEXT,
        exchange TEXT,
        list_date DATE,
        listing_days INTEGER,
        list_status VARCHAR(8),
        is_current_st BOOLEAN,
        has_ever_st BOOLEAN,
        has_price BOOLEAN,
        has_daily_basic BOOLEAN,
        has_financial BOOLEAN,
        min_history_ok BOOLEAN,
        sample_eligible BOOLEAN,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (trade_date, ts_code)
    );
    CREATE INDEX IF NOT EXISTS idx_{UNIVERSE_TABLE}_trade_date
        ON {UNIVERSE_TABLE}(trade_date DESC);
    CREATE INDEX IF NOT EXISTS idx_{UNIVERSE_TABLE}_ts_code
        ON {UNIVERSE_TABLE}(ts_code);
    CREATE INDEX IF NOT EXISTS idx_{UNIVERSE_TABLE}_eligible
        ON {UNIVERSE_TABLE}(sample_eligible, trade_date DESC);
    """
    with engine.begin() as conn:
        for statement in [item.strip() for item in sql.split(";") if item.strip()]:
            conn.execute(text(statement))


def ensure_label_table(engine: Engine) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {LABEL_TABLE} (
        trade_date DATE NOT NULL,
        ts_code VARCHAR(20) NOT NULL,
        y_up_1d BOOLEAN,
        y_up_3d BOOLEAN,
        y_up_5d BOOLEAN,
        y_up_10d BOOLEAN,
        y_up_20d BOOLEAN,
        ret_fwd_1d NUMERIC(18, 6),
        ret_fwd_3d NUMERIC(18, 6),
        ret_fwd_5d NUMERIC(18, 6),
        ret_fwd_10d NUMERIC(18, 6),
        ret_fwd_20d NUMERIC(18, 6),
        max_dd_fwd_3d NUMERIC(18, 6),
        max_dd_fwd_5d NUMERIC(18, 6),
        max_dd_fwd_10d NUMERIC(18, 6),
        max_dd_fwd_20d NUMERIC(18, 6),
        max_upside_fwd_5d NUMERIC(18, 6),
        max_upside_fwd_20d NUMERIC(18, 6),
        future_price_available_5d BOOLEAN,
        future_price_available_20d BOOLEAN,
        suspended_in_horizon_flag BOOLEAN,
        entry_price NUMERIC(18, 6),
        entry_basis VARCHAR(32) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (trade_date, ts_code)
    );
    CREATE INDEX IF NOT EXISTS idx_{LABEL_TABLE}_trade_date
        ON {LABEL_TABLE}(trade_date DESC);
    CREATE INDEX IF NOT EXISTS idx_{LABEL_TABLE}_ts_code
        ON {LABEL_TABLE}(ts_code);
    """
    with engine.begin() as conn:
        for statement in [item.strip() for item in sql.split(";") if item.strip()]:
            conn.execute(text(statement))


def ensure_feature_table(engine: Engine) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {FEATURE_TABLE} (
        trade_date DATE NOT NULL,
        ts_code VARCHAR(20) NOT NULL,
        close NUMERIC(18, 6),
        open NUMERIC(18, 6),
        high NUMERIC(18, 6),
        low NUMERIC(18, 6),
        vol NUMERIC(24, 6),
        amount NUMERIC(24, 6),
        ret_1d NUMERIC(18, 6),
        ret_3d NUMERIC(18, 6),
        ret_5d NUMERIC(18, 6),
        ret_10d NUMERIC(18, 6),
        ret_20d NUMERIC(18, 6),
        ret_60d NUMERIC(18, 6),
        close_over_ma5 NUMERIC(18, 6),
        close_over_ma20 NUMERIC(18, 6),
        close_over_ma60 NUMERIC(18, 6),
        ma5_over_ma20 NUMERIC(18, 6),
        ma20_over_ma60 NUMERIC(18, 6),
        volatility_5d NUMERIC(18, 6),
        volatility_20d NUMERIC(18, 6),
        distance_to_20d_high NUMERIC(18, 6),
        distance_to_20d_low NUMERIC(18, 6),
        distance_to_60d_high NUMERIC(18, 6),
        distance_to_60d_low NUMERIC(18, 6),
        turnover_rate NUMERIC(18, 6),
        turnover_rate_f NUMERIC(18, 6),
        volume_ratio NUMERIC(18, 6),
        vol_ma5_ratio NUMERIC(18, 6),
        amount_ma5_ratio NUMERIC(18, 6),
        pe NUMERIC(18, 6),
        pe_ttm NUMERIC(18, 6),
        pb NUMERIC(18, 6),
        ps NUMERIC(18, 6),
        ps_ttm NUMERIC(18, 6),
        dv_ratio NUMERIC(18, 6),
        dv_ttm NUMERIC(18, 6),
        total_mv NUMERIC(24, 6),
        circ_mv NUMERIC(24, 6),
        log_total_mv NUMERIC(18, 6),
        log_circ_mv NUMERIC(18, 6),
        w_ema5 NUMERIC(18, 6),
        w_ema30 NUMERIC(18, 6),
        m_ema5 NUMERIC(18, 6),
        m_ema30 NUMERIC(18, 6),
        is_weekly_ema_bearish BOOLEAN,
        is_monthly_ema_bearish BOOLEAN,
        w_ema5_over_30 NUMERIC(18, 6),
        m_ema5_over_30 NUMERIC(18, 6),
        has_price_feature BOOLEAN,
        has_daily_basic_feature BOOLEAN,
        has_technical_signal_feature BOOLEAN,
        feature_complete_ratio NUMERIC(8, 6),
        quality_flag VARCHAR(32),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (trade_date, ts_code)
    );
    CREATE INDEX IF NOT EXISTS idx_{FEATURE_TABLE}_trade_date
        ON {FEATURE_TABLE}(trade_date DESC);
    CREATE INDEX IF NOT EXISTS idx_{FEATURE_TABLE}_ts_code
        ON {FEATURE_TABLE}(ts_code);
    CREATE INDEX IF NOT EXISTS idx_{FEATURE_TABLE}_quality
        ON {FEATURE_TABLE}(quality_flag, trade_date DESC);
    """
    with engine.begin() as conn:
        for statement in [item.strip() for item in sql.split(";") if item.strip()]:
            conn.execute(text(statement))


def ensure_storage_objects(engine: Engine) -> None:
    ensure_universe_table(engine)
    ensure_feature_table(engine)
    ensure_label_table(engine)


def ensure_sample_view(engine: Engine) -> None:
    occupied_columns = set(UNIVERSE_COLUMNS)
    feature_specs = _build_sample_column_specs(FEATURE_COLUMNS, occupied_columns, "feature")
    label_specs = _build_sample_column_specs(LABEL_COLUMNS, occupied_columns, "label")

    select_columns = [f"u.{column}" for column in UNIVERSE_COLUMNS]
    select_columns.extend(
        f"f.{source_column}"
        if source_column == output_column
        else f"f.{source_column} AS {output_column}"
        for source_column, output_column in feature_specs
    )
    select_columns.extend(
        f"l.{source_column}"
        if source_column == output_column
        else f"l.{source_column} AS {output_column}"
        for source_column, output_column in label_specs
    )

    sql = f"""
    CREATE OR REPLACE VIEW {SAMPLE_VIEW} AS
    SELECT
        {", ".join(select_columns)}
    FROM {UNIVERSE_TABLE} u
    JOIN {FEATURE_TABLE} f
      ON u.trade_date = f.trade_date
     AND u.ts_code = f.ts_code
    JOIN {LABEL_TABLE} l
      ON u.trade_date = l.trade_date
     AND u.ts_code = l.ts_code
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def _build_financial_ann_cte_sql() -> str:
    union_sql = "\nUNION ALL\n".join(
        f"SELECT ts_code, ann_date FROM {view_name} WHERE ann_date IS NOT NULL"
        for view_name in FINANCIAL_VIEWS
    )
    return f"""
    financial_ann AS (
        SELECT ts_code, MIN(ann_date) AS first_financial_ann_date
        FROM (
            {union_sql}
        ) t
        GROUP BY ts_code
    )
    """


def load_universe_source_df(
    engine: Engine,
    start_date=None,
    end_date=None,
) -> pd.DataFrame:
    start_dt = _to_date(start_date)
    end_dt = _to_date(end_date)

    sql = f"""
    WITH
    {_build_financial_ann_cte_sql()},
    st_history AS (
        SELECT DISTINCT ts_code
        FROM vw_ts_stock_namechange
        WHERE (
            COALESCE(name, '') LIKE 'ST%%'
            OR COALESCE(name, '') LIKE '*ST%%'
            OR COALESCE(name, '') LIKE 'S*ST%%'
            OR COALESCE(name, '') LIKE 'SST%%'
        )
    ),
    target_rows AS (
        SELECT
            d.trade_date,
            d.ts_code,
            b.symbol,
            COALESCE(nc_hist.name, b.name) AS historical_name,
            b.name,
            b.industry,
            b.market,
            COALESCE(c.exchange, b.exchange) AS exchange,
            b.list_date,
            b.list_status,
            b.delist_date,
            d.close,
            db.close AS daily_basic_close,
            fa.first_financial_ann_date,
            (st.ts_code IS NOT NULL) AS has_ever_st
        FROM vw_ts_stock_daily d
        JOIN vw_ts_stock_basic b
          ON d.ts_code = b.ts_code
        LEFT JOIN vw_ts_stock_company c
          ON d.ts_code = c.ts_code
        LEFT JOIN vw_ts_stock_daily_basic db
          ON db.ts_code = d.ts_code
         AND db.trade_date = d.trade_date
        LEFT JOIN financial_ann fa
          ON fa.ts_code = d.ts_code
        LEFT JOIN st_history st
          ON st.ts_code = d.ts_code
        LEFT JOIN LATERAL (
            SELECT nc.name
            FROM vw_ts_stock_namechange nc
            WHERE nc.ts_code = d.ts_code
              AND nc.start_date IS NOT NULL
              AND nc.start_date <= d.trade_date
              AND (
                    COALESCE(nc.nc_end_date, nc.end_date) IS NULL
                    OR COALESCE(nc.nc_end_date, nc.end_date) >= d.trade_date
              )
            ORDER BY nc.start_date DESC, COALESCE(nc.nc_ann_date, nc.ann_date) DESC NULLS LAST
            LIMIT 1
        ) nc_hist ON TRUE
        WHERE (:start_date IS NULL OR d.trade_date >= :start_date)
          AND (:end_date IS NULL OR d.trade_date <= :end_date)
    ),
    price_history AS (
        SELECT d.ts_code, t.trade_date, COUNT(*) AS price_history_bars
        FROM vw_ts_stock_daily d
        JOIN (
            SELECT DISTINCT ts_code, trade_date
            FROM target_rows
        ) t
          ON t.ts_code = d.ts_code
         AND d.trade_date <= t.trade_date
        GROUP BY d.ts_code, t.trade_date
    )
    SELECT
        t.trade_date,
        t.ts_code,
        t.symbol,
        t.historical_name,
        t.name,
        t.industry,
        t.market,
        t.exchange,
        t.list_date,
        t.list_status,
        t.delist_date,
        t.close,
        t.daily_basic_close,
        t.first_financial_ann_date,
        t.has_ever_st,
        COALESCE(p.price_history_bars, 0) AS price_history_bars
    FROM target_rows t
    LEFT JOIN price_history p
      ON p.ts_code = t.ts_code
     AND p.trade_date = t.trade_date
    ORDER BY t.trade_date, t.ts_code
    """

    with engine.connect() as conn:
        return pd.read_sql(
            text(sql),
            conn,
            params={"start_date": start_dt, "end_date": end_dt},
        )


def upsert_universe_rows(engine: Engine, rows: list[dict]) -> int:
    if not rows:
        return 0

    ensure_universe_table(engine)
    update_columns = [
        column
        for column in UNIVERSE_COLUMNS
        if column not in {"trade_date", "ts_code", "created_at", "updated_at"}
    ]
    return _bulk_upsert_rows(
        engine,
        UNIVERSE_TABLE,
        UNIVERSE_COLUMNS,
        update_columns,
        rows,
    )


def _delete_rows(engine: Engine, table_name: str, start_date=None, end_date=None) -> None:
    start_dt = _to_date(start_date)
    end_dt = _to_date(end_date)

    if start_dt is None and end_dt is None:
        sql = text(f"TRUNCATE TABLE {table_name}")
        with engine.begin() as conn:
            conn.execute(sql)
        return

    conditions = []
    params: dict[str, object] = {}
    if start_dt is not None:
        conditions.append("trade_date >= :start_date")
        params["start_date"] = start_dt
    if end_dt is not None:
        conditions.append("trade_date <= :end_date")
        params["end_date"] = end_dt

    sql = text(f"DELETE FROM {table_name} WHERE {' AND '.join(conditions)}")
    with engine.begin() as conn:
        conn.execute(sql, params)


def build_universe_dataset(
    engine: Engine,
    start_date=None,
    end_date=None,
    min_history_days: int = DEFAULT_MIN_HISTORY_DAYS,
    rebuild: bool = False,
) -> dict:
    ensure_universe_table(engine)
    source_df = load_universe_source_df(engine, start_date=start_date, end_date=end_date)
    rows = build_universe_rows(source_df, min_history_days=min_history_days)

    if rebuild:
        _delete_rows(engine, UNIVERSE_TABLE, start_date=start_date, end_date=end_date)

    written = upsert_universe_rows(engine, rows)
    eligible_count = sum(1 for row in rows if row.get("sample_eligible"))
    return {
        "source_rows": int(len(source_df)),
        "rows_written": int(written),
        "eligible_rows": int(eligible_count),
        "start_date": str(_to_date(start_date)) if _to_date(start_date) else None,
        "end_date": str(_to_date(end_date)) if _to_date(end_date) else None,
        "min_history_days": int(min_history_days),
    }


def load_label_target_df(engine: Engine, start_date=None, end_date=None) -> pd.DataFrame:
    ensure_universe_table(engine)
    start_dt = _to_date(start_date)
    end_dt = _to_date(end_date)
    sql = text(
        f"""
        SELECT trade_date, ts_code
        FROM {UNIVERSE_TABLE}
        WHERE (:start_date IS NULL OR trade_date >= :start_date)
          AND (:end_date IS NULL OR trade_date <= :end_date)
        ORDER BY trade_date, ts_code
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"start_date": start_dt, "end_date": end_dt})


def load_feature_target_df(engine: Engine, start_date=None, end_date=None) -> pd.DataFrame:
    return load_label_target_df(engine, start_date=start_date, end_date=end_date)


def load_label_price_df(
    engine: Engine,
    start_date=None,
    end_date=None,
    future_buffer_days: int = DEFAULT_FUTURE_BUFFER_DAYS,
) -> pd.DataFrame:
    target_df = load_label_target_df(engine, start_date=start_date, end_date=end_date)
    if target_df.empty:
        return pd.DataFrame(columns=["trade_date", "ts_code", "open", "high", "low", "close"])

    min_target_date = pd.to_datetime(target_df["trade_date"]).min().date()
    max_target_date = pd.to_datetime(target_df["trade_date"]).max().date()
    price_end_date = max_target_date + timedelta(days=int(future_buffer_days))

    sql = text(
        f"""
        WITH target_codes AS (
            SELECT DISTINCT ts_code
            FROM {UNIVERSE_TABLE}
            WHERE (:start_date IS NULL OR trade_date >= :start_date)
              AND (:end_date IS NULL OR trade_date <= :end_date)
        )
        SELECT d.trade_date, d.ts_code, d.open, d.high, d.low, d.close
        FROM vw_ts_stock_daily d
        JOIN target_codes tc
          ON tc.ts_code = d.ts_code
        WHERE d.trade_date >= :price_start_date
          AND d.trade_date <= :price_end_date
        ORDER BY d.ts_code, d.trade_date
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(
            sql,
            conn,
            params={
                "start_date": _to_date(start_date),
                "end_date": _to_date(end_date),
                "price_start_date": min_target_date,
                "price_end_date": price_end_date,
            },
        )


def load_feature_source_df(
    engine: Engine,
    start_date=None,
    end_date=None,
    history_buffer_days: int = DEFAULT_FEATURE_HISTORY_BUFFER_DAYS,
) -> pd.DataFrame:
    target_df = load_feature_target_df(engine, start_date=start_date, end_date=end_date)
    if target_df.empty:
        return pd.DataFrame(columns=list(FEATURE_SOURCE_COLUMNS))

    min_target_date = pd.to_datetime(target_df["trade_date"]).min().date()
    max_target_date = pd.to_datetime(target_df["trade_date"]).max().date()
    feature_start_date = min_target_date - timedelta(days=int(history_buffer_days))

    sql = text(
        f"""
        WITH target_codes AS (
            SELECT DISTINCT ts_code
            FROM {UNIVERSE_TABLE}
            WHERE (:start_date IS NULL OR trade_date >= :start_date)
              AND (:end_date IS NULL OR trade_date <= :end_date)
        )
        SELECT
            d.trade_date,
            d.ts_code,
            d.open,
            d.high,
            d.low,
            d.close,
            d.vol,
            d.amount,
            db.turnover_rate,
            db.turnover_rate_f,
            db.volume_ratio,
            db.pe,
            db.pe_ttm,
            db.pb,
            db.ps,
            db.ps_ttm,
            db.dv_ratio,
            db.dv_ttm,
            db.total_mv,
            db.circ_mv,
            sig.w_ema5,
            sig.w_ema30,
            sig.m_ema5,
            sig.m_ema30,
            sig.is_weekly_ema_bearish,
            sig.is_monthly_ema_bearish
        FROM vw_ts_stock_daily d
        JOIN target_codes tc
          ON tc.ts_code = d.ts_code
        LEFT JOIN vw_ts_stock_daily_basic db
          ON db.ts_code = d.ts_code
         AND db.trade_date = d.trade_date
        LEFT JOIN ts_stock_technical_signals sig
          ON sig.ts_code = d.ts_code
         AND sig.trade_date = d.trade_date
        WHERE d.trade_date >= :feature_start_date
          AND d.trade_date <= :feature_end_date
        ORDER BY d.ts_code, d.trade_date
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(
            sql,
            conn,
            params={
                "start_date": _to_date(start_date),
                "end_date": _to_date(end_date),
                "feature_start_date": feature_start_date,
                "feature_end_date": max_target_date,
            },
        )


def load_market_trade_dates(
    engine: Engine,
    start_date,
    end_date,
) -> pd.DatetimeIndex:
    sql = text(
        """
        SELECT DISTINCT trade_date
        FROM vw_ts_stock_daily
        WHERE trade_date >= :start_date
          AND trade_date <= :end_date
        ORDER BY trade_date
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"start_date": start_date, "end_date": end_date})
    return pd.DatetimeIndex(pd.to_datetime(df["trade_date"], errors="coerce").dropna().tolist())


def upsert_label_rows(engine: Engine, rows: list[dict]) -> int:
    if not rows:
        return 0

    ensure_label_table(engine)
    update_columns = [
        column
        for column in LABEL_COLUMNS
        if column not in {"trade_date", "ts_code", "created_at", "updated_at"}
    ]
    return _bulk_upsert_rows(
        engine,
        LABEL_TABLE,
        LABEL_COLUMNS,
        update_columns,
        rows,
    )


def upsert_feature_rows(engine: Engine, rows: list[dict]) -> int:
    if not rows:
        return 0

    ensure_feature_table(engine)
    insert_columns = list(FEATURE_COLUMNS)
    update_columns = [
        column
        for column in insert_columns
        if column not in {"trade_date", "ts_code", "created_at", "updated_at"}
    ]
    return _bulk_upsert_rows(
        engine,
        FEATURE_TABLE,
        insert_columns,
        update_columns,
        rows,
    )


def build_label_dataset(
    engine: Engine,
    start_date=None,
    end_date=None,
    rebuild: bool = False,
    future_buffer_days: int = DEFAULT_FUTURE_BUFFER_DAYS,
) -> dict:
    ensure_label_table(engine)
    target_df = load_label_target_df(engine, start_date=start_date, end_date=end_date)
    if target_df.empty:
        return {
            "target_rows": 0,
            "price_rows": 0,
            "rows_written": 0,
            "start_date": str(_to_date(start_date)) if _to_date(start_date) else None,
            "end_date": str(_to_date(end_date)) if _to_date(end_date) else None,
        }

    price_df = load_label_price_df(
        engine,
        start_date=start_date,
        end_date=end_date,
        future_buffer_days=future_buffer_days,
    )
    min_target_date = pd.to_datetime(target_df["trade_date"]).min().date()
    max_target_date = pd.to_datetime(target_df["trade_date"]).max().date()
    market_trade_dates = load_market_trade_dates(
        engine,
        start_date=min_target_date,
        end_date=max_target_date + timedelta(days=int(future_buffer_days)),
    )

    label_df = build_forward_label_frame(price_df, market_trade_dates=market_trade_dates)
    target_keys = target_df.copy()
    target_keys["trade_date"] = pd.to_datetime(target_keys["trade_date"], errors="coerce")
    filtered = label_df.merge(target_keys, on=["trade_date", "ts_code"], how="inner")
    filtered = filtered.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)
    rows = filtered.to_dict(orient="records")

    if rebuild:
        _delete_rows(engine, LABEL_TABLE, start_date=start_date, end_date=end_date)

    written = upsert_label_rows(engine, rows)
    return {
        "target_rows": int(len(target_df)),
        "price_rows": int(len(price_df)),
        "rows_written": int(written),
        "start_date": str(_to_date(start_date)) if _to_date(start_date) else None,
        "end_date": str(_to_date(end_date)) if _to_date(end_date) else None,
        "future_buffer_days": int(future_buffer_days),
    }


def build_feature_dataset(
    engine: Engine,
    start_date=None,
    end_date=None,
    rebuild: bool = False,
    history_buffer_days: int = DEFAULT_FEATURE_HISTORY_BUFFER_DAYS,
) -> dict:
    ensure_feature_table(engine)
    target_df = load_feature_target_df(engine, start_date=start_date, end_date=end_date)
    if target_df.empty:
        return {
            "target_rows": 0,
            "source_rows": 0,
            "rows_written": 0,
            "complete_rows": 0,
            "start_date": str(_to_date(start_date)) if _to_date(start_date) else None,
            "end_date": str(_to_date(end_date)) if _to_date(end_date) else None,
            "history_buffer_days": int(history_buffer_days),
        }

    source_df = load_feature_source_df(
        engine,
        start_date=start_date,
        end_date=end_date,
        history_buffer_days=history_buffer_days,
    )
    feature_df = build_feature_frame(source_df)
    target_keys = target_df.copy()
    target_keys["trade_date"] = pd.to_datetime(target_keys["trade_date"], errors="coerce")
    filtered = feature_df.merge(target_keys, on=["trade_date", "ts_code"], how="inner")
    filtered = filtered.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)
    rows = filtered.to_dict(orient="records")

    if rebuild:
        _delete_rows(engine, FEATURE_TABLE, start_date=start_date, end_date=end_date)

    written = upsert_feature_rows(engine, rows)
    complete_rows = int((filtered["quality_flag"] == "complete").sum()) if not filtered.empty else 0
    return {
        "target_rows": int(len(target_df)),
        "source_rows": int(len(source_df)),
        "rows_written": int(written),
        "complete_rows": int(complete_rows),
        "start_date": str(_to_date(start_date)) if _to_date(start_date) else None,
        "end_date": str(_to_date(end_date)) if _to_date(end_date) else None,
        "history_buffer_days": int(history_buffer_days),
    }


def load_sample_dataset(
    engine: Engine,
    start_date=None,
    end_date=None,
    only_eligible: bool = True,
    limit: int | None = None,
) -> pd.DataFrame:
    ensure_storage_objects(engine)
    ensure_sample_view(engine)

    if limit is not None and int(limit) <= 0:
        raise ValueError("limit must be a positive integer")

    start_dt = _to_date(start_date)
    end_dt = _to_date(end_date)

    conditions = []
    params: dict[str, object] = {}
    if start_dt is not None:
        conditions.append("trade_date >= :start_date")
        params["start_date"] = start_dt
    if end_dt is not None:
        conditions.append("trade_date <= :end_date")
        params["end_date"] = end_dt
    if only_eligible:
        conditions.append("sample_eligible = TRUE")
    if limit is not None:
        params["limit"] = int(limit)

    sql = f"SELECT * FROM {SAMPLE_VIEW}"
    if conditions:
        sql = f"{sql} WHERE {' AND '.join(conditions)}"
    sql = f"{sql} ORDER BY trade_date, ts_code"
    if limit is not None:
        sql = f"{sql} LIMIT :limit"

    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def build_sample_dataset(
    engine: Engine,
    start_date=None,
    end_date=None,
    only_eligible: bool = True,
    limit: int | None = None,
    load: bool = False,
) -> dict:
    ensure_storage_objects(engine)
    ensure_sample_view(engine)

    rows_loaded = None
    column_count = None
    if load:
        sample_df = load_sample_dataset(
            engine,
            start_date=start_date,
            end_date=end_date,
            only_eligible=only_eligible,
            limit=limit,
        )
        rows_loaded = int(len(sample_df))
        column_count = int(len(sample_df.columns))

    return {
        "view_name": SAMPLE_VIEW,
        "view_refreshed": True,
        "rows_loaded": rows_loaded,
        "column_count": column_count,
        "start_date": str(_to_date(start_date)) if _to_date(start_date) else None,
        "end_date": str(_to_date(end_date)) if _to_date(end_date) else None,
        "only_eligible": bool(only_eligible),
        "limit": int(limit) if limit is not None else None,
        "load": bool(load),
    }
