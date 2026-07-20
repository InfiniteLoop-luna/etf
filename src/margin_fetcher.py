from __future__ import annotations

import argparse
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
from sqlalchemy.engine import Engine

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.moneyflow_fetcher import get_max_trade_date, get_required_trade_dates, upsert_rows
from src.volume_fetcher import _init_tushare

DEFAULT_START_DATE = "20220101"
DEFAULT_API_SLEEP = float(os.getenv("TUSHARE_MARGIN_API_SLEEP", "0.35"))
DEFAULT_LOOKBACK_DAYS = int(os.getenv("TUSHARE_MARGIN_LOOKBACK_DAYS", "2"))
DEFAULT_PUBLISH_CUTOFF_HOUR = int(os.getenv("TUSHARE_MARGIN_PUBLISH_CUTOFF_HOUR", "9"))

MARGIN_TABLES = {
    "margin": "ts_margin",
    "margin_detail": "ts_margin_detail",
}

MARGIN_FIELDS = (
    "trade_date,exchange_id,rzye,rzmre,rzche,rqye,rqmcl,rzrqye,rqyl"
)

MARGIN_DETAIL_FIELDS = (
    "trade_date,ts_code,name,rzye,rqye,rzmre,rqyl,rzche,rqchl,rqmcl,rzrqye"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
_ENGINE_CACHE: Optional[Engine] = None


def build_db_url():
    from src.sync_tushare_security_data import build_db_url as _sync_build_db_url

    return _sync_build_db_url()


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


def _get_engine_cached() -> Engine:
    global _ENGINE_CACHE
    if _ENGINE_CACHE is None:
        _ENGINE_CACHE = get_engine()
    return _ENGINE_CACHE


def ensure_landing_table(engine: Engine, table_name: str) -> None:
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
        for statement in [item.strip() for item in sql.split(";") if item.strip()]:
            conn.execute(text(statement))
    logger.info("表 %s 已就绪", table_name)


def ensure_normalized_views(engine: Engine) -> None:
    views = {
        "ts_margin": (
            "vw_margin",
            [
                "trade_date",
                "exchange_id AS exchange_id",
                "rzye::numeric AS rzye",
                "rzmre::numeric AS rzmre",
                "rzche::numeric AS rzche",
                "rqye::numeric AS rqye",
                "rqmcl::numeric AS rqmcl",
                "rzrqye::numeric AS rzrqye",
                "rqyl::numeric AS rqyl",
            ],
        ),
        "ts_margin_detail": (
            "vw_margin_detail",
            [
                "ts_code",
                "trade_date",
                "name AS name",
                "rzye::numeric AS rzye",
                "rqye::numeric AS rqye",
                "rzmre::numeric AS rzmre",
                "rqyl::numeric AS rqyl",
                "rzche::numeric AS rzche",
                "rqchl::numeric AS rqchl",
                "rqmcl::numeric AS rqmcl",
                "rzrqye::numeric AS rzrqye",
            ],
        ),
    }
    native_cols = {
        "business_key",
        "dataset_name",
        "ts_code",
        "trade_date",
        "ann_date",
        "end_date",
        "period",
        "record_hash",
        "ingested_at",
        "payload",
    }
    with engine.begin() as conn:
        for table_name, (view_name, columns) in views.items():
            select_parts = [
                "business_key",
                "dataset_name",
                "ts_code",
                "trade_date",
                "record_hash",
                "ingested_at",
                "payload",
            ]
            for col_expr in columns:
                if "::" in col_expr:
                    raw_part, rest = col_expr.split("::", 1)
                    raw = raw_part.strip()
                    cast_type, alias = rest.split(" AS ", 1)
                    alias = alias.strip()
                    if alias in native_cols or raw in native_cols:
                        continue
                    select_parts.append(
                        f"NULLIF(payload->>'{raw}', '')::{cast_type.strip()} AS {alias}"
                    )
                elif " AS " in col_expr:
                    raw, alias = col_expr.split(" AS ", 1)
                    raw = raw.strip()
                    alias = alias.strip()
                    if alias in native_cols or raw in native_cols:
                        continue
                    select_parts.append(f"NULLIF(payload->>'{raw}', '') AS {alias}")
                else:
                    field = col_expr.strip()
                    if field in native_cols:
                        continue
                    select_parts.append(f"NULLIF(payload->>'{field}', '') AS {field}")
            conn.execute(
                text(
                    f"""
                    CREATE OR REPLACE VIEW {view_name} AS
                    SELECT {', '.join(select_parts)}
                    FROM {table_name}
                    """
                )
            )
            logger.info("视图 %s 已更新", view_name)


def ensure_all_tables(engine: Engine) -> None:
    for table_name in MARGIN_TABLES.values():
        ensure_landing_table(engine, table_name)
    ensure_normalized_views(engine)


def resolve_start_date(
    engine: Engine,
    table_name: str,
    force_start: Optional[str] = None,
    lookback_days: int = 0,
) -> str:
    if force_start:
        return str(force_start).replace("-", "")
    existing_max = get_max_trade_date(engine, table_name)
    if not existing_max:
        return DEFAULT_START_DATE
    start_day = (
        datetime.strptime(existing_max, "%Y%m%d") - timedelta(days=max(0, int(lookback_days or 0)))
    ).strftime("%Y%m%d")
    return max(DEFAULT_START_DATE, start_day)


def sync_margin(
    engine: Engine,
    pro,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> int:
    table_name = MARGIN_TABLES["margin"]
    start = resolve_start_date(engine, table_name, start_date, lookback_days=DEFAULT_LOOKBACK_DAYS)
    end = str(end_date or datetime.now().strftime("%Y%m%d")).replace("-", "")
    if start > end:
        logger.info("margin: 数据已最新（%s > %s），跳过", start, end)
        return 0

    total = 0
    logger.info("margin: 拉取 %s -> %s", start, end)
    for trade_date in get_required_trade_dates(pro, start, end, DEFAULT_PUBLISH_CUTOFF_HOUR):
        try:
            df = pro.margin(trade_date=trade_date, fields=MARGIN_FIELDS)
            if df is None or df.empty:
                continue
            total += upsert_rows(
                engine,
                table_name,
                "margin",
                df.to_dict("records"),
                ts_code_fn=lambda row: row.get("exchange_id"),
            )
            logger.info("  margin %s: %s 行", trade_date, len(df))
        except Exception as exc:
            err_str = str(exc)
            if "积分" in err_str or "权限" in err_str or "抱歉" in err_str:
                logger.warning("  margin: 权限不足，跳过。%s", exc)
                break
            logger.warning("  margin %s 失败: %s", trade_date, exc)
        time.sleep(DEFAULT_API_SLEEP)
    return total


def sync_margin_detail(
    engine: Engine,
    pro,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> int:
    table_name = MARGIN_TABLES["margin_detail"]
    start = resolve_start_date(engine, table_name, start_date, lookback_days=DEFAULT_LOOKBACK_DAYS)
    end = str(end_date or datetime.now().strftime("%Y%m%d")).replace("-", "")
    if start > end:
        logger.info("margin_detail: 数据已最新（%s > %s），跳过", start, end)
        return 0

    total = 0
    logger.info("margin_detail: 拉取 %s -> %s", start, end)
    for trade_date in get_required_trade_dates(pro, start, end, DEFAULT_PUBLISH_CUTOFF_HOUR):
        try:
            df = pro.margin_detail(trade_date=trade_date, fields=MARGIN_DETAIL_FIELDS)
            if df is None or df.empty:
                continue
            total += upsert_rows(engine, table_name, "margin_detail", df.to_dict("records"))
            logger.info("  margin_detail %s: %s 行", trade_date, len(df))
        except Exception as exc:
            err_str = str(exc)
            if "积分" in err_str or "权限" in err_str or "抱歉" in err_str:
                logger.warning("  margin_detail: 权限不足，跳过。%s", exc)
                break
            logger.warning("  margin_detail %s 失败: %s", trade_date, exc)
        time.sleep(DEFAULT_API_SLEEP)
    return total


def run_sync(
    datasets: Optional[list[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict[str, int]:
    engine = _get_engine_cached()
    ensure_all_tables(engine)
    pro = _init_tushare()
    selected = datasets or list(MARGIN_TABLES.keys())
    results: dict[str, int] = {}
    for dataset in selected:
        if dataset == "margin":
            results[dataset] = sync_margin(engine, pro, start_date=start_date, end_date=end_date)
        elif dataset == "margin_detail":
            results[dataset] = sync_margin_detail(engine, pro, start_date=start_date, end_date=end_date)
        else:
            raise ValueError(f"不支持的数据集：{dataset}")
    return results


def get_margin_latest_date(engine: Optional[Engine] = None) -> Optional[str]:
    if engine is None:
        engine = _get_engine_cached()
    return get_max_trade_date(engine, MARGIN_TABLES["margin_detail"])


def query_margin_detail_history(
    ts_code: str,
    start_date: str = DEFAULT_START_DATE,
    end_date: Optional[str] = None,
    engine: Optional[Engine] = None,
) -> pd.DataFrame:
    if engine is None:
        engine = _get_engine_cached()

    params = {"ts_code": str(ts_code).strip().upper(), "start_date": datetime.strptime(str(start_date).replace("-", ""), "%Y%m%d").date()}
    conditions = ["ts_code = :ts_code", "trade_date >= :start_date"]
    if end_date:
        params["end_date"] = datetime.strptime(str(end_date).replace("-", ""), "%Y%m%d").date()
        conditions.append("trade_date <= :end_date")

    sql = f"""
    SELECT
        ts_code,
        trade_date,
        NULLIF(payload->>'name', '') AS name,
        (payload->>'rzye')::numeric AS rzye,
        (payload->>'rqye')::numeric AS rqye,
        (payload->>'rzmre')::numeric AS rzmre,
        (payload->>'rqyl')::numeric AS rqyl,
        (payload->>'rzche')::numeric AS rzche,
        (payload->>'rqchl')::numeric AS rqchl,
        (payload->>'rqmcl')::numeric AS rqmcl,
        (payload->>'rzrqye')::numeric AS rzrqye
    FROM ts_margin_detail
    WHERE {' AND '.join(conditions)}
    ORDER BY trade_date ASC
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def query_margin_exchange_snapshot(
    trade_date: Optional[str] = None,
    engine: Optional[Engine] = None,
) -> pd.DataFrame:
    if engine is None:
        engine = _get_engine_cached()

    params: dict[str, object] = {}
    where = ""
    if trade_date:
        params["trade_date"] = datetime.strptime(str(trade_date).replace("-", ""), "%Y%m%d").date()
        where = "WHERE trade_date = :trade_date"
    else:
        where = "WHERE trade_date = (SELECT MAX(trade_date) FROM ts_margin)"

    sql = f"""
    SELECT
        trade_date,
        NULLIF(payload->>'exchange_id', '') AS exchange_id,
        (payload->>'rzye')::numeric AS rzye,
        (payload->>'rzmre')::numeric AS rzmre,
        (payload->>'rzche')::numeric AS rzche,
        (payload->>'rqye')::numeric AS rqye,
        (payload->>'rqmcl')::numeric AS rqmcl,
        (payload->>'rzrqye')::numeric AS rzrqye,
        (payload->>'rqyl')::numeric AS rqyl
    FROM ts_margin
    {where}
    ORDER BY exchange_id ASC
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or None)


def query_margin_exchange_history(
    start_date: str = DEFAULT_START_DATE,
    end_date: Optional[str] = None,
    engine: Optional[Engine] = None,
) -> pd.DataFrame:
    if engine is None:
        engine = _get_engine_cached()

    params = {
        "start_date": datetime.strptime(str(start_date).replace("-", ""), "%Y%m%d").date(),
    }
    conditions = ["trade_date >= :start_date"]
    if end_date:
        params["end_date"] = datetime.strptime(str(end_date).replace("-", ""), "%Y%m%d").date()
        conditions.append("trade_date <= :end_date")

    sql = f"""
    SELECT
        trade_date,
        NULLIF(payload->>'exchange_id', '') AS exchange_id,
        (payload->>'rzye')::numeric AS rzye,
        (payload->>'rzmre')::numeric AS rzmre,
        (payload->>'rzche')::numeric AS rzche,
        (payload->>'rqye')::numeric AS rqye,
        (payload->>'rqmcl')::numeric AS rqmcl,
        (payload->>'rzrqye')::numeric AS rzrqye,
        (payload->>'rqyl')::numeric AS rqyl
    FROM ts_margin
    WHERE {' AND '.join(conditions)}
    ORDER BY trade_date ASC, exchange_id ASC
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def query_margin_market_latest(
    limit: int = 200,
    engine: Optional[Engine] = None,
) -> pd.DataFrame:
    if engine is None:
        engine = _get_engine_cached()

    sql = """
    WITH latest_trade AS (
        SELECT MAX(trade_date) AS trade_date FROM ts_margin_detail
    ),
    latest_rows AS (
        SELECT
            trade_date,
            ts_code,
            NULLIF(payload->>'name', '') AS name,
            (payload->>'rzye')::numeric AS rzye,
            (payload->>'rqye')::numeric AS rqye,
            (payload->>'rzmre')::numeric AS rzmre,
            (payload->>'rqyl')::numeric AS rqyl,
            (payload->>'rzche')::numeric AS rzche,
            (payload->>'rqchl')::numeric AS rqchl,
            (payload->>'rqmcl')::numeric AS rqmcl,
            (payload->>'rzrqye')::numeric AS rzrqye
        FROM ts_margin_detail
        WHERE trade_date = (SELECT trade_date FROM latest_trade)
    )
    SELECT *
    FROM latest_rows
    ORDER BY rzrqye DESC NULLS LAST, rzmre DESC NULLS LAST
    LIMIT :limit
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params={"limit": int(limit)})


def query_stock_price_history(
    ts_code: str,
    start_date: str = DEFAULT_START_DATE,
    end_date: Optional[str] = None,
    engine: Optional[Engine] = None,
) -> pd.DataFrame:
    if engine is None:
        engine = _get_engine_cached()

    params = {
        "ts_code": str(ts_code).strip().upper(),
        "start_date": datetime.strptime(str(start_date).replace("-", ""), "%Y%m%d").date(),
    }
    conditions = ["ts_code = :ts_code", "trade_date >= :start_date"]
    if end_date:
        params["end_date"] = datetime.strptime(str(end_date).replace("-", ""), "%Y%m%d").date()
        conditions.append("trade_date <= :end_date")

    sql = f"""
    SELECT
        ts_code,
        trade_date,
        close
    FROM vw_ts_stock_daily
    WHERE {' AND '.join(conditions)}
    ORDER BY trade_date ASC
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def prepare_margin_display_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    work = df.copy()
    work["trade_date"] = pd.to_datetime(work["trade_date"], errors="coerce")
    numeric_cols = ["rzye", "rqye", "rzmre", "rqyl", "rzche", "rqchl", "rqmcl", "rzrqye"]
    for col in numeric_cols:
        work[col] = pd.to_numeric(work.get(col), errors="coerce")
    work = work.dropna(subset=["trade_date"]).sort_values("trade_date")
    if work.empty:
        return work

    work["rzye_yi"] = work["rzye"] / 100000000.0
    work["rqye_yi"] = work["rqye"] / 100000000.0
    work["rzrqye_yi"] = work["rzrqye"] / 100000000.0
    work["rzmre_yi"] = work["rzmre"] / 100000000.0
    work["rzche_yi"] = work["rzche"] / 100000000.0
    work["rz_net_buy_yi"] = (work["rzmre"] - work["rzche"]) / 100000000.0
    work["rqyl_wan"] = work["rqyl"] / 10000.0
    work["rqchl_wan"] = work["rqchl"] / 10000.0
    work["rqmcl_wan"] = work["rqmcl"] / 10000.0
    work["rq_net_sell_wan"] = (work["rqmcl"] - work["rqchl"]) / 10000.0
    denominator = work["rzrqye"].where(work["rzrqye"] != 0)
    work["rqye_ratio_pct"] = work["rqye"] / denominator * 100.0
    return work


def build_margin_latest_metrics(df: pd.DataFrame) -> dict[str, object]:
    prepared = prepare_margin_display_frame(df)
    if prepared.empty:
        return {}

    latest = prepared.iloc[-1]
    previous = prepared.iloc[-2] if len(prepared) >= 2 else latest
    return {
        "latest_trade_date": latest["trade_date"],
        "rzrqye_yi": float(latest["rzrqye_yi"]) if pd.notna(latest["rzrqye_yi"]) else None,
        "rzye_yi": float(latest["rzye_yi"]) if pd.notna(latest["rzye_yi"]) else None,
        "rqye_yi": float(latest["rqye_yi"]) if pd.notna(latest["rqye_yi"]) else None,
        "rz_net_buy_yi": float(latest["rz_net_buy_yi"]) if pd.notna(latest["rz_net_buy_yi"]) else None,
        "rq_net_sell_wan": float(latest["rq_net_sell_wan"]) if pd.notna(latest["rq_net_sell_wan"]) else None,
        "rqye_ratio_pct": float(latest["rqye_ratio_pct"]) if pd.notna(latest["rqye_ratio_pct"]) else None,
        "rzrqye_delta_yi": (
            float(latest["rzrqye_yi"] - previous["rzrqye_yi"])
            if pd.notna(latest["rzrqye_yi"]) and pd.notna(previous["rzrqye_yi"])
            else None
        ),
    }


def _count_recent_streak(values: pd.Series, positive: bool = True) -> int:
    streak = 0
    for value in reversed(values.tolist()):
        if pd.isna(value):
            break
        if positive and value > 0:
            streak += 1
        elif not positive and value < 0:
            streak += 1
        else:
            break
    return streak


def build_margin_signal_summary(df: pd.DataFrame, lookback_days: int = 20) -> dict[str, object]:
    prepared = prepare_margin_display_frame(df)
    if prepared.empty:
        return {}

    window = prepared.tail(max(5, int(lookback_days or 20))).copy()
    latest = prepared.iloc[-1]

    rzrqye_rank_pct = None
    if len(window) > 1 and pd.notna(latest.get("rzrqye_yi")):
        rzrqye_series = window["rzrqye_yi"].dropna()
        if not rzrqye_series.empty:
            rzrqye_rank_pct = float((rzrqye_series <= latest["rzrqye_yi"]).mean() * 100.0)

    rz_net_buy_streak = _count_recent_streak(window["rz_net_buy_yi"], positive=True)
    rz_net_sell_streak = _count_recent_streak(window["rz_net_buy_yi"], positive=False)
    rq_net_sell_streak = _count_recent_streak(window["rq_net_sell_wan"], positive=True)
    rq_net_buyback_streak = _count_recent_streak(window["rq_net_sell_wan"], positive=False)

    rz_5d = pd.to_numeric(window["rz_net_buy_yi"], errors="coerce").tail(5).sum(min_count=1)
    rq_5d = pd.to_numeric(window["rq_net_sell_wan"], errors="coerce").tail(5).sum(min_count=1)

    if rz_net_buy_streak >= 3:
        financing_signal = f"融资端已连续 {rz_net_buy_streak} 日净买入"
    elif rz_net_sell_streak >= 3:
        financing_signal = f"融资端已连续 {rz_net_sell_streak} 日净偿还"
    elif pd.notna(rz_5d) and rz_5d > 0:
        financing_signal = "近 5 日融资端小幅偏多"
    elif pd.notna(rz_5d) and rz_5d < 0:
        financing_signal = "近 5 日融资端偏谨慎"
    else:
        financing_signal = "近 5 日融资端中性"

    if rq_net_sell_streak >= 3:
        short_signal = f"融券端已连续 {rq_net_sell_streak} 日净卖出"
    elif rq_net_buyback_streak >= 3:
        short_signal = f"融券端已连续 {rq_net_buyback_streak} 日净偿还"
    elif pd.notna(rq_5d) and rq_5d > 0:
        short_signal = "近 5 日融券端略有抬升"
    elif pd.notna(rq_5d) and rq_5d < 0:
        short_signal = "近 5 日融券端以回补为主"
    else:
        short_signal = "近 5 日融券端中性"

    rank_comment = None
    if rzrqye_rank_pct is not None:
        if rzrqye_rank_pct >= 80:
            rank_comment = f"两融余额处于近 {len(window)} 日高位区（{rzrqye_rank_pct:.0f}% 分位）"
        elif rzrqye_rank_pct <= 20:
            rank_comment = f"两融余额处于近 {len(window)} 日低位区（{rzrqye_rank_pct:.0f}% 分位）"
        else:
            rank_comment = f"两融余额位于近 {len(window)} 日中位区域（{rzrqye_rank_pct:.0f}% 分位）"

    alerts: list[dict[str, str]] = []
    if rzrqye_rank_pct is not None:
        if rzrqye_rank_pct >= 80 and rz_net_buy_streak >= 3:
            alerts.append(
                {
                    "level": "success",
                    "title": "高位加杠杆",
                    "message": (
                        f"两融余额位于高位区，同时融资端连续 {rz_net_buy_streak} 日净买入，"
                        "说明杠杆资金做多意愿较强。"
                    ),
                }
            )
        if rzrqye_rank_pct >= 70 and rq_net_sell_streak >= 3:
            alerts.append(
                {
                    "level": "warning",
                    "title": "空头压力抬升",
                    "message": (
                        f"两融余额不低且融券端连续 {rq_net_sell_streak} 日净卖出，"
                        "需要留意高位博弈加剧。"
                    ),
                }
            )
        if rzrqye_rank_pct <= 20 and rq_net_buyback_streak >= 3:
            alerts.append(
                {
                    "level": "info",
                    "title": "低位回补",
                    "message": (
                        f"两融余额处于低位区，融券端连续 {rq_net_buyback_streak} 日净偿还，"
                        "说明空头回补迹象在增强。"
                    ),
                }
            )
        if rzrqye_rank_pct <= 20 and rz_net_sell_streak >= 3:
            alerts.append(
                {
                    "level": "warning",
                    "title": "低位去杠杆",
                    "message": (
                        f"两融余额位于低位区，融资端连续 {rz_net_sell_streak} 日净偿还，"
                        "说明杠杆资金仍偏谨慎。"
                    ),
                }
            )

    return {
        "window_days": len(window),
        "rzrqye_rank_pct": rzrqye_rank_pct,
        "rz_net_buy_streak": rz_net_buy_streak,
        "rz_net_sell_streak": rz_net_sell_streak,
        "rq_net_sell_streak": rq_net_sell_streak,
        "rq_net_buyback_streak": rq_net_buyback_streak,
        "rz_5d_sum_yi": float(rz_5d) if pd.notna(rz_5d) else None,
        "rq_5d_sum_wan": float(rq_5d) if pd.notna(rq_5d) else None,
        "financing_signal": financing_signal,
        "short_signal": short_signal,
        "rank_comment": rank_comment,
        "alerts": alerts,
    }


def build_margin_price_overlay_frame(
    margin_df: pd.DataFrame,
    price_df: pd.DataFrame,
    lookback_days: int = 120,
) -> pd.DataFrame:
    margin_prepared = prepare_margin_display_frame(margin_df)
    if margin_prepared.empty or price_df is None or price_df.empty:
        return pd.DataFrame()

    price_work = price_df.copy()
    price_work["trade_date"] = pd.to_datetime(price_work["trade_date"], errors="coerce")
    price_work["close"] = pd.to_numeric(price_work["close"], errors="coerce")
    price_work = price_work.dropna(subset=["trade_date", "close"]).sort_values("trade_date")
    if price_work.empty:
        return pd.DataFrame()

    merged = pd.merge(
        margin_prepared[["trade_date", "rzrqye_yi", "rzye_yi", "rqye_yi"]],
        price_work[["trade_date", "close"]],
        on="trade_date",
        how="inner",
    ).sort_values("trade_date")
    if merged.empty:
        return pd.DataFrame()

    merged = merged.tail(max(20, int(lookback_days or 120))).copy()
    base_close = pd.to_numeric(merged["close"], errors="coerce").iloc[0]
    base_margin = pd.to_numeric(merged["rzrqye_yi"], errors="coerce").iloc[0]
    if pd.isna(base_close) or base_close == 0 or pd.isna(base_margin) or base_margin == 0:
        return pd.DataFrame()

    merged["price_index"] = merged["close"] / base_close * 100.0
    merged["margin_index"] = merged["rzrqye_yi"] / base_margin * 100.0
    merged["spread_index"] = merged["margin_index"] - merged["price_index"]
    return merged


def build_margin_price_divergence_summary(
    margin_df: pd.DataFrame,
    price_df: pd.DataFrame,
    lookback_days: int = 20,
) -> dict[str, object]:
    overlay = build_margin_price_overlay_frame(margin_df, price_df, lookback_days=max(lookback_days, 20))
    signal_summary = build_margin_signal_summary(margin_df, lookback_days=lookback_days)
    if overlay.empty:
        return {}

    window = overlay.tail(max(5, int(lookback_days or 20))).copy()
    latest = window.iloc[-1]
    first = window.iloc[0]

    price_return_pct = float((latest["close"] / first["close"] - 1.0) * 100.0) if first["close"] else None
    margin_return_pct = (
        float((latest["rzrqye_yi"] / first["rzrqye_yi"] - 1.0) * 100.0) if first["rzrqye_yi"] else None
    )
    spread_index = float(latest["spread_index"]) if pd.notna(latest["spread_index"]) else None

    observations: list[str] = []
    alerts: list[dict[str, str]] = []
    rank_pct = signal_summary.get("rzrqye_rank_pct")
    rz_streak = int(signal_summary.get("rz_net_buy_streak") or 0)
    rq_buyback_streak = int(signal_summary.get("rq_net_buyback_streak") or 0)

    if price_return_pct is not None and margin_return_pct is not None:
        observations.append(
            f"近 {len(window)} 日股价 `{price_return_pct:+.2f}%`，两融余额 `{margin_return_pct:+.2f}%`"
        )

    if rank_pct is not None and price_return_pct is not None:
        if rank_pct >= 80 and price_return_pct <= 0:
            alerts.append(
                {
                    "level": "warning",
                    "title": "高杠杆弱价格",
                    "message": "两融余额处于高位区，但同期股价未能同步走强，需留意资金拥挤后的承接风险。",
                }
            )
        if rank_pct >= 80 and price_return_pct <= -5 and rz_streak >= 3:
            alerts.append(
                {
                    "level": "warning",
                    "title": "加杠杆背离",
                    "message": f"融资端连续 {rz_streak} 日净买入，但近窗股价回落，说明杠杆资金与价格表现出现背离。",
                }
            )
        if rank_pct <= 30 and price_return_pct >= 5 and rq_buyback_streak >= 3:
            alerts.append(
                {
                    "level": "success",
                    "title": "低位回补转强",
                    "message": f"两融余额仍在低位区，且融券端连续 {rq_buyback_streak} 日净偿还，同时股价转强，改善信号更明确。",
                }
            )

    return {
        "window_days": len(window),
        "price_return_pct": price_return_pct,
        "margin_return_pct": margin_return_pct,
        "spread_index": spread_index,
        "observations": observations,
        "alerts": alerts,
        "overlay": window,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="融资融券数据更新工具")
    parser.add_argument("--start", type=str, default=None, help="起始日期 YYYYMMDD")
    parser.add_argument("--end", type=str, default=None, help="结束日期 YYYYMMDD")
    parser.add_argument("--full", action="store_true", help="全量从 2022-01-01 开始回补")
    parser.add_argument("--datasets", type=str, default=None, help="逗号分隔数据集，如 margin,margin_detail")
    parser.add_argument("--init-tables", action="store_true", help="仅初始化数据库表和视图")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS, help="增量同步回看天数")
    args = parser.parse_args()

    if args.lookback_days < 0:
        raise ValueError("--lookback-days 不能小于 0")
    os.environ["TUSHARE_MARGIN_LOOKBACK_DAYS"] = str(args.lookback_days)

    engine = _get_engine_cached()
    if args.init_tables:
        ensure_all_tables(engine)
        print("融资融券数据库表和视图初始化完成")
        raise SystemExit(0)

    target_datasets = [item.strip() for item in args.datasets.split(",")] if args.datasets else None
    start = DEFAULT_START_DATE if args.full else args.start
    result = run_sync(datasets=target_datasets, start_date=start, end_date=args.end)
    print(json.dumps(result, ensure_ascii=False))
