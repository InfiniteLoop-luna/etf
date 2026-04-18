# -*- coding: utf-8 -*-
"""
公募基金持仓热股（MVP）后端模块

能力：
1) Tushare 公募基金基础信息/持仓数据同步（fund_basic, fund_portfolio）
2) PostgreSQL JSONB landing tables + normalized views
3) 季度热门股票聚合表（agg_fund_holding_stock_quarterly）
4) 查询函数：
   - 热股榜 query_hot_stocks_leaderboard
   - 单股持仓透视 query_stock_fund_holding_detail
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import time
from datetime import date, datetime
from typing import Iterable, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.volume_fetcher import _init_tushare

DEFAULT_DB_HOST = "67.216.207.73"
DEFAULT_DB_PORT = 5432
DEFAULT_DB_NAME = "postgres"
DEFAULT_DB_USER = "postgres"
DEFAULT_DB_SSLMODE = "disable"
DEFAULT_API_SLEEP = float(os.getenv("TUSHARE_FUND_API_SLEEP", "0.25"))
DEFAULT_PORTFOLIO_START_PERIOD = os.getenv("TUSHARE_FUND_PORTFOLIO_START", "20240101")
DEFAULT_PORTFOLIO_LOOKBACK_QUARTERS = int(os.getenv("TUSHARE_FUND_PORTFOLIO_LOOKBACK_QUARTERS", "1"))

DATASET_TABLES = {
    "fund_basic": "ts_fund_basic",
    "fund_portfolio": "ts_fund_portfolio",
}

AGG_TABLE = "agg_fund_holding_stock_quarterly"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 数据库连接
# ---------------------------------------------------------------------------

def build_db_url():
    # 优先复用现有 sync 模块逻辑
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
            if password and not os.environ.get("ETF_PG_PASSWORD"):
                os.environ["ETF_PG_PASSWORD"] = str(password)
        except Exception:
            pass

    password = os.getenv("ETF_PG_PASSWORD") or os.getenv("PGPASSWORD")
    if not password:
        raise RuntimeError("未配置数据库密码，请设置 ETF_PG_PASSWORD 或 PGPASSWORD")

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


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def parse_yyyymmdd(raw) -> Optional[date]:
    if raw is None:
        return None
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    if isinstance(raw, datetime):
        return raw.date()

    s = str(raw).strip()
    if not s:
        return None
    s = s.replace("-", "")
    if len(s) == 8 and s.isdigit():
        return datetime.strptime(s, "%Y%m%d").date()
    return None


def yyyymmdd(raw) -> Optional[str]:
    d = parse_yyyymmdd(raw)
    return d.strftime("%Y%m%d") if d else None


def normalize_period(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    d = parse_yyyymmdd(raw)
    if not d:
        return None
    return pd.Timestamp(d).to_period("Q").end_time.date().strftime("%Y%m%d")


def iter_quarter_periods(start_period: str, end_period: str) -> list[str]:
    s = pd.Timestamp(parse_yyyymmdd(start_period)).to_period("Q")
    e = pd.Timestamp(parse_yyyymmdd(end_period)).to_period("Q")
    periods = [p.end_time.date().strftime("%Y%m%d") for p in pd.period_range(s, e, freq="Q")]
    return periods


def _normalize_value(v):
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            return v
    return v


def normalize_payload(row: dict) -> dict:
    return {k: _normalize_value(v) for k, v in row.items()}


def compute_record_hash(payload: dict) -> str:
    content = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:64]


def make_business_key(dataset_name: str, *parts) -> str:
    normalized = [dataset_name]
    for p in parts:
        if p is None:
            continue
        s = str(p).strip()
        if s:
            normalized.append(s)
    return "|".join(normalized)


# ---------------------------------------------------------------------------
# 建表 / 视图 / 聚合表
# ---------------------------------------------------------------------------

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
    CREATE INDEX IF NOT EXISTS idx_{table_name}_ts_code ON {table_name}(ts_code);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_ann_date ON {table_name}(ann_date);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_end_date ON {table_name}(end_date);
    CREATE INDEX IF NOT EXISTS idx_{table_name}_period ON {table_name}(period);
    """
    with engine.begin() as conn:
        for statement in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(statement))
    logger.info(f"✓ 表 {table_name} 已就绪")


def ensure_aggregate_table(engine: Engine):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {AGG_TABLE} (
        end_date DATE NOT NULL,
        symbol VARCHAR(20) NOT NULL,
        stock_name TEXT,
        holding_fund_count INTEGER NOT NULL DEFAULT 0,
        total_mkv NUMERIC(24, 2) NOT NULL DEFAULT 0,
        total_amount NUMERIC(24, 2) NOT NULL DEFAULT 0,
        avg_stk_mkv_ratio NUMERIC(18, 6),
        prev_holding_fund_count INTEGER,
        prev_total_mkv NUMERIC(24, 2),
        delta_holding_fund_count INTEGER,
        delta_total_mkv NUMERIC(24, 2),
        new_fund_count INTEGER NOT NULL DEFAULT 0,
        exited_fund_count INTEGER NOT NULL DEFAULT 0,
        increased_fund_count INTEGER NOT NULL DEFAULT 0,
        decreased_fund_count INTEGER NOT NULL DEFAULT 0,
        heat_score NUMERIC(18, 8),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (end_date, symbol)
    );
    CREATE INDEX IF NOT EXISTS idx_{AGG_TABLE}_symbol ON {AGG_TABLE}(symbol);
    CREATE INDEX IF NOT EXISTS idx_{AGG_TABLE}_heat_score ON {AGG_TABLE}(heat_score DESC);
    """
    with engine.begin() as conn:
        for statement in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(statement))
    logger.info(f"✓ 聚合表 {AGG_TABLE} 已就绪")


def ensure_normalized_views(engine: Engine):
    sql_fund_basic = f"""
    CREATE OR REPLACE VIEW vw_fund_basic AS
    SELECT
        business_key,
        dataset_name,
        ts_code AS fund_code,
        record_hash,
        ingested_at,
        payload,
        NULLIF(payload->>'name', '') AS name,
        NULLIF(payload->>'management', '') AS management,
        NULLIF(payload->>'custodian', '') AS custodian,
        NULLIF(payload->>'fund_type', '') AS fund_type,
        CASE WHEN NULLIF(payload->>'found_date', '') IS NOT NULL THEN to_date(payload->>'found_date', 'YYYYMMDD') END AS found_date,
        CASE WHEN NULLIF(payload->>'due_date', '') IS NOT NULL THEN to_date(payload->>'due_date', 'YYYYMMDD') END AS due_date,
        CASE WHEN NULLIF(payload->>'list_date', '') IS NOT NULL THEN to_date(payload->>'list_date', 'YYYYMMDD') END AS list_date,
        CASE WHEN NULLIF(payload->>'issue_date', '') IS NOT NULL THEN to_date(payload->>'issue_date', 'YYYYMMDD') END AS issue_date,
        CASE WHEN NULLIF(payload->>'delist_date', '') IS NOT NULL THEN to_date(payload->>'delist_date', 'YYYYMMDD') END AS delist_date,
        NULLIF(payload->>'issue_amount', '')::numeric AS issue_amount,
        NULLIF(payload->>'m_fee', '')::numeric AS m_fee,
        NULLIF(payload->>'c_fee', '')::numeric AS c_fee,
        NULLIF(payload->>'duration_year', '')::numeric AS duration_year,
        NULLIF(payload->>'p_value', '')::numeric AS p_value,
        NULLIF(payload->>'min_amount', '')::numeric AS min_amount,
        NULLIF(payload->>'exp_return', '')::numeric AS exp_return,
        NULLIF(payload->>'benchmark', '') AS benchmark,
        NULLIF(payload->>'status', '') AS status,
        NULLIF(payload->>'invest_type', '') AS invest_type,
        NULLIF(payload->>'type', '') AS type,
        NULLIF(payload->>'trustee', '') AS trustee,
        CASE WHEN NULLIF(payload->>'purc_startdate', '') IS NOT NULL THEN to_date(payload->>'purc_startdate', 'YYYYMMDD') END AS purc_startdate,
        CASE WHEN NULLIF(payload->>'redm_startdate', '') IS NOT NULL THEN to_date(payload->>'redm_startdate', 'YYYYMMDD') END AS redm_startdate,
        NULLIF(payload->>'market', '') AS market
    FROM {DATASET_TABLES['fund_basic']}
    """

    sql_fund_portfolio = f"""
    CREATE OR REPLACE VIEW vw_fund_portfolio AS
    SELECT
        business_key,
        dataset_name,
        ts_code AS fund_code,
        ann_date,
        end_date,
        period,
        record_hash,
        ingested_at,
        payload,
        COALESCE(NULLIF(payload->>'symbol', ''), NULLIF(payload->>'stk_code', '')) AS symbol,
        NULLIF(payload->>'mkv', '')::numeric AS mkv,
        NULLIF(payload->>'amount', '')::numeric AS amount,
        NULLIF(payload->>'stk_mkv_ratio', '')::numeric AS stk_mkv_ratio,
        NULLIF(payload->>'stk_float_ratio', '')::numeric AS stk_float_ratio
    FROM {DATASET_TABLES['fund_portfolio']}
    """

    with engine.begin() as conn:
        conn.execute(text(sql_fund_basic))
        conn.execute(text(sql_fund_portfolio))
    logger.info("✓ 视图 vw_fund_basic / vw_fund_portfolio 已更新")


def ensure_all_tables(engine: Engine):
    for table_name in DATASET_TABLES.values():
        ensure_landing_table(engine, table_name)
    ensure_normalized_views(engine)
    ensure_aggregate_table(engine)


# ---------------------------------------------------------------------------
# 通用 upsert
# ---------------------------------------------------------------------------

def upsert_rows(
    engine: Engine,
    table_name: str,
    dataset_name: str,
    rows: list[dict],
    business_key_fn=None,
    ts_code_fn=None,
    trade_date_fn=None,
    ann_date_fn=None,
    end_date_fn=None,
    period_fn=None,
    chunk_size: int = 2000,
) -> int:
    if not rows:
        return 0

    records = []
    for row in rows:
        payload = normalize_payload(row)

        ts_code = ts_code_fn(row) if ts_code_fn else payload.get("ts_code")
        trade_date_val = parse_yyyymmdd(trade_date_fn(row) if trade_date_fn else payload.get("trade_date"))
        ann_date_val = parse_yyyymmdd(ann_date_fn(row) if ann_date_fn else payload.get("ann_date"))
        end_date_val = parse_yyyymmdd(end_date_fn(row) if end_date_fn else payload.get("end_date"))

        period_val = period_fn(row) if period_fn else payload.get("period")
        if not period_val and end_date_val:
            period_val = end_date_val.strftime("%Y%m%d")

        if business_key_fn:
            business_key = business_key_fn(row)
        else:
            business_key = make_business_key(dataset_name, ts_code, yyyymmdd(end_date_val), yyyymmdd(ann_date_val))

        records.append(
            {
                "business_key": business_key,
                "dataset_name": dataset_name,
                "ts_code": ts_code,
                "trade_date": trade_date_val,
                "ann_date": ann_date_val,
                "end_date": end_date_val,
                "period": str(period_val) if period_val else None,
                "record_hash": compute_record_hash(payload),
                "payload": json.dumps(payload, ensure_ascii=False, default=str),
            }
        )

    sql = f"""
    INSERT INTO {table_name} (
        business_key, dataset_name, ts_code, trade_date, ann_date, end_date, period,
        record_hash, payload, ingested_at
    ) VALUES (
        :business_key, :dataset_name, :ts_code, :trade_date, :ann_date, :end_date, :period,
        :record_hash, CAST(:payload AS jsonb), NOW()
    )
    ON CONFLICT (business_key) DO UPDATE SET
        ts_code = EXCLUDED.ts_code,
        trade_date = EXCLUDED.trade_date,
        ann_date = EXCLUDED.ann_date,
        end_date = EXCLUDED.end_date,
        period = EXCLUDED.period,
        record_hash = EXCLUDED.record_hash,
        payload = EXCLUDED.payload,
        ingested_at = NOW()
    WHERE {table_name}.record_hash <> EXCLUDED.record_hash
    """

    chunk_size = max(1, int(chunk_size or 1))
    written = 0
    stmt = text(sql)
    total_batches = (len(records) + chunk_size - 1) // chunk_size
    for idx, i in enumerate(range(0, len(records), chunk_size), start=1):
        batch = records[i : i + chunk_size]
        with engine.begin() as conn:
            conn.execute(stmt, batch)
        written += len(batch)
        if total_batches > 1:
            logger.info(f"{dataset_name}: ????? {idx}/{total_batches}??? {written} ??")

    return written

# ---------------------------------------------------------------------------
# 同步逻辑
# ---------------------------------------------------------------------------

def fetch_fund_basic(pro, markets: Iterable[str] = ("E", "O"), statuses: Iterable[str] = ("L", "I", "D")) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for market in markets:
        for status in statuses:
            try:
                df = pro.fund_basic(market=market, status=status)
                if df is not None and not df.empty:
                    frames.append(df)
                    logger.info(f"fund_basic market={market} status={status}: {len(df)} 行")
                else:
                    logger.info(f"fund_basic market={market} status={status}: 0 行")
            except Exception as exc:
                logger.warning(f"fund_basic market={market} status={status} 拉取失败: {exc}")
            time.sleep(DEFAULT_API_SLEEP)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    if "ts_code" in result.columns:
        result = result.sort_values(by=[c for c in ["ts_code", "list_date", "found_date"] if c in result.columns]).drop_duplicates(
            subset=["ts_code"], keep="last"
        )
    return result.reset_index(drop=True)


def sync_fund_basic(engine: Engine, pro, markets: Iterable[str] = ("E", "O"), statuses: Iterable[str] = ("L", "I", "D")) -> int:
    df = fetch_fund_basic(pro, markets=markets, statuses=statuses)
    if df is None or df.empty:
        logger.info("fund_basic 无数据返回")
        return 0

    rows = df.to_dict("records")
    n = upsert_rows(
        engine,
        DATASET_TABLES["fund_basic"],
        "fund_basic",
        rows,
        business_key_fn=lambda r: make_business_key("fund_basic", str(r.get("ts_code", "")).strip().upper()),
        ts_code_fn=lambda r: str(r.get("ts_code", "")).strip().upper() or None,
        chunk_size=200,
    )
    logger.info(f"fund_basic 同步完成，写入 {n} 行")
    return n


def get_max_portfolio_end_date(engine: Engine) -> Optional[str]:
    table = DATASET_TABLES["fund_portfolio"]
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT MAX(end_date) FROM {table}")).fetchone()
    if row and row[0]:
        return row[0].strftime("%Y%m%d")
    return None


def resolve_portfolio_periods(
    engine: Engine,
    period: Optional[str] = None,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    lookback_quarters: int = DEFAULT_PORTFOLIO_LOOKBACK_QUARTERS,
) -> list[str]:
    if period:
        p = normalize_period(period)
        return [p] if p else []

    end_p = normalize_period(end_period) or normalize_period(datetime.now().strftime("%Y%m%d"))
    if not end_p:
        return []

    if start_period:
        start_p = normalize_period(start_period)
    else:
        max_period = get_max_portfolio_end_date(engine)
        if max_period:
            base = pd.Timestamp(parse_yyyymmdd(max_period)).to_period("Q") - max(0, int(lookback_quarters))
            start_p = base.end_time.date().strftime("%Y%m%d")
        else:
            start_p = normalize_period(DEFAULT_PORTFOLIO_START_PERIOD)

    if not start_p:
        return []

    if parse_yyyymmdd(start_p) > parse_yyyymmdd(end_p):
        return []

    return iter_quarter_periods(start_p, end_p)


def sync_fund_portfolio(
    engine: Engine,
    pro,
    period: Optional[str] = None,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
) -> int:
    periods = resolve_portfolio_periods(engine, period=period, start_period=start_period, end_period=end_period)
    if not periods:
        logger.info("fund_portfolio: 无需同步（periods 为空）")
        return 0

    total = 0
    for p in periods:
        try:
            df = pro.fund_portfolio(period=p)
        except Exception as exc:
            logger.warning(f"fund_portfolio period={p} 拉取失败: {exc}")
            time.sleep(DEFAULT_API_SLEEP)
            continue

        if df is None or df.empty:
            logger.info(f"fund_portfolio period={p}: 0 行")
            time.sleep(DEFAULT_API_SLEEP)
            continue

        rows = df.to_dict("records")
        n = upsert_rows(
            engine,
            DATASET_TABLES["fund_portfolio"],
            "fund_portfolio",
            rows,
            business_key_fn=lambda r: make_business_key(
                "fund_portfolio",
                str(r.get("ts_code", "")).strip().upper(),
                normalize_period(r.get("end_date")) or yyyymmdd(r.get("end_date")),
                yyyymmdd(r.get("ann_date")),
                str(r.get("symbol", "")).strip().upper(),
            ),
            ts_code_fn=lambda r: str(r.get("ts_code", "")).strip().upper() or None,
            ann_date_fn=lambda r: r.get("ann_date"),
            end_date_fn=lambda r: r.get("end_date"),
            period_fn=lambda r: normalize_period(r.get("end_date")) or normalize_period(r.get("period")),
            chunk_size=100,
        )
        logger.info(f"fund_portfolio period={p}: 写入 {n} 行")
        total += n
        time.sleep(DEFAULT_API_SLEEP)

    logger.info(f"fund_portfolio 同步完成，总写入 {total} 行")
    return total


# ---------------------------------------------------------------------------
# 季度聚合
# ---------------------------------------------------------------------------

def _compute_heat_score(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    for col in [
        "holding_fund_count",
        "total_mkv",
        "delta_holding_fund_count",
        "delta_total_mkv",
    ]:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    def _period_score(g: pd.DataFrame) -> pd.DataFrame:
        out = g.copy()
        out["r_holding"] = out["holding_fund_count"].rank(pct=True, method="average")
        out["r_mkv"] = out["total_mkv"].rank(pct=True, method="average")
        out["r_delta_holding"] = out["delta_holding_fund_count"].rank(pct=True, method="average")
        out["r_delta_mkv"] = out["delta_total_mkv"].rank(pct=True, method="average")
        out["heat_score"] = (
            0.30 * out["r_holding"]
            + 0.30 * out["r_mkv"]
            + 0.20 * out["r_delta_holding"]
            + 0.20 * out["r_delta_mkv"]
        )
        return out

    scored = df.groupby("end_date", group_keys=False).apply(_period_score)
    return scored.drop(columns=["r_holding", "r_mkv", "r_delta_holding", "r_delta_mkv"], errors="ignore")


def rebuild_hot_stock_aggregate(
    engine: Engine,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
) -> int:
    ensure_aggregate_table(engine)

    where = ["p.end_date IS NOT NULL", "p.symbol IS NOT NULL", "NULLIF(p.symbol, '') IS NOT NULL"]
    params = {}
    if start_period:
        params["start_period"] = parse_yyyymmdd(normalize_period(start_period) or start_period)
        where.append("p.end_date >= :start_period")
    if end_period:
        params["end_period"] = parse_yyyymmdd(normalize_period(end_period) or end_period)
        where.append("p.end_date <= :end_period")

    sql = f"""
    SELECT
        p.end_date,
        p.fund_code,
        p.symbol,
        COALESCE(sb.name, p.symbol) AS stock_name,
        p.mkv,
        p.amount,
        p.stk_mkv_ratio,
        p.ann_date
    FROM vw_fund_portfolio p
    LEFT JOIN vw_ts_stock_basic sb ON sb.ts_code = p.symbol
    WHERE {' AND '.join(where)}
    """

    base = pd.read_sql(text(sql), engine, params=params)
    if base.empty:
        logger.info("聚合跳过：vw_fund_portfolio 无数据")
        return 0

    # 同一基金-股票-季度可能存在多条公告，保留 ann_date 最新
    base["ann_date"] = pd.to_datetime(base["ann_date"], errors="coerce")
    base = base.sort_values(by=["end_date", "symbol", "fund_code", "ann_date"]).drop_duplicates(
        subset=["end_date", "symbol", "fund_code"], keep="last"
    )

    agg = (
        base.groupby(["end_date", "symbol", "stock_name"], as_index=False)
        .agg(
            holding_fund_count=("fund_code", "nunique"),
            total_mkv=("mkv", "sum"),
            total_amount=("amount", "sum"),
            avg_stk_mkv_ratio=("stk_mkv_ratio", "mean"),
        )
    )

    agg = agg.sort_values(by=["symbol", "end_date"]).reset_index(drop=True)
    agg["prev_holding_fund_count"] = agg.groupby("symbol")["holding_fund_count"].shift(1)
    agg["prev_total_mkv"] = agg.groupby("symbol")["total_mkv"].shift(1)
    agg["delta_holding_fund_count"] = agg["holding_fund_count"] - agg["prev_holding_fund_count"].fillna(0)
    agg["delta_total_mkv"] = agg["total_mkv"] - agg["prev_total_mkv"].fillna(0)

    # 逐季度计算 new/exited/increased/decreased
    periods = sorted(pd.to_datetime(agg["end_date"].dropna().unique()))
    pair_metrics: list[dict] = []

    # 映射每个季度对应上个季度（按数据内排序，不强依赖自然季度完整性）
    prev_map = {}
    prev = None
    for p in periods:
        prev_map[p.date()] = prev.date() if prev is not None else None
        prev = p

    for cur_end in [p.date() for p in periods]:
        prev_end = prev_map.get(cur_end)
        cur_df = base[base["end_date"] == pd.Timestamp(cur_end)]
        prev_df = base[base["end_date"] == pd.Timestamp(prev_end)] if prev_end else base.iloc[0:0]

        cur_key = cur_df[["symbol", "fund_code", "mkv"]].rename(columns={"mkv": "cur_mkv"})
        prev_key = prev_df[["symbol", "fund_code", "mkv"]].rename(columns={"mkv": "prev_mkv"})

        merged = cur_key.merge(prev_key, on=["symbol", "fund_code"], how="outer")
        if merged.empty:
            continue

        grp = merged.groupby("symbol", dropna=True)
        metric_df = pd.DataFrame(
            {
                "new_fund_count": grp.apply(lambda g: int(((g["cur_mkv"].notna()) & (g["prev_mkv"].isna())).sum())),
                "exited_fund_count": grp.apply(lambda g: int(((g["cur_mkv"].isna()) & (g["prev_mkv"].notna())).sum())),
                "increased_fund_count": grp.apply(lambda g: int(((g["cur_mkv"].notna()) & (g["prev_mkv"].notna()) & (g["cur_mkv"] > g["prev_mkv"]).fillna(False)).sum())),
                "decreased_fund_count": grp.apply(lambda g: int(((g["cur_mkv"].notna()) & (g["prev_mkv"].notna()) & (g["cur_mkv"] < g["prev_mkv"]).fillna(False)).sum())),
            }
        ).reset_index()
        metric_df["end_date"] = pd.Timestamp(cur_end)
        pair_metrics.append(metric_df)

    if pair_metrics:
        metric_all = pd.concat(pair_metrics, ignore_index=True)
        agg = agg.merge(metric_all, on=["end_date", "symbol"], how="left")
    else:
        agg["new_fund_count"] = 0
        agg["exited_fund_count"] = 0
        agg["increased_fund_count"] = 0
        agg["decreased_fund_count"] = 0

    for col in ["new_fund_count", "exited_fund_count", "increased_fund_count", "decreased_fund_count"]:
        agg[col] = pd.to_numeric(agg[col], errors="coerce").fillna(0).astype(int)

    agg = _compute_heat_score(agg)
    agg["updated_at"] = datetime.now()

    records = []
    for r in agg.to_dict("records"):
        records.append(
            {
                "end_date": parse_yyyymmdd(r.get("end_date")),
                "symbol": str(r.get("symbol", "")).strip().upper(),
                "stock_name": r.get("stock_name"),
                "holding_fund_count": int(r.get("holding_fund_count") or 0),
                "total_mkv": float(r.get("total_mkv") or 0),
                "total_amount": float(r.get("total_amount") or 0),
                "avg_stk_mkv_ratio": float(r["avg_stk_mkv_ratio"]) if pd.notna(r.get("avg_stk_mkv_ratio")) else None,
                "prev_holding_fund_count": int(r["prev_holding_fund_count"]) if pd.notna(r.get("prev_holding_fund_count")) else None,
                "prev_total_mkv": float(r["prev_total_mkv"]) if pd.notna(r.get("prev_total_mkv")) else None,
                "delta_holding_fund_count": int(r.get("delta_holding_fund_count") or 0),
                "delta_total_mkv": float(r.get("delta_total_mkv") or 0),
                "new_fund_count": int(r.get("new_fund_count") or 0),
                "exited_fund_count": int(r.get("exited_fund_count") or 0),
                "increased_fund_count": int(r.get("increased_fund_count") or 0),
                "decreased_fund_count": int(r.get("decreased_fund_count") or 0),
                "heat_score": float(r.get("heat_score") or 0),
                "updated_at": r.get("updated_at") or datetime.now(),
            }
        )

    delete_where = []
    delete_params = {}
    if start_period:
        delete_where.append("end_date >= :start_period")
        delete_params["start_period"] = parse_yyyymmdd(normalize_period(start_period) or start_period)
    if end_period:
        delete_where.append("end_date <= :end_period")
        delete_params["end_period"] = parse_yyyymmdd(normalize_period(end_period) or end_period)

    sql_delete = f"DELETE FROM {AGG_TABLE}" + (f" WHERE {' AND '.join(delete_where)}" if delete_where else "")

    sql_upsert = f"""
    INSERT INTO {AGG_TABLE} (
        end_date, symbol, stock_name, holding_fund_count, total_mkv, total_amount, avg_stk_mkv_ratio,
        prev_holding_fund_count, prev_total_mkv, delta_holding_fund_count, delta_total_mkv,
        new_fund_count, exited_fund_count, increased_fund_count, decreased_fund_count, heat_score, updated_at
    ) VALUES (
        :end_date, :symbol, :stock_name, :holding_fund_count, :total_mkv, :total_amount, :avg_stk_mkv_ratio,
        :prev_holding_fund_count, :prev_total_mkv, :delta_holding_fund_count, :delta_total_mkv,
        :new_fund_count, :exited_fund_count, :increased_fund_count, :decreased_fund_count, :heat_score, :updated_at
    )
    ON CONFLICT (end_date, symbol) DO UPDATE SET
        stock_name = EXCLUDED.stock_name,
        holding_fund_count = EXCLUDED.holding_fund_count,
        total_mkv = EXCLUDED.total_mkv,
        total_amount = EXCLUDED.total_amount,
        avg_stk_mkv_ratio = EXCLUDED.avg_stk_mkv_ratio,
        prev_holding_fund_count = EXCLUDED.prev_holding_fund_count,
        prev_total_mkv = EXCLUDED.prev_total_mkv,
        delta_holding_fund_count = EXCLUDED.delta_holding_fund_count,
        delta_total_mkv = EXCLUDED.delta_total_mkv,
        new_fund_count = EXCLUDED.new_fund_count,
        exited_fund_count = EXCLUDED.exited_fund_count,
        increased_fund_count = EXCLUDED.increased_fund_count,
        decreased_fund_count = EXCLUDED.decreased_fund_count,
        heat_score = EXCLUDED.heat_score,
        updated_at = EXCLUDED.updated_at
    """

    with engine.begin() as conn:
        conn.execute(text(sql_delete), delete_params)
        conn.execute(text(sql_upsert), records)

    logger.info(f"聚合完成：{len(records)} 条写入 {AGG_TABLE}")
    return len(records)


# ---------------------------------------------------------------------------
# 查询函数
# ---------------------------------------------------------------------------

def get_latest_agg_period(engine: Engine) -> Optional[date]:
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT MAX(end_date) FROM {AGG_TABLE}")).fetchone()
    return row[0] if row and row[0] else None


def query_hot_stocks_leaderboard(
    period: Optional[str] = None,
    top_n: int = 50,
    order_by: str = "heat_score",
    min_holding_funds: int = 1,
    engine: Optional[Engine] = None,
) -> pd.DataFrame:
    engine = engine or get_engine()

    order_map = {
        "heat_score": "heat_score DESC NULLS LAST",
        "holding_fund_count": "holding_fund_count DESC, total_mkv DESC",
        "total_mkv": "total_mkv DESC, holding_fund_count DESC",
        "delta_holding_fund_count": "delta_holding_fund_count DESC, total_mkv DESC",
        "delta_total_mkv": "delta_total_mkv DESC, holding_fund_count DESC",
    }
    order_sql = order_map.get(order_by, order_map["heat_score"])

    target_period = parse_yyyymmdd(normalize_period(period) or period) if period else get_latest_agg_period(engine)
    if not target_period:
        return pd.DataFrame()

    sql = f"""
    SELECT
        end_date,
        symbol,
        stock_name,
        holding_fund_count,
        total_mkv,
        total_amount,
        avg_stk_mkv_ratio,
        prev_holding_fund_count,
        prev_total_mkv,
        delta_holding_fund_count,
        delta_total_mkv,
        new_fund_count,
        exited_fund_count,
        increased_fund_count,
        decreased_fund_count,
        heat_score
    FROM {AGG_TABLE}
    WHERE end_date = :end_date
      AND holding_fund_count >= :min_holding_funds
    ORDER BY {order_sql}
    LIMIT :top_n
    """
    with engine.connect() as conn:
        return pd.read_sql(
            text(sql),
            conn,
            params={
                "end_date": target_period,
                "min_holding_funds": int(min_holding_funds),
                "top_n": int(top_n),
            },
        )


def query_stock_fund_holding_detail(
    symbol: str,
    period: Optional[str] = None,
    top_n: int = 200,
    engine: Optional[Engine] = None,
) -> pd.DataFrame:
    engine = engine or get_engine()
    symbol = str(symbol or "").strip().upper()
    if not symbol:
        return pd.DataFrame()

    target_period = parse_yyyymmdd(normalize_period(period) or period) if period else get_latest_agg_period(engine)
    if not target_period:
        return pd.DataFrame()

    sql = """
    WITH target AS (
        SELECT CAST(:symbol AS varchar) AS symbol, CAST(:end_date AS date) AS end_date
    ),
    prev_period AS (
        SELECT MAX(end_date) AS prev_end_date
        FROM vw_fund_portfolio
        WHERE end_date < (SELECT end_date FROM target)
    ),
    cur AS (
        SELECT
            p.fund_code,
            p.symbol,
            p.mkv,
            p.amount,
            p.stk_mkv_ratio,
            p.stk_float_ratio
        FROM vw_fund_portfolio p
        JOIN target t
          ON p.end_date = t.end_date
         AND p.symbol = t.symbol
    ),
    prev AS (
        SELECT
            p.fund_code,
            p.symbol,
            p.mkv,
            p.amount
        FROM vw_fund_portfolio p
        JOIN prev_period pp
          ON p.end_date = pp.prev_end_date
        JOIN target t
          ON p.symbol = t.symbol
    )
    SELECT
        c.fund_code,
        fb.name AS fund_name,
        fb.management,
        c.symbol,
        sb.name AS stock_name,
        c.mkv,
        c.amount,
        c.stk_mkv_ratio,
        c.stk_float_ratio,
        p.mkv AS prev_mkv,
        c.mkv - COALESCE(p.mkv, 0) AS delta_mkv,
        CASE
            WHEN p.fund_code IS NULL THEN 'new'
            WHEN c.mkv > p.mkv THEN 'increase'
            WHEN c.mkv < p.mkv THEN 'decrease'
            ELSE 'stable'
        END AS holding_change_flag
    FROM cur c
    LEFT JOIN prev p
      ON p.symbol = c.symbol
     AND p.fund_code = c.fund_code
    LEFT JOIN vw_fund_basic fb
      ON fb.fund_code = c.fund_code
    LEFT JOIN vw_ts_stock_basic sb
      ON sb.ts_code = c.symbol
    ORDER BY c.mkv DESC NULLS LAST
    LIMIT :top_n
    """

    with engine.connect() as conn:
        return pd.read_sql(
            text(sql),
            conn,
            params={"symbol": symbol, "end_date": target_period, "top_n": int(top_n)},
        )


def query_stock_holding_trend(
    symbol: str,
    periods: int = 8,
    engine: Optional[Engine] = None,
) -> pd.DataFrame:
    engine = engine or get_engine()
    symbol = str(symbol or "").strip().upper()
    if not symbol:
        return pd.DataFrame()

    sql = f"""
    SELECT
        end_date,
        symbol,
        stock_name,
        holding_fund_count,
        total_mkv,
        total_amount,
        avg_stk_mkv_ratio,
        delta_holding_fund_count,
        delta_total_mkv,
        new_fund_count,
        exited_fund_count,
        heat_score
    FROM {AGG_TABLE}
    WHERE symbol = :symbol
    ORDER BY end_date DESC
    LIMIT :periods
    """

    with engine.connect() as conn:
        df = pd.read_sql(
            text(sql),
            conn,
            params={"symbol": symbol, "periods": int(periods)},
        )

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.sort_values("end_date").reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# 一键运行
# ---------------------------------------------------------------------------

def run_sync(
    sync_basic: bool = True,
    sync_portfolio: bool = True,
    rebuild_agg: bool = True,
    period: Optional[str] = None,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
) -> dict:
    engine = get_engine()
    ensure_all_tables(engine)
    pro = _init_tushare()

    result = {
        "fund_basic": 0,
        "fund_portfolio": 0,
        "agg_rows": 0,
    }

    if sync_basic:
        result["fund_basic"] = sync_fund_basic(engine, pro)

    if sync_portfolio:
        result["fund_portfolio"] = sync_fund_portfolio(
            engine,
            pro,
            period=period,
            start_period=start_period,
            end_period=end_period,
        )

    if rebuild_agg:
        result["agg_rows"] = rebuild_hot_stock_aggregate(
            engine,
            start_period=start_period if not period else period,
            end_period=end_period if not period else period,
        )

    logger.info(f"fund_hot_stocks run_sync 完成: {result}")
    return result
