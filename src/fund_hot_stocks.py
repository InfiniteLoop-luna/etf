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
from src.sync_tushare_security_data import build_active_stock_sql_clause

DEFAULT_DB_HOST = "127.0.0.1"
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
ACTIVE_STOCK_FILTER_SQL_SB = build_active_stock_sql_clause("sb")
ACTIVE_STOCK_FILTER_SQL_SB_FILTER = build_active_stock_sql_clause("sb_filter")

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
    from src.sync_tushare_security_data import build_db_url as _sync_build_db_url

    return _sync_build_db_url()


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


def normalize_fund_ts_code(raw) -> Optional[str]:
    if raw is None:
        return None
    try:
        if pd.isna(raw):
            return None
    except (TypeError, ValueError):
        pass
    code = str(raw).strip().upper()
    if not code:
        return None
    if "." in code:
        return code
    if len(code) == 6 and code.isdigit():
        return f"{code}.OF"
    return code


def _dedupe_normalized_fund_codes(fund_codes: Optional[Iterable[str]], limit: Optional[int] = None) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for raw_code in fund_codes or []:
        code = normalize_fund_ts_code(raw_code)
        if not code or code in seen:
            continue
        seen.add(code)
        result.append(code)
        if limit and len(result) >= int(limit):
            break
    return result


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


def query_fund_codes_for_portfolio_sync(
    engine: Engine,
    fund_codes: Optional[Iterable[str]] = None,
    fund_keyword: Optional[str] = None,
    management_keyword: Optional[str] = None,
    statuses: Optional[Iterable[str]] = ("L",),
    limit: Optional[int] = None,
) -> list[str]:
    explicit_codes = _dedupe_normalized_fund_codes(fund_codes, limit=limit)
    if explicit_codes:
        return explicit_codes

    where_clauses = ["fund_code IS NOT NULL"]
    params: dict[str, object] = {}

    if fund_keyword:
        params["fund_keyword"] = f"%{str(fund_keyword).strip()}%"
        where_clauses.append("(fund_code ILIKE :fund_keyword OR name ILIKE :fund_keyword)")

    if management_keyword:
        params["management_keyword"] = f"%{str(management_keyword).strip()}%"
        where_clauses.append("management ILIKE :management_keyword")

    normalized_statuses = [str(s).strip().upper() for s in statuses or [] if str(s).strip()]
    if normalized_statuses:
        placeholders = []
        for idx, status in enumerate(normalized_statuses):
            key = f"status_{idx}"
            params[key] = status
            placeholders.append(f":{key}")
        where_clauses.append(f"COALESCE(status, '') IN ({', '.join(placeholders)})")

    sql = f"""
    SELECT DISTINCT fund_code
    FROM vw_fund_basic
    WHERE {' AND '.join(where_clauses)}
    ORDER BY fund_code
    """
    if limit:
        params["limit"] = int(limit)
        sql += "\nLIMIT :limit"

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)

    if df is None or df.empty:
        return []

    return _dedupe_normalized_fund_codes(df["fund_code"].tolist(), limit=limit)


def query_missing_fund_portfolio_tasks(
    engine: Engine,
    periods: Iterable[str],
    fund_codes: Optional[Iterable[str]] = None,
    fund_keyword: Optional[str] = None,
    management_keyword: Optional[str] = None,
    statuses: Optional[Iterable[str]] = ("L",),
    limit: Optional[int] = None,
) -> list[dict[str, str]]:
    normalized_periods = []
    seen_periods = set()
    for raw_period in periods or []:
        p = normalize_period(raw_period)
        if not p or p in seen_periods:
            continue
        seen_periods.add(p)
        normalized_periods.append(p)
    if not normalized_periods:
        return []

    params: dict[str, object] = {}
    period_values = []
    for idx, p in enumerate(normalized_periods):
        key = f"period_{idx}"
        params[key] = p
        period_values.append(f"(to_date(:{key}, 'YYYYMMDD'))")
    period_cte = f"periods(period) AS (VALUES {', '.join(period_values)})"

    explicit_codes = _dedupe_normalized_fund_codes(fund_codes, limit=limit)
    if explicit_codes:
        fund_values = []
        for idx, fund_code in enumerate(explicit_codes):
            key = f"fund_code_{idx}"
            params[key] = fund_code
            fund_values.append(f"(:{key})")
        fund_cte = f"funds(fund_code) AS (VALUES {', '.join(fund_values)})"
    else:
        where_clauses = ["fund_code IS NOT NULL"]

        if fund_keyword:
            params["fund_keyword"] = f"%{str(fund_keyword).strip()}%"
            where_clauses.append("(fund_code ILIKE :fund_keyword OR name ILIKE :fund_keyword)")

        if management_keyword:
            params["management_keyword"] = f"%{str(management_keyword).strip()}%"
            where_clauses.append("management ILIKE :management_keyword")

        normalized_statuses = [str(s).strip().upper() for s in statuses or [] if str(s).strip()]
        if normalized_statuses:
            placeholders = []
            for idx, status in enumerate(normalized_statuses):
                key = f"status_{idx}"
                params[key] = status
                placeholders.append(f":{key}")
            where_clauses.append(f"COALESCE(status, '') IN ({', '.join(placeholders)})")

        limit_sql = ""
        if limit:
            params["limit"] = int(limit)
            limit_sql = "\n        LIMIT :limit"

        fund_cte = f"""
        funds AS (
            SELECT DISTINCT fund_code
            FROM vw_fund_basic
            WHERE {' AND '.join(where_clauses)}
            ORDER BY fund_code{limit_sql}
        )
        """

    sql = f"""
    WITH {period_cte},
    {fund_cte}
    SELECT
        f.fund_code,
        to_char(p.period, 'YYYYMMDD') AS period
    FROM funds f
    CROSS JOIN periods p
    LEFT JOIN vw_fund_portfolio v
      ON v.fund_code = f.fund_code
     AND v.end_date = p.period
    WHERE v.fund_code IS NULL
    ORDER BY p.period, f.fund_code
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)

    if df is None or df.empty:
        return []

    tasks: list[dict[str, str]] = []
    for row in df.to_dict("records"):
        fund_code = normalize_fund_ts_code(row.get("fund_code"))
        period_value = normalize_period(row.get("period"))
        if fund_code and period_value:
            tasks.append({"fund_code": fund_code, "period": period_value})
    return tasks


def _upsert_fund_portfolio_rows(engine: Engine, rows: list[dict]) -> int:
    return upsert_rows(
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
        n = _upsert_fund_portfolio_rows(engine, rows)
        logger.info(f"fund_portfolio period={p}: 写入 {n} 行")
        total += n
        time.sleep(DEFAULT_API_SLEEP)

    logger.info(f"fund_portfolio 同步完成，总写入 {total} 行")
    return total


# ---------------------------------------------------------------------------
# 季度聚合
# ---------------------------------------------------------------------------

def sync_fund_portfolio_by_fund(
    engine: Engine,
    pro,
    period: Optional[str] = None,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    fund_codes: Optional[Iterable[str]] = None,
    fund_keyword: Optional[str] = None,
    management_keyword: Optional[str] = None,
    statuses: Optional[Iterable[str]] = ("L",),
    limit: Optional[int] = None,
    api_sleep: Optional[float] = DEFAULT_API_SLEEP,
) -> int:
    periods = resolve_portfolio_periods(engine, period=period, start_period=start_period, end_period=end_period)
    if not periods:
        logger.info("fund_portfolio by_fund: no periods to sync")
        return 0

    fund_code_list = query_fund_codes_for_portfolio_sync(
        engine,
        fund_codes=fund_codes,
        fund_keyword=fund_keyword,
        management_keyword=management_keyword,
        statuses=statuses,
        limit=limit,
    )
    if not fund_code_list:
        logger.warning("fund_portfolio by_fund: no fund codes matched")
        return 0

    sleep_seconds = max(0.0, float(DEFAULT_API_SLEEP if api_sleep is None else api_sleep))
    total = 0
    for p in periods:
        period_total = 0
        for fund_code in fund_code_list:
            try:
                df = pro.fund_portfolio(ts_code=fund_code, period=p)
            except Exception as exc:
                logger.warning(f"fund_portfolio ts_code={fund_code} period={p} fetch failed: {exc}")
                if sleep_seconds:
                    time.sleep(sleep_seconds)
                continue

            if df is None or df.empty:
                logger.info(f"fund_portfolio ts_code={fund_code} period={p}: 0 rows")
                if sleep_seconds:
                    time.sleep(sleep_seconds)
                continue

            rows = []
            for row in df.to_dict("records"):
                normalized_row = dict(row)
                normalized_row["ts_code"] = normalize_fund_ts_code(normalized_row.get("ts_code")) or fund_code
                normalized_row["end_date"] = normalized_row.get("end_date") or p
                rows.append(normalized_row)

            n = _upsert_fund_portfolio_rows(engine, rows)
            period_total += n
            total += n
            logger.info(f"fund_portfolio ts_code={fund_code} period={p}: wrote {n} rows")
            if sleep_seconds:
                time.sleep(sleep_seconds)

        logger.info(f"fund_portfolio by_fund period={p}: wrote {period_total} rows")

    logger.info(
        f"fund_portfolio by_fund sync finished: funds={len(fund_code_list)}, periods={len(periods)}, rows={total}"
    )
    return total


def sync_fund_portfolio_dynamic(
    engine: Engine,
    pro,
    period: Optional[str] = None,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    fund_codes: Optional[Iterable[str]] = None,
    fund_keyword: Optional[str] = None,
    management_keyword: Optional[str] = None,
    statuses: Optional[Iterable[str]] = ("L",),
    limit: Optional[int] = None,
    refresh_basic: bool = True,
    api_sleep: Optional[float] = DEFAULT_API_SLEEP,
) -> dict[str, int]:
    periods = resolve_portfolio_periods(engine, period=period, start_period=start_period, end_period=end_period)
    result = {
        "fund_basic": 0,
        "fund_portfolio": 0,
        "missing_tasks": 0,
    }
    if not periods:
        logger.info("fund_portfolio dynamic: no periods to sync")
        return result

    if refresh_basic:
        result["fund_basic"] = sync_fund_basic(engine, pro)

    tasks = query_missing_fund_portfolio_tasks(
        engine,
        periods=periods,
        fund_codes=fund_codes,
        fund_keyword=fund_keyword,
        management_keyword=management_keyword,
        statuses=statuses,
        limit=limit,
    )
    result["missing_tasks"] = len(tasks)
    if not tasks:
        logger.info("fund_portfolio dynamic: no missing fund-period tasks")
        return result

    sleep_seconds = max(0.0, float(DEFAULT_API_SLEEP if api_sleep is None else api_sleep))
    for task in tasks:
        fund_code = task["fund_code"]
        p = task["period"]
        try:
            df = pro.fund_portfolio(ts_code=fund_code, period=p)
        except Exception as exc:
            logger.warning(f"fund_portfolio dynamic ts_code={fund_code} period={p} fetch failed: {exc}")
            if sleep_seconds:
                time.sleep(sleep_seconds)
            continue

        if df is None or df.empty:
            logger.info(f"fund_portfolio dynamic ts_code={fund_code} period={p}: 0 rows")
            if sleep_seconds:
                time.sleep(sleep_seconds)
            continue

        rows = []
        for row in df.to_dict("records"):
            normalized_row = dict(row)
            normalized_row["ts_code"] = normalize_fund_ts_code(normalized_row.get("ts_code")) or fund_code
            normalized_row["end_date"] = normalized_row.get("end_date") or p
            rows.append(normalized_row)

        n = _upsert_fund_portfolio_rows(engine, rows)
        result["fund_portfolio"] += n
        logger.info(f"fund_portfolio dynamic ts_code={fund_code} period={p}: wrote {n} rows")
        if sleep_seconds:
            time.sleep(sleep_seconds)

    logger.info(f"fund_portfolio dynamic sync finished: {result}")
    return result


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
      AND {ACTIVE_STOCK_FILTER_SQL_SB}
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
    fund_type_filter: Optional[str] = None,
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

    if fund_type_filter and fund_type_filter != "全部":
        sql = f"""
        WITH funds AS (
            SELECT DISTINCT fund_code
            FROM vw_fund_basic
            WHERE fund_type = :fund_type_filter
               OR invest_type = :fund_type_filter
        ),
        cur AS (
            SELECT
                p.end_date,
                p.symbol,
                COALESCE(sb.name, p.symbol) AS stock_name,
                COUNT(DISTINCT p.fund_code) AS holding_fund_count,
                SUM(COALESCE(p.mkv, 0)) AS total_mkv,
                SUM(COALESCE(p.amount, 0)) AS total_amount,
                AVG(COALESCE(p.stk_mkv_ratio, 0)) AS avg_stk_mkv_ratio
            FROM vw_fund_portfolio p
            JOIN funds f ON f.fund_code = p.fund_code
            LEFT JOIN vw_ts_stock_basic sb ON sb.ts_code = p.symbol
            WHERE p.end_date = :end_date
              AND {ACTIVE_STOCK_FILTER_SQL_SB}
            GROUP BY p.end_date, p.symbol, COALESCE(sb.name, p.symbol)
        ),
        prev_date AS (
            SELECT MAX(end_date) AS prev_end_date
            FROM vw_fund_portfolio
            WHERE end_date < :end_date
        ),
        prev AS (
            SELECT
                p.symbol,
                COUNT(DISTINCT p.fund_code) AS prev_holding_fund_count,
                SUM(COALESCE(p.mkv, 0)) AS prev_total_mkv
            FROM vw_fund_portfolio p
            JOIN funds f ON f.fund_code = p.fund_code
            JOIN prev_date d ON p.end_date = d.prev_end_date
            GROUP BY p.symbol
        )
        SELECT
            c.end_date,
            c.symbol,
            c.stock_name,
            c.holding_fund_count,
            c.total_mkv,
            c.total_amount,
            c.avg_stk_mkv_ratio,
            p.prev_holding_fund_count,
            p.prev_total_mkv,
            c.holding_fund_count - COALESCE(p.prev_holding_fund_count, 0) AS delta_holding_fund_count,
            c.total_mkv - COALESCE(p.prev_total_mkv, 0) AS delta_total_mkv,
            NULL::bigint AS new_fund_count,
            NULL::bigint AS exited_fund_count,
            NULL::bigint AS increased_fund_count,
            NULL::bigint AS decreased_fund_count,
            (
                COALESCE(c.holding_fund_count, 0) * 0.5
                + LEAST(COALESCE(c.total_mkv, 0) / 100000000.0, 100) * 0.3
                + LEAST(COALESCE(c.avg_stk_mkv_ratio, 0), 20) * 0.2
            ) AS heat_score
        FROM cur c
        LEFT JOIN prev p ON p.symbol = c.symbol
        WHERE c.holding_fund_count >= :min_holding_funds
        ORDER BY """ + order_sql + """
        LIMIT :top_n
        """
        params = {
            "end_date": target_period,
            "min_holding_funds": int(min_holding_funds),
            "top_n": int(top_n),
            "fund_type_filter": fund_type_filter,
        }
    else:
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
        params = {
            "end_date": target_period,
            "min_holding_funds": int(min_holding_funds),
            "top_n": int(top_n),
        }

    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def query_stock_fund_holding_detail(
    symbol: str,
    period: Optional[str] = None,
    top_n: int = 200,
    fund_type_filter: Optional[str] = None,
    engine: Optional[Engine] = None,
) -> pd.DataFrame:
    engine = engine or get_engine()
    symbol = str(symbol or "").strip().upper()
    if not symbol:
        return pd.DataFrame()

    target_period = parse_yyyymmdd(normalize_period(period) or period) if period else get_latest_agg_period(engine)
    if not target_period:
        return pd.DataFrame()

    sql = f"""
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
        JOIN vw_ts_stock_basic sb_filter
          ON sb_filter.ts_code = p.symbol
         AND {ACTIVE_STOCK_FILTER_SQL_SB_FILTER}
        LEFT JOIN vw_fund_basic fb_filter
          ON fb_filter.fund_code = p.fund_code
        WHERE (:fund_type_filter IS NULL OR :fund_type_filter = '全部' OR fb_filter.fund_type = :fund_type_filter OR fb_filter.invest_type = :fund_type_filter)
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
        LEFT JOIN vw_fund_basic fb_filter
          ON fb_filter.fund_code = p.fund_code
        WHERE (:fund_type_filter IS NULL OR :fund_type_filter = '全部' OR fb_filter.fund_type = :fund_type_filter OR fb_filter.invest_type = :fund_type_filter)
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
     AND {ACTIVE_STOCK_FILTER_SQL_SB}
    ORDER BY c.mkv DESC NULLS LAST
    LIMIT :top_n
    """

    with engine.connect() as conn:
        return pd.read_sql(
            text(sql),
            conn,
            params={"symbol": symbol, "end_date": target_period, "top_n": int(top_n), "fund_type_filter": fund_type_filter},
        )


def query_stock_holding_trend(
    symbol: str,
    periods: int = 8,
    fund_type_filter: Optional[str] = None,
    engine: Optional[Engine] = None,
) -> pd.DataFrame:
    engine = engine or get_engine()
    symbol = str(symbol or "").strip().upper()
    if not symbol:
        return pd.DataFrame()

    if fund_type_filter and fund_type_filter != "全部":
        sql = f"""
        SELECT
            p.end_date,
            p.symbol,
            COALESCE(sb.name, p.symbol) AS stock_name,
            COUNT(DISTINCT p.fund_code) AS holding_fund_count,
            SUM(COALESCE(p.mkv, 0)) AS total_mkv,
            SUM(COALESCE(p.amount, 0)) AS total_amount,
            AVG(COALESCE(p.stk_mkv_ratio, 0)) AS avg_stk_mkv_ratio
        FROM vw_fund_portfolio p
        LEFT JOIN vw_fund_basic fb ON fb.fund_code = p.fund_code
        LEFT JOIN vw_ts_stock_basic sb ON sb.ts_code = p.symbol
        WHERE p.symbol = :symbol
          AND (fb.fund_type = :fund_type_filter OR fb.invest_type = :fund_type_filter)
          AND {ACTIVE_STOCK_FILTER_SQL_SB}
        GROUP BY p.end_date, p.symbol, COALESCE(sb.name, p.symbol)
        ORDER BY p.end_date DESC
        LIMIT :periods
        """
        params = {"symbol": symbol, "periods": int(periods), "fund_type_filter": fund_type_filter}
    else:
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
        params = {"symbol": symbol, "periods": int(periods)}

    with engine.connect() as conn:
        df = pd.read_sql(
            text(sql),
            conn,
            params=params,
        )

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.sort_values("end_date").reset_index(drop=True)
    return df


def _pick_latest_holder_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    for col in ["ann_date", "end_date"]:
        if col in out.columns:
            out[col] = out[col].astype(str).str.replace("-", "", regex=False)

    if "end_date" in out.columns and out["end_date"].notna().any():
        latest_end = out["end_date"].dropna().max()
        out = out[out["end_date"] == latest_end]
    if "ann_date" in out.columns and out["ann_date"].notna().any():
        latest_ann = out["ann_date"].dropna().max()
        out = out[out["ann_date"] == latest_ann]

    for col in ["hold_amount", "hold_ratio", "hold_float_ratio", "hold_change"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    order_cols = [c for c in ["hold_ratio", "hold_float_ratio", "hold_amount"] if c in out.columns]
    if order_cols:
        out = out.sort_values(order_cols, ascending=False, na_position="last")
    return out.reset_index(drop=True)


def query_stock_top10_shareholders(
    symbol: str,
    period: Optional[str] = None,
) -> dict:
    ts_code = str(symbol or "").strip().upper()
    if not ts_code:
        return {"top10_holders": pd.DataFrame(), "top10_floatholders": pd.DataFrame(), "errors": {}}

    pro = _init_tushare()
    target_period = normalize_period(period) or yyyymmdd(period)
    kwargs = {"ts_code": ts_code}
    if target_period:
        kwargs["period"] = target_period

    errors = {}
    try:
        top10_holders = pro.top10_holders(**kwargs)
    except Exception as exc:
        errors["top10_holders"] = str(exc)
        top10_holders = pd.DataFrame()

    try:
        top10_floatholders = pro.top10_floatholders(**kwargs)
    except Exception as exc:
        errors["top10_floatholders"] = str(exc)
        top10_floatholders = pd.DataFrame()

    return {
        "top10_holders": _pick_latest_holder_snapshot(top10_holders),
        "top10_floatholders": _pick_latest_holder_snapshot(top10_floatholders),
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# 一键运行
# ---------------------------------------------------------------------------



def search_funds(
    keyword: str,
    limit: int = 20,
    engine: Optional[Engine] = None,
) -> pd.DataFrame:
    engine = engine or get_engine()
    keyword = str(keyword or "").strip()
    if not keyword:
        return pd.DataFrame()

    kw = keyword.upper()
    sql = """
    SELECT
        fund_code,
        name,
        management,
        fund_type
    FROM vw_fund_basic
    WHERE fund_code ILIKE :prefix
       OR COALESCE(name, '') ILIKE :contains
       OR COALESCE(management, '') ILIKE :contains
    ORDER BY
        CASE
            WHEN UPPER(fund_code) = :exact THEN 0
            WHEN UPPER(COALESCE(name, '')) = :exact THEN 1
            WHEN UPPER(fund_code) LIKE :prefix_upper THEN 2
            WHEN UPPER(COALESCE(name, '')) LIKE :prefix_upper THEN 3
            ELSE 9
        END,
        fund_code
    LIMIT :limit
    """

    with engine.connect() as conn:
        return pd.read_sql(
            text(sql),
            conn,
            params={
                'prefix': f'{keyword}%',
                'contains': f'%{keyword}%',
                'exact': kw,
                'prefix_upper': f'{kw}%',
                'limit': int(limit),
            },
        )

def query_fund_preference_snapshot(
    fund_code: str,
    period: Optional[str] = None,
    top_n: int = 20,
    engine: Optional[Engine] = None,
) -> pd.DataFrame:
    engine = engine or get_engine()
    fund_code = str(fund_code or "").strip().upper()
    if not fund_code:
        return pd.DataFrame()

    target_period = period or get_latest_agg_period(engine)
    if not target_period:
        return pd.DataFrame()
    target_period = str(target_period).replace("-", "")

    sql = f"""
    WITH target AS (
        SELECT CAST(:fund_code AS varchar) AS fund_code, CAST(:end_date AS date) AS end_date
    ), prev_period AS (
        SELECT MAX(end_date) AS prev_end_date
        FROM vw_fund_portfolio
        WHERE end_date < (SELECT end_date FROM target)
    ), cur AS (
        SELECT
            p.fund_code,
            p.ann_date,
            p.end_date,
            p.symbol,
            sb.name AS stock_name,
            p.mkv,
            p.amount,
            p.stk_mkv_ratio,
            p.stk_float_ratio
        FROM vw_fund_portfolio p
        JOIN target t ON p.fund_code = t.fund_code AND p.end_date = t.end_date
        JOIN vw_ts_stock_basic sb_filter
          ON sb_filter.ts_code = p.symbol
         AND {ACTIVE_STOCK_FILTER_SQL_SB_FILTER}
        LEFT JOIN vw_ts_stock_basic sb ON sb.ts_code = p.symbol AND {ACTIVE_STOCK_FILTER_SQL_SB}
    ), prev AS (
        SELECT
            p.fund_code,
            p.symbol,
            p.mkv AS prev_mkv
        FROM vw_fund_portfolio p
        JOIN target t ON p.fund_code = t.fund_code
        JOIN prev_period pp ON p.end_date = pp.prev_end_date
    )
    SELECT
        c.fund_code,
        fb.name AS fund_name,
        fb.management,
        fb.fund_type,
        fb.invest_type,
        c.ann_date,
        c.end_date,
        c.symbol,
        COALESCE(c.stock_name, c.symbol) AS stock_name,
        c.mkv,
        c.amount,
        c.stk_mkv_ratio,
        c.stk_float_ratio,
        p.prev_mkv,
        c.mkv - COALESCE(p.prev_mkv, 0) AS delta_mkv,
        CASE
            WHEN p.symbol IS NULL THEN 'new'
            WHEN c.mkv > p.prev_mkv THEN 'increase'
            WHEN c.mkv < p.prev_mkv THEN 'decrease'
            ELSE 'stable'
        END AS holding_change_flag
    FROM cur c
    LEFT JOIN prev p ON p.fund_code = c.fund_code AND p.symbol = c.symbol
    LEFT JOIN vw_fund_basic fb ON fb.fund_code = c.fund_code
    ORDER BY c.mkv DESC NULLS LAST
    LIMIT :top_n
    """

    with engine.connect() as conn:
        return pd.read_sql(
            text(sql),
            conn,
            params={"fund_code": fund_code, "end_date": target_period, "top_n": int(top_n)},
        )

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
