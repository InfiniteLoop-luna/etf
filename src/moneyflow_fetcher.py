# -*- coding: utf-8 -*-
"""
资金流向数据获取与入库模块
支持以下 Tushare 接口：
  - moneyflow         个股资金流向（标准口径）
  - moneyflow_hsgt    沪深港通资金流向（北向/南向）
  - moneyflow_ind_ths 行业资金流向（同花顺口径）
  - moneyflow_dc_ind  板块/行业资金流向（东方财富口径）

数据起始：2026-01-01
存储：PostgreSQL JSONB landing 表（与现有 sync_tushare_security_data.py 同一架构）
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import sys
import time
from datetime import datetime, timedelta, date
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.volume_fetcher import _init_tushare

# ---------------------------------------------------------------------------
# 常量配置
# ---------------------------------------------------------------------------
DEFAULT_START_DATE = "20260101"          # ??????
DEFAULT_API_SLEEP = float(os.getenv("TUSHARE_MF_API_SLEEP", "0.4"))
DEFAULT_INCREMENTAL_LOOKBACK_DAYS = int(os.getenv("TUSHARE_MF_LOOKBACK_DAYS", "1"))
DEFAULT_MIN_COVERAGE_RATIO = float(os.getenv("TUSHARE_MF_MIN_COVERAGE_RATIO", "0.9"))
DEFAULT_PUBLISH_CUTOFF_HOUR = int(os.getenv("TUSHARE_MF_PUBLISH_CUTOFF_HOUR", "20"))
DEFAULT_DB_HOST = "67.216.207.73"
DEFAULT_DB_PORT = 5432
DEFAULT_DB_NAME = "postgres"
DEFAULT_DB_USER = "postgres"
DEFAULT_DB_SSLMODE = "disable"

# ????????????
MONEYFLOW_TABLES = {
    "moneyflow":         "ts_moneyflow",
    "moneyflow_hsgt":    "ts_moneyflow_hsgt",
    "moneyflow_ind_ths": "ts_moneyflow_ind_ths",
    "moneyflow_dc_ind":  "ts_moneyflow_dc_ind",
}

# 个股资金流向字段（moneyflow）
MONEYFLOW_FIELDS = (
    "ts_code,trade_date,"
    "buy_sm_vol,buy_sm_amount,sell_sm_vol,sell_sm_amount,"
    "buy_md_vol,buy_md_amount,sell_md_vol,sell_md_amount,"
    "buy_lg_vol,buy_lg_amount,sell_lg_vol,sell_lg_amount,"
    "buy_elg_vol,buy_elg_amount,sell_elg_vol,sell_elg_amount,"
    "net_mf_vol,net_mf_amount"
)

# 沪深港通字段（moneyflow_hsgt）
HSGT_FIELDS = (
    "trade_date,ggt_ss,ggt_sz,hgt,sgt,north_money,south_money"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
ACTIVE_STOCK_COUNT_CACHE: Optional[int] = None


# ---------------------------------------------------------------------------
# 数据库工具函数
# ---------------------------------------------------------------------------

def build_db_url():
    # 优先尝试从 sync_tushare_security_data 复用完全相同的凭证逻辑
    try:
        from src.sync_tushare_security_data import build_db_url as _sync_build_db_url
        return _sync_build_db_url()
    except (ImportError, RuntimeError):
        pass

    direct_url = os.getenv("ETF_PG_URL") or os.getenv("DATABASE_URL")
    if direct_url:
        return direct_url

    password = os.getenv("ETF_PG_PASSWORD") or os.getenv("PGPASSWORD")
    if not password:
        # 最后尝试从 Streamlit secrets 读取
        try:
            import streamlit as st
            password = (
                st.secrets.get("ETF_PG_PASSWORD")
                or st.secrets.get("PGPASSWORD")
                or st.secrets.get("database", {}).get("password")
            )
            if password:
                os.environ["ETF_PG_PASSWORD"] = str(password)
            # 同时尝试读取其他连接参数
            for key in ("ETF_PG_HOST", "ETF_PG_USER", "ETF_PG_DATABASE", "ETF_PG_URL"):
                val = st.secrets.get(key)
                if val and not os.environ.get(key):
                    os.environ[key] = str(val)
        except Exception:
            pass

    password = os.getenv("ETF_PG_PASSWORD") or os.getenv("PGPASSWORD")
    if not password:
        raise RuntimeError(
            "未配置数据库密码，请设置环境变量 ETF_PG_PASSWORD 或 PGPASSWORD，"
            "或在 .streamlit/secrets.toml 中配置"
        )

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
    """创建 JSONB landing 表（与现有架构兼容）"""
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
        for statement in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(statement))
    logger.info(f"✓ 表 {table_name} 已就绪")


def ensure_all_tables(engine: Engine):
    for table_name in MONEYFLOW_TABLES.values():
        ensure_landing_table(engine, table_name)
    ensure_normalized_views(engine)


def ensure_normalized_views(engine: Engine):
    """创建规范化视图，便于 SQL 查询和 Streamlit 调用"""
    views = {
        "ts_moneyflow": ("vw_moneyflow", [
            "ts_code", "trade_date",
            "buy_sm_vol::bigint AS buy_sm_vol", "buy_sm_amount::numeric AS buy_sm_amount",
            "sell_sm_vol::bigint AS sell_sm_vol", "sell_sm_amount::numeric AS sell_sm_amount",
            "buy_md_vol::bigint AS buy_md_vol", "buy_md_amount::numeric AS buy_md_amount",
            "sell_md_vol::bigint AS sell_md_vol", "sell_md_amount::numeric AS sell_md_amount",
            "buy_lg_vol::bigint AS buy_lg_vol", "buy_lg_amount::numeric AS buy_lg_amount",
            "sell_lg_vol::bigint AS sell_lg_vol", "sell_lg_amount::numeric AS sell_lg_amount",
            "buy_elg_vol::bigint AS buy_elg_vol", "buy_elg_amount::numeric AS buy_elg_amount",
            "sell_elg_vol::bigint AS sell_elg_vol", "sell_elg_amount::numeric AS sell_elg_amount",
            "net_mf_vol::bigint AS net_mf_vol", "net_mf_amount::numeric AS net_mf_amount",
        ]),
        "ts_moneyflow_hsgt": ("vw_moneyflow_hsgt", [
            "trade_date",
            "ggt_ss::numeric AS ggt_ss",
            "ggt_sz::numeric AS ggt_sz",
            "hgt::numeric AS hgt",
            "sgt::numeric AS sgt",
            "north_money::numeric AS north_money",
            "south_money::numeric AS south_money",
        ]),
        "ts_moneyflow_ind_ths": ("vw_moneyflow_ind_ths", [
            "trade_date", "ts_code",
            "industry AS industry",
            "lead_stock AS lead_stock",
            "pct_change::numeric AS pct_change",
            "company_num::integer AS company_num",
            "pct_change_stock::numeric AS pct_change_stock",
            "close::numeric AS close",
            "net_buy_amount::numeric AS net_buy_amount",
            "net_sell_amount::numeric AS net_sell_amount",
            "net_amount::numeric AS net_amount",
        ]),
        "ts_moneyflow_dc_ind": ("vw_moneyflow_dc_ind", [
            "trade_date", "ts_code",
            "name AS name",
            "pct_change::numeric AS pct_change",
            "net_amount::numeric AS net_amount",
            "net_amount_rate::numeric AS net_amount_rate",
            "buy_elg_amount::numeric AS buy_elg_amount",
            "buy_elg_amount_rate::numeric AS buy_elg_amount_rate",
            "buy_lg_amount::numeric AS buy_lg_amount",
            "buy_lg_amount_rate::numeric AS buy_lg_amount_rate",
            "buy_md_amount::numeric AS buy_md_amount",
            "buy_md_amount_rate::numeric AS buy_md_amount_rate",
            "buy_sm_amount::numeric AS buy_sm_amount",
            "buy_sm_amount_rate::numeric AS buy_sm_amount_rate",
        ]),
    }

    NATIVE_COLS = {"business_key", "dataset_name", "ts_code", "trade_date",
                   "ann_date", "end_date", "period", "record_hash", "ingested_at", "payload"}

    with engine.begin() as conn:
        for table_name, (view_name, cols) in views.items():
            # 始终选出这几个基础列
            select_parts = [
                "business_key", "dataset_name", "ts_code", "trade_date",
                "record_hash", "ingested_at", "payload"
            ]
            # 将 cols 列表中的字段展开为 JSONB 表达式，跳过已是原生列的字段
            for col_expr in cols:
                # 解析出原始字段名和别名
                if "::" in col_expr:
                    raw_part, rest = col_expr.split("::", 1)
                    raw = raw_part.strip()
                    if " AS " in rest:
                        cast_type, alias = rest.split(" AS ", 1)
                        cast_type = cast_type.strip()
                        alias = alias.strip()
                    else:
                        cast_type = rest.strip()
                        alias = raw
                    if alias in NATIVE_COLS or raw in NATIVE_COLS:
                        continue
                    select_parts.append(
                        f"NULLIF(payload->>'{raw}', '')::{cast_type} AS {alias}"
                    )
                elif " AS " in col_expr:
                    raw, alias = col_expr.split(" AS ", 1)
                    raw = raw.strip()
                    alias = alias.strip()
                    if alias in NATIVE_COLS or raw in NATIVE_COLS:
                        continue
                    select_parts.append(
                        f"NULLIF(payload->>'{raw}', '') AS {alias}"
                    )
                else:
                    field = col_expr.strip()
                    if field in NATIVE_COLS:
                        continue
                    select_parts.append(
                        f"NULLIF(payload->>'{field}', '') AS {field}"
                    )

            sql = f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT {', '.join(select_parts)}
            FROM {table_name}
            """
            conn.execute(text(sql))
            logger.info(f"✓ 视图 {view_name} 已更新")


# ---------------------------------------------------------------------------
# 哈希 & 业务键
# ---------------------------------------------------------------------------

def compute_record_hash(payload: dict) -> str:
    content = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(content.encode()).hexdigest()[:64]


def make_business_key(dataset_name: str, ts_code: Optional[str], trade_date_str: Optional[str]) -> str:
    parts = [dataset_name]
    if ts_code:
        parts.append(ts_code)
    if trade_date_str:
        parts.append(trade_date_str)
    return "|".join(parts)


# ---------------------------------------------------------------------------
# 增量日期解析
# ---------------------------------------------------------------------------

def get_max_trade_date(engine: Engine, table_name: str) -> Optional[str]:
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT MAX(trade_date) FROM {table_name}")).fetchone()
    if row and row[0]:
        return row[0].strftime("%Y%m%d")
    return None


def resolve_start_date(
    engine: Engine,
    table_name: str,
    force_start: Optional[str] = None,
    lookback_days: int = 0,
) -> str:
    if force_start:
        return force_start
    existing_max = get_max_trade_date(engine, table_name)
    if not existing_max:
        return DEFAULT_START_DATE
    lookback_days = max(0, int(lookback_days or 0))
    start_day = (datetime.strptime(existing_max, "%Y%m%d") - timedelta(days=lookback_days)).strftime("%Y%m%d")
    return max(DEFAULT_START_DATE, start_day)


# ---------------------------------------------------------------------------
# ??????
# ---------------------------------------------------------------------------

def upsert_rows(engine: Engine, table_name: str, dataset_name: str, rows: list[dict],
                ts_code_fn=None, trade_date_fn=None):
    """将 rows 列表批量 upsert 入 JSONB landing 表"""
    if not rows:
        return 0

    records = []
    for row in rows:
        payload = {k: (None if pd.isna(v) else v) for k, v in row.items()
                   if not isinstance(v, float) or not pd.isna(v)}
        # 处理 NaN
        cleaned = {}
        for k, v in row.items():
            if isinstance(v, float) and pd.isna(v):
                cleaned[k] = None
            elif hasattr(v, "item"):
                cleaned[k] = v.item()
            else:
                cleaned[k] = v
        payload = cleaned

        ts_code = ts_code_fn(row) if ts_code_fn else row.get("ts_code")
        trade_date_raw = trade_date_fn(row) if trade_date_fn else row.get("trade_date")

        # 解析 trade_date
        trade_date_val = None
        if trade_date_raw:
            td_str = str(trade_date_raw).replace("-", "").strip()
            if len(td_str) == 8 and td_str.isdigit():
                trade_date_val = datetime.strptime(td_str, "%Y%m%d").date()

        bk = make_business_key(dataset_name, ts_code, str(trade_date_raw) if trade_date_raw else None)
        record_hash = compute_record_hash(payload)

        records.append({
            "business_key": bk,
            "dataset_name": dataset_name,
            "ts_code": ts_code,
            "trade_date": trade_date_val,
            "ann_date": None,
            "end_date": None,
            "period": None,
            "record_hash": record_hash,
            "payload": json.dumps(payload, ensure_ascii=False, default=str),
        })

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


# ---------------------------------------------------------------------------
# 枚举交易日期区间（逐日）
# ---------------------------------------------------------------------------

def iter_trade_dates(start: str, end: str):
    """生成 start~end 之间所有 YYYYMMDD 字符串（含首尾）"""
    d = datetime.strptime(start, "%Y%m%d")
    e = datetime.strptime(end, "%Y%m%d")
    while d <= e:
        yield d.strftime("%Y%m%d")
        d += timedelta(days=1)


def get_today_str() -> str:
    return datetime.now().strftime("%Y%m%d")


def get_open_trade_dates(pro, start: str, end: str) -> list[str]:
    calendar_df = pro.trade_cal(exchange="SSE", start_date=start, end_date=end)
    if calendar_df is None or calendar_df.empty:
        return []
    filtered = calendar_df[calendar_df["is_open"] == 1].copy()
    if filtered.empty:
        return []
    return sorted(filtered["cal_date"].astype(str).tolist())


def get_required_trade_dates(pro, start: str, end: str, publish_cutoff_hour: int) -> list[str]:
    trade_dates = get_open_trade_dates(pro, start, end)
    if not trade_dates:
        return []

    now = datetime.now()
    today_str = now.strftime("%Y%m%d")
    required_dates = []
    for trade_date in trade_dates:
        if trade_date > today_str:
            continue
        if trade_date == today_str and now.hour < publish_cutoff_hour:
            continue
        required_dates.append(trade_date)
    return required_dates


def get_active_stock_count(pro) -> int:
    global ACTIVE_STOCK_COUNT_CACHE
    if ACTIVE_STOCK_COUNT_CACHE is not None:
        return ACTIVE_STOCK_COUNT_CACHE

    df = pro.stock_basic(list_status="L", fields="ts_code")
    if df is None or df.empty or "ts_code" not in df.columns:
        raise RuntimeError("??????????????? moneyflow ???")

    ACTIVE_STOCK_COUNT_CACHE = int(df["ts_code"].astype(str).str.strip().nunique())
    return ACTIVE_STOCK_COUNT_CACHE


def validate_moneyflow_result(pro, returned_counts: dict[str, int], required_trade_dates: list[str], min_coverage_ratio: float):
    if not required_trade_dates:
        return

    active_stock_count = get_active_stock_count(pro)
    minimum_required = max(1, math.ceil(active_stock_count * min_coverage_ratio))
    insufficient_dates = []
    for trade_date in required_trade_dates:
        row_count = int(returned_counts.get(trade_date, 0))
        if row_count < minimum_required:
            insufficient_dates.append(f"{trade_date}={row_count}")

    if insufficient_dates:
        raise RuntimeError(
            "moneyflow ???????"
            f"??????????? {minimum_required} ???"
            f"??????? {active_stock_count}??? {min_coverage_ratio:.0%}??"
            f"????: {', '.join(insufficient_dates[:10])}"
        )


def validate_trade_date_coverage(dataset_name: str, returned_dates: set[str], required_trade_dates: list[str]):
    if not required_trade_dates:
        return

    missing_dates = [trade_date for trade_date in required_trade_dates if trade_date not in returned_dates]
    if missing_dates:
        raise RuntimeError(
            f"{dataset_name} ??????? {len(missing_dates)} ???????: {', '.join(missing_dates[:10])}"
        )


# ---------------------------------------------------------------------------
# 1. 个股资金流向（moneyflow）
# ---------------------------------------------------------------------------

def sync_moneyflow(engine: Engine, pro, start_date: Optional[str] = None,
                   end_date: Optional[str] = None) -> int:
    """
    按日期范围拉取全市场个股资金流向（每次拉一天，循环）。
    返回写入总行数。
    """
    table = MONEYFLOW_TABLES["moneyflow"]
    s = resolve_start_date(engine, table, start_date, lookback_days=DEFAULT_INCREMENTAL_LOOKBACK_DAYS)
    e = end_date or get_today_str()

    if s > e:
        logger.info(f"moneyflow: 数据已最新（{s} > {e}），跳过")
        return 0

    logger.info(f"moneyflow: 拉取 {s} → {e}")
    total = 0

    for dt in iter_trade_dates(s, e):
        try:
            df = pro.moneyflow(trade_date=dt, fields=MONEYFLOW_FIELDS)
            if df is not None and not df.empty:
                n = upsert_rows(engine, table, "moneyflow", df.to_dict("records"))
                total += n
                logger.info(f"  moneyflow {dt}: {n} 行")
            else:
                logger.debug(f"  moneyflow {dt}: 无数据（非交易日或数据未发布）")
        except Exception as exc:
            logger.warning(f"  moneyflow {dt} 失败: {exc}")
        time.sleep(DEFAULT_API_SLEEP)

    logger.info(f"moneyflow 完成，共写入 {total} 行")
    return total


# ---------------------------------------------------------------------------
# 2. 沪深港通资金流向（moneyflow_hsgt）
# ---------------------------------------------------------------------------

def sync_moneyflow_hsgt(engine: Engine, pro, start_date: Optional[str] = None,
                        end_date: Optional[str] = None) -> int:
    table = MONEYFLOW_TABLES["moneyflow_hsgt"]
    s = resolve_start_date(engine, table, start_date, lookback_days=DEFAULT_INCREMENTAL_LOOKBACK_DAYS)
    e = end_date or get_today_str()

    if s > e:
        logger.info(f"moneyflow_hsgt: 数据已最新（{s} > {e}），跳过")
        return 0

    logger.info(f"moneyflow_hsgt: 拉取 {s} → {e}")
    try:
        df = pro.moneyflow_hsgt(start_date=s, end_date=e, fields=HSGT_FIELDS)
    except Exception as exc:
        logger.error(f"moneyflow_hsgt 拉取失败: {exc}")
        return 0

    if df is None or df.empty:
        logger.info("moneyflow_hsgt: 无数据返回")
        return 0

    # hsgt 无 ts_code，用 trade_date 作为 key
    n = upsert_rows(engine, table, "moneyflow_hsgt", df.to_dict("records"),
                    ts_code_fn=lambda _: None)
    logger.info(f"moneyflow_hsgt 完成，共写入 {n} 行")
    return n


# ---------------------------------------------------------------------------
# 3. 行业资金流向（THS 口径，moneyflow_ind_ths）
# ---------------------------------------------------------------------------

def sync_moneyflow_ind_ths(engine: Engine, pro, start_date: Optional[str] = None,
                           end_date: Optional[str] = None) -> int:
    """
    同花顺行业资金流向，按日期逐日拉取。
    注意：此接口需要 5000+ 积分。若权限不足会跳过。
    """
    table = MONEYFLOW_TABLES["moneyflow_ind_ths"]
    s = resolve_start_date(engine, table, start_date, lookback_days=DEFAULT_INCREMENTAL_LOOKBACK_DAYS)
    e = end_date or get_today_str()

    if s > e:
        logger.info(f"moneyflow_ind_ths: 数据已最新，跳过")
        return 0

    logger.info(f"moneyflow_ind_ths: 拉取 {s} → {e}")
    total = 0

    for dt in iter_trade_dates(s, e):
        try:
            df = pro.moneyflow_ind_ths(trade_date=dt)
            if df is not None and not df.empty:
                n = upsert_rows(engine, table, "moneyflow_ind_ths", df.to_dict("records"))
                total += n
                logger.info(f"  moneyflow_ind_ths {dt}: {n} 行")
            else:
                logger.debug(f"  moneyflow_ind_ths {dt}: 无数据")
        except Exception as exc:
            err_str = str(exc)
            if "积分" in err_str or "权限" in err_str or "抱歉" in err_str:
                logger.warning(f"  moneyflow_ind_ths: 权限不足，跳过此接口。{exc}")
                break
            logger.warning(f"  moneyflow_ind_ths {dt} 失败: {exc}")
        time.sleep(DEFAULT_API_SLEEP)

    logger.info(f"moneyflow_ind_ths 完成，共写入 {total} 行")
    return total


# ---------------------------------------------------------------------------
# 4. 板块/行业资金流向（DC 口径，moneyflow_dc_ind）
# ---------------------------------------------------------------------------

def sync_moneyflow_dc_ind(engine: Engine, pro, start_date: Optional[str] = None,
                          end_date: Optional[str] = None) -> int:
    """
    东方财富行业/板块资金流向，按日期逐日拉取。
    接口名兼容：moneyflow_ind_dc / moneyflow_dc_ind / moneyflow_dc_sector
    """
    table = MONEYFLOW_TABLES["moneyflow_dc_ind"]
    s = resolve_start_date(engine, table, start_date, lookback_days=DEFAULT_INCREMENTAL_LOOKBACK_DAYS)
    e = end_date or get_today_str()

    if s > e:
        logger.info(f"moneyflow_dc_ind: 数据已最新，跳过")
        return 0

    logger.info(f"moneyflow_dc_ind: 拉取 {s} → {e}")
    total = 0

    for dt in iter_trade_dates(s, e):
        try:
            # 兼容不同 Tushare 版本/别名：优先 moneyflow_ind_dc
            df = None
            last_exc = None
            for api_name in ("moneyflow_ind_dc", "moneyflow_dc_ind", "moneyflow_dc_sector"):
                fn = getattr(pro, api_name, None)
                if fn is None:
                    continue
                try:
                    df = fn(trade_date=dt)
                    break
                except Exception as api_exc:
                    last_exc = api_exc
                    continue

            if df is None:
                if last_exc is not None:
                    raise last_exc
                logger.debug(f"  moneyflow_dc_ind {dt}: 无可用接口")
                continue

            if df is not None and not df.empty:
                n = upsert_rows(engine, table, "moneyflow_dc_ind", df.to_dict("records"))
                total += n
                logger.info(f"  moneyflow_dc_ind {dt}: {n} 行")
            else:
                logger.debug(f"  moneyflow_dc_ind {dt}: 无数据")
        except Exception as exc:
            err_str = str(exc)
            if "积分" in err_str or "权限" in err_str or "抱歉" in err_str:
                logger.warning(f"  moneyflow_dc_ind: 权限不足，跳过。{exc}")
                break
            logger.warning(f"  moneyflow_dc_ind {dt} 失败: {exc}")
        time.sleep(DEFAULT_API_SLEEP)

    logger.info(f"moneyflow_dc_ind 完成，共写入 {total} 行")
    return total


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def run_sync(
    datasets: list[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    """
    执行资金流向数据同步。

    Args:
        datasets: 要同步的数据集列表，None 表示全部
                  可选值: moneyflow, moneyflow_hsgt, moneyflow_ind_ths, moneyflow_dc_ind
        start_date: 起始日期 YYYYMMDD，None 表示增量（从最新+1天）
        end_date:   结束日期 YYYYMMDD，None 表示今天
    """
    all_datasets = ["moneyflow", "moneyflow_hsgt", "moneyflow_ind_ths", "moneyflow_dc_ind"]
    target_datasets = datasets or all_datasets

    logger.info(f"=== 资金流向数据同步开始 ===")
    logger.info(f"数据集: {target_datasets}")
    logger.info(f"日期范围: {start_date or '增量'} → {end_date or '今天'}")

    engine = get_engine()
    ensure_all_tables(engine)
    pro = _init_tushare()

    results = {}

    if "moneyflow" in target_datasets:
        results["moneyflow"] = sync_moneyflow(engine, pro, start_date, end_date)

    if "moneyflow_hsgt" in target_datasets:
        results["moneyflow_hsgt"] = sync_moneyflow_hsgt(engine, pro, start_date, end_date)

    if "moneyflow_ind_ths" in target_datasets:
        results["moneyflow_ind_ths"] = sync_moneyflow_ind_ths(engine, pro, start_date, end_date)

    if "moneyflow_dc_ind" in target_datasets:
        results["moneyflow_dc_ind"] = sync_moneyflow_dc_ind(engine, pro, start_date, end_date)

    total = sum(results.values())
    logger.info(f"=== 同步完成，共写入 {total} 行 ===")
    for ds, n in results.items():
        logger.info(f"  {ds}: {n} 行")

    return results


# ---------------------------------------------------------------------------
# 查询函数（供 Streamlit 调用）
# ---------------------------------------------------------------------------

def _get_engine_cached() -> Engine:
    """获取一个数据库连接（调用方可选择缓存）"""
    return get_engine()


def query_moneyflow_daily_top(trade_date: str, top_n: int = 20,
                              engine: Optional[Engine] = None) -> pd.DataFrame:
    """
    查询指定日期主力净流入 Top N 个股。

    Returns: DataFrame with columns:
        ts_code, name, trade_date, net_mf_amount(万元),
        buy_elg_amount, sell_elg_amount, buy_lg_amount, sell_lg_amount,
        buy_sm_amount, sell_sm_amount
    """
    if engine is None:
        engine = _get_engine_cached()

    sql = """
    SELECT
        m.ts_code,
        COALESCE(b.name, m.ts_code)            AS name,
        m.trade_date,
        (m.payload->>'net_mf_amount')::numeric   AS net_mf_amount,
        (m.payload->>'buy_elg_amount')::numeric  AS buy_elg_amount,
        (m.payload->>'sell_elg_amount')::numeric AS sell_elg_amount,
        (m.payload->>'buy_lg_amount')::numeric   AS buy_lg_amount,
        (m.payload->>'sell_lg_amount')::numeric  AS sell_lg_amount,
        (m.payload->>'buy_md_amount')::numeric   AS buy_md_amount,
        (m.payload->>'sell_md_amount')::numeric  AS sell_md_amount,
        (m.payload->>'buy_sm_amount')::numeric   AS buy_sm_amount,
        (m.payload->>'sell_sm_amount')::numeric  AS sell_sm_amount
    FROM ts_moneyflow m
    LEFT JOIN vw_ts_stock_basic b ON b.ts_code = m.ts_code
    WHERE m.trade_date = :trade_date
      AND (m.payload->>'net_mf_amount') IS NOT NULL
    ORDER BY (m.payload->>'net_mf_amount')::numeric DESC
    LIMIT :top_n
    """
    dt_str = str(trade_date).replace("-", "")
    if len(dt_str) == 8:
        dt_val = datetime.strptime(dt_str, "%Y%m%d").date()
    else:
        dt_val = trade_date

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params={"trade_date": dt_val, "top_n": top_n})
    return df


def query_moneyflow_stock_history(ts_code: str, start_date: str = None,
                                  end_date: str = None,
                                  engine: Optional[Engine] = None) -> pd.DataFrame:
    """
    查询单只股票的历史资金流向。
    Returns: DataFrame ordered by trade_date
    """
    if engine is None:
        engine = _get_engine_cached()

    conditions = ["ts_code = :ts_code"]
    params: dict = {"ts_code": ts_code}

    if start_date:
        s_str = str(start_date).replace("-", "")
        conditions.append("trade_date >= :start_date")
        params["start_date"] = datetime.strptime(s_str, "%Y%m%d").date()

    if end_date:
        e_str = str(end_date).replace("-", "")
        conditions.append("trade_date <= :end_date")
        params["end_date"] = datetime.strptime(e_str, "%Y%m%d").date()

    where_clause = " AND ".join(conditions)
    sql = f"""
    SELECT
        ts_code,
        trade_date,
        (payload->>'net_mf_amount')::numeric   AS net_mf_amount,
        (payload->>'buy_elg_amount')::numeric  AS buy_elg_amount,
        (payload->>'sell_elg_amount')::numeric AS sell_elg_amount,
        (payload->>'buy_lg_amount')::numeric   AS buy_lg_amount,
        (payload->>'sell_lg_amount')::numeric  AS sell_lg_amount,
        (payload->>'buy_sm_amount')::numeric   AS buy_sm_amount,
        (payload->>'sell_sm_amount')::numeric  AS sell_sm_amount
    FROM ts_moneyflow
    WHERE {where_clause}
    ORDER BY trade_date ASC
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)
    return df


def query_moneyflow_consecutive_inflow(min_days: int = 3,
                                       end_date: str = None,
                                       engine: Optional[Engine] = None) -> pd.DataFrame:
    """
    查询连续 min_days 天主力净流入的个股（选股策略）。
    Returns: DataFrame with ts_code, consecutive_days, last_net_amount
    """
    if engine is None:
        engine = _get_engine_cached()

    e_str = end_date or get_today_str()
    e_val = datetime.strptime(e_str.replace("-", ""), "%Y%m%d").date()

    sql = """
    WITH daily AS (
        SELECT
            ts_code,
            trade_date,
            (payload->>'net_mf_amount')::numeric AS net_amount
        FROM ts_moneyflow
        WHERE trade_date <= :end_date
          AND (payload->>'net_mf_amount') IS NOT NULL
    ),
    flagged AS (
        SELECT
            ts_code, trade_date, net_amount,
            CASE WHEN net_amount > 0 THEN 1 ELSE 0 END AS is_inflow
        FROM daily
    ),
    grouped AS (
        SELECT
            ts_code, trade_date, net_amount, is_inflow,
            SUM(CASE WHEN is_inflow = 0 THEN 1 ELSE 0 END)
                OVER (PARTITION BY ts_code ORDER BY trade_date) AS grp
        FROM flagged
    ),
    streaks AS (
        SELECT
            ts_code,
            MAX(trade_date) AS last_date,
            COUNT(*) AS consecutive_days,
            SUM(net_amount) AS total_net_amount,
            AVG(net_amount) AS avg_net_amount
        FROM grouped
        WHERE is_inflow = 1
        GROUP BY ts_code, grp
    ),
    latest AS (
        SELECT DISTINCT ON (ts_code)
            ts_code, last_date, consecutive_days, total_net_amount, avg_net_amount
        FROM streaks
        ORDER BY ts_code, last_date DESC
    )
    SELECT
        l.ts_code,
        COALESCE(b.name, l.ts_code) AS name,
        l.last_date,
        l.consecutive_days,
        l.total_net_amount,
        l.avg_net_amount
    FROM latest l
    LEFT JOIN vw_ts_stock_basic b ON b.ts_code = l.ts_code
    WHERE l.consecutive_days >= :min_days
      AND l.last_date = :end_date
    ORDER BY l.consecutive_days DESC, l.total_net_amount DESC
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn,
                         params={"end_date": e_val, "min_days": min_days})
    return df


def query_moneyflow_hsgt_history(start_date: str = None, end_date: str = None,
                                 engine: Optional[Engine] = None) -> pd.DataFrame:
    """查询沪深港通（北向/南向）历史资金流向"""
    if engine is None:
        engine = _get_engine_cached()

    conditions = []
    params: dict = {}

    if start_date:
        conditions.append("trade_date >= :start_date")
        params["start_date"] = datetime.strptime(
            str(start_date).replace("-", ""), "%Y%m%d").date()
    if end_date:
        conditions.append("trade_date <= :end_date")
        params["end_date"] = datetime.strptime(
            str(end_date).replace("-", ""), "%Y%m%d").date()

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = f"""
    SELECT
        trade_date,
        (payload->>'north_money')::numeric AS north_money,
        (payload->>'south_money')::numeric AS south_money,
        (payload->>'hgt')::numeric AS hgt,
        (payload->>'sgt')::numeric AS sgt,
        (payload->>'ggt_ss')::numeric AS ggt_ss,
        (payload->>'ggt_sz')::numeric AS ggt_sz
    FROM ts_moneyflow_hsgt
    {where}
    ORDER BY trade_date ASC
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params if params else None)
    return df



def query_moneyflow_ind_ths_range(start_date: str,
                                  end_date: Optional[str] = None,
                                  engine: Optional[Engine] = None) -> pd.DataFrame:
    """查询日期区间行业资金流向（THS口径）"""
    if engine is None:
        engine = _get_engine_cached()

    s_val = datetime.strptime(str(start_date).replace("-", ""), "%Y%m%d").date()
    e_raw = end_date or start_date
    e_val = datetime.strptime(str(e_raw).replace("-", ""), "%Y%m%d").date()
    sql = """
    SELECT
        trade_date,
        NULLIF(payload->>'industry', '') AS sector_name,
        NULLIF(payload->>'lead_stock', '') AS lead_stock,
        (payload->>'pct_change')::numeric AS pct_change,
        (payload->>'net_amount')::numeric AS net_amount
    FROM ts_moneyflow_ind_ths
    WHERE trade_date BETWEEN :start_date AND :end_date
    ORDER BY trade_date ASC, (payload->>'net_amount')::numeric DESC
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params={"start_date": s_val, "end_date": e_val})
    return df

def query_moneyflow_ind_ths_daily(trade_date: str,
                                  engine: Optional[Engine] = None) -> pd.DataFrame:
    """查询某日行业资金流向（THS口径），按净流入额降序"""
    if engine is None:
        engine = _get_engine_cached()

    dt_val = datetime.strptime(str(trade_date).replace("-", ""), "%Y%m%d").date()
    sql = """
    SELECT
        trade_date,
        NULLIF(payload->>'industry', '') AS industry,
        NULLIF(payload->>'lead_stock', '') AS lead_stock,
        (payload->>'pct_change')::numeric AS pct_change,
        (payload->>'net_amount')::numeric AS net_amount,
        (payload->>'net_buy_amount')::numeric AS net_buy_amount,
        (payload->>'net_sell_amount')::numeric AS net_sell_amount
    FROM ts_moneyflow_ind_ths
    WHERE trade_date = :trade_date
    ORDER BY (payload->>'net_amount')::numeric DESC
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params={"trade_date": dt_val})
    return df



def query_moneyflow_dc_ind_range(start_date: str,
                                 end_date: Optional[str] = None,
                                 engine: Optional[Engine] = None) -> pd.DataFrame:
    """查询日期区间板块资金流向（DC口径）"""
    if engine is None:
        engine = _get_engine_cached()

    s_val = datetime.strptime(str(start_date).replace("-", ""), "%Y%m%d").date()
    e_raw = end_date or start_date
    e_val = datetime.strptime(str(e_raw).replace("-", ""), "%Y%m%d").date()
    sql = """
    SELECT
        trade_date,
        NULLIF(payload->>'name', '') AS sector_name,
        (payload->>'pct_change')::numeric AS pct_change,
        (payload->>'net_amount')::numeric AS net_amount,
        (payload->>'net_amount_rate')::numeric AS net_amount_rate
    FROM ts_moneyflow_dc_ind
    WHERE trade_date BETWEEN :start_date AND :end_date
    ORDER BY trade_date ASC, (payload->>'net_amount')::numeric DESC
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params={"start_date": s_val, "end_date": e_val})
    return df

def query_moneyflow_dc_ind_daily(trade_date: str,
                                 engine: Optional[Engine] = None) -> pd.DataFrame:
    """查询某日板块资金流向（DC口径），按净流入额降序"""
    if engine is None:
        engine = _get_engine_cached()

    dt_val = datetime.strptime(str(trade_date).replace("-", ""), "%Y%m%d").date()
    sql = """
    SELECT
        trade_date,
        NULLIF(payload->>'name', '') AS name,
        (payload->>'pct_change')::numeric AS pct_change,
        (payload->>'net_amount')::numeric AS net_amount,
        (payload->>'net_amount_rate')::numeric AS net_amount_rate,
        (payload->>'buy_elg_amount')::numeric AS buy_elg_amount,
        (payload->>'buy_lg_amount')::numeric AS buy_lg_amount,
        (payload->>'buy_md_amount')::numeric AS buy_md_amount,
        (payload->>'buy_sm_amount')::numeric AS buy_sm_amount
    FROM ts_moneyflow_dc_ind
    WHERE trade_date = :trade_date
    ORDER BY (payload->>'net_amount')::numeric DESC
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params={"trade_date": dt_val})
    return df



def get_moneyflow_sector_min_date(engine: Optional[Engine] = None) -> Optional[str]:
    """获取行业/板块资金流向数据的最早交易日期（THS/DC并集）"""
    if engine is None:
        engine = _get_engine_cached()
    sql = """
    SELECT TO_CHAR(MIN(dt), 'YYYYMMDD') AS min_date
    FROM (
        SELECT MIN(trade_date) AS dt FROM ts_moneyflow_ind_ths
        UNION ALL
        SELECT MIN(trade_date) AS dt FROM ts_moneyflow_dc_ind
    ) t
    """
    with engine.connect() as conn:
        row = conn.execute(text(sql)).fetchone()
    return row[0] if row and row[0] else None

def get_moneyflow_latest_date(engine: Optional[Engine] = None) -> Optional[str]:
    """获取个股资金流向数据的最新交易日期"""
    if engine is None:
        engine = _get_engine_cached()
    return get_max_trade_date(engine, MONEYFLOW_TABLES["moneyflow"])


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="??????????")
    parser.add_argument("--start", type=str, default=None,
                        help="???? YYYYMMDD??????")
    parser.add_argument("--end", type=str, default=None,
                        help="???? YYYYMMDD??????")
    parser.add_argument("--full", action="store_true",
                        help="??? DEFAULT_START_DATE ????")
    parser.add_argument("--datasets", type=str, default=None,
                        help="?????????? moneyflow,moneyflow_hsgt")
    parser.add_argument("--init-tables", action="store_true",
                        help="????????????????")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_INCREMENTAL_LOOKBACK_DAYS,
                        help="???????????????")
    parser.add_argument("--purge-before-start", action="store_true",
                        help="?? start ????????????? 2026 ????")
    args = parser.parse_args()

    if args.init_tables:
        eng = get_engine()
        ensure_all_tables(eng)
        print("????????????")
        sys.exit(0)

    if args.lookback_days < 0:
        raise ValueError("--lookback-days ???? 0")
    os.environ["TUSHARE_MF_LOOKBACK_DAYS"] = str(args.lookback_days)

    target_ds = [d.strip() for d in args.datasets.split(",")] if args.datasets else None
    start = DEFAULT_START_DATE if args.full else args.start

    if args.purge_before_start and start:
        eng = get_engine()
        with eng.begin() as conn:
            for table_name in MONEYFLOW_TABLES.values():
                conn.execute(text(f"DELETE FROM {table_name} WHERE trade_date < :cutoff"), {"cutoff": datetime.strptime(start, "%Y%m%d").date()})
        print(f"??? {start} ?????????")

    run_sync(datasets=target_ds, start_date=start, end_date=args.end)
