# -*- coding: utf-8 -*-
"""
ETF份额规模数据抓取脚本
数据来源: Tushare etf_share_size 接口 (doc_id=408)
功能:
  - 从 etf_summary 表中读取所有基金代码
  - 拉取 2023-01-01 至今的 ETF 份额规模数据
  - 支持每日增量更新（查询 DB 最新日期，从下一天开始）
  - 数据写入 PostgreSQL 表 etf_share_size

表结构 (etf_share_size):
  trade_date   DATE          - 交易日期
  ts_code      VARCHAR(20)   - ETF代码
  etf_name     VARCHAR(255)  - 基金名称
  total_share  NUMERIC(20,4) - 总份额（万份）
  total_size   NUMERIC(20,4) - 总规模（万元）
  nav          NUMERIC(12,4) - 基金份额净值（元）
  close        NUMERIC(12,4) - 收盘价（元）
  exchange     VARCHAR(20)   - 交易所（SSE/SZSE/BSE）
  PRIMARY KEY (trade_date, ts_code)
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta, date

import pandas as pd
from sqlalchemy import create_engine, text

# ── 路径设置（兼容直接运行和作为模块导入）──────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.volume_fetcher import _init_tushare

# ── 配置 ────────────────────────────────────────────────────────────────────
DB_URL          = 'postgresql://postgres:Zmx1018$@67.216.207.73:5432/postgres'
SUMMARY_TABLE   = 'etf_summary'
TARGET_TABLE    = 'etf_share_size'
DEFAULT_START   = '20230101'       # 历史全量起始日
BATCH_DAYS      = 7                # 每次按多少天为一批拉取（避免超5000条）
API_SLEEP       = 0.4              # 每次API调用后等待秒数（防限频）

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ── 工具函数 ─────────────────────────────────────────────────────────────────

def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)


def ensure_table(engine):
    """创建目标表（如果不存在）"""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        trade_date   DATE           NOT NULL,
        ts_code      VARCHAR(20)    NOT NULL,
        etf_name     VARCHAR(255),
        total_share  NUMERIC(20, 4),
        total_size   NUMERIC(20, 4),
        nav          NUMERIC(12, 4),
        close        NUMERIC(12, 4),
        exchange     VARCHAR(20),
        PRIMARY KEY (trade_date, ts_code)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logger.info(f"表 {TARGET_TABLE} 已就绪")


def get_latest_date(engine) -> str | None:
    """查询 DB 中已有数据的最新 trade_date，返回 'YYYYMMDD' 字符串，无数据则返回 None"""
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT MAX(trade_date) FROM {TARGET_TABLE}")).fetchone()
    if row and row[0]:
        return row[0].strftime('%Y%m%d')
    return None


def get_all_ts_codes(engine) -> list[str]:
    """从 etf_summary 表读取所有基金代码"""
    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT fund_trade_code FROM {SUMMARY_TABLE}")).fetchall()
    codes = [r[0] for r in rows if r[0]]
    logger.info(f"从 {SUMMARY_TABLE} 读取到 {len(codes)} 只 ETF 代码")
    return codes


def daterange(start: str, end: str, days: int):
    """生成 (start_str, end_str) 日期批次，格式 YYYYMMDD"""
    s = datetime.strptime(start, '%Y%m%d')
    e = datetime.strptime(end, '%Y%m%d')
    while s <= e:
        batch_end = min(s + timedelta(days=days - 1), e)
        yield s.strftime('%Y%m%d'), batch_end.strftime('%Y%m%d')
        s = batch_end + timedelta(days=1)


def fetch_batch(pro, start_date: str, end_date: str) -> pd.DataFrame:
    """
    按日期批次拉取全市场 ETF 份额规模（不传 ts_code，按日期区间拉取全部）
    Tushare etf_share_size 单次 5000 条上限，7天≈1629只，安全。
    """
    frames = []
    try:
        df = pro.etf_share_size(
            start_date=start_date,
            end_date=end_date,
            fields='trade_date,ts_code,etf_name,total_share,total_size,nav,close,exchange'
        )
        if df is not None and not df.empty:
            frames.append(df)
        time.sleep(API_SLEEP)
    except Exception as exc:
        logger.warning(f"  批次 {start_date}~{end_date} 拉取失败: {exc}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def upsert_df(engine, df: pd.DataFrame):
    """
    将 DataFrame 写入 PostgreSQL，使用 INSERT ... ON CONFLICT DO NOTHING
    保证主键冲突时不覆盖（幂等）
    """
    if df.empty:
        return 0

    # 数据类型转换
    df = df.copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d', errors='coerce').dt.date

    for col in ['total_share', 'total_size', 'nav', 'close']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=['trade_date', 'ts_code'])

    # 用临时表批量 upsert
    tmp_table = f"_tmp_{TARGET_TABLE}"
    with engine.begin() as conn:
        # 写入临时表
        df.to_sql(tmp_table, conn, if_exists='replace', index=False, method='multi', chunksize=2000)

        # INSERT ... ON CONFLICT DO NOTHING
        conn.execute(text(f"""
            INSERT INTO {TARGET_TABLE} (trade_date, ts_code, etf_name, total_share, total_size, nav, close, exchange)
            SELECT trade_date, ts_code, etf_name, total_share, total_size, nav, close, exchange
            FROM {tmp_table}
            ON CONFLICT (trade_date, ts_code) DO NOTHING;
        """))

        conn.execute(text(f"DROP TABLE IF EXISTS {tmp_table}"))

    return len(df)


# ── 主流程 ───────────────────────────────────────────────────────────────────

def run(full: bool = False):
    """
    主执行函数
    :param full: True=全量重拉（从 DEFAULT_START 开始）；False=增量（从 DB 最新日期+1 天开始）
    """
    engine = get_engine()
    ensure_table(engine)

    pro = _init_tushare()

    today = datetime.now().strftime('%Y%m%d')

    if full:
        start_date = DEFAULT_START
        logger.info(f"全量模式：从 {start_date} 到 {today}")
    else:
        latest = get_latest_date(engine)
        if latest:
            next_day = (datetime.strptime(latest, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
            if next_day > today:
                logger.info(f"数据已是最新（最新日期: {latest}），无需更新")
                return
            start_date = next_day
            logger.info(f"增量模式：从 {start_date} 到 {today}（DB 最新: {latest}）")
        else:
            start_date = DEFAULT_START
            logger.info(f"DB 无数据，全量拉取：从 {start_date} 到 {today}")

    total_inserted = 0
    batches = list(daterange(start_date, today, BATCH_DAYS))
    logger.info(f"共 {len(batches)} 个批次，每批 {BATCH_DAYS} 天")

    for i, (bs, be) in enumerate(batches, 1):
        logger.info(f"[{i}/{len(batches)}] 拉取 {bs} ~ {be} ...")
        df = fetch_batch(pro, bs, be)
        if df.empty:
            logger.info(f"  → 无数据")
            continue
        inserted = upsert_df(engine, df)
        total_inserted += inserted
        logger.info(f"  → 写入 {inserted} 条（原始 {len(df)} 条）")

    logger.info(f"完成！本次共写入 {total_inserted} 条数据")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='ETF份额规模数据抓取')
    parser.add_argument('--full', action='store_true', help='全量重拉（从2023-01-01开始）')
    args = parser.parse_args()
    run(full=args.full)
