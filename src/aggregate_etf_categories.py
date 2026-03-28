# -*- coding: utf-8 -*-
"""
ETF 分类聚合脚本
将 etf_share_size（每日明细） JOIN etf_summary（分类信息）
预聚合为 etf_category_daily_agg 表，供前端快速查询时序趋势。

聚合层级：
  level=1  二级分类明细（如 指数-宽基、债券-国债）
  level=2  一级分类小计（如 指数、债券、QDII）
  level=9  全部合计

用法：
  python src/aggregate_etf_categories.py          # 增量（仅聚合未处理的日期）
  python src/aggregate_etf_categories.py --full   # 全量回填
"""

import os
import sys
import logging
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_URL       = 'postgresql://postgres:Zmx1018$@67.216.207.73:5432/postgres'
TARGET_TABLE = 'etf_category_daily_agg'

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)


def ensure_table(engine):
    """建表（如不存在）"""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        trade_date          DATE          NOT NULL,
        category_key        VARCHAR(50)   NOT NULL,
        primary_category    VARCHAR(50)   NOT NULL,
        secondary_category  VARCHAR(50),
        level               SMALLINT      NOT NULL,
        etf_count           INT,
        total_share         NUMERIC(20,4),
        total_size          NUMERIC(20,4),
        PRIMARY KEY (trade_date, category_key)
    );
    CREATE INDEX IF NOT EXISTS idx_agg_category ON {TARGET_TABLE} (category_key, trade_date);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logger.info(f"表 {TARGET_TABLE} 已就绪")


def aggregate_dates(engine, start_date: str = None, end_date: str = None):
    """
    对指定日期范围执行聚合，写入 etf_category_daily_agg。
    使用 INSERT ... ON CONFLICT DO UPDATE 保证幂等。
    """
    date_filter = ""
    params = {}
    if start_date:
        date_filter += " AND s.trade_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        date_filter += " AND s.trade_date <= :end_date"
        params['end_date'] = end_date

    sql = f"""
    INSERT INTO {TARGET_TABLE}
        (trade_date, category_key, primary_category, secondary_category, level,
         etf_count, total_share, total_size)

    -- Level 1: 二级分类明细（仅 secondary_category 非空的）
    SELECT
        s.trade_date,
        e.primary_category || '-' || e.secondary_category  AS category_key,
        e.primary_category,
        e.secondary_category,
        1                               AS level,
        COUNT(DISTINCT s.ts_code)       AS etf_count,
        SUM(s.total_share)              AS total_share,
        SUM(s.total_size)               AS total_size
    FROM etf_share_size s
    JOIN etf_summary e ON s.ts_code = e.fund_trade_code
    WHERE e.secondary_category IS NOT NULL
      {date_filter}
    GROUP BY s.trade_date, e.primary_category, e.secondary_category

    UNION ALL

    -- Level 2: 一级分类小计
    SELECT
        s.trade_date,
        e.primary_category              AS category_key,
        e.primary_category,
        NULL                            AS secondary_category,
        2                               AS level,
        COUNT(DISTINCT s.ts_code)       AS etf_count,
        SUM(s.total_share)              AS total_share,
        SUM(s.total_size)               AS total_size
    FROM etf_share_size s
    JOIN etf_summary e ON s.ts_code = e.fund_trade_code
    WHERE 1=1
      {date_filter}
    GROUP BY s.trade_date, e.primary_category

    UNION ALL

    -- Level 9: 全部合计
    SELECT
        s.trade_date,
        '全部'                           AS category_key,
        '全部'                           AS primary_category,
        NULL                            AS secondary_category,
        9                               AS level,
        COUNT(DISTINCT s.ts_code)       AS etf_count,
        SUM(s.total_share)              AS total_share,
        SUM(s.total_size)               AS total_size
    FROM etf_share_size s
    JOIN etf_summary e ON s.ts_code = e.fund_trade_code
    WHERE 1=1
      {date_filter}
    GROUP BY s.trade_date

    ON CONFLICT (trade_date, category_key) DO UPDATE SET
        primary_category    = EXCLUDED.primary_category,
        secondary_category  = EXCLUDED.secondary_category,
        level               = EXCLUDED.level,
        etf_count           = EXCLUDED.etf_count,
        total_share         = EXCLUDED.total_share,
        total_size          = EXCLUDED.total_size
    ;
    """

    with engine.begin() as conn:
        result = conn.execute(text(sql), params)
        logger.info(f"聚合完成，影响 {result.rowcount} 行")
    return result.rowcount


def get_latest_agg_date(engine) -> str | None:
    """查询聚合表中最大日期"""
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT MAX(trade_date) FROM {TARGET_TABLE}")).fetchone()
    if row and row[0]:
        return row[0].strftime('%Y-%m-%d')
    return None


def get_latest_share_date(engine) -> str | None:
    """查询明细表中最大日期"""
    with engine.connect() as conn:
        row = conn.execute(text("SELECT MAX(trade_date) FROM etf_share_size")).fetchone()
    if row and row[0]:
        return row[0].strftime('%Y-%m-%d')
    return None


def run(full: bool = False):
    """
    主流程
    :param full: True=全量回填, False=增量（仅聚合新日期）
    """
    engine = get_engine()
    ensure_table(engine)

    if full:
        logger.info("全量回填模式")
        aggregate_dates(engine)
    else:
        latest_agg = get_latest_agg_date(engine)
        latest_share = get_latest_share_date(engine)
        if not latest_share:
            logger.info("etf_share_size 无数据，跳过")
            return
        if latest_agg and latest_agg >= latest_share:
            logger.info(f"聚合已是最新 (agg={latest_agg}, share={latest_share})，跳过")
            return
        start = latest_agg if latest_agg else '2023-01-01'
        logger.info(f"增量模式：聚合 {start} ~ {latest_share}")
        aggregate_dates(engine, start_date=start, end_date=latest_share)

    # 验证
    with engine.connect() as conn:
        row = conn.execute(text(f"""
            SELECT COUNT(*) as cnt, MIN(trade_date) as min_d, MAX(trade_date) as max_d
            FROM {TARGET_TABLE}
        """)).fetchone()
    logger.info(f"聚合表现状: {row[0]} 行, {row[1]} ~ {row[2]}")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='ETF 分类聚合')
    parser.add_argument('--full', action='store_true', help='全量回填')
    args = parser.parse_args()
    run(full=args.full)
