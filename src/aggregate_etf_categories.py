# -*- coding: utf-8 -*-
from __future__ import annotations
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
from datetime import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TARGET_TABLE = 'etf_category_daily_agg'
WIDE_INDEX_TABLE = 'etf_wide_index_daily_agg'
WIDE_INDEX_NAME_MAP = {
    '000300.SH': '沪深300指数ETF',
    '000905.SH': '中证500指数ETF',
    '000906.SH': '中证800指数ETF',
    '000852.SH': '中证1000指数ETF',
    '932000.CSI': '中证2000指数ETF',
    '000001.SH': '上证综合指数ETF',
    '000016.SH': '上证50指数ETF',
    '399001.SZ': '深证成份指数ETF',
    '399005.SZ': '中小100指数ETF',
    '399006.SZ': '创业板指数ETF',
    '399673.SZ': '创业板50指数ETF',
    '000680.SH': '科创板综合指数ETF',
    '000688.SH': '科创板50成份指数ETF'
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def get_engine():
    direct_url = os.getenv('ETF_PG_URL') or os.getenv('DATABASE_URL')
    if direct_url:
        return create_engine(direct_url, pool_pre_ping=True)

    password = os.getenv('ETF_PG_PASSWORD') or os.getenv('PGPASSWORD')
    if not password:
        raise RuntimeError('未配置数据库密码，请设置 ETF_PG_PASSWORD 或 PGPASSWORD')

    db_url = URL.create(
        'postgresql+psycopg2',
        username=os.getenv('ETF_PG_USER', 'postgres'),
        password=password,
        host=os.getenv('ETF_PG_HOST', '67.216.207.73'),
        port=int(os.getenv('ETF_PG_PORT', '5432')),
        database=os.getenv('ETF_PG_DATABASE', 'postgres'),
        query={'sslmode': os.getenv('ETF_PG_SSLMODE', 'require')}
    )
    return create_engine(db_url, pool_pre_ping=True)


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
        share_change        NUMERIC(20,4),
        share_change_pct    NUMERIC(20,6),
        size_change         NUMERIC(20,4),
        size_change_pct     NUMERIC(20,6),
        PRIMARY KEY (trade_date, category_key)
    );
    ALTER TABLE {TARGET_TABLE}
        ADD COLUMN IF NOT EXISTS share_change NUMERIC(20,4),
        ADD COLUMN IF NOT EXISTS share_change_pct NUMERIC(20,6),
        ADD COLUMN IF NOT EXISTS size_change NUMERIC(20,4),
        ADD COLUMN IF NOT EXISTS size_change_pct NUMERIC(20,6);
    CREATE INDEX IF NOT EXISTS idx_agg_category ON {TARGET_TABLE} (category_key, trade_date);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logger.info(f"表 {TARGET_TABLE} 已就绪")


def ensure_wide_index_table(engine):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {WIDE_INDEX_TABLE} (
        trade_date             DATE           NOT NULL,
        benchmark_index_code   VARCHAR(20)    NOT NULL,
        benchmark_index_name   VARCHAR(100)   NOT NULL,
        etf_count              INT,
        total_share            NUMERIC(20,4),
        total_size             NUMERIC(20,4),
        share_change           NUMERIC(20,4),
        share_change_pct       NUMERIC(20,6),
        size_change            NUMERIC(20,4),
        size_change_pct        NUMERIC(20,6),
        PRIMARY KEY (trade_date, benchmark_index_code)
    );
    CREATE INDEX IF NOT EXISTS idx_wide_index_code_date
        ON {WIDE_INDEX_TABLE} (benchmark_index_code, trade_date);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logger.info(f"表 {WIDE_INDEX_TABLE} 已就绪")


def aggregate_dates(engine, start_date: str = None, end_date: str = None):
    """
    对指定日期范围执行聚合，写入 etf_category_daily_agg。
    使用 INSERT ... ON CONFLICT DO UPDATE 保证幂等。
    """
    history_filter = ""
    output_filter = ""
    delete_filter = ""
    params = {}
    if start_date:
        output_filter += " AND trade_date >= :start_date"
        delete_filter += " AND trade_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        history_filter += " AND s.trade_date <= :end_date"
        output_filter += " AND trade_date <= :end_date"
        delete_filter += " AND trade_date <= :end_date"
        params['end_date'] = end_date

    delete_sql = text(f"DELETE FROM {TARGET_TABLE} WHERE 1=1 {delete_filter}")
    insert_sql = text(f"""
    INSERT INTO {TARGET_TABLE}
        (trade_date, category_key, primary_category, secondary_category, level,
         etf_count, total_share, total_size,
         share_change, share_change_pct, size_change, size_change_pct)
    WITH etf_base AS (
        SELECT
            s.trade_date,
            s.ts_code,
            e.primary_category,
            e.secondary_category,
            s.total_share,
            s.total_size,
            s.close,
            LAG(s.total_share) OVER (
                PARTITION BY s.ts_code
                ORDER BY s.trade_date
            ) AS prev_etf_total_share
        FROM etf_share_size s
        JOIN etf_summary e
          ON s.ts_code = e.fund_trade_code
        WHERE 1=1
          {history_filter}
    ),
    category_daily AS (
        SELECT
            trade_date,
            primary_category || '-' || secondary_category AS category_key,
            primary_category,
            secondary_category,
            1 AS level,
            COUNT(DISTINCT ts_code) AS etf_count,
            SUM(total_share) AS total_share,
            SUM(total_size) AS total_size,
            SUM(
                close * (
                    total_share - COALESCE(prev_etf_total_share, 0)
                )
            ) AS size_change_by_share
        FROM etf_base
        WHERE secondary_category IS NOT NULL
        GROUP BY trade_date, primary_category, secondary_category

        UNION ALL

        SELECT
            trade_date,
            primary_category AS category_key,
            primary_category,
            NULL AS secondary_category,
            2 AS level,
            COUNT(DISTINCT ts_code) AS etf_count,
            SUM(total_share) AS total_share,
            SUM(total_size) AS total_size,
            SUM(
                close * (
                    total_share - COALESCE(prev_etf_total_share, 0)
                )
            ) AS size_change_by_share
        FROM etf_base
        GROUP BY trade_date, primary_category

        UNION ALL

        SELECT
            trade_date,
            '全部' AS category_key,
            '全部' AS primary_category,
            NULL AS secondary_category,
            9 AS level,
            COUNT(DISTINCT ts_code) AS etf_count,
            SUM(total_share) AS total_share,
            SUM(total_size) AS total_size,
            SUM(
                close * (
                    total_share - COALESCE(prev_etf_total_share, 0)
                )
            ) AS size_change_by_share
        FROM etf_base
        GROUP BY trade_date
    ),
    lag_base AS (
        SELECT
            trade_date,
            category_key,
            primary_category,
            secondary_category,
            level,
            etf_count,
            total_share,
            total_size,
            size_change_by_share,
            LAG(total_share) OVER (
                PARTITION BY category_key
                ORDER BY trade_date
            ) AS prev_total_share,
            LAG(total_size) OVER (
                PARTITION BY category_key
                ORDER BY trade_date
            ) AS prev_total_size
        FROM category_daily
    ),
    calc AS (
        SELECT
            trade_date,
            category_key,
            primary_category,
            secondary_category,
            level,
            etf_count,
            total_share,
            total_size,
            total_share - prev_total_share AS share_change,
            CASE
                WHEN prev_total_share IS NULL OR prev_total_share = 0 THEN NULL
                ELSE (total_share - prev_total_share) / prev_total_share
            END AS share_change_pct,
            CASE
                WHEN prev_total_size IS NULL THEN NULL
                ELSE size_change_by_share
            END AS size_change,
            CASE
                WHEN prev_total_size IS NULL OR prev_total_size = 0 THEN NULL
                ELSE size_change_by_share / prev_total_size
            END AS size_change_pct
        FROM lag_base
    )
    SELECT
        trade_date,
        category_key,
        primary_category,
        secondary_category,
        level,
        etf_count,
        total_share,
        total_size,
        share_change,
        share_change_pct,
        size_change,
        size_change_pct
    FROM calc
    WHERE 1=1
      {output_filter}
    ON CONFLICT (trade_date, category_key) DO UPDATE SET
        primary_category    = EXCLUDED.primary_category,
        secondary_category  = EXCLUDED.secondary_category,
        level               = EXCLUDED.level,
        etf_count           = EXCLUDED.etf_count,
        total_share         = EXCLUDED.total_share,
        total_size          = EXCLUDED.total_size,
        share_change        = EXCLUDED.share_change,
        share_change_pct    = EXCLUDED.share_change_pct,
        size_change         = EXCLUDED.size_change,
        size_change_pct     = EXCLUDED.size_change_pct
    ;
    """)

    with engine.begin() as conn:
        conn.execute(delete_sql, params)
        result = conn.execute(insert_sql, params)
        logger.info(f"聚合完成，影响 {result.rowcount} 行")
    return result.rowcount


def aggregate_wide_index_dates(engine, start_date: str = None, end_date: str = None):
    benchmark_rows = ",\n            ".join(
        f"('{code}', '{name}')"
        for code, name in WIDE_INDEX_NAME_MAP.items()
    )

    history_filter = ""
    output_filter = ""
    delete_filter = ""
    params = {}

    if end_date:
        history_filter += " AND s.trade_date <= :end_date"
        output_filter += " AND trade_date <= :end_date"
        delete_filter += " AND trade_date <= :end_date"
        params['end_date'] = end_date
    if start_date:
        output_filter += " AND trade_date >= :start_date"
        delete_filter += " AND trade_date >= :start_date"
        params['start_date'] = start_date

    delete_sql = text(f"DELETE FROM {WIDE_INDEX_TABLE} WHERE 1=1 {delete_filter}")
    insert_sql = text(f"""
        INSERT INTO {WIDE_INDEX_TABLE} (
            trade_date,
            benchmark_index_code,
            benchmark_index_name,
            etf_count,
            total_share,
            total_size,
            share_change,
            share_change_pct,
            size_change,
            size_change_pct
        )
        WITH benchmark_map(benchmark_index_code, benchmark_index_name) AS (
            VALUES
            {benchmark_rows}
        ),
        etf_base AS (
            SELECT
                s.trade_date,
                s.ts_code,
                e.benchmark_index_code,
                bm.benchmark_index_name,
                s.total_share,
                s.total_size,
                s.close,
                LAG(s.total_share) OVER (
                    PARTITION BY s.ts_code
                    ORDER BY s.trade_date
                ) AS prev_etf_total_share
            FROM etf_share_size s
            JOIN etf_summary e
              ON s.ts_code = e.fund_trade_code
            JOIN benchmark_map bm
              ON e.benchmark_index_code = bm.benchmark_index_code
            WHERE e.secondary_category = '宽基'
              {history_filter}
        ),
        daily_base AS (
            SELECT
                trade_date,
                benchmark_index_code,
                benchmark_index_name,
                COUNT(DISTINCT ts_code) AS etf_count,
                SUM(total_share) AS total_share,
                SUM(total_size) AS total_size,
                SUM(
                    close * (
                        total_share - COALESCE(prev_etf_total_share, 0)
                    )
                ) AS size_change_by_share
            FROM etf_base
            GROUP BY
                trade_date,
                benchmark_index_code,
                benchmark_index_name
        ),
        lag_base AS (
            SELECT
                trade_date,
                benchmark_index_code,
                benchmark_index_name,
                etf_count,
                total_share,
                total_size,
                size_change_by_share,
                LAG(total_share) OVER (
                    PARTITION BY benchmark_index_code
                    ORDER BY trade_date
                ) AS prev_total_share,
                LAG(total_size) OVER (
                    PARTITION BY benchmark_index_code
                    ORDER BY trade_date
                ) AS prev_total_size
            FROM daily_base
        ),
        calc AS (
            SELECT
                trade_date,
                benchmark_index_code,
                benchmark_index_name,
                etf_count,
                total_share,
                total_size,
                total_share - prev_total_share AS share_change,
                CASE
                    WHEN prev_total_share IS NULL OR prev_total_share = 0 THEN NULL
                    ELSE (total_share - prev_total_share) / prev_total_share
                END AS share_change_pct,
                CASE
                    WHEN prev_total_size IS NULL THEN NULL
                    ELSE size_change_by_share
                END AS size_change,
                CASE
                    WHEN prev_total_size IS NULL OR prev_total_size = 0 THEN NULL
                    ELSE size_change_by_share / prev_total_size
                END AS size_change_pct
            FROM lag_base
        )
        SELECT
            trade_date,
            benchmark_index_code,
            benchmark_index_name,
            etf_count,
            total_share,
            total_size,
            share_change,
            share_change_pct,
            size_change,
            size_change_pct
        FROM calc
        WHERE 1=1
          {output_filter}
        ORDER BY trade_date, benchmark_index_code
        ON CONFLICT (trade_date, benchmark_index_code) DO UPDATE SET
            benchmark_index_name = EXCLUDED.benchmark_index_name,
            etf_count = EXCLUDED.etf_count,
            total_share = EXCLUDED.total_share,
            total_size = EXCLUDED.total_size,
            share_change = EXCLUDED.share_change,
            share_change_pct = EXCLUDED.share_change_pct,
            size_change = EXCLUDED.size_change,
            size_change_pct = EXCLUDED.size_change_pct
    """)

    with engine.begin() as conn:
        conn.execute(delete_sql, params)
        result = conn.execute(insert_sql, params)
        logger.info(f"宽基指数聚合完成，影响 {result.rowcount} 行")
    return result.rowcount


def get_latest_agg_date(engine) -> str | None:
    """查询聚合表中最大日期"""
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT MAX(trade_date) FROM {TARGET_TABLE}")).fetchone()
    if row and row[0]:
        return row[0].strftime('%Y-%m-%d')
    return None


def get_latest_wide_index_agg_date(engine) -> str | None:
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT MAX(trade_date) FROM {WIDE_INDEX_TABLE}")).fetchone()
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


def normalize_date(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.replace('-', '').strip()
    parsed = datetime.strptime(cleaned, '%Y%m%d')
    return parsed.strftime('%Y-%m-%d')


def run(full: bool = False, start_date: str | None = None, end_date: str | None = None):
    """
    主流程
    :param full: True=全量回填, False=增量（仅聚合新日期）
    """
    engine = get_engine()
    ensure_table(engine)
    ensure_wide_index_table(engine)
    start_date = normalize_date(start_date)
    end_date = normalize_date(end_date)

    if start_date or end_date:
        start_date = start_date or end_date
        end_date = end_date or start_date
        if start_date > end_date:
            raise ValueError(f'开始日期不能晚于结束日期: {start_date} > {end_date}')
        logger.info(f"指定区间模式：聚合 {start_date} ~ {end_date}")
        aggregate_dates(engine, start_date=start_date, end_date=end_date)
        aggregate_wide_index_dates(engine, start_date=start_date, end_date=end_date)
    elif full:
        logger.info("全量回填模式")
        aggregate_dates(engine)
        aggregate_wide_index_dates(engine)
    else:
        latest_agg = get_latest_agg_date(engine)
        latest_wide_index_agg = get_latest_wide_index_agg_date(engine)
        latest_share = get_latest_share_date(engine)
        if not latest_share:
            logger.info("etf_share_size 无数据，跳过")
            return
        if (
            latest_agg and latest_agg >= latest_share
            and latest_wide_index_agg and latest_wide_index_agg >= latest_share
        ):
            logger.info(
                f"聚合已是最新 (agg={latest_agg}, wide={latest_wide_index_agg}, share={latest_share})，跳过"
            )
            return
        if not latest_agg or not latest_wide_index_agg:
            start = '2023-01-01'
        else:
            start = min(latest_agg, latest_wide_index_agg)
        logger.info(f"增量模式：聚合 {start} ~ {latest_share}")
        aggregate_dates(engine, start_date=start, end_date=latest_share)
        aggregate_wide_index_dates(engine, start_date=start, end_date=latest_share)

    # 验证
    with engine.connect() as conn:
        row = conn.execute(text(f"""
            SELECT COUNT(*) as cnt, MIN(trade_date) as min_d, MAX(trade_date) as max_d
            FROM {TARGET_TABLE}
        """)).fetchone()
        wide_row = conn.execute(text(f"""
            SELECT COUNT(*) as cnt, MIN(trade_date) as min_d, MAX(trade_date) as max_d
            FROM {WIDE_INDEX_TABLE}
        """)).fetchone()
    logger.info(f"分类聚合表现状: {row[0]} 行, {row[1]} ~ {row[2]}")
    logger.info(f"宽基指数聚合表现状: {wide_row[0]} 行, {wide_row[1]} ~ {wide_row[2]}")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='ETF 分类聚合')
    parser.add_argument('--full', action='store_true', help='全量回填')
    parser.add_argument('--start-date', help='指定开始日期，格式 YYYYMMDD 或 YYYY-MM-DD')
    parser.add_argument('--end-date', help='指定结束日期，格式 YYYYMMDD 或 YYYY-MM-DD')
    args = parser.parse_args()
    run(full=args.full, start_date=args.start_date, end_date=args.end_date)
