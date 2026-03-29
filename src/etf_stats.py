# -*- coding: utf-8 -*-
from __future__ import annotations
"""
ETF分类统计查询模块
提供按一级分类汇总指定日期 ETF 份额/规模的工具函数
数据来源: 视图 v_etf_category_daily (etf_share_size JOIN etf_summary)
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

DEFAULT_DB_HOST = '67.216.207.73'
DEFAULT_DB_PORT = 5432
DEFAULT_DB_NAME = 'postgres'
DEFAULT_DB_USER = 'postgres'
DEFAULT_DB_SSLMODE = 'require'


def _build_db_url():
    direct_url = os.getenv('ETF_PG_URL') or os.getenv('DATABASE_URL')
    if direct_url:
        return direct_url

    password = os.getenv('ETF_PG_PASSWORD') or os.getenv('PGPASSWORD')
    if not password:
        raise RuntimeError('未配置数据库密码，请设置 ETF_PG_PASSWORD 或 PGPASSWORD')

    return URL.create(
        'postgresql+psycopg2',
        username=os.getenv('ETF_PG_USER', DEFAULT_DB_USER),
        password=password,
        host=os.getenv('ETF_PG_HOST', DEFAULT_DB_HOST),
        port=int(os.getenv('ETF_PG_PORT', str(DEFAULT_DB_PORT))),
        database=os.getenv('ETF_PG_DATABASE', DEFAULT_DB_NAME),
        query={'sslmode': os.getenv('ETF_PG_SSLMODE', DEFAULT_DB_SSLMODE)}
    )


def _get_engine():
    return create_engine(_build_db_url(), pool_pre_ping=True)


def get_category_daily_summary(
    trade_date: str,
    engine=None
) -> pd.DataFrame:
    """
    查询指定日期各一级分类 ETF 的份额/规模汇总（含全部合计行）。

    Args:
        trade_date: 交易日期，格式 'YYYY-MM-DD' 或 'YYYYMMDD'
        engine:     SQLAlchemy engine，不传则使用默认 DB_URL

    Returns:
        DataFrame，列说明：
          category        - 一级分类（最后一行为"全部"合计）
          etf_count       - ETF只数
          total_share_yi  - 总份额（亿份）
          total_size_yi   - 总规模（亿元）

    Example:
        df = get_category_daily_summary('2026-03-26')
        print(df)
    """
    # 日期格式归一化
    if len(trade_date) == 8 and '-' not in trade_date:
        trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"

    sql = """
        SELECT
            COALESCE(primary_category, '全部')  AS category,
            COUNT(DISTINCT ts_code)              AS etf_count,
            ROUND(SUM(total_share) / 10000, 2)  AS total_share_yi,
            ROUND(SUM(total_size)  / 10000, 2)  AS total_size_yi
        FROM v_etf_category_daily
        WHERE trade_date = :trade_date
        GROUP BY ROLLUP(primary_category)
        ORDER BY
            GROUPING(primary_category),
            primary_category
    """

    if engine is None:
        engine = _get_engine()

    df = pd.read_sql(text(sql), engine, params={"trade_date": trade_date})
    return df


def get_full_daily_summary(
    trade_date: str,
    engine=None
) -> pd.DataFrame:
    """
    查询指定日期的完整分类汇总，包括：
      - 各一级分类小计（货币/债券/商品/QDII）
      - 指数类按二级分类展开（宽基/行业&其他/港股/增强 + 指数小计）
      - 全部合计行

    Args:
        trade_date: 交易日期 'YYYY-MM-DD' 或 'YYYYMMDD'
        engine:     SQLAlchemy engine（可选）

    Returns:
        DataFrame 列：
          category        - 分类名称
          etf_count       - ETF只数
          total_share_yi  - 总份额（亿份）
          total_size_yi   - 总规模（亿元）

    输出示例：
        category     etf_count  total_share_yi  total_size_yi
        QDII               136         7020.09         ...
        债券                53          149.95         ...
        商品                17          376.98         ...
        指数-宽基           323         8000.00         ...
        指数-港股           101         1200.00         ...
        指数-增强            54          500.00         ...
        指数-行业&其他       898        14000.00         ...
        指数(小计)         1227        24315.49         ...
        货币                27           17.42         ...
        全部              1460        31879.94         ...
    """
    if len(trade_date) == 8 and '-' not in trade_date:
        trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"

    sql = """
        WITH base AS (
            SELECT
                primary_category,
                secondary_category,
                ts_code,
                total_share,
                total_size
            FROM v_etf_category_daily
            WHERE trade_date = :trade_date
        ),

        -- 非指数的一级分类汇总
        non_index AS (
            SELECT
                primary_category   AS category,
                1                  AS sort_order,
                primary_category   AS sort_key,
                COUNT(DISTINCT ts_code)              AS etf_count,
                ROUND(SUM(total_share) / 10000, 2)   AS total_share_yi,
                ROUND(SUM(total_size)  / 10000, 2)   AS total_size_yi
            FROM base
            WHERE primary_category != '指数'
            GROUP BY primary_category
        ),

        -- 指数按二级分类展开
        index_sub AS (
            SELECT
                '指数-' || COALESCE(secondary_category, '未分类')  AS category,
                2                   AS sort_order,
                COALESCE(secondary_category, 'zzz')  AS sort_key,
                COUNT(DISTINCT ts_code)              AS etf_count,
                ROUND(SUM(total_share) / 10000, 2)   AS total_share_yi,
                ROUND(SUM(total_size)  / 10000, 2)   AS total_size_yi
            FROM base
            WHERE primary_category = '指数'
            GROUP BY secondary_category
        ),

        -- 指数小计
        index_total AS (
            SELECT
                '指数(小计)'          AS category,
                3                   AS sort_order,
                ''                  AS sort_key,
                COUNT(DISTINCT ts_code)              AS etf_count,
                ROUND(SUM(total_share) / 10000, 2)   AS total_share_yi,
                ROUND(SUM(total_size)  / 10000, 2)   AS total_size_yi
            FROM base
            WHERE primary_category = '指数'
        ),

        -- 全部合计
        grand_total AS (
            SELECT
                '全部'               AS category,
                9                   AS sort_order,
                ''                  AS sort_key,
                COUNT(DISTINCT ts_code)              AS etf_count,
                ROUND(SUM(total_share) / 10000, 2)   AS total_share_yi,
                ROUND(SUM(total_size)  / 10000, 2)   AS total_size_yi
            FROM base
        )

        SELECT category, etf_count, total_share_yi, total_size_yi
        FROM (
            SELECT * FROM non_index
            UNION ALL SELECT * FROM index_sub
            UNION ALL SELECT * FROM index_total
            UNION ALL SELECT * FROM grand_total
        ) t
        ORDER BY sort_order, sort_key
    """

    if engine is None:
        engine = _get_engine()

    df = pd.read_sql(text(sql), engine, params={"trade_date": trade_date})
    return df


def get_category_detail(
    trade_date: str,
    primary_category: str,
    engine=None
) -> pd.DataFrame:
    """
    查询指定日期、指定一级分类下各支 ETF 的明细数据。

    Args:
        trade_date:        交易日期，格式 'YYYY-MM-DD' 或 'YYYYMMDD'
        primary_category:  一级分类，如 '货币'、'指数'、'债券'、'商品'、'QDII'
        engine:            SQLAlchemy engine（可选）

    Returns:
        DataFrame，包含该分类下每支 ETF 的 ts_code、etf_name、
        total_share（万份）、total_size（万元）、nav、close
    """
    if len(trade_date) == 8 and '-' not in trade_date:
        trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"

    sql = """
        SELECT
            ts_code,
            etf_name,
            secondary_category,
            total_share,
            total_size,
            nav,
            close,
            exchange
        FROM v_etf_category_daily
        WHERE trade_date = :trade_date
          AND primary_category = :primary_category
        ORDER BY total_size DESC NULLS LAST
    """

    if engine is None:
        engine = _get_engine()

    df = pd.read_sql(text(sql), engine, params={
        "trade_date": trade_date,
        "primary_category": primary_category
    })
    return df


def get_available_dates(
    limit: int = 30,
    engine=None
) -> list[str]:
    """
    查询 etf_share_size 表中有数据的最近 N 个交易日列表（降序）。

    Args:
        limit:  返回最近几个交易日，默认 30
        engine: SQLAlchemy engine（可选）

    Returns:
        list of str, 格式 'YYYY-MM-DD'
    """
    sql = """
        SELECT DISTINCT trade_date
        FROM etf_share_size
        ORDER BY trade_date DESC
        LIMIT :limit
    """
    if engine is None:
        engine = _get_engine()

    df = pd.read_sql(text(sql), engine, params={"limit": limit})
    return [str(d) for d in df['trade_date'].tolist()]


# ── 预聚合表查询（供 Streamlit 新 Tab 使用）───────────────────────────────────

AGG_TABLE = 'etf_category_daily_agg'
WIDE_INDEX_TABLE = 'etf_wide_index_daily_agg'


def get_category_tree(engine=None) -> dict:
    """
    从聚合表获取分类树结构，返回格式：
    {
        '指数': ['宽基', '港股', '增强', '行业&其他'],
        'QDII': ['宽基', '行业&其他'],
        '债券': ['国债', '信用债', ...],
        '商品': [],
        '货币': [],
    }
    """
    sql = f"""
        SELECT DISTINCT primary_category, secondary_category
        FROM {AGG_TABLE}
        WHERE level = 1
        ORDER BY primary_category, secondary_category
    """
    if engine is None:
        engine = _get_engine()

    df = pd.read_sql(text(sql), engine)
    tree = {}
    for _, row in df.iterrows():
        p = row['primary_category']
        s = row['secondary_category']
        if p not in tree:
            tree[p] = []
        if s and s not in tree[p]:
            tree[p].append(s)

    # 补上没有二级分类的（商品、货币）
    sql2 = f"""
        SELECT DISTINCT primary_category
        FROM {AGG_TABLE}
        WHERE level = 2 AND primary_category NOT IN ('全部')
    """
    df2 = pd.read_sql(text(sql2), engine)
    for _, row in df2.iterrows():
        p = row['primary_category']
        if p not in tree:
            tree[p] = []

    return tree


def get_category_timeseries(
    category_key: str,
    start_date: str = None,
    end_date: str = None,
    engine=None
) -> pd.DataFrame:
    """
    从预聚合表查询某分类的时序数据（供绘图用）。

    Args:
        category_key: 分类标识，如 '指数-宽基'、'指数'、'货币'、'全部'
        start_date:   起始日期 'YYYY-MM-DD'（可选）
        end_date:     结束日期 'YYYY-MM-DD'（可选）
        engine:       SQLAlchemy engine（可选）

    Returns:
        DataFrame 列:
          trade_date      DATE
          etf_count       INT
          total_share_yi  NUMERIC   总份额（亿份）
          total_size_yi   NUMERIC   总规模（亿元）
          share_change_yi NUMERIC   份额变动（亿份）
          share_change_pct NUMERIC  份额变动比例（%）
          size_change_yi  NUMERIC   规模变动（亿元）
          size_change_pct NUMERIC   规模变动比例（%）
    """
    conditions = ["category_key = :category_key"]
    params = {"category_key": category_key}

    if start_date:
        conditions.append("trade_date >= :start_date")
        params["start_date"] = start_date
    if end_date:
        conditions.append("trade_date <= :end_date")
        params["end_date"] = end_date

    where = " AND ".join(conditions)

    sql = f"""
        SELECT
            trade_date,
            etf_count,
            ROUND(total_share / 10000, 2) AS total_share_yi,
            ROUND(total_size  / 10000, 2) AS total_size_yi,
            ROUND(share_change / 10000, 2) AS share_change_yi,
            ROUND(share_change_pct * 100, 2) AS share_change_pct,
            ROUND(size_change / 10000, 2) AS size_change_yi,
            ROUND(size_change_pct * 100, 2) AS size_change_pct
        FROM {AGG_TABLE}
        WHERE {where}
        ORDER BY trade_date
    """

    if engine is None:
        engine = _get_engine()

    return pd.read_sql(text(sql), engine, params=params)


def get_agg_summary(trade_date: str, engine=None) -> pd.DataFrame:
    """
    从预聚合表获取某日的完整分类汇总（用于下方表格）。

    Args:
        trade_date: 'YYYY-MM-DD'

    Returns:
        DataFrame 列: category_key, primary_category, secondary_category,
                       level, etf_count, total_share_yi, total_size_yi,
                       share_change_yi, share_change_pct,
                       size_change_yi, size_change_pct
        按 level（二级明细→一级小计→合计）和名称排序
    """
    sql = f"""
        SELECT
            category_key,
            primary_category,
            secondary_category,
            level,
            etf_count,
            ROUND(total_share / 10000, 2) AS total_share_yi,
            ROUND(total_size  / 10000, 2) AS total_size_yi,
            ROUND(share_change / 10000, 2) AS share_change_yi,
            ROUND(share_change_pct * 100, 2) AS share_change_pct,
            ROUND(size_change / 10000, 2) AS size_change_yi,
            ROUND(size_change_pct * 100, 2) AS size_change_pct
        FROM {AGG_TABLE}
        WHERE trade_date = :trade_date
        ORDER BY
            CASE primary_category
                WHEN 'QDII' THEN 1
                WHEN '债券' THEN 2
                WHEN '商品' THEN 3
                WHEN '指数' THEN 4
                WHEN '货币' THEN 5
                WHEN '全部' THEN 9
            END,
            level,
            secondary_category NULLS LAST
    """
    if engine is None:
        engine = _get_engine()

    return pd.read_sql(text(sql), engine, params={"trade_date": trade_date})


def get_wide_index_available_dates(limit: int = 1000, engine=None) -> list[str]:
    sql = f"""
        SELECT DISTINCT trade_date
        FROM {WIDE_INDEX_TABLE}
        ORDER BY trade_date DESC
        LIMIT :limit
    """
    if engine is None:
        engine = _get_engine()

    df = pd.read_sql(text(sql), engine, params={"limit": limit})
    return [str(d) for d in df['trade_date'].tolist()]


def get_wide_index_timeseries(
    start_date: str = None,
    end_date: str = None,
    benchmark_codes: list[str] | None = None,
    engine=None
) -> pd.DataFrame:
    conditions = ["1=1"]
    params = {}

    if start_date:
        conditions.append("trade_date >= :start_date")
        params["start_date"] = start_date
    if end_date:
        conditions.append("trade_date <= :end_date")
        params["end_date"] = end_date
    if benchmark_codes:
        code_params = []
        for idx, code in enumerate(benchmark_codes):
            key = f"code_{idx}"
            code_params.append(f":{key}")
            params[key] = code
        conditions.append(f"benchmark_index_code IN ({', '.join(code_params)})")

    sql = f"""
        SELECT
            trade_date,
            benchmark_index_code,
            benchmark_index_name,
            etf_count,
            ROUND(total_share / 10000, 2) AS total_share_yi,
            ROUND(total_size / 10000, 2) AS total_size_yi,
            ROUND(share_change / 10000, 2) AS share_change_yi,
            ROUND(share_change_pct * 100, 2) AS share_change_pct,
            ROUND(size_change / 10000, 2) AS size_change_yi,
            ROUND(size_change_pct * 100, 2) AS size_change_pct
        FROM {WIDE_INDEX_TABLE}
        WHERE {' AND '.join(conditions)}
        ORDER BY trade_date, benchmark_index_code
    """

    if engine is None:
        engine = _get_engine()

    return pd.read_sql(text(sql), engine, params=params)


# ── 命令行快速验证 ────────────────────────────────────────────────────────────
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='ETF分类统计查询')
    parser.add_argument('--date', default=None, help='交易日期 YYYY-MM-DD（默认取最新）')
    parser.add_argument('--category', default=None, help='查看某一级分类明细（如 货币）')
    parser.add_argument('--full', action='store_true', help='完整汇总（含指数二级分类展开）')
    args = parser.parse_args()

    engine = _get_engine()

    # 若未指定日期，取最新交易日
    if args.date is None:
        dates = get_available_dates(limit=1, engine=engine)
        args.date = dates[0] if dates else '2026-03-26'
        print(f"使用最新交易日: {args.date}\n")

    if args.category:
        print(f"=== {args.date}  [{args.category}] 明细 ===")
        df = get_category_detail(args.date, args.category, engine)
        print(df.to_string(index=False))
    elif args.full:
        print(f"=== {args.date}  完整分类汇总 ===")
        df = get_full_daily_summary(args.date, engine)
        print(df.to_string(index=False))
    else:
        print(f"=== {args.date}  各一级分类汇总 ===")
        df = get_category_daily_summary(args.date, engine)
        print(df.to_string(index=False))
