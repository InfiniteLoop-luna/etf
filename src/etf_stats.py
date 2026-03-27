# -*- coding: utf-8 -*-
"""
ETF分类统计查询模块
提供按一级分类汇总指定日期 ETF 份额/规模的工具函数
数据来源: 视图 v_etf_category_daily (etf_share_size JOIN etf_summary)
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date, datetime
from typing import Optional

DB_URL = 'postgresql://postgres:Zmx1018$@67.216.207.73:5432/postgres'


def _get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)


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


# ── 命令行快速验证 ────────────────────────────────────────────────────────────
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='ETF分类统计查询')
    parser.add_argument('--date', default=None, help='交易日期 YYYY-MM-DD（默认取最新）')
    parser.add_argument('--category', default=None, help='查看某一级分类明细（如 货币）')
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
    else:
        print(f"=== {args.date}  各一级分类汇总 ===")
        df = get_category_daily_summary(args.date, engine)
        print(df.to_string(index=False))
