# -*- coding: utf-8 -*-
from __future__ import annotations
"""
ETF分类统计查询模块
提供按一级分类汇总指定日期 ETF 份额/规模的工具函数
数据来源: 视图 v_etf_category_daily (etf_share_size JOIN etf_summary)
"""

import io
import os
import re
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
STOCK_BASIC_VIEW = 'vw_ts_stock_basic'
STOCK_COMPANY_VIEW = 'vw_ts_stock_company'
STOCK_INCOME_VIEW = 'vw_ts_stock_income'
STOCK_BALANCE_VIEW = 'vw_ts_stock_balancesheet'
STOCK_CASHFLOW_VIEW = 'vw_ts_stock_cashflow'
STOCK_FINA_VIEW = 'vw_ts_stock_fina_indicator'
STOCK_DAILY_VIEW = 'vw_ts_stock_daily_basic'
INDEX_DAILY_VIEW = 'vw_ts_index_dailybasic'

STOCK_BASIC_EXPORT_RENAME_MAP = {
    'ts_code': '股票代码',
    'symbol': '股票代码简写',
    'name': '股票简称',
    'fullname': '股票全称',
    'enname': '英文全称',
    'cnspell': '拼音缩写',
    'area': '地域',
    'industry': '所属行业',
    'market': '市场类型',
    'exchange': '交易所',
    'curr_type': '交易币种',
    'list_status': '上市状态',
    'list_date': '上市日期',
    'delist_date': '退市日期',
    'is_hs': '沪深港通标识',
    'act_name': '实控人名称',
    'act_ent_type': '实控人企业性质',
    'chairman': '董事长',
    'manager': '总经理',
    'secretary': '董事会秘书',
    'reg_capital': '注册资本',
    'setup_date': '成立日期',
    'province': '省份',
    'city': '城市',
    'website': '公司网站',
    'email': '电子邮箱',
    'office': '办公地址',
    'employees': '员工人数',
    'main_business': '主营业务原文',
    'business_scope': '经营范围',
    'introduction': '公司介绍',
}


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


def search_security(keyword: str, security_type: str = 'all', limit: int = 20, engine=None) -> pd.DataFrame:
    keyword = (keyword or '').strip()
    if not keyword:
        return pd.DataFrame(columns=['security_type', 'ts_code', 'symbol', 'name', 'industry', 'market', 'latest_date'])

    if security_type not in {'all', 'stock', 'index'}:
        raise ValueError(f'不支持的 security_type: {security_type}')

    if engine is None:
        engine = _get_engine()

    like_kw = f'%{keyword}%'
    queries = []
    params = {'like_kw': like_kw, 'limit': max(1, int(limit))}

    if security_type in {'all', 'stock'}:
        queries.append(f"""
            SELECT
                'stock' AS security_type,
                ts_code,
                symbol,
                name,
                industry,
                market,
                NULL::date AS latest_date
            FROM {STOCK_BASIC_VIEW}
            WHERE
                ts_code ILIKE :like_kw
                OR COALESCE(symbol, '') ILIKE :like_kw
                OR COALESCE(name, '') ILIKE :like_kw
                OR COALESCE(fullname, '') ILIKE :like_kw
                OR COALESCE(cnspell, '') ILIKE :like_kw
        """)

    if security_type in {'all', 'index'}:
        queries.append(f"""
            SELECT
                'index' AS security_type,
                ts_code,
                ts_code AS symbol,
                name,
                NULL::text AS industry,
                NULL::text AS market,
                trade_date AS latest_date
            FROM (
                SELECT DISTINCT ON (ts_code)
                    ts_code,
                    trade_date,
                    COALESCE(NULLIF(payload->>'name', ''), NULLIF(payload->>'ts_name', ''), ts_code) AS name
                FROM {INDEX_DAILY_VIEW}
                ORDER BY ts_code, trade_date DESC NULLS LAST
            ) latest_index
            WHERE
                ts_code ILIKE :like_kw
                OR COALESCE(name, '') ILIKE :like_kw
        """)

    union_sql = "\nUNION ALL\n".join(queries)
    sql = f"""
        SELECT *
        FROM (
            {union_sql}
        ) s
        ORDER BY
            CASE security_type WHEN 'stock' THEN 1 ELSE 2 END,
            name NULLS LAST,
            ts_code
        LIMIT :limit
    """
    return pd.read_sql(text(sql), engine, params=params)


def _clean_export_text(value) -> str:
    if value is None or pd.isna(value):
        return ''
    text_value = str(value).strip()
    if not text_value or text_value.lower() == 'nan':
        return ''
    return re.sub(r'\s+', ' ', text_value)


def _split_text_segments(value) -> list[str]:
    text_value = _clean_export_text(value)
    if not text_value:
        return []
    normalized = (
        text_value
        .replace('\r', '；')
        .replace('\n', '；')
        .replace('|', '；')
        .replace('。', '；')
    )
    parts = re.split(r'[；;]+', normalized)
    return [part.strip(' ，,、:：') for part in parts if part and part.strip(' ，,、:：')]


def _split_list_items(value) -> list[str]:
    text_value = _clean_export_text(value)
    if not text_value:
        return []
    parts = re.split(r'[、,，/]+', text_value)
    cleaned = []
    seen = set()
    for part in parts:
        item = re.sub(r'(等|等产品|等业务)$', '', part).strip(' :：;；，,、')
        if item and item not in seen:
            cleaned.append(item)
            seen.add(item)
    return cleaned


def _deduplicate_text_items(items: list[str]) -> list[str]:
    cleaned = []
    seen = set()
    for item in items:
        text_value = _clean_export_text(item).strip(' :：;；，,、')
        if not text_value:
            continue
        if text_value in seen:
            continue
        cleaned.append(text_value)
        seen.add(text_value)
    return cleaned


def _strip_business_leading_text(value: str) -> str:
    text_value = _clean_export_text(value)
    if not text_value:
        return ''

    patterns = [
        r'^(公司|本公司)',
        r'^是一家',
        r'^主要',
        r'^主营',
        r'^(主营|主要)?业务(?:范围)?(?:集中)?(?:主要)?(?:在|为|是|包括|涵盖|涉及)?[:：]?',
        r'^(公司|本公司)?(?:主要|主营)?从事',
        r'^(公司|本公司)?专注于',
        r'^(公司|本公司)?致力于',
        r'^(公司|本公司)?深耕于',
        r'^(公司|本公司)?聚焦于',
        r'^(公司|本公司)?以.+?为主',
    ]
    for pattern in patterns:
        text_value = re.sub(pattern, '', text_value).strip()
    return text_value.strip(' :：;；，,、')


def _normalize_business_summary(value: str) -> str:
    text_value = _strip_business_leading_text(value)
    if not text_value:
        return ''

    text_value = re.sub(
        r'(产品(?:包括|有|为|涵盖|涉及|包含|系列|类别|结构)|业务(?:包括|有|为|涵盖|涉及|包含)|服务(?:包括|有|为|涵盖|涉及|包含))[:：]?.*$',
        '',
        text_value,
    ).strip()
    text_value = re.sub(r'(?:主要)?产品(?:线|类别|系列)?(?:包括|有|为|涵盖|涉及|包含)?$', '', text_value).strip()
    text_value = re.sub(r'(?:产品线|产品类别|产品系列|核心产品)$', '', text_value).strip()
    text_value = re.sub(r'^(以及|并|并且|同时|形成了|拥有|具备)', '', text_value).strip()
    text_value = re.sub(r'[，,]?(?:主要)?$', '', text_value).strip()
    text_value = re.sub(r'(等(相关)?(业务|服务|产品))$', '', text_value).strip()
    text_value = re.split(r'[，,](?=提供|形成|拥有|具备|覆盖)', text_value, maxsplit=1)[0]
    text_value = re.sub(r'\s+', ' ', text_value)
    text_value = text_value.strip(' :：;；，,、')

    clauses = [
        part.strip(' ，,、:：')
        for part in re.split(r'[；;]+', text_value)
        if part and part.strip(' ，,、:：')
    ]
    if not clauses:
        return ''
    return '；'.join(_deduplicate_text_items(clauses))


def _extract_explicit_product_text(segment: str) -> str:
    explicit_patterns = [
        r'(?:主要)?产品(?:包括|有|为|涵盖|涉及|包含|主要包括|主要有|主要为)?[:：]?\s*(.+)$',
        r'(?:产品线|产品类别|产品系列|核心产品)(?:包括|有|为|涵盖|涉及|包含)?[:：]?\s*(.+)$',
        r'(?:提供|形成)(?:了)?(?:.+?)?(?:产品|服务)(?:包括|有|为|涵盖|涉及|包含)?[:：]?\s*(.+)$',
    ]
    for pattern in explicit_patterns:
        match = re.search(pattern, segment)
        if match:
            return _clean_export_text(match.group(1))
    return ''


def _extract_product_candidates(value) -> list[str]:
    text_value = _clean_export_text(value)
    if not text_value:
        return []

    text_value = re.sub(r'^(产品线|产品类别|产品系列|核心产品)', '', text_value).strip()
    text_value = re.sub(r'^(包括|有|为|涵盖|涉及|包含|主要包括|主要有|主要为)', '', text_value).strip(' :：')

    parts = re.split(r'(?:、|,|，|/|以及|及|和|与)', text_value)
    results = []
    for part in parts:
        item = _clean_export_text(part)
        item = re.sub(r'^(主要|核心)', '', item).strip()
        item = re.sub(r'^(包括|有|为|涵盖|涉及|包含|线涵盖)', '', item).strip()
        item = re.sub(r'(等|等产品|等服务|等业务)$', '', item).strip(' :：;；，,、')
        if not item:
            continue
        if item in {'和', '与', '及', '以及'}:
            continue
        if any(keyword in item for keyword in ['研发', '生产', '销售', '制造', '运营', '服务', '开发', '设计', '施工', '建设', '管理', '加工', '集成', '推广', '代理', '维护', '租赁']):
            continue
        results.append(item)
    return _deduplicate_text_items(results)


def _infer_products_from_business_text(value: str) -> list[str]:
    text_value = _normalize_business_summary(value)
    if not text_value:
        return []

    inferred = []
    clauses = [
        clause.strip(' :：;；，,、')
        for clause in re.split(r'[；;，,]+', text_value)
        if clause and clause.strip(' :：;；，,、')
    ]
    for clause in clauses:
        match = re.match(
            r'^(.+?)(?:的)?(?:研发|生产|销售|制造|运营|服务|开发|设计|施工|建设|管理|加工|集成|推广|代理|维护|租赁)(?:$|[、,，及和与])',
            clause,
        )
        if not match:
            continue
        candidate = _clean_export_text(match.group(1))
        candidate = re.sub(r'^(相关|各类|系列|综合|专业|高端|中高端)', '', candidate).strip()
        candidate = candidate.strip(' :：;；，,、')
        if candidate and len(candidate) <= 30:
            inferred.extend(_extract_product_candidates(candidate) or [candidate])
    return _deduplicate_text_items(inferred)


def _extract_main_business_parts(value) -> tuple[list[str], list[str]]:
    business_items = []
    product_items = []

    for segment in _split_text_segments(value):
        explicit_product_text = _extract_explicit_product_text(segment)
        business_segment = segment
        if explicit_product_text:
            explicit_index = segment.find(explicit_product_text)
            if explicit_index > 0:
                business_segment = segment[:explicit_index]

        business_summary = _normalize_business_summary(business_segment)
        if business_summary:
            business_items.append(business_summary)

        if explicit_product_text:
            product_items.extend(_extract_product_candidates(explicit_product_text))

        if not explicit_product_text:
            product_items.extend(_infer_products_from_business_text(business_summary))

    business_items = _deduplicate_text_items(business_items)
    product_items = _deduplicate_text_items(product_items)
    return business_items, product_items


def build_stock_basic_summary_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    export_df = df.copy()
    if export_df.empty:
        base_columns = list(STOCK_BASIC_EXPORT_RENAME_MAP.values())
        return pd.DataFrame(columns=base_columns)

    export_df['main_business'] = export_df['main_business'].apply(_clean_export_text)
    business_product_parts = export_df['main_business'].apply(_extract_main_business_parts)
    export_df['_business_items'] = business_product_parts.apply(lambda value: value[0])
    export_df['_product_items'] = business_product_parts.apply(lambda value: value[1])
    export_df['主要业务'] = export_df.apply(
        lambda row: '、'.join(row['_business_items']) if row['_business_items'] else row['main_business'],
        axis=1
    )
    export_df['产品'] = export_df['_product_items'].apply(lambda items: '、'.join(items))

    export_df = export_df.drop(columns=['_business_items', '_product_items'])
    export_df = export_df.rename(columns=STOCK_BASIC_EXPORT_RENAME_MAP)

    ordered_columns = [
        column
        for column in list(STOCK_BASIC_EXPORT_RENAME_MAP.values())
        if column != '主营业务原文'
    ]
    insert_position = ordered_columns.index('经营范围') if '经营范围' in ordered_columns else len(ordered_columns)
    ordered_columns[insert_position:insert_position] = ['主营业务原文', '主要业务', '产品']
    export_df = export_df.loc[:, [column for column in ordered_columns if column in export_df.columns]]

    export_df = export_df.sort_values(by=['所属行业', '股票代码'], na_position='last').reset_index(drop=True)
    return export_df


def get_stock_basic_summary(engine=None) -> pd.DataFrame:
    if engine is None:
        engine = _get_engine()

    sql = f"""
        WITH latest_trade AS (
            SELECT MAX(trade_date) AS trade_date
            FROM {STOCK_DAILY_VIEW}
        ),
        active_codes AS (
            SELECT DISTINCT daily.ts_code
            FROM {STOCK_DAILY_VIEW} AS daily
            JOIN latest_trade
              ON daily.trade_date = latest_trade.trade_date
        )
        SELECT
            basic.ts_code,
            basic.symbol,
            basic.name,
            basic.fullname,
            basic.enname,
            basic.cnspell,
            basic.area,
            basic.industry,
            basic.market,
            COALESCE(company.exchange, basic.exchange) AS exchange,
            basic.curr_type,
            basic.list_status,
            basic.list_date,
            basic.delist_date,
            basic.is_hs,
            basic.act_name,
            basic.act_ent_type,
            company.chairman,
            company.manager,
            company.secretary,
            company.reg_capital,
            company.setup_date,
            company.province,
            company.city,
            company.website,
            company.email,
            company.office,
            company.employees,
            company.main_business,
            company.business_scope,
            company.introduction
        FROM {STOCK_BASIC_VIEW} AS basic
        LEFT JOIN {STOCK_COMPANY_VIEW} AS company
          ON basic.ts_code = company.ts_code
        LEFT JOIN active_codes
          ON basic.ts_code = active_codes.ts_code
        WHERE (
            COALESCE(basic.list_status, '') = 'L'
            OR (
                COALESCE(basic.list_status, '') = ''
                AND active_codes.ts_code IS NOT NULL
            )
        )
        ORDER BY basic.industry NULLS LAST, basic.ts_code
    """
    merged_df = pd.read_sql(text(sql), engine)
    return build_stock_basic_summary_dataframe(merged_df)


def export_stock_basic_summary_excel(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='股票基本信息汇总表', index=False)
        worksheet = writer.sheets['股票基本信息汇总表']
        worksheet.freeze_panes = 'A2'
        worksheet.auto_filter.ref = worksheet.dimensions
    return output.getvalue()


def get_stock_profile(ts_code: str, engine=None) -> pd.DataFrame:
    if engine is None:
        engine = _get_engine()

    sql = f"""
        WITH basic AS (
            SELECT * FROM {STOCK_BASIC_VIEW}
            WHERE ts_code = :ts_code
            LIMIT 1
        ),
        company AS (
            SELECT * FROM {STOCK_COMPANY_VIEW}
            WHERE ts_code = :ts_code
            LIMIT 1
        ),
        daily AS (
            SELECT * FROM {STOCK_DAILY_VIEW}
            WHERE ts_code = :ts_code
            ORDER BY trade_date DESC NULLS LAST
            LIMIT 1
        ),
        fina AS (
            SELECT * FROM {STOCK_FINA_VIEW}
            WHERE ts_code = :ts_code
            ORDER BY end_date DESC NULLS LAST, ann_date DESC NULLS LAST
            LIMIT 1
        ),
        income AS (
            SELECT * FROM {STOCK_INCOME_VIEW}
            WHERE ts_code = :ts_code
            ORDER BY end_date DESC NULLS LAST, ann_date DESC NULLS LAST
            LIMIT 1
        ),
        balance AS (
            SELECT * FROM {STOCK_BALANCE_VIEW}
            WHERE ts_code = :ts_code
            ORDER BY end_date DESC NULLS LAST, ann_date DESC NULLS LAST
            LIMIT 1
        ),
        cashflow AS (
            SELECT * FROM {STOCK_CASHFLOW_VIEW}
            WHERE ts_code = :ts_code
            ORDER BY end_date DESC NULLS LAST, ann_date DESC NULLS LAST
            LIMIT 1
        )
        SELECT
            COALESCE(basic.ts_code, daily.ts_code, fina.ts_code, income.ts_code, balance.ts_code, cashflow.ts_code, :ts_code) AS ts_code,
            basic.symbol,
            basic.name,
            basic.industry,
            basic.market,
            basic.exchange,
            basic.list_status,
            basic.list_date,
            basic.act_name,
            company.province,
            company.city,
            company.website,
            daily.trade_date AS latest_trade_date,
            daily.close,
            daily.turnover_rate,
            daily.volume_ratio,
            daily.pe_ttm,
            daily.pb,
            daily.ps_ttm,
            daily.total_mv,
            daily.circ_mv,
            fina.end_date AS fina_end_date,
            fina.roe,
            fina.roa,
            fina.grossprofit_margin AS gross_margin,
            fina.debt_to_assets,
            income.end_date AS income_end_date,
            income.total_revenue,
            income.n_income,
            balance.end_date AS balance_end_date,
            balance.total_assets,
            balance.total_liab,
            balance.total_hldr_eqy_exc_min_int,
            cashflow.end_date AS cashflow_end_date,
            cashflow.n_cashflow_act
        FROM (SELECT 1 AS anchor) AS seed
        LEFT JOIN basic ON TRUE
        LEFT JOIN company ON TRUE
        LEFT JOIN daily ON TRUE
        LEFT JOIN fina ON TRUE
        LEFT JOIN income ON TRUE
        LEFT JOIN balance ON TRUE
        LEFT JOIN cashflow ON TRUE
    """
    return pd.read_sql(text(sql), engine, params={'ts_code': ts_code})


def get_stock_timeseries(ts_code: str, start_date: str = None, end_date: str = None, engine=None) -> pd.DataFrame:
    if engine is None:
        engine = _get_engine()

    conditions = ["ts_code = :ts_code"]
    params = {'ts_code': ts_code}
    if start_date:
        conditions.append("trade_date >= :start_date")
        params['start_date'] = start_date
    if end_date:
        conditions.append("trade_date <= :end_date")
        params['end_date'] = end_date

    sql = f"""
        SELECT
            trade_date,
            close,
            turnover_rate,
            turnover_rate_f,
            volume_ratio,
            pe,
            pe_ttm,
            pb,
            ps,
            ps_ttm,
            dv_ratio,
            dv_ttm,
            total_share,
            float_share,
            free_share,
            total_mv,
            circ_mv
        FROM {STOCK_DAILY_VIEW}
        WHERE {' AND '.join(conditions)}
        ORDER BY trade_date
    """
    return pd.read_sql(text(sql), engine, params=params)


def get_stock_financial_timeseries(ts_code: str, engine=None) -> pd.DataFrame:
    if engine is None:
        engine = _get_engine()

    sql = f"""
        WITH income AS (
            SELECT DISTINCT ON (end_date)
                ts_code,
                end_date,
                ann_date,
                total_revenue,
                COALESCE(n_income_attr_p, n_income) AS net_profit
            FROM {STOCK_INCOME_VIEW}
            WHERE ts_code = :ts_code
            ORDER BY end_date DESC NULLS LAST, ann_date DESC NULLS LAST
        ),
        fina AS (
            SELECT DISTINCT ON (end_date)
                ts_code,
                end_date,
                ann_date,
                profit_dedt
            FROM {STOCK_FINA_VIEW}
            WHERE ts_code = :ts_code
            ORDER BY end_date DESC NULLS LAST, ann_date DESC NULLS LAST
        )
        SELECT
            COALESCE(income.ts_code, fina.ts_code, :ts_code) AS ts_code,
            COALESCE(income.end_date, fina.end_date) AS end_date,
            COALESCE(income.ann_date, fina.ann_date) AS ann_date,
            income.total_revenue,
            income.net_profit,
            fina.profit_dedt
        FROM income
        FULL OUTER JOIN fina
          ON income.ts_code = fina.ts_code
         AND income.end_date = fina.end_date
        WHERE COALESCE(income.end_date, fina.end_date) IS NOT NULL
        ORDER BY end_date
    """
    return pd.read_sql(text(sql), engine, params={'ts_code': ts_code})


def get_index_profile(ts_code: str, engine=None) -> pd.DataFrame:
    if engine is None:
        engine = _get_engine()

    sql = f"""
        WITH latest_index AS (
            SELECT *
            FROM {INDEX_DAILY_VIEW}
            WHERE ts_code = :ts_code
            ORDER BY trade_date DESC NULLS LAST
            LIMIT 1
        )
        SELECT
            ts_code,
            COALESCE(NULLIF(payload->>'name', ''), NULLIF(payload->>'ts_name', ''), ts_code) AS name,
            trade_date AS latest_trade_date,
            close,
            turnover_rate,
            turnover_rate_f,
            pe,
            pe_ttm,
            pb,
            total_share,
            float_share,
            free_share,
            total_mv,
            float_mv
        FROM latest_index
    """
    return pd.read_sql(text(sql), engine, params={'ts_code': ts_code})


def get_index_timeseries(ts_code: str, start_date: str = None, end_date: str = None, engine=None) -> pd.DataFrame:
    if engine is None:
        engine = _get_engine()

    conditions = ["ts_code = :ts_code"]
    params = {'ts_code': ts_code}
    if start_date:
        conditions.append("trade_date >= :start_date")
        params['start_date'] = start_date
    if end_date:
        conditions.append("trade_date <= :end_date")
        params['end_date'] = end_date

    sql = f"""
        SELECT
            trade_date,
            close,
            turnover_rate,
            turnover_rate_f,
            pe,
            pe_ttm,
            pb,
            total_share,
            float_share,
            free_share,
            total_mv,
            float_mv
        FROM {INDEX_DAILY_VIEW}
        WHERE {' AND '.join(conditions)}
        ORDER BY trade_date
    """
    return pd.read_sql(text(sql), engine, params=params)


def get_security_profile(ts_code: str, security_type: str, engine=None) -> pd.DataFrame:
    if security_type == 'stock':
        return get_stock_profile(ts_code, engine=engine)
    if security_type == 'index':
        return get_index_profile(ts_code, engine=engine)
    raise ValueError(f'不支持的 security_type: {security_type}')


def get_security_timeseries(ts_code: str, security_type: str, start_date: str = None, end_date: str = None, engine=None) -> pd.DataFrame:
    if security_type == 'stock':
        return get_stock_timeseries(ts_code, start_date=start_date, end_date=end_date, engine=engine)
    if security_type == 'index':
        return get_index_timeseries(ts_code, start_date=start_date, end_date=end_date, engine=engine)
    raise ValueError(f'不支持的 security_type: {security_type}')


def get_security_financial_timeseries(ts_code: str, security_type: str, engine=None) -> pd.DataFrame:
    if security_type == 'stock':
        return get_stock_financial_timeseries(ts_code, engine=engine)
    return pd.DataFrame(columns=['ts_code', 'end_date', 'ann_date', 'total_revenue', 'net_profit', 'profit_dedt'])


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
