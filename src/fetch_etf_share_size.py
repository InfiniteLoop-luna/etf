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
from sqlalchemy.engine import URL

# ── 路径设置（兼容直接运行和作为模块导入）──────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.volume_fetcher import _init_tushare

# ── 配置 ────────────────────────────────────────────────────────────────────
SUMMARY_TABLE   = 'etf_summary'
TARGET_TABLE    = 'etf_share_size'
AGG_TABLE       = 'etf_category_daily_agg'
DEFAULT_START   = '20230101'       # 历史全量起始日
BATCH_DAYS      = int(os.getenv('ETF_SHARE_BATCH_DAYS', '1'))
API_SLEEP       = float(os.getenv('ETF_SHARE_API_SLEEP', '0.4'))
VERIFY_ENABLED  = os.getenv('ETF_SHARE_VERIFY', '1').strip().lower() not in {'0', 'false', 'no'}
VERIFY_MAX_DAYS = int(os.getenv('ETF_SHARE_VERIFY_MAX_DAYS', '10'))
VERIFY_TOLERANCE = float(os.getenv('ETF_SHARE_VERIFY_TOLERANCE', '0.01'))
VERIFY_LOG_LIMIT = int(os.getenv('ETF_SHARE_VERIFY_LOG_LIMIT', '10'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ── 工具函数 ─────────────────────────────────────────────────────────────────

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


def normalize_date(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.replace('-', '').strip()
    if len(cleaned) != 8:
        raise ValueError(f'日期格式错误: {value}')
    datetime.strptime(cleaned, '%Y%m%d')
    return cleaned


def to_iso_date(value: str) -> str:
    return datetime.strptime(value, '%Y%m%d').strftime('%Y-%m-%d')


def count_range_days(start_date: str, end_date: str) -> int:
    start = datetime.strptime(start_date, '%Y%m%d').date()
    end = datetime.strptime(end_date, '%Y%m%d').date()
    return (end - start).days + 1


def summarize_df(df: pd.DataFrame) -> dict[str, dict[str, float | int]]:
    if df is None or df.empty:
        return {}

    data = df.copy()
    data['trade_date'] = pd.to_datetime(data['trade_date'], format='%Y%m%d', errors='coerce').dt.strftime('%Y%m%d')
    data = data.dropna(subset=['trade_date', 'ts_code'])

    for col in ['total_share', 'total_size']:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors='coerce').fillna(0.0)
        else:
            data[col] = 0.0

    grouped = (
        data.groupby('trade_date', as_index=False)
        .agg(
            row_count=('ts_code', 'count'),
            total_share=('total_share', 'sum'),
            total_size=('total_size', 'sum')
        )
    )

    return {
        row['trade_date']: {
            'row_count': int(row['row_count']),
            'total_share': float(row['total_share']),
            'total_size': float(row['total_size'])
        }
        for _, row in grouped.iterrows()
    }


def get_db_daily_stats(engine, start_date: str, end_date: str) -> dict[str, dict[str, float | int]]:
    sql = text(f"""
        SELECT
            TO_CHAR(trade_date, 'YYYYMMDD') AS trade_date,
            COUNT(*) AS row_count,
            COALESCE(SUM(total_share), 0) AS total_share,
            COALESCE(SUM(total_size), 0) AS total_size
        FROM {TARGET_TABLE}
        WHERE trade_date BETWEEN :start_date AND :end_date
        GROUP BY trade_date
    """)
    params = {'start_date': to_iso_date(start_date), 'end_date': to_iso_date(end_date)}
    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return {
        row[0]: {
            'row_count': int(row[1]),
            'total_share': float(row[2]),
            'total_size': float(row[3])
        }
        for row in rows
    }


def get_agg_daily_stats(engine, start_date: str, end_date: str) -> dict[str, dict[str, float | int]]:
    sql = text(f"""
        SELECT
            TO_CHAR(trade_date, 'YYYYMMDD') AS trade_date,
            etf_count,
            COALESCE(total_share, 0) AS total_share,
            COALESCE(total_size, 0) AS total_size
        FROM {AGG_TABLE}
        WHERE category_key = '全部'
          AND trade_date BETWEEN :start_date AND :end_date
    """)
    params = {'start_date': to_iso_date(start_date), 'end_date': to_iso_date(end_date)}
    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return {
        row[0]: {
            'row_count': int(row[1]),
            'total_share': float(row[2]),
            'total_size': float(row[3])
        }
        for row in rows
    }


def stats_match(left: dict[str, float | int], right: dict[str, float | int]) -> bool:
    return (
        int(left['row_count']) == int(right['row_count'])
        and abs(float(left['total_share']) - float(right['total_share'])) <= VERIFY_TOLERANCE
        and abs(float(left['total_size']) - float(right['total_size'])) <= VERIFY_TOLERANCE
    )


def format_stats(label: str, stats: dict[str, float | int]) -> str:
    return (
        f"{label}(count={int(stats['row_count'])}, "
        f"share={float(stats['total_share']):.4f}, "
        f"size={float(stats['total_size']):.4f})"
    )


def validate_range(engine, pro, start_date: str, end_date: str) -> list[str]:
    db_stats = get_db_daily_stats(engine, start_date, end_date)
    agg_stats = get_agg_daily_stats(engine, start_date, end_date)
    mismatches: list[str] = []
    total_days = count_range_days(start_date, end_date)

    for idx, (trade_date, _) in enumerate(daterange(start_date, end_date, 1), 1):
        ts_stats = summarize_df(fetch_batch(pro, trade_date, trade_date, strict=True)).get(
            trade_date,
            {'row_count': 0, 'total_share': 0.0, 'total_size': 0.0}
        )
        db_stat = db_stats.get(trade_date, {'row_count': 0, 'total_share': 0.0, 'total_size': 0.0})
        agg_stat = agg_stats.get(trade_date)

        if not stats_match(ts_stats, db_stat):
            mismatches.append(
                f"{trade_date} 明细不一致: "
                f"{format_stats('db', db_stat)} != {format_stats('tushare', ts_stats)}"
            )

        if agg_stat is None:
            if db_stat['row_count'] > 0:
                mismatches.append(f"{trade_date} 聚合缺失: {format_stats('db', db_stat)}")
        elif not stats_match(db_stat, agg_stat):
            mismatches.append(
                f"{trade_date} 聚合不一致: "
                f"{format_stats('agg', agg_stat)} != {format_stats('db', db_stat)}"
            )

        if total_days > 1 and idx % 5 == 0:
            logger.info(f"巡检进度: {idx}/{total_days}")

    return mismatches


def run_validation(engine, pro, start_date: str, end_date: str):
    logger.info(f"开始巡检：{start_date} ~ {end_date}")
    mismatches = validate_range(engine, pro, start_date, end_date)
    if mismatches:
        for item in mismatches[:VERIFY_LOG_LIMIT]:
            logger.error(item)
        if len(mismatches) > VERIFY_LOG_LIMIT:
            logger.error(f"其余 {len(mismatches) - VERIFY_LOG_LIMIT} 条异常已省略")
        raise RuntimeError(f"巡检失败，共发现 {len(mismatches)} 条异常")
    logger.info(f"巡检通过：{start_date} ~ {end_date}")


def resolve_fetch_range(engine, full: bool = False, start_date: str | None = None, end_date: str | None = None) -> tuple[str | None, str | None]:
    today = datetime.now().strftime('%Y%m%d')
    start_date = normalize_date(start_date)
    end_date = normalize_date(end_date)

    if start_date or end_date:
        start_date = start_date or end_date
        end_date = end_date or start_date
        if start_date > end_date:
            raise ValueError(f'开始日期不能晚于结束日期: {start_date} > {end_date}')
        logger.info(f"指定区间模式：从 {start_date} 到 {end_date}")
        return start_date, end_date

    if full:
        logger.info(f"全量模式：从 {DEFAULT_START} 到 {today}")
        return DEFAULT_START, today

    latest = get_latest_date(engine)
    if latest:
        next_day = (datetime.strptime(latest, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
        if next_day > today:
            logger.info(f"数据已是最新（最新日期: {latest}），无需更新")
            return None, None
        logger.info(f"增量模式：从 {next_day} 到 {today}（DB 最新: {latest}）")
        return next_day, today

    logger.info(f"DB 无数据，全量拉取：从 {DEFAULT_START} 到 {today}")
    return DEFAULT_START, today


def fetch_batch(pro, start_date: str, end_date: str, strict: bool = False) -> pd.DataFrame:
    """
    按日期批次拉取全市场 ETF 份额规模（不传 ts_code，按日期区间拉取全部）
    默认按单日拉取，规避 etf_share_size 单次 5000 条上限带来的截断风险。
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
        if strict:
            raise
        logger.warning(f"  批次 {start_date}~{end_date} 拉取失败: {exc}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def upsert_df(engine, df: pd.DataFrame):
    """
    将 DataFrame 写入 PostgreSQL，使用 INSERT ... ON CONFLICT DO UPDATE
    保证重复执行时可刷新同日数据
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

        # INSERT ... ON CONFLICT DO UPDATE
        conn.execute(text(f"""
            INSERT INTO {TARGET_TABLE} (trade_date, ts_code, etf_name, total_share, total_size, nav, close, exchange)
            SELECT trade_date, ts_code, etf_name, total_share, total_size, nav, close, exchange
            FROM {tmp_table}
            ON CONFLICT (trade_date, ts_code) DO UPDATE SET
                etf_name = EXCLUDED.etf_name,
                total_share = EXCLUDED.total_share,
                total_size = EXCLUDED.total_size,
                nav = EXCLUDED.nav,
                close = EXCLUDED.close,
                exchange = EXCLUDED.exchange;
        """))

        conn.execute(text(f"DROP TABLE IF EXISTS {tmp_table}"))

    return len(df)


# ── 主流程 ───────────────────────────────────────────────────────────────────

def run(
    full: bool = False,
    start_date: str | None = None,
    end_date: str | None = None,
    verify: bool = VERIFY_ENABLED,
    verify_only: bool = False
):
    """
    主执行函数
    :param full: True=全量重拉（从 DEFAULT_START 开始）；False=增量（从 DB 最新日期+1 天开始）
    """
    engine = get_engine()
    ensure_table(engine)

    pro = _init_tushare()

    if verify_only:
        if full:
            start_date = DEFAULT_START
            end_date = datetime.now().strftime('%Y%m%d')
        else:
            start_date = normalize_date(start_date)
            end_date = normalize_date(end_date)
            if start_date or end_date:
                start_date = start_date or end_date
                end_date = end_date or start_date
            else:
                latest = get_latest_date(engine)
                if not latest:
                    logger.info("etf_share_size 无数据，跳过巡检")
                    return
                start_date = latest
                end_date = latest

        if start_date > end_date:
            raise ValueError(f'开始日期不能晚于结束日期: {start_date} > {end_date}')

        run_validation(engine, pro, start_date, end_date)
        return

    start_date, end_date = resolve_fetch_range(
        engine,
        full=full,
        start_date=start_date,
        end_date=end_date
    )
    if not start_date or not end_date:
        return

    total_inserted = 0
    batches = list(daterange(start_date, end_date, BATCH_DAYS))
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

    # 自动触发分类聚合
    if total_inserted > 0:
        logger.info("触发分类聚合...")
        try:
            from src.aggregate_etf_categories import run as run_aggregate
            run_aggregate(full=full, start_date=start_date, end_date=end_date)
        except Exception as exc:
            logger.warning(f"分类聚合失败（不影响主流程）: {exc}")

        if verify:
            range_days = count_range_days(start_date, end_date)
            if range_days > VERIFY_MAX_DAYS:
                logger.info(
                    f"自动巡检已跳过：区间 {start_date} ~ {end_date} 共 {range_days} 天，超过上限 {VERIFY_MAX_DAYS} 天"
                )
            else:
                run_validation(engine, pro, start_date, end_date)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='ETF份额规模数据抓取')
    parser.add_argument('--full', action='store_true', help='全量重拉（从2023-01-01开始）')
    parser.add_argument('--start-date', help='指定开始日期，格式 YYYYMMDD 或 YYYY-MM-DD')
    parser.add_argument('--end-date', help='指定结束日期，格式 YYYYMMDD 或 YYYY-MM-DD')
    parser.add_argument('--skip-verify', action='store_true', help='跳过抓取后的自动巡检')
    parser.add_argument('--verify-only', action='store_true', help='仅执行巡检，不抓取数据')
    args = parser.parse_args()
    run(
        full=args.full,
        start_date=args.start_date,
        end_date=args.end_date,
        verify=not args.skip_verify,
        verify_only=args.verify_only
    )
