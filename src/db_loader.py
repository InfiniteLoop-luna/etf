"""数据库数据加载器 - 从数据库读取ETF数据"""

import os
import logging
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)


class DatabaseLoader:
    """数据库加载器基类"""

    def __init__(self, connection):
        """
        初始化加载器

        Args:
            connection: 数据库连接对象
        """
        self.conn = connection

    def load_etf_data(self) -> pd.DataFrame:
        """
        从数据库加载ETF数据

        Returns:
            DataFrame with columns: code, name, date, metric_type, value, is_aggregate
        """
        query = """
            SELECT
                code,
                (SELECT name FROM etf_info WHERE etf_info.code = etf_timeseries.code) as name,
                date,
                metric_type,
                value,
                is_aggregate
            FROM etf_timeseries
            ORDER BY date, code, metric_type
        """

        try:
            df = pd.read_sql_query(query, self.conn)

            # 转换数据类型
            df['date'] = pd.to_datetime(df['date'])
            df['value'] = pd.to_numeric(df['value'])

            # SQLite使用0/1表示布尔值，需要转换
            if 'is_aggregate' in df.columns:
                df['is_aggregate'] = df['is_aggregate'].astype(bool)

            logger.info(f"从数据库加载了 {len(df)} 条记录")
            return df

        except Exception as e:
            logger.error(f"从数据库加载数据失败: {e}")
            raise

    def get_etf_list(self) -> pd.DataFrame:
        """
        获取ETF列表

        Returns:
            DataFrame with columns: code, name
        """
        query = "SELECT code, name FROM etf_info ORDER BY code"

        try:
            df = pd.read_sql_query(query, self.conn)
            return df
        except Exception as e:
            logger.error(f"获取ETF列表失败: {e}")
            raise

    def get_date_range(self) -> tuple:
        """
        获取数据的日期范围

        Returns:
            (min_date, max_date) tuple
        """
        query = "SELECT MIN(date) as min_date, MAX(date) as max_date FROM etf_timeseries"

        try:
            df = pd.read_sql_query(query, self.conn)
            return (df['min_date'].iloc[0], df['max_date'].iloc[0])
        except Exception as e:
            logger.error(f"获取日期范围失败: {e}")
            raise


def create_sqlite_connection(db_path: str = 'etf_data.db'):
    """
    创建SQLite数据库连接

    Args:
        db_path: 数据库文件路径

    Returns:
        数据库连接对象
    """
    import sqlite3

    if not os.path.exists(db_path):
        raise FileNotFoundError(f"数据库文件不存在: {db_path}")

    conn = sqlite3.connect(db_path, check_same_thread=False)
    return conn


def create_postgresql_connection(connection_string: Optional[str] = None):
    """
    创建PostgreSQL数据库连接

    Args:
        connection_string: 连接字符串，如果为None则从环境变量读取

    Returns:
        数据库连接对象
    """
    import psycopg2

    if connection_string is None:
        # 从环境变量读取
        connection_string = os.getenv('DATABASE_URL')

        if connection_string is None:
            raise ValueError("未提供数据库连接字符串，且环境变量DATABASE_URL未设置")

    conn = psycopg2.connect(connection_string)
    return conn


def load_data_from_database(db_type: str = 'sqlite', **kwargs) -> pd.DataFrame:
    """
    从数据库加载数据的便捷函数

    Args:
        db_type: 数据库类型 ('sqlite' 或 'postgresql')
        **kwargs: 传递给连接函数的参数

    Returns:
        DataFrame with ETF data
    """
    if db_type == 'sqlite':
        db_path = kwargs.get('db_path', 'etf_data.db')
        conn = create_sqlite_connection(db_path)
    elif db_type == 'postgresql':
        connection_string = kwargs.get('connection_string')
        conn = create_postgresql_connection(connection_string)
    else:
        raise ValueError(f"不支持的数据库类型: {db_type}")

    try:
        loader = DatabaseLoader(conn)
        df = loader.load_etf_data()
        return df
    finally:
        conn.close()
