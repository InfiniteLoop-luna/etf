"""数据导入脚本 - 将Excel数据导入数据库"""

import os
import sys
import logging
from datetime import datetime
from typing import Optional
import pandas as pd

# 添加src目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.data_loader import load_etf_data

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseImporter:
    """数据库导入器基类"""

    def __init__(self, connection):
        """
        初始化导入器

        Args:
            connection: 数据库连接对象
        """
        self.conn = connection
        self.cursor = connection.cursor()

    def import_from_excel(self, excel_path: str) -> dict:
        """
        从Excel导入数据到数据库

        Args:
            excel_path: Excel文件路径

        Returns:
            导入统计信息字典
        """
        logger.info(f"开始从Excel导入数据: {excel_path}")

        # 1. 加载Excel数据
        df = load_etf_data(excel_path)
        logger.info(f"从Excel加载了 {len(df)} 条记录")

        if len(df) == 0:
            logger.warning("Excel文件中没有数据")
            return {'success': 0, 'updated': 0, 'failed': 0}

        # 2. 导入ETF基本信息
        etf_count = self._import_etf_info(df)
        logger.info(f"导入/更新了 {etf_count} 个ETF基本信息")

        # 3. 导入时间序列数据（使用Upsert）
        stats = self._import_timeseries_data(df)

        logger.info("数据导入完成")
        logger.info(f"  - 新增记录: {stats['inserted']}")
        logger.info(f"  - 更新记录: {stats['updated']}")
        logger.info(f"  - 失败记录: {stats['failed']}")

        return stats

    def _import_etf_info(self, df: pd.DataFrame) -> int:
        """导入ETF基本信息"""
        raise NotImplementedError("子类必须实现此方法")

    def _import_timeseries_data(self, df: pd.DataFrame) -> dict:
        """导入时间序列数据"""
        raise NotImplementedError("子类必须实现此方法")


class SQLiteImporter(DatabaseImporter):
    """SQLite数据库导入器"""

    def _import_etf_info(self, df: pd.DataFrame) -> int:
        """导入ETF基本信息到SQLite"""
        # 获取唯一的ETF列表（排除汇总行）
        etf_df = df[df['is_aggregate'] == False][['code', 'name']].drop_duplicates()

        count = 0
        for _, row in etf_df.iterrows():
            try:
                # 使用INSERT OR REPLACE实现Upsert
                self.cursor.execute("""
                    INSERT OR REPLACE INTO etf_info (code, name, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                """, (row['code'], row['name']))
                count += 1
            except Exception as e:
                logger.error(f"导入ETF信息失败 {row['code']}: {e}")

        self.conn.commit()
        return count

    def _import_timeseries_data(self, df: pd.DataFrame) -> dict:
        """导入时间序列数据到SQLite"""
        stats = {'inserted': 0, 'updated': 0, 'failed': 0}

        for _, row in df.iterrows():
            try:
                # 转换日期格式
                date_str = row['date'].strftime('%Y-%m-%d') if isinstance(row['date'], pd.Timestamp) else str(row['date'])

                # 转换is_aggregate为整数
                is_aggregate = 1 if row['is_aggregate'] else 0

                # 使用INSERT OR REPLACE实现Upsert
                # 先检查是否存在
                self.cursor.execute("""
                    SELECT id FROM etf_timeseries
                    WHERE code = ? AND date = ? AND metric_type = ?
                """, (row['code'], date_str, row['metric_type']))

                exists = self.cursor.fetchone()

                if exists:
                    # 更新现有记录
                    self.cursor.execute("""
                        UPDATE etf_timeseries
                        SET value = ?, is_aggregate = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE code = ? AND date = ? AND metric_type = ?
                    """, (float(row['value']), is_aggregate, row['code'], date_str, row['metric_type']))
                    stats['updated'] += 1
                else:
                    # 插入新记录
                    self.cursor.execute("""
                        INSERT INTO etf_timeseries (code, date, metric_type, value, is_aggregate)
                        VALUES (?, ?, ?, ?, ?)
                    """, (row['code'], date_str, row['metric_type'], float(row['value']), is_aggregate))
                    stats['inserted'] += 1

            except Exception as e:
                logger.error(f"导入数据失败 {row['code']} {date_str} {row['metric_type']}: {e}")
                stats['failed'] += 1

        self.conn.commit()
        return stats


class PostgreSQLImporter(DatabaseImporter):
    """PostgreSQL数据库导入器"""

    def _import_etf_info(self, df: pd.DataFrame) -> int:
        """导入ETF基本信息到PostgreSQL"""
        # 获取唯一的ETF列表（排除汇总行）
        etf_df = df[df['is_aggregate'] == False][['code', 'name']].drop_duplicates()

        count = 0
        for _, row in etf_df.iterrows():
            try:
                # 使用ON CONFLICT实现Upsert
                self.cursor.execute("""
                    INSERT INTO etf_info (code, name, updated_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (code) DO UPDATE
                    SET name = EXCLUDED.name, updated_at = CURRENT_TIMESTAMP
                """, (row['code'], row['name']))
                count += 1
            except Exception as e:
                logger.error(f"导入ETF信息失败 {row['code']}: {e}")

        self.conn.commit()
        return count

    def _import_timeseries_data(self, df: pd.DataFrame) -> dict:
        """导入时间序列数据到PostgreSQL"""
        stats = {'inserted': 0, 'updated': 0, 'failed': 0}

        for _, row in df.iterrows():
            try:
                # 转换日期格式
                date_str = row['date'].strftime('%Y-%m-%d') if isinstance(row['date'], pd.Timestamp) else str(row['date'])

                # 使用ON CONFLICT实现Upsert
                self.cursor.execute("""
                    INSERT INTO etf_timeseries (code, date, metric_type, value, is_aggregate)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (code, date, metric_type) DO UPDATE
                    SET value = EXCLUDED.value,
                        is_aggregate = EXCLUDED.is_aggregate,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING (xmax = 0) AS inserted
                """, (row['code'], date_str, row['metric_type'], float(row['value']), bool(row['is_aggregate'])))

                # xmax = 0 表示是INSERT，否则是UPDATE
                result = self.cursor.fetchone()
                if result and result[0]:
                    stats['inserted'] += 1
                else:
                    stats['updated'] += 1

            except Exception as e:
                logger.error(f"导入数据失败 {row['code']} {date_str} {row['metric_type']}: {e}")
                stats['failed'] += 1

        self.conn.commit()
        return stats


def import_to_sqlite(excel_path: str, db_path: str = 'etf_data.db') -> dict:
    """
    导入数据到SQLite数据库

    Args:
        excel_path: Excel文件路径
        db_path: SQLite数据库文件路径

    Returns:
        导入统计信息
    """
    import sqlite3

    logger.info(f"连接到SQLite数据库: {db_path}")

    # 创建数据库连接
    conn = sqlite3.connect(db_path)

    try:
        # 读取并执行schema
        schema_path = os.path.join(os.path.dirname(__file__), 'schema_sqlite.sql')
        if os.path.exists(schema_path):
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
                conn.executescript(schema_sql)
            logger.info("数据库表结构已创建/更新")

        # 导入数据
        importer = SQLiteImporter(conn)
        stats = importer.import_from_excel(excel_path)

        return stats

    finally:
        conn.close()


def import_to_postgresql(excel_path: str, connection_string: str) -> dict:
    """
    导入数据到PostgreSQL数据库

    Args:
        excel_path: Excel文件路径
        connection_string: PostgreSQL连接字符串

    Returns:
        导入统计信息
    """
    import psycopg2

    logger.info("连接到PostgreSQL数据库")

    # 创建数据库连接
    conn = psycopg2.connect(connection_string)

    try:
        # 读取并执行schema
        schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
        if os.path.exists(schema_path):
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
                cursor = conn.cursor()
                cursor.execute(schema_sql)
                conn.commit()
            logger.info("数据库表结构已创建/更新")

        # 导入数据
        importer = PostgreSQLImporter(conn)
        stats = importer.import_from_excel(excel_path)

        return stats

    finally:
        conn.close()


if __name__ == '__main__':
    # 示例：导入到SQLite
    excel_file = '../主要ETF基金份额变动情况.xlsx'

    if os.path.exists(excel_file):
        stats = import_to_sqlite(excel_file, 'etf_data.db')
        print(f"\n导入完成:")
        print(f"  新增: {stats['inserted']} 条")
        print(f"  更新: {stats['updated']} 条")
        print(f"  失败: {stats['failed']} 条")
    else:
        print(f"错误: 找不到Excel文件 {excel_file}")
