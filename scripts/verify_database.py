"""验证数据库更新是否成功"""

import os
import sys
import logging
from datetime import datetime, timedelta

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def verify_database():
    """验证数据库数据"""

    print("=" * 60)
    print("Database Verification")
    print("=" * 60)
    print()

    # 1. 检查环境变量
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL not set")
        print("[ERROR] DATABASE_URL not found")
        sys.exit(1)

    # 2. 连接数据库
    try:
        import psycopg2
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        print("[OK] Database connection successful")
        print()

    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        print(f"[ERROR] Cannot connect to database: {e}")
        sys.exit(1)

    try:
        # 3. 检查ETF数量
        cursor.execute("SELECT COUNT(*) FROM etf_info")
        etf_count = cursor.fetchone()[0]
        print(f"ETF count: {etf_count}")

        if etf_count == 0:
            logger.error("No ETFs found in database")
            print("[ERROR] No ETFs in database")
            sys.exit(1)

        # 4. 检查时间序列记录数
        cursor.execute("SELECT COUNT(*) FROM etf_timeseries")
        record_count = cursor.fetchone()[0]
        print(f"Timeseries records: {record_count}")

        if record_count == 0:
            logger.error("No timeseries data found")
            print("[ERROR] No timeseries data in database")
            sys.exit(1)

        # 5. 检查日期范围
        cursor.execute("SELECT MIN(date), MAX(date) FROM etf_timeseries")
        min_date, max_date = cursor.fetchone()
        print(f"Date range: {min_date} to {max_date}")

        # 6. 检查最新数据日期
        today = datetime.now().date()
        days_old = (today - max_date).days

        print(f"Latest data: {days_old} days old")

        if days_old > 7:
            logger.warning(f"Data is {days_old} days old")
            print(f"[WARNING] Data might be outdated ({days_old} days old)")

        # 7. 检查指标类型
        cursor.execute("SELECT COUNT(DISTINCT metric_type) FROM etf_timeseries")
        metric_count = cursor.fetchone()[0]
        print(f"Metric types: {metric_count}")

        if metric_count == 0:
            logger.error("No metric types found")
            print("[ERROR] No metric types in database")
            sys.exit(1)

        # 8. 检查最近更新的记录
        cursor.execute("""
            SELECT COUNT(*) FROM etf_timeseries
            WHERE updated_at >= NOW() - INTERVAL '1 hour'
        """)
        recent_updates = cursor.fetchone()[0]
        print(f"Records updated in last hour: {recent_updates}")

        print()
        print("=" * 60)
        print("[SUCCESS] Database verification passed!")
        print("=" * 60)
        print()
        print("Summary:")
        print(f"  - {etf_count} ETFs")
        print(f"  - {record_count} timeseries records")
        print(f"  - {metric_count} metric types")
        print(f"  - Latest data: {max_date}")
        print(f"  - Recent updates: {recent_updates} records")
        print()

    except Exception as e:
        logger.error(f"Verification failed: {e}", exc_info=True)
        print()
        print("[ERROR] Verification failed!")
        print(f"Error: {e}")
        sys.exit(1)

    finally:
        conn.close()


if __name__ == '__main__':
    verify_database()
