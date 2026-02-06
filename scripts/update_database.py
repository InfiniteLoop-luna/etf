"""GitHub Actions数据库更新脚本"""

import os
import sys
import logging
from datetime import datetime

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from database.import_data import import_to_postgresql

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """主函数：从Excel导入数据到PostgreSQL"""

    print("=" * 60)
    print("GitHub Actions - Database Update")
    print("=" * 60)
    print()

    # 1. 检查环境变量
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL environment variable not set")
        print("[ERROR] DATABASE_URL not found in environment variables")
        print("Please set DATABASE_URL in GitHub Secrets")
        sys.exit(1)

    logger.info("Database URL found")
    print("[OK] Database connection configured")
    print()

    # 2. 检查Excel文件
    excel_file = '主要ETF基金份额变动情况.xlsx'
    if not os.path.exists(excel_file):
        logger.error(f"Excel file not found: {excel_file}")
        print(f"[ERROR] Excel file not found: {excel_file}")
        sys.exit(1)

    file_size = os.path.getsize(excel_file) / 1024  # KB
    logger.info(f"Excel file found: {excel_file} ({file_size:.1f} KB)")
    print(f"[OK] Excel file found: {excel_file} ({file_size:.1f} KB)")
    print()

    # 3. 导入数据到数据库
    try:
        print("Starting data import to PostgreSQL...")
        print()

        stats = import_to_postgresql(excel_file, database_url)

        print()
        print("=" * 60)
        print("[SUCCESS] Database update completed!")
        print("=" * 60)
        print(f"Inserted: {stats['inserted']} records")
        print(f"Updated: {stats['updated']} records")
        print(f"Failed: {stats['failed']} records")
        print()

        # 4. 记录更新时间
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Database updated at: {update_time}")
        print(f"Update time: {update_time}")
        print()

        # 5. 检查是否有失败记录
        if stats['failed'] > 0:
            logger.warning(f"Warning: {stats['failed']} records failed to import")
            print(f"[WARNING] {stats['failed']} records failed")
            # 不退出，因为部分成功也是可以接受的

        # 6. 输出总结
        total_records = stats['inserted'] + stats['updated']
        logger.info(f"Total records processed: {total_records}")
        print(f"Total records in database: {total_records}")
        print()
        print("[SUCCESS] All done!")

    except Exception as e:
        logger.error(f"Database update failed: {e}", exc_info=True)
        print()
        print("=" * 60)
        print("[ERROR] Database update failed!")
        print("=" * 60)
        print(f"Error: {e}")
        print()
        print("Please check:")
        print("1. DATABASE_URL is correct")
        print("2. Database is accessible")
        print("3. Excel file format is correct")
        sys.exit(1)


if __name__ == '__main__':
    main()
