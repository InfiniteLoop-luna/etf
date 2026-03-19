# -*- coding: utf-8 -*-
"""成交量数据更新入口脚本"""

import argparse
import logging
import sys

from src.volume_fetcher import update_volume_data


def main():
    parser = argparse.ArgumentParser(description='A股每日成交量数据更新')
    parser.add_argument('--full', action='store_true', help='全量重新获取2024年至今的数据')
    parser.add_argument('--date', type=str, help='指定获取某天数据（YYYY-MM-DD）')
    args = parser.parse_args()

    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    try:
        logger.info("=" * 50)
        logger.info("A股每日成交量数据更新程序启动")
        logger.info("=" * 50)

        count = update_volume_data(full=args.full, target_date=args.date)

        if count > 0:
            logger.info(f"✓ 更新完成，新增 {count} 条数据")
        else:
            logger.info("✓ 数据已是最新，无需更新")

        return 0

    except Exception as e:
        logger.error(f"✗ 更新失败: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
