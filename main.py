"""ETF数据自动更新主程序"""

from datetime import datetime
import sys
import argparse
from typing import List

from src.excel_manager import DynamicExcelManager
from src.data_source_manager import DataSourceManager
from src.data_sources.base import ETFDataSource
from src.data_sources.akshare_source import AkShareSource
from src.exceptions import DataFetchError, ExcelUpdateError
from src.utils import setup_logging, ExecutionReport


def load_data_sources() -> List[ETFDataSource]:
    """加载并初始化数据源"""
    # 目前只实现AkShare数据源
    # 后续可以添加更多数据源
    return [
        AkShareSource()
    ]


def main(target_date: str = None) -> int:
    """
    主程序入口

    Args:
        target_date: 目标日期（YYYY-MM-DD），None表示今天

    Returns:
        int: 退出码（0=成功，1=部分失败，2=Excel错误，3=未知错误）
    """
    logger = setup_logging()
    report = ExecutionReport()

    try:
        # 1. 确定目标日期
        date = target_date or datetime.now().strftime('%Y-%m-%d')
        logger.info(f"{'='*60}")
        logger.info(f"ETF数据更新程序启动")
        logger.info(f"目标日期: {date}")
        logger.info(f"{'='*60}")

        # 2. 检查是否为交易日
        source_manager = DataSourceManager(load_data_sources())
        if not source_manager.is_trading_day(date):
            report.skipped_reason = f"{date} 不是交易日"
            logger.info(report.skipped_reason)
            report.print_summary()
            return 0

        logger.info(f"{date} 是交易日，开始更新数据")

        # 3. 初始化Excel管理器
        excel_manager = DynamicExcelManager('主要ETF基金份额变动情况.xlsx')

        # 4. 获取ETF列表
        etf_codes = excel_manager.get_etf_codes()
        logger.info(f"发现 {len(etf_codes)} 个ETF需要更新")

        # 5. 逐个更新ETF
        for idx, code in enumerate(etf_codes, 1):
            try:
                logger.info(f"[{idx}/{len(etf_codes)}] 正在获取 {code} 的数据...")

                # 获取数据
                data = source_manager.fetch_data(code, date)

                # 更新Excel
                excel_manager.update_data(
                    code=code,
                    date=date,
                    market_value=data['market_value'],
                    unit_price=data['unit_price']
                )

                report.add_success(code)
                logger.info(f"✓ {code} 更新成功")

            except DataFetchError as e:
                report.add_failure(code, str(e))
                logger.error(f"✗ {code} 数据获取失败: {e}")
                continue
            except Exception as e:
                report.add_failure(code, f"未知错误: {e}")
                logger.exception(f"✗ {code} 更新时发生异常")
                continue

        # 6. 保存Excel
        logger.info("正在保存Excel文件...")
        excel_manager.save()
        logger.info("✓ Excel文件保存成功")

        # 7. 打印报告
        report.print_summary()

        return 0 if not report.failed_etfs else 1

    except ExcelUpdateError as e:
        logger.error(f"Excel操作失败: {e}")
        return 2
    except Exception as e:
        logger.exception(f"未预期的错误: {e}")
        return 3


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ETF数据自动更新程序')
    parser.add_argument(
        '--date',
        type=str,
        help='目标日期（YYYY-MM-DD），默认为今天'
    )

    args = parser.parse_args()
    sys.exit(main(args.date))
