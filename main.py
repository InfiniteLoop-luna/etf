"""ETF数据自动更新主程序"""

from datetime import datetime
import sys
import argparse
import logging
from typing import List

from src.excel_manager import DynamicExcelManager
from src.xlwings_excel_manager import XlwingsExcelManager
from src.data_source_manager import DataSourceManager
from src.data_sources.base import ETFDataSource
from src.data_sources.akshare_source import AkShareSource
from src.data_sources.tushare_source import TushareSource
from src.exceptions import DataFetchError, ExcelUpdateError
from src.utils import setup_logging, ExecutionReport


def load_data_sources() -> List[ETFDataSource]:
    """加载并初始化数据源"""
    import yaml
    import os

    logger = logging.getLogger('etf_updater')

    # 读取配置文件
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        # 如果配置文件读取失败，使用默认配置
        logger.warning(f"无法读取配置文件，使用默认AkShare数据源: {e}")
        return [AkShareSource()]

    sources = []
    data_sources_config = config.get('data_sources', {})

    # 按优先级排序并加载启用的数据源
    enabled_sources = []
    for name, cfg in data_sources_config.items():
        if cfg.get('enabled', False):
            enabled_sources.append((name, cfg.get('priority', 999)))

    # 按优先级排序
    enabled_sources.sort(key=lambda x: x[1])

    logger.info(f"配置的数据源: {[name for name, _ in enabled_sources]}")

    # 创建数据源实例
    for name, _ in enabled_sources:
        try:
            if name == 'akshare':
                sources.append(AkShareSource())
                logger.info(f"✓ 已加载数据源: AkShare (优先级: {data_sources_config[name].get('priority')})")
            elif name == 'tushare':
                # 优先从环境变量读取 token，其次从配置文件读取
                token = os.environ.get('TUSHARE_TOKEN') or data_sources_config['tushare'].get('token')
                timeout = data_sources_config['tushare'].get('timeout', 10)
                if not token:
                    logger.warning(f"✗ Tushare数据源未配置token（需要环境变量 TUSHARE_TOKEN 或 config.yaml 中的 token），跳过")
                    continue
                sources.append(TushareSource(token=token, timeout=timeout))
                logger.info(f"✓ 已加载数据源: Tushare (优先级: {data_sources_config[name].get('priority')})")
            # 可以在这里添加其他数据源
            # elif name == 'eastmoney_scraper':
            #     ...
        except Exception as e:
            logger.warning(f"✗ 加载数据源 {name} 失败: {e}")

    if not sources:
        raise ValueError("没有启用的数据源！请在config.yaml中至少启用一个数据源。")

    return sources


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
        # 在CI环境使用openpyxl，本地使用xlwings以完美保留Excel格式
        import os
        is_ci = os.environ.get('CI') == 'true' or os.environ.get('GITHUB_ACTIONS') == 'true'

        if is_ci:
            logger.info("检测到CI环境，使用openpyxl管理器")
            excel_manager = DynamicExcelManager('主要ETF基金份额变动情况.xlsx')
        else:
            logger.info("使用xlwings管理器以完美保留Excel格式")
            excel_manager = XlwingsExcelManager('主要ETF基金份额变动情况.xlsx')

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

        # 6. 保存Excel并关闭
        logger.info("正在保存Excel文件...")
        excel_manager.save()
        excel_manager.close()
        logger.info("✓ Excel文件保存成功")

        if not is_ci:
            # xlwings会完美保留所有Excel格式和公式
            logger.info("✓ 使用xlwings保存，所有Excel格式和公式已完整保留")

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
