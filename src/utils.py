"""日志配置和执行报告"""

import logging
from logging.handlers import RotatingFileHandler
import os
from dataclasses import dataclass, field
from typing import List, Dict


def setup_logging(log_level: str = 'INFO') -> logging.Logger:
    """配置日志系统"""
    logger = logging.getLogger('etf_updater')
    logger.setLevel(getattr(logging, log_level.upper()))

    # 创建logs目录
    os.makedirs('logs', exist_ok=True)

    # 文件日志（轮转，保留10个文件，每个10MB）
    file_handler = RotatingFileHandler(
        'logs/etf_updater.log',
        maxBytes=10*1024*1024,
        backupCount=10,
        encoding='utf-8'
    )
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))

    # 控制台日志
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(
        '%(levelname)s: %(message)s'
    ))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


@dataclass
class ExecutionReport:
    """执行结果报告"""
    success_count: int = 0
    success_etfs: List[Dict[str, any]] = field(default_factory=list)
    failed_etfs: List[Dict[str, str]] = field(default_factory=list)
    skipped_reason: str = None

    def add_success(self, code: str, name: str = None, date: str = None,
                   market_value: float = None, unit_price: float = None,
                   prev_market_value: float = None, prev_unit_price: float = None):
        """记录成功"""
        self.success_count += 1
        self.success_etfs.append({
            'code': code,
            'name': name,
            'date': date,
            'market_value': market_value,
            'unit_price': unit_price,
            'prev_market_value': prev_market_value,
            'prev_unit_price': prev_unit_price
        })

    def add_failure(self, code: str, error: str):
        """记录失败"""
        self.failed_etfs.append({'code': code, 'error': error})

    def print_summary(self):
        """打印执行摘要"""
        if self.skipped_reason:
            print(f"\n✓ 跳过执行: {self.skipped_reason}\n")
            return

        print(f"\n{'='*60}")
        print(f"执行完成")
        print(f"{'='*60}")
        print(f"成功更新: {self.success_count} 个ETF")

        if self.success_etfs:
            print(f"\n详细信息:")
            for item in self.success_etfs:
                print(f"\n  {item['code']} - {item['name']}")
                print(f"    更新日期: {item['date']}")
                if item['market_value'] is not None and item['unit_price'] is not None:
                    print(f"    当日数据: 总市值={item['market_value']:.2f}亿元, 单位净值={item['unit_price']:.4f}元")
                if item['prev_market_value'] is not None and item['prev_unit_price'] is not None:
                    print(f"    前日数据: 总市值={item['prev_market_value']:.2f}亿元, 单位净值={item['prev_unit_price']:.4f}元")
                    # 计算变动
                    mv_change = item['market_value'] - item['prev_market_value']
                    price_change = item['unit_price'] - item['prev_unit_price']
                    price_change_pct = (price_change / item['prev_unit_price'] * 100) if item['prev_unit_price'] != 0 else 0
                    print(f"    变动情况: 市值变动={mv_change:+.2f}亿元, 净值变动={price_change:+.4f}元 ({price_change_pct:+.2f}%)")

        if self.failed_etfs:
            print(f"\n失败: {len(self.failed_etfs)} 个ETF")
            for item in self.failed_etfs:
                print(f"  - {item['code']}: {item['error']}")
        else:
            print("\nOK: 所有ETF更新成功！")

        print(f"{'='*60}\n")
