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
    failed_etfs: List[Dict[str, str]] = field(default_factory=list)
    skipped_reason: str = None

    def add_success(self, code: str):
        """记录成功"""
        self.success_count += 1

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

        if self.failed_etfs:
            print(f"失败: {len(self.failed_etfs)} 个ETF")
            for item in self.failed_etfs:
                print(f"  - {item['code']}: {item['error']}")
        else:
            print("OK: 所有ETF更新成功！")

        print(f"{'='*60}\n")
