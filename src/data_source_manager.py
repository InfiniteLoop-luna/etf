"""数据源管理器 - 实现优先级fallback机制"""

import logging
from typing import List
from .data_sources.base import ETFDataSource
from .exceptions import DataFetchError


class DataSourceManager:
    """数据源管理器，实现优先级fallback"""

    def __init__(self, sources: List[ETFDataSource]):
        self.sources = sources  # 按优先级排序
        self.logger = logging.getLogger(__name__)

    def fetch_data(self, code: str, date: str) -> dict:
        """
        按优先级尝试获取数据，自动fallback

        Returns:
            {'market_value': float, 'unit_price': float}

        Raises:
            DataFetchError: 所有数据源都失败
        """
        for source in self.sources:
            try:
                self.logger.info(f"尝试使用 {source.__class__.__name__}")
                data = source.get_etf_data(code, date)
                self.logger.info(f"✓ {source.__class__.__name__} 成功")
                return data
            except Exception as e:
                self.logger.warning(
                    f"✗ {source.__class__.__name__} 失败: {e}"
                )
                continue

        raise DataFetchError(f"所有数据源获取 {code} 数据失败")

    def is_trading_day(self, date: str) -> bool:
        """检查是否为交易日（使用第一个可用的数据源）"""
        for source in self.sources:
            try:
                return source.is_trading_day(date)
            except Exception:
                continue
        # 如果所有数据源都失败，假设是交易日（保守策略）
        self.logger.warning("无法确定交易日，假设为交易日")
        return True
