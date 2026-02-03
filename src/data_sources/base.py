"""数据源抽象基类"""

from abc import ABC, abstractmethod


class ETFDataSource(ABC):
    """ETF数据源抽象基类"""

    @abstractmethod
    def get_etf_data(self, code: str, date: str) -> dict:
        """
        获取ETF数据

        Args:
            code: ETF代码（如 SH510300）
            date: 日期（格式：YYYY-MM-DD）

        Returns:
            {'market_value': float, 'unit_price': float}

        Raises:
            DataSourceError: 数据获取失败
        """
        pass

    @abstractmethod
    def is_trading_day(self, date: str) -> bool:
        """
        检查是否为交易日

        Args:
            date: 日期（格式：YYYY-MM-DD）

        Returns:
            bool: True表示是交易日
        """
        pass
