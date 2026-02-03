"""AkShare数据源实现"""

import akshare as ak
import pandas as pd
from datetime import datetime
from .base import ETFDataSource
from ..exceptions import DataSourceError


class AkShareSource(ETFDataSource):
    """使用AkShare库获取ETF数据"""

    def get_etf_data(self, code: str, date: str) -> dict:
        """
        从AkShare获取ETF数据

        Args:
            code: ETF代码（如 SH510300）
            date: 日期（格式：YYYY-MM-DD）

        Returns:
            {'market_value': float, 'unit_price': float}
        """
        try:
            # 移除代码前缀，AkShare使用纯数字代码
            symbol = code.replace('SH', '').replace('SZ', '')

            # 获取ETF历史数据
            df = ak.fund_etf_hist_sina(symbol=symbol)

            # 查找指定日期的数据
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            target_data = df[df['date'] == date]

            if target_data.empty:
                raise DataSourceError(f"未找到 {code} 在 {date} 的数据")

            row = target_data.iloc[0]

            # 计算总市值（亿元）= 收盘价 * 成交量 / 100000000
            # 注意：这里的计算可能需要根据实际数据调整
            close_price = float(row['close'])
            volume = float(row['volume'])
            market_value = (close_price * volume) / 100000000

            return {
                'market_value': market_value,
                'unit_price': close_price
            }

        except Exception as e:
            raise DataSourceError(f"AkShare获取数据失败: {e}")

    def is_trading_day(self, date: str) -> bool:
        """
        检查是否为交易日

        Args:
            date: 日期（格式：YYYY-MM-DD）

        Returns:
            bool: True表示是交易日
        """
        try:
            # 获取交易日历
            df = ak.tool_trade_date_hist_sina()
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')

            return date in df['trade_date'].values

        except Exception as e:
            raise DataSourceError(f"AkShare检查交易日失败: {e}")
