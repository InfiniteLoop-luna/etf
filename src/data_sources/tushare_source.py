"""Tushare数据源实现"""

import tushare as ts
import pandas as pd
import logging
from datetime import datetime
from .base import ETFDataSource
from ..exceptions import DataSourceError


class TushareSource(ETFDataSource):
    """使用Tushare API获取ETF数据"""

    def __init__(self, token: str, timeout: int = 10):
        """
        初始化Tushare数据源

        Args:
            token: Tushare API token
            timeout: 超时时间（秒）
        """
        if not token:
            raise ValueError("Tushare token不能为空")

        self.token = token
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)

        # 初始化Tushare API
        try:
            ts.set_token(token)
            self.pro = ts.pro_api()
            self.logger.info("Tushare API初始化成功")
        except Exception as e:
            raise DataSourceError(f"Tushare API初始化失败: {e}")

    def get_etf_data(self, code: str, date: str) -> dict:
        """
        从Tushare获取ETF数据

        Args:
            code: ETF代码（如 SH510300）
            date: 日期（格式：YYYY-MM-DD）

        Returns:
            {'market_value': float, 'unit_price': float}
        """
        try:
            # 转换代码格式为Tushare格式
            # SH510300 -> 510300.SH
            # SZ159919 -> 159919.SZ
            if code.startswith('SH'):
                ts_code = f"{code[2:]}.SH"
            elif code.startswith('SZ'):
                ts_code = f"{code[2:]}.SZ"
            else:
                ts_code = code

            # 转换日期格式 YYYY-MM-DD -> YYYYMMDD
            trade_date = pd.to_datetime(date).strftime('%Y%m%d')

            # 获取ETF数据（使用日期范围查询，然后筛选）
            # 获取目标日期前后一段时间的数据
            start_date = (pd.to_datetime(date) - pd.Timedelta(days=7)).strftime('%Y%m%d')
            end_date = (pd.to_datetime(date) + pd.Timedelta(days=1)).strftime('%Y%m%d')

            df = self.pro.fund_daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )

            if df.empty:
                raise DataSourceError(f"未找到 {code} 的任何数据")

            # 筛选指定日期的数据
            df_filtered = df[df['trade_date'] == trade_date]

            if df_filtered.empty:
                raise DataSourceError(f"未找到 {code} 在 {date} 的数据")

            row = df_filtered.iloc[0]

            # 获取单位净值（收盘价）
            unit_price = float(row['close'])

            # 尝试获取总市值
            market_value = None

            # 方法1: 如果API返回了total_mv字段（总市值，单位：万元）
            if 'total_mv' in row and pd.notna(row['total_mv']):
                market_value = float(row['total_mv']) / 10000  # 转换为亿元
                self.logger.info(f"{code} {date}: 从API获取总市值={market_value:.2f}亿元")
            else:
                # 方法2: 尝试通过份额数据计算
                try:
                    # 获取最近的份额数据（通常是季度数据）
                    share_df = self.pro.fund_share(
                        ts_code=ts_code,
                        end_date=trade_date
                    )

                    if not share_df.empty:
                        # 取最近的份额数据
                        latest_share = share_df.iloc[0]
                        # 基金份额（Tushare返回的是万份，需要转换为亿份）
                        fd_share = float(latest_share['fd_share'])  # 单位：万份
                        fund_share = fd_share / 10000  # 转换为亿份

                        # 计算总市值（亿元）= 单位净值 × 基金份额
                        market_value = unit_price * fund_share

                        self.logger.info(
                            f"{code} {date}: 单位净值={unit_price:.4f}, "
                            f"份额={fund_share:.2f}亿份, 总市值={market_value:.2f}亿元"
                        )
                except Exception as e:
                    self.logger.warning(f"获取 {code} 份额数据失败: {e}")

            if market_value is None:
                self.logger.warning(f"{code} 无法获取总市值数据")

            return {
                'market_value': market_value,
                'unit_price': unit_price
            }

        except DataSourceError:
            raise
        except Exception as e:
            raise DataSourceError(f"Tushare获取数据失败: {e}")

    def is_trading_day(self, date: str) -> bool:
        """
        检查是否为交易日

        Args:
            date: 日期（格式：YYYY-MM-DD）

        Returns:
            bool: True表示是交易日
        """
        try:
            # 转换日期格式
            trade_date = pd.to_datetime(date).strftime('%Y%m%d')

            # 获取交易日历
            df = self.pro.trade_cal(
                exchange='SSE',
                start_date=trade_date,
                end_date=trade_date
            )

            if df.empty:
                return False

            # 检查is_open字段
            return bool(df.iloc[0]['is_open'])

        except Exception as e:
            raise DataSourceError(f"Tushare检查交易日失败: {e}")
