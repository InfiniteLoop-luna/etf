"""ETF数据更新系统的异常定义"""


class ETFDataError(Exception):
    """ETF数据相关的基础异常"""
    pass


class DataSourceError(ETFDataError):
    """数据源获取失败"""
    pass


class DataFetchError(DataSourceError):
    """所有数据源都失败"""
    pass


class TradingDayError(ETFDataError):
    """非交易日错误"""
    pass


class ExcelUpdateError(ETFDataError):
    """Excel更新失败"""
    pass
