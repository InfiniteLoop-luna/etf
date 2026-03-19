# -*- coding: utf-8 -*-
"""成交量数据获取模块 - 从Tushare获取A股市场每日成交量/成交额"""

import tushare as ts
import pandas as pd
import json
import os
import logging
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# 板块映射
SECTOR_NAMES = {
    '上海A股': 'SH_A',
    '深市主板': 'SZ_MAIN',
    '创业板': 'SZ_GEM',
    '科创板': 'SH_STAR',
}

# 数据文件路径
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
DATA_FILE = os.path.join(DATA_DIR, 'volume_data.json')

# 默认起始日期
DEFAULT_START_DATE = '2024-01-01'


def _init_tushare() -> ts.pro_api:
    """初始化Tushare API，从环境变量或config.yaml读取token"""
    import yaml

    token = os.environ.get('TUSHARE_TOKEN')

    if not token:
        try:
            config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.yaml')
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            token = config.get('data_sources', {}).get('tushare', {}).get('token', '')
        except Exception as e:
            logger.warning(f"读取config.yaml失败: {e}")

    if not token:
        raise ValueError("未找到Tushare token，请设置环境变量 TUSHARE_TOKEN 或在 config.yaml 中配置")

    ts.set_token(token)
    return ts.pro_api()


def fetch_volume_data(start_date: str, end_date: str, pro: Optional[ts.pro_api] = None) -> list:
    """
    从Tushare获取指定日期范围的成交量数据

    Args:
        start_date: 起始日期 YYYY-MM-DD
        end_date: 结束日期 YYYY-MM-DD
        pro: Tushare pro_api实例（可选）

    Returns:
        list of dict: [{"trade_date": "2024-01-02", "ts_name": "上海A股", "amount": 2345.67, "vol": 189.12}, ...]
    """
    if pro is None:
        pro = _init_tushare()

    ts_start = pd.to_datetime(start_date).strftime('%Y%m%d')
    ts_end = pd.to_datetime(end_date).strftime('%Y%m%d')

    logger.info(f"从Tushare获取成交量数据: {start_date} ~ {end_date}")

    all_data = []

    # 分别查询沪深两市
    for exchange in ['SSE', 'SZSE']:
        try:
            df = pro.daily_info(
                exchange=exchange,
                start_date=ts_start,
                end_date=ts_end,
                fields='trade_date,ts_name,ts_code,amount,vol'
            )

            if df is not None and not df.empty:
                # 只保留我们关心的板块
                target_names = set(SECTOR_NAMES.keys())
                df_filtered = df[df['ts_name'].isin(target_names)].copy()

                for _, row in df_filtered.iterrows():
                    trade_date_str = str(row['trade_date'])
                    # 转换日期格式 YYYYMMDD -> YYYY-MM-DD
                    if len(trade_date_str) == 8:
                        formatted_date = f"{trade_date_str[:4]}-{trade_date_str[4:6]}-{trade_date_str[6:]}"
                    else:
                        formatted_date = trade_date_str

                    all_data.append({
                        'trade_date': formatted_date,
                        'ts_name': row['ts_name'],
                        'amount': round(float(row['amount']), 2) if pd.notna(row['amount']) else 0,
                        'vol': round(float(row['vol']), 2) if pd.notna(row['vol']) else 0,
                    })

                logger.info(f"  {exchange}: 获取 {len(df_filtered)} 条记录")
            else:
                logger.warning(f"  {exchange}: 无数据")

        except Exception as e:
            logger.error(f"  {exchange} 获取失败: {e}")

    logger.info(f"共获取 {len(all_data)} 条成交量数据")
    return all_data


def load_existing_data() -> dict:
    """加载现有的JSON数据文件"""
    if not os.path.exists(DATA_FILE):
        return {'last_updated': None, 'data': []}

    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"读取数据文件失败: {e}")
        return {'last_updated': None, 'data': []}


def save_data(data: dict):
    """保存数据到JSON文件"""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"数据已保存至 {DATA_FILE}")


def update_volume_data(full: bool = False, target_date: Optional[str] = None) -> int:
    """
    更新成交量数据（增量或全量）

    Args:
        full: 是否全量更新
        target_date: 指定获取某天的数据 (YYYY-MM-DD)

    Returns:
        int: 新增数据条数
    """
    pro = _init_tushare()

    existing = load_existing_data()
    today = datetime.now().strftime('%Y-%m-%d')

    if target_date:
        # 获取指定日期的数据
        start_date = target_date
        end_date = target_date
        logger.info(f"获取指定日期数据: {target_date}")
    elif full or existing['last_updated'] is None:
        # 全量获取
        start_date = DEFAULT_START_DATE
        end_date = today
        existing['data'] = []  # 清空现有数据
        logger.info(f"全量获取数据: {start_date} ~ {end_date}")
    else:
        # 增量获取：从上次更新的下一天开始
        last_date = datetime.strptime(existing['last_updated'], '%Y-%m-%d')
        start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = today

        if start_date > end_date:
            logger.info("数据已是最新，无需更新")
            return 0

        logger.info(f"增量获取数据: {start_date} ~ {end_date}")

    # 获取新数据
    new_data = fetch_volume_data(start_date, end_date, pro)

    if not new_data:
        logger.info("未获取到新数据")
        return 0

    # 合并数据（去重）
    existing_set = set()
    for item in existing['data']:
        key = (item['trade_date'], item['ts_name'])
        existing_set.add(key)

    added_count = 0
    for item in new_data:
        key = (item['trade_date'], item['ts_name'])
        if key not in existing_set:
            existing['data'].append(item)
            existing_set.add(key)
            added_count += 1

    # 按日期排序
    existing['data'].sort(key=lambda x: (x['trade_date'], x['ts_name']))

    # 更新最后更新日期
    if existing['data']:
        existing['last_updated'] = max(item['trade_date'] for item in existing['data'])

    # 保存
    save_data(existing)

    logger.info(f"新增 {added_count} 条数据，总计 {len(existing['data'])} 条")
    return added_count


def load_volume_dataframe() -> pd.DataFrame:
    """
    加载成交量数据为DataFrame，供前端使用

    Returns:
        DataFrame with columns: trade_date, ts_name, amount, vol
    """
    existing = load_existing_data()

    if not existing['data']:
        return pd.DataFrame(columns=['trade_date', 'ts_name', 'amount', 'vol'])

    df = pd.DataFrame(existing['data'])
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df['vol'] = pd.to_numeric(df['vol'], errors='coerce')
    df = df.sort_values('trade_date')

    return df
