#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
自选股主力出货预警自动更新脚本
该脚本会扫描 app_user_watchlist 中的所有股票，
基于数据库日线缓存和深度报告同一套日线信号口径分析是否有出货迹象，并将结果写入数据库预警表。
"""
from __future__ import annotations

import os
import sys
import time
import warnings

warnings.filterwarnings("ignore")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

def inject_env_from_secrets():
    """从 .streamlit/secrets.toml 加载数据库配置到环境变量"""
    secrets_path = os.path.join(PROJECT_ROOT, ".streamlit", "secrets.toml")
    if not os.path.exists(secrets_path):
        return
    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib
        except ImportError:
            mapping = {}
            with open(secrets_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if "=" in line and not line.startswith("#"):
                        k, v = line.split("=", 1)
                        mapping[k.strip()] = v.strip().strip('"').strip("'")
            for key in ["ETF_PG_PASSWORD", "PGPASSWORD", "ETF_PG_HOST",
                        "ETF_PG_USER", "ETF_PG_DATABASE", "ETF_PG_URL",
                        "DATABASE_URL", "TUSHARE_TOKEN"]:
                if key in mapping and not os.environ.get(key):
                    os.environ[key] = mapping[key]
            return

    with open(secrets_path, "rb") as f:
        secrets = tomllib.load(f)

    pg_keys = {
        "ETF_PG_PASSWORD": secrets.get("ETF_PG_PASSWORD") or secrets.get("PGPASSWORD"),
        "ETF_PG_HOST": secrets.get("ETF_PG_HOST"),
        "ETF_PG_USER": secrets.get("ETF_PG_USER"),
        "ETF_PG_DATABASE": secrets.get("ETF_PG_DATABASE"),
        "ETF_PG_URL": secrets.get("ETF_PG_URL") or secrets.get("DATABASE_URL"),
        "TUSHARE_TOKEN": secrets.get("TUSHARE_TOKEN"),
    }
    for key, val in pg_keys.items():
        if val and not os.environ.get(key):
            os.environ[key] = str(val)

# 注入环境变量
inject_env_from_secrets()

from src.distribution_alert_store import upsert_alerts, get_engine
from src.distribution_analyzer import (
    analyze_daily_kline,
    build_distribution_alert_payload,
    fetch_daily_kline,
)
from src.watchlist_distribution_refresh import get_latest_source_trade_date, load_watchlist_stock_symbols


def fetch_and_analyze(ts_code: str, engine) -> dict:
    """分析单只股票并返回预警信息"""
    latest_trade_date = get_latest_source_trade_date(engine, ts_code)
    if not latest_trade_date:
        return {}

    kline = fetch_daily_kline(
        ts_code,
        engine=engine,
        allow_live_fetch=False,
        end_date=latest_trade_date,
    )
    if kline is None or kline.empty:
        return {}

    analyzed = analyze_daily_kline(kline)
    return build_distribution_alert_payload(ts_code, analyzed, report_trade_date=latest_trade_date) or {}


def main():
    print("="*60)
    print("🚀 开始进行自选股主力出货预警分析...")
    print("="*60)
    
    engine = get_engine()
    
    try:
        ts_codes = load_watchlist_stock_symbols(engine)
    except Exception as e:
        print(f"获取自选股失败: {e}")
        return
        
    if not ts_codes:
        print("自选股列表为空。")
        return
        
    print(f"共发现 {len(ts_codes)} 只自选股需要分析。")
    
    alerts_to_upsert = []

    for idx, ts_code in enumerate(ts_codes, 1):
        print(f"[{idx}/{len(ts_codes)}] 分析 {ts_code} ...", end="", flush=True)
        alert = fetch_and_analyze(ts_code, engine)

        if not alert:
            print(" 跳过 (无数据)")
            continue

        if alert["alert_level"] != "NONE":
            print(f" ⚠️ 发现信号: {alert['alert_level']} - {', '.join(alert['alert_details']['signals'])}")
        else:
            print(" ✅ 正常")

        alerts_to_upsert.append(alert)
        time.sleep(0.05)
            
    # 写入数据库
    if alerts_to_upsert:
        upsert_alerts(alerts_to_upsert, engine=engine)
        print(f"成功将 {len(alerts_to_upsert)} 条分析记录写入数据库。")
    
    print("="*60)
    print("🎉 预警分析完成！")
    print("="*60)

if __name__ == "__main__":
    main()
