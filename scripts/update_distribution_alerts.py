#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
自选股主力出货预警自动更新脚本
该脚本会扫描 app_user_watchlist 中的所有股票，
利用 mootdx 获取行情并分析是否有出货迹象，并将结果写入数据库预警表。
"""
from __future__ import annotations

import os
import sys
import time
import warnings
from datetime import datetime

import pandas as pd
import numpy as np

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
from sqlalchemy import text


# ============================================================================
# Mootdx 分析逻辑 (简化版)
# ============================================================================

def create_client():
    from mootdx.quotes import Quotes
    return Quotes.factory(market='std', timeout=10, heartbeat=True, auto_retry=True)

def analyze_daily_kline(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    for col in ["open", "close", "high", "low", "vol", "amount"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["pct_change"] = df["close"].pct_change() * 100
    df["vol_ma5"] = df["vol"].rolling(5).mean()
    df["vol_ratio"] = df["vol"] / df["vol_ma5"]
    
    total_range = df["high"] - df["low"]
    upper_shadow = df["high"] - df[["open", "close"]].max(axis=1)
    df["upper_shadow_ratio"] = np.where(total_range > 0, upper_shadow / total_range, 0)
    
    df["rolling_high_20"] = df["high"].rolling(20).max()
    df["near_high"] = df["high"] >= df["rolling_high_20"] * 0.97
    return df

def find_signals(df: pd.DataFrame) -> list[str]:
    signals = []
    if df.empty or len(df) < 5:
        return signals
        
    last_row = df.iloc[-1]
    
    # 放量下跌
    if last_row["vol_ratio"] > 1.5 and last_row["pct_change"] < -2.0:
        signals.append(f"放量下跌(量比{last_row['vol_ratio']:.1f}, 跌{last_row['pct_change']:.1f}%)")
        
    # 放量滞涨
    if last_row["vol_ratio"] > 1.5 and abs(last_row["pct_change"]) < 1.0 and last_row.get("near_high", False):
        signals.append(f"高位放量滞涨(量比{last_row['vol_ratio']:.1f})")
        
    # 天量天价
    if last_row["vol"] >= df["vol"].tail(20).max() * 0.95 and last_row.get("near_high", False):
        signals.append(f"天量天价")
        
    # 高位长上影
    if last_row["upper_shadow_ratio"] > 0.5 and last_row.get("near_high", False):
        signals.append(f"高位长上影")
        
    # 量价背离 (最近三天)
    if len(df) >= 3:
        if (df.iloc[-1]["pct_change"] > 0 and 
            df.iloc[-2]["pct_change"] > 0 and 
            df.iloc[-1]["vol"] < df.iloc[-2]["vol"] < df.iloc[-3]["vol"] and
            last_row.get("near_high", False)):
            signals.append("顶部量价背离")
            
    return signals

def fetch_and_analyze(ts_code: str, client) -> dict:
    """分析单只股票并返回预警信息"""
    symbol = ts_code.split('.')[0]
    
    try:
        kline = client.bars(symbol=symbol, frequency=9, offset=60)
    except Exception as e:
        print(f"[{ts_code}] 获取K线失败: {e}")
        return {}
        
    if kline is None or kline.empty:
        return {}
        
    analyzed = analyze_daily_kline(kline)
    signals = find_signals(analyzed)
    
    # 只取最后一天的数据
    last_row = analyzed.iloc[-1]
    trade_date = str(last_row.name).split()[0] if hasattr(last_row, 'name') else datetime.now().strftime("%Y-%m-%d")
    
    # 判断预警级别
    alert_level = "NONE"
    if len(signals) >= 2 or any("放量下跌" in s or "量价背离" in s for s in signals):
        alert_level = "HIGH"
    elif len(signals) == 1:
        alert_level = "MEDIUM"
        
    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "alert_level": alert_level,
        "alert_details": {
            "signals": signals,
            "close": float(last_row["close"]),
            "pct_change": float(last_row["pct_change"]),
            "vol_ratio": float(last_row["vol_ratio"])
        }
    }


def main():
    print("="*60)
    print("🚀 开始进行自选股主力出货预警分析...")
    print("="*60)
    
    engine = get_engine()
    
    # 1. 获取所有用户的自选股 (去重)
    sql = "SELECT DISTINCT ts_code FROM app_user_watchlist WHERE security_type = 'stock'"
    try:
        with engine.connect() as conn:
            df_stocks = pd.read_sql(text(sql), conn)
            ts_codes = df_stocks['ts_code'].tolist()
    except Exception as e:
        print(f"获取自选股失败: {e}")
        return
        
    if not ts_codes:
        print("自选股列表为空。")
        return
        
    print(f"共发现 {len(ts_codes)} 只自选股需要分析。")
    
    client = create_client()
    alerts_to_upsert = []
    
    try:
        for idx, ts_code in enumerate(ts_codes, 1):
            print(f"[{idx}/{len(ts_codes)}] 分析 {ts_code} ...", end="", flush=True)
            alert = fetch_and_analyze(ts_code, client)
            
            if not alert:
                print(" 跳过 (无数据)")
                continue
                
            if alert["alert_level"] != "NONE":
                print(f" ⚠️ 发现信号: {alert['alert_level']} - {', '.join(alert['alert_details']['signals'])}")
            else:
                print(" ✅ 正常")
                
            alerts_to_upsert.append(alert)
            time.sleep(0.2)  # 防止请求过快
            
    finally:
        try:
            client.close()
        except:
            pass
            
    # 写入数据库
    if alerts_to_upsert:
        upsert_alerts(alerts_to_upsert, engine=engine)
        print(f"成功将 {len(alerts_to_upsert)} 条分析记录写入数据库。")
    
    print("="*60)
    print("🎉 预警分析完成！")
    print("="*60)

if __name__ == "__main__":
    main()
