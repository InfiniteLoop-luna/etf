# -*- coding: utf-8 -*-
"""
冰轮环境 (000811.SZ) 主力出货分析脚本 (mootdx 版)
=============================================
纯使用 mootdx 获取数据，无需数据库连接。

分析方法：
1. 通过 mootdx 获取日K线数据 (client.bars)
2. 通过 mootdx 获取分笔成交数据 (client.transactions) 分析大单行为
3. 通过 mootdx 获取分时数据 (client.minutes) 分析盘中走势
4. 综合量价、大单流向判断主力出货时间

主力出货特征：
  - 放量滞涨或放量下跌
  - 高位缩量上涨（量价背离）
  - 分时图高开低走、脉冲式放量
  - 大单卖出明显增加
"""
from __future__ import annotations

import os
import sys
import time
import warnings
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")

# ---- 配置 ----
TS_CODE = "000811.SZ"
STOCK_NAME = "冰轮环境"
SYMBOL = "000811"
MARKET_CODE = 0  # 0=深圳, 1=上海

# ============================================================================
# mootdx 客户端
# ============================================================================

def create_client():
    """创建 mootdx 客户端"""
    from mootdx.quotes import Quotes
    client = Quotes.factory(market='std', timeout=10, heartbeat=True, auto_retry=True)
    return client


def close_client(client):
    """关闭 mootdx 客户端"""
    try:
        if client:
            client.close()
    except Exception:
        pass


# ============================================================================
# 第一部分：日K线分析
# ============================================================================

def fetch_daily_kline(client, offset=200):
    """获取日K线数据 (frequency=9 = 日K线)"""
    print(f"\n📊 获取日K线数据...")
    try:
        bars = client.bars(symbol=SYMBOL, frequency=9, offset=offset)
        if bars is not None and not bars.empty:
            print(f"   ✅ 获取 {len(bars)} 条日K线")
            return bars
        print("   ⚠️ 无数据")
        return pd.DataFrame()
    except Exception as exc:
        print(f"   ❌ 获取失败: {exc}")
        return pd.DataFrame()


def analyze_daily_kline(df: pd.DataFrame) -> pd.DataFrame:
    """
    分析日K线，计算量价指标
    """
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    for col in ["open", "close", "high", "low", "vol", "amount"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 涨跌幅
    df["pct_change"] = df["close"].pct_change() * 100

    # 成交量均线
    df["vol_ma5"] = df["vol"].rolling(5).mean()
    df["vol_ma10"] = df["vol"].rolling(10).mean()
    df["vol_ma20"] = df["vol"].rolling(20).mean()

    # 量比 (当日成交量 / 5日均量)
    df["vol_ratio"] = df["vol"] / df["vol_ma5"]

    # 价格均线
    df["ma5"] = df["close"].rolling(5).mean()
    df["ma10"] = df["close"].rolling(10).mean()
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma60"] = df["close"].rolling(60).mean()

    # 振幅
    df["amplitude"] = (df["high"] - df["low"]) / df["close"].shift(1) * 100

    # 上影线比率（长上影线可能是出货信号）
    body = abs(df["close"] - df["open"])
    upper_shadow = df["high"] - df[["open", "close"]].max(axis=1)
    total_range = df["high"] - df["low"]
    df["upper_shadow_ratio"] = np.where(total_range > 0, upper_shadow / total_range, 0)

    # 换手率近似 (需要流通股本，此处用成交量变化率替代)
    df["vol_change_pct"] = df["vol"].pct_change() * 100

    return df


def find_volume_price_signals(df: pd.DataFrame) -> dict:
    """
    寻找量价异动信号
    返回各类信号列表
    """
    signals = {
        "放量滞涨": [],         # 成交量放大但涨幅很小 -> 主力对倒/出货
        "放量下跌": [],         # 放量下跌 -> 主力砸盘出货
        "天量天价": [],         # 成交量创阶段新高且股价也在高位 -> 出货启动
        "量价背离_顶部": [],    # 价格上涨但成交量持续萎缩 -> 上涨乏力
        "高位长上影": [],       # 高位长上影线 -> 上方抛压重
        "破位下跌": [],         # 跌破重要均线
        "连续缩量阴跌": [],     # 出货后的自然回落
    }

    if df.empty or len(df) < 20:
        return signals

    # 计算阶段高点
    df["rolling_high_20"] = df["high"].rolling(20).max()
    df["rolling_high_60"] = df["high"].rolling(60).max()
    df["near_high"] = df["high"] >= df["rolling_high_20"] * 0.97

    for idx in range(20, len(df)):
        row = df.iloc[idx]
        date_label = str(row.name) if hasattr(row.name, '__str__') else str(idx)

        # 放量滞涨：量比>1.5 且 涨幅<1%
        if row["vol_ratio"] > 1.5 and abs(row["pct_change"]) < 1.0 and row.get("near_high", False):
            signals["放量滞涨"].append({
                "日期": date_label,
                "涨跌幅": f"{row['pct_change']:+.2f}%",
                "量比": f"{row['vol_ratio']:.2f}",
                "收盘": f"{row['close']:.2f}",
            })

        # 放量下跌：量比>1.5 且 跌幅>2%
        if row["vol_ratio"] > 1.5 and row["pct_change"] < -2.0:
            signals["放量下跌"].append({
                "日期": date_label,
                "涨跌幅": f"{row['pct_change']:+.2f}%",
                "量比": f"{row['vol_ratio']:.2f}",
                "收盘": f"{row['close']:.2f}",
            })

        # 天量天价：成交量创20日新高 且 价格在20日高位附近
        if (row["vol"] >= df["vol"].iloc[max(0,idx-20):idx+1].max() * 0.95 and
            row.get("near_high", False)):
            signals["天量天价"].append({
                "日期": date_label,
                "涨跌幅": f"{row['pct_change']:+.2f}%",
                "成交量": f"{row['vol']:,.0f}",
                "收盘": f"{row['close']:.2f}",
            })

        # 高位长上影线：上影线>50% 且 在近期高点附近
        if row["upper_shadow_ratio"] > 0.5 and row.get("near_high", False):
            signals["高位长上影"].append({
                "日期": date_label,
                "涨跌幅": f"{row['pct_change']:+.2f}%",
                "上影线比率": f"{row['upper_shadow_ratio']:.1%}",
                "收盘": f"{row['close']:.2f}",
            })

        # 破位下跌：跌破MA20
        if idx >= 1:
            prev = df.iloc[idx-1]
            if (prev["close"] > prev["ma20"] and
                row["close"] < row["ma20"] and
                row["pct_change"] < -1):
                signals["破位下跌"].append({
                    "日期": date_label,
                    "涨跌幅": f"{row['pct_change']:+.2f}%",
                    "MA20": f"{row['ma20']:.2f}",
                    "收盘": f"{row['close']:.2f}",
                })

    # 量价背离_顶部：连续3天价格上涨但成交量递减
    for idx in range(22, len(df)):
        if (df.iloc[idx]["pct_change"] > 0 and
            df.iloc[idx-1]["pct_change"] > 0 and
            df.iloc[idx-2]["pct_change"] > 0 and
            df.iloc[idx]["vol"] < df.iloc[idx-1]["vol"] < df.iloc[idx-2]["vol"] and
            df.iloc[idx].get("near_high", False)):
            signals["量价背离_顶部"].append({
                "日期": str(df.iloc[idx].name),
                "连续涨幅": f"{df.iloc[idx-2:idx+1]['pct_change'].sum():+.2f}%",
                "量缩幅度": f"{(1 - df.iloc[idx]['vol']/df.iloc[idx-2]['vol'])*100:.1f}%",
                "收盘": f"{df.iloc[idx]['close']:.2f}",
            })

    # 连续缩量阴跌：连续3天下跌且成交量递减
    for idx in range(22, len(df)):
        if (df.iloc[idx]["pct_change"] < 0 and
            df.iloc[idx-1]["pct_change"] < 0 and
            df.iloc[idx-2]["pct_change"] < 0 and
            df.iloc[idx]["vol_ratio"] < 0.8):
            signals["连续缩量阴跌"].append({
                "日期": str(df.iloc[idx].name),
                "连续跌幅": f"{df.iloc[idx-2:idx+1]['pct_change'].sum():.2f}%",
                "量比": f"{df.iloc[idx]['vol_ratio']:.2f}",
                "收盘": f"{df.iloc[idx]['close']:.2f}",
            })

    return signals


def identify_distribution_phase(df: pd.DataFrame) -> list[dict]:
    """
    识别出货阶段:
    通过识别"见顶"后的持续下跌趋势来判断出货窗口
    """
    if df.empty or len(df) < 30:
        return []

    phases = []

    # 找到阶段性高点（20日最高点）
    df["is_local_peak"] = False
    for idx in range(20, len(df) - 5):
        window = df.iloc[max(0, idx-10):min(len(df), idx+11)]
        if df.iloc[idx]["high"] == window["high"].max():
            # 确认后续有下跌
            after = df.iloc[idx+1:min(len(df), idx+11)]
            if len(after) >= 3 and after["pct_change"].mean() < 0:
                df.iloc[idx, df.columns.get_loc("is_local_peak")] = True

    # 对每个阶段性高点，找出后续的出货下跌阶段
    peak_indices = df[df["is_local_peak"]].index.tolist()

    for peak_pos in peak_indices:
        peak_idx = df.index.get_loc(peak_pos) if peak_pos in df.index else None
        if peak_idx is None:
            continue

        peak_row = df.iloc[peak_idx]
        peak_price = peak_row["high"]
        peak_date = str(peak_row.name)

        # 向后扫描，直到价格反弹超过3%或到达数据末尾
        end_idx = peak_idx + 1
        lowest_close = peak_row["close"]
        total_vol = 0

        while end_idx < len(df):
            current = df.iloc[end_idx]
            if current["close"] < lowest_close:
                lowest_close = current["close"]
            total_vol += current["vol"]

            # 如果从低点反弹超过3%，认为出货阶段结束
            if current["close"] > lowest_close * 1.03 and end_idx - peak_idx >= 3:
                break
            # 最多追踪40个交易日
            if end_idx - peak_idx >= 40:
                break
            end_idx += 1

        decline_pct = (lowest_close - peak_price) / peak_price * 100
        duration = end_idx - peak_idx

        if duration >= 3 and decline_pct < -3:
            end_row = df.iloc[min(end_idx, len(df)-1)]
            phases.append({
                "peak_date": peak_date,
                "peak_price": peak_price,
                "end_date": str(end_row.name),
                "low_price": lowest_close,
                "decline_pct": decline_pct,
                "duration_days": duration,
                "avg_vol": total_vol / max(duration, 1),
            })

    # 按跌幅排序
    phases.sort(key=lambda x: x["decline_pct"])
    return phases


# ============================================================================
# 第二部分：分笔成交（tick）分析 - 大单追踪
# ============================================================================

def fetch_transactions(client, trade_date_str: str, max_count=2000):
    """
    获取指定日期的分笔成交数据
    client.transactions(symbol, start, offset, date)
    """
    try:
        all_data = []
        offset = max_count
        start = 0
        raw = client.transactions(symbol=SYMBOL, start=start, offset=offset, date=trade_date_str)
        if raw is not None and not raw.empty:
            all_data.append(raw)

        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            return result
        return pd.DataFrame()
    except Exception as exc:
        # 非交易时间可能返回空
        return pd.DataFrame()


def analyze_tick_data(tick_df: pd.DataFrame) -> dict:
    """
    分析分笔成交数据，识别大单卖出
    分笔数据通常包含: time, price, vol, buyorsell
    buyorsell: 0=买入, 1=卖出, 2=不明
    """
    if tick_df is None or tick_df.empty:
        return {"status": "no_data"}

    df = tick_df.copy()

    # 尝试识别列名
    col_map = {}
    for col in df.columns:
        col_lower = str(col).lower()
        if "price" in col_lower:
            col_map["price"] = col
        elif "vol" in col_lower:
            col_map["vol"] = col
        elif "buy" in col_lower or "sell" in col_lower or "direction" in col_lower:
            col_map["direction"] = col

    if "price" not in col_map or "vol" not in col_map:
        return {"status": "columns_not_found", "columns": list(df.columns)}

    price_col = col_map["price"]
    vol_col = col_map["vol"]
    dir_col = col_map.get("direction")

    df["_price"] = pd.to_numeric(df[price_col], errors="coerce")
    df["_vol"] = pd.to_numeric(df[vol_col], errors="coerce")

    total_vol = df["_vol"].sum()
    total_amount = (df["_price"] * df["_vol"]).sum()
    avg_price = total_amount / total_vol if total_vol > 0 else 0

    # 大单定义：单笔成交量 > 500手 或 金额 > 50万
    df["_amount"] = df["_price"] * df["_vol"] * 100  # 换算为元
    big_order_mask = (df["_vol"] >= 500) | (df["_amount"] >= 500000)
    big_orders = df[big_order_mask]

    big_vol = big_orders["_vol"].sum()
    big_pct = big_vol / total_vol * 100 if total_vol > 0 else 0

    result = {
        "status": "ok",
        "total_ticks": len(df),
        "total_vol": total_vol,
        "avg_price": avg_price,
        "big_order_count": len(big_orders),
        "big_order_vol": big_vol,
        "big_order_pct": big_pct,
    }

    # 如果有买卖方向
    if dir_col:
        df["_dir"] = pd.to_numeric(df[dir_col], errors="coerce")
        buy_vol = df[df["_dir"] == 0]["_vol"].sum()
        sell_vol = df[df["_dir"] == 1]["_vol"].sum()
        result["buy_vol"] = buy_vol
        result["sell_vol"] = sell_vol
        result["net_vol"] = buy_vol - sell_vol
        result["sell_ratio"] = sell_vol / total_vol * 100 if total_vol > 0 else 50

        # 大单中的买卖分布
        if not big_orders.empty:
            big_orders_copy = big_orders.copy()
            big_orders_copy["_dir"] = pd.to_numeric(big_orders_copy[dir_col], errors="coerce")
            big_buy = big_orders_copy[big_orders_copy["_dir"] == 0]["_vol"].sum()
            big_sell = big_orders_copy[big_orders_copy["_dir"] == 1]["_vol"].sum()
            result["big_buy_vol"] = big_buy
            result["big_sell_vol"] = big_sell
            result["big_net"] = big_buy - big_sell
            result["big_sell_ratio"] = big_sell / big_vol * 100 if big_vol > 0 else 50

    return result


# ============================================================================
# 第三部分：分时数据分析
# ============================================================================

def fetch_minutes(client, trade_date_str: str):
    """获取1分钟分时数据"""
    try:
        data = client.minutes(symbol=SYMBOL, date=trade_date_str)
        if data is not None and not data.empty:
            return data
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def analyze_intraday(minutes_df: pd.DataFrame, date_str: str) -> dict:
    """分析分时数据出货特征"""
    if minutes_df is None or minutes_df.empty:
        return {"date": date_str, "patterns": ["无数据"]}

    df = minutes_df.copy()

    # 识别价格和成交量列
    price_col = None
    vol_col = None
    for col in df.columns:
        cl = str(col).lower()
        if "price" in cl or col == "close":
            price_col = col
        elif "vol" in cl:
            vol_col = col

    if not price_col:
        return {"date": date_str, "patterns": ["无法识别价格列"]}

    prices = pd.to_numeric(df[price_col], errors="coerce").dropna()
    vols = pd.to_numeric(df[vol_col], errors="coerce").dropna() if vol_col else pd.Series(dtype=float)

    if len(prices) < 10:
        return {"date": date_str, "patterns": ["数据点不足"]}

    open_p = prices.iloc[0]
    close_p = prices.iloc[-1]
    high_p = prices.max()
    low_p = prices.min()
    day_ret = (close_p - open_p) / open_p * 100 if open_p > 0 else 0

    # 高点位置
    high_idx = prices.values.argmax()
    high_pos = high_idx / len(prices)

    patterns = []

    # 高开低走
    if high_pos < 0.25 and day_ret < -1:
        patterns.append("⚠️ 高开低走（典型出货形态）")

    # 早盘拉高后持续回落
    if high_pos < 0.15 and day_ret < 0:
        patterns.append("⚠️ 早盘拉高后回落（诱多出货）")

    # 尾盘跳水
    tail_n = min(30, len(prices))
    tail_ret = (prices.iloc[-1] - prices.iloc[-tail_n]) / prices.iloc[-tail_n] * 100 if len(prices) > tail_n else 0
    if tail_ret < -0.8:
        patterns.append("⚠️ 尾盘跳水")

    # 前半段放量后半段缩量
    if len(vols) > 20:
        mid = len(vols) // 2
        v1 = vols.iloc[:mid].sum()
        v2 = vols.iloc[mid:].sum()
        if v2 > 0 and v1 / v2 > 1.8 and day_ret < 0:
            patterns.append("⚠️ 前半段放量后半段缩量（出货撤退）")

    # 脉冲放量
    if len(vols) > 10:
        vmean = vols.mean()
        spikes = (vols > vmean * 3).sum()
        if spikes >= 5:
            patterns.append(f"⚠️ 多次脉冲放量({spikes}次)（可能对倒出货）")

    if not patterns:
        if day_ret < -1:
            patterns.append("📉 下跌")
        elif day_ret > 1:
            patterns.append("📈 上涨")
        else:
            patterns.append("➖ 窄幅震荡")

    return {
        "date": date_str,
        "open": round(open_p, 2),
        "close": round(close_p, 2),
        "high": round(high_p, 2),
        "low": round(low_p, 2),
        "day_return": round(day_ret, 2),
        "high_position": round(high_pos, 2),
        "patterns": patterns,
    }


# ============================================================================
# 主执行流程
# ============================================================================

def main():
    print(f"\n{'='*70}")
    print(f"  {STOCK_NAME} ({TS_CODE}) 主力出货分析")
    print(f"  分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  数据来源: mootdx (通达信协议)")
    print(f"{'='*70}")

    client = create_client()

    try:
        # ========== STEP 1: 日K线分析 ==========
        print(f"\n{'─'*70}")
        print("📈 Step 1: 日K线量价分析")
        print(f"{'─'*70}")

        kline = fetch_daily_kline(client, offset=200)
        if kline.empty:
            print("❌ 无法获取日K线数据，退出分析")
            return

        analyzed = analyze_daily_kline(kline)

        # 打印近期K线概况
        print(f"\n📊 近期行情概况（最近30个交易日）：\n")
        recent = analyzed.tail(30)
        for _, row in recent.iterrows():
            date_str = str(row.name)
            pct = row["pct_change"]
            vol_r = row["vol_ratio"]
            arrow = "🟢" if pct >= 0 else "🔴"
            vol_tag = ""
            if vol_r > 2:
                vol_tag = " 📢放量"
            elif vol_r > 1.5:
                vol_tag = " 📈量增"
            elif vol_r < 0.6:
                vol_tag = " 📉缩量"

            shadow_tag = ""
            if row["upper_shadow_ratio"] > 0.5:
                shadow_tag = " ⛳长上影"

            print(f"  {date_str}  "
                  f"开{row['open']:>7.2f}  高{row['high']:>7.2f}  低{row['low']:>7.2f}  收{row['close']:>7.2f}  "
                  f"{arrow}{pct:>+6.2f}%  量比{vol_r:>5.2f}{vol_tag}{shadow_tag}")

        # ========== STEP 2: 量价信号识别 ==========
        print(f"\n{'─'*70}")
        print("🚨 Step 2: 量价异动信号识别")
        print(f"{'─'*70}")

        signals = find_volume_price_signals(analyzed)

        for signal_name, items in signals.items():
            if items:
                print(f"\n  ⚠️ {signal_name}（{len(items)} 个信号）：")
                for item in items[-10:]:  # 只显示最近10个
                    details = " | ".join(f"{k}: {v}" for k, v in item.items())
                    print(f"    • {details}")

        total_signals = sum(len(v) for v in signals.values())
        if total_signals == 0:
            print("\n  ℹ️ 未发现明显的量价异动信号")

        # ========== STEP 3: 出货阶段识别 ==========
        print(f"\n{'─'*70}")
        print("📅 Step 3: 出货阶段识别")
        print(f"{'─'*70}")

        phases = identify_distribution_phase(analyzed)
        if not phases:
            print("\n  ℹ️ 未识别到明显的出货阶段")
        else:
            print(f"\n  ⚠️ 发现 {len(phases)} 个可疑出货/下跌阶段：\n")
            for i, phase in enumerate(phases, 1):
                print(f"  阶段 {i}:")
                print(f"    📍 见顶日: {phase['peak_date']}  最高价: {phase['peak_price']:.2f}")
                print(f"    📍 结束日: {phase['end_date']}  最低价: {phase['low_price']:.2f}")
                print(f"    📉 跌幅: {phase['decline_pct']:.2f}%  持续: {phase['duration_days']} 个交易日")
                print(f"    📊 日均成交: {phase['avg_vol']:,.0f}")
                print()

        # ========== STEP 4: 分笔成交（大单分析） ==========
        print(f"\n{'─'*70}")
        print("💰 Step 4: 关键日期分笔成交分析（大单追踪）")
        print(f"{'─'*70}")

        # 选取有信号的日期 + 出货阶段的起始日
        target_dates = set()

        # 从量价信号中收集日期
        for signal_name, items in signals.items():
            for item in items[-5:]:
                target_dates.add(item["日期"])

        # 从出货阶段中收集日期
        for phase in phases[:3]:
            target_dates.add(phase["peak_date"])

        # 添加最近5个交易日
        for _, row in analyzed.tail(5).iterrows():
            target_dates.add(str(row.name))

        # 只保留看起来是日期格式的
        valid_dates = []
        for d in sorted(target_dates):
            d_clean = str(d).replace("-", "")
            if len(d_clean) >= 8 and d_clean[:8].isdigit():
                valid_dates.append(d_clean[:8])

        if valid_dates:
            print(f"\n  分析 {len(valid_dates)} 个关键日期的分笔成交...\n")
            tick_results = {}
            for dt in sorted(valid_dates)[-15:]:  # 最多分析15天
                tick_df = fetch_transactions(client, dt)
                if not tick_df.empty:
                    result = analyze_tick_data(tick_df)
                    tick_results[dt] = result
                    if result["status"] == "ok":
                        sell_info = ""
                        if "sell_ratio" in result:
                            sell_info = f"  卖出占比: {result['sell_ratio']:.1f}%"
                            if "big_sell_ratio" in result:
                                sell_info += f"  大单卖出占比: {result['big_sell_ratio']:.1f}%"
                            if "big_net" in result:
                                net_tag = "🟢" if result["big_net"] > 0 else "🔴"
                                sell_info += f"  大单净量: {net_tag}{result['big_net']:+.0f}"
                        print(f"  {dt}: 总成交{result['total_vol']:>10,.0f}手  "
                              f"大单{result['big_order_count']:>4}笔({result['big_order_pct']:.1f}%)"
                              f"{sell_info}")
                    elif result["status"] == "columns_not_found":
                        print(f"  {dt}: 列名: {result.get('columns', [])}")
                else:
                    print(f"  {dt}: 无分笔数据")
                time.sleep(0.3)
        else:
            print("\n  ℹ️ 没有需要分析的目标日期")

        # ========== STEP 5: 分时数据分析 ==========
        print(f"\n{'─'*70}")
        print("⏱️  Step 5: 关键日期分时走势分析")
        print(f"{'─'*70}")

        if valid_dates:
            print(f"\n  分析 {min(len(valid_dates), 15)} 个关键日期的分时数据...\n")
            for dt in sorted(valid_dates)[-15:]:
                mins_df = fetch_minutes(client, dt)
                if not mins_df.empty:
                    result = analyze_intraday(mins_df, dt)
                    patterns_str = " | ".join(result["patterns"])
                    price_info = ""
                    if "day_return" in result:
                        price_info = (f"开{result['open']:.2f} 收{result['close']:.2f} "
                                      f"涨跌{result['day_return']:+.2f}% "
                                      f"高点位置{result['high_position']:.0%}")
                    print(f"  {dt}: {price_info}")
                    print(f"    🏷️ {patterns_str}")
                else:
                    print(f"  {dt}: 无分时数据")
                time.sleep(0.3)

        # ========== STEP 6: 综合结论 ==========
        print(f"\n{'='*70}")
        print("📋 综合分析结论")
        print(f"{'='*70}\n")

        # 汇总所有出货信号
        distribution_evidence = []

        if signals["天量天价"]:
            latest = signals["天量天价"][-1]
            distribution_evidence.append(f"🔸 天量天价信号: {latest['日期']} (量={latest['成交量']}, 收盘={latest['收盘']})")

        if signals["放量滞涨"]:
            for item in signals["放量滞涨"][-3:]:
                distribution_evidence.append(f"🔸 放量滞涨: {item['日期']} (量比={item['量比']}, 涨跌={item['涨跌幅']})")

        if signals["放量下跌"]:
            for item in signals["放量下跌"][-3:]:
                distribution_evidence.append(f"🔸 放量下跌: {item['日期']} (量比={item['量比']}, 涨跌={item['涨跌幅']})")

        if signals["高位长上影"]:
            for item in signals["高位长上影"][-3:]:
                distribution_evidence.append(f"🔸 高位长上影: {item['日期']} (上影比={item['上影线比率']})")

        if signals["量价背离_顶部"]:
            for item in signals["量价背离_顶部"][-3:]:
                distribution_evidence.append(f"🔸 量价背离: {item['日期']} (量缩={item['量缩幅度']})")

        if signals["破位下跌"]:
            for item in signals["破位下跌"][-3:]:
                distribution_evidence.append(f"🔸 破均线: {item['日期']} (跌破MA20={item['MA20']})")

        if distribution_evidence:
            print("  🚨 出货信号汇总：\n")
            for ev in distribution_evidence:
                print(f"    {ev}")

        if phases:
            print(f"\n  📅 最可能的主力出货窗口：\n")
            for i, phase in enumerate(phases[:3], 1):
                print(f"    {i}. {phase['peak_date']} ~ {phase['end_date']}")
                print(f"       从 {phase['peak_price']:.2f} 跌至 {phase['low_price']:.2f}，"
                      f"跌幅 {phase['decline_pct']:.2f}%，持续 {phase['duration_days']} 天")
        else:
            print("\n  ✅ 未发现明显的阶段性出货行为")

        # 总结
        if not distribution_evidence and not phases:
            print("\n  📊 结论: 当前未发现明显的主力集中出货迹象")
        elif len(distribution_evidence) >= 3 or (phases and phases[0]["decline_pct"] < -10):
            print(f"\n  📊 结论: ⚠️ 存在较强的主力出货迹象，建议谨慎关注")
        else:
            print(f"\n  📊 结论: 存在一些出货信号，但尚不构成明确的系统性出货，需持续跟踪")

        print(f"\n{'='*70}")
        print("  分析完成！")
        print(f"{'='*70}\n")

    finally:
        close_client(client)


if __name__ == "__main__":
    main()
