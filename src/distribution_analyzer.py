# -*- coding: utf-8 -*-
"""
主力出货深度分析模块
供前端按需调用，生成 Markdown 格式的深度分析报告。
"""
from __future__ import annotations

import time
import warnings
from datetime import datetime

import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")

# ============================================================================
# mootdx 客户端
# ============================================================================

def create_client():
    from mootdx.quotes import Quotes
    client = Quotes.factory(market='std', timeout=10, heartbeat=True, auto_retry=True)
    return client

def close_client(client):
    try:
        if client:
            client.close()
    except Exception:
        pass

# ============================================================================
# 分析函数复用
# ============================================================================

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
    df["ma20"] = df["close"].rolling(20).mean()

    body = abs(df["close"] - df["open"])
    upper_shadow = df["high"] - df[["open", "close"]].max(axis=1)
    total_range = df["high"] - df["low"]
    df["upper_shadow_ratio"] = np.where(total_range > 0, upper_shadow / total_range, 0)
    
    return df

def find_volume_price_signals(df: pd.DataFrame) -> dict:
    signals = {
        "放量滞涨": [], "放量下跌": [], "天量天价": [],
        "量价背离_顶部": [], "高位长上影": [], "破位下跌": [], "连续缩量阴跌": []
    }
    if df.empty or len(df) < 20:
        return signals

    df["rolling_high_20"] = df["high"].rolling(20).max()
    df["near_high"] = df["high"] >= df["rolling_high_20"] * 0.97

    for idx in range(20, len(df)):
        row = df.iloc[idx]
        date_label = str(row.name).split()[0] if hasattr(row.name, '__str__') else str(idx)

        if row["vol_ratio"] > 1.5 and abs(row["pct_change"]) < 1.0 and row.get("near_high", False):
            signals["放量滞涨"].append((date_label, row['pct_change'], row['vol_ratio'], row['close']))
        if row["vol_ratio"] > 1.5 and row["pct_change"] < -2.0:
            signals["放量下跌"].append((date_label, row['pct_change'], row['vol_ratio'], row['close']))
        if row["vol"] >= df["vol"].iloc[max(0,idx-20):idx+1].max() * 0.95 and row.get("near_high", False):
            signals["天量天价"].append((date_label, row['pct_change'], row['vol'], row['close']))
        if row["upper_shadow_ratio"] > 0.5 and row.get("near_high", False):
            signals["高位长上影"].append((date_label, row['pct_change'], row['upper_shadow_ratio'], row['close']))
        if idx >= 1:
            prev = df.iloc[idx-1]
            if prev["close"] > prev["ma20"] and row["close"] < row["ma20"] and row["pct_change"] < -1:
                signals["破位下跌"].append((date_label, row['pct_change'], row['ma20'], row['close']))

    for idx in range(22, len(df)):
        date_label = str(df.iloc[idx].name).split()[0]
        if (df.iloc[idx]["pct_change"] > 0 and df.iloc[idx-1]["pct_change"] > 0 and df.iloc[idx-2]["pct_change"] > 0 and
            df.iloc[idx]["vol"] < df.iloc[idx-1]["vol"] < df.iloc[idx-2]["vol"] and df.iloc[idx].get("near_high", False)):
            signals["量价背离_顶部"].append((date_label, df.iloc[idx-2:idx+1]['pct_change'].sum(), (1 - df.iloc[idx]['vol']/df.iloc[idx-2]['vol'])*100, df.iloc[idx]['close']))

    return signals

def identify_distribution_phase(df: pd.DataFrame) -> list[dict]:
    if df.empty or len(df) < 30:
        return []
    phases = []
    df["is_local_peak"] = False
    for idx in range(20, len(df) - 5):
        window = df.iloc[max(0, idx-10):min(len(df), idx+11)]
        if df.iloc[idx]["high"] == window["high"].max():
            after = df.iloc[idx+1:min(len(df), idx+11)]
            if len(after) >= 3 and after["pct_change"].mean() < 0:
                df.iloc[idx, df.columns.get_loc("is_local_peak")] = True

    peak_indices = df[df["is_local_peak"]].index.tolist()
    for peak_pos in peak_indices:
        peak_idx = df.index.get_loc(peak_pos) if peak_pos in df.index else None
        if peak_idx is None: continue
        peak_row = df.iloc[peak_idx]
        peak_price = peak_row["high"]
        peak_date = str(peak_row.name).split()[0]

        end_idx = peak_idx + 1
        lowest_close = peak_row["close"]
        total_vol = 0
        while end_idx < len(df):
            current = df.iloc[end_idx]
            if current["close"] < lowest_close:
                lowest_close = current["close"]
            total_vol += current["vol"]
            if current["close"] > lowest_close * 1.03 and end_idx - peak_idx >= 3:
                break
            if end_idx - peak_idx >= 40:
                break
            end_idx += 1

        decline_pct = (lowest_close - peak_price) / peak_price * 100
        duration = end_idx - peak_idx
        if duration >= 3 and decline_pct < -3:
            end_row = df.iloc[min(end_idx, len(df)-1)]
            phases.append({
                "peak_date": peak_date, "peak_price": peak_price,
                "end_date": str(end_row.name).split()[0], "low_price": lowest_close,
                "decline_pct": decline_pct, "duration_days": duration,
                "avg_vol": total_vol / max(duration, 1),
            })
    phases.sort(key=lambda x: x["decline_pct"])
    return phases

def fetch_transactions(client, symbol: str, trade_date_str: str, max_count=2000):
    try:
        raw = client.transactions(symbol=symbol, start=0, offset=max_count, date=trade_date_str)
        if raw is not None and not raw.empty:
            return raw
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

def analyze_tick_data(tick_df: pd.DataFrame) -> dict:
    if tick_df is None or tick_df.empty: return {"status": "no_data"}
    df = tick_df.copy()
    col_map = {}
    for col in df.columns:
        cl = str(col).lower()
        if "price" in cl: col_map["price"] = col
        elif "vol" in cl: col_map["vol"] = col
        elif "buy" in cl or "sell" in cl or "direction" in cl: col_map["direction"] = col

    if "price" not in col_map or "vol" not in col_map: return {"status": "no_data"}
    df["_price"] = pd.to_numeric(df[col_map["price"]], errors="coerce")
    df["_vol"] = pd.to_numeric(df[col_map["vol"]], errors="coerce")
    total_vol = df["_vol"].sum()
    df["_amount"] = df["_price"] * df["_vol"] * 100
    big_orders = df[(df["_vol"] >= 500) | (df["_amount"] >= 500000)]
    big_vol = big_orders["_vol"].sum()
    
    res = {
        "status": "ok", "total_vol": total_vol,
        "big_order_count": len(big_orders), "big_pct": big_vol / total_vol * 100 if total_vol > 0 else 0,
    }
    if "direction" in col_map:
        df["_dir"] = pd.to_numeric(df[col_map["direction"]], errors="coerce")
        res["sell_ratio"] = df[df["_dir"] == 1]["_vol"].sum() / total_vol * 100 if total_vol > 0 else 50
        if not big_orders.empty:
            big_orders["_dir"] = pd.to_numeric(big_orders[col_map["direction"]], errors="coerce")
            big_buy = big_orders[big_orders["_dir"] == 0]["_vol"].sum()
            big_sell = big_orders[big_orders["_dir"] == 1]["_vol"].sum()
            res["big_net"] = big_buy - big_sell
            res["big_sell_ratio"] = big_sell / big_vol * 100 if big_vol > 0 else 50
    return res

def fetch_minutes(client, symbol: str, trade_date_str: str):
    try:
        data = client.minutes(symbol=symbol, date=trade_date_str)
        if data is not None and not data.empty: return data
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

def analyze_intraday(minutes_df: pd.DataFrame, date_str: str) -> dict:
    if minutes_df is None or minutes_df.empty: return {"date": date_str, "patterns": ["无数据"]}
    df = minutes_df.copy()
    price_col, vol_col = None, None
    for col in df.columns:
        cl = str(col).lower()
        if "price" in cl or col == "close": price_col = col
        elif "vol" in cl: vol_col = col

    if not price_col: return {"date": date_str, "patterns": ["无法识别价格列"]}
    prices = pd.to_numeric(df[price_col], errors="coerce").dropna()
    vols = pd.to_numeric(df[vol_col], errors="coerce").dropna() if vol_col else pd.Series(dtype=float)
    if len(prices) < 10: return {"date": date_str, "patterns": ["数据点不足"]}

    day_ret = (prices.iloc[-1] - prices.iloc[0]) / prices.iloc[0] * 100 if prices.iloc[0] > 0 else 0
    high_pos = prices.values.argmax() / len(prices)
    patterns = []
    if high_pos < 0.25 and day_ret < -1: patterns.append("高开低走")
    if high_pos < 0.15 and day_ret < 0: patterns.append("早盘拉高回落")
    tail_n = min(30, len(prices))
    if len(prices) > tail_n and (prices.iloc[-1] - prices.iloc[-tail_n]) / prices.iloc[-tail_n] * 100 < -0.8:
        patterns.append("尾盘跳水")
    if len(vols) > 20:
        mid = len(vols) // 2
        if vols.iloc[mid:].sum() > 0 and vols.iloc[:mid].sum() / vols.iloc[mid:].sum() > 1.8 and day_ret < 0:
            patterns.append("前半段放量后半段缩量")
    if len(vols) > 10:
        spikes = (vols > vols.mean() * 3).sum()
        if spikes >= 5: patterns.append(f"多次脉冲放量({spikes}次)")
    
    return {"patterns": patterns, "day_return": day_ret, "open": prices.iloc[0], "close": prices.iloc[-1]}

# ============================================================================
# Markdown 报告生成器
# ============================================================================

def generate_detailed_report(ts_code: str, stock_name: str) -> str:
    """生成深度 Markdown 出货分析报告"""
    symbol = ts_code.split('.')[0]
    md = [f"# {stock_name} ({ts_code}) 主力出货深度分析报告",
          f"> **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", ""]
    
    client = create_client()
    try:
        # Step 1: 日K线
        md.append("## 一、日K线量价分析")
        kline = pd.DataFrame()
        try:
            kline = client.bars(symbol=symbol, frequency=9, offset=200)
        except Exception as e:
            md.append(f"获取K线数据失败: {e}")
            return "\n".join(md)
            
        if kline is None or kline.empty:
            md.append("无K线数据")
            return "\n".join(md)
            
        analyzed = analyze_daily_kline(kline)
        recent = analyzed.tail(10)
        md.append("| 日期 | 收盘 | 涨跌幅 | 量比 | 特征 |")
        md.append("|---|---|---|---|---|")
        for _, row in recent.iterrows():
            d_str = str(row.name).split()[0]
            pct = row['pct_change']
            vol_r = row['vol_ratio']
            arr = "🔴" if pct < 0 else "🟢"
            tag = "放量" if vol_r > 1.5 else ("缩量" if vol_r < 0.6 else "")
            if row['upper_shadow_ratio'] > 0.5: tag += " 长上影"
            md.append(f"| {d_str} | {row['close']:.2f} | {arr} {pct:+.2f}% | {vol_r:.2f} | {tag} |")
        md.append("")

        # Step 2: 信号
        signals = find_volume_price_signals(analyzed)
        md.append("## 二、量价异动信号 (近半年)")
        has_signal = False
        for sname, items in signals.items():
            if items:
                has_signal = True
                md.append(f"**{sname}** ({len(items)}次):")
                for it in items[-3:]:
                    if len(it) == 4:
                        md.append(f"- {it[0]}: 涨跌 {it[1]:+.2f}%, 收盘 {it[3]:.2f}")
        if not has_signal:
            md.append("未发现明显量价异动。")
        md.append("")

        # Step 3: 出货阶段
        phases = identify_distribution_phase(analyzed)
        md.append("## 三、历史出货窗口识别")
        if phases:
            for i, p in enumerate(phases[:3]):
                md.append(f"**窗口 {i+1}**: {p['peak_date']} ~ {p['end_date']}")
                md.append(f"- 从 {p['peak_price']:.2f} 跌至 {p['low_price']:.2f} (跌幅 **{p['decline_pct']:.2f}%**)，持续 {p['duration_days']} 天")
        else:
            md.append("未发现显著的阶段性单边下跌出货窗口。")
        md.append("")

        # Step 4: 大单分析
        target_dates = set()
        for items in signals.values():
            for it in items[-5:]: target_dates.add(it[0].replace("-", ""))
        for _, row in analyzed.tail(5).iterrows():
            target_dates.add(str(row.name).split()[0].replace("-", ""))
            
        valid_dates = sorted(list(target_dates))[-10:]  # 最多看近10天有信号的日子
        
        md.append("## 四、关键日分时与大单分析")
        md.append("| 日期 | 涨跌幅 | 大单净量 | 卖出占比 | 分时特征 |")
        md.append("|---|---|---|---|---|")
        
        for dt in valid_dates:
            dt_fmt = f"{dt[:4]}-{dt[4:6]}-{dt[6:]}"
            tick = fetch_transactions(client, symbol, dt)
            mins = fetch_minutes(client, symbol, dt)
            
            t_res = analyze_tick_data(tick)
            m_res = analyze_intraday(mins, dt)
            
            day_ret_str = f"{m_res.get('day_return', 0):+.2f}%" if 'day_return' in m_res else "-"
            net_str = f"{t_res.get('big_net', 0):+.0f}" if t_res.get('status') == 'ok' and 'big_net' in t_res else "-"
            if net_str != "-" and not net_str.startswith("-"): net_str = "+" + net_str
            sell_ratio = f"{t_res.get('sell_ratio', 0):.1f}%" if t_res.get('status') == 'ok' and 'sell_ratio' in t_res else "-"
            pats = ", ".join(m_res.get('patterns', []))
            
            md.append(f"| {dt_fmt} | {day_ret_str} | {net_str} | {sell_ratio} | {pats} |")
            time.sleep(0.2)
            
        md.append("")
        md.append("## 五、综合结论")
        if phases or sum(len(v) for v in signals.values()) > 5:
            md.append("> [!CAUTION]\n> **存在较强的主力出货迹象，建议谨慎关注风险。**")
        else:
            md.append("> [!TIP]\n> **当前未发现明显的主力集中出货迹象。**")

        return "\n".join(md)
    finally:
        close_client(client)
