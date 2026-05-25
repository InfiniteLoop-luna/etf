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

from src.etf_stats import get_stock_kline_timeseries
from src.security_intraday_store import (
    fetch_stock_intraday_from_mootdx,
    get_stock_intraday_timeseries,
    upsert_stock_intraday_timeseries,
)

warnings.filterwarnings("ignore")

# ============================================================================
# mootdx 客户端
# ============================================================================

def fetch_daily_kline(ts_code: str, engine=None) -> pd.DataFrame:
    df = get_stock_kline_timeseries(ts_code, engine=engine)
    if df is None or df.empty:
        client = create_client()
        try:
            symbol = ts_code.split('.')[0]
            df = client.bars(symbol=symbol, frequency=9, offset=200)
        finally:
            close_client(client)

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        df = df.dropna(subset=["trade_date"]).sort_values("trade_date")
        df.index = df["trade_date"].dt.strftime("%Y-%m-%d")
    return df

def create_client():
    from mootdx.quotes import Quotes
    
    try:
        # 尝试默认优选
        client = Quotes.factory(market='std', timeout=10)
        # 验证连接是否真正可用且能返回数据
        test_df = client.bars(symbol='000001', frequency=9, offset=1)
        if test_df is not None and not test_df.empty:
            return client
        client.close()
    except Exception:
        pass

    # 备选的高可用服务器列表（电信/联通官方主力节点）
    good_servers = [
        ('119.147.212.81', 7709),  # 深圳电信
        ('121.14.110.194', 7709),  # 东莞电信
        ('114.115.234.141', 7709), # 华为云
        ('120.24.149.49', 7709)    # 阿里云
    ]
    
    for host, port in good_servers:
        try:
            client = Quotes.factory(market='std', server=(host, port), timeout=5)
            test_df = client.bars(symbol='000001', frequency=9, offset=1)
            if test_df is not None and not test_df.empty:
                return client
            client.close()
        except Exception:
            pass
            
    # 如果全部失败，强制返回第一个
    return Quotes.factory(market='std', server=good_servers[0], timeout=10)

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

    for idx in range(22, len(df)):
        if (
            df.iloc[idx]["pct_change"] < 0
            and df.iloc[idx - 1]["pct_change"] < 0
            and df.iloc[idx - 2]["pct_change"] < 0
            and df.iloc[idx]["vol_ratio"] < 0.8
        ):
            signals["连续缩量阴跌"].append(
                (
                    str(df.iloc[idx].name).split()[0],
                    df.iloc[idx - 2 : idx + 1]["pct_change"].sum(),
                    df.iloc[idx]["vol_ratio"],
                    df.iloc[idx]["close"],
                )
            )

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

def fetch_transactions(ts_code: str, trade_date_str: str, max_count=2000, engine=None, client=None):
    try:
        if engine is not None:
            from src.distribution_report_store import get_compressed_ticks, save_compressed_ticks
            symbol = ts_code.split('.')[0]
            df = get_compressed_ticks(engine, symbol, trade_date_str)
            if df is not None and not df.empty:
                return df

        symbol = ts_code.split('.')[0]
        own_client = client is None
        if own_client:
            client = create_client()
        raw = client.transactions(symbol=symbol, start=0, offset=max_count, date=trade_date_str)
        if raw is not None and not raw.empty:
            if engine is not None:
                save_compressed_ticks(engine, symbol, trade_date_str, raw)
            return raw
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()
    finally:
        if client is not None and "own_client" in locals() and own_client:
            close_client(client)

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
    res["big_order_pct"] = res["big_pct"]
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

def fetch_minutes(ts_code: str, trade_date_str: str, engine=None):
    try:
        df = get_stock_intraday_timeseries(ts_code, trade_date_str, freq="1min", engine=engine)
        if df is not None and not df.empty:
            return df
        fetched_df = fetch_stock_intraday_from_mootdx(ts_code, trade_date_str, freq="1min")
        if fetched_df is not None and not fetched_df.empty:
            if engine is not None:
                upsert_stock_intraday_timeseries(engine, fetched_df, source="mootdx.minutes")
            return fetched_df
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
    
    return {"patterns": patterns, "day_return": day_ret, "open": prices.iloc[0], "close": prices.iloc[-1], "high_position": high_pos}

# ============================================================================
# Markdown 报告生成器
# ============================================================================

def generate_detailed_report(ts_code: str, stock_name: str, engine=None) -> str:
    """生成深度 Markdown 出货分析报告"""
    symbol = ts_code.split('.')[0]
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    # 尝试读取当日缓存
    if engine is not None:
        from src.distribution_report_store import get_daily_report, save_daily_report
        cached = get_daily_report(engine, symbol, today_str)
        if cached and "无K线数据" not in cached:
            return cached

    md = [
        f"# {stock_name} ({ts_code}) 主力出货分析报告",
        f"> 分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"> 数据来源: mootdx (通达信协议)",
        ""
    ]
    # ========== STEP 1: 日K线分析 ==========
    md.append("---")
    md.append("## 📈 Step 1: 日K线量价分析")

    analyzed = fetch_daily_kline(ts_code, engine=engine)
    if analyzed is None or analyzed.empty:
        md.append("❌ 无K线数据")
        return "\n".join(md)

    analyzed = analyze_daily_kline(analyzed)

    md.append("📊 近期行情概况（最近30个交易日）：\n")
    for _, row in analyzed.tail(30).iterrows():
        date_str = str(row.name)
        pct = row["pct_change"]
        vol_r = row.get("vol_ratio", 1.0)
        arrow = "🟢" if pct >= 0 else "🔴"
        vol_tag = " 📢放量" if vol_r > 2 else (" 📈量增" if vol_r > 1.5 else (" 📉缩量" if vol_r < 0.6 else ""))
        shadow_tag = " ⛳长上影" if row.get("upper_shadow_ratio", 0) > 0.5 else ""
        md.append(
            f"- **{date_str}** | 开 {row['open']:>7.2f} | 高 {row['high']:>7.2f} | 低 {row['low']:>7.2f} | 收 {row['close']:>7.2f} | {arrow} {pct:>+6.2f}% | 量比 {vol_r:>5.2f}{vol_tag}{shadow_tag}"
        )

    # ========== STEP 2: 量价异动信号识别 ==========
    md.append("\n---")
    md.append("## 🚨 Step 2: 量价异动信号识别")
    signals = find_volume_price_signals(analyzed)
    signal_labels = {
        "放量滞涨": ("涨跌幅", "量比", "收盘"),
        "放量下跌": ("涨跌幅", "量比", "收盘"),
        "天量天价": ("涨跌幅", "成交量", "收盘"),
        "量价背离_顶部": ("涨跌幅", "量缩幅度", "收盘"),
        "高位长上影": ("涨跌幅", "上影线比率", "收盘"),
        "破位下跌": ("涨跌幅", "MA20", "收盘"),
        "连续缩量阴跌": ("涨跌幅", "指标", "收盘"),
    }

    total_signals = sum(len(v) for v in signals.values())
    if total_signals == 0:
        md.append("\nℹ️ 未发现明显的量价异动信号")
    else:
        for signal_name, items in signals.items():
            if items:
                labels = signal_labels.get(signal_name, ("涨跌幅", "指标", "收盘"))
                md.append(f"\n**⚠️ {signal_name}（{len(items)} 个信号）**：")
                for item in items[-10:]:
                    date_s = item[0]
                    pct_s = f"{item[1]:+.2f}%"
                    metric_s = f"{item[2]:,.2f}"
                    close_s = f"{item[3]:.2f}"
                    md.append(f"- 日期: {date_s} | {labels[0]}: {pct_s} | {labels[1]}: {metric_s} | {labels[2]}: {close_s}")

    # ========== STEP 3: 出货阶段识别 ==========
    md.append("\n---")
    md.append("## 📅 Step 3: 出货阶段识别")
    phases = identify_distribution_phase(analyzed)

    if not phases:
        md.append("\nℹ️ 未识别到明显的出货阶段")
    else:
        md.append(f"\n⚠️ 发现 {len(phases)} 个可疑出货/下跌阶段：\n")
        for i, phase in enumerate(phases, 1):
            md.append(f"**阶段 {i}:**")
            md.append(f"- 📍 见顶日: {phase['peak_date']} | 最高价: {phase['peak_price']:.2f}")
            md.append(f"- 📍 结束日: {phase['end_date']} | 最低价: {phase['low_price']:.2f}")
            md.append(f"- 📉 跌幅: **{phase['decline_pct']:.2f}%** | 持续: {phase['duration_days']} 个交易日")
            md.append(f"- 📊 日均成交: {phase['avg_vol']:,.0f}")
            md.append("")

    target_dates = set()
    for items in signals.values():
        for item in items[-5:]:
            target_dates.add(item[0])
    for phase in phases[:3]:
        target_dates.add(phase["peak_date"])
    for _, row in analyzed.tail(5).iterrows():
        target_dates.add(str(row.name))

    valid_dates = []
    for d in sorted(target_dates):
        d_clean = str(d).replace("-", "")
        if len(d_clean) >= 8 and d_clean[:8].isdigit():
            valid_dates.append(d_clean[:8])

    # ========== STEP 4: 分笔成交（大单追踪） ==========
    md.append("\n---")
    md.append("## 💰 Step 4: 关键日期分笔成交分析（大单追踪）")
    if valid_dates:
        md.append(f"\n分析 {len(valid_dates)} 个关键日期的分笔成交...\n")
        for dt in sorted(valid_dates)[-15:]:
            dt_fmt = f"{dt[:4]}-{dt[4:6]}-{dt[6:]}"
            tick_df = fetch_transactions(ts_code, dt, engine=engine)
            if not tick_df.empty:
                result = analyze_tick_data(tick_df)
                if result["status"] == "ok":
                    sell_info = ""
                    if "sell_ratio" in result:
                        sell_info = f" | 卖出占比: {result['sell_ratio']:.1f}%"
                        if "big_sell_ratio" in result:
                            sell_info += f" | 大单卖出占比: {result['big_sell_ratio']:.1f}%"
                        if "big_net" in result:
                            net_tag = "🟢" if result["big_net"] > 0 else "🔴"
                            sell_info += f" | 大单净量: {net_tag}{result['big_net']:+.0f}"
                    md.append(f"- **{dt_fmt}**: 总成交 {result.get('total_vol', 0):>10,.0f}手 | 大单 {result.get('big_order_count', 0)}笔 ({result.get('big_order_pct', 0):.1f}%){sell_info}")
                elif result["status"] == "columns_not_found":
                    md.append(f"- **{dt_fmt}**: 缺失列名")
            else:
                md.append(f"- **{dt_fmt}**: 无分笔数据")
    else:
        md.append("\nℹ️ 没有需要分析的目标日期")

    # ========== STEP 5: 分时走势分析 ==========
    md.append("\n---")
    md.append("## ⏱️ Step 5: 关键日期分时走势分析")
    if valid_dates:
        md.append(f"\n分析 {min(len(valid_dates), 15)} 个关键日期的分时数据...\n")
        for dt in sorted(valid_dates)[-15:]:
            dt_fmt = f"{dt[:4]}-{dt[4:6]}-{dt[6:]}"
            mins_df = fetch_minutes(ts_code, dt, engine=engine)
            if not mins_df.empty:
                result = analyze_intraday(mins_df, dt)
                patterns_str = " | ".join(result["patterns"])
                price_info = ""
                if "day_return" in result:
                    price_info = f"开 {result['open']:.2f} | 收 {result['close']:.2f} | 涨跌 {result['day_return']:+.2f}% | 高点位置 {result.get('high_position', 0):.0%}"
                md.append(f"- **{dt_fmt}**: {price_info}\n  > 🏷️ {patterns_str}")
            else:
                md.append(f"- **{dt_fmt}**: 无分时数据")
    else:
        md.append("\nℹ️ 没有需要分析的目标日期")

    # ========== STEP 6: 综合结论 ==========
    md.append("\n---")
    md.append("## 📋 Step 6: 综合分析结论\n")

    distribution_evidence = []
    if signals.get("天量天价"):
        t = signals["天量天价"][-1]
        distribution_evidence.append(f"🔸 **天量天价信号**: {t[0]} (量={t[2]:,.0f}, 收盘={t[3]:.2f})")
    if signals.get("放量滞涨"):
        for t in signals["放量滞涨"][-3:]:
            distribution_evidence.append(f"🔸 **放量滞涨**: {t[0]} (量比={t[2]:.2f}, 涨跌={t[1]:+.2f}%)")
    if signals.get("放量下跌"):
        for t in signals["放量下跌"][-3:]:
            distribution_evidence.append(f"🔸 **放量下跌**: {t[0]} (量比={t[2]:.2f}, 涨跌={t[1]:+.2f}%)")
    if signals.get("高位长上影"):
        for t in signals["高位长上影"][-3:]:
            distribution_evidence.append(f"🔸 **高位长上影**: {t[0]} (上影比={t[2]:.1%})")
    if signals.get("量价背离_顶部"):
        for t in signals["量价背离_顶部"][-3:]:
            distribution_evidence.append(f"🔸 **量价背离**: {t[0]} (量缩={t[2]:.1f}%)")
    if signals.get("破位下跌"):
        for t in signals["破位下跌"][-3:]:
            distribution_evidence.append(f"🔸 **破均线**: {t[0]} (跌破MA20={t[2]:.2f})")

    if distribution_evidence:
        md.append("### 🚨 出货信号汇总：")
        for ev in distribution_evidence:
            md.append(f"- {ev}")

    if phases:
        md.append("\n### 📅 最可能的主力出货窗口：")
        for i, phase in enumerate(phases[:3], 1):
            md.append(f"{i}. **{phase['peak_date']} ~ {phase['end_date']}**")
            md.append(f"   - 从 {phase['peak_price']:.2f} 跌至 {phase['low_price']:.2f}，跌幅 **{phase['decline_pct']:.2f}%**，持续 {phase['duration_days']} 天")
    else:
        md.append("\n✅ 未发现明显的阶段性出货行为")

    md.append("\n### 📊 最终结论")
    if not distribution_evidence and not phases:
        md.append("> [!TIP]\n> **当前未发现明显的主力集中出货迹象。**")
    elif len(distribution_evidence) >= 3 or (phases and phases[0].get("decline_pct", 0) < -10):
        md.append("> [!CAUTION]\n> **存在较强的主力出货迹象，建议谨慎关注风险！**")
    else:
        md.append("> [!WARNING]\n> **存在一些出货信号，但尚不构成明确的系统性出货，需持续跟踪。**")

    final_md = "\n".join(md)
    if engine is not None:
        from src.distribution_report_store import save_daily_report
        save_daily_report(engine, symbol, today_str, final_md)
    return final_md
