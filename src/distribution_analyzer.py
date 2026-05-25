# -*- coding: utf-8 -*-
"""
主力出货深度分析模块。
供前端按需调用，生成 Markdown 格式的深度分析报告。
"""
from __future__ import annotations

import warnings
from datetime import datetime

import numpy as np
import pandas as pd

from src.etf_stats import get_stock_kline_timeseries
from src.security_intraday_store import (
    fetch_stock_intraday_from_mootdx,
    get_stock_intraday_timeseries,
    upsert_stock_intraday_timeseries,
)

warnings.filterwarnings("ignore")

MAX_EXPENSIVE_TARGET_DATES = 4
RECENT_LIVE_FETCH_LOOKBACK_DAYS = 7


def normalize_report_trade_date(value: str | None) -> str | None:
    text_value = str(value or "").strip()
    if not text_value:
        return None
    compact = text_value.replace("-", "")
    if len(compact) == 8 and compact.isdigit():
        return f"{compact[:4]}-{compact[4:6]}-{compact[6:]}"
    return text_value[:10]


def should_attempt_live_fetch(trade_date_str: str, today: datetime | None = None) -> bool:
    trade_date_text = str(trade_date_str or "").strip().replace("-", "")
    if len(trade_date_text) < 8 or not trade_date_text[:8].isdigit():
        return False

    try:
        trade_date = datetime.strptime(trade_date_text[:8], "%Y%m%d").date()
    except ValueError:
        return False

    current_date = (today or datetime.now()).date()
    return (current_date - trade_date).days <= RECENT_LIVE_FETCH_LOOKBACK_DAYS


def select_expensive_target_dates(valid_dates: list[str]) -> list[str]:
    ordered_dates = sorted({str(item) for item in valid_dates if str(item).strip()})
    if len(ordered_dates) <= MAX_EXPENSIVE_TARGET_DATES:
        return ordered_dates
    return ordered_dates[-MAX_EXPENSIVE_TARGET_DATES:]


def create_client():
    from mootdx.quotes import Quotes

    try:
        client = Quotes.factory(market="std", timeout=10)
        test_df = client.bars(symbol="000001", frequency=9, offset=1)
        if test_df is not None and not test_df.empty:
            return client
        client.close()
    except Exception:
        pass

    good_servers = [
        ("119.147.212.81", 7709),
        ("121.14.110.194", 7709),
        ("114.115.234.141", 7709),
        ("120.24.149.49", 7709),
    ]

    for host, port in good_servers:
        try:
            client = Quotes.factory(market="std", server=(host, port), timeout=5)
            test_df = client.bars(symbol="000001", frequency=9, offset=1)
            if test_df is not None and not test_df.empty:
                return client
            client.close()
        except Exception:
            pass

    return Quotes.factory(market="std", server=good_servers[0], timeout=10)


def close_client(client):
    try:
        if client:
            client.close()
    except Exception:
        pass


def fetch_daily_kline(
    ts_code: str,
    engine=None,
    allow_live_fetch: bool = True,
    end_date: str | None = None,
) -> pd.DataFrame:
    df = get_stock_kline_timeseries(ts_code, end_date=end_date, engine=engine)
    if (df is None or df.empty) and allow_live_fetch:
        if end_date and not should_attempt_live_fetch(end_date):
            return pd.DataFrame()
        client = create_client()
        try:
            symbol = ts_code.split(".")[0]
            df = client.bars(symbol=symbol, frequency=9, offset=200)
        finally:
            close_client(client)

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        df = df.dropna(subset=["trade_date"]).sort_values("trade_date")
        if end_date:
            end_dt = pd.to_datetime(normalize_report_trade_date(end_date), errors="coerce")
            if pd.notna(end_dt):
                df = df[df["trade_date"] <= end_dt]
        df.index = df["trade_date"].dt.strftime("%Y-%m-%d")
    return df


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

    upper_shadow = df["high"] - df[["open", "close"]].max(axis=1)
    total_range = df["high"] - df["low"]
    df["upper_shadow_ratio"] = np.where(total_range > 0, upper_shadow / total_range, 0)
    return df


def find_volume_price_signals(df: pd.DataFrame) -> dict:
    signals = {
        "放量滞涨": [],
        "放量下跌": [],
        "天量天价": [],
        "量价背离_顶部": [],
        "高位长上影": [],
        "破位下跌": [],
        "连续缩量阴跌": [],
    }
    if df.empty or len(df) < 20:
        return signals

    df = df.copy()
    df["rolling_high_20"] = df["high"].rolling(20).max()
    df["near_high"] = df["high"] >= df["rolling_high_20"] * 0.97

    for idx in range(20, len(df)):
        row = df.iloc[idx]
        date_label = str(row.name).split()[0]

        if row["vol_ratio"] > 1.5 and abs(row["pct_change"]) < 1.0 and row.get("near_high", False):
            signals["放量滞涨"].append((date_label, row["pct_change"], row["vol_ratio"], row["close"]))
        if row["vol_ratio"] > 1.5 and row["pct_change"] < -2.0:
            signals["放量下跌"].append((date_label, row["pct_change"], row["vol_ratio"], row["close"]))
        if row["vol"] >= df["vol"].iloc[max(0, idx - 20) : idx + 1].max() * 0.95 and row.get("near_high", False):
            signals["天量天价"].append((date_label, row["pct_change"], row["vol"], row["close"]))
        if row["upper_shadow_ratio"] > 0.5 and row.get("near_high", False):
            signals["高位长上影"].append((date_label, row["pct_change"], row["upper_shadow_ratio"], row["close"]))
        if idx >= 1:
            prev = df.iloc[idx - 1]
            if prev["close"] > prev["ma20"] and row["close"] < row["ma20"] and row["pct_change"] < -1:
                signals["破位下跌"].append((date_label, row["pct_change"], row["ma20"], row["close"]))

    for idx in range(22, len(df)):
        date_label = str(df.iloc[idx].name).split()[0]
        if (
            df.iloc[idx]["pct_change"] > 0
            and df.iloc[idx - 1]["pct_change"] > 0
            and df.iloc[idx - 2]["pct_change"] > 0
            and df.iloc[idx]["vol"] < df.iloc[idx - 1]["vol"] < df.iloc[idx - 2]["vol"]
            and df.iloc[idx].get("near_high", False)
        ):
            signals["量价背离_顶部"].append(
                (
                    date_label,
                    df.iloc[idx - 2 : idx + 1]["pct_change"].sum(),
                    (1 - df.iloc[idx]["vol"] / df.iloc[idx - 2]["vol"]) * 100,
                    df.iloc[idx]["close"],
                )
            )

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
    working = df.copy()
    working["is_local_peak"] = False
    for idx in range(20, len(working) - 5):
        window = working.iloc[max(0, idx - 10) : min(len(working), idx + 11)]
        if working.iloc[idx]["high"] == window["high"].max():
            after = working.iloc[idx + 1 : min(len(working), idx + 11)]
            if len(after) >= 3 and after["pct_change"].mean() < 0:
                working.iloc[idx, working.columns.get_loc("is_local_peak")] = True

    peak_indices = working[working["is_local_peak"]].index.tolist()
    for peak_pos in peak_indices:
        peak_idx = working.index.get_loc(peak_pos) if peak_pos in working.index else None
        if peak_idx is None:
            continue

        peak_row = working.iloc[peak_idx]
        peak_price = peak_row["high"]
        peak_date = str(peak_row.name).split()[0]

        end_idx = peak_idx + 1
        lowest_close = peak_row["close"]
        total_vol = 0
        while end_idx < len(working):
            current = working.iloc[end_idx]
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
            end_row = working.iloc[min(end_idx, len(working) - 1)]
            phases.append(
                {
                    "peak_date": peak_date,
                    "peak_price": peak_price,
                    "end_date": str(end_row.name).split()[0],
                    "low_price": lowest_close,
                    "decline_pct": decline_pct,
                    "duration_days": duration,
                    "avg_vol": total_vol / max(duration, 1),
                }
            )

    phases.sort(key=lambda x: x["decline_pct"])
    return phases


def fetch_transactions(
    ts_code: str,
    trade_date_str: str,
    max_count=2000,
    engine=None,
    client=None,
    allow_live_fetch: bool = True,
):
    try:
        if engine is not None:
            from src.distribution_report_store import get_compressed_ticks, save_compressed_ticks

            symbol = ts_code.split(".")[0]
            df = get_compressed_ticks(engine, symbol, trade_date_str)
            if df is not None and not df.empty:
                return df

        if not allow_live_fetch or not should_attempt_live_fetch(trade_date_str):
            return pd.DataFrame()

        symbol = ts_code.split(".")[0]
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
    if tick_df is None or tick_df.empty:
        return {"status": "no_data"}

    df = tick_df.copy()
    col_map = {}
    for col in df.columns:
        cl = str(col).lower()
        if "price" in cl:
            col_map["price"] = col
        elif "vol" in cl:
            col_map["vol"] = col
        elif "buy" in cl or "sell" in cl or "direction" in cl:
            col_map["direction"] = col

    if "price" not in col_map or "vol" not in col_map:
        return {"status": "no_data"}

    df["_price"] = pd.to_numeric(df[col_map["price"]], errors="coerce")
    df["_vol"] = pd.to_numeric(df[col_map["vol"]], errors="coerce")
    total_vol = df["_vol"].sum()
    df["_amount"] = df["_price"] * df["_vol"] * 100
    big_orders = df[(df["_vol"] >= 500) | (df["_amount"] >= 500000)]
    big_vol = big_orders["_vol"].sum()

    res = {
        "status": "ok",
        "total_vol": total_vol,
        "big_order_count": len(big_orders),
        "big_pct": big_vol / total_vol * 100 if total_vol > 0 else 0,
    }
    res["big_order_pct"] = res["big_pct"]
    if "direction" in col_map:
        df["_dir"] = pd.to_numeric(df[col_map["direction"]], errors="coerce")
        res["sell_ratio"] = df[df["_dir"] == 1]["_vol"].sum() / total_vol * 100 if total_vol > 0 else 50
        if not big_orders.empty:
            big_orders = big_orders.copy()
            big_orders["_dir"] = pd.to_numeric(big_orders[col_map["direction"]], errors="coerce")
            big_buy = big_orders[big_orders["_dir"] == 0]["_vol"].sum()
            big_sell = big_orders[big_orders["_dir"] == 1]["_vol"].sum()
            res["big_net"] = big_buy - big_sell
            res["big_sell_ratio"] = big_sell / big_vol * 100 if big_vol > 0 else 50
    return res


def fetch_minutes(ts_code: str, trade_date_str: str, engine=None, allow_live_fetch: bool = True):
    try:
        df = get_stock_intraday_timeseries(ts_code, trade_date_str, freq="1min", engine=engine)
        if df is not None and not df.empty:
            return df
        if not allow_live_fetch or not should_attempt_live_fetch(trade_date_str):
            return pd.DataFrame()
        fetched_df = fetch_stock_intraday_from_mootdx(ts_code, trade_date_str, freq="1min")
        if fetched_df is not None and not fetched_df.empty:
            if engine is not None:
                upsert_stock_intraday_timeseries(engine, fetched_df, source="mootdx.minutes")
            return fetched_df
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def analyze_intraday(minutes_df: pd.DataFrame, date_str: str) -> dict:
    if minutes_df is None or minutes_df.empty:
        return {"date": date_str, "patterns": ["无数据"]}

    df = minutes_df.copy()
    price_col, vol_col = None, None
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

    day_ret = (prices.iloc[-1] - prices.iloc[0]) / prices.iloc[0] * 100 if prices.iloc[0] > 0 else 0
    high_pos = prices.values.argmax() / len(prices)
    patterns = []
    if high_pos < 0.25 and day_ret < -1:
        patterns.append("高开低走")
    if high_pos < 0.15 and day_ret < 0:
        patterns.append("早盘拉高回落")
    tail_n = min(30, len(prices))
    if len(prices) > tail_n and (prices.iloc[-1] - prices.iloc[-tail_n]) / prices.iloc[-tail_n] * 100 < -0.8:
        patterns.append("尾盘跳水")
    if len(vols) > 20:
        mid = len(vols) // 2
        if vols.iloc[mid:].sum() > 0 and vols.iloc[:mid].sum() / vols.iloc[mid:].sum() > 1.8 and day_ret < 0:
            patterns.append("前半段放量后半段缩量")
    if len(vols) > 10:
        spikes = (vols > vols.mean() * 3).sum()
        if spikes >= 5:
            patterns.append(f"多次脉冲放量({spikes}次)")

    return {
        "patterns": patterns,
        "day_return": day_ret,
        "open": prices.iloc[0],
        "close": prices.iloc[-1],
        "high_position": high_pos,
    }


def generate_detailed_report(
    ts_code: str,
    stock_name: str,
    engine=None,
    asof_trade_date: str | None = None,
    allow_live_fetch: bool = True,
) -> str:
    """生成深度 Markdown 出货分析报告。"""
    report_trade_date = normalize_report_trade_date(asof_trade_date) or datetime.now().strftime("%Y-%m-%d")

    if engine is not None:
        from src.distribution_report_store import get_daily_report

        cached = get_daily_report(engine, ts_code, report_trade_date)
        if cached and "无K线数据" not in cached:
            return cached

    analyzed = fetch_daily_kline(
        ts_code,
        engine=engine,
        allow_live_fetch=allow_live_fetch,
        end_date=report_trade_date,
    )

    md = [
        f"# {stock_name} ({ts_code}) 主力出货分析报告",
        f"> 分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"> 数据截止日: {report_trade_date}",
        f"> 数据来源: {'数据库缓存' if not allow_live_fetch else '数据库缓存 + 近期缺口实时补齐'}",
        "",
        "---",
        "## 📈 Step 1: 日K线量价分析",
    ]

    if analyzed is None or analyzed.empty:
        md.append("❌ 无K线数据")
        final_md = "\n".join(md)
        if engine is not None:
            from src.distribution_report_store import save_daily_report

            save_daily_report(engine, ts_code, report_trade_date, final_md)
        return final_md

    analyzed = analyze_daily_kline(analyzed)
    signals = find_volume_price_signals(analyzed)
    phases = identify_distribution_phase(analyzed)

    md.append("")
    md.append("📊 近期行情概况（最近30个交易日）：")
    md.append("")
    for _, row in analyzed.tail(30).iterrows():
        date_str = str(row.name).split()[0]
        pct = float(row["pct_change"]) if pd.notna(row["pct_change"]) else 0.0
        vol_ratio = float(row.get("vol_ratio", 1.0)) if pd.notna(row.get("vol_ratio", 1.0)) else 1.0
        shadow_ratio = float(row.get("upper_shadow_ratio", 0.0)) if pd.notna(row.get("upper_shadow_ratio", 0.0)) else 0.0
        arrow = "🟢" if pct >= 0 else "🔴"
        vol_tag = " 放量" if vol_ratio > 1.5 else (" 缩量" if vol_ratio < 0.6 else "")
        shadow_tag = " 长上影" if shadow_ratio > 0.5 else ""
        md.append(
            f"- **{date_str}** | 开 {row['open']:.2f} | 高 {row['high']:.2f} | 低 {row['low']:.2f} | 收 {row['close']:.2f} | {arrow} {pct:+.2f}% | 量比 {vol_ratio:.2f}{vol_tag}{shadow_tag}"
        )

    md.extend(["", "---", "## 🚨 Step 2: 量价异动信号识别"])
    total_signals = sum(len(items) for items in signals.values())
    if total_signals == 0:
        md.extend(["", "ℹ️ 未发现明显的量价异动信号"])
    else:
        for signal_name, items in signals.items():
            if not items:
                continue
            md.extend(["", f"**⚠️ {signal_name}（{len(items)} 个信号）**："])
            for date_s, pct_s, metric_s, close_s in items[-10:]:
                md.append(
                    f"- 日期: {date_s} | 涨跌幅: {pct_s:+.2f}% | 指标: {metric_s:,.2f} | 收盘: {close_s:.2f}"
                )

    md.extend(["", "---", "## 📅 Step 3: 出货阶段识别"])
    if not phases:
        md.extend(["", "ℹ️ 未识别到明显的出货阶段"])
    else:
        md.append("")
        for index, phase in enumerate(phases, 1):
            md.append(f"**阶段 {index}:**")
            md.append(f"- 📍 见顶日: {phase['peak_date']} | 最高价: {phase['peak_price']:.2f}")
            md.append(f"- 📍 结束日: {phase['end_date']} | 最低价: {phase['low_price']:.2f}")
            md.append(f"- 📉 跌幅: **{phase['decline_pct']:.2f}%** | 持续: {phase['duration_days']} 个交易日")
            md.append(f"- 📊 日均成交: {phase['avg_vol']:,.0f}")

    target_dates = set()
    for items in signals.values():
        for item in items[-5:]:
            target_dates.add(item[0])
    for phase in phases[:3]:
        target_dates.add(phase["peak_date"])
    for _, row in analyzed.tail(5).iterrows():
        target_dates.add(str(row.name).split()[0])

    valid_dates: list[str] = []
    for value in sorted(target_dates):
        compact = str(value).replace("-", "")
        if len(compact) >= 8 and compact[:8].isdigit():
            valid_dates.append(compact[:8])
    expensive_dates = select_expensive_target_dates(valid_dates)

    md.extend(["", "---", "## 💰 Step 4: 关键日期分笔成交分析（大单追踪）"])
    if not expensive_dates:
        md.extend(["", "ℹ️ 没有需要分析的目标日期"])
    else:
        md.extend(["", f"分析最近 {len(expensive_dates)} 个关键日期的分笔成交..."])
        for dt in expensive_dates:
            dt_fmt = f"{dt[:4]}-{dt[4:6]}-{dt[6:]}"
            tick_df = fetch_transactions(
                ts_code,
                dt,
                engine=engine,
                allow_live_fetch=allow_live_fetch,
            )
            if tick_df is None or tick_df.empty:
                md.append(f"- **{dt_fmt}**: 无分笔数据")
                continue
            result = analyze_tick_data(tick_df)
            if result.get("status") != "ok":
                md.append(f"- **{dt_fmt}**: 分笔数据不可用")
                continue
            sell_info = []
            if "sell_ratio" in result:
                sell_info.append(f"卖出占比: {result['sell_ratio']:.1f}%")
            if "big_sell_ratio" in result:
                sell_info.append(f"大单卖出占比: {result['big_sell_ratio']:.1f}%")
            if "big_net" in result:
                net_tag = "🟢" if result["big_net"] > 0 else "🔴"
                sell_info.append(f"大单净量: {net_tag}{result['big_net']:+.0f}")
            extra = f" | {' | '.join(sell_info)}" if sell_info else ""
            md.append(
                f"- **{dt_fmt}**: 总成交 {result.get('total_vol', 0):,.0f}手 | 大单 {result.get('big_order_count', 0)}笔 ({result.get('big_order_pct', 0):.1f}%){extra}"
            )

    md.extend(["", "---", "## ⏱️ Step 5: 关键日期分时走势分析"])
    if not expensive_dates:
        md.extend(["", "ℹ️ 没有需要分析的目标日期"])
    else:
        md.extend(["", f"分析最近 {len(expensive_dates)} 个关键日期的分时数据..."])
        for dt in expensive_dates:
            dt_fmt = f"{dt[:4]}-{dt[4:6]}-{dt[6:]}"
            mins_df = fetch_minutes(
                ts_code,
                dt,
                engine=engine,
                allow_live_fetch=allow_live_fetch,
            )
            if mins_df is None or mins_df.empty:
                md.append(f"- **{dt_fmt}**: 无分时数据")
                continue
            result = analyze_intraday(mins_df, dt)
            patterns_str = " | ".join(result.get("patterns", []))
            if "day_return" in result:
                md.append(
                    f"- **{dt_fmt}**: 开 {result['open']:.2f} | 收 {result['close']:.2f} | 涨跌 {result['day_return']:+.2f}% | 高点位置 {result.get('high_position', 0):.0%}\n  > 🏷️ {patterns_str}"
                )
            else:
                md.append(f"- **{dt_fmt}**: {patterns_str}")

    md.extend(["", "---", "## 📋 Step 6: 综合分析结论", ""])
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
        for evidence in distribution_evidence:
            md.append(f"- {evidence}")

    if phases:
        md.extend(["", "### 📅 最可能的主力出货窗口："])
        for index, phase in enumerate(phases[:3], 1):
            md.append(f"{index}. **{phase['peak_date']} ~ {phase['end_date']}**")
            md.append(
                f"   - 从 {phase['peak_price']:.2f} 跌至 {phase['low_price']:.2f}，跌幅 **{phase['decline_pct']:.2f}%**，持续 {phase['duration_days']} 天"
            )
    else:
        md.extend(["", "✅ 未发现明显的阶段性出货行为"])

    md.extend(["", "### 📊 最终结论"])
    if not distribution_evidence and not phases:
        md.append("> [!TIP]\n> **当前未发现明显的主力集中出货迹象。**")
    elif len(distribution_evidence) >= 3 or (phases and phases[0].get("decline_pct", 0) < -10):
        md.append("> [!CAUTION]\n> **存在较强的主力出货迹象，建议谨慎关注风险！**")
    else:
        md.append("> [!WARNING]\n> **存在一些出货信号，但尚不构成明确的系统性出货，需持续跟踪。**")

    final_md = "\n".join(md)
    if engine is not None:
        from src.distribution_report_store import save_daily_report

        save_daily_report(engine, ts_code, report_trade_date, final_md)
    return final_md
