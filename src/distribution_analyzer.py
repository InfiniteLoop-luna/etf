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

from src.distribution_llm_analysis import (
    analyze_distribution_payload,
    render_distribution_llm_markdown,
    should_require_llm_refresh,
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

    preferred_servers = [
        ("110.41.147.114", 7709),
        ("119.147.212.81", 7709),
        ("121.14.110.194", 7709),
        ("114.115.234.141", 7709),
        ("120.24.149.49", 7709),
    ]

    def _probe_transactions(client) -> bool:
        try:
            bars_df = client.bars(symbol="000001", frequency=9, offset=1)
            if bars_df is None or bars_df.empty:
                return False
            tick_df = client.transactions(symbol="000733", start=0, offset=10, date="20260520")
            return tick_df is not None and not tick_df.empty
        except Exception:
            return False

    try:
        client = Quotes.factory(market="std", timeout=10)
        if _probe_transactions(client):
            return client
        client.close()
    except Exception:
        pass

    for host, port in preferred_servers:
        try:
            client = Quotes.factory(market="std", server=(host, port), timeout=5)
            if _probe_transactions(client):
                return client
            client.close()
        except Exception:
            pass

    return Quotes.factory(market="std", server=preferred_servers[0], timeout=10)


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


def _serialize_signal_items(items: list[tuple]) -> list[dict[str, float | str]]:
    serialized: list[dict[str, float | str]] = []
    for date_s, pct_s, metric_s, close_s in items:
        serialized.append(
            {
                "date": str(date_s),
                "pct_change": float(pct_s),
                "metric": float(metric_s),
                "close": float(close_s),
            }
        )
    return serialized


def build_distribution_report_payload(
    ts_code: str,
    stock_name: str,
    *,
    analyzed: pd.DataFrame,
    signals: dict[str, list[tuple]],
    phases: list[dict[str, float | str | int]],
    expensive_dates: list[str],
    tick_results: dict[str, dict],
    intraday_results: dict[str, dict],
    report_trade_date: str,
    allow_live_fetch: bool,
    rule_summary: dict | None = None,
) -> dict:
    recent_daily_rows: list[dict[str, float | str]] = []
    for _, row in analyzed.tail(30).iterrows():
        date_str = str(row.name).split()[0]
        recent_daily_rows.append(
            {
                "date": date_str,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "pct_change": float(row["pct_change"]) if pd.notna(row["pct_change"]) else 0.0,
                "vol_ratio": float(row.get("vol_ratio", 1.0)) if pd.notna(row.get("vol_ratio", 1.0)) else 1.0,
                "upper_shadow_ratio": float(row.get("upper_shadow_ratio", 0.0)) if pd.notna(row.get("upper_shadow_ratio", 0.0)) else 0.0,
            }
        )

    signal_payload = {
        str(signal_name): _serialize_signal_items(items[-10:])
        for signal_name, items in signals.items()
        if items
    }
    phase_payload = [dict(phase) for phase in phases]
    tick_coverage = {
        "available_dates": [dt for dt, result in tick_results.items() if result.get("status") == "ok"],
        "missing_dates": [dt for dt, result in tick_results.items() if result.get("status") != "ok"],
    }
    intraday_coverage = {
        "available_dates": [dt for dt, result in intraday_results.items() if result.get("patterns") and result.get("patterns") != ["无数据"]],
        "missing_dates": [dt for dt, result in intraday_results.items() if not result.get("patterns") or result.get("patterns") == ["无数据"]],
    }
    return {
        "security": {
            "ts_code": ts_code,
            "stock_name": stock_name,
            "report_trade_date": report_trade_date,
            "data_source": "数据库缓存" if not allow_live_fetch else "数据库缓存 + 近期缺口实时补齐",
        },
        "coverage": {
            "tick": tick_coverage,
            "intraday": intraday_coverage,
            "expensive_dates": list(expensive_dates),
        },
        "daily_overview": recent_daily_rows,
        "signals": signal_payload,
        "phases": phase_payload,
        "tick_analysis": dict(tick_results),
        "intraday_analysis": dict(intraday_results),
        "rule_summary": dict(rule_summary or {}),
    }


def generate_detailed_report(
    ts_code: str,
    stock_name: str,
    engine=None,
    asof_trade_date: str | None = None,
    allow_live_fetch: bool = True,
    use_report_cache: bool = True,
    save_report: bool = True,
) -> str:
    """生成深度 Markdown 出货分析报告。"""
    report_trade_date = normalize_report_trade_date(asof_trade_date) or datetime.now().strftime("%Y-%m-%d")

    if engine is not None and use_report_cache:
        from src.distribution_report_store import get_daily_report

        cached = get_daily_report(engine, ts_code, report_trade_date)
        if cached and "无K线数据" not in cached and not should_require_llm_refresh(cached):
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
        if engine is not None and save_report:
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

    tick_results: dict[str, dict] = {}
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
                tick_results[dt_fmt] = {"status": "no_data"}
                md.append(f"- **{dt_fmt}**: 无分笔数据")
                continue
            result = analyze_tick_data(tick_df)
            tick_results[dt_fmt] = dict(result)
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

    intraday_results: dict[str, dict] = {}
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
                intraday_results[dt_fmt] = {"date": dt_fmt, "patterns": ["无数据"]}
                md.append(f"- **{dt_fmt}**: 无分时数据")
                continue
            result = analyze_intraday(mins_df, dt)
            intraday_results[dt_fmt] = dict(result)
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
        rule_risk_level = "低"
        rule_verdict = "中性"
        rule_conclusion = "当前未发现明显的主力集中出货迹象。"
        md.append(f"> [!TIP]\n> **{rule_conclusion}**")
    elif len(distribution_evidence) >= 3 or (phases and phases[0].get("decline_pct", 0) < -10):
        rule_risk_level = "高"
        rule_verdict = "强出货"
        rule_conclusion = "存在较强的主力出货迹象，建议谨慎关注风险！"
        md.append(f"> [!CAUTION]\n> **{rule_conclusion}**")
    else:
        rule_risk_level = "中"
        rule_verdict = "疑似出货"
        rule_conclusion = "存在一些出货信号，但尚不构成明确的系统性出货，需持续跟踪。"
        md.append(f"> [!WARNING]\n> **{rule_conclusion}**")

    rule_summary = {
        "verdict": rule_verdict,
        "risk_level": rule_risk_level,
        "conclusion": rule_conclusion,
        "distribution_evidence_count": len(distribution_evidence),
        "phase_count": len(phases),
        "has_tick_analysis": any(result.get("status") == "ok" for result in tick_results.values()),
        "has_intraday_analysis": any(
            result.get("patterns") and result.get("patterns") != ["无数据"]
            for result in intraday_results.values()
        ),
    }

    payload = build_distribution_report_payload(
        ts_code,
        stock_name,
        analyzed=analyzed,
        signals=signals,
        phases=phases,
        expensive_dates=[f"{dt[:4]}-{dt[4:6]}-{dt[6:]}" for dt in expensive_dates],
        tick_results=tick_results,
        intraday_results=intraday_results,
        report_trade_date=report_trade_date,
        allow_live_fetch=allow_live_fetch,
        rule_summary=rule_summary,
    )
    llm_result = analyze_distribution_payload(payload)
    md.extend(render_distribution_llm_markdown(llm_result))

    final_md = "\n".join(md)
    if engine is not None and save_report:
        from src.distribution_report_store import save_daily_report

        save_daily_report(engine, ts_code, report_trade_date, final_md)
    return final_md


def generate_detailed_report_markdown(
    ts_code: str,
    stock_name: str,
    engine=None,
    asof_trade_date: str | None = None,
    allow_live_fetch: bool = False,
    use_report_cache: bool = False,
    save_report: bool = False,
) -> str:
    """只生成 Markdown，不读取或写入报告缓存。"""
    _ = (use_report_cache, save_report)
    return generate_detailed_report(
        ts_code,
        stock_name,
        engine=engine,
        asof_trade_date=asof_trade_date,
        allow_live_fetch=allow_live_fetch,
        use_report_cache=False,
        save_report=False,
    )


def _format_alert_signal(signal_name: str, item: tuple) -> str:
    date_s, pct_s, metric_s, _close_s = item
    if signal_name == "放量下跌":
        return f"放量下跌(量比{metric_s:.1f}, 跌{abs(pct_s):.1f}%)"
    if signal_name == "放量滞涨":
        return f"高位放量滞涨(量比{metric_s:.1f})"
    if signal_name == "天量天价":
        return "天量天价"
    if signal_name == "高位长上影":
        return "高位长上影"
    if signal_name == "量价背离_顶部":
        return "顶部量价背离"
    if signal_name == "破位下跌":
        return f"破位下跌(跌破MA20={metric_s:.2f})"
    if signal_name == "连续缩量阴跌":
        return f"连续缩量阴跌(量比{metric_s:.1f})"
    return f"{signal_name}({date_s})"


def build_distribution_alert_payload(
    ts_code: str,
    analyzed: pd.DataFrame,
    report_trade_date: str | None = None,
) -> dict | None:
    """基于深度报告同一套日线信号，生成最新交易日预警 payload。"""
    if analyzed is None or analyzed.empty:
        return None

    target_trade_date = normalize_report_trade_date(report_trade_date)
    if not target_trade_date:
        target_trade_date = str(analyzed.index[-1]).split()[0]

    target_rows = analyzed[[str(idx).split()[0] == target_trade_date for idx in analyzed.index]]
    if target_rows.empty:
        target_rows = analyzed.tail(1)
        target_trade_date = str(target_rows.index[-1]).split()[0]

    signals = find_volume_price_signals(analyzed)
    matched_items: list[tuple[str, tuple]] = []
    for signal_name, items in signals.items():
        for item in items:
            item_date = normalize_report_trade_date(str(item[0]))
            if item_date == target_trade_date:
                matched_items.append((signal_name, item))

    signal_labels = [_format_alert_signal(signal_name, item) for signal_name, item in matched_items]
    signal_names = [signal_name for signal_name, _item in matched_items]
    alert_level = "NONE"
    if len(signal_labels) >= 2 or any(name in {"放量下跌", "量价背离_顶部", "破位下跌"} for name in signal_names):
        alert_level = "HIGH"
    elif len(signal_labels) == 1:
        alert_level = "MEDIUM"

    last_row = target_rows.iloc[-1]
    return {
        "ts_code": str(ts_code or "").strip().upper(),
        "trade_date": target_trade_date,
        "alert_level": alert_level,
        "alert_details": {
            "signals": signal_labels,
            "signal_names": signal_names,
            "close": float(last_row["close"]) if pd.notna(last_row.get("close")) else None,
            "pct_change": float(last_row["pct_change"]) if pd.notna(last_row.get("pct_change")) else None,
            "vol_ratio": float(last_row["vol_ratio"]) if pd.notna(last_row.get("vol_ratio")) else None,
            "source": "db:vw_ts_stock_daily",
        },
    }
