from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests


logger = logging.getLogger(__name__)

CACHE_PATH = Path(__file__).resolve().parents[1] / "data" / "nasdaq_sector_snapshot.json"
NASDAQ_LIST_URL = "https://api.nasdaq.com/api/quote/list-type/nasdaq100"
NASDAQ_HISTORICAL_URL = "https://api.nasdaq.com/api/quote/{symbol}/historical"
NASDAQ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 WealthSpark/1.0",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.nasdaq.com",
    "Referer": "https://www.nasdaq.com/",
}


@dataclass(frozen=True)
class NasdaqStock:
    symbol: str
    name: str
    sector: str
    core_weight: float = 1.0


# Trading-oriented Nasdaq growth groups. A stock has one primary group so that
# sector breadth and returns are not double counted.
NASDAQ_SECTOR_STOCKS: tuple[NasdaqStock, ...] = (
    NasdaqStock("NVDA", "英伟达", "半导体", 1.5),
    NasdaqStock("AVGO", "博通", "半导体", 1.4),
    NasdaqStock("AMD", "AMD", "半导体", 1.2),
    NasdaqStock("QCOM", "高通", "半导体", 1.0),
    NasdaqStock("MU", "美光科技", "半导体", 1.0),
    NasdaqStock("AMAT", "应用材料", "半导体", 1.0),
    NasdaqStock("LRCX", "泛林集团", "半导体", 1.0),
    NasdaqStock("KLAC", "科磊", "半导体", 0.9),
    NasdaqStock("ASML", "阿斯麦", "半导体", 1.0),
    NasdaqStock("INTC", "英特尔", "半导体", 0.8),
    NasdaqStock("MSFT", "微软", "AI与云计算", 1.5),
    NasdaqStock("AMZN", "亚马逊", "AI与云计算", 1.4),
    NasdaqStock("GOOGL", "谷歌", "AI与云计算", 1.4),
    NasdaqStock("META", "Meta", "AI与云计算", 1.3),
    NasdaqStock("ORCL", "甲骨文", "AI与云计算", 1.1),
    NasdaqStock("PLTR", "Palantir", "AI与云计算", 1.0),
    NasdaqStock("ADBE", "Adobe", "软件与网络安全", 1.1),
    NasdaqStock("INTU", "Intuit", "软件与网络安全", 1.0),
    NasdaqStock("PANW", "Palo Alto", "软件与网络安全", 1.1),
    NasdaqStock("CRWD", "CrowdStrike", "软件与网络安全", 1.0),
    NasdaqStock("FTNT", "Fortinet", "软件与网络安全", 0.9),
    NasdaqStock("DDOG", "Datadog", "软件与网络安全", 0.8),
    NasdaqStock("NFLX", "奈飞", "互联网平台", 1.2),
    NasdaqStock("PDD", "拼多多", "互联网平台", 1.0),
    NasdaqStock("MELI", "MercadoLibre", "互联网平台", 0.9),
    NasdaqStock("DASH", "DoorDash", "互联网平台", 0.8),
    NasdaqStock("ABNB", "爱彼迎", "互联网平台", 0.8),
    NasdaqStock("AAPL", "苹果", "消费电子与硬件", 1.5),
    NasdaqStock("DELL", "戴尔科技", "消费电子与硬件", 0.9),
    NasdaqStock("WDC", "西部数据", "消费电子与硬件", 0.8),
    NasdaqStock("STX", "希捷科技", "消费电子与硬件", 0.8),
    NasdaqStock("LOGI", "罗技", "消费电子与硬件", 0.7),
    NasdaqStock("TSLA", "特斯拉", "新能源汽车", 1.5),
    NasdaqStock("RIVN", "Rivian", "新能源汽车", 0.8),
    NasdaqStock("LI", "理想汽车", "新能源汽车", 0.9),
    NasdaqStock("NIO", "蔚来", "新能源汽车", 0.7),
    NasdaqStock("XPEV", "小鹏汽车", "新能源汽车", 0.7),
    NasdaqStock("AMGN", "安进", "生物科技", 1.2),
    NasdaqStock("GILD", "吉利德科学", "生物科技", 1.1),
    NasdaqStock("REGN", "再生元", "生物科技", 1.0),
    NasdaqStock("VRTX", "福泰制药", "生物科技", 1.1),
    NasdaqStock("MRNA", "Moderna", "生物科技", 0.8),
    NasdaqStock("BIIB", "百健", "生物科技", 0.8),
    NasdaqStock("ISRG", "直觉外科", "医疗科技", 1.2),
    NasdaqStock("DXCM", "德康医疗", "医疗科技", 0.9),
    NasdaqStock("IDXX", "爱德士", "医疗科技", 0.9),
    NasdaqStock("GEHC", "GE医疗", "医疗科技", 0.9),
    NasdaqStock("ALGN", "艾利科技", "医疗科技", 0.8),
    NasdaqStock("PYPL", "PayPal", "金融科技与加密", 1.0),
    NasdaqStock("COIN", "Coinbase", "金融科技与加密", 1.1),
    NasdaqStock("HOOD", "Robinhood", "金融科技与加密", 0.9),
    NasdaqStock("SOFI", "SoFi", "金融科技与加密", 0.8),
    NasdaqStock("AFRM", "Affirm", "金融科技与加密", 0.7),
    NasdaqStock("CSCO", "思科", "通信与数据中心", 1.1),
    NasdaqStock("ANET", "Arista Networks", "通信与数据中心", 1.2),
    NasdaqStock("MRVL", "Marvell", "通信与数据中心", 1.0),
    NasdaqStock("ARM", "Arm", "通信与数据中心", 1.0),
    NasdaqStock("VRT", "Vertiv", "通信与数据中心", 1.0),
)

BENCHMARKS = {"QQQ": "纳斯达克100 ETF", "SPY": "标普500 ETF", "IWM": "罗素2000 ETF"}
PERIOD_TO_DAYS = {"1日": 1, "5日": 5, "20日": 20, "60日": 60, "年初至今": -1}


def _parse_nasdaq_number(value) -> float | None:
    if value is None:
        return None
    cleaned = re.sub(r"[$,%\s,]", "", str(value))
    if cleaned in {"", "--", "N/A"}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def fetch_nasdaq100_constituents(timeout: int = 15) -> pd.DataFrame:
    response = requests.get(NASDAQ_LIST_URL, headers=NASDAQ_HEADERS, timeout=timeout)
    response.raise_for_status()
    payload = (response.json() or {}).get("data") or {}
    rows = ((payload.get("data") or {}).get("rows") or [])
    frame = pd.DataFrame(rows)
    if frame.empty or "symbol" not in frame.columns:
        raise RuntimeError("Nasdaq returned no Nasdaq-100 constituents")
    frame = frame.drop_duplicates(subset=["symbol"]).reset_index(drop=True)
    frame["marketCap"] = frame.get("marketCap", pd.Series(dtype="object")).map(_parse_nasdaq_number)
    frame["lastSalePrice"] = frame.get("lastSalePrice", pd.Series(dtype="object")).map(_parse_nasdaq_number)
    frame["percentageChange"] = frame.get("percentageChange", pd.Series(dtype="object")).map(_parse_nasdaq_number)
    frame.attrs["as_of"] = payload.get("date")
    return frame


def fetch_nasdaq_daily(symbol: str, *, days: int = 420, timeout: int = 15) -> pd.DataFrame:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=max(100, int(days)))
    response = requests.get(
        NASDAQ_HISTORICAL_URL.format(symbol=str(symbol).strip().upper()),
        params={
            "assetclass": "stocks",
            "fromdate": start.isoformat(),
            "todate": end.isoformat(),
            "limit": 5000,
        },
        headers=NASDAQ_HEADERS,
        timeout=timeout,
    )
    response.raise_for_status()
    data = (response.json() or {}).get("data") or {}
    rows = ((data.get("tradesTable") or {}).get("rows") or [])
    frame = pd.DataFrame(rows)
    if frame.empty or "date" not in frame.columns or "close" not in frame.columns:
        raise RuntimeError(f"Nasdaq returned no historical data for {symbol}")
    frame = frame.rename(
        columns={"date": "Date", "open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}
    )
    frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce")
    for column in ["Open", "High", "Low", "Close", "Volume"]:
        if column in frame.columns:
            frame[column] = frame[column].map(_parse_nasdaq_number)
    return frame.dropna(subset=["Date", "Close"]).sort_values("Date").reset_index(drop=True)


def fetch_akshare_daily(symbol: str) -> pd.DataFrame:
    import akshare as ak

    frame = ak.stock_us_daily(symbol=str(symbol).strip().upper(), adjust="")
    if frame is None or frame.empty:
        raise RuntimeError(f"AkShare returned no daily data for {symbol}")
    frame = frame.rename(
        columns={"date": "Date", "open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}
    )
    frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce")
    for column in ["Open", "High", "Low", "Close", "Volume"]:
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
    return frame.dropna(subset=["Date", "Close"]).sort_values("Date").reset_index(drop=True)


def fetch_us_daily(symbol: str, *, days: int = 420, timeout: int = 15) -> pd.DataFrame:
    try:
        return fetch_nasdaq_daily(symbol, days=days, timeout=timeout)
    except Exception as primary_error:
        logger.warning("Nasdaq history failed for %s, falling back to AkShare: %s", symbol, primary_error)
        return fetch_akshare_daily(symbol)


def calculate_period_return(frame: pd.DataFrame, period_label: str) -> float | None:
    if frame is None or frame.empty:
        return None
    closes = pd.to_numeric(frame["Close"], errors="coerce").dropna()
    if len(closes) < 2:
        return None
    if period_label == "年初至今":
        dates = pd.to_datetime(frame.loc[closes.index, "Date"], errors="coerce")
        current_year = dates.iloc[-1].year
        year_values = closes[dates.dt.year == current_year]
        base = year_values.iloc[0] if not year_values.empty else closes.iloc[0]
    else:
        days = PERIOD_TO_DAYS.get(period_label, 1)
        if len(closes) <= days:
            return None
        base = closes.iloc[-days - 1]
    if not base:
        return None
    return float((closes.iloc[-1] / base - 1.0) * 100.0)


def calculate_volume_ratio(frame: pd.DataFrame, window: int = 20) -> float | None:
    volumes = pd.to_numeric(frame.get("Volume"), errors="coerce").dropna()
    if len(volumes) < 2:
        return None
    history = volumes.iloc[-window - 1 : -1]
    average = history.mean()
    return float(volumes.iloc[-1] / average) if average and pd.notna(average) else None


def build_stock_metrics(
    frames: dict[str, pd.DataFrame],
    stocks: Iterable[NasdaqStock] = NASDAQ_SECTOR_STOCKS,
) -> pd.DataFrame:
    rows: list[dict] = []
    for stock in stocks:
        frame = frames.get(stock.symbol)
        if frame is None or frame.empty:
            continue
        last = frame.iloc[-1]
        row = {
            "symbol": stock.symbol,
            "name": stock.name,
            "sector": stock.sector,
            "core_weight": stock.core_weight,
            "trade_date": pd.Timestamp(last["Date"]).strftime("%Y-%m-%d"),
            "close": float(last["Close"]),
            "volume_ratio_20d": calculate_volume_ratio(frame),
        }
        for period in PERIOD_TO_DAYS:
            row[f"return_{period}"] = calculate_period_return(frame, period)
        rows.append(row)
    return pd.DataFrame(rows)


def aggregate_sector_metrics(stock_df: pd.DataFrame, *, period_label: str, qqq_return: float | None) -> pd.DataFrame:
    if stock_df is None or stock_df.empty:
        return pd.DataFrame()
    return_col = f"return_{period_label}"
    rows: list[dict] = []
    for sector, group in stock_df.groupby("sector", sort=False):
        valid = group.dropna(subset=[return_col]).copy()
        if valid.empty:
            continue
        weights = pd.to_numeric(valid["core_weight"], errors="coerce").fillna(1.0)
        sector_return = float((valid[return_col] * weights).sum() / weights.sum())
        leader = valid.sort_values([return_col, "core_weight"], ascending=[False, False]).iloc[0]
        active = valid.copy()
        active["leader_score"] = (
            active["core_weight"].rank(pct=True) * 35
            + active[return_col].rank(pct=True) * 40
            + active["volume_ratio_20d"].fillna(1.0).rank(pct=True) * 25
        )
        active_leader = active.sort_values("leader_score", ascending=False).iloc[0]
        rows.append(
            {
                "sector": sector,
                "return_pct": sector_return,
                "relative_qqq_pct": sector_return - qqq_return if qqq_return is not None else None,
                "up_count": int((valid[return_col] > 0).sum()),
                "down_count": int((valid[return_col] < 0).sum()),
                "stock_count": int(len(valid)),
                "breadth_pct": float((valid[return_col] > 0).mean() * 100.0),
                "leader_symbol": str(leader["symbol"]),
                "leader_name": str(leader["name"]),
                "leader_return_pct": float(leader[return_col]),
                "active_symbol": str(active_leader["symbol"]),
                "active_name": str(active_leader["name"]),
                "volume_ratio": float(valid["volume_ratio_20d"].dropna().median()) if valid["volume_ratio_20d"].notna().any() else None,
            }
        )
    return pd.DataFrame(rows).sort_values("return_pct", ascending=False).reset_index(drop=True)


def build_snapshot(
    *,
    period_label: str = "1日",
    stocks: Iterable[NasdaqStock] = NASDAQ_SECTOR_STOCKS,
    fetcher=fetch_us_daily,
) -> dict:
    stock_list = list(stocks)
    symbols = [stock.symbol for stock in stock_list] + list(BENCHMARKS)
    frames: dict[str, pd.DataFrame] = {}
    errors: dict[str, str] = {}
    for symbol in symbols:
        try:
            frames[symbol] = fetcher(symbol)
        except Exception as exc:
            errors[symbol] = f"{type(exc).__name__}: {exc}"
    stock_df = build_stock_metrics(frames, stock_list)
    benchmark_returns = {
        symbol: calculate_period_return(frames.get(symbol), period_label)
        for symbol in BENCHMARKS
    }
    sector_df = aggregate_sector_metrics(
        stock_df,
        period_label=period_label,
        qqq_return=benchmark_returns.get("QQQ"),
    )
    latest_dates = stock_df["trade_date"].dropna().tolist() if not stock_df.empty else []
    return {
        "schema_version": 1,
        "period": period_label,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "trade_date": max(latest_dates) if latest_dates else None,
        "source": "Nasdaq API / AkShare fallback",
        "is_stale": False,
        "benchmark_returns": benchmark_returns,
        "stocks": stock_df.where(pd.notna(stock_df), None).to_dict(orient="records"),
        "sectors": sector_df.where(pd.notna(sector_df), None).to_dict(orient="records"),
        "errors": errors,
        "coverage": {"loaded": len(stock_df), "total": len(stock_list)},
    }


def save_snapshot(snapshot: dict, path: Path = CACHE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def load_snapshot(path: Path = CACHE_PATH) -> dict | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except Exception as exc:
        logger.warning("Failed to read Nasdaq sector snapshot: %s", exc)
        return None


def load_or_refresh_snapshot(*, period_label: str = "1日", force: bool = False) -> dict:
    cached = load_snapshot()
    ttl_minutes = int(os.getenv("NASDAQ_SECTOR_CACHE_TTL_MINUTES", "360") or 360)
    if cached and not force and cached.get("period") == period_label:
        generated = pd.to_datetime(cached.get("generated_at"), errors="coerce", utc=True)
        if pd.notna(generated) and datetime.now(timezone.utc) - generated.to_pydatetime() < timedelta(minutes=ttl_minutes):
            return cached
    try:
        snapshot = build_snapshot(period_label=period_label)
        if snapshot.get("sectors"):
            save_snapshot(snapshot)
            return snapshot
        raise RuntimeError("no sector rows returned")
    except Exception as exc:
        if cached:
            cached = dict(cached)
            cached["is_stale"] = True
            cached["refresh_error"] = f"{type(exc).__name__}: {exc}"
            return cached
        raise
