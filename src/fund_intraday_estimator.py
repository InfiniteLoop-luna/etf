from __future__ import annotations

import re
from datetime import date, datetime, time
from typing import Iterable, Mapping, Optional
from zoneinfo import ZoneInfo

import requests


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
TENCENT_QUOTE_URL = "http://qt.gtimg.cn/q="
TENCENT_QUOTE_SOURCE = "腾讯证券行情"
DEFAULT_BATCH_SIZE = 50


def _optional_float(value) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_realtime_stock_symbol(symbol: str) -> Optional[dict]:
    """Normalize an A-share symbol for Tencent's public quote endpoint."""
    text = str(symbol or "").strip().upper()
    if not text:
        return None

    market = ""
    code = text
    if "." in text:
        code, market = [part.strip() for part in text.split(".", 1)]
    elif text[:2] in {"SH", "SZ", "BJ"}:
        market, code = text[:2], text[2:]

    if not code.isdigit() or len(code) != 6:
        return None

    if market not in {"SH", "SZ", "BJ"}:
        if code.startswith(("4", "8")):
            market = "BJ"
        elif code.startswith(("5", "6", "9")):
            market = "SH"
        else:
            market = "SZ"

    return {
        "ts_code": f"{code}.{market}",
        "code": code,
        "market": market,
        "quote_code": f"{market.lower()}{code}",
    }


def get_fund_intraday_market_state(now: Optional[datetime] = None) -> dict:
    """Return the requested weekday 09:30-15:00 valuation window in China time."""
    current = now or datetime.now(SHANGHAI_TZ)
    if current.tzinfo is None:
        current = current.replace(tzinfo=SHANGHAI_TZ)
    else:
        current = current.astimezone(SHANGHAI_TZ)

    if current.weekday() >= 5:
        status = "weekend"
        is_active = False
    elif current.time() < time(9, 30):
        status = "before_open"
        is_active = False
    elif current.time() > time(15, 0):
        status = "after_close"
        is_active = False
    else:
        status = "active"
        is_active = True

    return {
        "is_active": is_active,
        "status": status,
        "now": current,
        "market_date": current.date(),
        "window_label": "工作日 09:30–15:00",
    }


def collect_fund_holding_symbols(items: Iterable[dict]) -> tuple[str, ...]:
    normalized = {}
    for item in items:
        for holding in item.get("holdings", []):
            symbol_info = normalize_realtime_stock_symbol(holding.get("symbol"))
            if symbol_info:
                normalized[symbol_info["ts_code"]] = symbol_info["ts_code"]
    return tuple(sorted(normalized))


def _parse_quote_time(value: str) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text[:14], "%Y%m%d%H%M%S").replace(tzinfo=SHANGHAI_TZ)
    except ValueError:
        return None


def parse_tencent_quote_payload(
    payload: str,
    quote_code_map: Optional[Mapping[str, str]] = None,
) -> dict[str, dict]:
    """Parse Tencent's tilde-delimited batch quote response."""
    expected = {str(key).lower(): value for key, value in (quote_code_map or {}).items()}
    quotes: dict[str, dict] = {}
    for match in re.finditer(r'v_([a-z]{2}\d+)="([^"]*)";', str(payload or ""), re.IGNORECASE):
        quote_code = match.group(1).lower()
        fields = match.group(2).split("~")
        if len(fields) < 33:
            continue

        symbol_info = normalize_realtime_stock_symbol(quote_code)
        ts_code = expected.get(quote_code) or (
            symbol_info["ts_code"] if symbol_info else ""
        )
        if not ts_code:
            continue

        price = _optional_float(fields[3])
        previous_close = _optional_float(fields[4])
        change = _optional_float(fields[31])
        pct_change = _optional_float(fields[32])
        if pct_change is None and price is not None and previous_close not in {None, 0}:
            change = price - float(previous_close)
            pct_change = change / float(previous_close) * 100.0

        quotes[ts_code] = {
            "status": "ok" if pct_change is not None else "incomplete",
            "source": TENCENT_QUOTE_SOURCE,
            "ts_code": ts_code,
            "quote_code": quote_code,
            "stock_name": str(fields[1] or "").strip(),
            "price": price,
            "previous_close": previous_close,
            "change": change,
            "pct_change": pct_change,
            "quote_time": _parse_quote_time(fields[30]),
        }
    return quotes


def fetch_tencent_realtime_quotes(
    symbols: Iterable[str],
    *,
    session=None,
    timeout: float = 6.0,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, dict]:
    """Fetch unique A-share quotes in batches from Tencent Securities."""
    quote_code_map = {}
    for symbol in symbols:
        symbol_info = normalize_realtime_stock_symbol(symbol)
        if symbol_info:
            quote_code_map[symbol_info["quote_code"]] = symbol_info["ts_code"]
    if not quote_code_map:
        return {}

    client = session or requests.Session()
    own_client = session is None
    quotes: dict[str, dict] = {}
    quote_codes = sorted(quote_code_map)
    size = max(1, int(batch_size or DEFAULT_BATCH_SIZE))
    last_error = None
    try:
        for start in range(0, len(quote_codes), size):
            batch = quote_codes[start : start + size]
            try:
                response = client.get(
                    TENCENT_QUOTE_URL + ",".join(batch),
                    headers={
                        "Referer": "https://gu.qq.com/",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                    },
                    timeout=timeout,
                )
                response.raise_for_status()
                response.encoding = "gb18030"
                quotes.update(parse_tencent_quote_payload(response.text, quote_code_map))
            except requests.RequestException as exc:
                last_error = exc
    finally:
        if own_client:
            client.close()
    if not quotes and last_error is not None:
        raise last_error
    return quotes


def _is_quote_current(quote: dict, market_date: Optional[date]) -> bool:
    if quote.get("status") != "ok" or quote.get("pct_change") is None:
        return False
    quote_time = quote.get("quote_time")
    if market_date is None or quote_time is None:
        return True
    if isinstance(quote_time, datetime):
        if quote_time.tzinfo is not None:
            quote_time = quote_time.astimezone(SHANGHAI_TZ)
        return quote_time.date() == market_date
    return False


def enrich_fund_items_with_intraday_estimates(
    items: Iterable[dict],
    quotes: Mapping[str, dict],
    *,
    market_date: Optional[date] = None,
) -> list[dict]:
    """Estimate fund return as the sum of disclosed weight × live stock return."""
    enriched_items = []
    for item in items:
        enriched = dict(item)
        holdings = []
        contribution_total = 0.0
        covered_weight = 0.0
        quote_count = 0
        quote_times = []

        for holding in item.get("holdings", []):
            enriched_holding = dict(holding)
            symbol_info = normalize_realtime_stock_symbol(holding.get("symbol"))
            quote = quotes.get(symbol_info["ts_code"]) if symbol_info else None
            weight = _optional_float(holding.get("weight"))

            pct_change = None
            contribution = None
            if quote and weight is not None and weight > 0 and _is_quote_current(quote, market_date):
                pct_change = float(quote["pct_change"])
                contribution = weight * pct_change / 100.0
                contribution_total += contribution
                covered_weight += weight
                quote_count += 1
                if quote.get("quote_time") is not None:
                    quote_times.append(quote["quote_time"])

            enriched_holding.update(
                {
                    "realtime_price": quote.get("price") if pct_change is not None else None,
                    "realtime_pct_change": pct_change,
                    "estimate_contribution_pct": contribution,
                    "realtime_quote_time": quote.get("quote_time") if pct_change is not None else None,
                    "realtime_source": quote.get("source") if pct_change is not None else "",
                }
            )
            holdings.append(enriched_holding)

        top10_ratio = _optional_float(item.get("top10_ratio"))
        top10_coverage = (
            min(100.0, covered_weight / top10_ratio * 100.0)
            if top10_ratio not in {None, 0}
            else 0.0
        )
        enriched.update(
            {
                "holdings": holdings,
                "intraday_estimate_pct": round(contribution_total, 4) if quote_count else None,
                "intraday_covered_weight_pct": round(covered_weight, 2),
                "intraday_top10_coverage_pct": round(top10_coverage, 2),
                "intraday_quote_count": quote_count,
                "intraday_holding_count": len(holdings),
                "intraday_updated_at": max(quote_times) if quote_times else None,
                "intraday_source": TENCENT_QUOTE_SOURCE if quote_count else "",
            }
        )
        enriched_items.append(enriched)
    return enriched_items
