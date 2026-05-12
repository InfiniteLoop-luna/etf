from __future__ import annotations

import hashlib
import re
from datetime import date, datetime

CODE_PATTERN = re.compile(r"(?<!\d)(\d{6})(?!\d)")
PRICE_PATTERN = re.compile(r"(?<!\d)(\d{1,4}(?:\.\d{1,3})?)(?!\d)")

TRIM_KEYWORDS = ("减仓", "先减", "止盈", "减一点", "减掉", "先减一点")
EXIT_KEYWORDS = ("卖出", "清仓", "出货", "离场", "止损")
BEARISH_KEYWORDS = ("看空", "不看好", "走弱", "失效", "破位")


def normalize_timestamp(value) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())

    text_value = str(value or "").strip()
    if not text_value:
        raise ValueError("timestamp cannot be empty")

    normalized = text_value.replace("T", " ").replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
        try:
            return datetime.strptime(text_value, fmt)
        except ValueError:
            continue
    raise ValueError(f"unsupported timestamp: {value}")


def normalize_ts_code(value: str) -> str | None:
    text_value = str(value or "").strip().upper()
    if not text_value:
        return None
    if "." in text_value:
        symbol, market = text_value.split(".", 1)
        if len(symbol) == 6 and market in {"SH", "SZ", "BJ"}:
            return f"{symbol}.{market}"
        return None
    if not text_value.isdigit() or len(text_value) != 6:
        return None
    if text_value.startswith(("4", "8")):
        return f"{text_value}.BJ"
    if text_value.startswith(("5", "6", "9")):
        return f"{text_value}.SH"
    return f"{text_value}.SZ"


def infer_direction(text: str) -> str:
    text_value = str(text or "").strip()
    if not text_value:
        return "bullish"
    if any(keyword in text_value for keyword in EXIT_KEYWORDS):
        return "exit_signal"
    if any(keyword in text_value for keyword in TRIM_KEYWORDS):
        return "trim_signal"
    if any(keyword in text_value for keyword in BEARISH_KEYWORDS):
        return "bearish"
    return "bullish"


def extract_target_text(text: str) -> str | None:
    text_value = str(text or "")
    for match in PRICE_PATTERN.finditer(text_value):
        candidate = match.group(1)
        if "." in candidate:
            return candidate
    return None


def build_mention_id(*parts: object) -> str:
    payload = "|".join(str(part) for part in parts if part is not None)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]
