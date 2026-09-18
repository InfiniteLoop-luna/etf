"""Compare the disclosed stock holdings already loaded by the watchlist.

Weights are percentages of a fund's *stock assets*, not its net assets.  The
watchlist currently loads at most ten positions per fund; neither absence from
that sample nor an unavailable weight is evidence of a zero position.
"""

from __future__ import annotations

from collections import defaultdict
from math import fsum, isfinite
import re
from typing import Iterable

import pandas as pd


UNKNOWN_INDUSTRY = "未识别行业"
CONFLICTING_INDUSTRY = "分类不一致"
_MISSING_TEXT = {"", "-", "--", "NONE", "NULL", "NAN", "NAT", "<NA>", "N/A"}
_UNKNOWN_INDUSTRIES = {"未识别", "未识别行业", "未知", "未知行业"}
_SYMBOL_PATTERN = re.compile(r"[A-Z0-9]+(?:[.\-][A-Z0-9]+)*")
_FUND_COLUMNS = [
    "fund_name", "report_date", "holding_count", "observed_weight", "data_status"
]


def _text(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.upper() in _MISSING_TEXT else text


def _symbol(value) -> str:
    text = _text(value).upper()
    if text in {"UNKNOWN", "UNDEFINED"}:
        return ""
    return text if _SYMBOL_PATTERN.fullmatch(text) else ""


def _weight(value) -> float:
    if isinstance(value, bool):
        return float("nan")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return float("nan")
    return number if isfinite(number) and 0 <= number <= 100 else float("nan")


def _report_date(value):
    # Date strings with eight digits are calendar dates, including numeric
    # dates occasionally returned by a data provider, not Unix nanoseconds.
    text = _text(value)
    if not text:
        return pd.NaT
    timestamp = pd.to_datetime(text, errors="coerce")
    return pd.Timestamp(timestamp.date()) if not pd.isna(timestamp) else pd.NaT


def build_fund_comparison(items: Iterable[dict]) -> dict:
    """Return auditable matrices without fetching or mutating source data.

    ``funds`` is indexed by fund_code and has fund_name, report_date,
    holding_count (unique usable symbols), observed_weight and data_status.
    The status is ``available``, ``partial`` or ``no_data``; available only
    describes the loaded sample and does not imply complete disclosure.

    ``holdings`` / ``industries`` have symbols / industry names as rows and
    fund codes as columns. Values are stock-asset weight percentages. Their
    corresponding ``holding_presence`` / ``industry_presence`` nullable
    boolean matrices distinguish True (observed), False (not observed in a
    loaded sample), and pd.NA (no usable holding data). All missing or absent
    weights remain NaN; no financial exposure is imputed as zero.

    ``overlap_counts`` counts shared distinct symbols independently of their
    weights. No-data funds have NaN rows and columns. ``stock_details`` is
    indexed by symbol with stock_name, industry, observed_fund_count columns.
    ``warnings`` contains human-readable source-quality warnings.
    """
    warnings: list[str] = []
    fund_items: dict[str, dict] = {}
    for item in items:
        code = _text(item.get("fund_code")).upper()
        if not code:
            warnings.append("已跳过缺少基金代码的记录。")
        elif code in fund_items:
            warnings.append(f"{code} 重复出现，比较仅保留第一条基金记录。")
        else:
            fund_items[code] = item

    positions: dict[str, dict[str, dict]] = {}
    invalid_symbols: dict[str, bool] = {}
    industry_candidates: dict[str, set[str]] = defaultdict(set)
    stock_names: dict[str, str] = {}

    for code, item in fund_items.items():
        by_symbol: dict[str, list[dict]] = defaultdict(list)
        invalid_count = 0
        for holding in item.get("holdings") or []:
            symbol = _symbol(holding.get("symbol"))
            if not symbol:
                invalid_count += 1
                continue
            by_symbol[symbol].append(holding)
            name = _text(holding.get("stock_name"))
            if name and symbol not in stock_names:
                stock_names[symbol] = name
            industry = _text(holding.get("industry"))
            if industry and industry not in _UNKNOWN_INDUSTRIES:
                industry_candidates[symbol].add(industry)
        invalid_symbols[code] = invalid_count > 0
        if invalid_count:
            warnings.append(f"{code} 有 {invalid_count} 条持仓缺少有效证券代码，未纳入比较。")

        positions[code] = {}
        for symbol, records in by_symbol.items():
            weights = [_weight(record.get("weight")) for record in records]
            all_valid = all(isfinite(weight) for weight in weights)
            matching_weights = all_valid and len(set(weights)) == 1
            weight = weights[0] if matching_weights else float("nan")
            if len(records) > 1:
                if matching_weights:
                    warnings.append(f"{code} 的 {symbol} 持仓重复，已按证券代码去重，权重未重复累加。")
                else:
                    warnings.append(f"{code} 的 {symbol} 重复持仓权重冲突或缺失，权重按缺失处理。")
            elif not all_valid:
                warnings.append(f"{code} 的 {symbol} 权重缺失或无效，未按 0 处理。")
            positions[code][symbol] = {"weight": weight}

    symbols = list(dict.fromkeys(symbol for rows in positions.values() for symbol in rows))
    stock_industries = {}
    for symbol in symbols:
        candidates = industry_candidates[symbol]
        if len(candidates) > 1:
            stock_industries[symbol] = CONFLICTING_INDUSTRY
            warnings.append(f"{symbol} 的行业分类不一致，统一列入“{CONFLICTING_INDUSTRY}”。")
        else:
            stock_industries[symbol] = next(iter(candidates), UNKNOWN_INDUSTRY)

    fund_index = pd.Index(fund_items, name="fund_code")
    symbol_index = pd.Index(symbols, name="symbol")
    industry_index = pd.Index(sorted(set(stock_industries.values())), name="industry")
    holdings = pd.DataFrame(float("nan"), index=symbol_index, columns=fund_index)
    industries = pd.DataFrame(float("nan"), index=industry_index, columns=fund_index)
    holding_presence = pd.DataFrame(pd.NA, index=symbol_index, columns=fund_index, dtype="boolean")
    industry_presence = pd.DataFrame(pd.NA, index=industry_index, columns=fund_index, dtype="boolean")
    overlap_counts = pd.DataFrame(float("nan"), index=fund_index, columns=fund_index)
    fund_rows = []

    for code, item in fund_items.items():
        rows = positions[code]
        weights = [record["weight"] for record in rows.values()]
        all_valid = bool(rows) and all(isfinite(weight) for weight in weights)
        total_weight = fsum(weights) if all_valid else float("nan")
        observed_weight = total_weight if not invalid_symbols[code] else float("nan")
        if isfinite(total_weight) and total_weight > 100 + 1e-9:
            warnings.append(f"{code} 的已观察权重合计超过 100%，合计按缺失处理，请核对来源。")
            observed_weight = float("nan")
        status = "no_data" if not rows else (
            "available" if isfinite(observed_weight) else "partial"
        )
        report_date = _report_date(item.get("latest_end_date"))
        if rows and pd.isna(report_date):
            warnings.append(f"{code} 缺少持仓报告期，无法确认是否同一期披露。")
        fund_rows.append({
            "fund_name": _text(item.get("fund_name")) or _text(item.get("name")) or code,
            "report_date": report_date,
            "holding_count": len(rows),
            "observed_weight": observed_weight,
            "data_status": status,
        })
        if not rows:
            continue

        holding_presence.loc[:, code] = False
        industry_presence.loc[:, code] = False
        grouped_weights: dict[str, list[float]] = defaultdict(list)
        for symbol, record in rows.items():
            holding_presence.loc[symbol, code] = True
            holdings.loc[symbol, code] = record["weight"]
            grouped_weights[stock_industries[symbol]].append(record["weight"])
        for industry, values in grouped_weights.items():
            industry_presence.loc[industry, code] = True
            if all(isfinite(value) for value in values):
                total = fsum(values)
                if total <= 100 + 1e-9:
                    industries.loc[industry, code] = total
                else:
                    warnings.append(f"{code} 的“{industry}”权重合计超过 100%，该行业权重按缺失处理。")

        for other_code, other_rows in positions.items():
            if other_rows:
                overlap_counts.loc[code, other_code] = len(rows.keys() & other_rows.keys())

    funds = pd.DataFrame(fund_rows, index=fund_index, columns=_FUND_COLUMNS)
    if funds.loc[funds["holding_count"] > 0, "report_date"].nunique() > 1:
        warnings.append("所选基金的持仓报告期不同，矩阵反映各自已加载披露，不能视作同一时点的持仓比较。")

    stock_details = pd.DataFrame([
        {
            "stock_name": stock_names.get(symbol, symbol),
            "industry": stock_industries[symbol],
            "observed_fund_count": sum(symbol in rows for rows in positions.values()),
        }
        for symbol in symbols
    ], index=symbol_index, columns=["stock_name", "industry", "observed_fund_count"])
    return {
        "funds": funds,
        "holdings": holdings,
        "industries": industries,
        "holding_presence": holding_presence,
        "industry_presence": industry_presence,
        "overlap_counts": overlap_counts,
        "stock_details": stock_details,
        "warnings": list(dict.fromkeys(warnings)),
    }
