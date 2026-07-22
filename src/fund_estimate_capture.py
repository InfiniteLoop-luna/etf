from __future__ import annotations

from datetime import datetime, time
from typing import Iterable

import pandas as pd

from src.fund_estimate_snapshot_store import (
    ensure_fund_estimate_snapshot_table,
    list_distinct_fund_watchlist_codes,
    upsert_fund_estimate_snapshot,
)
from src.fund_intraday_estimator import (
    SHANGHAI_TZ,
    collect_fund_holding_symbols,
    enrich_fund_items_with_intraday_estimates,
    fetch_tencent_realtime_quotes,
)
from src.fund_watchlist_dashboard import build_fund_watchlist_item


CLOSE_CAPTURE_TIME = time(15, 0)
CLOSING_QUOTE_EARLIEST_TIME = time(15, 0)


def _build_snapshot_from_item(item: dict, current: datetime) -> tuple[dict | None, str | None]:
    quote_time = pd.to_datetime(item.get("intraday_updated_at"), errors="coerce")
    is_closing_quote = (
        not pd.isna(quote_time)
        and quote_time.date() == current.date()
        and quote_time.time() >= CLOSING_QUOTE_EARLIEST_TIME
    )
    if item.get("intraday_estimate_pct") is None or not is_closing_quote:
        return None, "no_valid_closing_quote"

    snapshot = {
        "fund_code": item["fund_code"],
        "estimate_date": current.date(),
        "estimate_pct": item["intraday_estimate_pct"],
        "covered_weight_pct": item.get("intraday_covered_weight_pct"),
        "top10_coverage_pct": item.get("intraday_top10_coverage_pct"),
        "quote_count": item.get("intraday_quote_count"),
        "holding_count": item.get("intraday_holding_count"),
        "quote_time": quote_time,
        "holding_end_date": item.get("latest_end_date"),
        "source": item.get("intraday_source"),
    }
    return snapshot, None


def _shanghai_now(now: datetime | None = None) -> datetime:
    current = now or datetime.now(SHANGHAI_TZ)
    if current.tzinfo is None:
        return current.replace(tzinfo=SHANGHAI_TZ)
    return current.astimezone(SHANGHAI_TZ)


def capture_fund_watchlist_closing_estimates(
    engine,
    *,
    now: datetime | None = None,
    fund_codes: Iterable[str] | None = None,
    require_after_close: bool = True,
    holding_loader=None,
    quote_fetcher=fetch_tencent_realtime_quotes,
    snapshot_writer=upsert_fund_estimate_snapshot,
) -> dict:
    """Capture current-day closing estimates for every distinct watchlist fund."""
    from src.fund_hot_stocks import query_fund_preference_snapshot

    current = _shanghai_now(now)
    summary = {
        "status": "ok",
        "estimate_date": current.date().isoformat(),
        "fund_count": 0,
        "symbol_count": 0,
        "captured_count": 0,
        "skipped_count": 0,
        "errors": [],
    }
    if current.weekday() >= 5:
        summary["status"] = "skipped_weekend"
        return summary
    if require_after_close and current.time() < CLOSE_CAPTURE_TIME:
        summary["status"] = "skipped_before_close"
        return summary

    normalized_codes = sorted(
        {
            str(code or "").strip().upper()
            for code in (
                fund_codes
                if fund_codes is not None
                else list_distinct_fund_watchlist_codes(engine)
            )
            if str(code or "").strip()
        }
    )
    summary["fund_count"] = len(normalized_codes)
    if not normalized_codes:
        summary["status"] = "no_watchlist_funds"
        return summary

    actual_holding_loader = holding_loader or query_fund_preference_snapshot
    items = []
    for fund_code in normalized_codes:
        try:
            holding_df = actual_holding_loader(
                fund_code=fund_code,
                top_n=10,
                engine=engine,
            )
            items.append(
                build_fund_watchlist_item(
                    pd.Series({"ts_code": fund_code, "security_name": fund_code}),
                    pd.DataFrame(),
                    holding_df,
                )
            )
        except Exception as exc:
            summary["errors"].append(f"{fund_code}: 持仓读取失败：{exc}")

    symbols = collect_fund_holding_symbols(items)
    summary["symbol_count"] = len(symbols)
    if not symbols:
        summary["status"] = "no_holding_symbols"
        summary["skipped_count"] = len(normalized_codes)
        return summary

    try:
        quotes = quote_fetcher(symbols)
    except Exception as exc:
        summary["status"] = "quote_fetch_failed"
        summary["errors"].append(f"收盘行情读取失败：{exc}")
        summary["skipped_count"] = len(normalized_codes)
        return summary

    enriched_items = enrich_fund_items_with_intraday_estimates(
        items,
        quotes,
        market_date=current.date(),
    )
    ensure_fund_estimate_snapshot_table(engine)
    for item in enriched_items:
        snapshot, snapshot_error = _build_snapshot_from_item(item, current)
        if snapshot is None:
            summary["skipped_count"] += 1
            continue
        try:
            snapshot_writer(engine, snapshot, ensure_table=False)
            summary["captured_count"] += 1
        except Exception as exc:
            summary["errors"].append(f"{item['fund_code']}: 快照保存失败：{exc}")
            summary["skipped_count"] += 1

    if summary["captured_count"] == 0 and summary["status"] == "ok":
        summary["status"] = "no_valid_closing_quotes"
    return summary


def backfill_single_fund_closing_estimate(
    engine,
    fund_code: str,
    *,
    now: datetime | None = None,
    holding_loader=None,
    quote_fetcher=fetch_tencent_realtime_quotes,
    snapshot_writer=upsert_fund_estimate_snapshot,
) -> dict:
    from src.fund_hot_stocks import query_fund_preference_snapshot

    current = _shanghai_now(now)
    normalized_code = str(fund_code or "").strip().upper()
    summary = {
        "status": "ok",
        "fund_code": normalized_code,
        "estimate_date": current.date().isoformat(),
        "captured": False,
        "errors": [],
    }
    if not normalized_code:
        summary["status"] = "invalid_fund_code"
        return summary
    if current.weekday() >= 5:
        summary["status"] = "skipped_weekend"
        return summary
    if current.time() < CLOSE_CAPTURE_TIME:
        summary["status"] = "skipped_before_close"
        return summary

    actual_holding_loader = holding_loader or query_fund_preference_snapshot
    try:
        holding_df = actual_holding_loader(
            fund_code=normalized_code,
            top_n=10,
            engine=engine,
        )
    except Exception as exc:
        summary["status"] = "holding_load_failed"
        summary["errors"].append(f"持仓读取失败：{exc}")
        return summary

    item = build_fund_watchlist_item(
        pd.Series({"ts_code": normalized_code, "security_name": normalized_code}),
        pd.DataFrame(),
        holding_df,
    )
    symbols = collect_fund_holding_symbols([item])
    if not symbols:
        summary["status"] = "no_holding_symbols"
        return summary

    try:
        quotes = quote_fetcher(symbols)
    except Exception as exc:
        summary["status"] = "quote_fetch_failed"
        summary["errors"].append(f"收盘行情读取失败：{exc}")
        return summary

    enriched_item = enrich_fund_items_with_intraday_estimates(
        [item],
        quotes,
        market_date=current.date(),
    )[0]
    snapshot, snapshot_error = _build_snapshot_from_item(enriched_item, current)
    if snapshot is None:
        summary["status"] = snapshot_error or "no_valid_closing_quote"
        return summary

    ensure_fund_estimate_snapshot_table(engine)
    try:
        snapshot_writer(engine, snapshot, ensure_table=False)
        summary["captured"] = True
    except Exception as exc:
        summary["status"] = "snapshot_write_failed"
        summary["errors"].append(f"快照保存失败：{exc}")
    return summary
