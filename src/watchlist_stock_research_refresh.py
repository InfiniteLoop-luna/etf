from __future__ import annotations

import inspect
import time
import uuid
from datetime import datetime
from typing import Callable, Any

from sqlalchemy.engine import Engine

from src.stock_research_analyzer import generate_stock_research_report_bundle
from src.stock_research_llm_analysis import should_require_stock_research_refresh
from src.stock_research_report_store import (
    ensure_tables,
    get_daily_report,
    get_report_status,
    release_refresh_lock,
    save_daily_report,
    try_acquire_refresh_lock,
    upsert_report_status,
)
from src.watchlist_distribution_refresh import (
    get_latest_source_trade_date,
    load_watchlist_stock_names,
    load_watchlist_stock_symbols,
)


WATCHLIST_STOCK_RESEARCH_LOCK_NAME = "watchlist_stock_research_refresh"


def _supports_kwarg(callable_obj: Callable[..., Any], kwarg_name: str) -> bool:
    try:
        sig = inspect.signature(callable_obj)
    except (TypeError, ValueError):
        return True

    params = sig.parameters.values()
    if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in params):
        return True
    return kwarg_name in sig.parameters


def _coerce_report_bundle(result: Any) -> dict[str, Any]:
    if isinstance(result, dict):
        return {
            "report_md": str(result.get("report_md") or ""),
            "fact_pack": result.get("fact_pack"),
            "llm_result": result.get("llm_result"),
        }
    return {"report_md": str(result or ""), "fact_pack": None, "llm_result": None}


def refresh_watchlist_stock_research_reports(
    engine: Engine,
    report_generator: Callable[..., Any] | None = None,
    *,
    username: str | None = None,
) -> dict[str, int]:
    ensure_tables(engine)
    report_generator = report_generator or generate_stock_research_report_bundle
    scope_username = str(username or "").strip() or None
    owner_id = f"stock-research-refresh-{uuid.uuid4().hex[:12]}"
    lock_name = WATCHLIST_STOCK_RESEARCH_LOCK_NAME if scope_username is None else f"{WATCHLIST_STOCK_RESEARCH_LOCK_NAME}:{scope_username}"
    if not try_acquire_refresh_lock(engine, lock_name, owner_id=owner_id, timeout_seconds=7200):
        return {"processed": 0, "generated": 0, "skipped": 0, "failed": 0, "locked": 1}

    symbols = load_watchlist_stock_symbols(engine, username=scope_username)
    names = load_watchlist_stock_names(engine, username=scope_username)
    summary = {"processed": 0, "generated": 0, "skipped": 0, "failed": 0, "locked": 0}

    try:
        for ts_code in symbols:
            summary["processed"] += 1
            stock_name = names.get(ts_code, ts_code)
            latest_source_trade_date = get_latest_source_trade_date(engine, ts_code)
            if not latest_source_trade_date:
                upsert_report_status(
                    engine,
                    ts_code,
                    status="failed",
                    error_message="missing source trade date",
                    target_trade_date=None,
                    last_attempt_at=datetime.now(),
                )
                summary["failed"] += 1
                continue

            current_status = get_report_status(engine, ts_code) or {}
            cached_report = get_daily_report(engine, ts_code, latest_source_trade_date)
            has_cached_report = bool(cached_report)
            cache_needs_refresh = has_cached_report and should_require_stock_research_refresh(cached_report)
            if (
                current_status.get("status") == "ready"
                and current_status.get("latest_ready_trade_date") == latest_source_trade_date
                and has_cached_report
                and not cache_needs_refresh
            ):
                upsert_report_status(
                    engine,
                    ts_code,
                    status="ready",
                    target_trade_date=latest_source_trade_date,
                    latest_ready_trade_date=latest_source_trade_date,
                    last_attempt_at=datetime.now(),
                    last_success_at=current_status.get("last_success_at") or datetime.now(),
                    latest_report_generated_at=current_status.get("latest_report_generated_at") or datetime.now(),
                    error_message=None,
                )
                summary["skipped"] += 1
                continue

            started_at = time.time()
            upsert_report_status(
                engine,
                ts_code,
                status="running",
                target_trade_date=latest_source_trade_date,
                latest_ready_trade_date=current_status.get("latest_ready_trade_date"),
                last_attempt_at=datetime.now(),
                error_message=None,
            )
            try:
                call_kwargs = {
                    "engine": engine,
                    "asof_trade_date": latest_source_trade_date,
                    "allow_live_fetch": False,
                }
                if _supports_kwarg(report_generator, "use_report_cache"):
                    call_kwargs["use_report_cache"] = False
                if _supports_kwarg(report_generator, "save_report"):
                    call_kwargs["save_report"] = False

                bundle = _coerce_report_bundle(
                    report_generator(
                        ts_code,
                        stock_name,
                        **call_kwargs,
                    )
                )
                if not bundle["report_md"]:
                    raise RuntimeError("empty stock research report")
                save_daily_report(
                    engine,
                    ts_code,
                    latest_source_trade_date,
                    bundle["report_md"],
                    fact_pack=bundle.get("fact_pack"),
                    llm_result=bundle.get("llm_result"),
                )
                duration_ms = int((time.time() - started_at) * 1000)
                now = datetime.now()
                upsert_report_status(
                    engine,
                    ts_code,
                    status="ready",
                    target_trade_date=latest_source_trade_date,
                    latest_ready_trade_date=latest_source_trade_date,
                    latest_report_generated_at=now,
                    last_attempt_at=now,
                    last_success_at=now,
                    duration_ms=duration_ms,
                    error_message=None,
                )
                summary["generated"] += 1
            except Exception as exc:
                duration_ms = int((time.time() - started_at) * 1000)
                upsert_report_status(
                    engine,
                    ts_code,
                    status="failed",
                    target_trade_date=latest_source_trade_date,
                    latest_ready_trade_date=current_status.get("latest_ready_trade_date"),
                    last_attempt_at=datetime.now(),
                    duration_ms=duration_ms,
                    error_message=str(exc),
                )
                summary["failed"] += 1
    finally:
        release_refresh_lock(engine, lock_name, owner_id=owner_id)

    return summary
