from __future__ import annotations

import inspect
import time
import uuid
from datetime import datetime
from typing import Callable, Any

from sqlalchemy.engine import Engine

from src.stock_research_akshare_enrichment import should_enable_stock_research_akshare
from src.stock_research_analyzer import generate_stock_research_report_bundle
from src.stock_research_html_renderer import render_stock_research_html
from src.stock_research_llm_analysis import should_require_stock_research_refresh
from src.stock_research_report_store import (
    ensure_tables,
    get_daily_report_record,
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


def _normalize_requested_ts_code(ts_code: str | None) -> str:
    code = str(ts_code or "").strip().upper()
    if not code:
        return ""
    if "." in code:
        return code
    if code.startswith(("SH", "SZ", "BJ")) and len(code) > 2:
        code = code[2:]
    if len(code) == 6 and code.isdigit():
        if code.startswith(("60", "68", "11", "12", "5")):
            return f"{code}.SH"
        if code.startswith(("4", "8", "92")):
            return f"{code}.BJ"
        return f"{code}.SZ"
    return code


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
            "report_html": str(result.get("report_html") or "") if result.get("report_html") is not None else None,
            "fact_pack": result.get("fact_pack"),
            "llm_result": result.get("llm_result"),
        }
    return {"report_md": str(result or ""), "report_html": None, "fact_pack": None, "llm_result": None}


def _needs_enriched_fact_pack_refresh(cached_record: dict | None, live_fetch_enabled: bool) -> bool:
    if not live_fetch_enabled:
        return False
    fact_pack = (cached_record or {}).get("fact_pack")
    if not isinstance(fact_pack, dict):
        return True
    if fact_pack.get("schema_version") != "stock-research-fact-pack-v2":
        return True
    supplemental = fact_pack.get("supplemental")
    if not isinstance(supplemental, dict) or not supplemental:
        return True
    quality = fact_pack.get("data_quality") or {}
    if not quality.get("supplemental_enabled"):
        return True
    statuses = [
        str((block or {}).get("status") or "missing")
        for block in supplemental.values()
        if isinstance(block, dict)
    ]
    return bool(statuses) and all(status in {"disabled", "missing"} for status in statuses)


def refresh_watchlist_stock_research_reports(
    engine: Engine,
    report_generator: Callable[..., Any] | None = None,
    *,
    username: str | None = None,
    limit: int | None = None,
    only_code: str | None = None,
    force: bool = False,
) -> dict[str, int]:
    ensure_tables(engine)
    report_generator = report_generator or generate_stock_research_report_bundle
    live_fetch_enabled = should_enable_stock_research_akshare()
    scope_username = str(username or "").strip() or None
    owner_id = f"stock-research-refresh-{uuid.uuid4().hex[:12]}"
    lock_name = WATCHLIST_STOCK_RESEARCH_LOCK_NAME if scope_username is None else f"{WATCHLIST_STOCK_RESEARCH_LOCK_NAME}:{scope_username}"
    requested_code = _normalize_requested_ts_code(only_code)
    if not try_acquire_refresh_lock(engine, lock_name, owner_id=owner_id, timeout_seconds=7200):
        if requested_code:
            current_status = get_report_status(engine, requested_code) or {}
            if current_status.get("status") == "running":
                upsert_report_status(
                    engine,
                    requested_code,
                    status="failed",
                    target_trade_date=current_status.get("target_trade_date"),
                    latest_ready_trade_date=current_status.get("latest_ready_trade_date"),
                    last_attempt_at=datetime.now(),
                    duration_ms=current_status.get("duration_ms"),
                    error_message="stale running status encountered while refresh lock is held",
                )
        return {"processed": 0, "generated": 0, "skipped": 0, "failed": 0, "locked": 1}

    symbols = load_watchlist_stock_symbols(engine, username=scope_username)
    if requested_code:
        symbols = [symbol for symbol in symbols if symbol == requested_code]
    if limit is not None:
        symbols = symbols[: max(0, int(limit))]
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
            cached_record = get_daily_report_record(engine, ts_code, latest_source_trade_date)
            cached_report = str((cached_record or {}).get("report_md") or "")
            cached_html = str((cached_record or {}).get("report_html") or "")
            has_cached_report = bool(cached_report)
            cache_needs_refresh = bool(force) or (
                has_cached_report and (
                    should_require_stock_research_refresh(cached_report)
                    or not cached_html
                    or _needs_enriched_fact_pack_refresh(cached_record, live_fetch_enabled)
                )
            )
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

            if (
                has_cached_report
                and not force
                and not cached_html
                and not should_require_stock_research_refresh(cached_report)
                and isinstance((cached_record or {}).get("fact_pack"), dict)
                and isinstance((cached_record or {}).get("llm_result"), dict)
            ):
                started_at = time.time()
                try:
                    report_html = render_stock_research_html(
                        cached_record["fact_pack"],
                        cached_record["llm_result"],
                        report_md=cached_report,
                    )
                    save_daily_report(
                        engine,
                        ts_code,
                        latest_source_trade_date,
                        cached_report,
                        report_html=report_html,
                        fact_pack=cached_record.get("fact_pack"),
                        llm_result=cached_record.get("llm_result"),
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
                    continue
                except Exception as exc:
                    upsert_report_status(
                        engine,
                        ts_code,
                        status="failed",
                        target_trade_date=latest_source_trade_date,
                        latest_ready_trade_date=current_status.get("latest_ready_trade_date"),
                        last_attempt_at=datetime.now(),
                        duration_ms=int((time.time() - started_at) * 1000),
                        error_message=str(exc),
                    )
                    summary["failed"] += 1
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
                    "allow_live_fetch": live_fetch_enabled,
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
                if (
                    not bundle.get("report_html")
                    and isinstance(bundle.get("fact_pack"), dict)
                    and isinstance(bundle.get("llm_result"), dict)
                ):
                    bundle["report_html"] = render_stock_research_html(
                        bundle["fact_pack"],
                        bundle["llm_result"],
                        report_md=bundle["report_md"],
                    )
                save_daily_report(
                    engine,
                    ts_code,
                    latest_source_trade_date,
                    bundle["report_md"],
                    report_html=bundle.get("report_html"),
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
