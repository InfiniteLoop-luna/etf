from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from .client import fetch_userdynamiclist_page
from .cycles import DEFAULT_BENCHMARK_TS_CODE, build_stock_cycles, score_cycles
from .extract import extract_stock_mentions
from .models import normalize_timestamp, normalize_ts_code
from .ocr import DeferredOcrProvider, NullOcrProvider
from .store import (
    build_author_summary,
    count_pending_author_images,
    ensure_storage_objects,
    load_author_image_records_map,
    load_mention_overrides_map,
    load_author_posts,
    load_existing_post_payloads,
    load_post_image_records_map,
    load_price_history_by_codes,
    list_pending_author_images,
    list_cycles_with_scores,
    replace_all_cycles,
    replace_all_mentions,
    replace_author_activity,
    update_post_image_ocr_result,
    upsert_author_score_snapshot,
)


def _apply_mention_overrides(
    mentions: list[dict[str, Any]],
    override_map: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    overrides = override_map or {}
    adjusted_mentions: list[dict[str, Any]] = []

    for mention in mentions:
        mention_id = str(mention.get("mention_id") or "").strip()
        override = overrides.get(mention_id)
        if override and bool(override.get("is_excluded")):
            continue

        adjusted = dict(mention)
        if override:
            override_ts_code = normalize_ts_code(override.get("override_ts_code"))
            if override_ts_code:
                adjusted["ts_code"] = override_ts_code
                adjusted["symbol"] = override_ts_code.split(".", 1)[0]
            override_direction = str(override.get("override_direction") or "").strip()
            if override_direction:
                adjusted["direction"] = override_direction
            if bool(override.get("force_new_cycle")):
                adjusted["force_new_cycle"] = True

            evidence_payload = dict(adjusted.get("evidence_payload") or {})
            evidence_payload["manual_override"] = {
                "override_ts_code": override.get("override_ts_code"),
                "override_direction": override.get("override_direction"),
                "force_new_cycle": bool(override.get("force_new_cycle")),
                "override_note": override.get("override_note"),
            }
            adjusted["evidence_payload"] = evidence_payload
        adjusted_mentions.append(adjusted)

    adjusted_mentions.sort(key=lambda item: (item["mention_time"], item["source_type"], item["ts_code"]))
    return adjusted_mentions


def _resolve_cycle_scores(
    engine,
    cycles: list[dict[str, Any]],
    price_history_by_code: dict[str, list[dict[str, Any]]] | None = None,
) -> list[dict[str, Any]]:
    resolved_price_history: dict[str, list[dict[str, Any]]] = {
        str(code): list(rows or [])
        for code, rows in (price_history_by_code or {}).items()
        if str(code or "").strip()
    }
    missing_codes = sorted(
        {
            str(cycle.get("ts_code") or "").strip()
            for cycle in cycles
            if str(cycle.get("ts_code") or "").strip()
            and not resolved_price_history.get(str(cycle.get("ts_code") or "").strip())
        }
    )
    if cycles and DEFAULT_BENCHMARK_TS_CODE not in resolved_price_history:
        missing_codes.append(DEFAULT_BENCHMARK_TS_CODE)
    missing_codes = sorted({code for code in missing_codes if str(code or "").strip()})
    if missing_codes:
        cycle_open_times = [str(cycle.get("cycle_open_time") or "").strip() for cycle in cycles if cycle.get("cycle_open_time")]
        start_date = min(cycle_open_times)[:10] if cycle_open_times else None
        try:
            resolved_price_history.update(load_price_history_by_codes(engine, missing_codes, start_date=start_date))
        except Exception:
            pass
    return score_cycles(cycles, resolved_price_history)


def _resolve_snapshot_date(snapshot_date: str | None = None) -> str:
    if snapshot_date:
        return normalize_timestamp(snapshot_date).date().isoformat()
    return datetime.now().date().isoformat()


def _merge_image_records(
    fresh_records: list[dict[str, Any]],
    existing_records: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    existing_by_index = {
        int(record.get("image_index") or 0): dict(record)
        for record in (existing_records or [])
    }
    merged_records: list[dict[str, Any]] = []
    for record in fresh_records:
        merged = dict(record)
        image_index = int(merged.get("image_index") or 0)
        existing = existing_by_index.get(image_index)
        if existing:
            new_status = str(merged.get("ocr_status") or "").strip()
            existing_status = str(existing.get("ocr_status") or "").strip()
            if (
                str(merged.get("image_url") or "").strip() == str(existing.get("image_url") or "").strip()
                and new_status in {"pending", "skipped"}
                and existing_status == "ok"
            ):
                merged = existing
        merged_records.append(merged)
    return merged_records


def rebuild_author_tracking_from_archive(
    engine,
    author_uid: str,
    *,
    stock_name_aliases: dict[str, dict] | None = None,
    price_history_by_code: dict[str, list[dict[str, Any]]] | None = None,
    snapshot_date: str | None = None,
) -> dict[str, Any]:
    ensure_storage_objects(engine)

    posts = load_author_posts(engine, author_uid)
    image_records_by_post = load_author_image_records_map(engine, author_uid)

    raw_mentions: list[dict[str, Any]] = []
    for post in posts:
        raw_mentions.extend(
            extract_stock_mentions(
                post,
                stock_name_aliases=stock_name_aliases or {},
                ocr_records=image_records_by_post.get(int(post.get("post_id") or 0), []),
            )
        )

    override_map = load_mention_overrides_map(engine, [item.get("mention_id") for item in raw_mentions])
    mentions = _apply_mention_overrides(raw_mentions, override_map)

    cycles = build_stock_cycles(mentions)
    for cycle in cycles:
        cycle["author_uid"] = author_uid

    scores = _resolve_cycle_scores(engine, cycles, price_history_by_code=price_history_by_code)

    replace_all_mentions(engine, mentions)
    replace_all_cycles(engine, cycles, scores)

    cycle_rows = list_cycles_with_scores(engine)
    summary = build_author_summary(cycle_rows)
    upsert_author_score_snapshot(
        engine,
        author_uid,
        snapshot_date=_resolve_snapshot_date(snapshot_date),
        summary=summary,
    )
    return {
        "author_uid": author_uid,
        "post_count": len(posts),
        "mention_count": len(mentions),
        "cycle_count": len(cycle_rows),
        "pending_image_count": count_pending_author_images(engine, author_uid),
        "summary": summary,
    }


def enrich_pending_author_images(
    engine,
    author_uid: str,
    *,
    ocr_provider,
    limit: int = 50,
    stock_name_aliases: dict[str, dict] | None = None,
    price_history_by_code: dict[str, list[dict[str, Any]]] | None = None,
    snapshot_date: str | None = None,
) -> dict[str, Any]:
    ensure_storage_objects(engine)
    if ocr_provider is None:
        raise ValueError("ocr_provider is required")

    pending_rows = list_pending_author_images(engine, author_uid, limit=limit)
    processed_image_count = 0
    for row in pending_rows:
        image_result = ocr_provider.extract_image(
            str(row.get("image_url") or ""),
            image_index=int(row.get("image_index") or 0),
        )
        update_post_image_ocr_result(
            engine,
            row.get("post_id"),
            int(row.get("image_index") or 0),
            image_url=image_result.get("image_url"),
            ocr_status=image_result.get("ocr_status"),
            ocr_text=image_result.get("ocr_text"),
            ocr_provider=image_result.get("ocr_provider"),
        )
        processed_image_count += 1

    if processed_image_count:
        result = rebuild_author_tracking_from_archive(
            engine,
            author_uid,
            stock_name_aliases=stock_name_aliases,
            price_history_by_code=price_history_by_code,
            snapshot_date=snapshot_date,
        )
    else:
        cycle_rows = list_cycles_with_scores(engine)
        result = {
            "author_uid": author_uid,
            "post_count": len(load_author_posts(engine, author_uid)),
            "mention_count": sum(int(row.get("event_count") or 0) for row in cycle_rows),
            "cycle_count": len(cycle_rows),
            "summary": build_author_summary(cycle_rows),
        }

    result["processed_image_count"] = processed_image_count
    result["pending_image_count"] = count_pending_author_images(engine, author_uid)
    return result


def sync_author_activity(
    engine,
    author_uid: str,
    fetch_page_fn: Callable[[int], list[dict[str, Any]]] | None = None,
    *,
    stock_name_aliases: dict[str, dict] | None = None,
    price_history_by_code: dict[str, list[dict[str, Any]]] | None = None,
    snapshot_date: str | None = None,
    max_pages: int = 5,
    page_size: int = 20,
    ocr_provider=None,
    unchanged_post_stop_count: int = 10,
) -> dict[str, Any]:
    ensure_storage_objects(engine)
    page_fetcher = fetch_page_fn or (lambda page_num: fetch_userdynamiclist_page(author_uid, page_num=page_num, page_size=page_size))
    resolved_ocr_provider = ocr_provider or DeferredOcrProvider()

    fetched_posts: list[dict[str, Any]] = []
    fetched_image_records_by_post: dict[int, list[dict[str, Any]]] = {}
    consecutive_known_unchanged = 0
    for page_num in range(1, int(max_pages) + 1):
        page_posts = page_fetcher(page_num)
        if not page_posts:
            break
        existing_payloads = load_existing_post_payloads(engine, author_uid, [post.get("post_id") for post in page_posts])
        existing_image_records_by_post = load_post_image_records_map(engine, [post.get("post_id") for post in page_posts])
        for post in page_posts:
            post_id = int(post.get("post_id") or 0)
            fetched_posts.append(post)
            fresh_image_records = resolved_ocr_provider.extract_post_images(post)
            fetched_image_records_by_post[post_id] = _merge_image_records(
                fresh_image_records,
                existing_image_records_by_post.get(post_id, []),
            )
            current_payload = post.get("raw_payload") or post
            existing_payload = existing_payloads.get(post_id)
            if existing_payload is not None and existing_payload == current_payload:
                consecutive_known_unchanged += 1
            else:
                consecutive_known_unchanged = 0
        if int(unchanged_post_stop_count or 0) > 0 and consecutive_known_unchanged >= int(unchanged_post_stop_count):
            break

    replace_author_activity(engine, author_uid, fetched_posts, image_records_by_post=fetched_image_records_by_post)
    return rebuild_author_tracking_from_archive(
        engine,
        author_uid,
        stock_name_aliases=stock_name_aliases,
        price_history_by_code=price_history_by_code,
        snapshot_date=snapshot_date,
    )
