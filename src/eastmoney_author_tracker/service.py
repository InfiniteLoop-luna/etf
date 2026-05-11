from __future__ import annotations

from typing import Any, Callable

from .client import fetch_userdynamiclist_page
from .cycles import build_stock_cycles, score_cycles
from .extract import extract_stock_mentions
from .ocr import NullOcrProvider
from .store import (
    build_author_summary,
    ensure_storage_objects,
    list_cycles_with_scores,
    replace_all_cycles,
    replace_all_mentions,
    replace_author_activity,
)


def sync_author_activity(
    engine,
    author_uid: str,
    fetch_page_fn: Callable[[int], list[dict[str, Any]]] | None = None,
    *,
    stock_name_aliases: dict[str, dict] | None = None,
    price_history_by_code: dict[str, list[dict[str, Any]]] | None = None,
    max_pages: int = 5,
    page_size: int = 20,
    ocr_provider=None,
) -> dict[str, Any]:
    ensure_storage_objects(engine)
    page_fetcher = fetch_page_fn or (lambda page_num: fetch_userdynamiclist_page(author_uid, page_num=page_num, page_size=page_size))
    resolved_ocr_provider = ocr_provider or NullOcrProvider()

    posts: list[dict[str, Any]] = []
    image_records_by_post: dict[int, list[dict[str, Any]]] = {}
    for page_num in range(1, int(max_pages) + 1):
        page_posts = page_fetcher(page_num)
        if not page_posts:
            break
        posts.extend(page_posts)
        for post in page_posts:
            image_records_by_post[int(post.get("post_id") or 0)] = resolved_ocr_provider.extract_post_images(post)

    mentions: list[dict[str, Any]] = []
    for post in posts:
        mentions.extend(
            extract_stock_mentions(
                post,
                stock_name_aliases=stock_name_aliases or {},
                ocr_records=image_records_by_post.get(int(post.get("post_id") or 0), []),
            )
        )

    cycles = build_stock_cycles(mentions)
    for cycle in cycles:
        cycle["author_uid"] = author_uid
    scores = score_cycles(cycles, price_history_by_code or {})

    replace_author_activity(engine, author_uid, posts, image_records_by_post=image_records_by_post)
    replace_all_mentions(engine, mentions)
    replace_all_cycles(engine, cycles, scores)

    cycle_rows = list_cycles_with_scores(engine)
    return {
        "author_uid": author_uid,
        "post_count": len(posts),
        "mention_count": len(mentions),
        "cycle_count": len(cycle_rows),
        "summary": build_author_summary(cycle_rows),
    }
