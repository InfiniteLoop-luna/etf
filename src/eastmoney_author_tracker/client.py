from __future__ import annotations

import time
from typing import Any

import requests

API_BASE_URL = "https://i.eastmoney.com/api/guba"
REPLY_API_URL = "https://guba.eastmoney.com/api/getData"
REPLY_API_PATH = "reply/api/Reply/ArticleNewReplyList"
REPLY_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)


def _build_reply_session(session: requests.Session | None = None) -> requests.Session:
    http_client = session or requests.Session()
    if not http_client.cookies.get("st_pvi"):
        http_client.cookies.set("st_pvi", str(int(time.time() * 1000)))
    return http_client


def build_eastmoney_session(session: requests.Session | None = None) -> requests.Session:
    return _build_reply_session(session)


def build_userdynamiclist_params(author_uid: str, page_num: int, page_size: int = 20, post_type: int = 0) -> dict[str, Any]:
    return {
        "uid": str(author_uid).strip(),
        "pagenum": int(page_num),
        "pagesize": int(page_size),
        "type": int(post_type),
    }


def parse_userdynamiclist_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if not payload or not payload.get("re"):
        return []

    posts: list[dict[str, Any]] = []
    for item in payload.get("result") or []:
        if not isinstance(item, dict):
            continue
        normalized = dict(item)
        normalized["post_pic_url"] = list(item.get("post_pic_url") or [])
        normalized["reply_list"] = list(item.get("reply_list") or [])
        normalized["post_guba"] = dict(item.get("post_guba") or {})
        normalized["raw_payload"] = dict(item)
        posts.append(normalized)
    return posts


def _iter_nested_replies(reply: dict[str, Any]) -> list[dict[str, Any]]:
    collected: list[dict[str, Any]] = []
    for child in reply.get("child_replys") or []:
        if not isinstance(child, dict):
            continue
        collected.append(child)
        collected.extend(_iter_nested_replies(child))
    return collected


def _normalize_reply_item(reply: dict[str, Any], *, author_uid: str | None = None) -> dict[str, Any]:
    normalized = dict(reply)
    normalized["child_replys"] = [dict(item) for item in reply.get("child_replys") or [] if isinstance(item, dict)]
    reply_user = normalized.get("reply_user")
    if isinstance(reply_user, dict):
        normalized.setdefault("user_id", reply_user.get("user_id"))
    reply_author_uid = str(normalized.get("user_id") or "").strip()
    normalized["reply_text"] = (
        normalized.get("reply_text")
        or normalized.get("reply_content")
        or normalized.get("reply_full_text")
        or normalized.get("content")
        or ""
    )
    normalized["reply_is_author"] = bool(normalized.get("reply_is_author")) or (
        bool(author_uid) and reply_author_uid == str(author_uid).strip()
    )
    return normalized


def parse_article_reply_payload(payload: dict[str, Any], *, author_uid: str | None = None) -> list[dict[str, Any]]:
    reply_root = payload.get("re") if isinstance(payload, dict) else None
    reply_rows = None
    if isinstance(reply_root, list):
        reply_rows = reply_root
    elif isinstance(reply_root, dict):
        reply_rows = reply_root.get("list")
    if not isinstance(reply_rows, list):
        return []

    seen_reply_ids: set[int] = set()
    author_replies: list[dict[str, Any]] = []
    for reply in reply_rows:
        if not isinstance(reply, dict):
            continue
        candidates = [reply, *_iter_nested_replies(reply)]
        for candidate in candidates:
            normalized = _normalize_reply_item(candidate, author_uid=author_uid)
            if not normalized.get("reply_is_author"):
                continue
            reply_id = normalized.get("reply_id")
            try:
                normalized_reply_id = int(reply_id)
            except (TypeError, ValueError):
                continue
            if normalized_reply_id in seen_reply_ids:
                continue
            seen_reply_ids.add(normalized_reply_id)
            author_replies.append(normalized)
    author_replies.sort(key=lambda item: (str(item.get("reply_time") or ""), int(item.get("reply_id") or 0)))
    return author_replies


def fetch_article_reply_page(
    post_id: int | str,
    stockbar_code: str,
    *,
    author_uid: str | None = None,
    page_num: int = 1,
    page_size: int = 30,
    session: requests.Session | None = None,
    timeout: int = 15,
) -> list[dict[str, Any]]:
    post_id_text = str(post_id or "").strip()
    stockbar_code_text = str(stockbar_code or "").strip()
    if not post_id_text or not stockbar_code_text:
        return []

    http_client = _build_reply_session(session)
    response = http_client.post(
        REPLY_API_URL,
        params={"code": stockbar_code_text, "path": REPLY_API_PATH},
        data={
            "param": f"postid={post_id_text}&sort=1&sorttype=1&p={int(page_num)}&ps={int(page_size)}",
            "plat": "Web",
            "path": REPLY_API_PATH,
            "env": "1",
            "origin": "",
            "version": "2022",
            "product": "Guba",
        },
        headers={
            "User-Agent": REPLY_USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://guba.eastmoney.com",
            "Referer": f"https://guba.eastmoney.com/news,{stockbar_code_text},{post_id_text}.html",
            "X-Requested-With": "XMLHttpRequest",
        },
        timeout=timeout,
    )
    response.raise_for_status()
    return parse_article_reply_payload(response.json(), author_uid=author_uid)


def fetch_post_author_replies(
    post_id: int | str,
    stockbar_code: str,
    *,
    author_uid: str | None = None,
    page_size: int = 30,
    max_pages: int = 10,
    session: requests.Session | None = None,
    timeout: int = 15,
) -> list[dict[str, Any]]:
    collected: list[dict[str, Any]] = []
    seen_reply_ids: set[int] = set()
    for page_num in range(1, max(int(max_pages or 0), 1) + 1):
        page_replies = fetch_article_reply_page(
            post_id,
            stockbar_code,
            author_uid=author_uid,
            page_num=page_num,
            page_size=page_size,
            session=session,
            timeout=timeout,
        )
        if not page_replies:
            break
        for reply in page_replies:
            reply_id = reply.get("reply_id")
            try:
                normalized_reply_id = int(reply_id)
            except (TypeError, ValueError):
                continue
            if normalized_reply_id in seen_reply_ids:
                continue
            seen_reply_ids.add(normalized_reply_id)
            collected.append(reply)
        if len(page_replies) < int(page_size):
            break
    collected.sort(key=lambda item: (str(item.get("reply_time") or ""), int(item.get("reply_id") or 0)))
    return collected


def fetch_userdynamiclist_page(
    author_uid: str,
    page_num: int,
    page_size: int = 20,
    post_type: int = 0,
    session: requests.Session | None = None,
    timeout: int = 15,
) -> list[dict[str, Any]]:
    http_client = session or requests.Session()
    response = http_client.get(
        f"{API_BASE_URL}/userdynamiclistv2",
        params=build_userdynamiclist_params(author_uid, page_num=page_num, page_size=page_size, post_type=post_type),
        timeout=timeout,
    )
    response.raise_for_status()
    return parse_userdynamiclist_payload(response.json())
