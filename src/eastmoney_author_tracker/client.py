from __future__ import annotations

from typing import Any

import requests

API_BASE_URL = "https://i.eastmoney.com/api/guba"


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
