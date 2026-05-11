from __future__ import annotations

from collections.abc import Mapping

from .models import (
    CODE_PATTERN,
    build_mention_id,
    extract_target_text,
    infer_direction,
    normalize_timestamp,
    normalize_ts_code,
)


def _build_mention(
    *,
    post: dict,
    ts_code: str,
    source_type: str,
    mention_time,
    security_name: str = "",
    text: str = "",
    reply_id=None,
    image_index=None,
    confidence_score: float = 0.8,
    rule_version: str = "v1",
) -> dict:
    reason_text = str(text or "").strip()
    symbol = ts_code.split(".", 1)[0]
    return {
        "mention_id": build_mention_id(post.get("post_id"), reply_id, image_index, ts_code, source_type, mention_time),
        "author_uid": ((post.get("post_user") or {}).get("user_id") or "").strip() or None,
        "post_id": post.get("post_id"),
        "reply_id": reply_id,
        "ts_code": ts_code,
        "symbol": symbol,
        "security_name": security_name or None,
        "mention_time": normalize_timestamp(mention_time).strftime("%Y-%m-%d %H:%M:%S"),
        "source_type": source_type,
        "direction": infer_direction(reason_text),
        "confidence_score": confidence_score,
        "target_text": extract_target_text(reason_text),
        "risk_text": None,
        "reason_text": reason_text or None,
        "rule_version": rule_version,
        "evidence_payload": {
            "post_id": post.get("post_id"),
            "reply_id": reply_id,
            "image_index": image_index,
        },
    }


def _extract_codes_from_text(text: str) -> list[str]:
    codes: list[str] = []
    for match in CODE_PATTERN.finditer(str(text or "")):
        normalized = normalize_ts_code(match.group(1))
        if normalized and normalized not in codes:
            codes.append(normalized)
    return codes


def _extract_alias_hits(text: str, stock_name_aliases: Mapping[str, dict]) -> list[dict]:
    text_value = str(text or "")
    hits: list[dict] = []
    for alias, payload in stock_name_aliases.items():
        alias_text = str(alias or "").strip()
        if alias_text and alias_text in text_value:
            ts_code = normalize_ts_code(payload.get("ts_code"))
            if ts_code:
                hits.append(
                    {
                        "ts_code": ts_code,
                        "name": str(payload.get("name") or alias_text).strip() or alias_text,
                    }
                )
    return hits


def extract_stock_mentions(
    post: dict,
    stock_name_aliases: Mapping[str, dict] | None = None,
    ocr_records: list[dict] | None = None,
    rule_version: str = "v1",
) -> list[dict]:
    aliases = stock_name_aliases or {}
    ocr_payloads = ocr_records or []
    mentions: list[dict] = []
    seen_keys: set[tuple[str, str, int | None]] = set()

    title = str(post.get("post_title") or "").strip()
    content = str(post.get("post_content") or "").strip()
    publish_time = post.get("post_publish_time") or post.get("post_last_time")
    combined_text = " ".join(part for part in [title, content] if part)

    primary_codes: list[str] = []

    stockbar = post.get("post_guba") or {}
    stockbar_code = normalize_ts_code(stockbar.get("stockbar_code"))
    if stockbar_code:
        primary_codes.append(stockbar_code)
        key = (stockbar_code, "stockbar", None)
        seen_keys.add(key)
        mentions.append(
            _build_mention(
                post=post,
                ts_code=stockbar_code,
                source_type="stockbar",
                mention_time=publish_time,
                security_name=str(stockbar.get("stockbar_name") or "").replace("吧", ""),
                text=combined_text,
                confidence_score=0.99,
                rule_version=rule_version,
            )
        )

    text_hits: list[tuple[str, str]] = []
    for ts_code in _extract_codes_from_text(combined_text):
        text_hits.append((ts_code, ""))
    for alias_hit in _extract_alias_hits(combined_text, aliases):
        text_hits.append((alias_hit["ts_code"], alias_hit["name"]))

    for ts_code, name in text_hits:
        if stockbar_code and ts_code == stockbar_code:
            continue
        if ts_code not in primary_codes:
            primary_codes.append(ts_code)
        key = (ts_code, "title_body", None)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        mentions.append(
            _build_mention(
                post=post,
                ts_code=ts_code,
                source_type="title_body",
                mention_time=publish_time,
                security_name=name,
                text=combined_text,
                confidence_score=0.9 if name else 0.95,
                rule_version=rule_version,
            )
        )

    for reply in post.get("reply_list") or []:
        if not reply.get("reply_is_author"):
            continue
        reply_text = str(reply.get("reply_text") or "").strip()
        reply_time = reply.get("reply_time") or publish_time
        reply_hits = _extract_codes_from_text(reply_text)
        reply_alias_hits = _extract_alias_hits(reply_text, aliases)
        if not reply_hits and not reply_alias_hits and len(primary_codes) == 1:
            reply_hits = [primary_codes[0]]

        resolved_hits: list[tuple[str, str]] = [(ts_code, "") for ts_code in reply_hits]
        resolved_hits.extend((item["ts_code"], item["name"]) for item in reply_alias_hits)
        for ts_code, name in resolved_hits:
            key = (ts_code, "author_reply", reply.get("reply_id"))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            mentions.append(
                _build_mention(
                    post=post,
                    ts_code=ts_code,
                    source_type="author_reply",
                    mention_time=reply_time,
                    security_name=name,
                    text=reply_text,
                    reply_id=reply.get("reply_id"),
                    confidence_score=0.88,
                    rule_version=rule_version,
                )
            )

    for ocr_record in ocr_payloads:
        ocr_text = str(ocr_record.get("ocr_text") or "").strip()
        if not ocr_text:
            continue
        resolved_hits: list[tuple[str, str]] = [(ts_code, "") for ts_code in _extract_codes_from_text(ocr_text)]
        resolved_hits.extend((item["ts_code"], item["name"]) for item in _extract_alias_hits(ocr_text, aliases))
        for ts_code, name in resolved_hits:
            key = (ts_code, "image_ocr", ocr_record.get("image_index"))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            mentions.append(
                _build_mention(
                    post=post,
                    ts_code=ts_code,
                    source_type="image_ocr",
                    mention_time=publish_time,
                    security_name=name,
                    text=ocr_text,
                    image_index=ocr_record.get("image_index"),
                    confidence_score=0.7,
                    rule_version=rule_version,
                )
            )

    mentions.sort(key=lambda item: (item["mention_time"], item["source_type"], item["ts_code"]))
    return mentions
