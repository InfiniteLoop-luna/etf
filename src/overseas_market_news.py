from __future__ import annotations

import hashlib
import logging
import re
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Any, Callable
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
BEIJING_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_TIMEOUT_SECONDS = 10
MAX_FEED_BYTES = 2_000_000


OFFICIAL_FEEDS = (
    {
        "country_code": "US",
        "country": "美国",
        "source": "Federal Reserve",
        "url": "https://www.federalreserve.gov/feeds/press_all.xml",
        "source_type": "official",
        "source_tier": 1,
    },
    {
        "country_code": "US",
        "country": "美国",
        "source": "U.S. Bureau of Labor Statistics",
        "url": "https://www.bls.gov/feed/bls_latest.rss",
        "source_type": "official",
        "source_tier": 1,
    },
    {
        "country_code": "JP",
        "country": "日本",
        "source": "Bank of Japan",
        "url": "https://www.boj.or.jp/en/rss/whatsnew.xml",
        "source_type": "official",
        "source_tier": 1,
    },
    {
        "country_code": "KR",
        "country": "韩国",
        "source": "Bank of Korea",
        "url": "https://www.bok.or.kr/eng/bbs/E0000634/news.rss?menuNo=400069",
        "source_type": "official",
        "source_tier": 1,
    },
    {
        "country_code": "KR",
        "country": "韩国",
        "source": "Bank of Korea · Monetary Policy",
        "url": "https://www.bok.or.kr/eng/bbs/E0000627/news.rss?menuNo=400022",
        "source_type": "official",
        "source_tier": 1,
    },
)

MEDIA_QUERIES = {
    "US": "US economy markets when:2d",
    "JP": "Japan economy markets when:2d",
    "KR": "South Korea economy markets when:2d",
}

# These sources are supplemental discovery, never promoted to an official confirmation.
TRUSTED_MEDIA = {
    "reuters",
    "bloomberg",
    "cnbc",
    "financial times",
    "the wall street journal",
    "wall street journal",
    "nikkei asia",
    "the japan times",
    "associated press",
    "ap news",
    "yahoo finance",
    "marketwatch",
    "fortune",
    "morningstar",
    "yonhap news agency",
    "the korea herald",
    "the korea times",
    "koreatimes.co.kr",
    "chosunbiz",
}

MARKET_KEYWORDS = {
    "rate": 4,
    "interest": 3,
    "inflation": 4,
    "cpi": 4,
    "jobs": 3,
    "employment": 3,
    "payroll": 4,
    "gdp": 4,
    "treasury": 3,
    "bond": 3,
    "yield": 4,
    "dollar": 3,
    "yen": 3,
    "won": 3,
    "export": 3,
    "trade": 2,
    "tariff": 4,
    "sanction": 4,
    "semiconductor": 4,
    "chip": 4,
    "market": 2,
    "economy": 2,
    "monetary": 3,
    "liquidity": 3,
}

COUNTRY_TERMS = {
    "US": ("u.s.", " us ", "united states", "american", "federal reserve", "fed ", "wall street", "treasury", "dollar"),
    "JP": ("japan", "japanese", "bank of japan", "boj", "yen", "tokyo"),
    "KR": ("south korea", "korea", "korean", "bank of korea", "bok", "won", "seoul", "samsung", "sk hynix"),
}


def _clean_text(value: Any, max_length: int = 420) -> str:
    if value is None:
        return ""
    text = BeautifulSoup(unescape(str(value)), "html.parser").get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return text if len(text) <= max_length else text[: max_length - 1].rstrip() + "…"


def _first_text(node: ET.Element, names: tuple[str, ...]) -> str:
    for child in node.iter():
        local_name = child.tag.rsplit("}", 1)[-1].lower()
        if local_name in names and child.text:
            return child.text.strip()
    return ""


def _parse_datetime(value: Any, *, now: datetime | None = None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    parsed: datetime | None = None
    try:
        parsed = parsedate_to_datetime(text)
    except (TypeError, ValueError, OverflowError):
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=(now or datetime.now(timezone.utc)).tzinfo or timezone.utc)
    return parsed.astimezone(BEIJING_TZ)


def parse_feed(
    payload: bytes | str,
    feed: dict[str, Any],
    *,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Parse RSS 2.0 or Atom into a small, source-labelled news record."""
    raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    root = ET.fromstring(raw)
    nodes = root.findall(".//item")
    if not nodes:
        nodes = [node for node in root.iter() if node.tag.rsplit("}", 1)[-1].lower() == "entry"]

    reference_now = (now or datetime.now(BEIJING_TZ)).astimezone(BEIJING_TZ)
    items: list[dict[str, Any]] = []
    for node in nodes:
        title = _clean_text(_first_text(node, ("title",)), 240)
        if not title:
            continue
        link = _first_text(node, ("link",))
        if not link:
            for child in node.iter():
                if child.tag.rsplit("}", 1)[-1].lower() == "link" and child.attrib.get("href"):
                    link = child.attrib["href"].strip()
                    break
        if not link.lower().startswith(("http://", "https://")):
            continue
        published = _parse_datetime(
            _first_text(node, ("pubdate", "published", "updated", "date")),
            now=reference_now,
        )
        source_node = next(
            (child for child in node.iter() if child.tag.rsplit("}", 1)[-1].lower() == "source"),
            None,
        )
        publisher = _clean_text(source_node.text if source_node is not None else "", 80)
        source = publisher or str(feed.get("source") or "未知来源")
        summary = _clean_text(_first_text(node, ("description", "summary", "content")), 420)
        age_hours = None
        if published:
            age_hours = max(0.0, (reference_now - published).total_seconds() / 3600)
        fingerprint = hashlib.sha1(
            f"{feed.get('country_code')}|{title.lower()}|{link}".encode("utf-8")
        ).hexdigest()[:12]
        items.append({
            "news_id": f"overseas.{feed.get('country_code')}.{fingerprint}",
            "country_code": feed.get("country_code"),
            "country": feed.get("country"),
            "title": title,
            "summary": summary,
            "source": source,
            "published_at": published.isoformat(timespec="seconds") if published else None,
            "url": link,
            "source_type": feed.get("source_type") or "media",
            "source_tier": int(feed.get("source_tier") or 2),
            "verification_status": "官方发布" if feed.get("source_type") == "official" else "媒体报道",
            "age_hours": None if age_hours is None else round(age_hours, 1),
        })
    return items


def _google_news_feed(country_code: str) -> dict[str, Any]:
    query = MEDIA_QUERIES[country_code]
    return {
        "country_code": country_code,
        "country": {"US": "美国", "JP": "日本", "KR": "韩国"}[country_code],
        "source": "Google News",
        "url": "https://news.google.com/rss/search?" + urlencode({
            "q": query,
            "hl": "en-US",
            "gl": "US",
            "ceid": "US:en",
        }),
        "source_type": "media",
        "source_tier": 2,
    }


def _fetch_feed(feed: dict[str, Any], *, timeout: int = DEFAULT_TIMEOUT_SECONDS) -> bytes:
    import requests

    response = None
    for _ in range(2):
        response = requests.get(
            str(feed["url"]),
            headers={
                "User-Agent": "ETF-Morning-Report/1.0 (+local analytical dashboard)",
                "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
            },
            timeout=timeout,
        )
        if response.ok:
            break
    assert response is not None
    response.raise_for_status()
    if len(response.content) > MAX_FEED_BYTES:
        raise ValueError(f"feed exceeds {MAX_FEED_BYTES} bytes")
    return response.content


def _normalized_title(title: Any) -> str:
    value = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", str(title or "").lower())
    return re.sub(r"\s+", " ", value).strip()


def _market_relevance(item: dict[str, Any]) -> int:
    text = f"{item.get('title', '')} {item.get('summary', '')}".lower()
    return sum(weight for keyword, weight in MARKET_KEYWORDS.items() if keyword in text)


def _country_relevant(item: dict[str, Any]) -> bool:
    code = str(item.get("country_code") or "")
    text = f" {item.get('title', '')} {item.get('summary', '')} ".lower()
    return any(term in text for term in COUNTRY_TERMS.get(code, ()))


def _rank_and_select(
    items: list[dict[str, Any]],
    *,
    now: datetime,
    limit: int = 4,
) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for item in items:
        key = _normalized_title(item.get("title"))
        if not key:
            continue
        existing = deduped.get(key)
        if not existing or int(item.get("source_tier") or 9) < int(existing.get("source_tier") or 9):
            deduped[key] = item
    candidates = list(deduped.values())
    recent = [item for item in candidates if item.get("age_hours") is not None and item["age_hours"] <= 72]
    if not recent:
        recent = [item for item in candidates if item.get("age_hours") is not None and item["age_hours"] <= 24 * 14]
    pool = recent or candidates

    def rank(item: dict[str, Any]) -> tuple[Any, ...]:
        tier = int(item.get("source_tier") or 9)
        relevance = _market_relevance(item)
        age = float(item.get("age_hours") if item.get("age_hours") is not None else 10_000)
        return (tier, -relevance, age, str(item.get("title") or ""))

    official = sorted((item for item in pool if item.get("source_type") == "official"), key=rank)
    media = sorted((item for item in pool if item.get("source_type") == "media"), key=rank)
    selected: list[dict[str, Any]] = []
    if official:
        selected.append(official.pop(0))
    while len(selected) < limit and (official or media):
        if media:
            selected.append(media.pop(0))
        if len(selected) < limit and official:
            selected.append(official.pop(0))
    return sorted(selected[:limit], key=lambda item: float(item.get("age_hours") or 0))


def _signal_text(items: list[dict[str, Any]]) -> str:
    return " ".join(f"{item.get('title', '')} {item.get('summary', '')}" for item in items).lower()


def infer_a_share_impact(country_code: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    """Conservative rule fallback. LLM output may replace this after evidence validation."""
    country = {"US": "美国", "JP": "日本", "KR": "韩国"}.get(country_code, country_code)
    basis_ids = [str(item.get("news_id")) for item in items if item.get("news_id")]
    if not items:
        return {
            "country": country,
            "direction": "待验证",
            "strength": "低",
            "confidence": "低",
            "analysis": "未取得足够的新近资讯，暂不推演对次日A股的方向影响。",
            "channels": [],
            "affected_sectors": [],
            "invalidating_conditions": "补齐权威来源后重新评估。",
            "basis_news_ids": [],
            "method": "规则兜底",
        }

    text = _signal_text(items)
    positive = 0
    negative = 0
    channels: list[str] = []
    sectors: list[str] = []
    analysis_parts: list[str] = []

    if country_code == "US":
        channels = ["全球风险偏好", "美元与人民币汇率", "海外贴现率"]
        sectors = ["成长科技", "半导体", "外资敏感板块"]
        if any(word in text for word in ("rate cut", "cuts rates", "dovish", "inflation cool", "inflation eas", "yield fall")):
            positive += 2
            analysis_parts.append("若美国利率或通胀信号继续缓和，全球贴现率和美元压力可能下降。")
        if any(word in text for word in ("rate hike", "raises interest", "hawkish", "inflation acceler", "yield rise", "tariff")):
            negative += 2
            analysis_parts.append("若紧缩、通胀或关税压力延续，可能经美元、利率和风险偏好压制A股估值。")
        if any(word in text for word in ("export control", "chip restriction", "semiconductor restriction")):
            negative += 2
            analysis_parts.append("科技限制类事件对半导体供应链的影响更直接。")
    elif country_code == "JP":
        channels = ["日元与套息交易", "亚洲资金风险偏好", "区域制造业竞争"]
        sectors = ["汽车", "电子", "高端制造"]
        if any(word in text for word in ("rate hike", "raise interest", "raises interest", "hawkish", "yen strengthens", "yen surge", "jgb yield")):
            negative += 2
            positive += 2
            analysis_parts.append("日元走强或日本利率上行可能触发套息交易回撤，但也可能改善亚洲货币的相对压力。")
        if any(word in text for word in ("stimulus", "rate cut", "exports rise", "export growth")):
            positive += 1
            analysis_parts.append("日本需求或出口改善可为区域制造链提供边际支撑。")
    elif country_code == "KR":
        channels = ["半导体景气", "韩元与亚洲货币", "中日韩出口链"]
        sectors = ["半导体", "消费电子", "汽车零部件"]
        if any(word in text for word in ("chip exports rise", "semiconductor exports rise", "export growth", "exports rise", "stimulus", "rate cut")):
            positive += 2
            analysis_parts.append("韩国出口或芯片周期改善，通常会强化亚洲电子供应链景气验证。")
        if any(word in text for word in ("exports fall", "export decline")):
            negative += 2
            analysis_parts.append("韩国出口走弱可能削弱亚洲电子与制造链的景气验证。")
        if "won weakens" in text:
            negative += 2
            analysis_parts.append("韩元承压可能经亚洲货币与外资风险偏好影响A股。")
        if any(word in text for word in ("rate hike", "inflation acceler")):
            negative += 2
            analysis_parts.append("外部紧缩或通胀压力可能抬升区域贴现率并压制风险偏好。")
        if any(word in text for word in ("stabilize", "limited market impact")):
            positive += 1
            analysis_parts.append("当地稳市场措施或有限冲击判断可能缓冲上述压力，但仍需价格信号确认。")

    if positive and negative:
        direction = "双向"
    elif positive > negative:
        direction = "偏利好"
    elif negative > positive:
        direction = "偏利空"
    else:
        direction = "中性"
        analysis_parts.append("现有资讯尚未形成可验证的单一方向，更多体现为盘中观察变量。")
    score = max(positive, negative)
    strength = "高" if score >= 4 else ("中" if score >= 2 else "低")
    has_official = any(item.get("source_type") == "official" for item in items)
    has_media = any(item.get("source_type") == "media" for item in items)
    confidence = "中" if has_official and has_media and direction != "中性" else "低"
    return {
        "country": country,
        "direction": direction,
        "strength": strength,
        "confidence": confidence,
        "analysis": "".join(analysis_parts),
        "channels": channels,
        "affected_sectors": sectors,
        "invalidating_conditions": "若人民币汇率、亚洲股指期货或相关商品在A股开盘前未确认该方向，应下调影响权重。",
        "basis_news_ids": basis_ids[:4],
        "method": "规则兜底",
    }


def collect_overseas_market_news(
    *,
    now: datetime | None = None,
    fetcher: Callable[[dict[str, Any]], bytes] | None = None,
) -> dict[str, Any]:
    reference_now = (now or datetime.now(BEIJING_TZ)).astimezone(BEIJING_TZ)
    feeds = list(OFFICIAL_FEEDS) + [_google_news_feed(code) for code in ("US", "JP", "KR")]
    fetch = fetcher or _fetch_feed
    collected: list[dict[str, Any]] = []
    warnings: list[str] = []

    def load(feed: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        return feed, parse_feed(fetch(feed), feed, now=reference_now)

    with ThreadPoolExecutor(max_workers=min(8, len(feeds))) as executor:
        futures = {executor.submit(load, feed): feed for feed in feeds}
        for future in as_completed(futures):
            feed = futures[future]
            try:
                _, parsed = future.result()
                if feed.get("source_type") == "media":
                    parsed = [
                        item for item in parsed
                        if str(item.get("source") or "").strip().lower() in TRUSTED_MEDIA
                        and _country_relevant(item)
                    ]
                collected.extend(parsed)
            except Exception as exc:
                logger.warning("overseas news feed failed (%s): %s", feed.get("source"), exc)
                warnings.append(f"{feed.get('country')} · {feed.get('source')} 暂不可用")

    countries: dict[str, dict[str, Any]] = {}
    for code, label in (("US", "美国"), ("JP", "日本"), ("KR", "韩国")):
        country_items = _rank_and_select(
            [item for item in collected if item.get("country_code") == code],
            now=reference_now,
        )
        countries[code] = {
            "label": label,
            "items": country_items,
            "impact": infer_a_share_impact(code, country_items),
        }

    total_items = sum(len(country["items"]) for country in countries.values())
    successful_countries = sum(bool(country["items"]) for country in countries.values())
    status = "ok" if successful_countries == 3 and not warnings else ("partial" if total_items else "empty")
    return {
        "generated_at": reference_now.isoformat(timespec="seconds"),
        "status": status,
        "countries": countries,
        "warnings": warnings,
        "source_policy": "官方机构为事实锚点；白名单财经媒体仅作补充，媒体报道不等同于官方确认。",
        "analysis_policy": "方向为次日A股情景推演，不是点位或涨跌幅预测。",
    }


__all__ = [
    "collect_overseas_market_news",
    "infer_a_share_impact",
    "parse_feed",
]
