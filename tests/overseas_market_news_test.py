from datetime import datetime
from zoneinfo import ZoneInfo

from src.overseas_market_news import (
    collect_overseas_market_news,
    infer_a_share_impact,
    parse_feed,
)


BEIJING = ZoneInfo("Asia/Shanghai")


def _feed(**overrides):
    base = {
        "country_code": "US",
        "country": "美国",
        "source": "Federal Reserve",
        "source_type": "official",
        "source_tier": 1,
        "url": "https://example.com/feed.xml",
    }
    base.update(overrides)
    return base


def test_parse_feed_supports_rss_and_normalizes_to_beijing_time():
    rss = b"""<?xml version="1.0" encoding="utf-8"?>
    <rss version="2.0"><channel><item>
      <title>Federal Reserve issues statement</title>
      <link>https://example.com/release</link>
      <description><![CDATA[<p>Policy statement.</p>]]></description>
      <pubDate>Wed, 16 Sep 2026 18:00:00 GMT</pubDate>
    </item></channel></rss>"""

    items = parse_feed(rss, _feed(), now=datetime(2026, 9, 17, 8, 0, tzinfo=BEIJING))

    assert items[0]["published_at"] == "2026-09-17T02:00:00+08:00"
    assert items[0]["summary"] == "Policy statement."
    assert items[0]["verification_status"] == "官方发布"
    assert items[0]["news_id"].startswith("overseas.US.")


def test_parse_feed_supports_atom_links_and_media_publisher():
    atom = """<?xml version="1.0" encoding="utf-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom"><entry>
      <title>Japan exports rise</title>
      <link href="https://example.com/japan"/>
      <summary>Exports improved.</summary>
      <updated>2026-09-17T00:30:00Z</updated>
      <source><title>Reuters</title></source>
    </entry></feed>"""

    items = parse_feed(
        atom,
        _feed(country_code="JP", country="日本", source="Google News", source_type="media", source_tier=2),
        now=datetime(2026, 9, 17, 9, 0, tzinfo=BEIJING),
    )

    assert items[0]["url"] == "https://example.com/japan"
    # Atom's nested source/title is deliberately not mistaken for the entry title.
    assert items[0]["title"] == "Japan exports rise"
    assert items[0]["verification_status"] == "媒体报道"


def test_infer_a_share_impact_is_scenario_not_point_forecast():
    items = [{
        "news_id": "overseas.US.demo",
        "title": "Fed raises interest rates as inflation accelerates",
        "summary": "Treasury yields rise.",
        "source_type": "official",
    }]

    impact = infer_a_share_impact("US", items)

    assert impact["direction"] == "偏利空"
    assert impact["strength"] in {"中", "高"}
    assert "点位" not in impact["analysis"]
    assert impact["invalidating_conditions"]
    assert impact["basis_news_ids"] == ["overseas.US.demo"]


def test_collect_overseas_news_keeps_running_when_some_feeds_fail():
    rss = b"""<rss version="2.0"><channel><item>
      <title>Inflation cools</title><link>https://example.com/release</link>
      <pubDate>Wed, 16 Sep 2026 18:00:00 GMT</pubDate>
    </item></channel></rss>"""

    def fetcher(feed):
        if feed["country_code"] == "JP":
            raise RuntimeError("temporary failure")
        return rss

    result = collect_overseas_market_news(
        now=datetime(2026, 9, 17, 8, 0, tzinfo=BEIJING),
        fetcher=fetcher,
    )

    assert result["status"] == "partial"
    assert result["countries"]["US"]["items"]
    assert result["countries"]["KR"]["items"]
    assert not result["countries"]["JP"]["items"]
    assert any("日本" in warning for warning in result["warnings"])
