from __future__ import annotations

import unittest

import pandas as pd

from src.pages.stock_object_page import (
    _build_recent_summary,
    _pick_important_events,
    _sort_latest_first,
)


class StockObjectPageSortingTest(unittest.TestCase):
    def test_event_feed_sorts_latest_date_first(self) -> None:
        source = pd.DataFrame(
            {
                "日期": ["2026-08-01", "2026-08-13 09:30:00", "无日期", "2026-08-12"],
                "标题": ["旧", "最新", "未知", "次新"],
            }
        )

        actual = _sort_latest_first(source)

        self.assertEqual(actual["标题"].tolist(), ["最新", "次新", "旧", "未知"])
        self.assertEqual(source["标题"].tolist(), ["旧", "最新", "未知", "次新"])
        self.assertNotIn("__stock_object_sort_time", actual.columns)

    def test_news_and_announcements_use_their_date_columns(self) -> None:
        news = pd.DataFrame(
            {"发布时间": ["2026-07-01", "2026-08-13"], "新闻标题": ["旧闻", "新消息"]}
        )
        notices = pd.DataFrame(
            {"公告日期": ["2026-08-11", "2026-08-13"], "公告标题": ["较早公告", "最新公告"]}
        )

        self.assertEqual(_sort_latest_first(news)["新闻标题"].tolist(), ["新消息", "旧闻"])
        self.assertEqual(_sort_latest_first(notices)["公告标题"].tolist(), ["最新公告", "较早公告"])

    def test_non_temporal_overview_order_is_preserved(self) -> None:
        overview = pd.DataFrame(
            {"字段": ["上市日期", "所属行业", "市场板块"], "值": ["2020-01-01", "电子", "主板"]}
        )

        actual = _sort_latest_first(overview)

        self.assertEqual(actual["字段"].tolist(), overview["字段"].tolist())

    def test_recent_summary_is_newest_first(self) -> None:
        today = pd.Timestamp.now().normalize()
        source = pd.DataFrame(
            {
                "日期": [today - pd.Timedelta(days=2), today, today - pd.Timedelta(days=1)],
                "类型": ["公告", "新闻", "研报"],
                "标题": ["较早", "最新", "次新"],
            }
        )

        recent, counts = _build_recent_summary(source, days=7)

        self.assertEqual(recent["标题"].tolist(), ["最新", "次新", "较早"])
        self.assertEqual(counts, {"公告": 1, "新闻": 1, "研报": 1})

    def test_important_events_are_sorted_by_date_after_filtering(self) -> None:
        source = pd.DataFrame(
            {
                "日期": ["2026-08-10", "2026-08-13", "2026-08-12"],
                "类型": ["公告", "公告", "公告"],
                "子类型": ["重大事项", "重大事项", "重大事项"],
                "标题": ["回购事项", "重大合同", "资产重组"],
                "机构": ["", "", ""],
                "评级": ["", "", ""],
            }
        )

        actual = _pick_important_events(source, limit=8)

        self.assertEqual(actual["日期"].tolist(), ["2026-08-13", "2026-08-12", "2026-08-10"])


if __name__ == "__main__":
    unittest.main()
