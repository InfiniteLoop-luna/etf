import unittest

from src.page_filter_utils import (
    build_metric_categories,
    build_quick_metric_groups,
    build_secondary_category_options,
    resolve_trend_category_key,
)


class PageFilterUtilsTests(unittest.TestCase):
    def test_build_metric_categories_allows_overlapping_keyword_membership(self):
        categories = build_metric_categories([
            "总市值",
            "基金份额",
            "份额变动",
            "申赎份额",
            "涨跌幅",
            "跟踪误差",
        ])

        self.assertEqual(categories["市值类"], ["总市值"])
        self.assertEqual(categories["份额类"], ["基金份额", "份额变动", "申赎份额"])
        self.assertEqual(categories["变动类"], ["份额变动", "申赎份额"])
        self.assertEqual(categories["涨跌类"], ["涨跌幅"])
        self.assertEqual(categories["其他"], ["跟踪误差"])

    def test_build_quick_metric_groups_matches_existing_shortcuts(self):
        groups = build_quick_metric_groups(["总市值", "基金份额", "涨跌幅"])

        self.assertEqual(groups["总市值"], ["总市值"])
        self.assertEqual(groups["份额"], ["基金份额"])
        self.assertEqual(groups["涨跌幅"], ["涨跌幅"])

    def test_build_secondary_category_options_returns_subtotal_entry(self):
        options = build_secondary_category_options("指数", {"指数": ["宽基", "港股"]})

        self.assertEqual(options, ["全部(小计)", "宽基", "港股"])

    def test_resolve_trend_category_key_handles_all_and_subtotal(self):
        tree = {"指数": ["宽基", "港股"]}

        self.assertEqual(resolve_trend_category_key("全部", None, tree), "全部")
        self.assertEqual(resolve_trend_category_key("指数", "全部(小计)", tree), "指数")
        self.assertEqual(resolve_trend_category_key("指数", "宽基", tree), "指数-宽基")


if __name__ == "__main__":
    unittest.main()
