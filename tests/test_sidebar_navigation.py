import unittest

from src.navigation_config import (
    DECISION_PAGE_OPTIONS,
    ETF_PAGE_OPTIONS,
    MACRO_PAGE_OPTIONS,
    MONEY_PAGE_OPTIONS,
    STOCK_PAGE_OPTIONS,
)
from src.sidebar_navigation import (
    MAX_RECENT_PAGES,
    RECENT_VISITS_KEY,
    ensure_sidebar_state,
    get_default_shortcuts,
    get_module_label_for_page,
    get_module_labels,
    get_page_by_label,
    get_page_labels,
    get_recent_visits,
    record_recent_visit,
)


class SidebarNavigationTests(unittest.TestCase):
    def test_module_labels_keep_expected_order(self):
        self.assertEqual(get_module_labels(), ["决策", "基金", "个股", "资金", "宏观"])

    def test_legacy_option_exports_match_structured_catalog(self):
        self.assertEqual(get_page_labels("决策"), DECISION_PAGE_OPTIONS)
        self.assertEqual(get_page_labels("基金"), ETF_PAGE_OPTIONS)
        self.assertEqual(get_page_labels("个股"), STOCK_PAGE_OPTIONS)
        self.assertEqual(get_page_labels("资金"), MONEY_PAGE_OPTIONS)
        self.assertEqual(get_page_labels("宏观"), MACRO_PAGE_OPTIONS)

    def test_get_page_by_label_exposes_toolbar_variant(self):
        page = get_page_by_label("个股", "🧠 因子选股工作台")

        self.assertEqual(page.id, "factor_workbench")
        self.assertEqual(page.toolbar_variant, "heavy")

    def test_get_module_label_for_page_supports_quick_jump(self):
        self.assertEqual(get_module_label_for_page("💹 资金流向"), "资金")
        self.assertEqual(get_module_label_for_page("🌏 宏观经济"), "宏观")

    def test_default_shortcuts_stay_curated_and_stable(self):
        self.assertEqual(
            get_default_shortcuts(),
            ["💼 今日机会清单", "🔎 个股/指数查询", "💹 资金流向"],
        )

    def test_record_recent_visit_deduplicates_and_trims(self):
        session_state = {}
        ensure_sidebar_state(session_state)

        record_recent_visit(session_state, "决策", "💼 今日机会清单")
        record_recent_visit(session_state, "基金", "📈 基金监测")
        record_recent_visit(session_state, "决策", "💼 今日机会清单")
        record_recent_visit(session_state, "个股", "🔎 个股/指数查询")
        record_recent_visit(session_state, "资金", "💹 资金流向")
        record_recent_visit(session_state, "宏观", "🌏 宏观经济")

        recent = get_recent_visits(session_state)

        self.assertEqual(session_state[RECENT_VISITS_KEY], recent)
        self.assertEqual(len(recent), MAX_RECENT_PAGES)
        self.assertEqual(recent[0], {"module": "宏观", "page": "🌏 宏观经济"})
        self.assertEqual(sum(1 for item in recent if item["page"] == "💼 今日机会清单"), 1)


if __name__ == "__main__":
    unittest.main()
