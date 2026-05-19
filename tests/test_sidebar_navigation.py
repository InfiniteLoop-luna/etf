import unittest

from src.navigation_config import (
    DECISION_DAILY_RECO_PAGE_LABEL,
    DECISION_ML_PAGE_LABEL,
    DECISION_PAGE_OPTIONS,
    DECISION_RECO_EVAL_PAGE_LABEL,
    DECISION_TODAY_PAGE_LABEL,
    ETF_FUND_MONITOR_PAGE_LABEL,
    ETF_MAIN_PAGE_LABEL,
    ETF_PAGE_OPTIONS,
    ETF_RATIO_PAGE_LABEL,
    ETF_TREND_PAGE_LABEL,
    ETF_WIDE_INDEX_PAGE_LABEL,
    MACRO_DEPOSIT_PAGE_LABEL,
    MACRO_INDEX_MONITOR_PAGE_LABEL,
    MACRO_MAIN_PAGE_LABEL,
    MACRO_PAGE_OPTIONS,
    MONEY_FLOW_PAGE_LABEL,
    MONEY_FUND_HOT_PAGE_LABEL,
    MONEY_HOTMONEY_PAGE_LABEL,
    MONEY_LIMITUP_PAGE_LABEL,
    MONEY_PAGE_OPTIONS,
    MONEY_VOLUME_PAGE_LABEL,
    STOCK_COMPANY_SCREENER_LABEL,
    STOCK_PAGE_OPTIONS,
    STOCK_SECURITY_SEARCH_LABEL,
    STOCK_TECH_PICKER_LABEL,
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
        self.assertEqual(get_module_labels(), ["决策", "基金", "股票", "资金", "宏观"])

    def test_legacy_option_exports_match_structured_catalog(self):
        self.assertEqual(get_page_labels("决策"), DECISION_PAGE_OPTIONS)
        self.assertEqual(get_page_labels("基金"), ETF_PAGE_OPTIONS)
        self.assertEqual(get_page_labels("股票"), STOCK_PAGE_OPTIONS)
        self.assertEqual(get_page_labels("资金"), MONEY_PAGE_OPTIONS)
        self.assertEqual(get_page_labels("宏观"), MACRO_PAGE_OPTIONS)

    def test_navigation_config_label_constants_stay_stable(self):
        self.assertEqual(DECISION_TODAY_PAGE_LABEL, get_page_by_label("决策", "💼 今日机会清单").label)
        self.assertEqual(DECISION_DAILY_RECO_PAGE_LABEL, get_page_by_label("决策", "⭐ 每日趋势推荐").label)
        self.assertEqual(DECISION_RECO_EVAL_PAGE_LABEL, get_page_by_label("决策", "🧪 推荐评估").label)
        self.assertEqual(DECISION_ML_PAGE_LABEL, get_page_by_label("决策", "🧠 ML预测升级").label)

        self.assertEqual(ETF_MAIN_PAGE_LABEL, get_page_by_label("基金", "📈 主要宽基ETF份额").label)
        self.assertEqual(ETF_RATIO_PAGE_LABEL, get_page_by_label("基金", "🥧 ETF分类占比").label)
        self.assertEqual(ETF_TREND_PAGE_LABEL, get_page_by_label("基金", "📈 ETF分类趋势").label)
        self.assertEqual(ETF_WIDE_INDEX_PAGE_LABEL, get_page_by_label("基金", "📊 宽基指数ETF").label)
        self.assertEqual(ETF_FUND_MONITOR_PAGE_LABEL, get_page_by_label("基金", "📈 基金监测").label)

        self.assertEqual(STOCK_SECURITY_SEARCH_LABEL, get_page_by_label("股票", "🔎 个股/指数查询").label)
        self.assertEqual(STOCK_COMPANY_SCREENER_LABEL, get_page_by_label("股票", "🏢 公司筛选").label)
        self.assertEqual(STOCK_TECH_PICKER_LABEL, get_page_by_label("股票", "🎯 技术选股").label)

        self.assertEqual(MONEY_FLOW_PAGE_LABEL, get_page_by_label("资金", "💹 资金流向").label)
        self.assertEqual(MONEY_VOLUME_PAGE_LABEL, get_page_by_label("资金", "📊 每日成交量").label)
        self.assertEqual(MONEY_FUND_HOT_PAGE_LABEL, get_page_by_label("资金", "🏦 公募持仓热股").label)
        self.assertEqual(MONEY_LIMITUP_PAGE_LABEL, get_page_by_label("资金", "🔥 打板情绪").label)
        self.assertEqual(MONEY_HOTMONEY_PAGE_LABEL, get_page_by_label("资金", "🧾 游资名录").label)

        self.assertEqual(MACRO_MAIN_PAGE_LABEL, get_page_by_label("宏观", "🌏 宏观经济").label)
        self.assertEqual(MACRO_DEPOSIT_PAGE_LABEL, get_page_by_label("宏观", "🏦 本外币存款").label)
        self.assertEqual(MACRO_INDEX_MONITOR_PAGE_LABEL, get_page_by_label("宏观", "📊 指数监测").label)

    def test_get_page_by_label_exposes_toolbar_variant(self):
        page = get_page_by_label("股票", "🧠 因子选股工作台")

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

    def test_get_recent_visits_returns_copies(self):
        session_state = {}
        ensure_sidebar_state(session_state)
        record_recent_visit(session_state, "决策", "💼 今日机会清单")

        recent = get_recent_visits(session_state)
        recent[0]["page"] = "mutated"

        self.assertEqual(get_recent_visits(session_state)[0]["page"], "💼 今日机会清单")
        self.assertIsNot(recent[0], session_state[RECENT_VISITS_KEY][0])

    def test_record_recent_visit_validates_module_and_page_membership(self):
        session_state = {}

        with self.assertRaises(KeyError):
            record_recent_visit(session_state, "不存在", "💼 今日机会清单")

        with self.assertRaises(KeyError):
            record_recent_visit(session_state, "资金", "💼 今日机会清单")

        self.assertNotIn(RECENT_VISITS_KEY, session_state)

    def test_record_recent_visit_deduplicates_and_trims(self):
        session_state = {}
        ensure_sidebar_state(session_state)

        record_recent_visit(session_state, "决策", "💼 今日机会清单")
        record_recent_visit(session_state, "基金", "📈 基金监测")
        record_recent_visit(session_state, "决策", "💼 今日机会清单")
        record_recent_visit(session_state, "股票", "🔎 个股/指数查询")
        record_recent_visit(session_state, "资金", "💹 资金流向")
        record_recent_visit(session_state, "宏观", "🌏 宏观经济")

        recent = get_recent_visits(session_state)

        self.assertEqual(session_state[RECENT_VISITS_KEY], recent)
        self.assertEqual(len(recent), MAX_RECENT_PAGES)
        self.assertEqual(recent[0], {"module": "宏观", "page": "🌏 宏观经济"})
        self.assertEqual(sum(1 for item in recent if item["page"] == "💼 今日机会清单"), 1)


if __name__ == "__main__":
    unittest.main()
