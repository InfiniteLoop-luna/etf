import unittest

from src.sidebar_navigation import (
    MAX_RECENT_PAGES,
    get_module_by_id,
    get_module_id_for_page_id,
    get_page_by_id,
    get_recent_visits,
    record_recent_visit,
    resolve_expanded_module_id,
    search_sidebar_pages,
)


class SidebarNavigationTests(unittest.TestCase):
    def test_lookup_helpers_return_expected_stock_page_metadata(self):
        module = get_module_by_id("stock")
        page = get_page_by_id("security_search")

        self.assertEqual(module.id, "stock")
        self.assertEqual(page.id, "security_search")
        self.assertEqual(get_module_id_for_page_id("security_search"), "stock")
        self.assertIn(page, module.pages)

    def test_search_prefers_page_hits_for_security_query(self):
        results = search_sidebar_pages("security")

        self.assertGreater(len(results), 0)
        self.assertEqual(results[0].page_id, "security_search")
        self.assertEqual(results[0].module_id, "stock")

    def test_module_query_returns_module_pages_including_security_search(self):
        results = search_sidebar_pages("stock")

        self.assertGreater(len(results), 0)
        stock_page_ids = [result.page_id for result in results if result.module_id == "stock"]
        self.assertIn("security_search", stock_page_ids)
        self.assertIn("company_screener", stock_page_ids)

    def test_module_matches_do_not_suppress_higher_ranked_page_hits_from_other_modules(self):
        results = search_sidebar_pages("fund")

        self.assertGreater(len(results), 0)
        result_page_ids = [result.page_id for result in results]
        self.assertIn("fund_hot_stocks", result_page_ids)
        self.assertIn("etf_main", result_page_ids)
        self.assertLess(result_page_ids.index("fund_hot_stocks"), result_page_ids.index("etf_main"))

    def test_record_recent_visit_deduplicates_trims_and_exposes_labels(self):
        session_state = {}

        page_ids = [
            "commercial_mvp",
            "daily_trend_reco",
            "reco_eval",
            "ml_upgrade",
            "security_search",
            "company_screener",
            "tech_picker",
        ]
        for page_id in page_ids:
            record_recent_visit(session_state, get_module_id_for_page_id(page_id), page_id)
        record_recent_visit(session_state, "stock", "security_search")

        visits = get_recent_visits(session_state)

        self.assertEqual(len(visits), MAX_RECENT_PAGES)
        self.assertEqual(visits[0]["module_id"], "stock")
        self.assertEqual(visits[0]["page_id"], "security_search")
        self.assertIn("module_label", visits[0])
        self.assertIn("page_label", visits[0])
        self.assertEqual(
            [visit["page_id"] for visit in visits].count("security_search"),
            1,
        )

    def test_get_recent_visits_upgrades_legacy_label_entry(self):
        module = get_module_by_id("stock")
        page = get_page_by_id("security_search")
        session_state = {
            "sidebar_recent_pages": [
                {
                    "module": module.label,
                    "page": page.label,
                }
            ]
        }

        visits = get_recent_visits(session_state)

        self.assertEqual(
            visits,
            [
                {
                    "module_id": "stock",
                    "module_label": module.label,
                    "page_id": "security_search",
                    "page_label": page.label,
                }
            ],
        )
        self.assertEqual(session_state["sidebar_recent_pages"], visits)

    def test_resolve_expanded_module_id_falls_back_to_active_page_module(self):
        self.assertEqual(
            resolve_expanded_module_id("security_search", "not-a-module"),
            "stock",
        )


if __name__ == "__main__":
    unittest.main()
