import unittest


DEPOSIT_PAGE = "🏦 本外币存款"
INDEX_MONITOR_PAGE = "📊 指数监测"


class NavigationConfigTests(unittest.TestCase):
    def test_deposit_page_belongs_to_macro_not_etf(self):
        from src.navigation_config import ETF_PAGE_OPTIONS, MACRO_PAGE_OPTIONS

        self.assertNotIn(DEPOSIT_PAGE, ETF_PAGE_OPTIONS)
        self.assertIn(DEPOSIT_PAGE, MACRO_PAGE_OPTIONS)

    def test_index_monitor_page_belongs_to_macro(self):
        from src.navigation_config import ETF_PAGE_OPTIONS, MACRO_PAGE_OPTIONS

        self.assertNotIn(INDEX_MONITOR_PAGE, ETF_PAGE_OPTIONS)
        self.assertIn(INDEX_MONITOR_PAGE, MACRO_PAGE_OPTIONS)


if __name__ == "__main__":
    unittest.main()
