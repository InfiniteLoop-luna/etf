import unittest

from src.apple_theme import build_global_apple_theme_css


class SidebarTreeThemeTest(unittest.TestCase):
    def test_tree_navigation_hooks_exist_in_global_theme_css(self) -> None:
        css = build_global_apple_theme_css()

        expected_selectors = [
            '[data-testid="stSidebar"] .ws-sidebar-tree',
            '[data-testid="stSidebar"] .ws-sidebar-page-description',
            '[data-testid="stSidebar"] .ws-sidebar-search-result-meta',
            '[data-testid="stSidebar"] .ws-sidebar-empty',
            '[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] > div button',
            '[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] > div button',
            '[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] > div button',
            '[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] > div button',
            '[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-active"] > div button',
            '[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-current"] > div button',
            '[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-expanded"] > div button',
        ]

        for selector in expected_selectors:
            with self.subTest(selector=selector):
                self.assertIn(selector, css)


if __name__ == "__main__":
    unittest.main()
