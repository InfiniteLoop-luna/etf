import unittest

from src.apple_theme import build_global_apple_theme_css


class SidebarTreeThemeTest(unittest.TestCase):
    def test_tree_navigation_hooks_exist_in_global_theme_css(self) -> None:
        css = build_global_apple_theme_css()

        expected_selectors = [
            '[data-testid="stSidebar"] [class*="st-key-ws-sidebar-tree"]',
            '[data-testid="stSidebar"] .ws-sidebar-page-description',
            '[data-testid="stSidebar"] .ws-sidebar-search-result-meta',
            '[data-testid="stSidebar"] .ws-sidebar-empty',
            '[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"] button',
            '[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] button',
            '[data-testid="stSidebar"] [class*="st-key-ws-sidebar-search-result-"] button',
            '[data-testid="stSidebar"] [class*="st-key-ws-sidebar-recent-link-"] button',
            '[data-testid="stSidebar"] [class*="st-key-ws-sidebar-favorite-"] button',
            '[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-active"] button',
            '[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"][class*="-current"] button',
            '[data-testid="stSidebar"] [class*="st-key-ws-sidebar-module-"][class*="-expanded"] button',
        ]

        for selector in expected_selectors:
            with self.subTest(selector=selector):
                self.assertIn(selector, css)

        self.assertIn('align-items: flex-start', css)
        self.assertIn('[data-testid="stMarkdownContainer"]', css)
        self.assertIn('background: var(--ws-sidebar-active-bg)', css)
        self.assertIn('border-left: 2px solid var(--ws-sidebar-accent)', css)
        self.assertIn('[data-testid="stSidebarContent"]', css)
        self.assertIn('display: none !important', css)
        self.assertNotIn('[class*="st-key-ws-sidebar-module-"] > div button', css)
        self.assertNotIn('[class*="st-key-ws-sidebar-page-"] > div button', css)


if __name__ == "__main__":
    unittest.main()
