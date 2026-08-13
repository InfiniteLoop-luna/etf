import unittest

from src.apple_theme import build_global_apple_theme_css
from src.sidebar_navigation import get_module_by_id, search_sidebar_pages


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
            '[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] button > div',
            '[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] button > div > span',
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
        self.assertIn('justify-content: flex-start !important', css)
        self.assertIn('text-align: left !important', css)
        self.assertIn('margin-right: auto !important', css)
        self.assertIn('display: inline-block !important', css)
        self.assertIn('flex: 1 1 auto !important', css)
        self.assertNotIn('[class*="st-key-ws-sidebar-module-"] > div button', css)
        self.assertNotIn('[class*="st-key-ws-sidebar-page-"] > div button', css)

        self.assertIn("--ws-sidebar-row-height: 34px", css)
        self.assertIn("--ws-sidebar-row-gap: 2px", css)
        self.assertIn('height: var(--ws-sidebar-row-height)', css)
        self.assertIn('[class*="st-key-ws-sidebar-recent-list"]', css)
        self.assertIn('[class*="st-key-ws-sidebar-favorite-list"]', css)
        self.assertNotIn("min-height: 29px", css)
        self.assertNotIn("st-key-ws-theme-switcher", css)
        self.assertNotIn("st-key-ws-theme-btn-", css)

        page_icon_rule = css.split(
            '[data-testid="stSidebar"] [class*="st-key-ws-sidebar-page-"] button img {',
            1,
        )[1].split('\n}', 1)[0]
        self.assertNotIn('display: none', page_icon_rule)

    def test_theme_center_is_a_dedicated_sidebar_module(self) -> None:
        module = get_module_by_id("theme")
        self.assertEqual(module.label, "主题")
        self.assertEqual(len(module.pages), 1)
        self.assertEqual(module.pages[0].id, "theme_center")
        self.assertEqual(module.pages[0].label, "🎨 主题中心")

        results = search_sidebar_pages("主题")
        self.assertTrue(any(item.page_id == "theme_center" for item in results))


if __name__ == "__main__":
    unittest.main()
