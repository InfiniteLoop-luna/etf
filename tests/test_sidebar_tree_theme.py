import unittest

from src.apple_theme import build_global_apple_theme_css


class SidebarTreeThemeTest(unittest.TestCase):
    def test_tree_navigation_hooks_exist_in_global_theme_css(self) -> None:
        css = build_global_apple_theme_css()

        expected_hooks = [
            ".ws-sidebar-tree",
            ".ws-sidebar-page-description",
            ".ws-sidebar-search-result-meta",
            ".ws-sidebar-empty",
            "st-key-ws-sidebar-module-",
            "st-key-ws-sidebar-page-",
            "st-key-ws-sidebar-search-result-",
            "st-key-ws-sidebar-recent-link-",
        ]

        for hook in expected_hooks:
            with self.subTest(hook=hook):
                self.assertIn(hook, css)


if __name__ == "__main__":
    unittest.main()
