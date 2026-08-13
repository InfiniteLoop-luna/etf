from __future__ import annotations

import unittest
from pathlib import Path


PAGE = Path(__file__).resolve().parents[1] / "src" / "pages" / "nasdaq_sector_page.py"


class NasdaqSectorThemeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = PAGE.read_text(encoding="utf-8")

    def test_page_emits_current_theme_marker(self) -> None:
        self.assertIn("get_active_theme_id", self.source)
        self.assertIn("ws-us-theme-marker--{escape(theme_id)}", self.source)

    def test_both_supported_themes_have_component_rules(self) -> None:
        self.assertIn(".ws-us-theme-marker--apple", self.source)
        self.assertIn(".ws-us-theme-marker--doraemon", self.source)
        self.assertIn("border-top:3px solid #0066CC", self.source)
        self.assertIn("border-top:5px solid #11A9EE", self.source)
        self.assertIn("background:#F46968", self.source)
        self.assertIn("background:#FCCD3D", self.source)

    def test_plotly_chart_rebuilds_template_for_current_theme(self) -> None:
        self.assertIn("build_apple_plotly_template", self.source)
        self.assertIn("template=build_apple_plotly_template()", self.source)


if __name__ == "__main__":
    unittest.main()
