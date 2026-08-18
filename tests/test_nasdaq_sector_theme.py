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

    def test_a_share_mapping_has_both_theme_styles(self) -> None:
        self.assertIn(".ws-cn-map-card", self.source)
        self.assertIn(".ws-us-theme-marker--apple) .ws-cn-map-card", self.source)
        self.assertIn(".ws-us-theme-marker--doraemon) .ws-cn-map-card", self.source)
        self.assertIn("background:#F46968", self.source)
        self.assertIn("background:#FCCD3D", self.source)

    def test_plotly_chart_rebuilds_template_for_current_theme(self) -> None:
        self.assertIn("build_apple_plotly_template", self.source)
        self.assertIn("template=build_apple_plotly_template()", self.source)

    def test_price_changes_use_china_market_colors(self) -> None:
        self.assertIn(".ws-market-up{color:#D94C51!important}", self.source)
        self.assertIn(".ws-market-down{color:#248A3D!important}", self.source)
        self.assertIn('return "ws-market-up" if float(number) > 0 else "ws-market-down"', self.source)
        self.assertIn('color_continuous_scale=["#248A3D", "#E8EEF3", "#D94C51"]', self.source)
        self.assertIn('_style_change_columns(leader_df, [f"{period}涨跌(%)"])', self.source)


if __name__ == "__main__":
    unittest.main()
