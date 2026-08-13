from __future__ import annotations

import ast
import unittest
from pathlib import Path


APP_PATH = Path(__file__).resolve().parents[1] / "app.py"


class FundWatchlistCardLayoutTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = APP_PATH.read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source)

    def test_watchlist_cards_are_chunked_into_three_columns(self) -> None:
        function = next(
            node
            for node in self.tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "render_fund_watchlist_cards"
        )
        function_source = ast.get_source_segment(self.source, function) or ""

        self.assertIn("cards_per_row = 3", function_source)
        self.assertIn("range(0, len(items), cards_per_row)", function_source)
        self.assertIn("st.columns(cards_per_row)", function_source)
        self.assertNotIn("st.columns(len(items))", function_source)

    def test_card_overlay_height_matches_compact_card_height(self) -> None:
        self.assertIn("min-height:500px", self.source)
        self.assertIn("margin-bottom:-500px", self.source)
        self.assertIn("height:500px", self.source)
        self.assertNotIn("margin-bottom:-390px", self.source)


if __name__ == "__main__":
    unittest.main()
