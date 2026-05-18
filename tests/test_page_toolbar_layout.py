import re
import unittest
from pathlib import Path


APP_SOURCE = Path("app.py").read_text(encoding="utf-8", errors="ignore")


def function_chunk(name: str) -> str:
    match = re.search(rf"def {name}\(.*?(?=^def |\Z)", APP_SOURCE, flags=re.S | re.M)
    if not match:
        raise AssertionError(f"Function {name} not found")
    return match.group(0)


class PageToolbarLayoutTests(unittest.TestCase):
    def test_render_volume_tab_no_longer_uses_sidebar(self):
        chunk = function_chunk("render_volume_tab")

        self.assertNotIn("st.sidebar", chunk)
        self.assertIn("ws-page-toolbar", chunk)

    def test_render_etf_trend_tab_no_longer_uses_sidebar(self):
        chunk = function_chunk("render_etf_trend_tab")

        self.assertNotIn("st.sidebar", chunk)
        self.assertIn("更多筛选", chunk)

    def test_render_etf_tab_moves_desktop_filters_into_page_toolbar(self):
        chunk = function_chunk("render_etf_tab")

        self.assertNotIn("st.sidebar", chunk)
        self.assertIn("更多筛选", chunk)
        self.assertIn("build_metric_categories", chunk)
        self.assertIn("build_quick_metric_groups", chunk)

    def test_render_etf_tab_uses_pending_quick_metric_state_before_widget_keys(self):
        chunk = function_chunk("render_etf_tab")

        self.assertIn('st.session_state["etf_pending_metric"]', chunk)
        self.assertIn('st.session_state["etf_pending_metric_category"]', chunk)
        self.assertNotIn('st.session_state["etf_selected_metric"] = target_metric', chunk)
        self.assertNotIn(
            'st.session_state["etf_metric_category"] = resolve_metric_category(target_metric)',
            chunk,
        )


if __name__ == "__main__":
    unittest.main()
