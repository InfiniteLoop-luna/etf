import unittest

from streamlit.testing.v1 import AppTest


ETF_TAB_MINIMAL_APP = r"""
import pandas as pd
import plotly.graph_objects as go
import app

share_metric = "\u57fa\u91d1\u4efd\u989d"
market_metric = "\u603b\u5e02\u503c"

rows = [
    {"date": pd.Timestamp("2026-05-18"), "metric_type": share_metric, "is_aggregate": False, "name": "\u6caa\u6df1300ETF", "value": 50.0},
    {"date": pd.Timestamp("2026-05-18"), "metric_type": share_metric, "is_aggregate": False, "name": "\u4e2d\u8bc1500ETF", "value": 30.0},
    {"date": pd.Timestamp("2026-05-19"), "metric_type": share_metric, "is_aggregate": False, "name": "\u6caa\u6df1300ETF", "value": 51.0},
    {"date": pd.Timestamp("2026-05-19"), "metric_type": share_metric, "is_aggregate": False, "name": "\u4e2d\u8bc1500ETF", "value": 31.0},
    {"date": pd.Timestamp("2026-05-18"), "metric_type": market_metric, "is_aggregate": True, "name": "\u5168\u5e02\u573a", "value": 100.0},
    {"date": pd.Timestamp("2026-05-19"), "metric_type": market_metric, "is_aggregate": True, "name": "\u5168\u5e02\u573a", "value": 101.0},
]

app.load_data = lambda _: pd.DataFrame(rows)
app.get_query_param_value = lambda name: ""
app.create_line_chart = lambda *args, **kwargs: go.Figure()
app.calculate_statistics = lambda *args, **kwargs: pd.DataFrame()

app.render_etf_tab()
"""


class EtfTabWidgetStateTests(unittest.TestCase):
    def test_render_etf_tab_first_load_emits_no_widget_policy_warning(self):
        at = AppTest.from_string(ETF_TAB_MINIMAL_APP, default_timeout=5)

        with self.assertNoLogs("streamlit.elements.lib.policies", level="WARNING"):
            at.run(timeout=5)


if __name__ == "__main__":
    unittest.main()
