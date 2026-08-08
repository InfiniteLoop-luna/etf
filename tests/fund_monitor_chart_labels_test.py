import unittest

import pandas as pd

from app import create_fund_monitor_chart


class FundMonitorChartLabelsTests(unittest.TestCase):
    def setUp(self):
        self.data_frame = pd.DataFrame(
            [
                {
                    "month": pd.Timestamp("2026-05-01"),
                    "value": 1234.5678,
                    "category_name": "股票基金",
                    "change_type": "同比",
                },
                {
                    "month": pd.Timestamp("2026-06-01"),
                    "value": 1300.1234,
                    "category_name": "股票基金",
                    "change_type": "同比",
                },
            ]
        )

    def test_line_chart_shows_a_value_at_every_breakpoint(self):
        figure = create_fund_monitor_chart(
            self.data_frame,
            metric_key="nav_amount",
            title="净值趋势",
        )

        self.assertEqual(len(figure.data), 1)
        trace = figure.data[0]
        self.assertEqual(trace.mode, "lines+markers+text")
        self.assertEqual(trace.texttemplate, "%{y:,.2f}")
        self.assertEqual(trace.textposition, "top center")
        self.assertEqual(trace.textfont.size, 14)
        self.assertEqual(trace.marker.size, 7)
        self.assertFalse(trace.cliponaxis)
        self.assertEqual(len(trace.text), len(self.data_frame))

    def test_area_chart_keeps_fill_and_adds_point_labels(self):
        figure = create_fund_monitor_chart(
            self.data_frame,
            metric_key="share_amount",
            title="公募结构趋势",
            area=True,
        )

        trace = figure.data[0]
        self.assertTrue(trace.stackgroup)
        self.assertEqual(trace.mode, "lines+markers+text")
        self.assertEqual(trace.texttemplate, "%{y:,.2f}")
        self.assertEqual(len(trace.text), len(self.data_frame))

    def test_metric_specific_number_formats_are_applied(self):
        expected_formats = {
            "fund_count": "%{y:,.0f}",
            "unit_nav": "%{y:,.4f}",
            "nav_amount": "%{y:,.2f}",
            "share_amount": "%{y:,.2f}",
        }

        for metric_key, expected_format in expected_formats.items():
            with self.subTest(metric_key=metric_key):
                figure = create_fund_monitor_chart(
                    self.data_frame,
                    metric_key=metric_key,
                    title="指标趋势",
                    line_dash="change_type",
                )
                self.assertEqual(figure.data[0].texttemplate, expected_format)


if __name__ == "__main__":
    unittest.main()
