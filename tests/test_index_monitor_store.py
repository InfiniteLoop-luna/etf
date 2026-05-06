import unittest

import pandas as pd

from src.index_monitor_store import (
    build_index_monitor_summary,
    build_price_trend_df,
    build_valuation_trend_df,
    classify_index_import_rows,
)


class IndexMonitorStoreTests(unittest.TestCase):
    def test_build_index_monitor_summary_returns_core_kpis(self):
        df = pd.DataFrame(
            [
                {
                    "month": "2026-05-01",
                    "index_name": "上证指数",
                    "monthly_change_pct": 0.82,
                    "static_pe": 16.1,
                    "dynamic_pe": 14.5,
                },
                {
                    "month": "2026-05-01",
                    "index_name": "深证成指",
                    "monthly_change_pct": -1.35,
                    "static_pe": 27.3,
                    "dynamic_pe": 22.9,
                },
            ]
        )

        summary = build_index_monitor_summary(df)

        self.assertEqual(summary["latest_month"], "2026-05")
        self.assertAlmostEqual(summary["avg_change_pct"], -0.265, places=3)
        self.assertEqual(summary["strongest_index"], "上证指数")
        self.assertEqual(summary["weakest_index"], "深证成指")
        self.assertAlmostEqual(summary["avg_static_pe"], 21.7, places=1)
        self.assertAlmostEqual(summary["avg_dynamic_pe"], 18.7, places=1)

    def test_build_price_trend_df_shapes_selected_value_field(self):
        df = pd.DataFrame(
            [
                {"month": "2026-04-01", "index_name": "上证指数", "close_price": 3300},
                {"month": "2026-05-01", "index_name": "上证指数", "close_price": 3367.46},
            ]
        )

        trend_df = build_price_trend_df(df, value_field="close_price")

        self.assertEqual(trend_df["index_name"].tolist(), ["上证指数", "上证指数"])
        self.assertEqual(trend_df.iloc[-1]["value"], 3367.46)

    def test_build_valuation_trend_df_melts_two_series(self):
        df = pd.DataFrame(
            [
                {
                    "month": "2026-05-01",
                    "index_name": "上证指数",
                    "static_pe": 16.1,
                    "dynamic_pe": 14.5,
                }
            ]
        )

        trend_df = build_valuation_trend_df(df)

        self.assertEqual(
            trend_df["metric"].tolist(),
            ["期末静态市盈率", "期末动态市盈率"],
        )

    def test_classify_index_import_rows_splits_insert_and_overwrite(self):
        incoming = pd.DataFrame(
            [
                {"month": "2026-05-01", "index_name": "上证指数"},
                {"month": "2026-05-01", "index_name": "深证成指"},
            ]
        )
        existing = pd.DataFrame(
            [{"month": "2026-05-01", "index_name": "上证指数"}]
        )

        preview = classify_index_import_rows(incoming, existing)

        self.assertEqual(preview["to_insert"]["index_name"].tolist(), ["深证成指"])
        self.assertEqual(preview["to_overwrite"]["index_name"].tolist(), ["上证指数"])


if __name__ == "__main__":
    unittest.main()
