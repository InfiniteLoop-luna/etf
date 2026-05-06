import unittest

import pandas as pd

from src.fund_monitor_store import (
    build_fund_monitor_change_trend_df,
    build_fund_monitor_summary,
    build_fund_monitor_trend_df,
    classify_fund_monitor_import_rows,
)


class FundMonitorStoreTests(unittest.TestCase):
    def test_build_fund_monitor_summary_returns_core_kpis(self):
        df = pd.DataFrame(
            [
                {"month": "2026-05-01", "category_name": "合计", "nav_amount": 375322.31},
                {"month": "2026-05-01", "category_name": "其中：股票基金", "nav_amount": 51128.57},
                {"month": "2026-05-01", "category_name": "私募证券投资基金", "nav_amount": 74600},
            ]
        )
        summary = build_fund_monitor_summary(df)
        self.assertEqual(summary["latest_month"], "2026-05")
        self.assertAlmostEqual(summary["public_total_nav"], 375322.31)
        self.assertAlmostEqual(summary["equity_fund_nav"], 51128.57)
        self.assertAlmostEqual(summary["private_nav"], 74600)

    def test_build_fund_monitor_trend_df_shapes_selected_metric(self):
        df = pd.DataFrame(
            [
                {"month": "2026-04-01", "category_name": "合计", "nav_amount": 386051.94},
                {"month": "2026-05-01", "category_name": "合计", "nav_amount": 375322.31},
            ]
        )
        trend_df = build_fund_monitor_trend_df(df, value_field="nav_amount")
        self.assertEqual(trend_df.iloc[-1]["value"], 375322.31)

    def test_build_fund_monitor_change_trend_df_shapes_mom_and_yoy_series(self):
        df = pd.DataFrame(
            [
                {
                    "month": "2026-04-01",
                    "category_name": "其中：股票基金",
                    "mom_nav_amount": -790.35,
                    "yoy_nav_amount": 11452.52,
                },
                {
                    "month": "2026-05-01",
                    "category_name": "其中：股票基金",
                    "mom_nav_amount": -5168.49,
                    "yoy_nav_amount": 6467.17,
                },
            ]
        )
        trend_df = build_fund_monitor_change_trend_df(df, metric_key="nav_amount")
        self.assertEqual(trend_df["change_type"].tolist(), ["环比", "同比", "环比", "同比"])
        self.assertEqual(trend_df["metric"].unique().tolist(), ["净值（亿元）"])

    def test_classify_fund_monitor_import_rows_splits_insert_and_overwrite(self):
        incoming = pd.DataFrame(
            [
                {"month": "2026-05-01", "category_name": "合计"},
                {"month": "2026-05-01", "category_name": "私募证券投资基金"},
            ]
        )
        existing = pd.DataFrame([{"month": "2026-05-01", "category_name": "合计"}])
        preview = classify_fund_monitor_import_rows(incoming, existing)
        self.assertEqual(preview["to_insert"]["category_name"].tolist(), ["私募证券投资基金"])
        self.assertEqual(preview["to_overwrite"]["category_name"].tolist(), ["合计"])


if __name__ == "__main__":
    unittest.main()
