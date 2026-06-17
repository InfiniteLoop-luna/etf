import unittest
from unittest.mock import Mock, patch

import pandas as pd

from src.fund_monitor_store import (
    build_fund_monitor_change_trend_df,
    build_fund_monitor_summary,
    build_fund_monitor_trend_df,
    classify_fund_monitor_import_rows,
    delete_fund_monitor_months,
    upsert_fund_monitor_rows,
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

    def test_upsert_fund_monitor_rows_preserves_existing_sort_metadata_when_missing(self):
        conn = Mock()
        context_manager = Mock()
        context_manager.__enter__ = Mock(return_value=conn)
        context_manager.__exit__ = Mock(return_value=False)
        engine = Mock()
        engine.begin.return_value = context_manager

        row = {
            "month": pd.Timestamp("2026-05-01").date(),
            "category_name": "临时分类",
            "category_group": None,
            "category_level": None,
            "sort_order": None,
            "source_type": "manual",
            "source_file": None,
        }
        numeric_fields = [
            "fund_count",
            "share_amount",
            "nav_amount",
            "unit_nav",
            "mom_fund_count",
            "mom_share_amount",
            "mom_nav_amount",
            "mom_unit_nav",
            "yoy_fund_count",
            "yoy_share_amount",
            "yoy_nav_amount",
            "yoy_unit_nav",
        ]
        row.update({field: None for field in numeric_fields})

        with patch("src.fund_monitor_store.ensure_fund_monitor_table"):
            upsert_fund_monitor_rows(engine, [row])

        sql_text = str(conn.execute.call_args.args[0])
        self.assertIn(
            "sort_order = COALESCE(EXCLUDED.sort_order, macro_fund_monitor_monthly.sort_order)",
            sql_text,
        )
        self.assertIn(
            "category_group = COALESCE(EXCLUDED.category_group, macro_fund_monitor_monthly.category_group)",
            sql_text,
        )

    def test_delete_fund_monitor_months_normalizes_and_deduplicates_input(self):
        conn = Mock()
        conn.execute.return_value.rowcount = 3
        context_manager = Mock()
        context_manager.__enter__ = Mock(return_value=conn)
        context_manager.__exit__ = Mock(return_value=False)
        engine = Mock()
        engine.begin.return_value = context_manager

        with patch("src.fund_monitor_store.ensure_fund_monitor_table"):
            deleted = delete_fund_monitor_months(engine, ["2026-5", "2026/06", "2026-05-01", "  "])

        self.assertEqual(deleted, 6)
        self.assertEqual(conn.execute.call_count, 2)
        params_list = [call.args[1] for call in conn.execute.call_args_list]
        self.assertEqual(
            [params["month"].isoformat() for params in params_list],
            ["2026-05-01", "2026-06-01"],
        )


if __name__ == "__main__":
    unittest.main()
