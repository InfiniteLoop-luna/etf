import unittest
from unittest.mock import Mock, patch

import pandas as pd

from src.etf_deposit_store import (
    build_balance_trend_df,
    build_change_trend_df,
    build_deposit_summary,
    build_upsert_rows,
    classify_import_rows,
    delete_deposit_months,
    normalize_month,
    to_deposit_display_df,
)


class EtfDepositStoreTests(unittest.TestCase):
    def test_normalize_month_accepts_common_formats(self):
        self.assertEqual(str(normalize_month("2026-03")), "2026-03-01")
        self.assertEqual(str(normalize_month("2026-3")), "2026-03-01")
        self.assertEqual(str(normalize_month("2026/03")), "2026-03-01")
        self.assertEqual(str(normalize_month("2026年03月")), "2026-03-01")
        self.assertEqual(str(normalize_month("2026-03-01")), "2026-03-01")
        self.assertEqual(str(normalize_month("20260301")), "2026-03-01")

    def test_build_deposit_summary_computes_mom_and_yoy(self):
        df = pd.DataFrame(
            [
                {"month": "2025-03-01", "total_deposit_balance": 327.12},
                {"month": "2026-02-01", "total_deposit_balance": 345.72},
                {"month": "2026-03-01", "total_deposit_balance": 350.23},
            ]
        )

        summary = build_deposit_summary(df)

        self.assertEqual(summary["latest_month"], "2026-03")
        self.assertAlmostEqual(summary["latest_value"], 350.23)
        self.assertAlmostEqual(summary["mom_delta"], 4.51, places=2)
        self.assertAlmostEqual(summary["yoy_delta"], 23.11, places=2)

    def test_build_balance_trend_df_sorts_months_and_keeps_three_series(self):
        df = pd.DataFrame(
            [
                {
                    "month": "2026-02-01",
                    "rmb_deposit_balance": 337.94,
                    "fx_deposit_balance": 1.12,
                    "total_deposit_balance": 345.72,
                },
                {
                    "month": "2026-03-01",
                    "rmb_deposit_balance": 342.41,
                    "fx_deposit_balance": 1.13,
                    "total_deposit_balance": 350.23,
                },
            ]
        )

        trend_df = build_balance_trend_df(df)

        self.assertEqual(
            trend_df["metric"].tolist(),
            ["人民币存款余额", "外币存款余额", "本外币存款余额"] * 2,
        )
        self.assertEqual(trend_df.iloc[0]["month"].strftime("%Y-%m-%d"), "2026-02-01")

    def test_build_change_trend_df_computes_mom_and_yoy_amounts(self):
        df = pd.DataFrame(
            [
                {"month": "2025-03-01", "total_deposit_balance": 327.12},
                {"month": "2026-02-01", "total_deposit_balance": 345.72},
                {"month": "2026-03-01", "total_deposit_balance": 350.23},
            ]
        )

        change_df = build_change_trend_df(df)

        latest_rows = change_df[change_df["month"] == pd.Timestamp("2026-03-01")]
        latest_map = dict(zip(latest_rows["metric"], latest_rows["value"]))
        self.assertAlmostEqual(latest_map["环比变动额"], 4.51, places=2)
        self.assertAlmostEqual(latest_map["同比变动额"], 23.11, places=2)

    def test_build_upsert_rows_normalizes_month_and_source_fields(self):
        rows = build_upsert_rows(
            [
                {
                    "month": "2026-03",
                    "rmb_deposit_balance": 342.41,
                    "fx_deposit_balance": 1.13,
                    "total_deposit_balance": 350.23,
                    "household_deposit_increase": 7.68,
                    "corp_deposit_increase": 2.68,
                    "fiscal_deposit_increase": 0.4606,
                    "nonbank_deposit_increase": 2.03,
                    "total_deposit_increase": 13.73,
                    "household_long_loan_increase": 0.4607,
                }
            ],
            source_type="manual",
            source_file=None,
        )

        self.assertEqual(rows[0]["month"].isoformat(), "2026-03-01")
        self.assertEqual(rows[0]["source_type"], "manual")
        self.assertIsNone(rows[0]["source_file"])

    def test_classify_import_rows_splits_insert_and_overwrite(self):
        incoming = pd.DataFrame(
            [
                {"month": "2026-03-01", "total_deposit_balance": 350.23},
                {"month": "2026-04-01", "total_deposit_balance": 352.10},
            ]
        )
        existing = pd.DataFrame([{"month": "2026-03-01", "total_deposit_balance": 349.50}])

        preview = classify_import_rows(incoming, existing)

        self.assertEqual(preview["to_insert"]["month"].tolist(), ["2026-04-01"])
        self.assertEqual(preview["to_overwrite"]["month"].tolist(), ["2026-03-01"])

    def test_delete_deposit_months_normalizes_and_deduplicates_input(self):
        conn = Mock()
        conn.execute.return_value.rowcount = 1
        context_manager = Mock()
        context_manager.__enter__ = Mock(return_value=conn)
        context_manager.__exit__ = Mock(return_value=False)
        engine = Mock()
        engine.begin.return_value = context_manager

        with patch("src.etf_deposit_store.ensure_deposit_table"):
            deleted = delete_deposit_months(engine, ["2026-3", "2026/04", "2026-03-01", "  "])

        self.assertEqual(deleted, 2)
        self.assertEqual(conn.execute.call_count, 2)
        params_list = [call.args[1] for call in conn.execute.call_args_list]
        self.assertEqual(
            [params["month"].isoformat() for params in params_list],
            ["2026-03-01", "2026-04-01"],
        )

    def test_to_deposit_display_df_uses_chinese_column_labels(self):
        df = pd.DataFrame(
            [
                {
                    "month": "2026-03-01",
                    "rmb_deposit_balance": 342.41,
                    "fx_deposit_balance": 1.13,
                    "total_deposit_balance": 350.23,
                    "source_type": "import",
                    "updated_at": "2026-05-06 07:24:00",
                }
            ]
        )

        display_df = to_deposit_display_df(df)

        self.assertEqual(
            display_df.columns.tolist(),
            ["月份", "人民币存款余额", "外币存款余额", "本外币存款余额", "数据来源", "更新时间"],
        )
        self.assertEqual(display_df.iloc[0]["月份"], "2026-03-01")


if __name__ == "__main__":
    unittest.main()
