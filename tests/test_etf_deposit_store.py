import unittest

import pandas as pd

from src.etf_deposit_store import (
    build_balance_trend_df,
    build_deposit_summary,
    build_upsert_rows,
    classify_import_rows,
    normalize_month,
)


class EtfDepositStoreTests(unittest.TestCase):
    def test_normalize_month_accepts_common_formats(self):
        self.assertEqual(str(normalize_month("2026-03")), "2026-03-01")
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


if __name__ == "__main__":
    unittest.main()
