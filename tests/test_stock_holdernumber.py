import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from src import etf_stats
from src import sync_tushare_security_data as sync_mod


class StockHolderNumberQueryTests(unittest.TestCase):
    def test_get_latest_stock_holder_number_queries_latest_snapshot(self):
        captured = {}

        def fake_read_sql(query, engine, params=None):
            captured["sql"] = str(query)
            captured["params"] = params
            return pd.DataFrame(
                [
                    {
                        "ts_code": "000001.SZ",
                        "holder_num": 45678,
                        "holder_ann_date": "2026-05-20",
                        "holder_end_date": "2026-05-15",
                    }
                ]
            )

        with patch("src.etf_stats.pd.read_sql", side_effect=fake_read_sql):
            df = etf_stats.get_latest_stock_holder_number("000001.SZ", engine=object())

        self.assertEqual(int(df.iloc[0]["holder_num"]), 45678)
        self.assertIn("vw_ts_stock_holdernumber", captured["sql"])
        self.assertEqual(captured["params"]["ts_code"], "000001.SZ")


class StockHolderNumberSyncTests(unittest.TestCase):
    def test_stock_holdernumber_metadata_registered(self):
        self.assertEqual(sync_mod.DATASET_TABLES["stock_holdernumber"], "ts_stock_holdernumber")
        self.assertEqual(
            sync_mod.NORMALIZED_VIEW_SPECS["stock_holdernumber"]["view_name"],
            "vw_ts_stock_holdernumber",
        )

    def test_resolve_business_key_uses_ts_code_end_date_ann_date_for_holdernumber(self):
        business_key = sync_mod.resolve_business_key(
            "stock_holdernumber",
            {
                "ts_code": "000001.SZ",
                "end_date": "20260515",
                "ann_date": "20260520",
                "holder_num": 45678,
            },
        )

        self.assertEqual(business_key, "stock_holdernumber|000001.SZ|20260515|20260520")

    def test_resolve_sync_window_advances_from_latest_ann_date_for_holdernumber(self):
        args = SimpleNamespace(
            financial_start="20240101",
            financial_lookback_days=30,
            daily_start="20250101",
            daily_lookback_days=1,
        )

        with patch("src.sync_tushare_security_data.get_max_date", return_value="20260520"):
            start_date, end_date = sync_mod.resolve_sync_window(
                engine=object(),
                dataset_name="stock_holdernumber",
                table_name="ts_stock_holdernumber",
                args=args,
                run_end_date="20260524",
            )

        self.assertEqual(start_date, "20260521")
        self.assertEqual(end_date, "20260524")

    def test_build_json_integral_numeric_expr_accepts_integral_decimal_strings(self):
        expr = sync_mod.build_json_integral_numeric_expr("holder_num")

        self.assertIn("::numeric", expr)
        self.assertIn("::integer", expr)


class StockHolderNumberFetchTests(unittest.TestCase):
    def test_fetch_stock_holdernumber_calls_tushare_endpoint(self):
        calls = []

        class FakePro:
            def stk_holdernumber(self, **kwargs):
                calls.append(kwargs)
                return pd.DataFrame(
                    [
                        {
                            "ts_code": "000001.SZ",
                            "ann_date": "20260520",
                            "end_date": "20260515",
                            "holder_num": 45678,
                        }
                    ]
                )

        df = sync_mod.fetch_stock_holdernumber(FakePro(), "20260501", "20260524")

        self.assertEqual(len(df), 1)
        self.assertEqual(calls, [{"start_date": "20260501", "end_date": "20260524"}])

    def test_nightly_script_includes_stock_holdernumber_dataset(self):
        script_path = "D:/sourcecode/etf/scripts/etf-data-update.sh"
        with open(script_path, "r", encoding="utf-8") as handle:
            content = handle.read()

        self.assertIn("stock_holdernumber", content)


class StockProfileHolderNumberTests(unittest.TestCase):
    def test_get_stock_profile_selects_holder_number_fields(self):
        captured = {}

        def fake_read_sql(query, engine, params=None):
            captured["sql"] = str(query)
            return pd.DataFrame(
                [
                    {
                        "ts_code": "000001.SZ",
                        "name": "平安银行",
                        "holder_num": 45678,
                        "holder_ann_date": "2026-05-20",
                        "holder_end_date": "2026-05-15",
                    }
                ]
            )

        with patch("src.etf_stats.pd.read_sql", side_effect=fake_read_sql):
            df = etf_stats.get_stock_profile("000001.SZ", engine=object())

        self.assertEqual(int(df.iloc[0]["holder_num"]), 45678)
        self.assertIn("holder_num", captured["sql"])
        self.assertIn("vw_ts_stock_holdernumber", captured["sql"])


class HolderNumberDisplayTests(unittest.TestCase):
    def test_format_holder_number_metric_appends_end_date(self):
        from app import format_holder_number_metric

        value_text, delta_text = format_holder_number_metric(457610, "2026-03-31")

        self.assertEqual(value_text, "457,610")
        self.assertEqual(delta_text, "截止 2026-03-31")

    def test_format_holder_number_metric_handles_missing_date(self):
        from app import format_holder_number_metric

        value_text, delta_text = format_holder_number_metric(59942, None)

        self.assertEqual(value_text, "59,942")
        self.assertIsNone(delta_text)


if __name__ == "__main__":
    unittest.main()
