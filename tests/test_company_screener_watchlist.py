import unittest
from unittest.mock import patch

import pandas as pd

import app


class CompanyScreenerWatchlistTests(unittest.TestCase):
    def test_build_company_screener_result_action_df_marks_existing_rows(self):
        results_df = pd.DataFrame([
            {"ts_code": "000001.SZ", "name": "平安银行", "industry": "银行", "main_business": "零售银行", "product": "存贷款", "has_ever_st": False},
            {"ts_code": "000733.SZ", "name": "振华科技", "industry": "电子", "main_business": "电子元件", "product": "连接器", "has_ever_st": True},
        ])
        existing_df = pd.DataFrame([
            {"ts_code": "000733.SZ", "security_type": "stock", "security_name": "振华科技"},
        ])

        df = app.build_company_screener_result_action_df(results_df, existing_df)

        self.assertEqual(df["代码"].tolist(), ["000001.SZ", "000733.SZ"])
        self.assertEqual(df["已在自选"].tolist(), ["", "✅ 已在自选"])
        self.assertEqual(df["标签"].tolist(), ["", "曾经ST"])
        self.assertEqual(df["主要业务"].tolist(), ["零售银行", "电子元件"])
        self.assertEqual(df["产品及服务"].tolist(), ["存贷款", "连接器"])
        self.assertTrue(df["查询"].astype(str).str.contains("security_query=").all())
        self.assertFalse(df["选择"].any())

    def test_add_company_screener_rows_to_watchlist_adds_only_missing_codes(self):
        selected_rows = [
            {"代码": "000001.SZ", "简称": "平安银行"},
            {"代码": "000733.SZ", "简称": "振华科技"},
            {"代码": "000733.SZ", "简称": "振华科技"},
            {"代码": "", "简称": "空白"},
        ]
        existing_df = pd.DataFrame([
            {"ts_code": "000733.SZ", "security_type": "stock", "security_name": "振华科技"},
        ])
        add_calls = []
        distribution_calls = []
        research_calls = []

        def fake_add(username, ts_code, security_name="", security_type="stock"):
            add_calls.append((username, ts_code, security_name, security_type))

        def fake_dist(username, ts_code, engine):
            distribution_calls.append((username, ts_code, engine))

        def fake_research(username, ts_code, engine):
            research_calls.append((username, ts_code, engine))

        with patch.object(app, "add_watchlist_item", side_effect=fake_add):
            with patch.object(app, "trigger_single_distribution_refresh_bg", side_effect=fake_dist):
                with patch.object(app, "trigger_single_stock_research_refresh_bg", side_effect=fake_research):
                    summary = app.add_company_screener_rows_to_watchlist(
                        selected_rows,
                        "alice",
                        existing_watchlist_df=existing_df,
                        report_engine="ENGINE",
                    )

        self.assertEqual(summary["added"], 1)
        self.assertEqual(summary["skipped_existing"], 2)
        self.assertEqual(summary["skipped_invalid"], 1)
        self.assertEqual(summary["failed"], 0)
        self.assertEqual(add_calls, [("alice", "000001.SZ", "平安银行", "stock")])
        self.assertEqual(distribution_calls, [("alice", "000001.SZ", "ENGINE")])
        self.assertEqual(research_calls, [("alice", "000001.SZ", "ENGINE")])


if __name__ == "__main__":
    unittest.main()
