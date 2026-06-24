import unittest
from unittest.mock import patch

import pandas as pd

import src.fund_hot_stocks as fhs


class FundPortfolioByFundSyncTests(unittest.TestCase):
    def test_normalize_fund_ts_code_accepts_plain_six_digit_codes(self):
        self.assertTrue(hasattr(fhs, "normalize_fund_ts_code"), "normalize_fund_ts_code should exist")

        self.assertEqual(fhs.normalize_fund_ts_code("005850"), "005850.OF")
        self.assertEqual(fhs.normalize_fund_ts_code("010703.of"), "010703.OF")
        self.assertIsNone(fhs.normalize_fund_ts_code(""))

    def test_query_fund_codes_for_portfolio_sync_filters_by_management_keyword(self):
        self.assertTrue(
            hasattr(fhs, "query_fund_codes_for_portfolio_sync"),
            "query_fund_codes_for_portfolio_sync should exist",
        )

        class FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeEngine:
            def connect(self):
                return FakeConnection()

        with patch.object(
            fhs.pd,
            "read_sql",
            return_value=pd.DataFrame({"fund_code": ["005850.OF", "005850.OF", "010703.of", None, ""]}),
        ) as mock_read_sql:
            codes = fhs.query_fund_codes_for_portfolio_sync(
                engine=FakeEngine(),
                management_keyword="\u8d22\u901a",
                statuses=("L",),
                limit=50,
            )

        self.assertEqual(codes, ["005850.OF", "010703.OF"])
        sql_text = str(mock_read_sql.call_args.args[0])
        params = mock_read_sql.call_args.kwargs["params"]
        self.assertIn("management ILIKE :management_keyword", sql_text)
        self.assertIn("status_0", sql_text)
        self.assertEqual(params["management_keyword"], "%\u8d22\u901a%")
        self.assertEqual(params["status_0"], "L")
        self.assertEqual(params["limit"], 50)

    def test_sync_fund_portfolio_by_fund_calls_tushare_for_each_fund_and_period(self):
        self.assertTrue(
            hasattr(fhs, "sync_fund_portfolio_by_fund"),
            "sync_fund_portfolio_by_fund should exist",
        )

        class FakePro:
            def __init__(self):
                self.calls = []

            def fund_portfolio(self, **kwargs):
                self.calls.append(kwargs)
                return pd.DataFrame(
                    [
                        {
                            "ts_code": kwargs["ts_code"],
                            "ann_date": "20260420",
                            "end_date": kwargs["period"],
                            "symbol": "600519.SH",
                            "mkv": 100.0,
                            "amount": 10.0,
                            "stk_mkv_ratio": 1.2,
                            "stk_float_ratio": 0.01,
                        }
                    ]
                )

        fake_pro = FakePro()
        inserted_batches = []

        def fake_upsert(engine, rows):
            inserted_batches.append(rows)
            return len(rows)

        with patch.object(fhs, "resolve_portfolio_periods", return_value=["20260331", "20260630"]), patch.object(
            fhs,
            "query_fund_codes_for_portfolio_sync",
            return_value=["005850.OF", "010703.OF"],
        ), patch.object(fhs, "_upsert_fund_portfolio_rows", side_effect=fake_upsert), patch.object(
            fhs.time, "sleep"
        ):
            total = fhs.sync_fund_portfolio_by_fund(engine=object(), pro=fake_pro, api_sleep=0)

        self.assertEqual(total, 4)
        self.assertEqual(
            fake_pro.calls,
            [
                {"ts_code": "005850.OF", "period": "20260331"},
                {"ts_code": "010703.OF", "period": "20260331"},
                {"ts_code": "005850.OF", "period": "20260630"},
                {"ts_code": "010703.OF", "period": "20260630"},
            ],
        )
        self.assertEqual(len(inserted_batches), 4)
        self.assertTrue(all(batch[0]["ts_code"].endswith(".OF") for batch in inserted_batches))

    def test_query_missing_fund_portfolio_tasks_finds_fund_period_gaps(self):
        self.assertTrue(
            hasattr(fhs, "query_missing_fund_portfolio_tasks"),
            "query_missing_fund_portfolio_tasks should exist",
        )

        class FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeEngine:
            def connect(self):
                return FakeConnection()

        with patch.object(
            fhs.pd,
            "read_sql",
            return_value=pd.DataFrame({"fund_code": ["005850.OF"], "period": ["20260331"]}),
        ) as mock_read_sql:
            tasks = fhs.query_missing_fund_portfolio_tasks(
                engine=FakeEngine(),
                periods=["20260331", "20260630"],
                management_keyword="\u8d22\u901a",
                statuses=("L",),
                limit=10,
            )

        self.assertEqual(tasks, [{"fund_code": "005850.OF", "period": "20260331"}])
        sql_text = str(mock_read_sql.call_args.args[0])
        params = mock_read_sql.call_args.kwargs["params"]
        self.assertIn("LEFT JOIN vw_fund_portfolio", sql_text)
        self.assertIn("management ILIKE :management_keyword", sql_text)
        self.assertIn("v.fund_code IS NULL", sql_text)
        self.assertEqual(params["period_0"], "20260331")
        self.assertEqual(params["period_1"], "20260630")
        self.assertEqual(params["management_keyword"], "%\u8d22\u901a%")
        self.assertEqual(params["limit"], 10)

    def test_sync_fund_portfolio_dynamic_refreshes_basic_and_fetches_only_missing_tasks(self):
        self.assertTrue(
            hasattr(fhs, "sync_fund_portfolio_dynamic"),
            "sync_fund_portfolio_dynamic should exist",
        )

        class FakePro:
            def __init__(self):
                self.calls = []

            def fund_portfolio(self, **kwargs):
                self.calls.append(kwargs)
                return pd.DataFrame(
                    [
                        {
                            "ts_code": kwargs["ts_code"],
                            "ann_date": "20260420",
                            "end_date": kwargs["period"],
                            "symbol": "600519.SH",
                            "mkv": 100.0,
                        }
                    ]
                )

        fake_pro = FakePro()
        missing_tasks = [
            {"fund_code": "005850.OF", "period": "20260331"},
            {"fund_code": "010703.OF", "period": "20260630"},
        ]

        with patch.object(fhs, "resolve_portfolio_periods", return_value=["20260331", "20260630"]), patch.object(
            fhs, "sync_fund_basic", return_value=7
        ) as mock_sync_basic, patch.object(
            fhs, "query_missing_fund_portfolio_tasks", return_value=missing_tasks
        ), patch.object(
            fhs, "_upsert_fund_portfolio_rows", side_effect=lambda engine, rows: len(rows)
        ), patch.object(
            fhs.time, "sleep"
        ):
            result = fhs.sync_fund_portfolio_dynamic(engine=object(), pro=fake_pro, api_sleep=0)

        mock_sync_basic.assert_called_once()
        self.assertEqual(result, {"fund_basic": 7, "fund_portfolio": 2, "missing_tasks": 2})
        self.assertEqual(
            fake_pro.calls,
            [
                {"ts_code": "005850.OF", "period": "20260331"},
                {"ts_code": "010703.OF", "period": "20260630"},
            ],
        )

    def test_search_funds_keeps_portfolio_only_code_searchable(self):
        self.assertTrue(hasattr(fhs, "search_funds"), "search_funds should exist")

        class FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeEngine:
            def connect(self):
                return FakeConnection()

        portfolio_only_df = pd.DataFrame(
            [
                {
                    "fund_code": "159915.SZ",
                    "name": "159915.SZ",
                    "management": "持仓表补全",
                    "fund_type": "场内基金/ETF",
                    "invest_type": None,
                    "status": None,
                    "latest_end_date": pd.Timestamp("2026-06-30"),
                    "source_priority": 2,
                    "holding_priority": 1,
                    "match_rank": 0,
                    "name_pos": 999999,
                    "name_len": 9,
                }
            ]
        )

        with patch.object(fhs.pd, "read_sql", return_value=portfolio_only_df) as mock_read_sql:
            result = fhs.search_funds("159915", limit=10, engine=FakeEngine())

        self.assertEqual(result.iloc[0]["fund_code"], "159915.SZ")
        sql_text = str(mock_read_sql.call_args.args[0])
        params = mock_read_sql.call_args.kwargs["params"]
        self.assertIn("portfolio_only", sql_text)
        self.assertEqual(params["exact"], "159915.OF")
        self.assertEqual(params["bare_code"], "159915")
        self.assertEqual(params["prefix_upper"], "159915%")


if __name__ == "__main__":
    unittest.main()
