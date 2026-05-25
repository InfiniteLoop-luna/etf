import unittest
from datetime import date
from unittest.mock import Mock, patch

import pandas as pd

from src.distribution_analyzer import (
    analyze_tick_data,
    fetch_daily_kline,
    generate_detailed_report,
    find_volume_price_signals,
)


class DistributionAnalyzerTests(unittest.TestCase):
    def test_analyze_tick_data_exposes_big_order_pct(self):
        tick_df = pd.DataFrame(
            {
                "price": [10.0, 10.2, 9.9],
                "vol": [100, 600, 200],
                "buyorsell": [0, 1, 1],
            }
        )

        result = analyze_tick_data(tick_df)

        self.assertEqual(result["status"], "ok")
        self.assertAlmostEqual(result["big_order_pct"], 66.6666666667, places=6)
        self.assertAlmostEqual(result["sell_ratio"], 88.8888888889, places=6)
        self.assertAlmostEqual(result["big_sell_ratio"], 100.0, places=6)
        self.assertEqual(result["big_net"], -600)

    def test_find_volume_price_signals_detects_continuous_shrinking_bearish_decline(self):
        rows = []
        for idx in range(25):
            rows.append(
                {
                    "high": 10.0,
                    "close": 10.0,
                    "vol": 1000 if idx < 22 else [700, 600, 500][idx - 22],
                    "pct_change": 0.2 if idx < 22 else [-0.6, -0.8, -0.5][idx - 22],
                    "vol_ratio": 1.0 if idx < 22 else [0.7, 0.6, 0.5][idx - 22],
                    "ma20": 9.5,
                    "upper_shadow_ratio": 0.1,
                }
            )
        df = pd.DataFrame(rows)
        df.index = pd.date_range("2025-01-01", periods=len(df))

        signals = find_volume_price_signals(df)

        self.assertIn("连续缩量阴跌", signals)
        self.assertEqual(len(signals["连续缩量阴跌"]), 1)

    @patch("src.distribution_analyzer.create_client")
    @patch("src.distribution_analyzer.get_stock_kline_timeseries")
    def test_fetch_daily_kline_prefers_db_cache(self, mock_get_kline, mock_create_client):
        cached_df = pd.DataFrame(
            [
                {
                    "trade_date": date(2026, 5, 22),
                    "open": 46.07,
                    "high": 49.18,
                    "low": 46.07,
                    "close": 49.17,
                    "vol": 329668.22,
                    "amount": 1594416.664,
                }
            ]
        )
        mock_get_kline.return_value = cached_df

        result = fetch_daily_kline("000733.SZ", engine=Mock())

        self.assertEqual(len(result), 1)
        self.assertEqual(str(result.index[0]), "2026-05-22")
        self.assertEqual(float(result.iloc[0]["close"]), 49.17)
        mock_get_kline.assert_called_once()
        mock_create_client.assert_not_called()

    @patch("src.distribution_analyzer.create_client", side_effect=AssertionError("client should not be created"))
    @patch("src.distribution_analyzer.fetch_minutes")
    @patch("src.distribution_analyzer.fetch_transactions")
    @patch("src.distribution_analyzer.identify_distribution_phase", return_value=[])
    @patch("src.distribution_analyzer.find_volume_price_signals", return_value={})
    @patch("src.distribution_analyzer.fetch_daily_kline")
    def test_generate_detailed_report_can_use_cached_helpers_without_client(
        self,
        mock_fetch_daily_kline,
        mock_find_signals,
        mock_phases,
        mock_fetch_transactions,
        mock_fetch_minutes,
        _mock_create_client,
    ):
        daily_df = pd.DataFrame(
            [
                {
                    "trade_date": date(2026, 5, 1 + idx),
                    "open": 46.0 + idx * 0.1,
                    "high": 46.5 + idx * 0.1,
                    "low": 45.8 + idx * 0.1,
                    "close": 46.1 + idx * 0.1,
                    "vol": 100000 + idx * 1000,
                    "amount": 4600000 + idx * 10000,
                }
                for idx in range(30)
            ]
        )
        daily_df.index = pd.to_datetime(daily_df["trade_date"])
        mock_fetch_daily_kline.return_value = daily_df
        mock_fetch_transactions.return_value = pd.DataFrame()
        mock_fetch_minutes.return_value = pd.DataFrame()

        report = generate_detailed_report("000733.SZ", "振华科技", engine=None)

        self.assertIn("振华科技", report)
        self.assertIn("Step 1", report)
        mock_fetch_daily_kline.assert_called_once()
        mock_find_signals.assert_called_once()
        mock_phases.assert_called_once()
        mock_fetch_transactions.assert_called()
        mock_fetch_minutes.assert_called()


if __name__ == "__main__":
    unittest.main()
