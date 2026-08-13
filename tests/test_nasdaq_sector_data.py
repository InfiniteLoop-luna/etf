from __future__ import annotations

import unittest

import pandas as pd

from src.nasdaq_sector_data import (
    NasdaqStock,
    aggregate_sector_metrics,
    build_snapshot,
    calculate_period_return,
)


def _frame(closes: list[float], volumes: list[float] | None = None) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Date": pd.date_range("2026-01-02", periods=len(closes), freq="B"),
            "Close": closes,
            "Volume": volumes or [100.0] * len(closes),
        }
    )


class NasdaqSectorDataTest(unittest.TestCase):
    def test_period_return_uses_trading_observations(self) -> None:
        frame = _frame([100, 101, 102, 104, 110, 121])
        self.assertAlmostEqual(calculate_period_return(frame, "1日"), 10.0)
        self.assertAlmostEqual(calculate_period_return(frame, "5日"), 21.0)

    def test_sector_metrics_rank_strongest_and_pick_leader(self) -> None:
        stock_df = pd.DataFrame(
            [
                {"symbol": "AAA", "name": "甲", "sector": "半导体", "core_weight": 2.0, "return_1日": 4.0, "volume_ratio_20d": 1.5},
                {"symbol": "BBB", "name": "乙", "sector": "半导体", "core_weight": 1.0, "return_1日": 1.0, "volume_ratio_20d": 1.0},
                {"symbol": "CCC", "name": "丙", "sector": "软件", "core_weight": 1.0, "return_1日": -2.0, "volume_ratio_20d": 0.9},
            ]
        )
        actual = aggregate_sector_metrics(stock_df, period_label="1日", qqq_return=1.0)
        self.assertEqual(actual.iloc[0]["sector"], "半导体")
        self.assertEqual(actual.iloc[0]["leader_symbol"], "AAA")
        self.assertAlmostEqual(actual.iloc[0]["return_pct"], 3.0)
        self.assertAlmostEqual(actual.iloc[0]["relative_qqq_pct"], 2.0)

    def test_snapshot_records_partial_failures_without_losing_available_rows(self) -> None:
        stocks = (
            NasdaqStock("AAA", "甲", "半导体", 1.0),
            NasdaqStock("BAD", "坏数据", "半导体", 1.0),
        )

        def fetcher(symbol: str):
            if symbol == "BAD":
                raise RuntimeError("boom")
            return _frame([100, 101, 102, 103, 104, 105], [80, 90, 100, 110, 120, 130])

        snapshot = build_snapshot(period_label="1日", stocks=stocks, fetcher=fetcher)
        self.assertEqual(snapshot["coverage"], {"loaded": 1, "total": 2})
        self.assertIn("BAD", snapshot["errors"])
        self.assertEqual(len(snapshot["sectors"]), 1)
        self.assertEqual(snapshot["sectors"][0]["leader_symbol"], "AAA")


if __name__ == "__main__":
    unittest.main()
