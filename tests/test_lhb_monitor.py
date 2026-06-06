import unittest
from datetime import date

import pandas as pd

from src.lhb_monitor import (
    build_lhb_stock_summary,
    fetch_lhb_data,
    prepare_lhb_inst_frame,
    prepare_lhb_top_list_frame,
    resolve_lhb_date_window,
)


class FakeLhbPro:
    def __init__(self):
        self.top_list_dates = []
        self.top_inst_dates = []

    def trade_cal(self, exchange, start_date, end_date, is_open):
        self.trade_cal_args = {
            "exchange": exchange,
            "start_date": start_date,
            "end_date": end_date,
            "is_open": is_open,
        }
        return pd.DataFrame(
            [
                {"cal_date": "20260101", "is_open": 0},
                {"cal_date": "20260102", "is_open": 1},
                {"cal_date": "20260105", "is_open": 1},
            ]
        )

    def top_list(self, trade_date, ts_code=None, fields=None):
        self.top_list_dates.append(trade_date)
        return pd.DataFrame(
            [
                {
                    "trade_date": trade_date,
                    "ts_code": "000001.SZ",
                    "name": "平安银行",
                    "close": 10.0,
                    "pct_change": 9.9,
                    "turnover_rate": 8.2,
                    "amount": 1_000_000_000.0,
                    "l_sell": 20_000_000.0,
                    "l_buy": 60_000_000.0,
                    "l_amount": 80_000_000.0,
                    "net_amount": 40_000_000.0,
                    "net_rate": 5.0,
                    "amount_rate": 8.0,
                    "float_values": 10_000_000_000.0,
                    "reason": "涨幅偏离值达7%的证券",
                }
            ]
        )

    def top_inst(self, trade_date, ts_code=None, fields=None):
        self.top_inst_dates.append(trade_date)
        return pd.DataFrame(
            [
                {
                    "trade_date": trade_date,
                    "ts_code": "000001.SZ",
                    "exalter": "机构专用",
                    "side": "0",
                    "buy": 30_000_000.0,
                    "buy_rate": 3.0,
                    "sell": 10_000_000.0,
                    "sell_rate": 1.0,
                    "net_buy": 20_000_000.0,
                    "reason": "涨幅偏离值达7%的证券",
                }
            ]
        )


class LhbMonitorTests(unittest.TestCase):
    def test_resolve_lhb_date_window_clamps_to_current_year_and_today(self):
        start_date, end_date = resolve_lhb_date_window(
            start_date="2025-12-01",
            end_date="2026-06-10",
            today=date(2026, 6, 6),
        )

        self.assertEqual(start_date, "20260101")
        self.assertEqual(end_date, "20260606")

    def test_fetch_lhb_data_only_requests_current_year_trade_dates(self):
        pro = FakeLhbPro()

        result = fetch_lhb_data(
            pro=pro,
            start_date="20250101",
            end_date="20260606",
            today=date(2026, 6, 6),
            request_sleep_seconds=0,
        )

        self.assertEqual(pro.trade_cal_args["start_date"], "20260101")
        self.assertEqual(pro.trade_cal_args["end_date"], "20260606")
        self.assertEqual(pro.top_list_dates, ["20260102", "20260105"])
        self.assertEqual(pro.top_inst_dates, ["20260102", "20260105"])
        self.assertEqual(len(result["top_list"]), 2)
        self.assertEqual(len(result["top_inst"]), 2)

    def test_build_lhb_stock_summary_merges_top_list_and_institution_flows(self):
        top_list_df = prepare_lhb_top_list_frame(
            pd.DataFrame(
                [
                    {
                        "trade_date": "20260102",
                        "ts_code": "000001.SZ",
                        "name": "平安银行",
                        "close": 10.0,
                        "pct_change": 9.9,
                        "turnover_rate": 8.2,
                        "amount": 1_000_000_000.0,
                        "l_sell": 20_000_000.0,
                        "l_buy": 60_000_000.0,
                        "l_amount": 80_000_000.0,
                        "net_amount": 40_000_000.0,
                        "net_rate": 5.0,
                        "amount_rate": 8.0,
                        "float_values": 10_000_000_000.0,
                        "reason": "涨幅偏离值达7%的证券",
                    },
                    {
                        "trade_date": "20260105",
                        "ts_code": "000001.SZ",
                        "name": "平安银行",
                        "close": 10.8,
                        "pct_change": 4.5,
                        "turnover_rate": 10.1,
                        "amount": 900_000_000.0,
                        "l_sell": 30_000_000.0,
                        "l_buy": 40_000_000.0,
                        "l_amount": 70_000_000.0,
                        "net_amount": 10_000_000.0,
                        "net_rate": 1.1,
                        "amount_rate": 7.7,
                        "float_values": 10_800_000_000.0,
                        "reason": "连续三个交易日内，涨幅偏离值累计达20%的证券",
                    },
                    {
                        "trade_date": "20260102",
                        "ts_code": "000002.SZ",
                        "name": "万科A",
                        "close": 8.0,
                        "pct_change": -9.8,
                        "turnover_rate": 5.0,
                        "amount": 500_000_000.0,
                        "l_sell": 35_000_000.0,
                        "l_buy": 15_000_000.0,
                        "l_amount": 50_000_000.0,
                        "net_amount": -20_000_000.0,
                        "net_rate": -4.0,
                        "amount_rate": 10.0,
                        "float_values": 5_000_000_000.0,
                        "reason": "跌幅偏离值达7%的证券",
                    },
                ]
            )
        )
        inst_df = prepare_lhb_inst_frame(
            pd.DataFrame(
                [
                    {
                        "trade_date": "20260102",
                        "ts_code": "000001.SZ",
                        "exalter": "机构专用",
                        "side": "0",
                        "buy": 30_000_000.0,
                        "buy_rate": 3.0,
                        "sell": 10_000_000.0,
                        "sell_rate": 1.0,
                        "net_buy": 20_000_000.0,
                        "reason": "涨幅偏离值达7%的证券",
                    },
                    {
                        "trade_date": "20260102",
                        "ts_code": "000002.SZ",
                        "exalter": "机构专用",
                        "side": "1",
                        "buy": 5_000_000.0,
                        "buy_rate": 1.0,
                        "sell": 15_000_000.0,
                        "sell_rate": 3.0,
                        "net_buy": -10_000_000.0,
                        "reason": "跌幅偏离值达7%的证券",
                    },
                ]
            )
        )

        summary = build_lhb_stock_summary(top_list_df, inst_df)

        first = summary.iloc[0]
        self.assertEqual(first["ts_code"], "000001.SZ")
        self.assertEqual(first["hit_count"], 2)
        self.assertEqual(first["trade_days"], 2)
        self.assertAlmostEqual(first["net_amount_yi"], 0.5)
        self.assertAlmostEqual(first["inst_net_yi"], 0.2)
        self.assertAlmostEqual(first["combined_net_yi"], 0.7)
        self.assertEqual(first["latest_date_label"], "2026-01-05")
        self.assertIn("涨幅偏离值达7%", first["reasons"])


if __name__ == "__main__":
    unittest.main()
