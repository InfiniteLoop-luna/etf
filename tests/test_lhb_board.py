import math
import unittest

import pandas as pd

from src.lhb_board import build_lhb_today_board_model, extract_lhb_treemap_stock_code


class LhbBoardTests(unittest.TestCase):
    def test_build_lhb_today_board_model_groups_latest_day_by_sector(self):
        top_list_df = pd.DataFrame(
            [
                {
                    "trade_date": "20260604",
                    "ts_code": "000001.SZ",
                    "name": "平安银行",
                    "pct_change": 2.0,
                    "turnover_rate": 8.0,
                    "l_buy": 20_000_000,
                    "l_sell": 10_000_000,
                    "l_amount": 30_000_000,
                    "net_amount": 10_000_000,
                    "reason": "前一日原因",
                },
                {
                    "trade_date": "20260605",
                    "ts_code": "000001.SZ",
                    "name": "平安银行",
                    "pct_change": 9.8,
                    "turnover_rate": 13.0,
                    "l_buy": 80_000_000,
                    "l_sell": 30_000_000,
                    "l_amount": 110_000_000,
                    "net_amount": 50_000_000,
                    "reason": "日涨幅偏离值达7%",
                },
                {
                    "trade_date": "20260605",
                    "ts_code": "300274.SZ",
                    "name": "阳光电源",
                    "pct_change": -4.2,
                    "turnover_rate": 18.0,
                    "l_buy": 15_000_000,
                    "l_sell": 45_000_000,
                    "l_amount": 60_000_000,
                    "net_amount": -30_000_000,
                    "reason": "日跌幅偏离值达7%",
                },
                {
                    "trade_date": "20260605",
                    "ts_code": "688123.SH",
                    "name": "芯片科技",
                    "pct_change": 6.6,
                    "turnover_rate": 21.0,
                    "l_buy": 25_000_000,
                    "l_sell": 5_000_000,
                    "l_amount": 30_000_000,
                    "net_amount": 20_000_000,
                    "reason": "有价格涨跌幅限制的日收盘价格涨幅达到15%",
                },
            ]
        )
        inst_df = pd.DataFrame(
            [
                {
                    "trade_date": "20260605",
                    "ts_code": "000001.SZ",
                    "exalter": "机构专用",
                    "side": "0",
                    "buy": 20_000_000,
                    "sell": 5_000_000,
                    "net_buy": 15_000_000,
                    "reason": "日涨幅偏离值达7%",
                }
            ]
        )

        model = build_lhb_today_board_model(
            top_list_df,
            inst_df,
            industry_map={
                "000001.SZ": "银行",
                "300274.SZ": "电力设备",
                "688123.SH": "电子",
            },
        )

        self.assertEqual(model["trade_date_label"], "2026-06-05")
        self.assertEqual(model["stock_count"], 3)
        self.assertEqual([sector["sector"] for sector in model["sectors"]], ["银行", "电力设备", "电子"])
        bank_stock = model["sectors"][0]["stocks"][0]
        self.assertEqual(bank_stock["ts_code"], "000001.SZ")
        self.assertEqual(bank_stock["name"], "平安银行")
        self.assertAlmostEqual(bank_stock["net_amount_yi"], 0.5)
        self.assertAlmostEqual(bank_stock["inst_net_yi"], 0.15)
        self.assertEqual(bank_stock["direction"], "positive")
        self.assertIn("日涨幅偏离值达7%", bank_stock["reason"])

    def test_extract_lhb_treemap_stock_code_prefers_stock_customdata(self):
        click_points = [
            {"id": "sector:银行", "customdata": ["", "银行"]},
            {"id": "stock:000001.SZ", "customdata": ["000001.SZ", "平安银行"]},
        ]

        self.assertEqual(extract_lhb_treemap_stock_code(click_points), "000001.SZ")

    def test_today_board_tile_values_use_sqrt_compression_and_single_stock_cap(self):
        rows = [
            {
                "trade_date": "20260605",
                "ts_code": "300319.SZ",
                "name": "麦捷科技",
                "pct_change": 14.43,
                "turnover_rate": 22.0,
                "l_buy": 6_000_000_000,
                "l_sell": 3_000_000_000,
                "l_amount": 10_000_000_000,
                "net_amount": 2_000_000_000,
                "reason": "严重异常期间日收盘价格涨幅偏离值累计达到100%",
            }
        ]
        for index in range(9):
            rows.append(
                {
                    "trade_date": "20260605",
                    "ts_code": f"002{index:03d}.SZ",
                    "name": f"普通股票{index}",
                    "pct_change": 3.0 + index,
                    "turnover_rate": 8.0,
                    "l_buy": 60_000_000,
                    "l_sell": 40_000_000,
                    "l_amount": 100_000_000,
                    "net_amount": 20_000_000,
                    "reason": "日涨幅偏离值达7%",
                }
            )

        model = build_lhb_today_board_model(
            pd.DataFrame(rows),
            pd.DataFrame(),
            industry_map={row["ts_code"]: "电子" for row in rows},
        )
        stocks = [stock for sector in model["sectors"] for stock in sector["stocks"]]
        total_tile_value = sum(stock["tile_value"] for stock in stocks)
        largest_stock = max(stocks, key=lambda stock: stock["tile_value"])
        small_stock = next(stock for stock in stocks if stock["ts_code"] != "300319.SZ")

        self.assertEqual(largest_stock["ts_code"], "300319.SZ")
        self.assertAlmostEqual(largest_stock["tile_raw_value"], 100.0)
        self.assertAlmostEqual(largest_stock["tile_compressed_value"], math.sqrt(100.0))
        self.assertLess(largest_stock["tile_value"], largest_stock["tile_compressed_value"])
        self.assertLessEqual(largest_stock["tile_value"], total_tile_value * 0.151)
        self.assertGreater(largest_stock["tile_value"], small_stock["tile_value"])


if __name__ == "__main__":
    unittest.main()
