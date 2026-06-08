import unittest

import pandas as pd

from src.hotmoney_stock_tracker import (
    build_single_stock_hotmoney_model,
    normalize_stock_code,
)


class HotmoneyStockTrackerTests(unittest.TestCase):
    def test_normalize_stock_code_adds_exchange_suffix(self):
        self.assertEqual(normalize_stock_code("600519"), "600519.SH")
        self.assertEqual(normalize_stock_code("000001"), "000001.SZ")
        self.assertEqual(normalize_stock_code("300750"), "300750.SZ")
        self.assertEqual(normalize_stock_code("688981"), "688981.SH")
        self.assertEqual(normalize_stock_code("600519.sh"), "600519.SH")
        self.assertEqual(normalize_stock_code("贵州茅台"), "贵州茅台")

    def test_build_single_stock_hotmoney_model_combines_direct_and_lhb_evidence(self):
        hotmoney_detail = pd.DataFrame(
            [
                {
                    "trade_date": "2026-06-01",
                    "ts_code": "000001.SZ",
                    "ts_name": "Ping An Bank",
                    "hm_name": "Alpha Hotmoney",
                    "hm_orgs": "Alpha Seat A",
                    "tag": "momentum",
                    "buy_amount": 80_000_000,
                    "sell_amount": 20_000_000,
                    "net_amount": 60_000_000,
                },
                {
                    "trade_date": "2026-06-03",
                    "ts_code": "000001.SZ",
                    "ts_name": "Ping An Bank",
                    "hm_name": "Beta Hotmoney",
                    "hm_orgs": "Beta Seat B",
                    "tag": "sell pressure",
                    "buy_amount": 10_000_000,
                    "sell_amount": 50_000_000,
                    "net_amount": -40_000_000,
                },
            ]
        )
        lhb_top_list = pd.DataFrame(
            [
                {
                    "trade_date": "2026-06-03",
                    "ts_code": "000001.SZ",
                    "name": "Ping An Bank",
                    "l_buy": 120_000_000,
                    "l_sell": 60_000_000,
                    "l_amount": 180_000_000,
                    "net_amount": 60_000_000,
                    "reason": "price deviation",
                }
            ]
        )
        lhb_inst = pd.DataFrame(
            [
                {
                    "trade_date": "2026-06-03",
                    "ts_code": "000001.SZ",
                    "exalter": "Gamma Seat",
                    "buy": 30_000_000,
                    "sell": 5_000_000,
                    "net_buy": 25_000_000,
                    "reason": "price deviation",
                }
            ]
        )

        model = build_single_stock_hotmoney_model(
            hotmoney_detail,
            lhb_top_list_df=lhb_top_list,
            lhb_inst_df=lhb_inst,
        )

        self.assertEqual(model["stock_code"], "000001.SZ")
        self.assertEqual(model["stock_name"], "Ping An Bank")
        self.assertEqual(model["date_label"], "2026-06-01~2026-06-03")
        self.assertEqual(model["direct_hotmoney_count"], 2)
        self.assertEqual(model["lhb_seat_count"], 1)
        self.assertAlmostEqual(model["direct_net_yi"], 0.2)
        self.assertAlmostEqual(model["lhb_seat_net_yi"], 0.25)
        self.assertEqual(model["confidence_label"], "direct+seat")

        actor_summary = model["actor_summary"]
        self.assertEqual(actor_summary.iloc[0]["actor_name"], "Alpha Hotmoney")
        self.assertEqual(actor_summary.iloc[0]["evidence_type"], "direct_hotmoney")
        self.assertAlmostEqual(actor_summary.iloc[0]["net_amount_yi"], 0.6)

        evidence = model["evidence_detail"]
        self.assertEqual(len(evidence), 3)
        self.assertEqual(set(evidence["source"]), {"hm_detail", "lhb_top_inst"})
        self.assertIn("price deviation", model["lhb_reason_summary"].iloc[0]["reasons"])

        daily = model["daily_summary"].sort_values("trade_date")
        self.assertEqual(daily.iloc[1]["direct_actor_count"], 1)
        self.assertEqual(daily.iloc[1]["lhb_seat_count"], 1)
        self.assertAlmostEqual(daily.iloc[1]["net_amount_yi"], -0.15)


if __name__ == "__main__":
    unittest.main()
