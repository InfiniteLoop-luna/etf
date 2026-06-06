import unittest
from datetime import date
from unittest.mock import patch

import pandas as pd

from src.lhb_sync import resolve_start_date, sync_lhb_dataset


class FakeLhbSyncPro:
    def __init__(self):
        self.top_list_dates = []
        self.top_inst_dates = []

    def trade_cal(self, exchange, start_date, end_date, is_open):
        return pd.DataFrame(
            [
                {"cal_date": "20260101", "is_open": 0},
                {"cal_date": "20260102", "is_open": 1},
                {"cal_date": "20260105", "is_open": 1},
                {"cal_date": "20260106", "is_open": 1},
            ]
        )

    def top_list(self, trade_date, fields=None):
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

    def top_inst(self, trade_date, fields=None):
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


class LhbSyncTests(unittest.TestCase):
    def test_resolve_start_date_stays_inside_current_year(self):
        with patch("src.lhb_sync.get_max_trade_date", return_value=None):
            self.assertEqual(
                resolve_start_date(object(), "ts_lhb_top_list", force_start="20250101", today=date(2026, 6, 6)),
                "20260101",
            )

        with patch("src.lhb_sync.get_max_trade_date", return_value="20260605"):
            self.assertEqual(
                resolve_start_date(object(), "ts_lhb_top_list", lookback_days=2, today=date(2026, 6, 6)),
                "20260603",
            )

        with patch("src.lhb_sync.get_max_trade_date", return_value="20251231"):
            self.assertEqual(
                resolve_start_date(object(), "ts_lhb_top_list", lookback_days=2, today=date(2026, 6, 6)),
                "20260101",
            )

    @patch("src.lhb_sync.upsert_rows")
    def test_sync_lhb_dataset_batches_trade_dates_and_uses_distinct_keys(self, mock_upsert):
        mock_upsert.return_value = 2
        pro = FakeLhbSyncPro()

        written = sync_lhb_dataset(
            engine=object(),
            pro=pro,
            dataset_name="top_list",
            start_date="20260101",
            end_date="20260106",
            batch_days=2,
            request_sleep_seconds=0,
            today=date(2026, 6, 6),
        )

        self.assertEqual(written, 4)
        self.assertEqual(pro.top_list_dates, ["20260105", "20260106"])
        first_call = mock_upsert.call_args_list[0]
        self.assertEqual(first_call.args[1], "ts_lhb_top_list")
        self.assertEqual(first_call.args[2], "top_list")
        self.assertEqual(first_call.kwargs["extra_key"], "__lhb_extra_key")
        self.assertEqual(first_call.args[3][0]["__lhb_extra_key"], "涨幅偏离值达7%的证券")

        mock_upsert.reset_mock()
        written_inst = sync_lhb_dataset(
            engine=object(),
            pro=pro,
            dataset_name="top_inst",
            start_date="20260101",
            end_date="20260106",
            batch_days=1,
            request_sleep_seconds=0,
            today=date(2026, 6, 6),
        )

        self.assertEqual(written_inst, 2)
        self.assertEqual(pro.top_inst_dates, ["20260106"])
        inst_rows = mock_upsert.call_args.args[3]
        self.assertEqual(inst_rows[0]["__lhb_extra_key"], "机构专用|0|涨幅偏离值达7%的证券")


if __name__ == "__main__":
    unittest.main()
