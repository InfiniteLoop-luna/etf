import unittest
from datetime import date
from unittest.mock import Mock, patch

import pandas as pd

from app import create_security_intraday_chart
from src.security_intraday_store import (
    _create_mootdx_client,
    empty_intraday_frame,
    fetch_stock_realtime_snapshot_from_mootdx,
    load_or_fetch_stock_intraday_timeseries,
    normalize_mootdx_code,
    normalize_mootdx_minutes_frame,
)


class SecurityIntradayStoreTests(unittest.TestCase):
    def test_normalize_mootdx_code_supports_ts_code_prefix_and_bj(self):
        self.assertEqual(normalize_mootdx_code("600036.SH")["symbol"], "600036")
        self.assertEqual(normalize_mootdx_code("600036.SH")["market_name"], "sh")
        self.assertEqual(normalize_mootdx_code("sz000001")["ts_code"], "000001.SZ")
        self.assertEqual(normalize_mootdx_code("430090")["market_name"], "bj")
        self.assertFalse(normalize_mootdx_code("430090")["supports_minutes"])

    def test_normalize_mootdx_minutes_frame_builds_trade_time_and_ohlc(self):
        raw_df = pd.DataFrame([
            {"price": 10.0, "vol": 100},
            {"price": 10.2, "vol": 150},
            {"price": 10.1, "vol": 120},
        ])

        df = normalize_mootdx_minutes_frame(raw_df, ts_code="600036.SH", trade_date="2026-05-08")

        self.assertEqual(len(df), 3)
        self.assertEqual(df.iloc[0]["trade_time"].strftime("%Y-%m-%d %H:%M:%S"), "2026-05-08 09:30:00")
        self.assertEqual(df.iloc[1]["trade_time"].strftime("%Y-%m-%d %H:%M:%S"), "2026-05-08 09:31:00")
        self.assertAlmostEqual(float(df.iloc[0]["open"]), 10.0)
        self.assertAlmostEqual(float(df.iloc[1]["open"]), 10.0)
        self.assertAlmostEqual(float(df.iloc[1]["close"]), 10.2)
        self.assertAlmostEqual(float(df.iloc[1]["high"]), 10.2)
        self.assertAlmostEqual(float(df.iloc[2]["low"]), 10.1)

    def test_create_security_intraday_chart_compresses_lunch_break(self):
        df = pd.DataFrame([
            {
                "ts_code": "600036.SH",
                "trade_date": date(2026, 5, 8),
                "trade_time": pd.Timestamp("2026-05-08 11:29:00"),
                "freq": "1min",
                "open": 10.0,
                "high": 10.1,
                "low": 9.9,
                "close": 10.0,
                "vol": 100,
                "amount": None,
            },
            {
                "ts_code": "600036.SH",
                "trade_date": date(2026, 5, 8),
                "trade_time": pd.Timestamp("2026-05-08 13:00:00"),
                "freq": "1min",
                "open": 10.0,
                "high": 10.2,
                "low": 10.0,
                "close": 10.1,
                "vol": 120,
                "amount": None,
            },
        ])

        fig = create_security_intraday_chart(df, title="test")

        self.assertIsNotNone(fig)
        self.assertEqual(fig.layout.xaxis.type, "date")
        self.assertTrue(fig.layout.xaxis.rangebreaks)
        self.assertEqual(fig.layout.xaxis.rangebreaks[0]["pattern"], "hour")
        self.assertEqual(list(fig.layout.xaxis.rangebreaks[0]["bounds"]), [11.5, 13])

    @patch("src.security_intraday_store._get_mootdx_default_server")
    @patch("src.security_intraday_store._get_mootdx_quotes_class")
    def test_create_mootdx_client_retries_with_explicit_server(self, mock_get_quotes_class, mock_get_default_server):
        quotes_cls = Mock()
        quotes_cls.factory.side_effect = [ValueError("not enough values to unpack"), Mock(name="client")]
        mock_get_quotes_class.return_value = quotes_cls
        mock_get_default_server.return_value = ("110.41.147.114", 7709)

        client = _create_mootdx_client(timeout=8)

        self.assertIsNotNone(client)
        self.assertEqual(quotes_cls.factory.call_count, 2)
        self.assertEqual(quotes_cls.factory.call_args_list[1].kwargs["server"], ("110.41.147.114", 7709))

    def test_fetch_stock_realtime_snapshot_from_mootdx_builds_change_fields(self):
        client = Mock()
        client.quotes.return_value = pd.DataFrame([
            {
                "price": 12.5,
                "last_close": 12.0,
                "open": 12.1,
                "high": 12.6,
                "low": 12.0,
                "amount": 123456789.0,
                "vol": 99999,
                "cur_vol": 321,
                "s_vol": 40000,
                "b_vol": 59999,
                "bid1": 12.49,
                "ask1": 12.5,
                "bid_vol1": 100,
                "ask_vol1": 200,
                "servertime": "14:59:59.000",
            }
        ])

        snapshot = fetch_stock_realtime_snapshot_from_mootdx("600036.SH", client=client)

        self.assertEqual(snapshot["status"], "ok")
        self.assertAlmostEqual(snapshot["change"], 0.5)
        self.assertAlmostEqual(snapshot["pct_change"], 0.5 / 12.0 * 100.0)
        self.assertEqual(snapshot["market_name"], "sh")
        self.assertEqual(snapshot["symbol"], "600036")

    @patch("src.security_intraday_store.get_stock_intraday_timeseries")
    @patch("src.security_intraday_store.upsert_stock_intraday_timeseries")
    @patch("src.security_intraday_store.fetch_stock_intraday_from_tushare")
    @patch("src.security_intraday_store.fetch_stock_intraday_from_mootdx")
    def test_load_or_fetch_stock_intraday_timeseries_prefers_mootdx(
        self,
        mock_fetch_mootdx,
        mock_fetch_tushare,
        mock_upsert,
        mock_get_cached,
    ):
        first_cached = empty_intraday_frame()
        mootdx_df = pd.DataFrame([
            {
                "ts_code": "600036.SH",
                "trade_date": date(2026, 5, 8),
                "trade_time": pd.Timestamp("2026-05-08 09:30:00"),
                "freq": "1min",
                "open": 10.0,
                "high": 10.0,
                "low": 10.0,
                "close": 10.0,
                "vol": 100,
                "amount": None,
            }
        ])
        cached_after_upsert = mootdx_df.assign(source="mootdx.minutes", ingested_at=pd.NaT, updated_at=pd.NaT)
        mock_get_cached.side_effect = [first_cached, cached_after_upsert]
        mock_fetch_mootdx.return_value = mootdx_df

        df, source = load_or_fetch_stock_intraday_timeseries(
            ts_code="600036.SH",
            trade_date="2026-05-08",
            freq="1min",
            engine=Mock(),
        )

        self.assertEqual(source, "mootdx")
        self.assertEqual(len(df), 1)
        mock_fetch_mootdx.assert_called_once()
        mock_fetch_tushare.assert_not_called()
        mock_upsert.assert_called_once()

    @patch("src.security_intraday_store.get_stock_intraday_timeseries")
    @patch("src.security_intraday_store.upsert_stock_intraday_timeseries")
    @patch("src.security_intraday_store.fetch_stock_intraday_from_tushare")
    @patch("src.security_intraday_store.fetch_stock_intraday_from_mootdx")
    def test_load_or_fetch_stock_intraday_timeseries_prefers_mootdx_over_existing_tushare_cache(
        self,
        mock_fetch_mootdx,
        mock_fetch_tushare,
        mock_upsert,
        mock_get_cached,
    ):
        cached_tushare_df = pd.DataFrame([
            {
                "ts_code": "600036.SH",
                "trade_date": pd.Timestamp("2026-05-08"),
                "trade_time": pd.Timestamp("2026-05-08 09:30:00"),
                "freq": "1min",
                "open": 9.9,
                "high": 10.1,
                "low": 9.8,
                "close": 10.0,
                "vol": 100,
                "amount": 1000,
                "source": "tushare.stk_mins",
                "ingested_at": pd.NaT,
                "updated_at": pd.NaT,
            }
        ])
        mootdx_df = pd.DataFrame([
            {
                "ts_code": "600036.SH",
                "trade_date": date(2026, 5, 8),
                "trade_time": pd.Timestamp("2026-05-08 09:30:00"),
                "freq": "1min",
                "open": 10.0,
                "high": 10.0,
                "low": 10.0,
                "close": 10.0,
                "vol": 100,
                "amount": None,
            }
        ])
        cached_after_upsert = mootdx_df.assign(source="mootdx.minutes", ingested_at=pd.NaT, updated_at=pd.NaT)
        mock_get_cached.side_effect = [cached_tushare_df, cached_after_upsert]
        mock_fetch_mootdx.return_value = mootdx_df

        df, source = load_or_fetch_stock_intraday_timeseries(
            ts_code="600036.SH",
            trade_date="2026-05-08",
            freq="1min",
            engine=Mock(),
        )

        self.assertEqual(source, "mootdx")
        self.assertEqual(len(df), 1)
        mock_fetch_mootdx.assert_called_once()
        mock_fetch_tushare.assert_not_called()
        mock_upsert.assert_called_once()

    @patch("src.security_intraday_store.get_stock_intraday_timeseries")
    @patch("src.security_intraday_store.upsert_stock_intraday_timeseries")
    @patch("src.security_intraday_store.fetch_stock_intraday_from_tushare")
    @patch("src.security_intraday_store.fetch_stock_intraday_from_mootdx")
    def test_load_or_fetch_stock_intraday_timeseries_falls_back_to_tushare(
        self,
        mock_fetch_mootdx,
        mock_fetch_tushare,
        mock_upsert,
        mock_get_cached,
    ):
        first_cached = empty_intraday_frame()
        tushare_df = pd.DataFrame([
            {
                "ts_code": "600036.SH",
                "trade_date": date(2026, 5, 8),
                "trade_time": pd.Timestamp("2026-05-08 09:30:00"),
                "freq": "1min",
                "open": 10.0,
                "high": 10.1,
                "low": 9.9,
                "close": 10.0,
                "vol": 100,
                "amount": 1000,
            }
        ])
        cached_after_upsert = tushare_df.assign(source="tushare.stk_mins", ingested_at=pd.NaT, updated_at=pd.NaT)
        mock_get_cached.side_effect = [first_cached, cached_after_upsert]
        mock_fetch_mootdx.return_value = empty_intraday_frame()
        mock_fetch_tushare.return_value = tushare_df

        df, source = load_or_fetch_stock_intraday_timeseries(
            ts_code="600036.SH",
            trade_date="2026-05-08",
            freq="1min",
            engine=Mock(),
        )

        self.assertEqual(source, "tushare")
        self.assertEqual(len(df), 1)
        mock_fetch_mootdx.assert_called_once()
        mock_fetch_tushare.assert_called_once()
        mock_upsert.assert_called_once()


if __name__ == "__main__":
    unittest.main()
