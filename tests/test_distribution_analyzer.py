import unittest
from datetime import date
from unittest.mock import Mock, patch

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from src.distribution_analyzer import (
    analyze_tick_data,
    fetch_daily_kline,
    fetch_minutes,
    fetch_transactions,
    generate_detailed_report,
    find_volume_price_signals,
)
from app import (
    _get_distribution_report_state,
    queue_security_search_navigation,
    should_show_distribution_report_section,
)


class DistributionAnalyzerTests(unittest.TestCase):
    def _build_sqlite_engine(self):
        return create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )

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
    @patch("src.distribution_analyzer.get_stock_kline_timeseries", return_value=pd.DataFrame())
    def test_fetch_daily_kline_can_run_db_only_without_live_fallback(
        self,
        mock_get_kline,
        _mock_create_client,
    ):
        result = fetch_daily_kline("000733.SZ", engine=Mock(), allow_live_fetch=False)

        self.assertTrue(result.empty)
        mock_get_kline.assert_called_once()

    @patch("src.distribution_analyzer.fetch_daily_kline")
    @patch("src.distribution_report_store.get_daily_report", return_value="# cached report")
    def test_generate_detailed_report_prefers_cached_report_for_asof_trade_date(
        self,
        mock_get_daily_report,
        mock_fetch_daily_kline,
    ):
        report = generate_detailed_report(
            "000733.SZ",
            "\u632f\u534e\u79d1\u6280",
            engine=Mock(),
            asof_trade_date="2026-05-23",
            allow_live_fetch=False,
        )

        self.assertEqual(report, "# cached report")
        mock_get_daily_report.assert_called_once()
        mock_fetch_daily_kline.assert_not_called()

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

    @patch("src.distribution_analyzer.create_client", side_effect=AssertionError("client should not be created"))
    @patch("src.distribution_analyzer.fetch_minutes")
    @patch("src.distribution_analyzer.fetch_transactions")
    @patch(
        "src.distribution_analyzer.identify_distribution_phase",
        return_value=[
            {"peak_date": "2026-04-24", "end_date": "2026-04-30", "peak_price": 50.0, "low_price": 45.0, "decline_pct": -10.0, "duration_days": 5, "avg_vol": 1000.0},
            {"peak_date": "2026-03-03", "end_date": "2026-03-07", "peak_price": 48.0, "low_price": 43.0, "decline_pct": -10.4, "duration_days": 4, "avg_vol": 1200.0},
            {"peak_date": "2026-02-26", "end_date": "2026-03-01", "peak_price": 47.0, "low_price": 42.0, "decline_pct": -10.6, "duration_days": 3, "avg_vol": 1100.0},
        ],
    )
    @patch(
        "src.distribution_analyzer.find_volume_price_signals",
        return_value={
            "放量滞涨": [("2026-05-11", 0.1, 1.8, 46.2)],
            "放量下跌": [("2026-05-12", -2.1, 1.9, 45.1)],
            "天量天价": [("2026-05-15", 1.5, 300000, 47.8)],
            "量价背离_顶部": [("2026-05-18", 2.0, 10.0, 48.3)],
            "高位长上影": [("2026-05-19", -0.3, 0.6, 48.1)],
            "破位下跌": [("2026-05-20", -1.5, 46.7, 46.5)],
            "连续缩量阴跌": [("2026-05-21", -1.0, 0.7, 46.0)],
        },
    )
    @patch("src.distribution_analyzer.fetch_daily_kline")
    def test_generate_detailed_report_limits_expensive_intraday_dates(
        self,
        mock_fetch_daily_kline,
        _mock_find_signals,
        _mock_phases,
        mock_fetch_transactions,
        mock_fetch_minutes,
        _mock_create_client,
    ):
        daily_df = pd.DataFrame(
            [
                {
                    "trade_date": (pd.Timestamp("2026-04-13") + pd.Timedelta(days=idx)).date(),
                    "open": 46.0 + idx * 0.1,
                    "high": 46.5 + idx * 0.1,
                    "low": 45.8 + idx * 0.1,
                    "close": 46.1 + idx * 0.1,
                    "vol": 100000 + idx * 1000,
                    "amount": 4600000 + idx * 10000,
                }
                for idx in range(40)
            ]
        )
        daily_df.index = pd.to_datetime(daily_df["trade_date"])
        mock_fetch_daily_kline.return_value = daily_df
        mock_fetch_transactions.return_value = pd.DataFrame()
        mock_fetch_minutes.return_value = pd.DataFrame()

        generate_detailed_report("000733.SZ", "振华科技", engine=None)

        self.assertEqual(mock_fetch_transactions.call_count, 4)
        self.assertEqual(mock_fetch_minutes.call_count, 4)
        called_dates = [call.args[1] for call in mock_fetch_transactions.call_args_list]
        self.assertEqual(called_dates, sorted(called_dates)[-4:])

    def test_create_client_prefers_server_with_live_tick_data(self):
        from src.distribution_analyzer import create_client

        class FakeClient:
            def __init__(self, bars_ok: bool, tick_ok: bool):
                self.bars_ok = bars_ok
                self.tick_ok = tick_ok
                self.closed = False

            def bars(self, symbol, frequency, offset):
                return pd.DataFrame([{"close": 1}]) if self.bars_ok else pd.DataFrame()

            def transactions(self, symbol, start, offset, date):
                return pd.DataFrame([{"price": 1, "vol": 1}]) if self.tick_ok else pd.DataFrame()

            def close(self):
                self.closed = True

        default_client = FakeClient(bars_ok=True, tick_ok=False)
        good_client = FakeClient(bars_ok=True, tick_ok=True)

        class FakeQuotes:
            calls = []

            @staticmethod
            def factory(*args, **kwargs):
                FakeQuotes.calls.append(kwargs)
                if len(FakeQuotes.calls) == 1:
                    return default_client
                if len(FakeQuotes.calls) == 2:
                    return good_client
                raise AssertionError("unexpected extra factory call")

        with patch.dict("sys.modules", {"mootdx.quotes": Mock(Quotes=FakeQuotes)}):
            client = create_client()

        self.assertIs(client, good_client)
        self.assertTrue(default_client.closed)
        self.assertEqual(len(FakeQuotes.calls), 2)

    @patch("src.distribution_analyzer.create_client", side_effect=AssertionError("client should not be created"))
    def test_fetch_transactions_reads_compact_date_ticks_from_db_cache(self, _mock_create_client):
        from src.distribution_report_store import ensure_tables, save_compressed_ticks

        engine = self._build_sqlite_engine()
        ensure_tables(engine)
        tick_df = pd.DataFrame(
            {
                "price": [10.0, 10.1],
                "vol": [100, 200],
                "buyorsell": [0, 1],
            }
        )
        save_compressed_ticks(engine, "000733", "20260522", tick_df)

        result = fetch_transactions("000733.SZ", "20260522", engine=engine, allow_live_fetch=False)

        self.assertEqual(len(result), 2)
        self.assertListEqual(list(result["price"]), [10.0, 10.1])

    @patch("src.distribution_analyzer.create_client", side_effect=AssertionError("client should not be created"))
    def test_fetch_transactions_skips_live_fallback_for_stale_dates(self, _mock_create_client):
        result = fetch_transactions("000733.SZ", "20200102", engine=None)

        self.assertTrue(result.empty)

    @patch("src.distribution_analyzer.fetch_stock_intraday_from_mootdx", side_effect=AssertionError("live fetch should not run"))
    @patch("src.distribution_analyzer.get_stock_intraday_timeseries", return_value=pd.DataFrame())
    def test_fetch_minutes_skips_live_fallback_for_stale_dates(self, _mock_cached, _mock_live):
        result = fetch_minutes("000733.SZ", "2020-01-02", engine=None)

        self.assertTrue(result.empty)

    @patch("app.get_daily_report", return_value="# report")
    @patch(
        "app.get_report_status",
        return_value={
            "ts_code": "000733.SZ",
            "status": "ready",
            "latest_ready_trade_date": "2026-05-23",
            "error_message": None,
        },
    )
    def test_get_distribution_report_state_uses_full_ts_code_key(
        self,
        mock_get_report_status,
        mock_get_daily_report,
    ):
        engine = Mock()

        state = _get_distribution_report_state("000733.SZ", engine)

        self.assertTrue(state["ready"])
        self.assertEqual(state["trade_date"], "2026-05-23")
        self.assertEqual(state["report_md"], "# report")
        mock_get_report_status.assert_called_once_with(engine, "000733.SZ")
        mock_get_daily_report.assert_called_once_with(engine, "000733.SZ", "2026-05-23")

    def test_queue_security_search_navigation_sets_sidebar_and_pending_keyword(self):
        import app

        with patch.object(app.st, "session_state", {}, create=True):
            queue_security_search_navigation("000733.SZ", "stock")

            self.assertEqual(app.st.session_state["pending_security_search_keyword"], "000733.SZ")
            self.assertEqual(app.st.session_state["pending_security_search_type"], "股票")
            self.assertEqual(app.st.session_state["sidebar_nav_group"], "股票")
            self.assertEqual(app.st.session_state["sidebar_expanded_module_id"], "stock")
            self.assertEqual(app.st.session_state["stock_subpage"], app.STOCK_SECURITY_SEARCH_LABEL)
            self.assertTrue(app.st.session_state["jump_to_security_tab"])

    def test_should_show_distribution_report_section_requires_stock_watchlist(self):
        self.assertTrue(should_show_distribution_report_section("stock", True))
        self.assertFalse(should_show_distribution_report_section("stock", False))
        self.assertFalse(should_show_distribution_report_section("index", True))


if __name__ == "__main__":
    unittest.main()
