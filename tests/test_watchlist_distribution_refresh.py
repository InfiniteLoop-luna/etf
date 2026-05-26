import unittest
from unittest.mock import Mock

from sqlalchemy import create_engine, text

from src.distribution_report_store import (
    ensure_tables,
    get_daily_report,
    get_report_status,
    release_refresh_lock,
    save_daily_report,
    try_acquire_refresh_lock,
)
from src.watchlist_distribution_refresh import (
    load_watchlist_stock_symbols,
    refresh_watchlist_distribution_reports,
)


class WatchlistDistributionRefreshTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE app_user_watchlist (
                        username VARCHAR(64) NOT NULL,
                        ts_code VARCHAR(20) NOT NULL,
                        security_type VARCHAR(20) NOT NULL,
                        security_name VARCHAR(120),
                        updated_at TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE vw_ts_stock_daily (
                        ts_code VARCHAR(20) NOT NULL,
                        trade_date DATE NOT NULL,
                        open NUMERIC,
                        high NUMERIC,
                        low NUMERIC,
                        close NUMERIC,
                        vol NUMERIC,
                        amount NUMERIC
                    )
                    """
                )
            )
        ensure_tables(self.engine)

    def _insert_watchlist_row(self, username: str, ts_code: str, security_type: str = "stock", security_name: str = ""):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO app_user_watchlist (username, ts_code, security_type, security_name, updated_at)
                    VALUES (:username, :ts_code, :security_type, :security_name, CURRENT_TIMESTAMP)
                    """
                ),
                {
                    "username": username,
                    "ts_code": ts_code,
                    "security_type": security_type,
                    "security_name": security_name,
                },
            )

    def _insert_daily_row(self, ts_code: str, trade_date: str):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO vw_ts_stock_daily (ts_code, trade_date, open, high, low, close, vol, amount)
                    VALUES (:ts_code, :trade_date, 10, 11, 9, 10.5, 1000, 10000)
                    """
                ),
                {
                    "ts_code": ts_code,
                    "trade_date": trade_date,
                },
            )

    def test_watchlist_union_is_deduplicated(self):
        self._insert_watchlist_row("alice", "000733.SZ", security_name="\u632f\u534e\u79d1\u6280")
        self._insert_watchlist_row("bob", "000733.SZ", security_name="\u632f\u534e\u79d1\u6280")
        self._insert_watchlist_row("alice", "399001.SZ", security_type="index", security_name="\u6df1\u8bc1\u6210\u6307")
        self._insert_watchlist_row("carol", "000001.SZ", security_name="\u5e73\u5b89\u94f6\u884c")

        symbols = load_watchlist_stock_symbols(self.engine)

        self.assertEqual(symbols, ["000001.SZ", "000733.SZ"])

    def test_ready_reports_skip_recompute(self):
        self._insert_watchlist_row("alice", "000733.SZ", security_name="\u632f\u534e\u79d1\u6280")
        self._insert_daily_row("000733.SZ", "2026-05-23")
        save_daily_report(self.engine, "000733.SZ", "2026-05-23", "# cached report")
        report_generator = Mock(side_effect=AssertionError("report generator should not run"))

        summary = refresh_watchlist_distribution_reports(
            self.engine,
            report_generator=report_generator,
        )

        status = get_report_status(self.engine, "000733.SZ")
        self.assertEqual(summary["skipped"], 1)
        self.assertEqual(summary["generated"], 0)
        self.assertEqual(status["status"], "ready")
        self.assertEqual(status["latest_ready_trade_date"], "2026-05-23")
        report_generator.assert_not_called()

    def test_background_refresh_generates_report_and_marks_ready(self):
        self._insert_watchlist_row("alice", "000733.SZ", security_name="\u632f\u534e\u79d1\u6280")
        self._insert_daily_row("000733.SZ", "2026-05-23")
        report_generator = Mock(return_value="# generated report")

        summary = refresh_watchlist_distribution_reports(
            self.engine,
            report_generator=report_generator,
        )

        cached_report = get_daily_report(self.engine, "000733.SZ", "2026-05-23")
        status = get_report_status(self.engine, "000733.SZ")
        self.assertEqual(summary["generated"], 1)
        self.assertEqual(cached_report, "# generated report")
        self.assertEqual(status["status"], "ready")
        self.assertEqual(status["latest_ready_trade_date"], "2026-05-23")
        report_generator.assert_called_once_with(
            "000733.SZ",
            "\u632f\u534e\u79d1\u6280",
            engine=self.engine,
            asof_trade_date="2026-05-23",
            allow_live_fetch=True,
        )

    def test_refresh_skips_when_global_lock_is_held(self):
        acquired = try_acquire_refresh_lock(
            self.engine,
            "watchlist_distribution_refresh",
            owner_id="holder-1",
            timeout_seconds=1800,
        )
        self.assertTrue(acquired)
        report_generator = Mock(side_effect=AssertionError("report generator should not run while locked"))

        summary = refresh_watchlist_distribution_reports(
            self.engine,
            report_generator=report_generator,
        )

        self.assertEqual(summary["locked"], 1)
        self.assertEqual(summary["processed"], 0)
        report_generator.assert_not_called()
        release_refresh_lock(self.engine, "watchlist_distribution_refresh", owner_id="holder-1")


if __name__ == "__main__":
    unittest.main()
