import unittest
from datetime import date

from src.hotmoney_window import DAILY_QUERY_LABEL, resolve_hotmoney_detail_date_window


class HotmoneyWindowTests(unittest.TestCase):
    def test_daily_query_uses_selected_historical_trade_date(self):
        window = resolve_hotmoney_detail_date_window(
            latest_date="20260605",
            detail_window=DAILY_QUERY_LABEL,
            selected_date=date(2026, 6, 3),
        )

        self.assertEqual(window.start_date.strftime("%Y%m%d"), "20260603")
        self.assertEqual(window.end_date.strftime("%Y%m%d"), "20260603")
        self.assertEqual(window.label, "2026-06-03")

    def test_daily_query_defaults_to_latest_date(self):
        window = resolve_hotmoney_detail_date_window(
            latest_date="20260605",
            detail_window=DAILY_QUERY_LABEL,
            selected_date=None,
        )

        self.assertEqual(window.start_date.strftime("%Y%m%d"), "20260605")
        self.assertEqual(window.end_date.strftime("%Y%m%d"), "20260605")

    def test_recent_windows_keep_existing_ranges(self):
        five_day = resolve_hotmoney_detail_date_window(
            latest_date="20260605",
            detail_window="最近5日",
            selected_date=date(2026, 6, 1),
        )
        twenty_day = resolve_hotmoney_detail_date_window(
            latest_date="20260605",
            detail_window="最近20日",
            selected_date=date(2026, 6, 1),
        )

        self.assertEqual(five_day.start_date.strftime("%Y%m%d"), "20260529")
        self.assertEqual(five_day.end_date.strftime("%Y%m%d"), "20260605")
        self.assertEqual(twenty_day.start_date.strftime("%Y%m%d"), "20260506")
        self.assertEqual(twenty_day.end_date.strftime("%Y%m%d"), "20260605")


if __name__ == "__main__":
    unittest.main()
