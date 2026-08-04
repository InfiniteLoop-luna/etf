import unittest
from unittest.mock import patch

import pandas as pd

from scripts.funding_freshness_summary import get_latest_open_trade_date_ymd


class FundingFreshnessSummaryTests(unittest.TestCase):
    def _fake_calendar(self):
        return pd.DataFrame(
            [
                {"cal_date": "20260807", "is_open": 1},
                {"cal_date": "20260808", "is_open": 0},
                {"cal_date": "20260809", "is_open": 0},
                {"cal_date": "20260810", "is_open": 1},
            ]
        )

    def test_before_21_on_trade_day_uses_previous_open_day(self):
        fake_calendar = self._fake_calendar()

        class FakePro:
            def trade_cal(self, exchange, start_date, end_date, is_open):
                return fake_calendar

        fake_now = pd.Timestamp("2026-08-10 20:59:00", tz="Asia/Shanghai").to_pydatetime()

        class FakeDateTime:
            @classmethod
            def now(cls, tz=None):
                return fake_now

        with patch("scripts.funding_freshness_summary._init_tushare", return_value=FakePro()):
            with patch("scripts.funding_freshness_summary.datetime", FakeDateTime):
                self.assertEqual(get_latest_open_trade_date_ymd(publish_cutoff_hour=21), "20260807")

    def test_before_23_on_trade_day_uses_previous_open_day_by_default(self):
        fake_calendar = self._fake_calendar()

        class FakePro:
            def trade_cal(self, exchange, start_date, end_date, is_open):
                return fake_calendar

        fake_now = pd.Timestamp("2026-08-10 22:01:00", tz="Asia/Shanghai").to_pydatetime()

        class FakeDateTime:
            @classmethod
            def now(cls, tz=None):
                return fake_now

        with patch("scripts.funding_freshness_summary._init_tushare", return_value=FakePro()):
            with patch("scripts.funding_freshness_summary.datetime", FakeDateTime):
                self.assertEqual(get_latest_open_trade_date_ymd(), "20260807")

    def test_after_21_on_trade_day_uses_current_open_day_when_explicit_cutoff_is_21(self):
        fake_calendar = self._fake_calendar()

        class FakePro:
            def trade_cal(self, exchange, start_date, end_date, is_open):
                return fake_calendar

        fake_now = pd.Timestamp("2026-08-10 21:01:00", tz="Asia/Shanghai").to_pydatetime()

        class FakeDateTime:
            @classmethod
            def now(cls, tz=None):
                return fake_now

        with patch("scripts.funding_freshness_summary._init_tushare", return_value=FakePro()):
            with patch("scripts.funding_freshness_summary.datetime", FakeDateTime):
                self.assertEqual(get_latest_open_trade_date_ymd(publish_cutoff_hour=21), "20260810")

    def test_latest_open_trade_date_falls_back_to_weekday_when_calendar_unavailable(self):
        fake_now = pd.Timestamp("2026-08-09 10:00:00", tz="Asia/Shanghai").to_pydatetime()

        class FakeDateTime:
            @classmethod
            def now(cls, tz=None):
                return fake_now

        with patch("scripts.funding_freshness_summary._init_tushare", side_effect=RuntimeError("boom")):
            with patch("scripts.funding_freshness_summary.datetime", FakeDateTime):
                self.assertEqual(get_latest_open_trade_date_ymd(), "20260807")


if __name__ == "__main__":
    unittest.main()
