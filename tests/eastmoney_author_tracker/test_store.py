import unittest

from src.eastmoney_author_tracker.store import build_author_summary, list_cycles_with_scores


class AuthorSummaryTests(unittest.TestCase):
    def test_build_author_summary_uses_closed_scored_cycles_for_win_rate(self):
        rows = [
            {"cycle_status": "active", "total_return": None, "exit_quality_2d": None},
            {"cycle_status": "closed", "total_return": 0.10, "exit_quality_2d": True},
            {"cycle_status": "closed", "total_return": -0.02, "exit_quality_2d": False},
            {"cycle_status": "expired", "total_return": None, "exit_quality_2d": None},
        ]

        summary = build_author_summary(rows)

        self.assertEqual(summary["cycle_count"], 4)
        self.assertEqual(summary["active_count"], 1)
        self.assertEqual(summary["closed_count"], 3)
        self.assertEqual(summary["scored_closed_count"], 2)
        self.assertEqual(summary["win_count"], 1)
        self.assertAlmostEqual(summary["win_rate"], 0.5)
        self.assertAlmostEqual(summary["avg_return"], 0.04)
        self.assertAlmostEqual(summary["effective_exit_rate"], 0.5)

    def test_list_cycles_with_scores_includes_security_name_from_basic_or_latest_event(self):
        class _FakeResult:
            def mappings(self):
                return self

            def __iter__(self):
                return iter([
                    {
                        "cycle_id": "c1",
                        "author_uid": "u1",
                        "ts_code": "600030.SH",
                        "cycle_open_time": "2026-05-08 14:57:43",
                        "cycle_close_time": None,
                        "cycle_status": "active",
                        "open_mention_id": "m1",
                        "close_mention_id": None,
                        "close_reason": None,
                        "security_name": "中信证券",
                        "total_return": None,
                        "max_drawdown": None,
                        "hold_days": None,
                        "exit_quality_2d": 1,
                        "event_count": 2,
                        "latest_mention_time": "2026-05-12 10:00:00",
                        "latest_direction": "bullish",
                        "latest_source_type": "author_reply",
                        "latest_reason_text": "继续看好",
                    }
                ])

        class _FakeConn:
            def execute(self, *_args, **_kwargs):
                return _FakeResult()

        class _FakeBegin:
            def __enter__(self):
                return _FakeConn()

            def __exit__(self, exc_type, exc, tb):
                return False

        class _FakeEngine:
            def begin(self):
                return _FakeBegin()

        rows = list_cycles_with_scores(_FakeEngine())

        self.assertEqual(rows[0]["security_name"], "中信证券")
        self.assertIs(rows[0]["exit_quality_2d"], True)


if __name__ == "__main__":
    unittest.main()
