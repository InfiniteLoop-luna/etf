import unittest

from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from src.eastmoney_author_tracker.store import (
    build_author_summary,
    get_author_tracking_metadata,
    list_cycles_with_scores,
    list_author_score_snapshots,
    load_price_history_by_codes,
    load_mention_overrides_map,
    upsert_author_score_snapshot,
    upsert_mention_override,
)


def _build_sqlite_engine():
    return create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


class AuthorSummaryTests(unittest.TestCase):
    def test_build_author_summary_uses_closed_scored_cycles_for_win_rate(self):
        rows = [
            {"cycle_status": "active", "total_return": None, "exit_quality_2d": None, "exit_quality_10d": None},
            {
                "cycle_status": "closed",
                "total_return": 0.10,
                "excess_return": 0.06,
                "hold_days": 5,
                "exit_quality_2d": True,
                "exit_quality_10d": True,
            },
            {
                "cycle_status": "closed",
                "total_return": -0.02,
                "excess_return": -0.01,
                "hold_days": 3,
                "exit_quality_2d": False,
                "exit_quality_10d": False,
            },
            {"cycle_status": "expired", "total_return": None, "hold_days": 10, "exit_quality_2d": None, "exit_quality_10d": None},
        ]

        summary = build_author_summary(rows)

        self.assertEqual(summary["cycle_count"], 4)
        self.assertEqual(summary["active_count"], 1)
        self.assertEqual(summary["closed_count"], 3)
        self.assertEqual(summary["scored_closed_count"], 2)
        self.assertEqual(summary["win_count"], 1)
        self.assertAlmostEqual(summary["win_rate"], 0.5)
        self.assertAlmostEqual(summary["avg_return"], 0.04)
        self.assertAlmostEqual(summary["avg_excess_return"], 0.025)
        self.assertAlmostEqual(summary["avg_hold_days"], 6.0)
        self.assertAlmostEqual(summary["payoff_ratio"], 5.0)
        self.assertAlmostEqual(summary["effective_exit_rate"], 0.5)

    def test_list_cycles_with_scores_includes_security_name_from_basic_or_latest_event(self):
        class _FakeResult:
            def mappings(self):
                return self

            def __iter__(self):
                return iter(
                    [
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
                            "benchmark_return": 0.03,
                            "excess_return": 0.02,
                            "max_drawdown": None,
                            "hold_days": None,
                            "exit_quality_2d": 1,
                            "exit_quality_5d": 0,
                            "exit_quality_10d": 1,
                            "exit_quality_20d": 0,
                            "event_count": 2,
                            "latest_mention_time": "2026-05-12 10:00:00",
                            "latest_direction": "bullish",
                            "latest_source_type": "author_reply",
                            "latest_reason_text": "继续看好",
                        }
                    ]
                )

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
        self.assertIs(rows[0]["exit_quality_5d"], False)
        self.assertIs(rows[0]["exit_quality_10d"], True)
        self.assertIs(rows[0]["exit_quality_20d"], False)
        self.assertAlmostEqual(rows[0]["benchmark_return"], 0.03)
        self.assertAlmostEqual(rows[0]["excess_return"], 0.02)

    def test_load_price_history_by_codes_groups_rows_by_ts_code(self):
        engine = _build_sqlite_engine()
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE vw_ts_stock_daily (
                        ts_code TEXT,
                        trade_date TEXT,
                        close REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO vw_ts_stock_daily (ts_code, trade_date, close)
                    VALUES (:ts_code, :trade_date, :close)
                    """
                ),
                [
                    {"ts_code": "600030.SH", "trade_date": "2026-05-08", "close": 10.0},
                    {"ts_code": "600030.SH", "trade_date": "2026-05-09", "close": 10.6},
                    {"ts_code": "000001.SZ", "trade_date": "2026-05-09", "close": 12.3},
                ],
            )

        history = load_price_history_by_codes(engine, ["600030.SH", "000001.SZ"], start_date="2026-05-09")

        self.assertEqual(sorted(history.keys()), ["000001.SZ", "600030.SH"])
        self.assertEqual(len(history["600030.SH"]), 1)
        self.assertEqual(history["600030.SH"][0]["trade_date"], "2026-05-09")
        self.assertEqual(history["000001.SZ"][0]["close"], 12.3)

    def test_upsert_mention_override_round_trips_flags(self):
        engine = _build_sqlite_engine()

        upsert_mention_override(
            engine,
            "m1",
            override_ts_code="000001.SZ",
            override_direction="bullish",
            is_excluded=True,
            force_new_cycle=True,
            override_note="manual note",
        )

        override_map = load_mention_overrides_map(engine, ["m1"])

        self.assertEqual(override_map["m1"]["override_ts_code"], "000001.SZ")
        self.assertEqual(override_map["m1"]["override_direction"], "bullish")
        self.assertIs(override_map["m1"]["is_excluded"], True)
        self.assertIs(override_map["m1"]["force_new_cycle"], True)
        self.assertEqual(override_map["m1"]["override_note"], "manual note")

    def test_upsert_author_score_snapshot_round_trips_summary_metrics(self):
        engine = _build_sqlite_engine()

        upsert_author_score_snapshot(
            engine,
            "4348595203199492",
            snapshot_date="2026-05-12",
            summary={
                "cycle_count": 3,
                "active_count": 1,
                "closed_count": 2,
                "scored_closed_count": 2,
                "win_count": 1,
                "win_rate": 0.5,
                "avg_return": 0.04,
                "effective_exit_rate": 1.0,
            },
        )
        upsert_author_score_snapshot(
            engine,
            "4348595203199492",
            snapshot_date="2026-05-13",
            summary={
                "cycle_count": 4,
                "active_count": 1,
                "closed_count": 3,
                "scored_closed_count": 3,
                "win_count": 2,
                "win_rate": 2 / 3,
                "avg_return": 0.06,
                "effective_exit_rate": 0.5,
            },
        )

        rows = list_author_score_snapshots(engine, "4348595203199492", limit=10)

        self.assertEqual([row["snapshot_date"] for row in rows], ["2026-05-12", "2026-05-13"])
        self.assertEqual(rows[-1]["cycle_count"], 4)
        self.assertEqual(rows[-1]["closed_count"], 3)
        self.assertAlmostEqual(rows[-1]["win_rate"], 2 / 3)
        self.assertAlmostEqual(rows[-1]["avg_return"], 0.06)

    def test_get_author_tracking_metadata_includes_pending_ocr_and_last_ocr_update_time(self):
        engine = _build_sqlite_engine()
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE em_author_posts (
                        post_id INTEGER PRIMARY KEY,
                        author_uid TEXT,
                        post_publish_time TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE em_stock_mentions (
                        mention_id TEXT PRIMARY KEY,
                        mention_time TEXT
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE em_author_post_images (
                        post_id INTEGER,
                        image_index INTEGER,
                        image_url TEXT,
                        ocr_status TEXT,
                        ocr_text TEXT,
                        ocr_provider TEXT,
                        ocr_updated_at TEXT,
                        PRIMARY KEY (post_id, image_index)
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO em_author_posts (post_id, author_uid, post_publish_time)
                    VALUES (1001, '4348595203199492', '2026-05-12 20:08:17')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO em_stock_mentions (mention_id, mention_time)
                    VALUES ('m1', '2026-05-12 21:00:00')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO em_author_post_images (
                        post_id, image_index, image_url, ocr_status, ocr_text, ocr_provider, ocr_updated_at
                    ) VALUES
                    (1001, 0, 'https://example.com/0.png', 'pending', '', 'deferred', NULL),
                    (1001, 1, 'https://example.com/1.png', 'ok', '继续看好 600030', 'fake-ocr', '2026-05-13 09:30:00')
                    """
                )
            )

        metadata = get_author_tracking_metadata(engine)

        self.assertEqual(metadata["post_count"], 1)
        self.assertEqual(metadata["mention_count"], 1)
        self.assertEqual(metadata["pending_image_count"], 1)
        self.assertEqual(metadata["ocr_processed_image_count"], 1)
        self.assertEqual(metadata["last_ocr_update_time"], "2026-05-13 09:30:00")


if __name__ == "__main__":
    unittest.main()
